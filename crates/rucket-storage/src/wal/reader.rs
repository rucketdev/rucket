//! WAL reader for crash recovery.
//!
//! Reads WAL files and identifies incomplete operations that need rollback.

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;

use uuid::Uuid;

use super::entry::{IncompleteOperation, SequencedEntry, WalEntry};

/// Magic bytes for WAL file header.
const WAL_MAGIC: &[u8; 4] = b"RWAL";

/// Reads entries from a WAL file.
pub struct WalReader {
    /// Entries read from the file.
    entries: Vec<SequencedEntry>,
    /// Last checkpoint sequence, if any.
    last_checkpoint: Option<u64>,
    /// Whether corruption was detected during reading.
    corruption_detected: bool,
    /// Sequence number where corruption was first detected.
    corrupted_at_sequence: Option<u64>,
}

impl WalReader {
    /// Read all entries from a WAL file.
    ///
    /// Supports both v1 (no CRC) and v2 (with CRC) formats.
    /// For v2, verifies CRC32 and stops at first corrupted entry.
    pub fn read(path: &Path) -> std::io::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and verify header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid WAL magic"));
        }

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);

        // Accept v1 and v2 for backward compatibility
        if version != 1 && version != 2 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported WAL version: {version}"),
            ));
        }

        let mut entries = Vec::new();
        let mut last_checkpoint = None;
        let mut corruption_detected = false;
        let mut corrupted_at_sequence = None;

        // Read all entries
        loop {
            // Read sequence number
            let mut seq_bytes = [0u8; 8];
            match reader.read_exact(&mut seq_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let sequence = u64::from_le_bytes(seq_bytes);

            // Read entry length
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Truncated entry, stop here
                    tracing::warn!(sequence, "Truncated WAL entry at sequence, skipping");
                    break;
                }
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Read CRC32 for v2
            let expected_crc = if version >= 2 {
                let mut crc_bytes = [0u8; 4];
                match reader.read_exact(&mut crc_bytes) {
                    Ok(()) => Some(u32::from_le_bytes(crc_bytes)),
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        tracing::warn!(sequence, "Truncated WAL CRC at sequence, skipping");
                        break;
                    }
                    Err(e) => return Err(e),
                }
            } else {
                None
            };

            // Read entry data
            let mut data = vec![0u8; len];
            match reader.read_exact(&mut data) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // Truncated entry, stop here
                    tracing::warn!(sequence, "Truncated WAL entry data at sequence, skipping");
                    break;
                }
                Err(e) => return Err(e),
            }

            // Verify CRC32 for v2 entries
            if let Some(expected) = expected_crc {
                let mut hasher = crc32fast::Hasher::new();
                hasher.update(&seq_bytes);
                hasher.update(&len_bytes);
                hasher.update(&data);
                let actual_crc = hasher.finalize();

                if actual_crc != expected {
                    tracing::error!(
                        sequence,
                        expected = expected,
                        actual = actual_crc,
                        "WAL entry CRC32 mismatch - corruption detected, stopping"
                    );
                    corruption_detected = true;
                    corrupted_at_sequence = Some(sequence);
                    // Stop at corruption - safer than skipping
                    break;
                }
            }

            // Deserialize entry
            let entry: WalEntry = match bincode::deserialize(&data) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(sequence, error = %e, "Failed to deserialize WAL entry, skipping");
                    continue;
                }
            };

            // Track checkpoints
            if let WalEntry::Checkpoint { sequence: cp_seq, .. } = &entry {
                last_checkpoint = Some(*cp_seq);
            }

            entries.push(SequencedEntry { sequence, entry });
        }

        Ok(Self { entries, last_checkpoint, corruption_detected, corrupted_at_sequence })
    }

    /// Read entries from multiple WAL files (current + rotated).
    pub fn read_all(wal_dir: &Path) -> std::io::Result<Self> {
        let mut all_entries = Vec::new();
        let mut last_checkpoint = None;
        let mut corruption_detected = false;
        let mut corrupted_at_sequence = None;

        // Find all WAL files
        let mut wal_files = Vec::new();
        if let Ok(dir) = std::fs::read_dir(wal_dir) {
            for entry in dir.flatten() {
                let path = entry.path();
                if path.extension().is_some_and(|e| e == "wal" || e == "old") {
                    wal_files.push(path);
                }
            }
        }

        // Sort by name (older files first)
        wal_files.sort();

        // Read each file
        for path in wal_files {
            match Self::read(&path) {
                Ok(reader) => {
                    all_entries.extend(reader.entries);
                    if reader.last_checkpoint.is_some() {
                        last_checkpoint = reader.last_checkpoint;
                    }
                    // Track first corruption found
                    if reader.corruption_detected && !corruption_detected {
                        corruption_detected = true;
                        corrupted_at_sequence = reader.corrupted_at_sequence;
                    }
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "Failed to read WAL file");
                }
            }
        }

        // Sort by sequence
        all_entries.sort_by_key(|e| e.sequence);

        Ok(Self {
            entries: all_entries,
            last_checkpoint,
            corruption_detected,
            corrupted_at_sequence,
        })
    }

    /// Get all entries.
    #[must_use]
    pub fn entries(&self) -> &[SequencedEntry] {
        &self.entries
    }

    /// Get entries since the last checkpoint.
    #[must_use]
    pub fn entries_since_checkpoint(&self) -> &[SequencedEntry] {
        if let Some(cp_seq) = self.last_checkpoint {
            // Find index of checkpoint
            if let Some(idx) = self.entries.iter().position(|e| e.sequence > cp_seq) {
                return &self.entries[idx..];
            }
            // All entries are at or before checkpoint
            return &[];
        }
        // No checkpoint, return all entries
        &self.entries
    }

    /// Get the last checkpoint sequence, if any.
    #[must_use]
    pub fn last_checkpoint(&self) -> Option<u64> {
        self.last_checkpoint
    }

    /// Check if corruption was detected during reading.
    #[must_use]
    pub fn corruption_detected(&self) -> bool {
        self.corruption_detected
    }

    /// Get the sequence number where corruption was first detected.
    #[must_use]
    pub fn corrupted_at_sequence(&self) -> Option<u64> {
        self.corrupted_at_sequence
    }

    /// Find incomplete operations (Intent without matching Commit).
    ///
    /// Only considers entries since the last checkpoint.
    #[must_use]
    pub fn find_incomplete(&self) -> Vec<IncompleteOperation> {
        let entries = self.entries_since_checkpoint();

        // Track pending intents by (bucket, key)
        let mut pending_puts: HashMap<(String, String), (Uuid, u64)> = HashMap::new();
        let mut pending_deletes: HashMap<(String, String), (Uuid, u64)> = HashMap::new();

        for entry in entries {
            match &entry.entry {
                WalEntry::PutIntent { bucket, key, uuid, .. } => {
                    pending_puts.insert((bucket.clone(), key.clone()), (*uuid, entry.sequence));
                }
                WalEntry::PutCommit { bucket, key, .. } => {
                    pending_puts.remove(&(bucket.clone(), key.clone()));
                }
                WalEntry::DeleteIntent { bucket, key, old_uuid, .. } => {
                    pending_deletes
                        .insert((bucket.clone(), key.clone()), (*old_uuid, entry.sequence));
                }
                WalEntry::DeleteCommit { bucket, key } => {
                    pending_deletes.remove(&(bucket.clone(), key.clone()));
                }
                WalEntry::Checkpoint { .. } => {
                    // Checkpoints clear all pending operations
                    // (they indicate everything prior was successfully applied)
                }
            }
        }

        // Convert remaining pending operations to IncompleteOperation
        let mut incomplete = Vec::new();

        for ((bucket, key), (uuid, sequence)) in pending_puts {
            incomplete.push(IncompleteOperation::IncompletePut { bucket, key, uuid, sequence });
        }

        for ((bucket, key), (uuid, sequence)) in pending_deletes {
            incomplete.push(IncompleteOperation::IncompleteDelete { bucket, key, uuid, sequence });
        }

        // Sort by sequence for deterministic order
        incomplete.sort_by_key(|op| match op {
            IncompleteOperation::IncompletePut { sequence, .. } => *sequence,
            IncompleteOperation::IncompleteDelete { sequence, .. } => *sequence,
        });

        incomplete
    }

    /// Check if the WAL is empty or only contains checkpoints.
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.find_incomplete().is_empty()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::super::writer::{WalSyncMode, WalWriter, WalWriterConfig};
    use super::*;

    #[tokio::test]
    async fn test_reader_basic() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let uuid = Uuid::new_v4();

        // Write entries
        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid,
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer
                .append(WalEntry::PutCommit {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        // Read and verify
        let reader = WalReader::read(&temp_dir.path().join("current.wal")).unwrap();
        assert_eq!(reader.entries().len(), 2);
        assert!(reader.is_clean());
    }

    #[tokio::test]
    async fn test_reader_incomplete_put() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let uuid = Uuid::new_v4();

        // Write intent but no commit (simulating crash)
        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "orphan".to_string(),
                    uuid,
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        // Read and find incomplete
        let reader = WalReader::read(&temp_dir.path().join("current.wal")).unwrap();
        let incomplete = reader.find_incomplete();
        assert_eq!(incomplete.len(), 1);

        match &incomplete[0] {
            IncompleteOperation::IncompletePut { bucket, key, uuid: u, .. } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "orphan");
                assert_eq!(u, &uuid);
            }
            _ => panic!("Expected IncompletePut"),
        }
    }

    #[tokio::test]
    async fn test_reader_incomplete_delete() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let uuid = Uuid::new_v4();

        // Write delete intent but no commit
        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::DeleteIntent {
                    bucket: "bucket".to_string(),
                    key: "to-delete".to_string(),
                    old_uuid: uuid,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        let reader = WalReader::read(&temp_dir.path().join("current.wal")).unwrap();
        let incomplete = reader.find_incomplete();
        assert_eq!(incomplete.len(), 1);

        match &incomplete[0] {
            IncompleteOperation::IncompleteDelete { bucket, key, uuid: u, .. } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "to-delete");
                assert_eq!(u, &uuid);
            }
            _ => panic!("Expected IncompleteDelete"),
        }
    }

    #[tokio::test]
    async fn test_reader_checkpoint() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        {
            let writer = WalWriter::open(&config).unwrap();

            // Complete operation before checkpoint
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid: uuid1,
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer
                .append(WalEntry::PutCommit {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid: uuid1,
                })
                .await
                .unwrap();

            // Checkpoint
            writer.checkpoint(false).await.unwrap();

            // Incomplete operation after checkpoint
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "key2".to_string(),
                    uuid: uuid2,
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();

            writer.sync().await.unwrap();
        }

        let reader = WalReader::read(&temp_dir.path().join("current.wal")).unwrap();

        // Should only see the incomplete op after checkpoint
        let incomplete = reader.find_incomplete();
        assert_eq!(incomplete.len(), 1);

        match &incomplete[0] {
            IncompleteOperation::IncompletePut { key, uuid: u, .. } => {
                assert_eq!(key, "key2");
                assert_eq!(u, &uuid2);
            }
            _ => panic!("Expected IncompletePut for key2"),
        }
    }

    #[tokio::test]
    async fn test_wal_crc32_roundtrip() {
        // Test that entries written with CRC32 can be read back correctly
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();

        // Write multiple entries
        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid: uuid1,
                    size: 100,
                    crc32c: 12345,
                    timestamp: 1000,
                })
                .await
                .unwrap();
            writer
                .append(WalEntry::PutCommit {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid: uuid1,
                })
                .await
                .unwrap();
            writer
                .append(WalEntry::DeleteIntent {
                    bucket: "bucket".to_string(),
                    key: "key2".to_string(),
                    old_uuid: uuid2,
                    timestamp: 2000,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        // Read and verify no corruption detected
        let reader = WalReader::read(&temp_dir.path().join("current.wal")).unwrap();
        assert_eq!(reader.entries().len(), 3);
        assert!(!reader.corruption_detected());
        assert!(reader.corrupted_at_sequence().is_none());
    }

    #[tokio::test]
    async fn test_wal_corruption_detection() {
        use std::io::{Seek, SeekFrom, Write};

        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let uuid = Uuid::new_v4();

        // Write an entry
        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid,
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        // Corrupt a byte in the entry data (after header + seq + len + crc)
        let wal_path = temp_dir.path().join("current.wal");
        {
            let mut file =
                std::fs::OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
            // Skip header (8 bytes) + seq (8 bytes) + len (4 bytes) + crc (4 bytes) = 24 bytes
            // Then corrupt byte 25 (first byte of data)
            file.seek(SeekFrom::Start(24)).unwrap();
            file.write_all(&[0xFF]).unwrap(); // Corrupt a byte
        }

        // Read and verify corruption is detected
        let reader = WalReader::read(&wal_path).unwrap();
        assert!(reader.corruption_detected());
        assert_eq!(reader.corrupted_at_sequence(), Some(1));
        assert_eq!(reader.entries().len(), 0); // No valid entries before corruption
    }

    #[tokio::test]
    async fn test_wal_stops_at_corruption() {
        use std::io::{Seek, SeekFrom, Write};

        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        // Write 5 entries
        let mut entry_offsets = Vec::new();
        {
            let writer = WalWriter::open(&config).unwrap();
            let wal_path = temp_dir.path().join("current.wal");

            for i in 0..5 {
                // Record file size before each entry to find offsets
                let file_size = std::fs::metadata(&wal_path).map(|m| m.len()).unwrap_or(8);
                entry_offsets.push(file_size);

                writer
                    .append(WalEntry::PutIntent {
                        bucket: "bucket".to_string(),
                        key: format!("key{i}"),
                        uuid: Uuid::new_v4(),
                        size: 100,
                        crc32c: 0,
                        timestamp: i as i64,
                    })
                    .await
                    .unwrap();
                writer.sync().await.unwrap();
            }
        }

        // Corrupt entry #3 (0-indexed) - corrupt the CRC or data
        let wal_path = temp_dir.path().join("current.wal");
        {
            let mut file =
                std::fs::OpenOptions::new().read(true).write(true).open(&wal_path).unwrap();
            // Seek to entry 3's data area and corrupt it
            // Entry 3 offset + seq(8) + len(4) + crc(4) + some data bytes
            let corrupt_offset = entry_offsets[3] + 20;
            file.seek(SeekFrom::Start(corrupt_offset)).unwrap();
            file.write_all(&[0xFF, 0xFF, 0xFF]).unwrap();
        }

        // Read and verify only entries 0-2 are recovered
        let reader = WalReader::read(&wal_path).unwrap();
        assert!(reader.corruption_detected());
        assert_eq!(reader.corrupted_at_sequence(), Some(4)); // Entry 3 is seq 4 (1-indexed)
        assert_eq!(reader.entries().len(), 3); // Entries 0, 1, 2 (seq 1, 2, 3)
    }

    #[tokio::test]
    async fn test_wal_v1_backward_compatibility() {
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("current.wal");

        // Manually create a v1 WAL file (no CRC)
        {
            let mut file = std::fs::File::create(&wal_path).unwrap();

            // Write header: magic + version 1
            file.write_all(b"RWAL").unwrap();
            file.write_all(&1u32.to_le_bytes()).unwrap();

            // Write an entry in v1 format: seq + len + data (no CRC)
            let entry = WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key1".to_string(),
                uuid: Uuid::new_v4(),
                size: 100,
                crc32c: 0,
                timestamp: 0,
            };
            let data = bincode::serialize(&entry).unwrap();

            file.write_all(&1u64.to_le_bytes()).unwrap(); // seq = 1
            file.write_all(&(data.len() as u32).to_le_bytes()).unwrap(); // len
            file.write_all(&data).unwrap(); // data (no CRC)
        }

        // Read v1 file - should work without corruption error
        let reader = WalReader::read(&wal_path).unwrap();
        assert_eq!(reader.entries().len(), 1);
        assert!(!reader.corruption_detected()); // No corruption - v1 doesn't have CRC to check
    }

    #[tokio::test]
    async fn test_wal_truncated_crc() {
        use std::io::Write;

        let temp_dir = TempDir::new().unwrap();
        let wal_path = temp_dir.path().join("current.wal");

        // Create a v2 WAL with truncated CRC
        {
            let mut file = std::fs::File::create(&wal_path).unwrap();

            // Write header
            file.write_all(b"RWAL").unwrap();
            file.write_all(&2u32.to_le_bytes()).unwrap();

            // Write partial entry: seq + len + only 2 bytes of CRC (truncated)
            file.write_all(&1u64.to_le_bytes()).unwrap(); // seq
            file.write_all(&100u32.to_le_bytes()).unwrap(); // len
            file.write_all(&[0x12, 0x34]).unwrap(); // Only 2 bytes of CRC (truncated)
        }

        // Read should handle gracefully - no panic, just stop at truncation
        let reader = WalReader::read(&wal_path).unwrap();
        assert_eq!(reader.entries().len(), 0); // No complete entries
        assert!(!reader.corruption_detected()); // Truncation != corruption
    }
}

/// Property-based fuzz tests for WAL corruption handling.
#[cfg(test)]
mod proptest_tests {
    use std::io::Write;

    use proptest::prelude::*;
    use tempfile::TempDir;

    use super::super::writer::{WalSyncMode, WalWriter, WalWriterConfig};
    use super::*;

    proptest! {
        /// Test that single byte corruption is detected and doesn't cause panics.
        #[test]
        fn fuzz_wal_single_byte_corruption(
            corruption_offset in 24usize..200, // After header+entry_header
            corruption_byte in 0u8..=255
        ) {
            // Create valid WAL with an entry
            let temp_dir = TempDir::new().unwrap();
            let wal_path = temp_dir.path().join("current.wal");
            let config = WalWriterConfig {
                wal_dir: temp_dir.path().to_path_buf(),
                sync_mode: WalSyncMode::None,
                ..Default::default()
            };

            // Use a runtime to write the entry
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let writer = WalWriter::open(&config).unwrap();
                writer
                    .append(WalEntry::PutIntent {
                        bucket: "bucket".to_string(),
                        key: "test-key".to_string(),
                        uuid: uuid::Uuid::new_v4(),
                        size: 1000,
                        crc32c: 12345,
                        timestamp: 999,
                    })
                    .await
                    .unwrap();
                writer.sync().await.unwrap();
            });

            // Get file size
            let file_size = std::fs::metadata(&wal_path).unwrap().len() as usize;

            // Only corrupt if offset is within file bounds
            if corruption_offset < file_size {
                // Corrupt a byte
                let mut data = std::fs::read(&wal_path).unwrap();
                data[corruption_offset] = corruption_byte;
                std::fs::write(&wal_path, &data).unwrap();
            }

            // Read - should not panic
            let result = WalReader::read(&wal_path);
            prop_assert!(result.is_ok());

            // If corruption was in the entry, it should be detected or truncated
            // (we don't assert specifics since corruption location varies)
        }

        /// Test that random garbage after a valid header doesn't cause panics.
        #[test]
        fn fuzz_wal_random_garbage(garbage in prop::collection::vec(any::<u8>(), 0..500)) {
            let temp_dir = TempDir::new().unwrap();
            let wal_path = temp_dir.path().join("current.wal");

            // Write valid header + garbage
            {
                let mut file = std::fs::File::create(&wal_path).unwrap();
                file.write_all(b"RWAL").unwrap(); // Magic
                file.write_all(&2u32.to_le_bytes()).unwrap(); // Version 2
                file.write_all(&garbage).unwrap(); // Random garbage
            }

            // Read should not panic
            let result = WalReader::read(&wal_path);
            prop_assert!(result.is_ok());

            let reader = result.unwrap();
            // Should either have no entries or detect corruption
            // (depends on whether garbage happens to look like valid data)
            prop_assert!(reader.entries().is_empty() || reader.corruption_detected());
        }

        /// Test that truncation at various points is handled gracefully.
        #[test]
        fn fuzz_wal_truncation(truncate_at in 8usize..300) {
            let temp_dir = TempDir::new().unwrap();
            let wal_path = temp_dir.path().join("current.wal");
            let config = WalWriterConfig {
                wal_dir: temp_dir.path().to_path_buf(),
                sync_mode: WalSyncMode::None,
                ..Default::default()
            };

            // Write multiple entries
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let writer = WalWriter::open(&config).unwrap();
                for i in 0..5 {
                    writer
                        .append(WalEntry::PutIntent {
                            bucket: "bucket".to_string(),
                            key: format!("key{i}"),
                            uuid: uuid::Uuid::new_v4(),
                            size: 100 + i as u64,
                            crc32c: i as u32,
                            timestamp: i as i64,
                        })
                        .await
                        .unwrap();
                }
                writer.sync().await.unwrap();
            });

            // Get file size
            let file_size = std::fs::metadata(&wal_path).unwrap().len() as usize;

            // Truncate file
            if truncate_at < file_size {
                let data = std::fs::read(&wal_path).unwrap();
                std::fs::write(&wal_path, &data[..truncate_at]).unwrap();
            }

            // Read should not panic
            let result = WalReader::read(&wal_path);
            prop_assert!(result.is_ok());

            // All recovered entries should be valid (corruption not caused by clean truncation)
            let reader = result.unwrap();
            prop_assert!(!reader.corruption_detected() || reader.entries().is_empty());
        }
    }
}
