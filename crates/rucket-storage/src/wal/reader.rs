//! WAL reader for crash recovery.
//!
//! Reads WAL files and identifies incomplete operations that need rollback.

use std::collections::HashMap;
use std::io::{BufReader, Read};
use std::path::Path;

use uuid::Uuid;

use super::entry::{IncompleteOperation, SequencedEntry, WalEntry};

/// WAL file format version.
const WAL_VERSION: u32 = 1;

/// Magic bytes for WAL file header.
const WAL_MAGIC: &[u8; 4] = b"RWAL";

/// Reads entries from a WAL file.
pub struct WalReader {
    /// Entries read from the file.
    entries: Vec<SequencedEntry>,
    /// Last checkpoint sequence, if any.
    last_checkpoint: Option<u64>,
}

impl WalReader {
    /// Read all entries from a WAL file.
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
        if version != WAL_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported WAL version: {version}"),
            ));
        }

        let mut entries = Vec::new();
        let mut last_checkpoint = None;

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

        Ok(Self { entries, last_checkpoint })
    }

    /// Read entries from multiple WAL files (current + rotated).
    pub fn read_all(wal_dir: &Path) -> std::io::Result<Self> {
        let mut all_entries = Vec::new();
        let mut last_checkpoint = None;

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
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "Failed to read WAL file");
                }
            }
        }

        // Sort by sequence
        all_entries.sort_by_key(|e| e.sequence);

        Ok(Self { entries: all_entries, last_checkpoint })
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
}
