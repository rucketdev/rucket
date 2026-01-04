//! Durability testing for WAL crash recovery.
//!
//! This module provides comprehensive tests for:
//! - Crash recovery at various points in the write/delete lifecycle
//! - Data integrity verification after recovery
//! - Multi-file WAL recovery across rotations
//!
//! # Test Infrastructure
//!
//! - [`WalTestHarness`]: Manages isolated test directories for WAL and data
//! - [`CorruptionInjector`]: Utilities for corrupting WAL and data files

use std::io::{Seek, SeekFrom, Write};
use std::path::PathBuf;

use tempfile::TempDir;
use uuid::Uuid;

use super::entry::WalEntry;
use super::reader::WalReader;
use super::recovery::RecoveryManager;
use super::writer::{WalSyncMode, WalWriter, WalWriterConfig};

/// Test harness for WAL durability testing.
///
/// Provides isolated directories and utilities for testing crash recovery scenarios.
pub struct WalTestHarness {
    /// Temporary directory (kept alive for the test duration).
    _temp_dir: TempDir,
    /// Path to WAL directory.
    pub wal_dir: PathBuf,
    /// Path to data directory.
    pub data_dir: PathBuf,
    /// WAL writer configuration.
    pub config: WalWriterConfig,
}

impl WalTestHarness {
    /// Create a new test harness with isolated directories.
    pub fn new() -> std::io::Result<Self> {
        let temp_dir = TempDir::new()?;
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");

        std::fs::create_dir_all(&wal_dir)?;
        std::fs::create_dir_all(&data_dir)?;

        let config = WalWriterConfig {
            wal_dir: wal_dir.clone(),
            sync_mode: WalSyncMode::None, // Fast for tests
            ..Default::default()
        };

        Ok(Self { _temp_dir: temp_dir, wal_dir, data_dir, config })
    }

    /// Open a WAL writer with the harness configuration.
    pub fn open_writer(&self) -> std::io::Result<WalWriter> {
        WalWriter::open(&self.config)
    }

    /// Create a recovery manager for this harness.
    pub fn recovery_manager(&self) -> RecoveryManager {
        RecoveryManager::new(self.wal_dir.clone(), self.data_dir.clone())
    }

    /// Create a data file for an object (simulating a write).
    pub fn create_data_file(
        &self,
        bucket: &str,
        uuid: Uuid,
        content: &[u8],
    ) -> std::io::Result<PathBuf> {
        let bucket_dir = self.data_dir.join(bucket);
        std::fs::create_dir_all(&bucket_dir)?;
        let file_path = bucket_dir.join(format!("{uuid}.dat"));
        std::fs::write(&file_path, content)?;
        Ok(file_path)
    }

    /// Check if a data file exists.
    pub fn data_file_exists(&self, bucket: &str, uuid: Uuid) -> bool {
        self.data_dir.join(bucket).join(format!("{uuid}.dat")).exists()
    }

    /// Get the path to the current WAL file.
    pub fn wal_path(&self) -> PathBuf {
        self.wal_dir.join("current.wal")
    }
}

impl Default for WalTestHarness {
    fn default() -> Self {
        Self::new().expect("Failed to create test harness")
    }
}

/// Utility for injecting corruption into files.
pub struct CorruptionInjector;

impl CorruptionInjector {
    /// Flip a single bit at the specified byte offset.
    pub fn flip_bit(path: &std::path::Path, offset: u64, bit: u8) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut buf = [0u8; 1];
        std::io::Read::read_exact(&mut file, &mut buf)?;
        buf[0] ^= 1 << (bit % 8);
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&buf)?;
        Ok(())
    }

    /// Overwrite bytes at the specified offset.
    pub fn corrupt_bytes(path: &std::path::Path, offset: u64, bytes: &[u8]) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new().read(true).write(true).open(path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(bytes)?;
        Ok(())
    }

    /// Truncate a file to the specified size.
    pub fn truncate(path: &std::path::Path, size: u64) -> std::io::Result<()> {
        let file = std::fs::OpenOptions::new().write(true).open(path)?;
        file.set_len(size)?;
        Ok(())
    }

    /// Append garbage bytes to a file.
    #[allow(dead_code)]
    pub fn append_garbage(path: &std::path::Path, garbage: &[u8]) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new().append(true).open(path)?;
        file.write_all(garbage)?;
        Ok(())
    }
}

// =============================================================================
// Phase 2: Crash Recovery Tests
// =============================================================================

/// Test: No recovery needed if crash occurs before WAL intent is written.
///
/// Scenario: Process crashes before any WAL entry is written.
/// Expected: Recovery finds nothing to do.
#[tokio::test]
async fn test_crash_before_wal_intent() {
    let harness = WalTestHarness::new().unwrap();

    // Don't write any WAL entries - simulating crash before intent
    // Just create an empty WAL file (or none at all)

    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    assert!(!stats.recovery_needed);
    assert_eq!(stats.puts_rolled_back, 0);
    assert_eq!(stats.deletes_rolled_back, 0);
}

/// Test: Incomplete PUT is rolled back when data file exists but commit is missing.
///
/// Scenario: PutIntent written → data file created → crash before PutCommit
/// Expected: Data file is deleted during recovery.
#[tokio::test]
async fn test_crash_after_intent_before_commit() {
    let harness = WalTestHarness::new().unwrap();
    let uuid = Uuid::new_v4();

    // Write PutIntent to WAL
    {
        let writer = harness.open_writer().unwrap();
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
                uuid,
                size: 100,
                crc32c: 0x12345678,
                timestamp: chrono::Utc::now().timestamp_millis(),
            })
            .await
            .unwrap();
    }

    // Create the data file (simulating successful write before crash)
    harness.create_data_file("test-bucket", uuid, b"test data content").unwrap();
    assert!(harness.data_file_exists("test-bucket", uuid));

    // Simulate crash (no PutCommit written)
    // Run recovery
    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    assert!(stats.recovery_needed);
    assert_eq!(stats.puts_rolled_back, 1);
    assert!(!harness.data_file_exists("test-bucket", uuid), "Orphaned data file should be deleted");
}

/// Test: Completed PUT survives recovery.
///
/// Scenario: PutIntent → data write → PutCommit → crash → recovery
/// Expected: Data file is preserved.
#[tokio::test]
async fn test_crash_after_commit_is_durable() {
    let harness = WalTestHarness::new().unwrap();
    let uuid = Uuid::new_v4();

    // Write complete operation to WAL
    {
        let writer = harness.open_writer().unwrap();
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
                uuid,
                size: 100,
                crc32c: 0x12345678,
                timestamp: chrono::Utc::now().timestamp_millis(),
            })
            .await
            .unwrap();
        writer
            .append_sync(WalEntry::PutCommit {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
                uuid,
            })
            .await
            .unwrap();
    }

    // Create the data file
    harness.create_data_file("test-bucket", uuid, b"committed data").unwrap();

    // Run recovery
    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    assert!(!stats.recovery_needed);
    assert_eq!(stats.puts_rolled_back, 0);
    assert!(
        harness.data_file_exists("test-bucket", uuid),
        "Committed data file should be preserved"
    );
}

/// Test: Incomplete DELETE preserves the file.
///
/// Scenario: DeleteIntent written → crash before DeleteCommit
/// Expected: File is preserved (delete rolled back).
#[tokio::test]
async fn test_crash_during_delete_intent() {
    let harness = WalTestHarness::new().unwrap();
    let uuid = Uuid::new_v4();

    // Create the data file first (object exists before delete)
    harness.create_data_file("test-bucket", uuid, b"data to preserve").unwrap();
    assert!(harness.data_file_exists("test-bucket", uuid));

    // Write DeleteIntent to WAL (but not DeleteCommit - simulating crash)
    {
        let writer = harness.open_writer().unwrap();
        writer
            .append_sync(WalEntry::DeleteIntent {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
                old_uuid: uuid,
                timestamp: chrono::Utc::now().timestamp_millis(),
            })
            .await
            .unwrap();
    }

    // Run recovery
    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    assert!(stats.recovery_needed);
    assert_eq!(stats.deletes_rolled_back, 1);
    assert!(
        harness.data_file_exists("test-bucket", uuid),
        "File should be preserved after incomplete delete"
    );
}

/// Test: Complete DELETE removes the file.
///
/// Scenario: DeleteIntent → file deleted → DeleteCommit → recovery
/// Expected: No rollback needed, file stays deleted.
#[tokio::test]
async fn test_crash_after_delete_commit() {
    let harness = WalTestHarness::new().unwrap();
    let uuid = Uuid::new_v4();

    // Write complete delete operation to WAL
    {
        let writer = harness.open_writer().unwrap();
        writer
            .append_sync(WalEntry::DeleteIntent {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
                old_uuid: uuid,
                timestamp: chrono::Utc::now().timestamp_millis(),
            })
            .await
            .unwrap();
        writer
            .append_sync(WalEntry::DeleteCommit {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
            })
            .await
            .unwrap();
    }

    // Note: file was already deleted as part of the delete operation
    // Don't create the file - it should stay deleted

    // Run recovery
    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    assert!(!stats.recovery_needed);
    assert_eq!(stats.deletes_rolled_back, 0);
    assert!(!harness.data_file_exists("test-bucket", uuid));
}

/// Test: Multiple incomplete operations are all rolled back.
///
/// Scenario: Multiple PutIntents without commits
/// Expected: All orphaned files are deleted.
#[tokio::test]
async fn test_multiple_incomplete_puts() {
    let harness = WalTestHarness::new().unwrap();
    let uuids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

    // Write multiple PutIntents
    {
        let writer = harness.open_writer().unwrap();
        for (i, uuid) in uuids.iter().enumerate() {
            writer
                .append_sync(WalEntry::PutIntent {
                    bucket: "test-bucket".to_string(),
                    key: format!("key-{i}"),
                    uuid: *uuid,
                    size: 100,
                    crc32c: 0,
                    timestamp: chrono::Utc::now().timestamp_millis(),
                })
                .await
                .unwrap();
        }
    }

    // Create data files for all
    for uuid in &uuids {
        harness.create_data_file("test-bucket", *uuid, b"orphaned data").unwrap();
    }

    // Run recovery
    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    assert!(stats.recovery_needed);
    assert_eq!(stats.puts_rolled_back, 5);

    // All files should be deleted
    for uuid in &uuids {
        assert!(!harness.data_file_exists("test-bucket", *uuid));
    }
}

/// Test: Mixed complete and incomplete operations.
///
/// Scenario: Some operations complete, some don't
/// Expected: Only incomplete operations are rolled back.
#[tokio::test]
async fn test_mixed_complete_and_incomplete() {
    let harness = WalTestHarness::new().unwrap();
    let complete_uuid = Uuid::new_v4();
    let incomplete_uuid = Uuid::new_v4();

    {
        let writer = harness.open_writer().unwrap();

        // Complete operation
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "test-bucket".to_string(),
                key: "complete".to_string(),
                uuid: complete_uuid,
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();
        writer
            .append_sync(WalEntry::PutCommit {
                bucket: "test-bucket".to_string(),
                key: "complete".to_string(),
                uuid: complete_uuid,
            })
            .await
            .unwrap();

        // Incomplete operation
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "test-bucket".to_string(),
                key: "incomplete".to_string(),
                uuid: incomplete_uuid,
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();
    }

    // Create both data files
    harness.create_data_file("test-bucket", complete_uuid, b"complete data").unwrap();
    harness.create_data_file("test-bucket", incomplete_uuid, b"incomplete data").unwrap();

    // Run recovery
    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    assert!(stats.recovery_needed);
    assert_eq!(stats.puts_rolled_back, 1);

    // Complete file should exist, incomplete should be deleted
    assert!(harness.data_file_exists("test-bucket", complete_uuid));
    assert!(!harness.data_file_exists("test-bucket", incomplete_uuid));
}

// =============================================================================
// Phase 3: Data Integrity / Corruption Detection Tests
// =============================================================================

/// Test: Single bit flip in WAL entry is detected via CRC32.
#[tokio::test]
async fn test_single_bit_flip_detection() {
    let harness = WalTestHarness::new().unwrap();

    // Write a valid entry
    {
        let writer = harness.open_writer().unwrap();
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "test-bucket".to_string(),
                key: "test-key".to_string(),
                uuid: Uuid::new_v4(),
                size: 1000,
                crc32c: 0x12345678,
                timestamp: 12345,
            })
            .await
            .unwrap();
    }

    // Corrupt a bit in the data section (after header + seq + len + crc = 24 bytes)
    CorruptionInjector::flip_bit(&harness.wal_path(), 30, 3).unwrap();

    // Read should detect corruption
    let reader = WalReader::read(&harness.wal_path()).unwrap();
    assert!(reader.corruption_detected());
    assert_eq!(reader.corrupted_at_sequence(), Some(1));
}

/// Test: Corruption in middle of multi-entry WAL stops at corruption point.
#[tokio::test]
async fn test_corruption_stops_at_bad_entry() {
    let harness = WalTestHarness::new().unwrap();

    // Track file size after each entry
    let mut entry_sizes = Vec::new();

    // Write 5 entries
    {
        let writer = harness.open_writer().unwrap();
        for i in 0..5 {
            let size_before = std::fs::metadata(harness.wal_path()).map(|m| m.len()).unwrap_or(8);
            writer
                .append_sync(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: format!("key{i}"),
                    uuid: Uuid::new_v4(),
                    size: 100,
                    crc32c: i as u32,
                    timestamp: i as i64,
                })
                .await
                .unwrap();
            let size_after = std::fs::metadata(harness.wal_path()).unwrap().len();
            entry_sizes.push((size_before, size_after));
        }
    }

    // Corrupt entry #3 (0-indexed) - corrupt data portion
    let (start, _end) = entry_sizes[3];
    CorruptionInjector::corrupt_bytes(&harness.wal_path(), start + 20, &[0xFF, 0xFF]).unwrap();

    // Read and verify only entries 0-2 are recovered
    let reader = WalReader::read(&harness.wal_path()).unwrap();
    assert!(reader.corruption_detected());
    assert_eq!(reader.entries().len(), 3); // Only 3 valid entries (0, 1, 2)
}

/// Test: Truncated WAL is handled gracefully.
#[tokio::test]
async fn test_truncated_wal_handled() {
    let harness = WalTestHarness::new().unwrap();

    // Write multiple entries
    {
        let writer = harness.open_writer().unwrap();
        for i in 0..5 {
            writer
                .append_sync(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: format!("key{i}"),
                    uuid: Uuid::new_v4(),
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();
        }
    }

    let original_size = std::fs::metadata(harness.wal_path()).unwrap().len();

    // Truncate in the middle of an entry
    CorruptionInjector::truncate(&harness.wal_path(), original_size / 2).unwrap();

    // Read should not panic, and should recover partial data
    let reader = WalReader::read(&harness.wal_path()).unwrap();
    // Truncation is not detected as corruption, just ends early
    assert!(reader.entries().len() < 5);
}

/// Test: Random garbage after valid header is handled.
#[tokio::test]
async fn test_garbage_after_header() {
    let harness = WalTestHarness::new().unwrap();

    // Create WAL with valid header but garbage data
    {
        let mut file = std::fs::File::create(harness.wal_path()).unwrap();
        file.write_all(b"RWAL").unwrap(); // Magic
        file.write_all(&2u32.to_le_bytes()).unwrap(); // Version 2
        file.write_all(&[0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE]).unwrap();
        // Garbage
    }

    // Read should not panic
    let reader = WalReader::read(&harness.wal_path()).unwrap();
    // Garbage might be interpreted as truncated/corrupt entry
    assert!(reader.entries().is_empty() || reader.corruption_detected());
}

// =============================================================================
// Phase 5: Multi-file WAL Tests
// =============================================================================

/// Test: Recovery works across rotated WAL files.
#[tokio::test]
async fn test_recovery_across_rotated_wals() {
    let harness = WalTestHarness::new().unwrap();
    let uuid1 = Uuid::new_v4();
    let uuid2 = Uuid::new_v4();

    // Write to WAL, rotate, write more
    {
        let writer = harness.open_writer().unwrap();

        // Write first entry
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key1".to_string(),
                uuid: uuid1,
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();

        // Rotate WAL
        writer.rotate().await.unwrap();

        // Write second entry (in new WAL file)
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key2".to_string(),
                uuid: uuid2,
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();
    }

    // Create both data files
    harness.create_data_file("bucket", uuid1, b"data1").unwrap();
    harness.create_data_file("bucket", uuid2, b"data2").unwrap();

    // Recovery should find both incomplete operations across files
    let reader = WalReader::read_all(&harness.wal_dir).unwrap();
    let incomplete = reader.find_incomplete();

    assert_eq!(incomplete.len(), 2, "Should find incomplete ops from both WAL files");
}

/// Test: Checkpoint skips prior entries during recovery.
#[tokio::test]
async fn test_checkpoint_skips_prior_entries() {
    let harness = WalTestHarness::new().unwrap();
    let uuid_before = Uuid::new_v4();
    let uuid_after = Uuid::new_v4();

    {
        let writer = harness.open_writer().unwrap();

        // Write incomplete operation before checkpoint
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "before".to_string(),
                uuid: uuid_before,
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();

        // Write checkpoint (marks all prior as complete)
        writer.checkpoint(false).await.unwrap();

        // Write incomplete operation after checkpoint
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "after".to_string(),
                uuid: uuid_after,
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();
    }

    // Create data files
    harness.create_data_file("bucket", uuid_before, b"before data").unwrap();
    harness.create_data_file("bucket", uuid_after, b"after data").unwrap();

    // Read WAL
    let reader = WalReader::read(&harness.wal_path()).unwrap();
    let incomplete = reader.find_incomplete();

    // Only the post-checkpoint entry should be incomplete
    assert_eq!(incomplete.len(), 1);

    // Run recovery
    let recovery = harness.recovery_manager();
    let stats = recovery.recover().await.unwrap();

    // Only post-checkpoint operation should be rolled back
    assert_eq!(stats.puts_rolled_back, 1);

    // Before-checkpoint file should remain (checkpoint implies it was committed elsewhere)
    // After-checkpoint file should be deleted
    assert!(!harness.data_file_exists("bucket", uuid_after));
}

/// Test: Old WAL files are cleaned up after recovery.
#[tokio::test]
async fn test_cleanup_old_wal_files() {
    let harness = WalTestHarness::new().unwrap();

    // Write and rotate multiple times
    {
        let writer = harness.open_writer().unwrap();
        for i in 0..3 {
            writer
                .append_sync(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: format!("key{i}"),
                    uuid: Uuid::new_v4(),
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer.checkpoint(true).await.unwrap(); // Checkpoint and rotate
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await; // Ensure unique timestamps
        }
    }

    // Count WAL files before cleanup
    let files_before: Vec<_> = std::fs::read_dir(&harness.wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "old" || ext == "wal").unwrap_or(false))
        .collect();

    assert!(files_before.len() > 1, "Should have multiple WAL files");

    // Cleanup old WALs
    let recovery = harness.recovery_manager();
    let cleaned = recovery.cleanup_old_wals().await.unwrap();

    assert!(cleaned > 0, "Should have cleaned some old WAL files");

    // Only current.wal should remain
    let files_after: Vec<_> = std::fs::read_dir(&harness.wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|ext| ext == "wal").unwrap_or(false))
        .collect();

    assert_eq!(files_after.len(), 1);
    assert!(files_after[0].path().file_name().unwrap() == "current.wal");
}

/// Test: Recovery is idempotent - running twice produces same result.
#[tokio::test]
async fn test_recovery_idempotent() {
    let harness = WalTestHarness::new().unwrap();
    let uuid = Uuid::new_v4();

    // Write incomplete operation
    {
        let writer = harness.open_writer().unwrap();
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                uuid,
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();
    }

    harness.create_data_file("bucket", uuid, b"data").unwrap();

    // First recovery
    let recovery = harness.recovery_manager();
    let stats1 = recovery.recover().await.unwrap();
    assert_eq!(stats1.puts_rolled_back, 1);
    assert!(!harness.data_file_exists("bucket", uuid));

    // Second recovery should find nothing to do
    // Note: The WAL still has the incomplete entry, but the file is already gone
    let stats2 = recovery.recover().await.unwrap();
    assert_eq!(stats2.puts_rolled_back, 0); // File already deleted
}

// =============================================================================
// Property-based Corruption Tests (using proptest)
// =============================================================================

#[cfg(test)]
mod proptest_durability {
    use proptest::prelude::*;

    use super::*;

    proptest! {
        /// Test that WAL survives random bit flips in the data section.
        #[test]
        fn fuzz_bit_flip_recovery(
            bit_offset in 24u64..200,
            bit_position in 0u8..8
        ) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let (read_ok, recovery_ok) = rt.block_on(async {
                let harness = WalTestHarness::new().unwrap();
                let uuid = Uuid::new_v4();

                // Write valid entry
                {
                    let writer = harness.open_writer().unwrap();
                    writer
                        .append_sync(WalEntry::PutIntent {
                            bucket: "bucket".to_string(),
                            key: "key".to_string(),
                            uuid,
                            size: 500,
                            crc32c: 0xDEADBEEF,
                            timestamp: 12345,
                        })
                        .await
                        .unwrap();
                }

                let file_size = std::fs::metadata(harness.wal_path()).unwrap().len();

                // Only corrupt if offset is within file
                if bit_offset < file_size {
                    let _ = CorruptionInjector::flip_bit(&harness.wal_path(), bit_offset, bit_position);
                }

                // Reading should not panic
                let read_result = WalReader::read(&harness.wal_path());

                // Recovery should not panic
                let recovery = harness.recovery_manager();
                let stats_result = recovery.recover().await;

                (read_result.is_ok(), stats_result.is_ok())
            });

            prop_assert!(read_ok);
            prop_assert!(recovery_ok);
        }

        /// Test that various truncation points are handled gracefully.
        #[test]
        fn fuzz_truncation_recovery(truncate_point in 8u64..500) {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let (read_ok, recovery_ok) = rt.block_on(async {
                let harness = WalTestHarness::new().unwrap();

                // Write multiple entries
                {
                    let writer = harness.open_writer().unwrap();
                    for i in 0..10 {
                        writer
                            .append_sync(WalEntry::PutIntent {
                                bucket: "bucket".to_string(),
                                key: format!("key{i}"),
                                uuid: Uuid::new_v4(),
                                size: 100,
                                crc32c: i as u32,
                                timestamp: i as i64,
                            })
                            .await
                            .unwrap();
                    }
                }

                let file_size = std::fs::metadata(harness.wal_path()).unwrap().len();

                // Truncate if within bounds
                if truncate_point < file_size {
                    let _ = CorruptionInjector::truncate(&harness.wal_path(), truncate_point);
                }

                // Reading should not panic
                let read_result = WalReader::read(&harness.wal_path());

                // Recovery should not panic
                let recovery = harness.recovery_manager();
                let stats_result = recovery.recover().await;

                (read_result.is_ok(), stats_result.is_ok())
            });

            prop_assert!(read_ok);
            prop_assert!(recovery_ok);
        }
    }
}
