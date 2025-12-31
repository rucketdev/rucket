//! Crash recovery using WAL.
//!
//! The recovery manager runs on startup to identify and rollback
//! incomplete operations from the WAL.

use std::path::PathBuf;

use uuid::Uuid;

use super::entry::IncompleteOperation;
use super::reader::WalReader;

/// Statistics from recovery process.
#[derive(Debug, Default, Clone)]
pub struct RecoveryStats {
    /// Number of WAL entries processed.
    pub entries_processed: usize,
    /// Number of incomplete PUT operations rolled back.
    pub puts_rolled_back: usize,
    /// Number of incomplete DELETE operations rolled back (file kept).
    pub deletes_rolled_back: usize,
    /// Number of orphaned files cleaned up.
    pub orphans_cleaned: usize,
    /// Number of errors encountered (non-fatal).
    pub errors: usize,
}

/// Recovery manager for crash recovery.
///
/// Runs on startup to:
/// 1. Read WAL entries since last checkpoint
/// 2. Find incomplete operations
/// 3. Roll back incomplete PUTs (delete orphaned files)
/// 4. Roll back incomplete DELETEs (keep files)
/// 5. Optionally scan for orphaned files
pub struct RecoveryManager {
    /// Directory containing WAL files.
    wal_dir: PathBuf,
    /// Directory containing data files.
    data_dir: PathBuf,
}

impl RecoveryManager {
    /// Create a new recovery manager.
    #[must_use]
    pub fn new(wal_dir: PathBuf, data_dir: PathBuf) -> Self {
        Self { wal_dir, data_dir }
    }

    /// Run recovery and return statistics.
    ///
    /// This should be called on startup before normal operations begin.
    pub async fn recover(&self) -> std::io::Result<RecoveryStats> {
        let mut stats = RecoveryStats::default();

        // Read WAL
        let reader = match WalReader::read_all(&self.wal_dir) {
            Ok(r) => r,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                // No WAL exists yet, nothing to recover
                tracing::debug!("No WAL found, skipping recovery");
                return Ok(stats);
            }
            Err(e) => return Err(e),
        };

        stats.entries_processed = reader.entries().len();

        if reader.is_clean() {
            tracing::debug!(entries = stats.entries_processed, "WAL is clean, no recovery needed");
            return Ok(stats);
        }

        // Find incomplete operations
        let incomplete = reader.find_incomplete();
        tracing::info!(count = incomplete.len(), "Found incomplete operations to recover");

        // Process each incomplete operation
        for op in incomplete {
            match op {
                IncompleteOperation::IncompletePut { bucket, key, uuid, sequence } => {
                    match self.rollback_put(&bucket, uuid).await {
                        Ok(true) => {
                            tracing::info!(%bucket, %key, %uuid, sequence, "Rolled back incomplete PUT");
                            stats.puts_rolled_back += 1;
                        }
                        Ok(false) => {
                            // File didn't exist, nothing to do
                            tracing::debug!(%bucket, %key, %uuid, sequence, "Incomplete PUT file already gone");
                        }
                        Err(e) => {
                            tracing::warn!(%bucket, %key, %uuid, error = %e, "Error rolling back PUT");
                            stats.errors += 1;
                        }
                    }
                }
                IncompleteOperation::IncompleteDelete { bucket, key, uuid, sequence } => {
                    // For incomplete deletes, we keep the file (rollback = don't delete)
                    // The file should still exist since the delete didn't complete
                    tracing::info!(%bucket, %key, %uuid, sequence, "Rolled back incomplete DELETE (file kept)");
                    stats.deletes_rolled_back += 1;
                }
            }
        }

        tracing::info!(?stats, "Recovery complete");
        Ok(stats)
    }

    /// Roll back an incomplete PUT by deleting the orphaned file.
    ///
    /// Returns Ok(true) if a file was deleted, Ok(false) if no file existed.
    async fn rollback_put(&self, bucket: &str, uuid: Uuid) -> std::io::Result<bool> {
        let file_path = self.object_path(bucket, uuid);

        if file_path.exists() {
            tokio::fs::remove_file(&file_path).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get the path to an object file.
    fn object_path(&self, bucket: &str, uuid: Uuid) -> PathBuf {
        self.data_dir.join(bucket).join(format!("{uuid}.dat"))
    }

    /// Scan for orphaned files that have no metadata.
    ///
    /// This is a slower operation that scans all data files and checks
    /// if corresponding metadata exists. Use `check_uuid_exists` to verify.
    ///
    /// Returns the list of orphaned file paths.
    pub async fn scan_orphans<F>(&self, check_uuid_exists: F) -> std::io::Result<Vec<PathBuf>>
    where
        F: Fn(&str, Uuid) -> bool,
    {
        let mut orphans = Vec::new();

        // Iterate through bucket directories
        let mut bucket_entries = match tokio::fs::read_dir(&self.data_dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(orphans),
            Err(e) => return Err(e),
        };

        while let Some(bucket_entry) = bucket_entries.next_entry().await? {
            let bucket_path = bucket_entry.path();
            if !bucket_path.is_dir() {
                continue;
            }

            let bucket_name = match bucket_path.file_name().and_then(|n| n.to_str()) {
                Some(n) => n.to_string(),
                None => continue,
            };

            // Skip special directories
            if bucket_name.starts_with('.') {
                continue;
            }

            // Iterate through files in bucket
            let mut file_entries = tokio::fs::read_dir(&bucket_path).await?;
            while let Some(file_entry) = file_entries.next_entry().await? {
                let file_path = file_entry.path();

                // Only check .dat files
                match file_path.extension() {
                    Some(ext) if ext == "dat" => {}
                    _ => continue,
                }

                // Extract UUID from filename
                let uuid = match file_path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| Uuid::parse_str(s).ok())
                {
                    Some(u) => u,
                    None => continue,
                };

                // Check if metadata exists
                if !check_uuid_exists(&bucket_name, uuid) {
                    orphans.push(file_path);
                }
            }
        }

        Ok(orphans)
    }

    /// Clean orphaned files.
    ///
    /// Returns the number of files deleted.
    pub async fn clean_orphans(&self, orphans: &[PathBuf]) -> std::io::Result<usize> {
        let mut cleaned = 0;

        for path in orphans {
            match tokio::fs::remove_file(path).await {
                Ok(()) => {
                    tracing::info!(path = %path.display(), "Deleted orphaned file");
                    cleaned += 1;
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already deleted
                }
                Err(e) => {
                    tracing::warn!(path = %path.display(), error = %e, "Failed to delete orphan");
                }
            }
        }

        Ok(cleaned)
    }

    /// Delete old WAL files after successful recovery.
    ///
    /// Keeps only the current.wal file.
    pub async fn cleanup_old_wals(&self) -> std::io::Result<usize> {
        let mut cleaned = 0;

        let mut entries = match tokio::fs::read_dir(&self.wal_dir).await {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        };

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            // Keep current.wal, delete *.old files
            if path.extension().is_some_and(|e| e == "old") {
                match tokio::fs::remove_file(&path).await {
                    Ok(()) => {
                        tracing::debug!(path = %path.display(), "Deleted old WAL file");
                        cleaned += 1;
                    }
                    Err(e) => {
                        tracing::warn!(path = %path.display(), error = %e, "Failed to delete old WAL");
                    }
                }
            }
        }

        Ok(cleaned)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::super::writer::{WalSyncMode, WalWriter, WalWriterConfig};
    use super::*;
    use crate::wal::entry::WalEntry;

    #[tokio::test]
    async fn test_recovery_no_wal() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");

        let manager = RecoveryManager::new(wal_dir, data_dir);
        let stats = manager.recover().await.unwrap();

        assert_eq!(stats.entries_processed, 0);
        assert_eq!(stats.puts_rolled_back, 0);
    }

    #[tokio::test]
    async fn test_recovery_clean_wal() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");

        let config = WalWriterConfig {
            wal_dir: wal_dir.clone(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let uuid = Uuid::new_v4();

        // Write complete operation
        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "key".to_string(),
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
                    key: "key".to_string(),
                    uuid,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        let manager = RecoveryManager::new(wal_dir, data_dir);
        let stats = manager.recover().await.unwrap();

        assert_eq!(stats.entries_processed, 2);
        assert_eq!(stats.puts_rolled_back, 0);
    }

    #[tokio::test]
    async fn test_recovery_incomplete_put() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");

        // Create orphaned file
        let bucket_dir = data_dir.join("bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let uuid = Uuid::new_v4();
        let orphan_path = bucket_dir.join(format!("{uuid}.dat"));
        let mut file = std::fs::File::create(&orphan_path).unwrap();
        file.write_all(b"orphaned data").unwrap();

        // Write incomplete PUT
        let config = WalWriterConfig {
            wal_dir: wal_dir.clone(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

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

        assert!(orphan_path.exists());

        // Run recovery
        let manager = RecoveryManager::new(wal_dir, data_dir);
        let stats = manager.recover().await.unwrap();

        assert_eq!(stats.puts_rolled_back, 1);
        assert!(!orphan_path.exists());
    }

    #[tokio::test]
    async fn test_recovery_incomplete_delete() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");

        // Create file that was about to be deleted
        let bucket_dir = data_dir.join("bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        let uuid = Uuid::new_v4();
        let file_path = bucket_dir.join(format!("{uuid}.dat"));
        let mut file = std::fs::File::create(&file_path).unwrap();
        file.write_all(b"data to keep").unwrap();

        // Write incomplete DELETE
        let config = WalWriterConfig {
            wal_dir: wal_dir.clone(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::DeleteIntent {
                    bucket: "bucket".to_string(),
                    key: "keep-me".to_string(),
                    old_uuid: uuid,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        // Run recovery
        let manager = RecoveryManager::new(wal_dir, data_dir);
        let stats = manager.recover().await.unwrap();

        assert_eq!(stats.deletes_rolled_back, 1);
        // File should still exist (delete was rolled back)
        assert!(file_path.exists());
    }

    #[tokio::test]
    async fn test_scan_orphans() {
        let temp_dir = TempDir::new().unwrap();
        let wal_dir = temp_dir.path().join("wal");
        let data_dir = temp_dir.path().join("data");

        // Create bucket directory
        let bucket_dir = data_dir.join("test-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();

        // Create some files
        let uuid1 = Uuid::new_v4(); // Will be "known"
        let uuid2 = Uuid::new_v4(); // Will be "orphan"

        std::fs::write(bucket_dir.join(format!("{uuid1}.dat")), b"known").unwrap();
        std::fs::write(bucket_dir.join(format!("{uuid2}.dat")), b"orphan").unwrap();

        let manager = RecoveryManager::new(wal_dir, data_dir);

        // Mock metadata check - only uuid1 exists
        let orphans = manager.scan_orphans(|_bucket, uuid| uuid == uuid1).await.unwrap();

        assert_eq!(orphans.len(), 1);
        assert!(orphans[0].to_string_lossy().contains(&uuid2.to_string()));
    }
}
