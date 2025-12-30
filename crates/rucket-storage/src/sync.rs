// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Sync management for controlling durability vs performance.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use rucket_core::{SyncConfig, SyncStrategy};
use tokio::fs::File;
use tokio::sync::Notify;

/// Manages sync operations based on configuration.
///
/// Tracks write operations and bytes written, triggering syncs
/// according to the configured strategy.
#[derive(Debug)]
pub struct SyncManager {
    config: SyncConfig,
    bytes_written: AtomicU64,
    ops_count: AtomicU32,
    pending_files: tokio::sync::Mutex<Vec<std::path::PathBuf>>,
    notify: Arc<Notify>,
    shutdown: Arc<Notify>,
}

impl SyncManager {
    /// Create a new sync manager with the given configuration.
    #[must_use]
    pub fn new(config: SyncConfig) -> Arc<Self> {
        let manager = Arc::new(Self {
            config,
            bytes_written: AtomicU64::new(0),
            ops_count: AtomicU32::new(0),
            pending_files: tokio::sync::Mutex::new(Vec::new()),
            notify: Arc::new(Notify::new()),
            shutdown: Arc::new(Notify::new()),
        });

        // Start background sync task for periodic mode
        if manager.config.data == SyncStrategy::Periodic {
            let manager_clone = Arc::clone(&manager);
            tokio::spawn(async move {
                manager_clone.periodic_sync_task().await;
            });
        }

        manager
    }

    /// Create a sync manager that never explicitly syncs (maximum performance).
    #[must_use]
    pub fn never() -> Arc<Self> {
        Self::new(SyncConfig::never())
    }

    /// Create a sync manager with periodic sync.
    #[must_use]
    pub fn periodic() -> Arc<Self> {
        Self::new(SyncConfig::periodic())
    }

    /// Create a sync manager that always syncs (maximum durability).
    #[must_use]
    pub fn always() -> Arc<Self> {
        Self::new(SyncConfig::always())
    }

    /// Get the current sync configuration.
    #[must_use]
    pub fn config(&self) -> &SyncConfig {
        &self.config
    }

    /// Record that bytes were written. Returns true if sync should happen now.
    pub fn record_write(&self, bytes: u64) -> bool {
        let total_bytes = self.bytes_written.fetch_add(bytes, Ordering::SeqCst) + bytes;
        let ops = self.ops_count.fetch_add(1, Ordering::SeqCst) + 1;

        match self.config.data {
            SyncStrategy::Always => true,
            SyncStrategy::None => false,
            SyncStrategy::Periodic => {
                // Notify the background task
                self.notify.notify_one();
                false
            }
            SyncStrategy::Threshold => {
                total_bytes >= self.config.bytes_threshold
                    || ops >= self.config.ops_threshold
            }
        }
    }

    /// Reset the write counters after a sync.
    pub fn reset_counters(&self) {
        self.bytes_written.store(0, Ordering::SeqCst);
        self.ops_count.store(0, Ordering::SeqCst);
    }

    /// Add a file path to the list of files pending sync.
    pub async fn add_pending_file(&self, path: std::path::PathBuf) {
        if self.config.data == SyncStrategy::Periodic
            || self.config.data == SyncStrategy::Threshold
        {
            let mut pending = self.pending_files.lock().await;
            pending.push(path);
        }
    }

    /// Sync all pending files.
    pub async fn sync_pending(&self) -> std::io::Result<()> {
        let mut pending = self.pending_files.lock().await;

        for path in pending.drain(..) {
            if path.exists() {
                if let Ok(file) = File::open(&path).await {
                    let _ = file.sync_all().await;
                }
            }
        }

        self.reset_counters();
        Ok(())
    }

    /// Shutdown the sync manager, flushing any pending writes.
    pub async fn shutdown(&self) {
        self.shutdown.notify_one();
        // Sync any remaining files
        let _ = self.sync_pending().await;
    }

    /// Background task for periodic sync.
    async fn periodic_sync_task(&self) {
        let interval = Duration::from_millis(self.config.interval_ms);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(interval) => {
                    let _ = self.sync_pending().await;
                }
                _ = self.notify.notified() => {
                    // Just continue, will sync on next interval
                }
                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }
    }
}

// Re-export write function from streaming module
pub use crate::streaming::write_and_hash_with_sync as write_and_hash_with_strategy;

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_sync_manager_always() {
        let config = SyncConfig {
            data: SyncStrategy::Always,
            ..Default::default()
        };
        let manager = SyncManager::new(config);

        assert!(manager.record_write(1024));
        assert!(manager.record_write(1024));
    }

    #[tokio::test]
    async fn test_sync_manager_none() {
        let config = SyncConfig {
            data: SyncStrategy::None,
            ..Default::default()
        };
        let manager = SyncManager::new(config);

        assert!(!manager.record_write(1024));
        assert!(!manager.record_write(1024 * 1024 * 100));
    }

    #[tokio::test]
    async fn test_sync_manager_threshold() {
        let config = SyncConfig {
            data: SyncStrategy::Threshold,
            bytes_threshold: 1000,
            ops_threshold: 10,
            ..Default::default()
        };
        let manager = SyncManager::new(config);

        // Under threshold
        assert!(!manager.record_write(500));

        // Over threshold
        assert!(manager.record_write(600));

        // Reset and test ops threshold
        manager.reset_counters();
        for _ in 0..9 {
            assert!(!manager.record_write(1));
        }
        assert!(manager.record_write(1)); // 10th op triggers
    }

    #[tokio::test]
    async fn test_write_with_no_sync() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        let data = b"Hello, World!";
        let etag = write_and_hash_with_strategy(&path, data, SyncStrategy::None)
            .await
            .unwrap();

        // Verify file was written
        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(content, data);
        assert!(!etag.is_multipart());
    }

    #[tokio::test]
    async fn test_write_with_always_sync() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        let data = b"Hello, World!";
        let etag = write_and_hash_with_strategy(&path, data, SyncStrategy::Always)
            .await
            .unwrap();

        // Verify file was written
        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(content, data);
        assert!(!etag.is_multipart());
    }
}
