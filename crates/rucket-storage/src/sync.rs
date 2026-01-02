//! Sync management for controlling durability vs performance.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    /// Timestamp of last write in milliseconds since manager creation.
    /// Used for idle detection in Threshold mode.
    last_write_time_ms: AtomicU64,
    /// Instant when manager was created, used as epoch for last_write_time_ms.
    start_time: Instant,
    pending_files: tokio::sync::Mutex<Vec<std::path::PathBuf>>,
    notify: Arc<Notify>,
    shutdown: Arc<Notify>,
}

impl SyncManager {
    /// Create a new sync manager with the given configuration.
    #[must_use]
    pub fn new(config: SyncConfig) -> Arc<Self> {
        let start_time = Instant::now();
        let manager = Arc::new(Self {
            config,
            bytes_written: AtomicU64::new(0),
            ops_count: AtomicU32::new(0),
            last_write_time_ms: AtomicU64::new(0),
            start_time,
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

        // Start idle timeout task for threshold mode
        if manager.config.data == SyncStrategy::Threshold {
            let manager_clone = Arc::clone(&manager);
            tokio::spawn(async move {
                manager_clone.threshold_idle_task().await;
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

    /// Check if we should sync immediately based on thresholds.
    /// Returns true if sync should happen now (Always mode, or threshold reached for Periodic/Threshold).
    /// Does NOT update counters - call `record_write()` after the sync decision.
    pub fn should_sync_now(&self, bytes: u64) -> bool {
        let current_bytes = self.bytes_written.load(Ordering::SeqCst);
        let current_ops = self.ops_count.load(Ordering::SeqCst);

        match self.config.data {
            SyncStrategy::Always => true,
            SyncStrategy::None => false,
            SyncStrategy::Periodic | SyncStrategy::Threshold => {
                (current_bytes + bytes) >= self.config.bytes_threshold
                    || (current_ops + 1) >= self.config.ops_threshold
            }
        }
    }

    /// Record that bytes were written and update counters.
    /// For Periodic mode, also notifies the background task.
    /// Returns true if sync should happen now (for backwards compatibility).
    pub fn record_write(&self, bytes: u64) -> bool {
        let total_bytes = self.bytes_written.fetch_add(bytes, Ordering::SeqCst) + bytes;
        let ops = self.ops_count.fetch_add(1, Ordering::SeqCst) + 1;

        // Update last write time for idle detection
        let now_ms = self.start_time.elapsed().as_millis() as u64;
        self.last_write_time_ms.store(now_ms, Ordering::SeqCst);

        match self.config.data {
            SyncStrategy::Always => true,
            SyncStrategy::None => false,
            SyncStrategy::Periodic => {
                // Check threshold
                if total_bytes >= self.config.bytes_threshold || ops >= self.config.ops_threshold {
                    true
                } else {
                    // Notify the background task for time-based sync
                    self.notify.notify_one();
                    false
                }
            }
            SyncStrategy::Threshold => {
                // Notify the idle task that a write occurred
                self.notify.notify_one();
                total_bytes >= self.config.bytes_threshold || ops >= self.config.ops_threshold
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
        if self.config.data == SyncStrategy::Periodic || self.config.data == SyncStrategy::Threshold
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

    /// Background task for threshold mode idle detection.
    /// Flushes dirty data if no writes occur within max_idle_ms.
    async fn threshold_idle_task(&self) {
        let check_interval = Duration::from_millis(self.config.max_idle_ms / 2);
        let max_idle = self.config.max_idle_ms;

        loop {
            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {
                    // Check if we've been idle long enough
                    let last_write = self.last_write_time_ms.load(Ordering::SeqCst);
                    let now_ms = self.start_time.elapsed().as_millis() as u64;
                    let pending_count = self.pending_files.lock().await.len();

                    // If we have pending files and have been idle for max_idle_ms, sync
                    if pending_count > 0 && last_write > 0 && (now_ms - last_write) >= max_idle {
                        tracing::debug!(
                            idle_ms = now_ms - last_write,
                            pending_files = pending_count,
                            "Threshold idle timeout reached, syncing"
                        );
                        let _ = self.sync_pending().await;
                    }
                }
                _ = self.notify.notified() => {
                    // Write occurred, continue checking
                }
                _ = self.shutdown.notified() => {
                    break;
                }
            }
        }
    }
}

// Re-export write function and result type from streaming module
pub use crate::streaming::{write_and_hash_with_sync as write_and_hash_with_strategy, WriteResult};

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_sync_manager_always() {
        let config = SyncConfig { data: SyncStrategy::Always, ..Default::default() };
        let manager = SyncManager::new(config);

        assert!(manager.record_write(1024));
        assert!(manager.record_write(1024));
    }

    #[tokio::test]
    async fn test_sync_manager_none() {
        let config = SyncConfig { data: SyncStrategy::None, ..Default::default() };
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
        let result = write_and_hash_with_strategy(&path, data, SyncStrategy::None).await.unwrap();

        // Verify file was written
        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(content, data);
        assert!(!result.etag.is_multipart());
        // Verify CRC32C was computed
        assert_ne!(result.crc32c, 0);
    }

    #[tokio::test]
    async fn test_write_with_always_sync() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        let data = b"Hello, World!";
        let result = write_and_hash_with_strategy(&path, data, SyncStrategy::Always).await.unwrap();

        // Verify file was written
        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(content, data);
        assert!(!result.etag.is_multipart());
        // Verify CRC32C was computed
        assert_ne!(result.crc32c, 0);
    }
}
