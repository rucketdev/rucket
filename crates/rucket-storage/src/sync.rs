//! Sync management for controlling durability vs performance.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use rucket_core::{SyncConfig, SyncStrategy};
use tokio::fs::File;
use tokio::sync::Notify;

/// Test-only sync statistics for verifying durability behavior.
///
/// These counters track how many times sync operations are called,
/// allowing tests to verify that each sync strategy behaves correctly.
#[cfg(test)]
pub mod test_stats {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Count of data file sync operations (sync_data/sync_all on files).
    pub static DATA_SYNCS: AtomicU64 = AtomicU64::new(0);

    /// Count of directory sync operations.
    pub static DIR_SYNCS: AtomicU64 = AtomicU64::new(0);

    /// Reset all counters to zero.
    pub fn reset() {
        DATA_SYNCS.store(0, Ordering::SeqCst);
        DIR_SYNCS.store(0, Ordering::SeqCst);
    }

    /// Record that a data sync occurred.
    pub fn record_data_sync() {
        DATA_SYNCS.fetch_add(1, Ordering::SeqCst);
    }

    /// Record that a directory sync occurred.
    pub fn record_dir_sync() {
        DIR_SYNCS.fetch_add(1, Ordering::SeqCst);
    }

    /// Get the current data sync count.
    pub fn data_sync_count() -> u64 {
        DATA_SYNCS.load(Ordering::SeqCst)
    }

    /// Get the current directory sync count.
    pub fn dir_sync_count() -> u64 {
        DIR_SYNCS.load(Ordering::SeqCst)
    }
}

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
                    #[cfg(test)]
                    test_stats::record_data_sync();
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

    // =========================================================================
    // Test helper methods
    // =========================================================================

    /// Get count of pending files (test only).
    #[cfg(test)]
    pub async fn pending_count(&self) -> usize {
        self.pending_files.lock().await.len()
    }

    /// Get bytes written since last reset (test only).
    #[cfg(test)]
    pub fn get_bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::SeqCst)
    }

    /// Get ops count since last reset (test only).
    #[cfg(test)]
    pub fn get_ops_count(&self) -> u32 {
        self.ops_count.load(Ordering::SeqCst)
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

    // =========================================================================
    // Background Task Tests
    // =========================================================================

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_periodic_sync_runs_at_interval() {
        let temp_dir = TempDir::new().unwrap();

        let config = SyncConfig {
            data: SyncStrategy::Periodic,
            interval_ms: 50, // Short interval for testing
            ..Default::default()
        };
        let manager = SyncManager::new(config);

        // Add a pending file
        let path = temp_dir.path().join("test.dat");
        tokio::fs::write(&path, b"data").await.unwrap();
        manager.add_pending_file(path).await;
        manager.record_write(100);

        // Verify file is pending
        assert_eq!(manager.pending_count().await, 1);

        // Wait for 2+ intervals (50ms * 3 = 150ms with buffer)
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Should have been synced and cleared
        assert_eq!(manager.pending_count().await, 0, "Periodic sync should clear pending files");

        manager.shutdown().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_threshold_idle_triggers_sync() {
        let temp_dir = TempDir::new().unwrap();

        let config = SyncConfig {
            data: SyncStrategy::Threshold,
            max_idle_ms: 100,           // Longer idle for reliability
            bytes_threshold: 1_000_000, // High threshold so idle triggers first
            ops_threshold: 1000,        // High threshold so idle triggers first
            ..Default::default()
        };
        let manager = SyncManager::new(config);

        // Give the background task time to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Add a pending file
        let path = temp_dir.path().join("idle.dat");
        tokio::fs::write(&path, b"data").await.unwrap();
        manager.add_pending_file(path).await;
        manager.record_write(100);

        // Verify file is pending
        assert_eq!(manager.pending_count().await, 1);

        // Wait for idle timeout + check interval + buffer
        // check_interval = max_idle_ms / 2 = 50ms
        // Need to wait: max_idle_ms (100ms) + check_interval (50ms) + buffer (100ms)
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Should have been synced due to idle timeout
        let pending = manager.pending_count().await;
        assert_eq!(
            pending, 0,
            "Idle timeout should trigger sync, but {} files still pending",
            pending
        );

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn test_threshold_ops_threshold_triggers_sync() {
        let config = SyncConfig {
            data: SyncStrategy::Threshold,
            ops_threshold: 5,
            bytes_threshold: 1_000_000, // High so ops triggers first
            ..Default::default()
        };
        let manager = SyncManager::new(config);

        // 4 writes under threshold
        for _ in 0..4 {
            assert!(!manager.record_write(10));
        }

        // 5th write should trigger sync
        assert!(manager.record_write(10), "5th op should trigger sync");

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn test_threshold_bytes_threshold_triggers_sync() {
        let config = SyncConfig {
            data: SyncStrategy::Threshold,
            bytes_threshold: 1000,
            ops_threshold: 100, // High so bytes triggers first
            ..Default::default()
        };
        let manager = SyncManager::new(config);

        // Under threshold
        assert!(!manager.record_write(500));
        assert!(!manager.record_write(400));

        // This write exceeds threshold
        assert!(manager.record_write(200), "Exceeding bytes threshold should trigger sync");

        manager.shutdown().await;
    }

    // =========================================================================
    // Sync Counter Tests
    // =========================================================================

    #[tokio::test]
    async fn test_sync_counter_always_mode() {
        test_stats::reset();
        let temp_dir = TempDir::new().unwrap();

        // Write with Always strategy
        let path = temp_dir.path().join("test1.dat");
        write_and_hash_with_strategy(&path, b"data", SyncStrategy::Always).await.unwrap();

        // Should have recorded a sync
        assert!(
            test_stats::data_sync_count() >= 1,
            "Always mode should sync: got {} syncs",
            test_stats::data_sync_count()
        );
    }

    /// Test that None sync strategy doesn't trigger data syncs.
    ///
    /// Note: This test is ignored in parallel mode because it relies on global test_stats
    /// counters which can be affected by background sync tasks from other tests.
    /// Run with `cargo test -- --ignored --test-threads=1` to verify this behavior.
    #[tokio::test]
    #[ignore = "Flaky in parallel: global test_stats affected by background tasks from other tests"]
    async fn test_sync_counter_none_mode() {
        test_stats::reset();
        let temp_dir = TempDir::new().unwrap();

        // Write with None strategy
        for i in 0..5 {
            let path = temp_dir.path().join(format!("test{i}.dat"));
            write_and_hash_with_strategy(&path, b"data", SyncStrategy::None).await.unwrap();
        }

        // None mode should not trigger any syncs
        let count = test_stats::data_sync_count();
        assert_eq!(count, 0, "None mode should not call sync in streaming (count={count})");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_sync_counter_periodic_background_sync() {
        test_stats::reset();
        let temp_dir = TempDir::new().unwrap();

        let config =
            SyncConfig { data: SyncStrategy::Periodic, interval_ms: 50, ..Default::default() };
        let manager = SyncManager::new(config);

        // Add pending files
        for i in 0..3 {
            let path = temp_dir.path().join(format!("test{i}.dat"));
            tokio::fs::write(&path, b"data").await.unwrap();
            manager.add_pending_file(path).await;
            manager.record_write(10);
        }

        // Wait for background sync (2+ intervals)
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Background sync should have synced the files
        assert!(
            test_stats::data_sync_count() >= 3,
            "Periodic background sync should sync pending files: got {} syncs",
            test_stats::data_sync_count()
        );

        manager.shutdown().await;
    }

    // =========================================================================
    // Coverage Tests for Convenience Methods and Getters
    // =========================================================================

    #[tokio::test]
    async fn test_sync_manager_convenience_constructors() {
        // Test never() constructor
        let never_manager = SyncManager::never();
        assert_eq!(never_manager.config().data, SyncStrategy::None);
        assert!(!never_manager.record_write(1024));

        // Test periodic() constructor
        let periodic_manager = SyncManager::periodic();
        assert_eq!(periodic_manager.config().data, SyncStrategy::Periodic);
        periodic_manager.shutdown().await;

        // Test always() constructor
        let always_manager = SyncManager::always();
        assert_eq!(always_manager.config().data, SyncStrategy::Always);
        assert!(always_manager.record_write(1024));
    }

    #[tokio::test]
    async fn test_sync_manager_config_getter() {
        let config = SyncConfig {
            data: SyncStrategy::Threshold,
            bytes_threshold: 5000,
            ops_threshold: 50,
            max_idle_ms: 1000,
            ..Default::default()
        };
        let manager = SyncManager::new(config.clone());

        // Verify config getter returns correct values
        assert_eq!(manager.config().data, SyncStrategy::Threshold);
        assert_eq!(manager.config().bytes_threshold, 5000);
        assert_eq!(manager.config().ops_threshold, 50);
        assert_eq!(manager.config().max_idle_ms, 1000);

        manager.shutdown().await;
    }

    #[tokio::test]
    async fn test_sync_manager_get_counters() {
        let config = SyncConfig { data: SyncStrategy::None, ..Default::default() };
        let manager = SyncManager::new(config);

        // Initial state
        assert_eq!(manager.get_bytes_written(), 0);
        assert_eq!(manager.get_ops_count(), 0);

        // After writes
        manager.record_write(100);
        manager.record_write(200);
        assert_eq!(manager.get_bytes_written(), 300);
        assert_eq!(manager.get_ops_count(), 2);

        // After reset
        manager.reset_counters();
        assert_eq!(manager.get_bytes_written(), 0);
        assert_eq!(manager.get_ops_count(), 0);
    }

    #[tokio::test]
    async fn test_dir_sync_counter() {
        test_stats::reset();

        // dir_sync_count should start at 0
        assert_eq!(test_stats::dir_sync_count(), 0);

        // Manually record a dir sync to cover the function
        test_stats::record_dir_sync();
        assert_eq!(test_stats::dir_sync_count(), 1);

        test_stats::record_dir_sync();
        assert_eq!(test_stats::dir_sync_count(), 2);

        // Reset should clear it
        test_stats::reset();
        assert_eq!(test_stats::dir_sync_count(), 0);
    }
}
