//! Fsync guarantee tests for durability verification.
//!
//! These tests verify that sync strategies behave correctly:
//! - `SyncStrategy::None` doesn't call fsync (performance mode)
//! - `SyncStrategy::Always` calls fsync after each write (maximum durability)
//! - WAL respects configured sync modes
//!
//! Note: These tests use the `test_stats` infrastructure from the sync module
//! to count sync operations. Tests that use global counters are marked with
//! `#[serial]` to prevent race conditions.

use std::time::Duration;

use rucket_core::{SyncConfig, SyncStrategy};
use serial_test::serial;
use tempfile::TempDir;

use crate::sync::{test_stats, write_and_hash_with_strategy, SyncManager};
use crate::wal::{WalEntry, WalSyncMode, WalWriter, WalWriterConfig};

/// Test that SyncStrategy::None doesn't trigger fsync.
#[tokio::test]
#[serial]
async fn test_sync_strategy_none_no_fsync() {
    test_stats::reset();
    let temp_dir = TempDir::new().unwrap();

    let initial_syncs = test_stats::data_sync_count();

    // Write multiple files with None strategy
    for i in 0..5 {
        let path = temp_dir.path().join(format!("test{i}.dat"));
        write_and_hash_with_strategy(&path, b"test data", SyncStrategy::None).await.unwrap();
    }

    // No additional syncs should have occurred
    let final_syncs = test_stats::data_sync_count();
    assert_eq!(
        final_syncs,
        initial_syncs,
        "SyncStrategy::None should not trigger fsync (got {} syncs)",
        final_syncs - initial_syncs
    );
}

/// Test that SyncStrategy::Always calls fsync for each write.
#[tokio::test]
#[serial]
async fn test_sync_strategy_always_fsyncs() {
    test_stats::reset();
    let temp_dir = TempDir::new().unwrap();

    let initial_syncs = test_stats::data_sync_count();

    // Write 3 files with Always strategy
    for i in 0..3 {
        let path = temp_dir.path().join(format!("test{i}.dat"));
        write_and_hash_with_strategy(&path, b"synced data", SyncStrategy::Always).await.unwrap();
    }

    let final_syncs = test_stats::data_sync_count();
    assert!(
        final_syncs >= initial_syncs + 3,
        "SyncStrategy::Always should sync at least once per write (got {} syncs for 3 writes)",
        final_syncs - initial_syncs
    );
}

/// Test that SyncManager correctly tracks bytes and ops.
#[tokio::test]
async fn test_sync_manager_tracking() {
    let config = SyncConfig {
        data: SyncStrategy::Threshold,
        bytes_threshold: 10000,
        ops_threshold: 100,
        ..Default::default()
    };
    let manager = SyncManager::new(config);

    // Write some data
    manager.record_write(500);
    manager.record_write(300);

    assert_eq!(manager.get_bytes_written(), 800);
    assert_eq!(manager.get_ops_count(), 2);

    // Reset
    manager.reset_counters();
    assert_eq!(manager.get_bytes_written(), 0);
    assert_eq!(manager.get_ops_count(), 0);

    manager.shutdown().await;
}

/// Test that threshold-based sync triggers at the right point.
#[tokio::test]
async fn test_threshold_sync_trigger() {
    let config = SyncConfig {
        data: SyncStrategy::Threshold,
        bytes_threshold: 1000,
        ops_threshold: 10,
        ..Default::default()
    };
    let manager = SyncManager::new(config);

    // Under threshold - should not trigger
    assert!(!manager.record_write(500));
    assert!(!manager.record_write(400));

    // Over threshold - should trigger
    assert!(manager.record_write(200), "Should trigger sync when threshold exceeded");

    manager.shutdown().await;
}

/// Test that ops threshold triggers sync.
#[tokio::test]
async fn test_ops_threshold_trigger() {
    let config = SyncConfig {
        data: SyncStrategy::Threshold,
        bytes_threshold: 1_000_000, // High so it doesn't trigger
        ops_threshold: 5,
        ..Default::default()
    };
    let manager = SyncManager::new(config);

    for i in 0..4 {
        assert!(!manager.record_write(10), "Op {} should not trigger", i);
    }

    // 5th write should trigger
    assert!(manager.record_write(10), "5th op should trigger sync");

    manager.shutdown().await;
}

/// Test WAL with different sync modes.
#[tokio::test]
async fn test_wal_sync_modes() {
    let temp_dir = TempDir::new().unwrap();

    // Test with None mode
    {
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().join("wal_none"),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let writer = WalWriter::open(&config).unwrap();
        writer
            .append(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                uuid: uuid::Uuid::new_v4(),
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();

        // Should succeed without explicit sync
        assert!(config.wal_dir.join("current.wal").exists());
    }

    // Test with Fdatasync mode
    {
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().join("wal_fdatasync"),
            sync_mode: WalSyncMode::Fdatasync,
            ..Default::default()
        };

        let writer = WalWriter::open(&config).unwrap();
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                uuid: uuid::Uuid::new_v4(),
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();

        assert!(config.wal_dir.join("current.wal").exists());
    }

    // Test with Fsync mode
    {
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().join("wal_fsync"),
            sync_mode: WalSyncMode::Fsync,
            ..Default::default()
        };

        let writer = WalWriter::open(&config).unwrap();
        writer
            .append_sync(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                uuid: uuid::Uuid::new_v4(),
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();

        assert!(config.wal_dir.join("current.wal").exists());
    }
}

/// Test that periodic sync background task works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn test_periodic_sync_background() {
    test_stats::reset();
    let temp_dir = TempDir::new().unwrap();

    let config = SyncConfig {
        data: SyncStrategy::Periodic,
        interval_ms: 50, // Short for testing
        ..Default::default()
    };
    let manager = SyncManager::new(config);

    // Add pending files
    for i in 0..3 {
        let path = temp_dir.path().join(format!("test{i}.dat"));
        tokio::fs::write(&path, b"data").await.unwrap();
        manager.add_pending_file(path).await;
        manager.record_write(100);
    }

    // Should have pending files
    assert_eq!(manager.pending_count().await, 3);

    // Wait for background sync
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Pending should be cleared
    assert_eq!(manager.pending_count().await, 0, "Periodic sync should clear pending files");

    manager.shutdown().await;
}

/// Test that idle timeout triggers sync in threshold mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_threshold_idle_timeout() {
    let temp_dir = TempDir::new().unwrap();

    let config = SyncConfig {
        data: SyncStrategy::Threshold,
        max_idle_ms: 100,
        bytes_threshold: 1_000_000, // High so only idle triggers
        ops_threshold: 1000,
        ..Default::default()
    };
    let manager = SyncManager::new(config);

    // Give background task time to start
    tokio::time::sleep(Duration::from_millis(20)).await;

    // Add pending file
    let path = temp_dir.path().join("idle.dat");
    tokio::fs::write(&path, b"data").await.unwrap();
    manager.add_pending_file(path).await;
    manager.record_write(100);

    assert_eq!(manager.pending_count().await, 1);

    // Wait for idle timeout (max_idle_ms + check_interval + buffer)
    tokio::time::sleep(Duration::from_millis(400)).await;

    // Should have synced due to idle
    assert_eq!(manager.pending_count().await, 0, "Idle timeout should trigger sync");

    manager.shutdown().await;
}

/// Test that shutdown syncs all pending files.
#[tokio::test]
async fn test_shutdown_syncs_pending() {
    let temp_dir = TempDir::new().unwrap();

    let config = SyncConfig {
        data: SyncStrategy::Periodic,
        interval_ms: 10000, // Long interval so it doesn't auto-sync
        ..Default::default()
    };
    let manager = SyncManager::new(config);

    // Add pending files
    for i in 0..3 {
        let path = temp_dir.path().join(format!("test{i}.dat"));
        tokio::fs::write(&path, b"data").await.unwrap();
        manager.add_pending_file(path).await;
    }

    assert_eq!(manager.pending_count().await, 3);

    // Shutdown should sync
    manager.shutdown().await;

    assert_eq!(manager.pending_count().await, 0, "Shutdown should sync all pending files");
}

/// Test SyncManager convenience constructors.
#[tokio::test]
async fn test_sync_manager_constructors() {
    // Never
    let never = SyncManager::never();
    assert_eq!(never.config().data, SyncStrategy::None);
    assert!(!never.record_write(1000));

    // Periodic
    let periodic = SyncManager::periodic();
    assert_eq!(periodic.config().data, SyncStrategy::Periodic);
    periodic.shutdown().await;

    // Always
    let always = SyncManager::always();
    assert_eq!(always.config().data, SyncStrategy::Always);
    assert!(always.record_write(1));
}
