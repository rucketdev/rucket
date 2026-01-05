//! Replication lag monitoring.
//!
//! This module provides tracking and monitoring of replication lag
//! between the primary node and its replicas.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use dashmap::DashMap;
use metrics::{gauge, histogram};

/// Tracks replication lag for each replica node.
///
/// Lag is measured as the time difference between when an operation
/// was performed on the primary and when it was acknowledged by the replica.
pub struct LagTracker {
    /// Per-replica lag information.
    replicas: DashMap<String, ReplicaLagInfo>,
}

/// Lag information for a single replica.
struct ReplicaLagInfo {
    /// Last successful HLC timestamp replicated.
    last_hlc: AtomicU64,

    /// Wall-clock time of last successful replication.
    last_success: parking_lot::RwLock<Option<Instant>>,

    /// Last known success timestamp (for reporting).
    last_success_time: parking_lot::RwLock<Option<DateTime<Utc>>>,

    /// Consecutive failure count.
    failure_count: AtomicU64,

    /// Total successful replications.
    success_count: AtomicU64,
}

impl ReplicaLagInfo {
    fn new() -> Self {
        Self {
            last_hlc: AtomicU64::new(0),
            last_success: parking_lot::RwLock::new(None),
            last_success_time: parking_lot::RwLock::new(None),
            failure_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
        }
    }
}

impl LagTracker {
    /// Creates a new lag tracker.
    pub fn new() -> Self {
        Self { replicas: DashMap::new() }
    }

    /// Adds a replica to track.
    pub fn add_replica(&self, node_id: String) {
        self.replicas.insert(node_id, ReplicaLagInfo::new());
    }

    /// Removes a replica from tracking.
    pub fn remove_replica(&self, node_id: &str) {
        self.replicas.remove(node_id);
    }

    /// Records a successful replication to a replica.
    pub fn record_success(&self, node_id: &str, hlc_timestamp: u64) {
        if let Some(info) = self.replicas.get(node_id) {
            info.last_hlc.store(hlc_timestamp, Ordering::SeqCst);
            *info.last_success.write() = Some(Instant::now());
            *info.last_success_time.write() = Some(Utc::now());
            info.failure_count.store(0, Ordering::SeqCst);
            info.success_count.fetch_add(1, Ordering::SeqCst);

            // Update metrics
            gauge!("replication_last_hlc", "node_id" => node_id.to_string())
                .set(hlc_timestamp as f64);
            gauge!("replication_failure_count", "node_id" => node_id.to_string()).set(0.0);
        }
    }

    /// Records a failed replication attempt.
    pub fn record_failure(&self, node_id: &str) {
        if let Some(info) = self.replicas.get(node_id) {
            let failures = info.failure_count.fetch_add(1, Ordering::SeqCst) + 1;

            // Update metrics
            gauge!("replication_failure_count", "node_id" => node_id.to_string())
                .set(failures as f64);
        }
    }

    /// Gets the replication lag in milliseconds for a replica.
    ///
    /// Returns `None` if the replica hasn't had any successful replications yet.
    pub fn get_lag_ms(&self, node_id: &str) -> Option<u64> {
        self.replicas.get(node_id).and_then(|info| {
            info.last_success.read().map(|instant| instant.elapsed().as_millis() as u64)
        })
    }

    /// Gets the last HLC timestamp replicated to a replica.
    pub fn get_last_hlc(&self, node_id: &str) -> Option<u64> {
        self.replicas.get(node_id).map(|info| info.last_hlc.load(Ordering::SeqCst))
    }

    /// Gets the consecutive failure count for a replica.
    pub fn get_failure_count(&self, node_id: &str) -> Option<u64> {
        self.replicas.get(node_id).map(|info| info.failure_count.load(Ordering::SeqCst))
    }

    /// Gets the total successful replication count for a replica.
    pub fn get_success_count(&self, node_id: &str) -> Option<u64> {
        self.replicas.get(node_id).map(|info| info.success_count.load(Ordering::SeqCst))
    }

    /// Gets the last success timestamp for a replica.
    pub fn last_success_time(&self, node_id: &str) -> Option<DateTime<Utc>> {
        self.replicas.get(node_id).and_then(|info| *info.last_success_time.read())
    }

    /// Returns a summary of all replica lag information.
    pub fn get_all_lag_info(&self) -> Vec<LagInfo> {
        self.replicas
            .iter()
            .map(|entry| {
                let node_id = entry.key().clone();
                let info = entry.value();

                LagInfo {
                    node_id,
                    lag_ms: info.last_success.read().map(|i| i.elapsed().as_millis() as u64),
                    last_hlc: info.last_hlc.load(Ordering::SeqCst),
                    failure_count: info.failure_count.load(Ordering::SeqCst),
                    success_count: info.success_count.load(Ordering::SeqCst),
                    last_success_time: *info.last_success_time.read(),
                }
            })
            .collect()
    }

    /// Checks if any replica has lag exceeding the threshold.
    pub fn any_exceeds_threshold(&self, threshold_ms: u64) -> bool {
        self.replicas.iter().any(|entry| {
            entry
                .value()
                .last_success
                .read()
                .map(|i| i.elapsed().as_millis() as u64 > threshold_ms)
                .unwrap_or(true) // No success yet = lagging
        })
    }

    /// Returns the maximum lag across all replicas.
    pub fn max_lag_ms(&self) -> Option<u64> {
        self.replicas
            .iter()
            .filter_map(|entry| {
                entry.value().last_success.read().map(|i| i.elapsed().as_millis() as u64)
            })
            .max()
    }

    /// Returns the number of healthy replicas (lag below threshold).
    pub fn healthy_count(&self, threshold_ms: u64) -> usize {
        self.replicas
            .iter()
            .filter(|entry| {
                entry
                    .value()
                    .last_success
                    .read()
                    .map(|i| i.elapsed().as_millis() as u64 <= threshold_ms)
                    .unwrap_or(false)
            })
            .count()
    }

    /// Updates Prometheus metrics for all replicas.
    pub fn update_metrics(&self) {
        for entry in self.replicas.iter() {
            let node_id = entry.key();
            let info = entry.value();

            if let Some(lag) = info.last_success.read().map(|i| i.elapsed().as_millis() as u64) {
                gauge!("replication_lag_ms", "node_id" => node_id.clone()).set(lag as f64);
                histogram!("replication_lag_histogram_ms", "node_id" => node_id.clone())
                    .record(lag as f64);
            }

            gauge!("replication_success_count", "node_id" => node_id.clone())
                .set(info.success_count.load(Ordering::SeqCst) as f64);
        }
    }
}

impl Default for LagTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Summary of lag information for a single replica.
#[derive(Debug, Clone)]
pub struct LagInfo {
    /// Node ID.
    pub node_id: String,

    /// Current lag in milliseconds (None if no successful replications).
    pub lag_ms: Option<u64>,

    /// Last replicated HLC timestamp.
    pub last_hlc: u64,

    /// Consecutive failure count.
    pub failure_count: u64,

    /// Total successful replication count.
    pub success_count: u64,

    /// Last success wall-clock time.
    pub last_success_time: Option<DateTime<Utc>>,
}

impl LagInfo {
    /// Returns true if the replica is healthy (has recent successful replications).
    pub fn is_healthy(&self, threshold_ms: u64) -> bool {
        self.lag_ms.map(|lag| lag <= threshold_ms).unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use std::thread;
    use std::time::Duration;

    use super::*;

    #[test]
    fn test_lag_tracker_creation() {
        let tracker = LagTracker::new();
        assert!(tracker.get_all_lag_info().is_empty());
    }

    #[test]
    fn test_add_remove_replica() {
        let tracker = LagTracker::new();

        tracker.add_replica("node1".to_string());
        assert_eq!(tracker.get_all_lag_info().len(), 1);

        tracker.add_replica("node2".to_string());
        assert_eq!(tracker.get_all_lag_info().len(), 2);

        tracker.remove_replica("node1");
        assert_eq!(tracker.get_all_lag_info().len(), 1);
        assert_eq!(tracker.get_all_lag_info()[0].node_id, "node2");
    }

    #[test]
    fn test_record_success() {
        let tracker = LagTracker::new();
        tracker.add_replica("node1".to_string());

        assert!(tracker.get_lag_ms("node1").is_none());
        assert_eq!(tracker.get_last_hlc("node1"), Some(0));

        tracker.record_success("node1", 12345);

        assert!(tracker.get_lag_ms("node1").is_some());
        assert_eq!(tracker.get_last_hlc("node1"), Some(12345));
        assert_eq!(tracker.get_success_count("node1"), Some(1));
        assert_eq!(tracker.get_failure_count("node1"), Some(0));
    }

    #[test]
    fn test_record_failure() {
        let tracker = LagTracker::new();
        tracker.add_replica("node1".to_string());

        tracker.record_failure("node1");
        assert_eq!(tracker.get_failure_count("node1"), Some(1));

        tracker.record_failure("node1");
        assert_eq!(tracker.get_failure_count("node1"), Some(2));

        // Success resets failure count
        tracker.record_success("node1", 100);
        assert_eq!(tracker.get_failure_count("node1"), Some(0));
    }

    #[test]
    fn test_lag_calculation() {
        let tracker = LagTracker::new();
        tracker.add_replica("node1".to_string());

        tracker.record_success("node1", 100);

        // Wait a bit to accumulate lag
        thread::sleep(Duration::from_millis(50));

        let lag = tracker.get_lag_ms("node1").unwrap();
        assert!(lag >= 50);
        assert!(lag < 200); // Reasonable upper bound
    }

    #[test]
    fn test_threshold_check() {
        let tracker = LagTracker::new();
        tracker.add_replica("node1".to_string());
        tracker.add_replica("node2".to_string());

        // No successes yet - all are lagging
        assert!(tracker.any_exceeds_threshold(1000));
        assert_eq!(tracker.healthy_count(1000), 0);

        // Record success for node1
        tracker.record_success("node1", 100);

        // node1 should be healthy, node2 still lagging
        assert!(tracker.any_exceeds_threshold(1000));
        assert_eq!(tracker.healthy_count(1000), 1);

        // Record success for node2
        tracker.record_success("node2", 100);

        // Both should be healthy now
        assert!(!tracker.any_exceeds_threshold(1000));
        assert_eq!(tracker.healthy_count(1000), 2);
    }

    #[test]
    fn test_max_lag() {
        let tracker = LagTracker::new();
        tracker.add_replica("node1".to_string());
        tracker.add_replica("node2".to_string());

        // No successes - no max lag
        assert!(tracker.max_lag_ms().is_none());

        tracker.record_success("node1", 100);
        thread::sleep(Duration::from_millis(10));
        tracker.record_success("node2", 100);

        // node1 should have higher lag (older success)
        let max = tracker.max_lag_ms().unwrap();
        let node1_lag = tracker.get_lag_ms("node1").unwrap();
        assert_eq!(max, node1_lag);
    }

    #[test]
    fn test_lag_info_is_healthy() {
        let info = LagInfo {
            node_id: "node1".to_string(),
            lag_ms: Some(100),
            last_hlc: 12345,
            failure_count: 0,
            success_count: 10,
            last_success_time: Some(Utc::now()),
        };

        assert!(info.is_healthy(1000));
        assert!(info.is_healthy(100));
        assert!(!info.is_healthy(50));

        // No lag = unhealthy
        let info_no_lag = LagInfo { lag_ms: None, ..info.clone() };
        assert!(!info_no_lag.is_healthy(1000));
    }

    #[test]
    fn test_get_all_lag_info() {
        let tracker = LagTracker::new();
        tracker.add_replica("node1".to_string());
        tracker.add_replica("node2".to_string());

        tracker.record_success("node1", 100);
        tracker.record_success("node2", 200);
        tracker.record_failure("node2");

        let infos = tracker.get_all_lag_info();
        assert_eq!(infos.len(), 2);

        // Find node2 info
        let node2_info = infos.iter().find(|i| i.node_id == "node2").unwrap();
        assert_eq!(node2_info.last_hlc, 200);
        assert_eq!(node2_info.failure_count, 1);
        assert_eq!(node2_info.success_count, 1);
    }

    #[test]
    fn test_nonexistent_replica() {
        let tracker = LagTracker::new();

        assert!(tracker.get_lag_ms("nonexistent").is_none());
        assert!(tracker.get_last_hlc("nonexistent").is_none());
        assert!(tracker.get_failure_count("nonexistent").is_none());
        assert!(tracker.get_success_count("nonexistent").is_none());

        // Recording to nonexistent replica should be a no-op
        tracker.record_success("nonexistent", 100);
        tracker.record_failure("nonexistent");
    }
}
