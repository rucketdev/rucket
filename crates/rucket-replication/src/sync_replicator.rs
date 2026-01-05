//! Synchronous replication with quorum acknowledgment.
//!
//! This module implements sync replication where writes wait for
//! a quorum of replicas to acknowledge before returning success.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use super::async_replicator::ReplicaClient;
use super::config::ReplicationConfig;
use super::error::{ReplicationError, Result};
use super::lag::LagTracker;
use super::level::ReplicationLevel;
use super::replicator::{ReplicaInfo, ReplicationEntry, ReplicationResult, Replicator};

/// Synchronous replicator that waits for quorum acknowledgment.
///
/// This replicator provides strong consistency by:
/// 1. Sending writes to all replicas in parallel
/// 2. Waiting for quorum (RF/2 + 1) acknowledgments
/// 3. Returning success only after quorum is achieved
pub struct SyncReplicator {
    config: ReplicationConfig,
    replicas: DashMap<String, Arc<dyn ReplicaClient>>,
    lag_tracker: Arc<LagTracker>,
}

impl SyncReplicator {
    /// Creates a new sync replicator.
    pub fn new(config: ReplicationConfig) -> Self {
        Self { config, replicas: DashMap::new(), lag_tracker: Arc::new(LagTracker::new()) }
    }

    /// Adds a replica to the replication set.
    pub fn add_replica(&self, client: Arc<dyn ReplicaClient>) {
        let node_id = client.node_id().to_string();
        self.replicas.insert(node_id.clone(), client);
        self.lag_tracker.add_replica(node_id.clone());
        info!(node_id = %node_id, "Added replica for sync replication");
    }

    /// Removes a replica from the replication set.
    pub fn remove_replica(&self, node_id: &str) {
        self.replicas.remove(node_id);
        self.lag_tracker.remove_replica(node_id);
        info!(node_id = %node_id, "Removed replica from sync replication");
    }

    /// Returns the lag tracker for monitoring.
    pub fn lag_tracker(&self) -> &LagTracker {
        &self.lag_tracker
    }

    /// Replicates to all replicas and waits for quorum.
    async fn replicate_with_quorum(&self, entry: &ReplicationEntry) -> ReplicationResult {
        let quorum_needed = self.config.quorum_size();
        let timeout_duration = Duration::from_millis(self.config.timeout_ms);

        // Primary counts as 1 ack
        let mut acks = 1usize;
        let mut errors = Vec::new();

        // Collect all replicas to replicate to
        let replicas: Vec<_> =
            self.replicas.iter().map(|r| (r.key().clone(), r.value().clone())).collect();

        if replicas.is_empty() {
            // No replicas - check if quorum can be achieved with just primary
            if quorum_needed <= 1 {
                return ReplicationResult::success(entry.id, 1, quorum_needed);
            } else {
                return ReplicationResult::quorum_failed(entry.id, 1, quorum_needed, vec![]);
            }
        }

        // Send to all replicas in parallel
        let mut handles = Vec::with_capacity(replicas.len());

        for (node_id, client) in replicas {
            let entry_clone = entry.clone();
            let node_id_clone = node_id.clone();
            let lag_tracker = self.lag_tracker.clone();

            handles.push(tokio::spawn(async move {
                let result = timeout(timeout_duration, client.send(&entry_clone)).await;

                match result {
                    Ok(Ok(())) => {
                        lag_tracker.record_success(&node_id_clone, entry_clone.hlc_timestamp);
                        Ok(node_id_clone)
                    }
                    Ok(Err(e)) => {
                        lag_tracker.record_failure(&node_id_clone);
                        Err((node_id_clone, e))
                    }
                    Err(_) => {
                        lag_tracker.record_failure(&node_id_clone);
                        Err((
                            node_id_clone.clone(),
                            ReplicationError::Timeout {
                                node_id: node_id_clone,
                                timeout_ms: timeout_duration.as_millis() as u64,
                            },
                        ))
                    }
                }
            }));
        }

        // Wait for all results
        for handle in handles {
            match handle.await {
                Ok(Ok(node_id)) => {
                    acks += 1;
                    debug!(node_id = %node_id, acks = acks, "Received replication ack");
                }
                Ok(Err((node_id, e))) => {
                    warn!(node_id = %node_id, error = %e, "Replication to replica failed");
                    errors.push((node_id, e));
                }
                Err(e) => {
                    error!(error = %e, "Replication task panicked");
                }
            }
        }

        if acks >= quorum_needed {
            ReplicationResult::success(entry.id, acks, quorum_needed)
        } else {
            ReplicationResult::quorum_failed(entry.id, acks, quorum_needed, errors)
        }
    }
}

#[async_trait]
impl Replicator for SyncReplicator {
    async fn replicate(&self, entry: ReplicationEntry) -> Result<ReplicationResult> {
        match entry.level {
            ReplicationLevel::Local => {
                // No replication needed
                Ok(ReplicationResult::success(entry.id, 1, 1))
            }
            ReplicationLevel::Replicated => {
                // For replicated level, we still do sync but don't require quorum
                // Just best-effort to all replicas
                let result = self.replicate_with_quorum(&entry).await;

                // Even if quorum not achieved, return success for replicated level
                Ok(ReplicationResult::success(entry.id, result.acks_received, 1))
            }
            ReplicationLevel::Durable => {
                // For durable, we require quorum
                let result = self.replicate_with_quorum(&entry).await;

                if result.quorum_achieved {
                    Ok(result)
                } else if self.config.allow_degraded_writes {
                    // Fall back to replicated mode if allowed
                    warn!(
                        entry_id = %entry.id,
                        acks = result.acks_received,
                        required = result.acks_required,
                        "Quorum not achieved, accepting degraded write"
                    );
                    Ok(ReplicationResult::success(entry.id, result.acks_received, 1))
                } else {
                    Err(ReplicationError::QuorumNotAchieved {
                        required: result.acks_required,
                        achieved: result.acks_received,
                    })
                }
            }
        }
    }

    async fn get_replicas(&self) -> Vec<ReplicaInfo> {
        let mut replicas = Vec::with_capacity(self.replicas.len());

        for item in self.replicas.iter() {
            let node_id = item.key().clone();
            let client = item.value();

            let lag_ms = self.lag_tracker.get_lag_ms(&node_id).unwrap_or(0);
            let is_healthy = lag_ms < self.config.lag_threshold_ms;

            replicas.push(ReplicaInfo {
                node_id: node_id.clone(),
                address: client.address().to_string(),
                is_healthy,
                lag_ms,
                last_replicated_at: self.lag_tracker.last_success_time(&node_id),
            });
        }

        replicas
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tokio::sync::Mutex;

    use super::*;

    /// Mock replica client for testing.
    struct MockReplicaClient {
        node_id: String,
        address: String,
        send_count: AtomicUsize,
        should_fail: AtomicBool,
        delay_ms: Mutex<Option<u64>>,
    }

    impl MockReplicaClient {
        fn new(node_id: &str, address: &str) -> Self {
            Self {
                node_id: node_id.to_string(),
                address: address.to_string(),
                send_count: AtomicUsize::new(0),
                should_fail: AtomicBool::new(false),
                delay_ms: Mutex::new(None),
            }
        }

        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::SeqCst);
        }

        async fn set_delay(&self, delay: Option<u64>) {
            *self.delay_ms.lock().await = delay;
        }
    }

    #[async_trait]
    impl ReplicaClient for MockReplicaClient {
        async fn send(&self, _entry: &ReplicationEntry) -> Result<()> {
            // Apply delay if configured
            if let Some(delay) = *self.delay_ms.lock().await {
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }

            if self.should_fail.load(Ordering::SeqCst) {
                Err(ReplicationError::ConnectionFailed {
                    node_id: self.node_id.clone(),
                    reason: "mock failure".to_string(),
                })
            } else {
                self.send_count.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }

        fn node_id(&self) -> &str {
            &self.node_id
        }

        fn address(&self) -> &str {
            &self.address
        }

        async fn health_check(&self) -> bool {
            !self.should_fail.load(Ordering::SeqCst)
        }
    }

    #[tokio::test]
    async fn test_sync_replicator_creation() {
        let config = ReplicationConfig::default();
        let replicator = SyncReplicator::new(config);

        let replicas = replicator.get_replicas().await;
        assert!(replicas.is_empty());
    }

    #[tokio::test]
    async fn test_local_level_no_replication() {
        let config = ReplicationConfig::default();
        let replicator = SyncReplicator::new(config);

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Local,
        );

        let result = replicator.replicate(entry).await.unwrap();
        assert!(result.quorum_achieved);
        assert_eq!(result.acks_received, 1);
    }

    #[tokio::test]
    async fn test_durable_quorum_achieved() {
        let config = ReplicationConfig::new().replication_factor(3);
        let replicator = SyncReplicator::new(config);

        // Add 2 replicas (with primary = 3 nodes, quorum = 2)
        let client1 = Arc::new(MockReplicaClient::new("node1", "192.168.1.1:9000"));
        let client2 = Arc::new(MockReplicaClient::new("node2", "192.168.1.2:9000"));

        replicator.add_replica(client1.clone());
        replicator.add_replica(client2.clone());

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Durable,
        );

        let result = replicator.replicate(entry).await.unwrap();
        assert!(result.quorum_achieved);
        assert_eq!(result.acks_received, 3); // primary + 2 replicas
        assert_eq!(result.acks_required, 2); // RF/2 + 1 = 2
    }

    #[tokio::test]
    async fn test_durable_quorum_not_achieved() {
        let config = ReplicationConfig::new().replication_factor(3).allow_degraded_writes(false);
        let replicator = SyncReplicator::new(config);

        // Add 2 replicas, both will fail
        let client1 = Arc::new(MockReplicaClient::new("node1", "192.168.1.1:9000"));
        let client2 = Arc::new(MockReplicaClient::new("node2", "192.168.1.2:9000"));
        client1.set_should_fail(true);
        client2.set_should_fail(true);

        replicator.add_replica(client1);
        replicator.add_replica(client2);

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Durable,
        );

        let result = replicator.replicate(entry).await;
        assert!(result.is_err());

        if let Err(ReplicationError::QuorumNotAchieved { required, achieved }) = result {
            assert_eq!(required, 2);
            assert_eq!(achieved, 1); // Only primary
        } else {
            panic!("Expected QuorumNotAchieved error");
        }
    }

    #[tokio::test]
    async fn test_durable_degraded_writes_allowed() {
        let config = ReplicationConfig::new().replication_factor(3).allow_degraded_writes(true);
        let replicator = SyncReplicator::new(config);

        // Add 2 replicas, both will fail
        let client1 = Arc::new(MockReplicaClient::new("node1", "192.168.1.1:9000"));
        let client2 = Arc::new(MockReplicaClient::new("node2", "192.168.1.2:9000"));
        client1.set_should_fail(true);
        client2.set_should_fail(true);

        replicator.add_replica(client1);
        replicator.add_replica(client2);

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Durable,
        );

        // Should succeed with degraded write
        let result = replicator.replicate(entry).await.unwrap();
        assert!(result.quorum_achieved); // Fallback to quorum=1
        assert_eq!(result.acks_received, 1);
    }

    #[tokio::test]
    async fn test_replicated_level_best_effort() {
        let config = ReplicationConfig::new().replication_factor(3);
        let replicator = SyncReplicator::new(config);

        // Add 2 replicas, one will fail
        let client1 = Arc::new(MockReplicaClient::new("node1", "192.168.1.1:9000"));
        let client2 = Arc::new(MockReplicaClient::new("node2", "192.168.1.2:9000"));
        client1.set_should_fail(true);

        replicator.add_replica(client1);
        replicator.add_replica(client2);

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Replicated,
        );

        // Should succeed even without quorum for replicated level
        let result = replicator.replicate(entry).await.unwrap();
        assert!(result.quorum_achieved);
        assert_eq!(result.acks_received, 2); // primary + 1 successful replica
    }

    #[tokio::test]
    async fn test_timeout_handling() {
        let config = ReplicationConfig::new()
            .replication_factor(3)
            .timeout(Duration::from_millis(100))
            .allow_degraded_writes(false);
        let replicator = SyncReplicator::new(config);

        // Add replica that will timeout
        let client = Arc::new(MockReplicaClient::new("node1", "192.168.1.1:9000"));
        client.set_delay(Some(200)).await; // 200ms delay, 100ms timeout

        replicator.add_replica(client);

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Durable,
        );

        // Should fail due to timeout preventing quorum
        let result = replicator.replicate(entry).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_remove_replica() {
        let config = ReplicationConfig::default();
        let replicator = SyncReplicator::new(config);

        let client = Arc::new(MockReplicaClient::new("node1", "192.168.1.1:9000"));
        replicator.add_replica(client);

        let replicas = replicator.get_replicas().await;
        assert_eq!(replicas.len(), 1);

        replicator.remove_replica("node1");

        let replicas = replicator.get_replicas().await;
        assert!(replicas.is_empty());
    }
}
