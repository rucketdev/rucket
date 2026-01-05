//! Asynchronous replication to backup nodes.
//!
//! This module implements async replication where writes are acknowledged
//! immediately after local persistence, with background replication to backups.

use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::config::ReplicationConfig;
use super::error::{ReplicationError, Result};
use super::lag::LagTracker;
use super::level::ReplicationLevel;
use super::replicator::{ReplicaInfo, ReplicationEntry, ReplicationResult, Replicator};

/// A client for sending replication data to a replica node.
#[async_trait]
pub trait ReplicaClient: Send + Sync {
    /// Sends a replication entry to the replica.
    async fn send(&self, entry: &ReplicationEntry) -> Result<()>;

    /// Returns the node ID of this replica.
    fn node_id(&self) -> &str;

    /// Returns the address of this replica.
    fn address(&self) -> &str;

    /// Checks if the replica is reachable.
    async fn health_check(&self) -> bool;
}

/// Queue item for async replication.
struct QueuedEntry {
    entry: ReplicationEntry,
    retries: u32,
}

/// Asynchronous replicator that queues writes for background replication.
///
/// This replicator provides eventual consistency by:
/// 1. Immediately acknowledging writes after local persistence
/// 2. Queueing writes for background replication to backups
/// 3. Retrying failed replications with exponential backoff
pub struct AsyncReplicator {
    config: ReplicationConfig,
    replicas: DashMap<String, Arc<dyn ReplicaClient>>,
    queues: DashMap<String, mpsc::Sender<QueuedEntry>>,
    lag_tracker: Arc<LagTracker>,
}

impl AsyncReplicator {
    /// Creates a new async replicator.
    pub fn new(config: ReplicationConfig) -> Self {
        Self {
            config,
            replicas: DashMap::new(),
            queues: DashMap::new(),
            lag_tracker: Arc::new(LagTracker::new()),
        }
    }

    /// Adds a replica to the replication set.
    pub fn add_replica(&self, client: Arc<dyn ReplicaClient>) {
        let node_id = client.node_id().to_string();
        let address = client.address().to_string();

        // Create a queue for this replica
        let (tx, rx) = mpsc::channel::<QueuedEntry>(self.config.queue_size);

        // Store the replica and queue
        self.replicas.insert(node_id.clone(), client.clone());
        self.queues.insert(node_id.clone(), tx);

        // Initialize lag tracking
        self.lag_tracker.add_replica(node_id.clone());

        // Spawn background worker
        let config = self.config.clone();
        let lag_tracker = self.lag_tracker.clone();
        tokio::spawn(async move {
            Self::replication_worker(node_id, address, client, rx, config, lag_tracker).await;
        });
    }

    /// Removes a replica from the replication set.
    pub fn remove_replica(&self, node_id: &str) {
        self.replicas.remove(node_id);
        self.queues.remove(node_id);
        self.lag_tracker.remove_replica(node_id);
        info!(node_id = %node_id, "Removed replica from replication set");
    }

    /// Background worker that processes the replication queue.
    async fn replication_worker(
        node_id: String,
        address: String,
        client: Arc<dyn ReplicaClient>,
        mut rx: mpsc::Receiver<QueuedEntry>,
        config: ReplicationConfig,
        lag_tracker: Arc<LagTracker>,
    ) {
        info!(node_id = %node_id, address = %address, "Starting replication worker");

        let mut batch = Vec::with_capacity(config.batch_size);

        loop {
            // Collect a batch of entries
            batch.clear();

            // Wait for at least one entry
            match rx.recv().await {
                Some(entry) => batch.push(entry),
                None => {
                    info!(node_id = %node_id, "Replication queue closed, stopping worker");
                    break;
                }
            }

            // Try to collect more entries up to batch size (non-blocking)
            while batch.len() < config.batch_size {
                match rx.try_recv() {
                    Ok(entry) => batch.push(entry),
                    Err(_) => break,
                }
            }

            // Process the batch
            for queued in batch.drain(..) {
                Self::send_with_retry(
                    &node_id,
                    &client,
                    queued.entry,
                    queued.retries,
                    &config,
                    &lag_tracker,
                )
                .await;
            }
        }
    }

    /// Sends an entry with retry logic using exponential backoff.
    async fn send_with_retry(
        node_id: &str,
        client: &Arc<dyn ReplicaClient>,
        entry: ReplicationEntry,
        initial_retries: u32,
        config: &ReplicationConfig,
        lag_tracker: &LagTracker,
    ) {
        let mut retries = initial_retries;

        loop {
            match client.send(&entry).await {
                Ok(()) => {
                    if retries > initial_retries {
                        debug!(
                            node_id = %node_id,
                            entry_id = %entry.id,
                            retries = retries,
                            "Successfully replicated entry on retry"
                        );
                    } else {
                        debug!(
                            node_id = %node_id,
                            entry_id = %entry.id,
                            "Successfully replicated entry"
                        );
                    }
                    lag_tracker.record_success(node_id, entry.hlc_timestamp);
                    return;
                }
                Err(e) => {
                    lag_tracker.record_failure(node_id);

                    if retries >= config.max_retries {
                        error!(
                            node_id = %node_id,
                            entry_id = %entry.id,
                            error = %e,
                            retries = retries,
                            max_retries = config.max_retries,
                            "Max retries exceeded, dropping entry"
                        );
                        return;
                    }

                    let backoff = config.backoff_for_retry(retries);
                    warn!(
                        node_id = %node_id,
                        entry_id = %entry.id,
                        error = %e,
                        retries = retries,
                        max_retries = config.max_retries,
                        backoff_ms = backoff.as_millis(),
                        "Failed to replicate entry, retrying after backoff"
                    );

                    tokio::time::sleep(backoff).await;
                    retries += 1;
                }
            }
        }
    }

    /// Returns the lag tracker for monitoring.
    pub fn lag_tracker(&self) -> &LagTracker {
        &self.lag_tracker
    }

    /// Queues an entry for replication to all replicas.
    async fn queue_to_all(&self, entry: &ReplicationEntry) -> Vec<(String, ReplicationError)> {
        let mut errors = Vec::new();

        for queue in self.queues.iter() {
            let node_id = queue.key().clone();
            let tx = queue.value();

            let queued = QueuedEntry { entry: entry.clone(), retries: 0 };

            if let Err(e) = tx.try_send(queued) {
                let error = match e {
                    mpsc::error::TrySendError::Full(_) => {
                        ReplicationError::QueueFull { pending_items: self.config.queue_size }
                    }
                    mpsc::error::TrySendError::Closed(_) => {
                        ReplicationError::ReplicaUnavailable { node_id: node_id.clone() }
                    }
                };
                error!(
                    node_id = %node_id,
                    entry_id = %entry.id,
                    error = %error,
                    "Failed to queue replication entry"
                );
                errors.push((node_id, error));
            }
        }

        errors
    }
}

#[async_trait]
impl Replicator for AsyncReplicator {
    async fn replicate(&self, entry: ReplicationEntry) -> Result<ReplicationResult> {
        match entry.level {
            ReplicationLevel::Local => {
                // No replication needed
                Ok(ReplicationResult::success(entry.id, 1, 1))
            }
            ReplicationLevel::Replicated => {
                // Queue for async replication
                let errors = self.queue_to_all(&entry).await;
                let queued_count = self.queues.len() - errors.len();

                // For async replication, we succeed if we queued to at least one replica
                if errors.len() < self.queues.len() {
                    Ok(ReplicationResult::success(
                        entry.id,
                        queued_count + 1, // +1 for primary
                        1,
                    ))
                } else if self.queues.is_empty() {
                    // No replicas configured - that's okay for single-node
                    Ok(ReplicationResult::success(entry.id, 1, 1))
                } else {
                    // Failed to queue to any replica
                    Ok(ReplicationResult::quorum_failed(entry.id, 1, 2, errors))
                }
            }
            ReplicationLevel::Durable => {
                // For durable, we need sync replication - delegate to SyncReplicator
                // This is handled at a higher level; async replicator treats it as replicated
                let errors = self.queue_to_all(&entry).await;
                let queued_count = self.queues.len() - errors.len();

                Ok(ReplicationResult::success(
                    entry.id,
                    queued_count + 1,
                    self.config.quorum_size(),
                ))
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
    use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
    use std::time::Duration;

    use tokio::sync::Mutex;

    use super::*;

    /// Mock replica client for testing.
    struct MockReplicaClient {
        node_id: String,
        address: String,
        send_count: AtomicUsize,
        should_fail: Mutex<bool>,
    }

    impl MockReplicaClient {
        fn new(node_id: &str, address: &str) -> Self {
            Self {
                node_id: node_id.to_string(),
                address: address.to_string(),
                send_count: AtomicUsize::new(0),
                should_fail: Mutex::new(false),
            }
        }

        #[allow(dead_code)]
        fn send_count(&self) -> usize {
            self.send_count.load(Ordering::SeqCst)
        }

        #[allow(dead_code)]
        async fn set_should_fail(&self, fail: bool) {
            *self.should_fail.lock().await = fail;
        }
    }

    #[async_trait]
    impl ReplicaClient for MockReplicaClient {
        async fn send(&self, _entry: &ReplicationEntry) -> Result<()> {
            if *self.should_fail.lock().await {
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
            !*self.should_fail.lock().await
        }
    }

    /// Mock replica client that fails a configurable number of times.
    struct FailingReplicaClient {
        node_id: String,
        address: String,
        send_count: AtomicUsize,
        fail_count: AtomicU32,
        failures_remaining: AtomicU32,
    }

    impl FailingReplicaClient {
        fn new(node_id: &str, address: &str, fail_count: u32) -> Self {
            Self {
                node_id: node_id.to_string(),
                address: address.to_string(),
                send_count: AtomicUsize::new(0),
                fail_count: AtomicU32::new(fail_count),
                failures_remaining: AtomicU32::new(fail_count),
            }
        }

        fn send_count(&self) -> usize {
            self.send_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl ReplicaClient for FailingReplicaClient {
        async fn send(&self, _entry: &ReplicationEntry) -> Result<()> {
            self.send_count.fetch_add(1, Ordering::SeqCst);

            let remaining = self.failures_remaining.load(Ordering::SeqCst);
            if remaining > 0 {
                self.failures_remaining.fetch_sub(1, Ordering::SeqCst);
                Err(ReplicationError::ConnectionFailed {
                    node_id: self.node_id.clone(),
                    reason: format!(
                        "simulated failure {}/{}",
                        self.fail_count.load(Ordering::SeqCst) - remaining + 1,
                        self.fail_count.load(Ordering::SeqCst)
                    ),
                })
            } else {
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
            self.failures_remaining.load(Ordering::SeqCst) == 0
        }
    }

    #[tokio::test]
    async fn test_async_replicator_creation() {
        let config = ReplicationConfig::default();
        let replicator = AsyncReplicator::new(config);

        let replicas = replicator.get_replicas().await;
        assert!(replicas.is_empty());
    }

    #[tokio::test]
    async fn test_local_replication() {
        let config = ReplicationConfig::default();
        let replicator = AsyncReplicator::new(config);

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
    async fn test_replicated_level_no_replicas() {
        let config = ReplicationConfig::default();
        let replicator = AsyncReplicator::new(config);

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Replicated,
        );

        // Should succeed even with no replicas (single-node mode)
        let result = replicator.replicate(entry).await.unwrap();
        assert!(result.quorum_achieved);
    }

    #[tokio::test]
    async fn test_add_remove_replica() {
        let config = ReplicationConfig::default();
        let replicator = AsyncReplicator::new(config);

        let client = Arc::new(MockReplicaClient::new("node1", "192.168.1.1:9000"));
        replicator.add_replica(client);

        let replicas = replicator.get_replicas().await;
        assert_eq!(replicas.len(), 1);
        assert_eq!(replicas[0].node_id, "node1");

        replicator.remove_replica("node1");

        let replicas = replicator.get_replicas().await;
        assert!(replicas.is_empty());
    }

    #[tokio::test]
    async fn test_send_with_retry_success() {
        // Test that send_with_retry succeeds on first attempt
        let failing_client = Arc::new(FailingReplicaClient::new("node1", "192.168.1.1:9000", 0));
        let client: Arc<dyn ReplicaClient> = failing_client.clone();
        let config = ReplicationConfig::new()
            .max_retries(3)
            .initial_backoff(Duration::from_millis(10))
            .max_backoff(Duration::from_millis(100));
        let lag_tracker = LagTracker::new();
        lag_tracker.add_replica("node1".to_string());

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Replicated,
        );

        AsyncReplicator::send_with_retry("node1", &client, entry, 0, &config, &lag_tracker).await;

        // Should have sent once successfully
        assert_eq!(failing_client.send_count(), 1);
    }

    #[tokio::test]
    async fn test_send_with_retry_transient_failure() {
        // Test that send_with_retry retries and succeeds after transient failures
        let failing_client = Arc::new(FailingReplicaClient::new("node1", "192.168.1.1:9000", 2));
        let client: Arc<dyn ReplicaClient> = failing_client.clone();
        let config = ReplicationConfig::new()
            .max_retries(3)
            .initial_backoff(Duration::from_millis(1))
            .max_backoff(Duration::from_millis(10));
        let lag_tracker = LagTracker::new();
        lag_tracker.add_replica("node1".to_string());

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Replicated,
        );

        AsyncReplicator::send_with_retry("node1", &client, entry, 0, &config, &lag_tracker).await;

        // Should have sent 3 times (2 failures + 1 success)
        assert_eq!(failing_client.send_count(), 3);
    }

    #[tokio::test]
    async fn test_send_with_retry_max_retries_exceeded() {
        // Test that send_with_retry gives up after max retries
        let failing_client = Arc::new(FailingReplicaClient::new("node1", "192.168.1.1:9000", 10));
        let client: Arc<dyn ReplicaClient> = failing_client.clone();
        let config = ReplicationConfig::new()
            .max_retries(3)
            .initial_backoff(Duration::from_millis(1))
            .max_backoff(Duration::from_millis(10));
        let lag_tracker = LagTracker::new();
        lag_tracker.add_replica("node1".to_string());

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Replicated,
        );

        AsyncReplicator::send_with_retry("node1", &client, entry, 0, &config, &lag_tracker).await;

        // Should have sent 4 times (initial + 3 retries) before giving up
        assert_eq!(failing_client.send_count(), 4);
    }

    #[tokio::test]
    async fn test_send_with_retry_zero_max_retries() {
        // Test that with max_retries=0, no retries occur
        let failing_client = Arc::new(FailingReplicaClient::new("node1", "192.168.1.1:9000", 10));
        let client: Arc<dyn ReplicaClient> = failing_client.clone();
        let config = ReplicationConfig::new()
            .max_retries(0)
            .initial_backoff(Duration::from_millis(1))
            .max_backoff(Duration::from_millis(10));
        let lag_tracker = LagTracker::new();
        lag_tracker.add_replica("node1".to_string());

        let entry = ReplicationEntry::new(
            super::super::replicator::ReplicationOperation::CreateBucket {
                bucket: "test".to_string(),
            },
            1,
            ReplicationLevel::Replicated,
        );

        AsyncReplicator::send_with_retry("node1", &client, entry, 0, &config, &lag_tracker).await;

        // Should have sent once only
        assert_eq!(failing_client.send_count(), 1);
    }
}
