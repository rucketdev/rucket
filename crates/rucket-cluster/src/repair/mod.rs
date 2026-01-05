//! Shard repair loop for automatic data recovery.
//!
//! This module provides automatic repair of data when nodes fail:
//! - Monitors for node failures via HeartbeatManager events
//! - Identifies shards/objects affected by the failure
//! - Coordinates reconstruction using erasure coding or replication
//! - Places repaired data on healthy nodes
//!
//! # Architecture
//!
//! The repair system operates in three phases:
//! 1. **Detection**: HeartbeatManager detects node failure
//! 2. **Identification**: RepairManager queries affected placement groups
//! 3. **Repair**: ShardRepairer reconstructs and redistributes data
//!
//! # Example
//!
//! ```ignore
//! use rucket_cluster::{RepairManager, RepairConfig, HeartbeatManager};
//!
//! let repair_config = RepairConfig::default();
//! let mut repair_manager = RepairManager::new(repair_config);
//!
//! // Subscribe to heartbeat events
//! let events = heartbeat_manager.subscribe();
//! repair_manager.start(events).await;
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, trace, warn};

use super::heartbeat::HeartbeatEvent;

/// Configuration for the repair manager.
#[derive(Debug, Clone)]
pub struct RepairConfig {
    /// Delay before starting repair after node failure.
    /// Allows for transient failures to recover.
    pub repair_delay: Duration,

    /// Maximum concurrent repair operations.
    pub max_concurrent_repairs: usize,

    /// Timeout for individual shard repair operations.
    pub repair_timeout: Duration,

    /// Interval between repair status checks.
    pub check_interval: Duration,

    /// Minimum healthy replicas required before repair.
    /// If fewer replicas are healthy, repair may fail.
    pub min_healthy_replicas: usize,

    /// Whether to prioritize repairs by data age.
    pub prioritize_by_age: bool,

    /// Maximum repairs per second (rate limiting).
    pub max_repairs_per_second: u32,
}

impl Default for RepairConfig {
    fn default() -> Self {
        Self {
            repair_delay: Duration::from_secs(30),
            max_concurrent_repairs: 4,
            repair_timeout: Duration::from_secs(300),
            check_interval: Duration::from_secs(10),
            min_healthy_replicas: 1,
            prioritize_by_age: true,
            max_repairs_per_second: 100,
        }
    }
}

/// Information about a shard that needs repair.
#[derive(Debug, Clone)]
pub struct ShardRepairTask {
    /// Unique identifier for the repair task.
    pub task_id: String,
    /// Bucket containing the object.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID if versioned.
    pub version_id: Option<String>,
    /// Shard index (for erasure coded objects).
    pub shard_index: Option<u32>,
    /// Node that was hosting this shard.
    pub failed_node: String,
    /// Nodes that still have replicas.
    pub available_nodes: Vec<String>,
    /// Priority (lower = higher priority).
    pub priority: u32,
    /// When the repair was scheduled.
    pub scheduled_at: Instant,
    /// Number of retry attempts.
    pub retry_count: u32,
}

impl ShardRepairTask {
    /// Creates a new repair task.
    pub fn new(
        bucket: String,
        key: String,
        failed_node: String,
        available_nodes: Vec<String>,
    ) -> Self {
        Self {
            task_id: uuid::Uuid::new_v4().to_string(),
            bucket,
            key,
            version_id: None,
            shard_index: None,
            failed_node,
            available_nodes,
            priority: 100,
            scheduled_at: Instant::now(),
            retry_count: 0,
        }
    }

    /// Sets the shard index for erasure coded objects.
    pub fn with_shard_index(mut self, index: u32) -> Self {
        self.shard_index = Some(index);
        self
    }

    /// Sets the version ID.
    pub fn with_version(mut self, version_id: String) -> Self {
        self.version_id = Some(version_id);
        self
    }

    /// Sets the priority.
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

/// Result of a repair operation.
#[derive(Debug, Clone)]
pub enum RepairResult {
    /// Repair completed successfully.
    Success {
        /// The task that was repaired.
        task_id: String,
        /// Node where the repaired shard was placed.
        target_node: String,
        /// Duration of the repair operation.
        duration: Duration,
    },
    /// Repair failed.
    Failed {
        /// The task that failed.
        task_id: String,
        /// Error message.
        error: String,
        /// Whether the task should be retried.
        retriable: bool,
    },
    /// Not enough healthy replicas to repair.
    InsufficientReplicas {
        /// The task that couldn't be repaired.
        task_id: String,
        /// Number of available replicas.
        available: usize,
        /// Number of required replicas.
        required: usize,
    },
}

/// Trait for providing shard location information.
#[async_trait]
pub trait ShardLocator: Send + Sync {
    /// Returns the nodes hosting shards for the given object.
    async fn get_shard_locations(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
    ) -> Result<Vec<ShardLocation>, String>;

    /// Returns all objects/shards hosted on the given node.
    async fn get_objects_on_node(&self, node_id: &str) -> Result<Vec<ObjectInfo>, String>;
}

/// Location of a shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardLocation {
    /// Node hosting this shard.
    pub node_id: String,
    /// Shard index (0 for replicated objects).
    pub shard_index: u32,
    /// Whether this is a parity shard.
    pub is_parity: bool,
    /// Checksum of the shard.
    pub checksum: Option<String>,
}

/// Information about an object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectInfo {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID if versioned.
    pub version_id: Option<String>,
    /// Size in bytes.
    pub size: u64,
    /// Whether the object uses erasure coding.
    pub is_erasure_coded: bool,
    /// Number of shards (for erasure coded objects).
    pub shard_count: Option<u32>,
}

/// Trait for executing shard repairs.
#[async_trait]
pub trait ShardRepairer: Send + Sync {
    /// Repairs a shard by reading from available replicas and writing to a new node.
    async fn repair_shard(&self, task: &ShardRepairTask, target_node: &str) -> Result<(), String>;

    /// Selects a target node for the repaired shard.
    async fn select_target_node(
        &self,
        task: &ShardRepairTask,
        healthy_nodes: &[String],
    ) -> Result<String, String>;
}

/// Events emitted by the repair manager.
#[derive(Debug, Clone)]
pub enum RepairEvent {
    /// A repair task was scheduled.
    TaskScheduled {
        /// Task ID.
        task_id: String,
        /// Bucket.
        bucket: String,
        /// Key.
        key: String,
    },
    /// A repair task started execution.
    TaskStarted {
        /// Task ID.
        task_id: String,
    },
    /// A repair task completed.
    TaskCompleted {
        /// Task ID.
        task_id: String,
        /// Result.
        result: RepairResult,
    },
    /// Repair queue status update.
    QueueStatus {
        /// Number of pending tasks.
        pending: usize,
        /// Number of in-progress tasks.
        in_progress: usize,
        /// Number of completed tasks.
        completed: usize,
        /// Number of failed tasks.
        failed: usize,
    },
}

/// Status of a repair task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    /// Task is waiting to be executed.
    Pending,
    /// Task is currently being executed.
    InProgress,
    /// Task completed successfully.
    Completed,
    /// Task failed.
    Failed,
}

/// Manages shard repair operations.
pub struct RepairManager {
    config: RepairConfig,
    /// Pending repair tasks, keyed by task_id.
    pending_tasks: Arc<DashMap<String, ShardRepairTask>>,
    /// Tasks currently being repaired.
    in_progress: Arc<DashMap<String, ShardRepairTask>>,
    /// Completed task count.
    completed_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Failed task count.
    failed_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Nodes currently marked as failed.
    failed_nodes: Arc<RwLock<HashSet<String>>>,
    /// Known healthy nodes.
    healthy_nodes: Arc<RwLock<HashSet<String>>>,
    /// Event broadcaster.
    event_tx: broadcast::Sender<RepairEvent>,
    /// Shutdown channel.
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl RepairManager {
    /// Creates a new repair manager.
    pub fn new(config: RepairConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            config,
            pending_tasks: Arc::new(DashMap::new()),
            in_progress: Arc::new(DashMap::new()),
            completed_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            failed_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            failed_nodes: Arc::new(RwLock::new(HashSet::new())),
            healthy_nodes: Arc::new(RwLock::new(HashSet::new())),
            event_tx,
            shutdown_tx: None,
        }
    }

    /// Subscribes to repair events.
    pub fn subscribe(&self) -> broadcast::Receiver<RepairEvent> {
        self.event_tx.subscribe()
    }

    /// Schedules a repair task.
    pub fn schedule_repair(&self, task: ShardRepairTask) {
        let task_id = task.task_id.clone();
        let bucket = task.bucket.clone();
        let key = task.key.clone();

        self.pending_tasks.insert(task_id.clone(), task);
        counter!("rucket_repair_tasks_scheduled").increment(1);
        gauge!("rucket_repair_pending_tasks").set(self.pending_tasks.len() as f64);

        let _ = self.event_tx.send(RepairEvent::TaskScheduled { task_id, bucket, key });

        debug!(pending = self.pending_tasks.len(), "Scheduled repair task");
    }

    /// Schedules repairs for all objects on a failed node.
    pub async fn schedule_repairs_for_node<L: ShardLocator>(
        &self,
        node_id: &str,
        locator: &L,
    ) -> Result<usize, String> {
        // Mark node as failed
        {
            let mut failed = self.failed_nodes.write().await;
            failed.insert(node_id.to_string());
        }

        // Get all objects on the failed node
        let objects = locator.get_objects_on_node(node_id).await?;
        let count = objects.len();

        info!(
            node_id = %node_id,
            object_count = count,
            "Scheduling repairs for failed node"
        );

        // Schedule repair for each object
        for obj in objects {
            let locations = locator
                .get_shard_locations(&obj.bucket, &obj.key, obj.version_id.as_deref())
                .await?;

            let available_nodes: Vec<String> = locations
                .iter()
                .filter(|loc| loc.node_id != node_id)
                .map(|loc| loc.node_id.clone())
                .collect();

            if obj.is_erasure_coded {
                // Schedule repair for each shard on the failed node
                for loc in locations.iter().filter(|l| l.node_id == node_id) {
                    let task = ShardRepairTask::new(
                        obj.bucket.clone(),
                        obj.key.clone(),
                        node_id.to_string(),
                        available_nodes.clone(),
                    )
                    .with_shard_index(loc.shard_index);

                    if let Some(ver) = &obj.version_id {
                        self.schedule_repair(task.with_version(ver.clone()));
                    } else {
                        self.schedule_repair(task);
                    }
                }
            } else {
                // Schedule single repair task for replicated object
                let task = ShardRepairTask::new(
                    obj.bucket.clone(),
                    obj.key.clone(),
                    node_id.to_string(),
                    available_nodes,
                );

                if let Some(ver) = &obj.version_id {
                    self.schedule_repair(task.with_version(ver.clone()));
                } else {
                    self.schedule_repair(task);
                }
            }
        }

        counter!("rucket_repair_node_failures").increment(1);

        Ok(count)
    }

    /// Marks a node as healthy (recovered).
    pub async fn mark_node_healthy(&self, node_id: &str) {
        {
            let mut failed = self.failed_nodes.write().await;
            failed.remove(node_id);
        }
        {
            let mut healthy = self.healthy_nodes.write().await;
            healthy.insert(node_id.to_string());
        }
        info!(node_id = %node_id, "Node marked as healthy");
    }

    /// Starts the repair loop.
    pub async fn start<R: ShardRepairer + 'static>(
        &mut self,
        mut heartbeat_events: broadcast::Receiver<HeartbeatEvent>,
        repairer: Arc<R>,
    ) {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let config = self.config.clone();
        let pending_tasks = Arc::clone(&self.pending_tasks);
        let in_progress = Arc::clone(&self.in_progress);
        let completed_count = Arc::clone(&self.completed_count);
        let failed_count = Arc::clone(&self.failed_count);
        let failed_nodes = Arc::clone(&self.failed_nodes);
        let healthy_nodes = Arc::clone(&self.healthy_nodes);
        let event_tx = self.event_tx.clone();

        // Task to process heartbeat events
        let failed_nodes_clone = Arc::clone(&failed_nodes);
        let healthy_nodes_clone = Arc::clone(&healthy_nodes);
        tokio::spawn(async move {
            loop {
                match heartbeat_events.recv().await {
                    Ok(HeartbeatEvent::NodeFailed { node_id, phi }) => {
                        warn!(node_id = %node_id, phi = phi, "Node failed, will schedule repairs");
                        let mut failed = failed_nodes_clone.write().await;
                        failed.insert(node_id);
                    }
                    Ok(HeartbeatEvent::NodeRecovered { node_id }) => {
                        info!(node_id = %node_id, "Node recovered");
                        let mut failed = failed_nodes_clone.write().await;
                        failed.remove(&node_id);
                        let mut healthy = healthy_nodes_clone.write().await;
                        healthy.insert(node_id);
                    }
                    Ok(HeartbeatEvent::NodeHealthy { node_id }) => {
                        let mut healthy = healthy_nodes_clone.write().await;
                        healthy.insert(node_id);
                    }
                    Ok(_) => {}
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(skipped = n, "Heartbeat event receiver lagged");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("Heartbeat event channel closed");
                        break;
                    }
                }
            }
        });

        // Main repair loop
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            let semaphore = Arc::new(tokio::sync::Semaphore::new(config.max_concurrent_repairs));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::process_repair_queue(
                            &config,
                            &pending_tasks,
                            &in_progress,
                            &completed_count,
                            &failed_count,
                            &healthy_nodes,
                            &event_tx,
                            &repairer,
                            &semaphore,
                        ).await;
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Repair manager shutting down");
                        break;
                    }
                }
            }
        });

        info!(
            max_concurrent = self.config.max_concurrent_repairs,
            check_interval_ms = self.config.check_interval.as_millis(),
            "Repair manager started"
        );
    }

    /// Processes the repair queue.
    #[allow(clippy::too_many_arguments)]
    async fn process_repair_queue<R: ShardRepairer + 'static>(
        config: &RepairConfig,
        pending_tasks: &Arc<DashMap<String, ShardRepairTask>>,
        in_progress: &Arc<DashMap<String, ShardRepairTask>>,
        completed_count: &Arc<std::sync::atomic::AtomicUsize>,
        failed_count: &Arc<std::sync::atomic::AtomicUsize>,
        healthy_nodes: &Arc<RwLock<HashSet<String>>>,
        event_tx: &broadcast::Sender<RepairEvent>,
        repairer: &Arc<R>,
        semaphore: &Arc<tokio::sync::Semaphore>,
    ) {
        // Update metrics
        gauge!("rucket_repair_pending_tasks").set(pending_tasks.len() as f64);
        gauge!("rucket_repair_in_progress_tasks").set(in_progress.len() as f64);

        // Find tasks ready for repair (past delay period)
        let now = Instant::now();
        let ready_tasks: Vec<(String, ShardRepairTask)> = pending_tasks
            .iter()
            .filter(|entry| now.duration_since(entry.scheduled_at) >= config.repair_delay)
            .filter(|entry| entry.retry_count < 3)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .take(config.max_concurrent_repairs - in_progress.len())
            .collect();

        if ready_tasks.is_empty() {
            return;
        }

        let healthy: Vec<String> = healthy_nodes.read().await.iter().cloned().collect();

        for (task_id, task) in ready_tasks {
            // Try to acquire semaphore
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    trace!("Max concurrent repairs reached");
                    break;
                }
            };

            // Move from pending to in_progress
            pending_tasks.remove(&task_id);
            in_progress.insert(task_id.clone(), task.clone());

            let _ = event_tx.send(RepairEvent::TaskStarted { task_id: task_id.clone() });

            // Check if we have enough healthy replicas
            let available = task.available_nodes.len();
            if available < config.min_healthy_replicas {
                in_progress.remove(&task_id);
                failed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                counter!("rucket_repair_insufficient_replicas").increment(1);

                let _ = event_tx.send(RepairEvent::TaskCompleted {
                    task_id: task_id.clone(),
                    result: RepairResult::InsufficientReplicas {
                        task_id,
                        available,
                        required: config.min_healthy_replicas,
                    },
                });
                drop(permit);
                continue;
            }

            // Select target node
            let target_result = repairer.select_target_node(&task, &healthy).await;
            let target_node = match target_result {
                Ok(node) => node,
                Err(e) => {
                    warn!(task_id = %task_id, error = %e, "Failed to select target node");
                    in_progress.remove(&task_id);

                    // Re-schedule with retry
                    let mut retry_task = task;
                    retry_task.retry_count += 1;
                    pending_tasks.insert(task_id.clone(), retry_task);

                    drop(permit);
                    continue;
                }
            };

            // Spawn repair task
            let repairer = Arc::clone(repairer);
            let in_progress = Arc::clone(in_progress);
            let completed_count = Arc::clone(completed_count);
            let failed_count = Arc::clone(failed_count);
            let pending_tasks = Arc::clone(pending_tasks);
            let event_tx = event_tx.clone();
            let timeout = config.repair_timeout;
            let retry_count = task.retry_count;

            tokio::spawn(async move {
                let start = Instant::now();
                let repair_result =
                    tokio::time::timeout(timeout, repairer.repair_shard(&task, &target_node)).await;

                let duration = start.elapsed();
                in_progress.remove(&task_id);

                let result = match repair_result {
                    Ok(Ok(())) => {
                        completed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        counter!("rucket_repair_completed").increment(1);
                        histogram!("rucket_repair_duration_seconds").record(duration.as_secs_f64());

                        info!(
                            task_id = %task_id,
                            target_node = %target_node,
                            duration_ms = duration.as_millis(),
                            "Repair completed successfully"
                        );

                        RepairResult::Success { task_id: task_id.clone(), target_node, duration }
                    }
                    Ok(Err(e)) => {
                        failed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        counter!("rucket_repair_failed").increment(1);

                        error!(
                            task_id = %task_id,
                            error = %e,
                            "Repair failed"
                        );

                        // Re-schedule if retriable
                        let retriable = retry_count < 3;
                        if retriable {
                            let mut retry_task = task;
                            retry_task.retry_count += 1;
                            pending_tasks.insert(task_id.clone(), retry_task);
                        }

                        RepairResult::Failed { task_id: task_id.clone(), error: e, retriable }
                    }
                    Err(_) => {
                        failed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        counter!("rucket_repair_timeout").increment(1);

                        error!(
                            task_id = %task_id,
                            "Repair timed out"
                        );

                        let retriable = retry_count < 3;
                        RepairResult::Failed {
                            task_id: task_id.clone(),
                            error: "Repair timed out".to_string(),
                            retriable,
                        }
                    }
                };

                let _ = event_tx.send(RepairEvent::TaskCompleted { task_id, result });

                drop(permit);
            });
        }

        // Emit queue status
        let _ = event_tx.send(RepairEvent::QueueStatus {
            pending: pending_tasks.len(),
            in_progress: in_progress.len(),
            completed: completed_count.load(std::sync::atomic::Ordering::SeqCst),
            failed: failed_count.load(std::sync::atomic::Ordering::SeqCst),
        });
    }

    /// Stops the repair manager.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Returns the number of pending repairs.
    pub fn pending_count(&self) -> usize {
        self.pending_tasks.len()
    }

    /// Returns the number of in-progress repairs.
    pub fn in_progress_count(&self) -> usize {
        self.in_progress.len()
    }

    /// Returns the total number of completed repairs.
    pub fn completed_count(&self) -> usize {
        self.completed_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the total number of failed repairs.
    pub fn failed_count(&self) -> usize {
        self.failed_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the list of failed nodes.
    pub async fn failed_nodes(&self) -> Vec<String> {
        self.failed_nodes.read().await.iter().cloned().collect()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &RepairConfig {
        &self.config
    }
}

/// A no-op shard locator for testing.
pub struct NoOpShardLocator;

#[async_trait]
impl ShardLocator for NoOpShardLocator {
    async fn get_shard_locations(
        &self,
        _bucket: &str,
        _key: &str,
        _version_id: Option<&str>,
    ) -> Result<Vec<ShardLocation>, String> {
        Ok(vec![])
    }

    async fn get_objects_on_node(&self, _node_id: &str) -> Result<Vec<ObjectInfo>, String> {
        Ok(vec![])
    }
}

/// A no-op shard repairer for testing.
pub struct NoOpShardRepairer;

#[async_trait]
impl ShardRepairer for NoOpShardRepairer {
    async fn repair_shard(
        &self,
        _task: &ShardRepairTask,
        _target_node: &str,
    ) -> Result<(), String> {
        Ok(())
    }

    async fn select_target_node(
        &self,
        _task: &ShardRepairTask,
        healthy_nodes: &[String],
    ) -> Result<String, String> {
        healthy_nodes.first().cloned().ok_or_else(|| "No healthy nodes available".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_repair_config_defaults() {
        let config = RepairConfig::default();
        assert_eq!(config.repair_delay, Duration::from_secs(30));
        assert_eq!(config.max_concurrent_repairs, 4);
        assert_eq!(config.min_healthy_replicas, 1);
    }

    #[test]
    fn test_shard_repair_task_creation() {
        let task = ShardRepairTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "failed-node".to_string(),
            vec!["node-1".to_string(), "node-2".to_string()],
        );

        assert_eq!(task.bucket, "bucket");
        assert_eq!(task.key, "key");
        assert_eq!(task.failed_node, "failed-node");
        assert_eq!(task.available_nodes.len(), 2);
        assert_eq!(task.retry_count, 0);
    }

    #[test]
    fn test_shard_repair_task_with_shard() {
        let task = ShardRepairTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "failed-node".to_string(),
            vec![],
        )
        .with_shard_index(5)
        .with_version("v1".to_string())
        .with_priority(10);

        assert_eq!(task.shard_index, Some(5));
        assert_eq!(task.version_id, Some("v1".to_string()));
        assert_eq!(task.priority, 10);
    }

    #[tokio::test]
    async fn test_repair_manager_creation() {
        let config = RepairConfig::default();
        let manager = RepairManager::new(config);

        assert_eq!(manager.pending_count(), 0);
        assert_eq!(manager.in_progress_count(), 0);
        assert_eq!(manager.completed_count(), 0);
        assert_eq!(manager.failed_count(), 0);
    }

    #[tokio::test]
    async fn test_schedule_repair() {
        let config = RepairConfig::default();
        let manager = RepairManager::new(config);

        let task = ShardRepairTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "failed-node".to_string(),
            vec!["node-1".to_string()],
        );

        manager.schedule_repair(task);

        assert_eq!(manager.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_mark_node_healthy() {
        let config = RepairConfig::default();
        let manager = RepairManager::new(config);

        {
            let mut failed = manager.failed_nodes.write().await;
            failed.insert("node-1".to_string());
        }

        manager.mark_node_healthy("node-1").await;

        let failed = manager.failed_nodes().await;
        assert!(!failed.contains(&"node-1".to_string()));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let config = RepairConfig::default();
        let manager = RepairManager::new(config);
        let mut rx = manager.subscribe();

        let task = ShardRepairTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "failed-node".to_string(),
            vec!["node-1".to_string()],
        );

        manager.schedule_repair(task);

        // Should receive TaskScheduled event
        let event = rx.try_recv().unwrap();
        match event {
            RepairEvent::TaskScheduled { bucket, key, .. } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "key");
            }
            _ => panic!("Expected TaskScheduled event"),
        }
    }

    #[tokio::test]
    async fn test_no_op_locator() {
        let locator = NoOpShardLocator;

        let locations = locator.get_shard_locations("bucket", "key", None).await.unwrap();
        assert!(locations.is_empty());

        let objects = locator.get_objects_on_node("node-1").await.unwrap();
        assert!(objects.is_empty());
    }

    #[tokio::test]
    async fn test_no_op_repairer() {
        let repairer = NoOpShardRepairer;

        let task = ShardRepairTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "failed-node".to_string(),
            vec![],
        );

        let result = repairer.repair_shard(&task, "target").await;
        assert!(result.is_ok());

        let target = repairer.select_target_node(&task, &["node-1".to_string()]).await;
        assert_eq!(target.unwrap(), "node-1");

        let target = repairer.select_target_node(&task, &[]).await;
        assert!(target.is_err());
    }

    #[tokio::test]
    async fn test_schedule_multiple_repairs() {
        let config = RepairConfig::default();
        let manager = RepairManager::new(config);

        for i in 0..5 {
            let task = ShardRepairTask::new(
                "bucket".to_string(),
                format!("key-{i}"),
                "failed-node".to_string(),
                vec!["node-1".to_string()],
            );
            manager.schedule_repair(task);
        }

        assert_eq!(manager.pending_count(), 5);
    }

    #[test]
    fn test_repair_result_variants() {
        let success = RepairResult::Success {
            task_id: "task-1".to_string(),
            target_node: "node-1".to_string(),
            duration: Duration::from_secs(1),
        };

        match success {
            RepairResult::Success { task_id, .. } => assert_eq!(task_id, "task-1"),
            _ => panic!("Expected Success"),
        }

        let failed = RepairResult::Failed {
            task_id: "task-2".to_string(),
            error: "test error".to_string(),
            retriable: true,
        };

        match failed {
            RepairResult::Failed { retriable, .. } => assert!(retriable),
            _ => panic!("Expected Failed"),
        }

        let insufficient = RepairResult::InsufficientReplicas {
            task_id: "task-3".to_string(),
            available: 0,
            required: 1,
        };

        match insufficient {
            RepairResult::InsufficientReplicas { available, required, .. } => {
                assert_eq!(available, 0);
                assert_eq!(required, 1);
            }
            _ => panic!("Expected InsufficientReplicas"),
        }
    }
}
