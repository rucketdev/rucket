//! Background scrubbing for data integrity verification.
//!
//! This module provides automatic background scrubbing to detect silent data corruption:
//! - Periodically scans all stored shards
//! - Verifies checksums to detect bit rot
//! - Reports corrupted data for repair
//! - Integrates with RepairManager for automatic recovery
//!
//! # Architecture
//!
//! The scrubbing system operates continuously:
//! 1. **Scheduling**: ScrubManager schedules shards for verification
//! 2. **Verification**: DataValidator computes and compares checksums
//! 3. **Reporting**: Corrupted shards are reported for repair
//!
//! # Example
//!
//! ```ignore
//! use rucket_cluster::{ScrubManager, ScrubConfig};
//!
//! let config = ScrubConfig::default();
//! let mut manager = ScrubManager::new(config);
//!
//! // Start the scrub loop
//! manager.start(data_validator, repair_manager).await;
//! ```

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tracing::{error, info, trace, warn};

/// Configuration for the scrub manager.
#[derive(Debug, Clone)]
pub struct ScrubConfig {
    /// Interval between scrub cycles (how often to rescan all data).
    pub scrub_interval: Duration,

    /// Maximum concurrent scrub operations.
    pub max_concurrent_scrubs: usize,

    /// Timeout for individual shard verification.
    pub verification_timeout: Duration,

    /// Interval between progress checks.
    pub check_interval: Duration,

    /// Maximum shards to verify per second (rate limiting).
    pub max_verifications_per_second: u32,

    /// Whether to enable automatic scrubbing.
    pub auto_scrub: bool,

    /// Whether to automatically schedule repairs for corrupted data.
    pub auto_repair: bool,

    /// Delay between completing one full scrub and starting the next.
    pub cycle_delay: Duration,
}

impl Default for ScrubConfig {
    fn default() -> Self {
        Self {
            scrub_interval: Duration::from_secs(7 * 24 * 60 * 60), // 7 days
            max_concurrent_scrubs: 2,
            verification_timeout: Duration::from_secs(60),
            check_interval: Duration::from_secs(5),
            max_verifications_per_second: 100,
            auto_scrub: true,
            auto_repair: true,
            cycle_delay: Duration::from_secs(60 * 60), // 1 hour
        }
    }
}

/// Information about a shard to be scrubbed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScrubTarget {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID if versioned.
    pub version_id: Option<String>,
    /// Shard index (for erasure coded objects).
    pub shard_index: Option<u32>,
    /// Node hosting this shard.
    pub node_id: String,
    /// Expected checksum.
    pub expected_checksum: String,
    /// Size in bytes.
    pub size: u64,
}

/// A task representing a shard verification.
#[derive(Debug, Clone)]
pub struct ScrubTask {
    /// Unique identifier for the scrub task.
    pub task_id: String,
    /// Target shard to verify.
    pub target: ScrubTarget,
    /// When the scrub was scheduled.
    pub scheduled_at: Instant,
    /// Number of retry attempts.
    pub retry_count: u32,
}

impl ScrubTask {
    /// Creates a new scrub task.
    pub fn new(target: ScrubTarget) -> Self {
        Self {
            task_id: uuid::Uuid::new_v4().to_string(),
            target,
            scheduled_at: Instant::now(),
            retry_count: 0,
        }
    }
}

/// Result of a scrub verification.
#[derive(Debug, Clone)]
pub enum ScrubResult {
    /// Data is valid, checksum matches.
    Valid {
        /// The task that was verified.
        task_id: String,
        /// Duration of the verification.
        duration: Duration,
    },
    /// Data is corrupted, checksum mismatch.
    Corrupted {
        /// The task that was verified.
        task_id: String,
        /// Expected checksum.
        expected: String,
        /// Actual checksum computed.
        actual: String,
    },
    /// Data is missing.
    Missing {
        /// The task that was verified.
        task_id: String,
    },
    /// Verification failed.
    Error {
        /// The task that failed.
        task_id: String,
        /// Error message.
        error: String,
        /// Whether the task should be retried.
        retriable: bool,
    },
}

/// Trait for validating data integrity.
#[async_trait]
pub trait DataValidator: Send + Sync {
    /// Computes the checksum of a shard.
    async fn compute_checksum(&self, target: &ScrubTarget) -> Result<String, String>;

    /// Checks if a shard exists.
    async fn shard_exists(&self, target: &ScrubTarget) -> bool;

    /// Returns all shards to scrub.
    async fn get_all_shards(&self) -> Result<Vec<ScrubTarget>, String>;

    /// Returns shards on a specific node.
    async fn get_shards_on_node(&self, node_id: &str) -> Result<Vec<ScrubTarget>, String>;
}

/// Trait for handling corrupted data.
#[async_trait]
pub trait CorruptionHandler: Send + Sync {
    /// Called when corruption is detected.
    async fn on_corruption(&self, target: &ScrubTarget, expected: &str, actual: &str);

    /// Called when data is missing.
    async fn on_missing(&self, target: &ScrubTarget);
}

/// Events emitted by the scrub manager.
#[derive(Debug, Clone)]
pub enum ScrubEvent {
    /// A scrub cycle started.
    CycleStarted {
        /// Total shards to verify.
        total_shards: usize,
    },
    /// A scrub task was scheduled.
    TaskScheduled {
        /// Task ID.
        task_id: String,
        /// Bucket.
        bucket: String,
        /// Key.
        key: String,
    },
    /// A scrub task completed.
    TaskCompleted {
        /// Task ID.
        task_id: String,
        /// Result.
        result: ScrubResult,
    },
    /// Corruption was detected.
    CorruptionDetected {
        /// Task ID.
        task_id: String,
        /// Bucket.
        bucket: String,
        /// Key.
        key: String,
        /// Node where corruption was found.
        node_id: String,
    },
    /// A scrub cycle completed.
    CycleCompleted {
        /// Number of shards verified.
        verified: usize,
        /// Number of valid shards.
        valid: usize,
        /// Number of corrupted shards.
        corrupted: usize,
        /// Number of missing shards.
        missing: usize,
        /// Number of errors.
        errors: usize,
        /// Duration of the cycle.
        duration: Duration,
    },
    /// Scrub progress update.
    Progress {
        /// Number of pending verifications.
        pending: usize,
        /// Number of in-progress verifications.
        in_progress: usize,
        /// Number of completed verifications.
        completed: usize,
        /// Percentage complete.
        percent_complete: f64,
    },
}

/// Status of a scrub task.
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

/// Statistics for the current scrub cycle.
#[derive(Debug, Clone, Default)]
pub struct ScrubStats {
    /// Total shards to verify in this cycle.
    pub total_shards: usize,
    /// Number of valid shards.
    pub valid_count: usize,
    /// Number of corrupted shards.
    pub corrupted_count: usize,
    /// Number of missing shards.
    pub missing_count: usize,
    /// Number of errors.
    pub error_count: usize,
    /// Bytes verified.
    pub bytes_verified: u64,
    /// When the cycle started.
    pub cycle_start: Option<Instant>,
}

/// Manages background scrubbing operations.
pub struct ScrubManager {
    config: ScrubConfig,
    /// Pending scrub tasks, keyed by task_id.
    pending_tasks: Arc<DashMap<String, ScrubTask>>,
    /// Tasks currently being verified.
    in_progress: Arc<DashMap<String, ScrubTask>>,
    /// Current cycle statistics.
    stats: Arc<RwLock<ScrubStats>>,
    /// Nodes currently being scrubbed (reserved for future node-level scrub coordination).
    #[allow(dead_code)]
    nodes_in_progress: Arc<RwLock<HashSet<String>>>,
    /// Whether a scrub cycle is in progress.
    cycle_active: Arc<std::sync::atomic::AtomicBool>,
    /// Last completed cycle time.
    last_cycle_completed: Arc<RwLock<Option<Instant>>>,
    /// Event broadcaster.
    event_tx: broadcast::Sender<ScrubEvent>,
    /// Shutdown channel.
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl ScrubManager {
    /// Creates a new scrub manager.
    pub fn new(config: ScrubConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            config,
            pending_tasks: Arc::new(DashMap::new()),
            in_progress: Arc::new(DashMap::new()),
            stats: Arc::new(RwLock::new(ScrubStats::default())),
            nodes_in_progress: Arc::new(RwLock::new(HashSet::new())),
            cycle_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            last_cycle_completed: Arc::new(RwLock::new(None)),
            event_tx,
            shutdown_tx: None,
        }
    }

    /// Subscribes to scrub events.
    pub fn subscribe(&self) -> broadcast::Receiver<ScrubEvent> {
        self.event_tx.subscribe()
    }

    /// Schedules a scrub task.
    pub fn schedule_scrub(&self, task: ScrubTask) {
        let task_id = task.task_id.clone();
        let bucket = task.target.bucket.clone();
        let key = task.target.key.clone();

        self.pending_tasks.insert(task_id.clone(), task);
        counter!("rucket_scrub_tasks_scheduled").increment(1);
        gauge!("rucket_scrub_pending_tasks").set(self.pending_tasks.len() as f64);

        let _ = self.event_tx.send(ScrubEvent::TaskScheduled { task_id, bucket, key });

        trace!(pending = self.pending_tasks.len(), "Scheduled scrub task");
    }

    /// Starts a new scrub cycle.
    pub async fn start_cycle<V: DataValidator>(&self, validator: &V) -> Result<usize, String> {
        if self.cycle_active.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err("Scrub cycle already in progress".to_string());
        }

        // Get all shards to scrub
        let shards = validator.get_all_shards().await?;
        let count = shards.len();

        info!(shard_count = count, "Starting scrub cycle");

        // Reset stats
        {
            let mut stats = self.stats.write().await;
            *stats = ScrubStats {
                total_shards: count,
                cycle_start: Some(Instant::now()),
                ..Default::default()
            };
        }

        let _ = self.event_tx.send(ScrubEvent::CycleStarted { total_shards: count });

        // Schedule all shards for verification
        for shard in shards {
            let task = ScrubTask::new(shard);
            self.schedule_scrub(task);
        }

        counter!("rucket_scrub_cycles_started").increment(1);

        Ok(count)
    }

    /// Starts the scrub manager.
    pub async fn start<V: DataValidator + 'static, H: CorruptionHandler + 'static>(
        &mut self,
        validator: Arc<V>,
        handler: Arc<H>,
    ) {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let config = self.config.clone();
        let pending_tasks = Arc::clone(&self.pending_tasks);
        let in_progress = Arc::clone(&self.in_progress);
        let stats = Arc::clone(&self.stats);
        let cycle_active = Arc::clone(&self.cycle_active);
        let last_cycle_completed = Arc::clone(&self.last_cycle_completed);
        let event_tx = self.event_tx.clone();

        // Main scrub loop
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config.check_interval);
            let semaphore = Arc::new(Semaphore::new(config.max_concurrent_scrubs));
            let mut last_cycle_start: Option<Instant> = None;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check if we should start a new cycle
                        let should_start = if config.auto_scrub {
                            let last = last_cycle_completed.read().await;
                            match *last {
                                None => last_cycle_start.is_none(),
                                Some(completed) => {
                                    completed.elapsed() >= config.scrub_interval
                                        && !cycle_active.load(std::sync::atomic::Ordering::SeqCst)
                                }
                            }
                        } else {
                            false
                        };

                        if should_start {
                            last_cycle_start = Some(Instant::now());
                            if let Ok(count) = Self::start_cycle_static(
                                validator.as_ref(),
                                &pending_tasks,
                                &stats,
                                &cycle_active,
                                &event_tx,
                            ).await {
                                info!(shard_count = count, "Started new scrub cycle");
                            }
                        }

                        // Process verification queue
                        Self::process_verification_queue(
                            &config,
                            &pending_tasks,
                            &in_progress,
                            &stats,
                            &cycle_active,
                            &last_cycle_completed,
                            &event_tx,
                            &validator,
                            &handler,
                            &semaphore,
                        ).await;
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Scrub manager shutting down");
                        break;
                    }
                }
            }
        });

        info!(
            max_concurrent = self.config.max_concurrent_scrubs,
            check_interval_ms = self.config.check_interval.as_millis(),
            auto_scrub = self.config.auto_scrub,
            "Scrub manager started"
        );
    }

    /// Static version of start_cycle for use in spawned tasks.
    async fn start_cycle_static<V: DataValidator>(
        validator: &V,
        pending_tasks: &DashMap<String, ScrubTask>,
        stats: &RwLock<ScrubStats>,
        cycle_active: &std::sync::atomic::AtomicBool,
        event_tx: &broadcast::Sender<ScrubEvent>,
    ) -> Result<usize, String> {
        if cycle_active.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err("Scrub cycle already in progress".to_string());
        }

        let shards = validator.get_all_shards().await?;
        let count = shards.len();

        {
            let mut s = stats.write().await;
            *s = ScrubStats {
                total_shards: count,
                cycle_start: Some(Instant::now()),
                ..Default::default()
            };
        }

        let _ = event_tx.send(ScrubEvent::CycleStarted { total_shards: count });

        for shard in shards {
            let task = ScrubTask::new(shard);
            let task_id = task.task_id.clone();
            let bucket = task.target.bucket.clone();
            let key = task.target.key.clone();

            pending_tasks.insert(task_id.clone(), task);
            let _ = event_tx.send(ScrubEvent::TaskScheduled { task_id, bucket, key });
        }

        counter!("rucket_scrub_cycles_started").increment(1);
        gauge!("rucket_scrub_pending_tasks").set(pending_tasks.len() as f64);

        Ok(count)
    }

    /// Processes the verification queue.
    #[allow(clippy::too_many_arguments)]
    async fn process_verification_queue<
        V: DataValidator + 'static,
        H: CorruptionHandler + 'static,
    >(
        config: &ScrubConfig,
        pending_tasks: &Arc<DashMap<String, ScrubTask>>,
        in_progress: &Arc<DashMap<String, ScrubTask>>,
        stats: &Arc<RwLock<ScrubStats>>,
        cycle_active: &Arc<std::sync::atomic::AtomicBool>,
        last_cycle_completed: &Arc<RwLock<Option<Instant>>>,
        event_tx: &broadcast::Sender<ScrubEvent>,
        validator: &Arc<V>,
        handler: &Arc<H>,
        semaphore: &Arc<Semaphore>,
    ) {
        // Update metrics
        gauge!("rucket_scrub_pending_tasks").set(pending_tasks.len() as f64);
        gauge!("rucket_scrub_in_progress_tasks").set(in_progress.len() as f64);

        // Emit progress
        let current_stats = stats.read().await.clone();
        let completed = current_stats.valid_count
            + current_stats.corrupted_count
            + current_stats.missing_count
            + current_stats.error_count;
        let percent = if current_stats.total_shards > 0 {
            (completed as f64 / current_stats.total_shards as f64) * 100.0
        } else {
            0.0
        };

        let _ = event_tx.send(ScrubEvent::Progress {
            pending: pending_tasks.len(),
            in_progress: in_progress.len(),
            completed,
            percent_complete: percent,
        });

        // Check if cycle is complete
        if pending_tasks.is_empty()
            && in_progress.is_empty()
            && cycle_active.load(std::sync::atomic::Ordering::SeqCst)
        {
            cycle_active.store(false, std::sync::atomic::Ordering::SeqCst);
            *last_cycle_completed.write().await = Some(Instant::now());

            let duration = current_stats.cycle_start.map(|s| s.elapsed()).unwrap_or_default();

            let _ = event_tx.send(ScrubEvent::CycleCompleted {
                verified: current_stats.total_shards,
                valid: current_stats.valid_count,
                corrupted: current_stats.corrupted_count,
                missing: current_stats.missing_count,
                errors: current_stats.error_count,
                duration,
            });

            counter!("rucket_scrub_cycles_completed").increment(1);
            histogram!("rucket_scrub_cycle_duration_seconds").record(duration.as_secs_f64());

            info!(
                valid = current_stats.valid_count,
                corrupted = current_stats.corrupted_count,
                missing = current_stats.missing_count,
                errors = current_stats.error_count,
                duration_secs = duration.as_secs(),
                "Scrub cycle completed"
            );

            return;
        }

        // Find tasks ready for verification
        let ready_tasks: Vec<(String, ScrubTask)> = pending_tasks
            .iter()
            .filter(|entry| entry.retry_count < 3)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .take(config.max_concurrent_scrubs - in_progress.len())
            .collect();

        if ready_tasks.is_empty() {
            return;
        }

        for (task_id, task) in ready_tasks {
            // Try to acquire semaphore
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    trace!("Max concurrent scrubs reached");
                    break;
                }
            };

            // Move from pending to in_progress
            pending_tasks.remove(&task_id);
            in_progress.insert(task_id.clone(), task.clone());

            // Spawn verification task
            let validator = Arc::clone(validator);
            let handler = Arc::clone(handler);
            let in_progress = Arc::clone(in_progress);
            let stats = Arc::clone(stats);
            let event_tx = event_tx.clone();
            let timeout = config.verification_timeout;
            let auto_repair = config.auto_repair;
            let pending_tasks = Arc::clone(pending_tasks);
            let retry_count = task.retry_count;

            tokio::spawn(async move {
                let start = Instant::now();
                let target = task.target.clone();

                // Check if shard exists
                let exists = validator.shard_exists(&target).await;
                if !exists {
                    in_progress.remove(&task_id);

                    {
                        let mut s = stats.write().await;
                        s.missing_count += 1;
                    }

                    counter!("rucket_scrub_missing").increment(1);

                    if auto_repair {
                        handler.on_missing(&target).await;
                    }

                    let _ = event_tx.send(ScrubEvent::TaskCompleted {
                        task_id: task_id.clone(),
                        result: ScrubResult::Missing { task_id },
                    });

                    drop(permit);
                    return;
                }

                // Compute checksum
                let result =
                    tokio::time::timeout(timeout, validator.compute_checksum(&target)).await;

                let duration = start.elapsed();
                in_progress.remove(&task_id);

                let scrub_result = match result {
                    Ok(Ok(actual_checksum)) => {
                        if actual_checksum == target.expected_checksum {
                            {
                                let mut s = stats.write().await;
                                s.valid_count += 1;
                                s.bytes_verified += target.size;
                            }

                            counter!("rucket_scrub_valid").increment(1);
                            histogram!("rucket_scrub_verification_seconds")
                                .record(duration.as_secs_f64());

                            trace!(
                                task_id = %task_id,
                                duration_ms = duration.as_millis(),
                                "Shard verified successfully"
                            );

                            ScrubResult::Valid { task_id: task_id.clone(), duration }
                        } else {
                            {
                                let mut s = stats.write().await;
                                s.corrupted_count += 1;
                            }

                            counter!("rucket_scrub_corrupted").increment(1);

                            error!(
                                task_id = %task_id,
                                bucket = %target.bucket,
                                key = %target.key,
                                node_id = %target.node_id,
                                expected = %target.expected_checksum,
                                actual = %actual_checksum,
                                "Corruption detected"
                            );

                            if auto_repair {
                                handler
                                    .on_corruption(
                                        &target,
                                        &target.expected_checksum,
                                        &actual_checksum,
                                    )
                                    .await;
                            }

                            let _ = event_tx.send(ScrubEvent::CorruptionDetected {
                                task_id: task_id.clone(),
                                bucket: target.bucket.clone(),
                                key: target.key.clone(),
                                node_id: target.node_id.clone(),
                            });

                            ScrubResult::Corrupted {
                                task_id: task_id.clone(),
                                expected: target.expected_checksum.clone(),
                                actual: actual_checksum,
                            }
                        }
                    }
                    Ok(Err(e)) => {
                        {
                            let mut s = stats.write().await;
                            s.error_count += 1;
                        }

                        counter!("rucket_scrub_errors").increment(1);

                        warn!(task_id = %task_id, error = %e, "Verification error");

                        let retriable = retry_count < 3;
                        if retriable {
                            let mut retry_task = task;
                            retry_task.retry_count += 1;
                            pending_tasks.insert(task_id.clone(), retry_task);
                        }

                        ScrubResult::Error { task_id: task_id.clone(), error: e, retriable }
                    }
                    Err(_) => {
                        {
                            let mut s = stats.write().await;
                            s.error_count += 1;
                        }

                        counter!("rucket_scrub_timeout").increment(1);

                        warn!(task_id = %task_id, "Verification timed out");

                        ScrubResult::Error {
                            task_id: task_id.clone(),
                            error: "Verification timed out".to_string(),
                            retriable: retry_count < 3,
                        }
                    }
                };

                let _ = event_tx.send(ScrubEvent::TaskCompleted { task_id, result: scrub_result });

                drop(permit);
            });
        }
    }

    /// Triggers an immediate scrub cycle.
    pub async fn trigger_scrub<V: DataValidator>(&self, validator: &V) -> Result<usize, String> {
        self.start_cycle(validator).await
    }

    /// Stops the scrub manager.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Returns the number of pending verifications.
    pub fn pending_count(&self) -> usize {
        self.pending_tasks.len()
    }

    /// Returns the number of in-progress verifications.
    pub fn in_progress_count(&self) -> usize {
        self.in_progress.len()
    }

    /// Returns the current cycle statistics.
    pub async fn stats(&self) -> ScrubStats {
        self.stats.read().await.clone()
    }

    /// Returns whether a scrub cycle is active.
    pub fn is_cycle_active(&self) -> bool {
        self.cycle_active.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the last completed cycle time.
    pub async fn last_cycle_completed(&self) -> Option<Instant> {
        *self.last_cycle_completed.read().await
    }

    /// Returns the configuration.
    pub fn config(&self) -> &ScrubConfig {
        &self.config
    }
}

/// A no-op data validator for testing.
pub struct NoOpDataValidator;

#[async_trait]
impl DataValidator for NoOpDataValidator {
    async fn compute_checksum(&self, target: &ScrubTarget) -> Result<String, String> {
        Ok(target.expected_checksum.clone())
    }

    async fn shard_exists(&self, _target: &ScrubTarget) -> bool {
        true
    }

    async fn get_all_shards(&self) -> Result<Vec<ScrubTarget>, String> {
        Ok(vec![])
    }

    async fn get_shards_on_node(&self, _node_id: &str) -> Result<Vec<ScrubTarget>, String> {
        Ok(vec![])
    }
}

/// A no-op corruption handler for testing.
pub struct NoOpCorruptionHandler;

#[async_trait]
impl CorruptionHandler for NoOpCorruptionHandler {
    async fn on_corruption(&self, target: &ScrubTarget, expected: &str, actual: &str) {
        warn!(
            bucket = %target.bucket,
            key = %target.key,
            expected = %expected,
            actual = %actual,
            "Corruption detected (no-op handler)"
        );
    }

    async fn on_missing(&self, target: &ScrubTarget) {
        warn!(
            bucket = %target.bucket,
            key = %target.key,
            "Missing shard detected (no-op handler)"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scrub_config_defaults() {
        let config = ScrubConfig::default();
        assert_eq!(config.scrub_interval, Duration::from_secs(7 * 24 * 60 * 60));
        assert_eq!(config.max_concurrent_scrubs, 2);
        assert!(config.auto_scrub);
        assert!(config.auto_repair);
    }

    #[test]
    fn test_scrub_target_creation() {
        let target = ScrubTarget {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            shard_index: Some(0),
            node_id: "node-1".to_string(),
            expected_checksum: "abc123".to_string(),
            size: 1024,
        };

        assert_eq!(target.bucket, "bucket");
        assert_eq!(target.key, "key");
        assert_eq!(target.shard_index, Some(0));
        assert_eq!(target.expected_checksum, "abc123");
    }

    #[test]
    fn test_scrub_task_creation() {
        let target = ScrubTarget {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            shard_index: None,
            node_id: "node-1".to_string(),
            expected_checksum: "checksum".to_string(),
            size: 2048,
        };

        let task = ScrubTask::new(target);

        assert_eq!(task.target.bucket, "bucket");
        assert_eq!(task.target.key, "key");
        assert_eq!(task.retry_count, 0);
    }

    #[tokio::test]
    async fn test_scrub_manager_creation() {
        let config = ScrubConfig::default();
        let manager = ScrubManager::new(config);

        assert_eq!(manager.pending_count(), 0);
        assert_eq!(manager.in_progress_count(), 0);
        assert!(!manager.is_cycle_active());
    }

    #[tokio::test]
    async fn test_schedule_scrub() {
        let config = ScrubConfig::default();
        let manager = ScrubManager::new(config);

        let target = ScrubTarget {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            shard_index: None,
            node_id: "node-1".to_string(),
            expected_checksum: "checksum".to_string(),
            size: 1024,
        };

        let task = ScrubTask::new(target);
        manager.schedule_scrub(task);

        assert_eq!(manager.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let config = ScrubConfig::default();
        let manager = ScrubManager::new(config);
        let mut rx = manager.subscribe();

        let target = ScrubTarget {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            shard_index: None,
            node_id: "node-1".to_string(),
            expected_checksum: "checksum".to_string(),
            size: 1024,
        };

        let task = ScrubTask::new(target);
        manager.schedule_scrub(task);

        // Should receive TaskScheduled event
        let event = rx.try_recv().unwrap();
        match event {
            ScrubEvent::TaskScheduled { bucket, key, .. } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(key, "key");
            }
            _ => panic!("Expected TaskScheduled event"),
        }
    }

    #[tokio::test]
    async fn test_no_op_validator() {
        let validator = NoOpDataValidator;

        let target = ScrubTarget {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            shard_index: None,
            node_id: "node-1".to_string(),
            expected_checksum: "checksum".to_string(),
            size: 1024,
        };

        let checksum = validator.compute_checksum(&target).await.unwrap();
        assert_eq!(checksum, "checksum");

        assert!(validator.shard_exists(&target).await);

        let shards = validator.get_all_shards().await.unwrap();
        assert!(shards.is_empty());

        let node_shards = validator.get_shards_on_node("node-1").await.unwrap();
        assert!(node_shards.is_empty());
    }

    #[tokio::test]
    async fn test_no_op_corruption_handler() {
        let handler = NoOpCorruptionHandler;

        let target = ScrubTarget {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            shard_index: None,
            node_id: "node-1".to_string(),
            expected_checksum: "expected".to_string(),
            size: 1024,
        };

        // Should not panic
        handler.on_corruption(&target, "expected", "actual").await;
        handler.on_missing(&target).await;
    }

    #[test]
    fn test_scrub_result_variants() {
        let valid =
            ScrubResult::Valid { task_id: "task-1".to_string(), duration: Duration::from_secs(1) };

        match valid {
            ScrubResult::Valid { task_id, .. } => assert_eq!(task_id, "task-1"),
            _ => panic!("Expected Valid"),
        }

        let corrupted = ScrubResult::Corrupted {
            task_id: "task-2".to_string(),
            expected: "abc".to_string(),
            actual: "xyz".to_string(),
        };

        match corrupted {
            ScrubResult::Corrupted { expected, actual, .. } => {
                assert_eq!(expected, "abc");
                assert_eq!(actual, "xyz");
            }
            _ => panic!("Expected Corrupted"),
        }

        let missing = ScrubResult::Missing { task_id: "task-3".to_string() };

        match missing {
            ScrubResult::Missing { task_id } => assert_eq!(task_id, "task-3"),
            _ => panic!("Expected Missing"),
        }

        let error = ScrubResult::Error {
            task_id: "task-4".to_string(),
            error: "test error".to_string(),
            retriable: true,
        };

        match error {
            ScrubResult::Error { retriable, .. } => assert!(retriable),
            _ => panic!("Expected Error"),
        }
    }

    #[tokio::test]
    async fn test_scrub_stats_default() {
        let stats = ScrubStats::default();
        assert_eq!(stats.total_shards, 0);
        assert_eq!(stats.valid_count, 0);
        assert_eq!(stats.corrupted_count, 0);
        assert_eq!(stats.missing_count, 0);
        assert_eq!(stats.error_count, 0);
        assert_eq!(stats.bytes_verified, 0);
        assert!(stats.cycle_start.is_none());
    }

    #[tokio::test]
    async fn test_schedule_multiple_scrubs() {
        let config = ScrubConfig::default();
        let manager = ScrubManager::new(config);

        for i in 0..5 {
            let target = ScrubTarget {
                bucket: "bucket".to_string(),
                key: format!("key-{i}"),
                version_id: None,
                shard_index: None,
                node_id: "node-1".to_string(),
                expected_checksum: format!("checksum-{i}"),
                size: 1024,
            };
            let task = ScrubTask::new(target);
            manager.schedule_scrub(task);
        }

        assert_eq!(manager.pending_count(), 5);
    }

    #[tokio::test]
    async fn test_start_cycle_empty() {
        let config = ScrubConfig::default();
        let manager = ScrubManager::new(config);
        let validator = NoOpDataValidator;

        let count = manager.start_cycle(&validator).await.unwrap();
        assert_eq!(count, 0);
        assert!(manager.is_cycle_active());
    }

    #[tokio::test]
    async fn test_start_cycle_already_active() {
        let config = ScrubConfig::default();
        let manager = ScrubManager::new(config);
        let validator = NoOpDataValidator;

        // Start first cycle
        manager.start_cycle(&validator).await.unwrap();

        // Try to start second cycle - should fail
        let result = manager.start_cycle(&validator).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already in progress"));
    }
}
