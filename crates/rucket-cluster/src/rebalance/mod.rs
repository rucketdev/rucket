// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Rebalancing module for shard redistribution on cluster membership changes.
//!
//! This module provides automatic rebalancing when nodes join or leave:
//! - Monitors HeartbeatManager events for membership changes
//! - Computes optimal shard distribution using placement policy
//! - Coordinates shard migration between nodes
//! - Supports rate limiting and prioritization
//!
//! # Architecture
//!
//! The rebalancing system operates in three phases:
//! 1. **Detection**: HeartbeatManager detects node join/leave
//! 2. **Planning**: RebalanceManager computes migration plan
//! 3. **Execution**: ShardMover transfers data between nodes
//!
//! # Example
//!
//! ```ignore
//! use rucket_cluster::{RebalanceManager, RebalanceConfig};
//!
//! let config = RebalanceConfig::default();
//! let mut manager = RebalanceManager::new(config);
//!
//! // Subscribe to heartbeat events
//! let events = heartbeat_manager.subscribe();
//! manager.start(events, placement_policy, shard_mover).await;
//! ```

mod config;
mod events;
mod task;
mod traits;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

pub use config::RebalanceConfig;
use dashmap::DashMap;
pub use events::{MigrationResult, RebalanceEvent, RebalancePlan, RebalanceReason};
use metrics::{counter, gauge, histogram};
pub use task::{RebalanceTask, ShardInfo, TaskStatus};
use tokio::sync::{broadcast, mpsc, RwLock, Semaphore};
use tracing::{debug, error, info, trace, warn};
pub use traits::{NoOpPlacementComputer, NoOpShardMover, PlacementComputer, ShardMover};

use crate::heartbeat::HeartbeatEvent;

/// Manages shard rebalancing operations.
pub struct RebalanceManager {
    config: RebalanceConfig,
    /// Pending migration tasks, keyed by task_id.
    pending_tasks: Arc<DashMap<String, RebalanceTask>>,
    /// Tasks currently being migrated.
    in_progress: Arc<DashMap<String, RebalanceTask>>,
    /// Completed task count.
    completed_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Failed task count.
    failed_count: Arc<std::sync::atomic::AtomicUsize>,
    /// Total bytes transferred.
    bytes_transferred: Arc<std::sync::atomic::AtomicU64>,
    /// Current cluster members.
    cluster_members: Arc<RwLock<HashSet<String>>>,
    /// Nodes pending join (waiting for delay).
    pending_joins: Arc<DashMap<String, Instant>>,
    /// Nodes pending leave (waiting for delay).
    pending_leaves: Arc<DashMap<String, Instant>>,
    /// Whether a rebalance is in progress.
    rebalance_active: Arc<std::sync::atomic::AtomicBool>,
    /// Event broadcaster.
    event_tx: broadcast::Sender<RebalanceEvent>,
    /// Shutdown channel.
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl RebalanceManager {
    /// Creates a new rebalance manager.
    pub fn new(config: RebalanceConfig) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        Self {
            config,
            pending_tasks: Arc::new(DashMap::new()),
            in_progress: Arc::new(DashMap::new()),
            completed_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            failed_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            bytes_transferred: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            cluster_members: Arc::new(RwLock::new(HashSet::new())),
            pending_joins: Arc::new(DashMap::new()),
            pending_leaves: Arc::new(DashMap::new()),
            rebalance_active: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            event_tx,
            shutdown_tx: None,
        }
    }

    /// Subscribes to rebalance events.
    pub fn subscribe(&self) -> broadcast::Receiver<RebalanceEvent> {
        self.event_tx.subscribe()
    }

    /// Schedules a migration task.
    pub fn schedule_migration(&self, task: RebalanceTask) {
        let task_id = task.task_id.clone();
        let source = task.source_node.clone();
        let target = task.target_node.clone();

        self.pending_tasks.insert(task_id.clone(), task);
        counter!("rucket_rebalance_tasks_scheduled").increment(1);
        gauge!("rucket_rebalance_pending_tasks").set(self.pending_tasks.len() as f64);

        let _ = self.event_tx.send(RebalanceEvent::TaskScheduled {
            task_id,
            source_node: source,
            target_node: target,
        });

        debug!(pending = self.pending_tasks.len(), "Scheduled migration task");
    }

    /// Creates a rebalance plan for a node join event.
    pub async fn plan_for_node_join<P: PlacementComputer>(
        &self,
        node_id: &str,
        placement: &P,
    ) -> Result<RebalancePlan, String> {
        info!(node_id = %node_id, "Creating rebalance plan for node join");

        // Get all shards and their ideal placements
        let shards = placement.get_all_shard_placements().await?;
        let members: Vec<String> = self.cluster_members.read().await.iter().cloned().collect();

        let mut migrations = Vec::new();
        let mut total_bytes = 0u64;
        let mut affected_nodes = HashSet::new();

        for shard in shards {
            // Recompute ideal placement with new node
            let ideal = placement
                .compute_placement(&shard.bucket, &shard.key, shard.shard_index, &members)
                .await?;

            // If ideal node changed and is the new node, schedule migration
            if ideal == node_id && shard.current_node != node_id {
                let mut task = RebalanceTask::new(
                    shard.bucket.clone(),
                    shard.key.clone(),
                    shard.current_node.clone(),
                    node_id.to_string(),
                    shard.size,
                );

                if let Some(idx) = shard.shard_index {
                    task = task.with_shard_index(idx);
                }
                if let Some(ver) = shard.version_id.clone() {
                    task = task.with_version(ver);
                }

                affected_nodes.insert(shard.current_node.clone());
                affected_nodes.insert(node_id.to_string());
                total_bytes += shard.size;
                migrations.push(task);
            }
        }

        let plan = RebalancePlan {
            shard_count: migrations.len(),
            migrations,
            total_bytes,
            affected_nodes,
            reason: RebalanceReason::NodeJoined { node_id: node_id.to_string() },
        };

        info!(
            node_id = %node_id,
            migrations = plan.shard_count,
            bytes = total_bytes,
            "Created rebalance plan for node join"
        );

        Ok(plan)
    }

    /// Creates a rebalance plan for a node leave event.
    /// Note: This is different from repair - repair recreates lost data,
    /// while rebalance redistributes data that still exists elsewhere.
    pub async fn plan_for_node_leave<P: PlacementComputer>(
        &self,
        node_id: &str,
        placement: &P,
    ) -> Result<RebalancePlan, String> {
        info!(node_id = %node_id, "Creating rebalance plan for node leave");

        // Get current distribution
        let _distribution = placement.get_shard_distribution().await?;
        let members: Vec<String> = self.cluster_members.read().await.iter().cloned().collect();
        let remaining_members: Vec<String> =
            members.iter().filter(|m| *m != node_id).cloned().collect();

        if remaining_members.is_empty() {
            return Err("No remaining members to rebalance to".to_string());
        }

        // Calculate average shards per remaining node
        let shards = placement.get_all_shard_placements().await?;
        let mut migrations = Vec::new();
        let mut total_bytes = 0u64;
        let mut affected_nodes = HashSet::new();

        for shard in shards {
            // Recompute placement without the leaving node
            let ideal = placement
                .compute_placement(&shard.bucket, &shard.key, shard.shard_index, &remaining_members)
                .await?;

            // If current node is the leaving node or ideal changed
            if shard.current_node == node_id || shard.ideal_node != ideal {
                // Only migrate if there's an actual change needed
                if shard.current_node != ideal {
                    let mut task = RebalanceTask::new(
                        shard.bucket.clone(),
                        shard.key.clone(),
                        shard.current_node.clone(),
                        ideal.clone(),
                        shard.size,
                    );

                    if let Some(idx) = shard.shard_index {
                        task = task.with_shard_index(idx);
                    }
                    if let Some(ver) = shard.version_id.clone() {
                        task = task.with_version(ver);
                    }

                    affected_nodes.insert(shard.current_node.clone());
                    affected_nodes.insert(ideal);
                    total_bytes += shard.size;
                    migrations.push(task);
                }
            }
        }

        let plan = RebalancePlan {
            shard_count: migrations.len(),
            migrations,
            total_bytes,
            affected_nodes,
            reason: RebalanceReason::NodeLeft { node_id: node_id.to_string() },
        };

        info!(
            node_id = %node_id,
            migrations = plan.shard_count,
            bytes = total_bytes,
            "Created rebalance plan for node leave"
        );

        Ok(plan)
    }

    /// Executes a rebalance plan.
    pub async fn execute_plan(&self, plan: RebalancePlan) {
        if plan.migrations.is_empty() {
            info!("Rebalance plan has no migrations, skipping");
            return;
        }

        let reason = match &plan.reason {
            RebalanceReason::NodeJoined { node_id } => format!("node_joined:{}", node_id),
            RebalanceReason::NodeLeft { node_id } => format!("node_left:{}", node_id),
            RebalanceReason::Manual => "manual".to_string(),
            RebalanceReason::Periodic => "periodic".to_string(),
        };

        let _ = self.event_tx.send(RebalanceEvent::PlanCreated {
            migration_count: plan.shard_count,
            total_bytes: plan.total_bytes,
            reason,
        });

        // Schedule all migrations
        for task in plan.migrations {
            self.schedule_migration(task);
        }

        self.rebalance_active.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Adds a cluster member.
    pub async fn add_member(&self, node_id: String) {
        let mut members = self.cluster_members.write().await;
        if members.insert(node_id.clone()) {
            // Record pending join with timestamp
            self.pending_joins.insert(node_id.clone(), Instant::now());
            info!(node_id = %node_id, "Member added, pending rebalance after delay");
        }
    }

    /// Removes a cluster member.
    pub async fn remove_member(&self, node_id: &str) {
        let mut members = self.cluster_members.write().await;
        if members.remove(node_id) {
            // Record pending leave with timestamp
            self.pending_leaves.insert(node_id.to_string(), Instant::now());
            info!(node_id = %node_id, "Member removed, pending rebalance after delay");
        }
    }

    /// Starts the rebalance manager.
    pub async fn start<P: PlacementComputer + 'static, M: ShardMover + 'static>(
        &mut self,
        mut heartbeat_events: broadcast::Receiver<HeartbeatEvent>,
        placement: Arc<P>,
        mover: Arc<M>,
    ) {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let config = self.config.clone();
        let pending_tasks = Arc::clone(&self.pending_tasks);
        let in_progress = Arc::clone(&self.in_progress);
        let completed_count = Arc::clone(&self.completed_count);
        let failed_count = Arc::clone(&self.failed_count);
        let bytes_transferred = Arc::clone(&self.bytes_transferred);
        let cluster_members = Arc::clone(&self.cluster_members);
        let pending_joins = Arc::clone(&self.pending_joins);
        let pending_leaves = Arc::clone(&self.pending_leaves);
        let rebalance_active = Arc::clone(&self.rebalance_active);
        let event_tx = self.event_tx.clone();

        // Task to process heartbeat events
        let members_clone = Arc::clone(&cluster_members);
        let joins_clone = Arc::clone(&pending_joins);
        let leaves_clone = Arc::clone(&pending_leaves);

        tokio::spawn(async move {
            loop {
                match heartbeat_events.recv().await {
                    Ok(HeartbeatEvent::NodeJoined { node_id }) => {
                        let mut members = members_clone.write().await;
                        if members.insert(node_id.clone()) {
                            joins_clone.insert(node_id.clone(), Instant::now());
                            info!(node_id = %node_id, "Node joined, scheduling rebalance");
                        }
                    }
                    Ok(HeartbeatEvent::NodeLeft { node_id }) => {
                        let mut members = members_clone.write().await;
                        if members.remove(&node_id) {
                            leaves_clone.insert(node_id.clone(), Instant::now());
                            info!(node_id = %node_id, "Node left, scheduling rebalance");
                        }
                    }
                    Ok(HeartbeatEvent::NodeFailed { node_id, .. }) => {
                        // On failure, we might want to rebalance after repair completes
                        // For now, just track the leave
                        let mut members = members_clone.write().await;
                        if members.remove(&node_id) {
                            leaves_clone.insert(node_id.clone(), Instant::now());
                            warn!(node_id = %node_id, "Node failed, may need rebalance after repair");
                        }
                    }
                    Ok(HeartbeatEvent::NodeRecovered { node_id }) => {
                        // Node recovered, treat as a join
                        let mut members = members_clone.write().await;
                        if members.insert(node_id.clone()) {
                            joins_clone.insert(node_id.clone(), Instant::now());
                            info!(node_id = %node_id, "Node recovered, scheduling rebalance");
                        }
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

        // Main rebalance loop
        let config_clone = config.clone();
        let placement_clone = Arc::clone(&placement);
        let pending_joins_clone = Arc::clone(&pending_joins);
        let pending_leaves_clone = Arc::clone(&pending_leaves);
        let pending_tasks_clone = Arc::clone(&pending_tasks);
        let event_tx_clone = event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(config_clone.check_interval);
            let semaphore = Arc::new(Semaphore::new(config_clone.max_concurrent_migrations));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Check for pending joins ready for rebalance
                        let ready_joins: Vec<String> = pending_joins_clone
                            .iter()
                            .filter(|entry| entry.value().elapsed() >= config_clone.join_delay)
                            .map(|entry| entry.key().clone())
                            .collect();

                        for node_id in ready_joins {
                            pending_joins_clone.remove(&node_id);
                            if config_clone.auto_rebalance {
                                if let Ok(plan) = Self::plan_for_node_join_static(
                                    &node_id,
                                    &*placement_clone,
                                    &cluster_members,
                                ).await {
                                    Self::execute_plan_static(
                                        &plan,
                                        &pending_tasks_clone,
                                        &rebalance_active,
                                        &event_tx_clone,
                                    );
                                }
                            }
                        }

                        // Check for pending leaves ready for rebalance
                        let ready_leaves: Vec<String> = pending_leaves_clone
                            .iter()
                            .filter(|entry| entry.value().elapsed() >= config_clone.leave_delay)
                            .map(|entry| entry.key().clone())
                            .collect();

                        for node_id in ready_leaves {
                            pending_leaves_clone.remove(&node_id);
                            if config_clone.auto_rebalance {
                                if let Ok(plan) = Self::plan_for_node_leave_static(
                                    &node_id,
                                    &*placement_clone,
                                    &cluster_members,
                                ).await {
                                    Self::execute_plan_static(
                                        &plan,
                                        &pending_tasks_clone,
                                        &rebalance_active,
                                        &event_tx_clone,
                                    );
                                }
                            }
                        }

                        // Process migration queue
                        Self::process_migration_queue(
                            &config_clone,
                            &pending_tasks,
                            &in_progress,
                            &completed_count,
                            &failed_count,
                            &bytes_transferred,
                            &rebalance_active,
                            &event_tx,
                            &mover,
                            &semaphore,
                        ).await;
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Rebalance manager shutting down");
                        break;
                    }
                }
            }
        });

        info!(
            max_concurrent = self.config.max_concurrent_migrations,
            check_interval_ms = self.config.check_interval.as_millis(),
            auto_rebalance = self.config.auto_rebalance,
            "Rebalance manager started"
        );
    }

    /// Static version of plan_for_node_join for use in spawned tasks.
    async fn plan_for_node_join_static<P: PlacementComputer>(
        node_id: &str,
        placement: &P,
        cluster_members: &RwLock<HashSet<String>>,
    ) -> Result<RebalancePlan, String> {
        let members: Vec<String> = cluster_members.read().await.iter().cloned().collect();
        let shards = placement.get_all_shard_placements().await?;

        let mut migrations = Vec::new();
        let mut total_bytes = 0u64;
        let mut affected_nodes = HashSet::new();

        for shard in shards {
            let ideal = placement
                .compute_placement(&shard.bucket, &shard.key, shard.shard_index, &members)
                .await?;

            if ideal == node_id && shard.current_node != node_id {
                let mut task = RebalanceTask::new(
                    shard.bucket.clone(),
                    shard.key.clone(),
                    shard.current_node.clone(),
                    node_id.to_string(),
                    shard.size,
                );

                if let Some(idx) = shard.shard_index {
                    task = task.with_shard_index(idx);
                }
                if let Some(ver) = shard.version_id.clone() {
                    task = task.with_version(ver);
                }

                affected_nodes.insert(shard.current_node.clone());
                affected_nodes.insert(node_id.to_string());
                total_bytes += shard.size;
                migrations.push(task);
            }
        }

        Ok(RebalancePlan {
            shard_count: migrations.len(),
            migrations,
            total_bytes,
            affected_nodes,
            reason: RebalanceReason::NodeJoined { node_id: node_id.to_string() },
        })
    }

    /// Static version of plan_for_node_leave for use in spawned tasks.
    async fn plan_for_node_leave_static<P: PlacementComputer>(
        node_id: &str,
        placement: &P,
        cluster_members: &RwLock<HashSet<String>>,
    ) -> Result<RebalancePlan, String> {
        let members: Vec<String> = cluster_members.read().await.iter().cloned().collect();
        let remaining: Vec<String> = members.iter().filter(|m| *m != node_id).cloned().collect();

        if remaining.is_empty() {
            return Err("No remaining members".to_string());
        }

        let shards = placement.get_all_shard_placements().await?;
        let mut migrations = Vec::new();
        let mut total_bytes = 0u64;
        let mut affected_nodes = HashSet::new();

        for shard in shards {
            let ideal = placement
                .compute_placement(&shard.bucket, &shard.key, shard.shard_index, &remaining)
                .await?;

            // Migrate if shard is on the leaving node or if placement changed
            if shard.current_node != ideal {
                let mut task = RebalanceTask::new(
                    shard.bucket.clone(),
                    shard.key.clone(),
                    shard.current_node.clone(),
                    ideal.clone(),
                    shard.size,
                );

                if let Some(idx) = shard.shard_index {
                    task = task.with_shard_index(idx);
                }
                if let Some(ver) = shard.version_id.clone() {
                    task = task.with_version(ver);
                }

                affected_nodes.insert(shard.current_node.clone());
                affected_nodes.insert(ideal);
                total_bytes += shard.size;
                migrations.push(task);
            }
        }

        Ok(RebalancePlan {
            shard_count: migrations.len(),
            migrations,
            total_bytes,
            affected_nodes,
            reason: RebalanceReason::NodeLeft { node_id: node_id.to_string() },
        })
    }

    /// Static version of execute_plan for use in spawned tasks.
    fn execute_plan_static(
        plan: &RebalancePlan,
        pending_tasks: &DashMap<String, RebalanceTask>,
        rebalance_active: &std::sync::atomic::AtomicBool,
        event_tx: &broadcast::Sender<RebalanceEvent>,
    ) {
        if plan.migrations.is_empty() {
            return;
        }

        let reason = match &plan.reason {
            RebalanceReason::NodeJoined { node_id } => format!("node_joined:{}", node_id),
            RebalanceReason::NodeLeft { node_id } => format!("node_left:{}", node_id),
            RebalanceReason::Manual => "manual".to_string(),
            RebalanceReason::Periodic => "periodic".to_string(),
        };

        let _ = event_tx.send(RebalanceEvent::PlanCreated {
            migration_count: plan.shard_count,
            total_bytes: plan.total_bytes,
            reason,
        });

        for task in &plan.migrations {
            let task_id = task.task_id.clone();
            let source = task.source_node.clone();
            let target = task.target_node.clone();

            pending_tasks.insert(task_id.clone(), task.clone());
            counter!("rucket_rebalance_tasks_scheduled").increment(1);

            let _ = event_tx.send(RebalanceEvent::TaskScheduled {
                task_id,
                source_node: source,
                target_node: target,
            });
        }

        gauge!("rucket_rebalance_pending_tasks").set(pending_tasks.len() as f64);
        rebalance_active.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Processes the migration queue.
    #[allow(clippy::too_many_arguments)]
    async fn process_migration_queue<M: ShardMover + 'static>(
        config: &RebalanceConfig,
        pending_tasks: &Arc<DashMap<String, RebalanceTask>>,
        in_progress: &Arc<DashMap<String, RebalanceTask>>,
        completed_count: &Arc<std::sync::atomic::AtomicUsize>,
        failed_count: &Arc<std::sync::atomic::AtomicUsize>,
        bytes_transferred: &Arc<std::sync::atomic::AtomicU64>,
        rebalance_active: &Arc<std::sync::atomic::AtomicBool>,
        event_tx: &broadcast::Sender<RebalanceEvent>,
        mover: &Arc<M>,
        semaphore: &Arc<Semaphore>,
    ) {
        // Update metrics
        gauge!("rucket_rebalance_pending_tasks").set(pending_tasks.len() as f64);
        gauge!("rucket_rebalance_in_progress_tasks").set(in_progress.len() as f64);

        // Find tasks ready for migration
        let ready_tasks: Vec<(String, RebalanceTask)> = pending_tasks
            .iter()
            .filter(|entry| entry.retry_count < 3)
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .take(config.max_concurrent_migrations - in_progress.len())
            .collect();

        if ready_tasks.is_empty() {
            // Check if rebalance is complete
            if in_progress.is_empty()
                && pending_tasks.is_empty()
                && rebalance_active.load(std::sync::atomic::Ordering::SeqCst)
            {
                rebalance_active.store(false, std::sync::atomic::Ordering::SeqCst);
                let _ = event_tx.send(RebalanceEvent::RebalanceCompleted {
                    successful: completed_count.load(std::sync::atomic::Ordering::SeqCst),
                    failed: failed_count.load(std::sync::atomic::Ordering::SeqCst),
                    bytes_transferred: bytes_transferred.load(std::sync::atomic::Ordering::SeqCst),
                    duration: std::time::Duration::from_secs(0), // Would need to track start time
                });
            }
            return;
        }

        for (task_id, task) in ready_tasks {
            // Try to acquire semaphore
            let permit = match semaphore.clone().try_acquire_owned() {
                Ok(p) => p,
                Err(_) => {
                    trace!("Max concurrent migrations reached");
                    break;
                }
            };

            // Check if nodes are available
            if !mover.is_node_available(&task.source_node).await {
                let _ = event_tx.send(RebalanceEvent::TaskCompleted {
                    task_id: task_id.clone(),
                    result: MigrationResult::SourceUnavailable {
                        task_id: task_id.clone(),
                        source_node: task.source_node.clone(),
                    },
                });
                pending_tasks.remove(&task_id);
                failed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(permit);
                continue;
            }

            if !mover.is_node_available(&task.target_node).await {
                let _ = event_tx.send(RebalanceEvent::TaskCompleted {
                    task_id: task_id.clone(),
                    result: MigrationResult::TargetUnavailable {
                        task_id: task_id.clone(),
                        target_node: task.target_node.clone(),
                    },
                });
                pending_tasks.remove(&task_id);
                failed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                drop(permit);
                continue;
            }

            // Move from pending to in_progress
            pending_tasks.remove(&task_id);
            in_progress.insert(task_id.clone(), task.clone());

            let _ = event_tx.send(RebalanceEvent::TaskStarted { task_id: task_id.clone() });

            // Spawn migration task
            let mover = Arc::clone(mover);
            let in_progress = Arc::clone(in_progress);
            let completed_count = Arc::clone(completed_count);
            let failed_count = Arc::clone(failed_count);
            let bytes_transferred = Arc::clone(bytes_transferred);
            let pending_tasks = Arc::clone(pending_tasks);
            let event_tx = event_tx.clone();
            let timeout = config.migration_timeout;
            let retry_count = task.retry_count;

            tokio::spawn(async move {
                let start = Instant::now();
                let result = tokio::time::timeout(timeout, mover.migrate_shard(&task)).await;

                let duration = start.elapsed();
                in_progress.remove(&task_id);

                let migration_result = match result {
                    Ok(Ok(bytes)) => {
                        // Delete source shard after successful migration
                        if let Err(e) = mover.delete_source_shard(&task).await {
                            warn!(
                                task_id = %task_id,
                                error = %e,
                                "Failed to delete source shard after migration"
                            );
                        }

                        completed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        bytes_transferred.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
                        counter!("rucket_rebalance_completed").increment(1);
                        histogram!("rucket_rebalance_duration_seconds")
                            .record(duration.as_secs_f64());

                        info!(
                            task_id = %task_id,
                            bytes = bytes,
                            duration_ms = duration.as_millis(),
                            "Migration completed successfully"
                        );

                        MigrationResult::Success {
                            task_id: task_id.clone(),
                            bytes_transferred: bytes,
                            duration,
                        }
                    }
                    Ok(Err(e)) => {
                        failed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        counter!("rucket_rebalance_failed").increment(1);

                        error!(task_id = %task_id, error = %e, "Migration failed");

                        let retriable = retry_count < 3;
                        if retriable {
                            let mut retry_task = task;
                            retry_task.retry_count += 1;
                            pending_tasks.insert(task_id.clone(), retry_task);
                        }

                        MigrationResult::Failed { task_id: task_id.clone(), error: e, retriable }
                    }
                    Err(_) => {
                        failed_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        counter!("rucket_rebalance_timeout").increment(1);

                        error!(task_id = %task_id, "Migration timed out");

                        MigrationResult::Failed {
                            task_id: task_id.clone(),
                            error: "Migration timed out".to_string(),
                            retriable: retry_count < 3,
                        }
                    }
                };

                let _ = event_tx
                    .send(RebalanceEvent::TaskCompleted { task_id, result: migration_result });

                drop(permit);
            });
        }

        // Emit status
        let _ = event_tx.send(RebalanceEvent::Status {
            pending: pending_tasks.len(),
            in_progress: in_progress.len(),
            completed: completed_count.load(std::sync::atomic::Ordering::SeqCst),
            failed: failed_count.load(std::sync::atomic::Ordering::SeqCst),
        });
    }

    /// Stops the rebalance manager.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Returns the number of pending migrations.
    pub fn pending_count(&self) -> usize {
        self.pending_tasks.len()
    }

    /// Returns the number of in-progress migrations.
    pub fn in_progress_count(&self) -> usize {
        self.in_progress.len()
    }

    /// Returns the total number of completed migrations.
    pub fn completed_count(&self) -> usize {
        self.completed_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the total number of failed migrations.
    pub fn failed_count(&self) -> usize {
        self.failed_count.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the total bytes transferred.
    pub fn bytes_transferred(&self) -> u64 {
        self.bytes_transferred.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns whether a rebalance is currently active.
    pub fn is_active(&self) -> bool {
        self.rebalance_active.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Returns the current cluster members.
    pub async fn members(&self) -> Vec<String> {
        self.cluster_members.read().await.iter().cloned().collect()
    }

    /// Returns the configuration.
    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rebalance_manager_creation() {
        let config = RebalanceConfig::default();
        let manager = RebalanceManager::new(config);

        assert_eq!(manager.pending_count(), 0);
        assert_eq!(manager.in_progress_count(), 0);
        assert_eq!(manager.completed_count(), 0);
        assert_eq!(manager.failed_count(), 0);
        assert!(!manager.is_active());
    }

    #[tokio::test]
    async fn test_schedule_migration() {
        let config = RebalanceConfig::default();
        let manager = RebalanceManager::new(config);

        let task = RebalanceTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "source".to_string(),
            "target".to_string(),
            1024,
        );

        manager.schedule_migration(task);

        assert_eq!(manager.pending_count(), 1);
    }

    #[tokio::test]
    async fn test_add_remove_member() {
        let config = RebalanceConfig::default();
        let manager = RebalanceManager::new(config);

        manager.add_member("node-1".to_string()).await;
        manager.add_member("node-2".to_string()).await;

        let members = manager.members().await;
        assert_eq!(members.len(), 2);

        manager.remove_member("node-1").await;
        let members = manager.members().await;
        assert_eq!(members.len(), 1);
        assert!(members.contains(&"node-2".to_string()));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let config = RebalanceConfig::default();
        let manager = RebalanceManager::new(config);
        let mut rx = manager.subscribe();

        let task = RebalanceTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "source".to_string(),
            "target".to_string(),
            1024,
        );

        manager.schedule_migration(task);

        // Should receive TaskScheduled event
        let event = rx.try_recv().unwrap();
        match event {
            RebalanceEvent::TaskScheduled { source_node, target_node, .. } => {
                assert_eq!(source_node, "source");
                assert_eq!(target_node, "target");
            }
            _ => panic!("Expected TaskScheduled event"),
        }
    }

    #[tokio::test]
    async fn test_schedule_multiple_migrations() {
        let config = RebalanceConfig::default();
        let manager = RebalanceManager::new(config);

        for i in 0..5 {
            let task = RebalanceTask::new(
                "bucket".to_string(),
                format!("key-{i}"),
                "source".to_string(),
                "target".to_string(),
                1024,
            );
            manager.schedule_migration(task);
        }

        assert_eq!(manager.pending_count(), 5);
    }

    #[tokio::test]
    async fn test_execute_empty_plan() {
        let config = RebalanceConfig::default();
        let manager = RebalanceManager::new(config);

        let plan = RebalancePlan::default();
        manager.execute_plan(plan).await;

        // Should not activate rebalance for empty plan
        assert!(!manager.is_active());
    }
}
