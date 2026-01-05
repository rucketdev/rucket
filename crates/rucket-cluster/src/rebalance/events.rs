// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Events and results for rebalancing operations.

use std::collections::HashSet;
use std::time::Duration;

use super::task::RebalanceTask;

/// Result of a migration operation.
#[derive(Debug, Clone)]
pub enum MigrationResult {
    /// Migration completed successfully.
    Success {
        /// The task that was migrated.
        task_id: String,
        /// Bytes transferred.
        bytes_transferred: u64,
        /// Duration of the migration.
        duration: Duration,
    },
    /// Migration failed.
    Failed {
        /// The task that failed.
        task_id: String,
        /// Error message.
        error: String,
        /// Whether the task should be retried.
        retriable: bool,
    },
    /// Source node unavailable.
    SourceUnavailable {
        /// The task that couldn't be migrated.
        task_id: String,
        /// Source node ID.
        source_node: String,
    },
    /// Target node unavailable.
    TargetUnavailable {
        /// The task that couldn't be migrated.
        task_id: String,
        /// Target node ID.
        target_node: String,
    },
}

/// A plan for rebalancing shards across the cluster.
#[derive(Debug, Clone, Default)]
pub struct RebalancePlan {
    /// Shards to migrate.
    pub migrations: Vec<RebalanceTask>,
    /// Total bytes to transfer.
    pub total_bytes: u64,
    /// Number of shards to move.
    pub shard_count: usize,
    /// Nodes involved in the rebalance.
    pub affected_nodes: HashSet<String>,
    /// Reason for the rebalance.
    pub reason: RebalanceReason,
}

/// Reason for triggering a rebalance.
#[derive(Debug, Clone, Default)]
pub enum RebalanceReason {
    /// A new node joined the cluster.
    NodeJoined {
        /// The node that joined.
        node_id: String,
    },
    /// A node left the cluster.
    NodeLeft {
        /// The node that left.
        node_id: String,
    },
    /// Manual rebalance triggered by operator.
    Manual,
    /// Periodic rebalance to fix imbalance.
    #[default]
    Periodic,
}

/// Events emitted by the rebalance manager.
#[derive(Debug, Clone)]
pub enum RebalanceEvent {
    /// A rebalance plan was created.
    PlanCreated {
        /// Number of migrations in the plan.
        migration_count: usize,
        /// Total bytes to transfer.
        total_bytes: u64,
        /// Reason for rebalance.
        reason: String,
    },
    /// A migration task was scheduled.
    TaskScheduled {
        /// Task ID.
        task_id: String,
        /// Source node.
        source_node: String,
        /// Target node.
        target_node: String,
    },
    /// A migration task started.
    TaskStarted {
        /// Task ID.
        task_id: String,
    },
    /// A migration task completed.
    TaskCompleted {
        /// Task ID.
        task_id: String,
        /// Result.
        result: MigrationResult,
    },
    /// Rebalance completed.
    RebalanceCompleted {
        /// Number of successful migrations.
        successful: usize,
        /// Number of failed migrations.
        failed: usize,
        /// Total bytes transferred.
        bytes_transferred: u64,
        /// Total duration.
        duration: Duration,
    },
    /// Rebalance status update.
    Status {
        /// Number of pending migrations.
        pending: usize,
        /// Number of in-progress migrations.
        in_progress: usize,
        /// Number of completed migrations.
        completed: usize,
        /// Number of failed migrations.
        failed: usize,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_migration_result_variants() {
        let success = MigrationResult::Success {
            task_id: "task-1".to_string(),
            bytes_transferred: 1024,
            duration: Duration::from_secs(1),
        };

        match success {
            MigrationResult::Success { bytes_transferred, .. } => {
                assert_eq!(bytes_transferred, 1024);
            }
            _ => panic!("Expected Success"),
        }

        let failed = MigrationResult::Failed {
            task_id: "task-2".to_string(),
            error: "test error".to_string(),
            retriable: true,
        };

        match failed {
            MigrationResult::Failed { retriable, .. } => assert!(retriable),
            _ => panic!("Expected Failed"),
        }

        let source_unavailable = MigrationResult::SourceUnavailable {
            task_id: "task-3".to_string(),
            source_node: "node-1".to_string(),
        };

        match source_unavailable {
            MigrationResult::SourceUnavailable { source_node, .. } => {
                assert_eq!(source_node, "node-1");
            }
            _ => panic!("Expected SourceUnavailable"),
        }
    }

    #[test]
    fn test_rebalance_plan_default() {
        let plan = RebalancePlan::default();
        assert!(plan.migrations.is_empty());
        assert_eq!(plan.total_bytes, 0);
        assert_eq!(plan.shard_count, 0);
        assert!(plan.affected_nodes.is_empty());
    }

    #[test]
    fn test_rebalance_reason_variants() {
        let join = RebalanceReason::NodeJoined { node_id: "node-1".to_string() };
        match join {
            RebalanceReason::NodeJoined { node_id } => assert_eq!(node_id, "node-1"),
            _ => panic!("Expected NodeJoined"),
        }

        let leave = RebalanceReason::NodeLeft { node_id: "node-2".to_string() };
        match leave {
            RebalanceReason::NodeLeft { node_id } => assert_eq!(node_id, "node-2"),
            _ => panic!("Expected NodeLeft"),
        }
    }
}
