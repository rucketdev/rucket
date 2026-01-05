//! Error types for replication operations.

use thiserror::Error;

/// Result type for replication operations.
pub type Result<T> = std::result::Result<T, ReplicationError>;

/// Errors that can occur during replication.
#[derive(Error, Debug, Clone)]
pub enum ReplicationError {
    /// Failed to connect to a replica node.
    #[error("failed to connect to replica {node_id}: {reason}")]
    ConnectionFailed {
        /// The node that failed to connect.
        node_id: String,
        /// The reason for failure.
        reason: String,
    },

    /// Replication to a replica timed out.
    #[error("replication to {node_id} timed out after {timeout_ms}ms")]
    Timeout {
        /// The node that timed out.
        node_id: String,
        /// The timeout duration in milliseconds.
        timeout_ms: u64,
    },

    /// Failed to achieve required quorum for durable writes.
    #[error("quorum not achieved: required {required}, got {achieved}")]
    QuorumNotAchieved {
        /// Number of acknowledgments required.
        required: usize,
        /// Number of acknowledgments actually received.
        achieved: usize,
    },

    /// A replica rejected the write due to version conflict.
    #[error(
        "version conflict on {node_id}: local version {local_version}, incoming {incoming_version}"
    )]
    VersionConflict {
        /// The node with the conflict.
        node_id: String,
        /// The local version on the replica.
        local_version: u64,
        /// The incoming version being replicated.
        incoming_version: u64,
    },

    /// The replication queue is full.
    #[error("replication queue full: {pending_items} items pending")]
    QueueFull {
        /// Number of items pending in the queue.
        pending_items: usize,
    },

    /// Replica is too far behind (lag exceeded threshold).
    #[error("replica {node_id} lag {lag_ms}ms exceeds threshold {threshold_ms}ms")]
    LagExceeded {
        /// The lagging node.
        node_id: String,
        /// Current lag in milliseconds.
        lag_ms: u64,
        /// Maximum allowed lag in milliseconds.
        threshold_ms: u64,
    },

    /// The replica is currently unavailable.
    #[error("replica {node_id} is unavailable")]
    ReplicaUnavailable {
        /// The unavailable node.
        node_id: String,
    },

    /// Internal replication error.
    #[error("internal replication error: {0}")]
    Internal(String),
}
