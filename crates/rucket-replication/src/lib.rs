//! Primary-backup replication for Rucket distributed storage.
//!
//! This crate provides replication functionality with configurable durability levels:
//!
//! - **Local**: Write acknowledged after local persistence only (fastest, single-node)
//! - **Replicated**: Async replication to backups (balanced, eventual consistency)
//! - **Durable**: Sync replication with quorum acknowledgment (strongest, consistent)
//!
//! # Architecture
//!
//! The replication system follows a primary-backup model where:
//! 1. All writes go to the primary node first
//! 2. The primary replicates to backup nodes based on the replication level
//! 3. Acknowledgment depends on the level (immediate, async, or quorum)
//!
//! ```text
//! Client Write
//!      │
//!      ▼
//! ┌─────────┐
//! │ Primary │──────────────────────────────────────┐
//! └────┬────┘                                      │
//!      │                                           │
//!      ├── Local Level: Return immediately        │
//!      │                                           │
//!      ├── Replicated Level:                       │
//!      │   Queue for async replication ──────────►│
//!      │   Return immediately                      │
//!      │                                           │
//!      └── Durable Level:                          │
//!          Send to all replicas ─────────────────►│
//!          Wait for quorum acks                    │
//!          Return after quorum                     │
//!                                                  │
//!                                                  ▼
//!                                          ┌─────────────┐
//!                                          │   Backups   │
//!                                          │ (RF-1 nodes)│
//!                                          └─────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use rucket_replication::{ReplicationConfig, ReplicationLevel, SyncReplicator};
//!
//! // Create a sync replicator with RF=3
//! let config = ReplicationConfig::new()
//!     .replication_factor(3)
//!     .default_level(ReplicationLevel::Durable);
//!
//! let replicator = SyncReplicator::new(config);
//!
//! // Add replica clients
//! replicator.add_replica(replica1_client);
//! replicator.add_replica(replica2_client);
//!
//! // Replicate an entry
//! let entry = ReplicationEntry::new(
//!     ReplicationOperation::CreateBucket { bucket: "mybucket".to_string() },
//!     hlc_timestamp,
//!     ReplicationLevel::Durable,
//! );
//!
//! let result = replicator.replicate(entry).await?;
//! assert!(result.quorum_achieved);
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

mod async_replicator;
mod config;
mod error;
mod lag;
mod level;
mod replicator;
mod sync_replicator;

// Re-export async replicator types
pub use async_replicator::{AsyncReplicator, ReplicaClient};
// Re-export configuration
pub use config::{
    ConfigValidationError, ReplicationConfig, DEFAULT_BATCH_SIZE, DEFAULT_LAG_CHECK_INTERVAL_MS,
    DEFAULT_LAG_THRESHOLD_MS, DEFAULT_QUEUE_SIZE, DEFAULT_REPLICATION_FACTOR, DEFAULT_TIMEOUT_MS,
};
// Re-export error types
pub use error::{ReplicationError, Result};
// Re-export lag monitoring
pub use lag::{LagInfo, LagTracker};
// Re-export replication levels
pub use level::{ParseReplicationLevelError, ReplicationLevel};
// Re-export core replicator types
pub use replicator::{
    NoOpReplicator, ReplicaInfo, ReplicationEntry, ReplicationId, ReplicationOperation,
    ReplicationResult, Replicator,
};
// Re-export sync replicator
pub use sync_replicator::SyncReplicator;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exports() {
        // Verify all public types are accessible
        let _level = ReplicationLevel::default();
        let _config = ReplicationConfig::default();

        assert_eq!(_level, ReplicationLevel::Replicated);
        assert_eq!(_config.replication_factor, DEFAULT_REPLICATION_FACTOR);
    }

    #[test]
    fn test_replication_level_serialization() {
        let level = ReplicationLevel::Durable;
        let json = serde_json::to_string(&level).unwrap();
        assert_eq!(json, "\"durable\"");

        let parsed: ReplicationLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, ReplicationLevel::Durable);
    }

    #[test]
    fn test_config_builder() {
        let config = ReplicationConfig::new()
            .enabled(true)
            .replication_factor(5)
            .default_level(ReplicationLevel::Durable);

        assert!(config.enabled);
        assert_eq!(config.replication_factor, 5);
        assert_eq!(config.default_level, ReplicationLevel::Durable);
        assert_eq!(config.quorum_size(), 3); // 5/2 + 1
    }

    #[tokio::test]
    async fn test_noop_replicator() {
        let replicator = NoOpReplicator;

        let entry = ReplicationEntry::new(
            ReplicationOperation::CreateBucket { bucket: "test".to_string() },
            12345,
            ReplicationLevel::Durable,
        );

        let result = replicator.replicate(entry).await.unwrap();
        assert!(result.quorum_achieved);
        assert_eq!(result.acks_received, 1);

        let replicas = replicator.get_replicas().await;
        assert!(replicas.is_empty());
    }

    #[test]
    fn test_error_display() {
        let error = ReplicationError::QuorumNotAchieved { required: 3, achieved: 1 };
        assert_eq!(error.to_string(), "quorum not achieved: required 3, got 1");

        let error = ReplicationError::Timeout { node_id: "node1".to_string(), timeout_ms: 5000 };
        assert_eq!(error.to_string(), "replication to node1 timed out after 5000ms");
    }

    #[test]
    fn test_lag_tracker_basic() {
        let tracker = LagTracker::new();
        tracker.add_replica("node1".to_string());

        assert!(tracker.get_lag_ms("node1").is_none());

        tracker.record_success("node1", 100);
        assert!(tracker.get_lag_ms("node1").is_some());
        assert_eq!(tracker.get_last_hlc("node1"), Some(100));

        tracker.record_failure("node1");
        assert_eq!(tracker.get_failure_count("node1"), Some(1));
    }
}
