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
