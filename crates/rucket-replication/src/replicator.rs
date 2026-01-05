//! Core replication traits and types.
//!
//! This module defines the `Replicator` trait and associated types used
//! for replicating data to backup nodes.

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::error::{ReplicationError, Result};
use super::level::ReplicationLevel;

/// Unique identifier for a replication operation.
pub type ReplicationId = Uuid;

/// A write operation to be replicated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    /// Unique ID for this replication entry.
    pub id: ReplicationId,

    /// The type of operation.
    pub operation: ReplicationOperation,

    /// HLC timestamp from the primary.
    pub hlc_timestamp: u64,

    /// Wall-clock time when the operation occurred on primary.
    pub timestamp: DateTime<Utc>,

    /// The replication level requested.
    pub level: ReplicationLevel,
}

impl ReplicationEntry {
    /// Creates a new replication entry.
    pub fn new(
        operation: ReplicationOperation,
        hlc_timestamp: u64,
        level: ReplicationLevel,
    ) -> Self {
        Self { id: Uuid::new_v4(), operation, hlc_timestamp, timestamp: Utc::now(), level }
    }
}

/// Types of operations that can be replicated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationOperation {
    /// Put/create an object.
    PutObject {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (if versioned).
        version_id: Option<String>,
        /// Object data.
        #[serde(with = "bytes_serde")]
        data: Bytes,
        /// Content type.
        content_type: Option<String>,
        /// ETag/MD5.
        etag: String,
    },

    /// Delete an object.
    DeleteObject {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID to delete (if versioned).
        version_id: Option<String>,
        /// Whether this creates a delete marker.
        is_delete_marker: bool,
    },

    /// Create a bucket.
    CreateBucket {
        /// Bucket name.
        bucket: String,
    },

    /// Delete a bucket.
    DeleteBucket {
        /// Bucket name.
        bucket: String,
    },
}

/// Serde helper for Bytes.
mod bytes_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        bytes.as_ref().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Bytes::from(vec))
    }
}

/// Result of a replication operation.
#[derive(Debug, Clone)]
pub struct ReplicationResult {
    /// The entry that was replicated.
    pub entry_id: ReplicationId,

    /// Number of replicas that acknowledged.
    pub acks_received: usize,

    /// Number of replicas required for quorum.
    pub acks_required: usize,

    /// Whether quorum was achieved.
    pub quorum_achieved: bool,

    /// Errors from individual replicas (if any).
    pub replica_errors: Vec<(String, ReplicationError)>,
}

impl ReplicationResult {
    /// Creates a successful result with quorum achieved.
    pub fn success(entry_id: ReplicationId, acks: usize, required: usize) -> Self {
        Self {
            entry_id,
            acks_received: acks,
            acks_required: required,
            quorum_achieved: acks >= required,
            replica_errors: Vec::new(),
        }
    }

    /// Creates a result where quorum was not achieved.
    pub fn quorum_failed(
        entry_id: ReplicationId,
        acks: usize,
        required: usize,
        errors: Vec<(String, ReplicationError)>,
    ) -> Self {
        Self {
            entry_id,
            acks_received: acks,
            acks_required: required,
            quorum_achieved: false,
            replica_errors: errors,
        }
    }
}

/// Information about a replica node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicaInfo {
    /// Unique node identifier.
    pub node_id: String,

    /// Network address for replication.
    pub address: String,

    /// Whether the replica is currently healthy.
    pub is_healthy: bool,

    /// Current replication lag in milliseconds.
    pub lag_ms: u64,

    /// Last successful replication timestamp.
    pub last_replicated_at: Option<DateTime<Utc>>,
}

impl ReplicaInfo {
    /// Creates new replica info.
    pub fn new(node_id: String, address: String) -> Self {
        Self { node_id, address, is_healthy: true, lag_ms: 0, last_replicated_at: None }
    }
}

/// Trait for implementing replication to backup nodes.
///
/// This trait defines the core interface for both async and sync replication.
#[async_trait]
pub trait Replicator: Send + Sync {
    /// Replicates an entry to backup nodes.
    ///
    /// For `ReplicationLevel::Replicated`, this queues the entry for
    /// async replication and returns immediately.
    ///
    /// For `ReplicationLevel::Durable`, this waits for quorum acknowledgment
    /// before returning.
    async fn replicate(&self, entry: ReplicationEntry) -> Result<ReplicationResult>;

    /// Returns the list of known replicas.
    async fn get_replicas(&self) -> Vec<ReplicaInfo>;

    /// Returns the number of healthy replicas.
    async fn healthy_replica_count(&self) -> usize {
        self.get_replicas().await.iter().filter(|r| r.is_healthy).count()
    }

    /// Checks if quorum can be achieved with current healthy replicas.
    async fn can_achieve_quorum(&self, quorum_size: usize) -> bool {
        // +1 for the primary node
        self.healthy_replica_count().await + 1 >= quorum_size
    }
}

/// A no-op replicator for single-node deployments.
pub struct NoOpReplicator;

#[async_trait]
impl Replicator for NoOpReplicator {
    async fn replicate(&self, entry: ReplicationEntry) -> Result<ReplicationResult> {
        // No replication needed - just return success
        Ok(ReplicationResult::success(entry.id, 1, 1))
    }

    async fn get_replicas(&self) -> Vec<ReplicaInfo> {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_entry_creation() {
        let op = ReplicationOperation::CreateBucket { bucket: "test-bucket".to_string() };
        let entry = ReplicationEntry::new(op, 12345, ReplicationLevel::Durable);

        assert_eq!(entry.hlc_timestamp, 12345);
        assert_eq!(entry.level, ReplicationLevel::Durable);
    }

    #[test]
    fn test_replication_result_success() {
        let id = Uuid::new_v4();
        let result = ReplicationResult::success(id, 3, 2);

        assert!(result.quorum_achieved);
        assert_eq!(result.acks_received, 3);
        assert_eq!(result.acks_required, 2);
        assert!(result.replica_errors.is_empty());
    }

    #[test]
    fn test_replication_result_quorum_failed() {
        let id = Uuid::new_v4();
        let errors = vec![(
            "node1".to_string(),
            ReplicationError::Timeout { node_id: "node1".to_string(), timeout_ms: 5000 },
        )];
        let result = ReplicationResult::quorum_failed(id, 1, 2, errors);

        assert!(!result.quorum_achieved);
        assert_eq!(result.acks_received, 1);
        assert_eq!(result.acks_required, 2);
        assert_eq!(result.replica_errors.len(), 1);
    }

    #[test]
    fn test_replica_info() {
        let info = ReplicaInfo::new("node1".to_string(), "192.168.1.1:9000".to_string());

        assert_eq!(info.node_id, "node1");
        assert_eq!(info.address, "192.168.1.1:9000");
        assert!(info.is_healthy);
        assert_eq!(info.lag_ms, 0);
        assert!(info.last_replicated_at.is_none());
    }

    #[tokio::test]
    async fn test_noop_replicator() {
        let replicator = NoOpReplicator;

        let op = ReplicationOperation::CreateBucket { bucket: "test".to_string() };
        let entry = ReplicationEntry::new(op, 1, ReplicationLevel::Local);

        let result = replicator.replicate(entry).await.unwrap();
        assert!(result.quorum_achieved);

        let replicas = replicator.get_replicas().await;
        assert!(replicas.is_empty());

        assert_eq!(replicator.healthy_replica_count().await, 0);
        assert!(replicator.can_achieve_quorum(1).await);
    }

    #[test]
    fn test_put_object_operation() {
        let op = ReplicationOperation::PutObject {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
            version_id: Some("v1".to_string()),
            data: Bytes::from("hello world"),
            content_type: Some("text/plain".to_string()),
            etag: "abc123".to_string(),
        };

        let entry = ReplicationEntry::new(op.clone(), 100, ReplicationLevel::Replicated);

        // Test serialization
        let json = serde_json::to_string(&entry).unwrap();
        let parsed: ReplicationEntry = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.hlc_timestamp, 100);
        if let ReplicationOperation::PutObject { bucket, key, .. } = parsed.operation {
            assert_eq!(bucket, "mybucket");
            assert_eq!(key, "mykey");
        } else {
            panic!("Expected PutObject operation");
        }
    }

    #[test]
    fn test_delete_object_operation() {
        let op = ReplicationOperation::DeleteObject {
            bucket: "mybucket".to_string(),
            key: "mykey".to_string(),
            version_id: None,
            is_delete_marker: true,
        };

        let entry = ReplicationEntry::new(op, 200, ReplicationLevel::Durable);

        let json = serde_json::to_string(&entry).unwrap();
        let parsed: ReplicationEntry = serde_json::from_str(&json).unwrap();

        if let ReplicationOperation::DeleteObject { is_delete_marker, .. } = parsed.operation {
            assert!(is_delete_marker);
        } else {
            panic!("Expected DeleteObject operation");
        }
    }
}
