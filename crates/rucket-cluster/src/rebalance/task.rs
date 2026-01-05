// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Task and shard types for rebalancing.

use std::time::Instant;

use serde::{Deserialize, Serialize};

/// A task representing a shard migration.
#[derive(Debug, Clone)]
pub struct RebalanceTask {
    /// Unique identifier for the migration task.
    pub task_id: String,
    /// Bucket containing the object.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID if versioned.
    pub version_id: Option<String>,
    /// Shard index (for erasure coded objects).
    pub shard_index: Option<u32>,
    /// Source node (current location).
    pub source_node: String,
    /// Target node (destination).
    pub target_node: String,
    /// Size in bytes.
    pub size: u64,
    /// Priority (lower = higher priority).
    pub priority: u32,
    /// When the migration was scheduled.
    pub scheduled_at: Instant,
    /// Number of retry attempts.
    pub retry_count: u32,
}

impl RebalanceTask {
    /// Creates a new rebalance task.
    pub fn new(
        bucket: String,
        key: String,
        source_node: String,
        target_node: String,
        size: u64,
    ) -> Self {
        Self {
            task_id: uuid::Uuid::new_v4().to_string(),
            bucket,
            key,
            version_id: None,
            shard_index: None,
            source_node,
            target_node,
            size,
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

/// Information about a shard for rebalancing purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardInfo {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID if versioned.
    pub version_id: Option<String>,
    /// Shard index (for erasure coded objects).
    pub shard_index: Option<u32>,
    /// Size in bytes.
    pub size: u64,
    /// Current node hosting this shard.
    pub current_node: String,
    /// Ideal node according to placement policy.
    pub ideal_node: String,
}

/// Status of a migration task.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_task_creation() {
        let task = RebalanceTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "source-node".to_string(),
            "target-node".to_string(),
            1024,
        );

        assert_eq!(task.bucket, "bucket");
        assert_eq!(task.key, "key");
        assert_eq!(task.source_node, "source-node");
        assert_eq!(task.target_node, "target-node");
        assert_eq!(task.size, 1024);
        assert_eq!(task.retry_count, 0);
    }

    #[test]
    fn test_rebalance_task_with_shard() {
        let task = RebalanceTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "source".to_string(),
            "target".to_string(),
            1024,
        )
        .with_shard_index(5)
        .with_version("v1".to_string())
        .with_priority(10);

        assert_eq!(task.shard_index, Some(5));
        assert_eq!(task.version_id, Some("v1".to_string()));
        assert_eq!(task.priority, 10);
    }
}
