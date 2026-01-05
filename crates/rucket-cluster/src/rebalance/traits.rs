// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Traits for placement computation and shard migration.

use std::collections::HashMap;

use async_trait::async_trait;

use super::task::{RebalanceTask, ShardInfo};

/// Trait for computing optimal shard placement.
#[async_trait]
pub trait PlacementComputer: Send + Sync {
    /// Computes the ideal node for a shard.
    async fn compute_placement(
        &self,
        bucket: &str,
        key: &str,
        shard_index: Option<u32>,
        available_nodes: &[String],
    ) -> Result<String, String>;

    /// Returns all shards and their current/ideal placements.
    async fn get_all_shard_placements(&self) -> Result<Vec<ShardInfo>, String>;

    /// Returns the current distribution of shards per node.
    async fn get_shard_distribution(&self) -> Result<HashMap<String, usize>, String>;
}

/// Trait for executing shard migrations.
#[async_trait]
pub trait ShardMover: Send + Sync {
    /// Migrates a shard from source to target node.
    async fn migrate_shard(&self, task: &RebalanceTask) -> Result<u64, String>;

    /// Checks if a node is available for migration.
    async fn is_node_available(&self, node_id: &str) -> bool;

    /// Deletes a shard from the source after successful migration.
    async fn delete_source_shard(&self, task: &RebalanceTask) -> Result<(), String>;
}

/// A no-op placement computer for testing.
pub struct NoOpPlacementComputer;

#[async_trait]
impl PlacementComputer for NoOpPlacementComputer {
    async fn compute_placement(
        &self,
        _bucket: &str,
        _key: &str,
        _shard_index: Option<u32>,
        available_nodes: &[String],
    ) -> Result<String, String> {
        available_nodes.first().cloned().ok_or_else(|| "No available nodes".to_string())
    }

    async fn get_all_shard_placements(&self) -> Result<Vec<ShardInfo>, String> {
        Ok(vec![])
    }

    async fn get_shard_distribution(&self) -> Result<HashMap<String, usize>, String> {
        Ok(HashMap::new())
    }
}

/// A no-op shard mover for testing.
pub struct NoOpShardMover;

#[async_trait]
impl ShardMover for NoOpShardMover {
    async fn migrate_shard(&self, task: &RebalanceTask) -> Result<u64, String> {
        Ok(task.size)
    }

    async fn is_node_available(&self, _node_id: &str) -> bool {
        true
    }

    async fn delete_source_shard(&self, _task: &RebalanceTask) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_no_op_placement_computer() {
        let computer = NoOpPlacementComputer;

        let nodes = vec!["node-1".to_string(), "node-2".to_string()];
        let result = computer.compute_placement("bucket", "key", None, &nodes).await;
        assert_eq!(result.unwrap(), "node-1");

        let result = computer.compute_placement("bucket", "key", None, &[]).await;
        assert!(result.is_err());

        let placements = computer.get_all_shard_placements().await.unwrap();
        assert!(placements.is_empty());

        let distribution = computer.get_shard_distribution().await.unwrap();
        assert!(distribution.is_empty());
    }

    #[tokio::test]
    async fn test_no_op_shard_mover() {
        let mover = NoOpShardMover;

        let task = RebalanceTask::new(
            "bucket".to_string(),
            "key".to_string(),
            "source".to_string(),
            "target".to_string(),
            1024,
        );

        let bytes = mover.migrate_shard(&task).await.unwrap();
        assert_eq!(bytes, 1024);

        assert!(mover.is_node_available("any-node").await);
        assert!(mover.delete_source_shard(&task).await.is_ok());
    }
}
