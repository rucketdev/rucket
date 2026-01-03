//! Placement policy abstraction for data distribution.
//!
//! This module provides the `PlacementPolicy` trait and implementations for
//! determining where objects should be stored in the cluster.
//!
//! # Phase 1 (Single Node)
//!
//! In Phase 1, we use `SingleNodePlacement` which always returns placement
//! group 0 and the local node. This provides forward compatibility for when
//! distributed placement (CRUSH algorithm) is added in Phase 3.
//!
//! # Future Phases
//!
//! Phase 3 will introduce `CRUSHPlacement` implementing the CRUSH algorithm
//! for deterministic, failure-domain-aware data placement across nodes.

use std::fmt;
use std::sync::Arc;

/// Unique identifier for a node in the cluster.
///
/// In Phase 1, there's only one node (local). In distributed mode,
/// this will contain the node's address and unique ID.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId {
    /// Unique identifier for this node (e.g., UUID or hostname).
    pub id: String,
    /// Network address (host:port) for inter-node communication.
    pub address: String,
}

impl NodeId {
    /// Creates a new NodeId.
    #[must_use]
    pub fn new(id: impl Into<String>, address: impl Into<String>) -> Self {
        Self { id: id.into(), address: address.into() }
    }

    /// Creates the local node identifier for single-node deployment.
    #[must_use]
    pub fn local() -> Self {
        Self { id: "local".to_string(), address: "127.0.0.1:0".to_string() }
    }

    /// Returns true if this is the local node.
    #[must_use]
    pub fn is_local(&self) -> bool {
        self.id == "local"
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.address)
    }
}

/// Result of a placement computation.
///
/// Contains the placement group and the nodes responsible for storing
/// the data (primary and replicas).
#[derive(Debug, Clone)]
pub struct PlacementResult {
    /// Placement group ID for this object.
    /// PGs partition the key space for efficient management.
    pub placement_group: u32,
    /// The primary node responsible for this object.
    pub primary_node: NodeId,
    /// Replica nodes for durability (empty in single-node mode).
    pub replica_nodes: Vec<NodeId>,
}

impl PlacementResult {
    /// Creates a new placement result.
    #[must_use]
    pub fn new(placement_group: u32, primary_node: NodeId, replica_nodes: Vec<NodeId>) -> Self {
        Self { placement_group, primary_node, replica_nodes }
    }

    /// Creates a single-node placement result (PG=0, local node, no replicas).
    #[must_use]
    pub fn single_node() -> Self {
        Self { placement_group: 0, primary_node: NodeId::local(), replica_nodes: Vec::new() }
    }

    /// Returns all nodes involved (primary + replicas).
    #[must_use]
    pub fn all_nodes(&self) -> Vec<&NodeId> {
        let mut nodes = vec![&self.primary_node];
        nodes.extend(self.replica_nodes.iter());
        nodes
    }
}

/// Trait for placement policies that determine where data should be stored.
///
/// Implementations map (bucket, key) pairs to placement groups and nodes.
/// This abstraction enables swapping single-node placement for distributed
/// placement (CRUSH) without changing the storage layer.
pub trait PlacementPolicy: Send + Sync + fmt::Debug {
    /// Computes the placement for an object.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `key` - The object key
    ///
    /// # Returns
    ///
    /// A `PlacementResult` containing the placement group and responsible nodes.
    fn compute_placement(&self, bucket: &str, key: &str) -> PlacementResult;

    /// Gets the nodes responsible for a placement group.
    ///
    /// This is used for routing requests and replication.
    ///
    /// # Arguments
    ///
    /// * `pg` - The placement group ID
    ///
    /// # Returns
    ///
    /// The list of nodes responsible for this PG (primary first, then replicas).
    fn get_nodes_for_pg(&self, pg: u32) -> Vec<NodeId>;

    /// Returns the total number of placement groups.
    ///
    /// In single-node mode, this is 1. In distributed mode, this is
    /// configurable (e.g., 256, 1024).
    fn pg_count(&self) -> u32;
}

/// Single-node placement policy for Phase 1.
///
/// Always returns placement group 0 and the local node.
/// This is the default for single-node deployments.
#[derive(Debug, Clone, Default)]
pub struct SingleNodePlacement;

impl SingleNodePlacement {
    /// Creates a new single-node placement policy.
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl PlacementPolicy for SingleNodePlacement {
    fn compute_placement(&self, _bucket: &str, _key: &str) -> PlacementResult {
        PlacementResult::single_node()
    }

    fn get_nodes_for_pg(&self, _pg: u32) -> Vec<NodeId> {
        vec![NodeId::local()]
    }

    fn pg_count(&self) -> u32 {
        1
    }
}

/// Type alias for a shared placement policy.
pub type SharedPlacementPolicy = Arc<dyn PlacementPolicy>;

/// Creates a default placement policy for single-node deployment.
#[must_use]
pub fn default_placement_policy() -> SharedPlacementPolicy {
    Arc::new(SingleNodePlacement::new())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_id_local() {
        let node = NodeId::local();
        assert!(node.is_local());
        assert_eq!(node.id, "local");
    }

    #[test]
    fn test_node_id_display() {
        let node = NodeId::new("node-1", "192.168.1.10:9000");
        assert_eq!(format!("{node}"), "node-1@192.168.1.10:9000");
    }

    #[test]
    fn test_placement_result_single_node() {
        let result = PlacementResult::single_node();
        assert_eq!(result.placement_group, 0);
        assert!(result.primary_node.is_local());
        assert!(result.replica_nodes.is_empty());
    }

    #[test]
    fn test_placement_result_all_nodes() {
        let primary = NodeId::new("node-1", "host1:9000");
        let replicas =
            vec![NodeId::new("node-2", "host2:9000"), NodeId::new("node-3", "host3:9000")];
        let result = PlacementResult::new(42, primary.clone(), replicas.clone());

        let all = result.all_nodes();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "node-1");
        assert_eq!(all[1].id, "node-2");
        assert_eq!(all[2].id, "node-3");
    }

    #[test]
    fn test_single_node_placement_always_pg0() {
        let policy = SingleNodePlacement::new();

        // Test multiple buckets and keys
        let result1 = policy.compute_placement("bucket1", "key1");
        let result2 = policy.compute_placement("bucket2", "some/deep/path/file.txt");
        let result3 = policy.compute_placement("another-bucket", "");

        assert_eq!(result1.placement_group, 0);
        assert_eq!(result2.placement_group, 0);
        assert_eq!(result3.placement_group, 0);

        assert!(result1.primary_node.is_local());
        assert!(result2.primary_node.is_local());
        assert!(result3.primary_node.is_local());
    }

    #[test]
    fn test_single_node_placement_pg_count() {
        let policy = SingleNodePlacement::new();
        assert_eq!(policy.pg_count(), 1);
    }

    #[test]
    fn test_single_node_placement_get_nodes_for_pg() {
        let policy = SingleNodePlacement::new();
        let nodes = policy.get_nodes_for_pg(0);
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].is_local());

        // Even non-existent PGs return local node (single-node mode)
        let nodes = policy.get_nodes_for_pg(999);
        assert_eq!(nodes.len(), 1);
        assert!(nodes[0].is_local());
    }

    #[test]
    fn test_default_placement_policy() {
        let policy = default_placement_policy();
        let result = policy.compute_placement("bucket", "key");
        assert_eq!(result.placement_group, 0);
        assert!(result.primary_node.is_local());
    }
}
