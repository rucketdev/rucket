//! Placement policy abstraction for data distribution.
//!
//! This module provides the `PlacementPolicy` trait and implementations for
//! determining where objects should be stored in the cluster.
//!
//! # Implementations
//!
//! - `SingleNodePlacement`: Always returns the local node (single-node deployment)
//! - `CrushPlacementPolicy`: CRUSH algorithm for distributed placement
//!
//! # Usage
//!
//! ```ignore
//! use rucket_storage::placement::{PlacementPolicy, CrushPlacementPolicy};
//! use rucket_placement::{ClusterMap, PlacementConfig};
//!
//! // Create a cluster map with devices
//! let map = ClusterMap::new();
//! // ... add devices and buckets ...
//!
//! // Create CRUSH placement policy
//! let policy = CrushPlacementPolicy::new(map, config, node_registry);
//! let result = policy.compute_placement("bucket", "key");
//! ```

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, RwLock};

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

/// Registry mapping CRUSH device IDs to NodeIds.
///
/// This provides the mapping between the abstract device IDs used in CRUSH
/// and the actual network addresses of nodes.
#[derive(Debug, Clone, Default)]
pub struct NodeRegistry {
    /// Maps CRUSH device ID to NodeId.
    devices: HashMap<i32, NodeId>,
}

impl NodeRegistry {
    /// Creates a new empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self { devices: HashMap::new() }
    }

    /// Registers a device with its NodeId.
    pub fn register(&mut self, device_id: i32, node: NodeId) {
        self.devices.insert(device_id, node);
    }

    /// Gets the NodeId for a device ID.
    #[must_use]
    pub fn get(&self, device_id: i32) -> Option<&NodeId> {
        self.devices.get(&device_id)
    }

    /// Returns the number of registered devices.
    #[must_use]
    pub fn len(&self) -> usize {
        self.devices.len()
    }

    /// Returns true if no devices are registered.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.devices.is_empty()
    }
}

/// CRUSH-based placement policy for distributed storage.
///
/// Uses the CRUSH algorithm to deterministically place objects across
/// multiple nodes while respecting failure domains.
pub struct CrushPlacementPolicy {
    /// The underlying CRUSH placement engine.
    crush: RwLock<rucket_placement::CrushPlacement>,
    /// Registry mapping device IDs to node addresses.
    registry: RwLock<NodeRegistry>,
    /// Number of placement groups.
    pg_count: u32,
}

impl CrushPlacementPolicy {
    /// Creates a new CRUSH placement policy.
    ///
    /// # Arguments
    ///
    /// * `cluster_map` - The cluster topology
    /// * `config` - Placement configuration
    /// * `registry` - Mapping from device IDs to node addresses
    #[must_use]
    pub fn new(
        cluster_map: rucket_placement::ClusterMap,
        config: rucket_placement::PlacementConfig,
        registry: NodeRegistry,
    ) -> Self {
        let pg_count = config.pg_count;
        Self {
            crush: RwLock::new(rucket_placement::CrushPlacement::new(cluster_map, config)),
            registry: RwLock::new(registry),
            pg_count,
        }
    }

    /// Updates the cluster map.
    ///
    /// This is used when the cluster topology changes (nodes added/removed).
    pub fn update_cluster_map(&self, cluster_map: rucket_placement::ClusterMap) {
        let mut crush = self.crush.write().unwrap();
        let config = crush.config().clone();
        *crush = rucket_placement::CrushPlacement::new(cluster_map, config);
    }

    /// Updates the node registry.
    pub fn update_registry(&self, registry: NodeRegistry) {
        let mut reg = self.registry.write().unwrap();
        *reg = registry;
    }

    /// Registers a new device.
    pub fn register_device(&self, device_id: i32, node: NodeId) {
        let mut reg = self.registry.write().unwrap();
        reg.register(device_id, node);
    }

    /// Converts CRUSH device IDs to NodeIds.
    fn device_ids_to_nodes(&self, device_ids: &[i32]) -> Vec<NodeId> {
        let registry = self.registry.read().unwrap();
        device_ids.iter().filter_map(|&id| registry.get(id).cloned()).collect()
    }
}

impl fmt::Debug for CrushPlacementPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CrushPlacementPolicy").field("pg_count", &self.pg_count).finish()
    }
}

impl PlacementPolicy for CrushPlacementPolicy {
    fn compute_placement(&self, bucket: &str, key: &str) -> PlacementResult {
        let crush = self.crush.read().unwrap();
        let result = crush.place(bucket, key);

        let all_osds = result.all_osds();
        let nodes = self.device_ids_to_nodes(&all_osds);

        if nodes.is_empty() {
            // Fallback to local if no nodes found
            return PlacementResult::single_node();
        }

        let primary = nodes[0].clone();
        let replicas: Vec<NodeId> = nodes.into_iter().skip(1).collect();

        PlacementResult::new(result.pg_id, primary, replicas)
    }

    fn get_nodes_for_pg(&self, pg: u32) -> Vec<NodeId> {
        let crush = self.crush.read().unwrap();
        let result = crush.place_pg(pg);
        self.device_ids_to_nodes(&result.all_osds())
    }

    fn pg_count(&self) -> u32 {
        self.pg_count
    }
}

/// Builder for creating a CRUSH placement policy.
///
/// Provides a convenient way to set up cluster topology and configuration.
#[derive(Debug)]
pub struct CrushPlacementBuilder {
    cluster_map: rucket_placement::ClusterMap,
    config: rucket_placement::PlacementConfig,
    registry: NodeRegistry,
}

impl Default for CrushPlacementBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CrushPlacementBuilder {
    /// Creates a new builder with default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cluster_map: rucket_placement::ClusterMap::new(),
            config: rucket_placement::PlacementConfig::default(),
            registry: NodeRegistry::new(),
        }
    }

    /// Sets the number of placement groups.
    #[must_use]
    pub fn pg_count(mut self, count: u32) -> Self {
        self.config.pg_count = count;
        self
    }

    /// Sets the number of replicas.
    #[must_use]
    pub fn replica_count(mut self, count: usize) -> Self {
        self.config.replica_count = count;
        self
    }

    /// Adds a device to the cluster.
    ///
    /// # Arguments
    ///
    /// * `device_id` - Unique device ID (must be >= 0)
    /// * `name` - Human-readable name
    /// * `weight` - Weight for placement (higher = more data)
    /// * `address` - Network address (host:port)
    pub fn add_device(
        mut self,
        device_id: i32,
        name: impl Into<String>,
        weight: f64,
        address: impl Into<String>,
    ) -> Self {
        let name = name.into();
        let address = address.into();

        // Add to cluster map
        let device = rucket_placement::DeviceInfo::new(device_id, &name, weight);
        let _ = self.cluster_map.add_device(device);

        // Add to registry
        self.registry.register(device_id, NodeId::new(&name, address));

        self
    }

    /// Adds a host bucket containing devices.
    pub fn add_host(mut self, name: impl Into<String>, device_ids: Vec<i32>) -> Self {
        let _ = self.cluster_map.add_bucket_with_domain(
            name,
            rucket_placement::BucketType::Straw2,
            rucket_placement::bucket::FailureDomain::Host,
            device_ids,
        );
        self
    }

    /// Adds a rack bucket containing hosts.
    pub fn add_rack(mut self, name: impl Into<String>, host_names: Vec<&str>) -> Self {
        let _ = self.cluster_map.add_parent_bucket(
            name,
            rucket_placement::BucketType::Straw2,
            rucket_placement::bucket::FailureDomain::Rack,
            host_names,
        );
        self
    }

    /// Adds a zone bucket containing racks.
    pub fn add_zone(mut self, name: impl Into<String>, rack_names: Vec<&str>) -> Self {
        let _ = self.cluster_map.add_parent_bucket(
            name,
            rucket_placement::BucketType::Straw2,
            rucket_placement::bucket::FailureDomain::Zone,
            rack_names,
        );
        self
    }

    /// Sets the root bucket.
    pub fn set_root(mut self, name: &str) -> Self {
        let _ = self.cluster_map.set_root(name);
        self
    }

    /// Builds the CRUSH placement policy.
    #[must_use]
    pub fn build(self) -> CrushPlacementPolicy {
        CrushPlacementPolicy::new(self.cluster_map, self.config, self.registry)
    }

    /// Builds the policy as a shared placement policy.
    #[must_use]
    pub fn build_shared(self) -> SharedPlacementPolicy {
        Arc::new(self.build())
    }
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

    #[test]
    fn test_node_registry() {
        let mut registry = NodeRegistry::new();
        assert!(registry.is_empty());

        registry.register(0, NodeId::new("osd.0", "host1:9000"));
        registry.register(1, NodeId::new("osd.1", "host2:9000"));

        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());

        let node = registry.get(0).unwrap();
        assert_eq!(node.id, "osd.0");
        assert_eq!(node.address, "host1:9000");

        assert!(registry.get(999).is_none());
    }

    #[test]
    fn test_crush_placement_builder() {
        let policy = CrushPlacementBuilder::new()
            .pg_count(64)
            .replica_count(3)
            // Add 3 devices, one per host
            .add_device(0, "osd.0", 1.0, "host1:9000")
            .add_device(1, "osd.1", 1.0, "host2:9000")
            .add_device(2, "osd.2", 1.0, "host3:9000")
            // Create hosts
            .add_host("host1", vec![0])
            .add_host("host2", vec![1])
            .add_host("host3", vec![2])
            // Create root
            .add_rack("root", vec!["host1", "host2", "host3"])
            .set_root("root")
            .build();

        assert_eq!(policy.pg_count(), 64);
    }

    #[test]
    fn test_crush_placement_basic() {
        let policy = CrushPlacementBuilder::new()
            .pg_count(64)
            .replica_count(3)
            .add_device(0, "osd.0", 1.0, "host1:9000")
            .add_device(1, "osd.1", 1.0, "host2:9000")
            .add_device(2, "osd.2", 1.0, "host3:9000")
            .add_host("host1", vec![0])
            .add_host("host2", vec![1])
            .add_host("host3", vec![2])
            .add_rack("root", vec!["host1", "host2", "host3"])
            .set_root("root")
            .build();

        let result = policy.compute_placement("bucket", "key");

        // Should return placement with nodes
        assert!(result.placement_group < 64);
        // Primary node should be one of our devices
        assert!(["osd.0", "osd.1", "osd.2"].contains(&result.primary_node.id.as_str()));
    }

    #[test]
    fn test_crush_placement_deterministic() {
        let policy = CrushPlacementBuilder::new()
            .pg_count(64)
            .replica_count(3)
            .add_device(0, "osd.0", 1.0, "host1:9000")
            .add_device(1, "osd.1", 1.0, "host2:9000")
            .add_device(2, "osd.2", 1.0, "host3:9000")
            .add_host("host1", vec![0])
            .add_host("host2", vec![1])
            .add_host("host3", vec![2])
            .add_rack("root", vec!["host1", "host2", "host3"])
            .set_root("root")
            .build();

        // Same input should produce same output
        let result1 = policy.compute_placement("bucket", "key");
        let result2 = policy.compute_placement("bucket", "key");

        assert_eq!(result1.placement_group, result2.placement_group);
        assert_eq!(result1.primary_node.id, result2.primary_node.id);
    }

    #[test]
    fn test_crush_placement_get_nodes_for_pg() {
        let policy = CrushPlacementBuilder::new()
            .pg_count(16)
            .replica_count(2)
            .add_device(0, "osd.0", 1.0, "host1:9000")
            .add_device(1, "osd.1", 1.0, "host2:9000")
            .add_host("host1", vec![0])
            .add_host("host2", vec![1])
            .add_rack("root", vec!["host1", "host2"])
            .set_root("root")
            .build();

        let nodes = policy.get_nodes_for_pg(0);

        // Should return nodes
        assert!(!nodes.is_empty());
        // All nodes should have valid addresses
        for node in &nodes {
            assert!(node.address.contains(":9000"));
        }
    }

    #[test]
    fn test_crush_placement_shared() {
        let policy: SharedPlacementPolicy = CrushPlacementBuilder::new()
            .pg_count(16)
            .replica_count(1)
            .add_device(0, "osd.0", 1.0, "host1:9000")
            .add_host("host1", vec![0])
            .add_rack("root", vec!["host1"])
            .set_root("root")
            .build_shared();

        let result = policy.compute_placement("bucket", "key");
        assert!(result.placement_group < 16);
    }

    #[test]
    fn test_crush_placement_distribution() {
        let policy = CrushPlacementBuilder::new()
            .pg_count(256)
            .replica_count(1)
            .add_device(0, "osd.0", 1.0, "host1:9000")
            .add_device(1, "osd.1", 1.0, "host2:9000")
            .add_device(2, "osd.2", 1.0, "host3:9000")
            .add_host("host1", vec![0])
            .add_host("host2", vec![1])
            .add_host("host3", vec![2])
            .add_rack("root", vec!["host1", "host2", "host3"])
            .set_root("root")
            .build();

        // Count how many times each OSD is primary
        let mut counts = std::collections::HashMap::new();
        for i in 0..300 {
            let result = policy.compute_placement("bucket", &format!("key{i}"));
            *counts.entry(result.primary_node.id.clone()).or_insert(0) += 1;
        }

        // All 3 OSDs should be used
        assert!(counts.len() >= 2, "Not all OSDs used: {:?}", counts);
    }
}
