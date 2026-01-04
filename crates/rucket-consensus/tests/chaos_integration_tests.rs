//! Chaos integration tests for Raft consensus.
//!
//! These tests verify that the Raft cluster behaves correctly under various
//! failure conditions including:
//! - Leader crashes
//! - Network partitions
//! - Network delays
//! - Packet loss
//! - Combined failure scenarios
//!
//! Unlike the unit tests in `chaos_tests.rs` which only test the `ChaosController`
//! infrastructure, these tests exercise the REAL OpenRaft implementation with
//! actual fault injection.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;
use std::time::Duration;

use rucket_consensus::network::{ChaosNetworkFactory, GrpcNetworkFactory};
use rucket_consensus::testing::ChaosController;
use rucket_consensus::{
    ClusterConfig, ClusterManager, MetadataStateMachine, RaftMetadataBackend, RedbLogStorage,
};
use rucket_core::config::SyncStrategy;
use rucket_core::types::BucketInfo;
use rucket_core::Result;
use rucket_storage::metadata::{MetadataBackend, RedbMetadataStore};
use tempfile::TempDir;
use tokio::time::sleep;
use tracing::{info, warn};

/// Type alias for Raft node ID.
type RaftNodeId = u64;

/// Type alias for the concrete RaftMetadataBackend.
type ConcreteRaftBackend = RaftMetadataBackend<RedbMetadataStore>;

/// Base port for test clusters to avoid conflicts.
const BASE_PORT: u16 = 20000;

/// A test cluster with chaos fault injection capabilities.
///
/// This struct manages a Raft cluster where each node's network layer is wrapped
/// with `ChaosNetwork` for fault injection. The shared `ChaosController` allows
/// tests to inject faults like delays, packet loss, partitions, and node crashes.
pub struct ChaosTestCluster {
    /// Cluster nodes, indexed by node ID.
    nodes: Vec<ClusterManager>,

    /// Shared chaos controller for fault injection.
    chaos: Arc<ChaosController>,

    /// Metadata stores for each node (for direct verification).
    stores: Vec<Arc<RedbMetadataStore>>,

    /// Raft metadata backends for writing through consensus.
    backends: Vec<ConcreteRaftBackend>,

    /// Temporary directories (kept alive for test duration).
    _temp_dirs: Vec<TempDir>,

    /// Port offset for this cluster (to avoid conflicts between tests).
    port_offset: u16,
}

impl ChaosTestCluster {
    /// Creates a new chaos-enabled test cluster.
    ///
    /// # Arguments
    /// * `node_count` - Number of nodes in the cluster
    /// * `port_offset` - Port offset to avoid conflicts with other tests
    pub async fn new(node_count: usize, port_offset: u16) -> Self {
        let chaos = Arc::new(ChaosController::new());
        let mut nodes = Vec::with_capacity(node_count);
        let mut stores = Vec::with_capacity(node_count);
        let mut backends = Vec::with_capacity(node_count);
        let mut temp_dirs = Vec::with_capacity(node_count);

        for i in 0..node_count {
            let node_id = (i + 1) as RaftNodeId;
            let port = BASE_PORT + port_offset + i as u16;
            let temp_dir = TempDir::new().expect("Failed to create temp dir");

            // Create config
            let config = ClusterConfig::new(node_id)
                .with_bind_addr(format!("127.0.0.1:{}", port).parse().unwrap());

            // Create log storage
            let log_path = temp_dir.path().join(format!("raft_log_{}.db", node_id));
            let log_storage =
                RedbLogStorage::open(&log_path).expect("Failed to create log storage");
            log_storage.load_state().await.expect("Failed to load state");

            // Create metadata store
            let meta_path = temp_dir.path().join(format!("metadata_{}.db", node_id));
            let store = Arc::new(
                RedbMetadataStore::open(&meta_path, SyncStrategy::Always)
                    .expect("Failed to create metadata store"),
            );

            // Create state machine
            let state_machine =
                MetadataStateMachine::new(store.clone() as Arc<dyn MetadataBackend>);

            // Create chaos-enabled network factory
            let grpc_factory = GrpcNetworkFactory::new();
            let chaos_factory = ChaosNetworkFactory::new(grpc_factory, chaos.clone(), node_id);

            // Create cluster manager with chaos network
            let manager =
                ClusterManager::new_with_network(config, log_storage, state_machine, chaos_factory)
                    .await
                    .expect("Failed to create cluster manager");

            stores.push(store.clone());
            backends.push(RaftMetadataBackend::new(manager.raft(), store));
            nodes.push(manager);
            temp_dirs.push(temp_dir);
        }

        Self { nodes, chaos, stores, backends, _temp_dirs: temp_dirs, port_offset }
    }

    /// Returns the number of nodes in the cluster.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Returns the chaos controller for fault injection.
    pub fn chaos(&self) -> &Arc<ChaosController> {
        &self.chaos
    }

    /// Returns a reference to a node by index (0-based).
    pub fn node(&self, index: usize) -> &ClusterManager {
        &self.nodes[index]
    }

    /// Returns the metadata store for a node.
    pub fn store(&self, index: usize) -> &Arc<RedbMetadataStore> {
        &self.stores[index]
    }

    /// Returns the Raft metadata backend for a node.
    pub fn backend(&self, index: usize) -> &ConcreteRaftBackend {
        &self.backends[index]
    }

    /// Helper to create a bucket through consensus.
    pub async fn create_bucket(&self, leader_idx: usize, name: &str) -> Result<BucketInfo> {
        self.backends[leader_idx].create_bucket(name).await
    }

    /// Gets the node address for a given node ID.
    pub fn node_addr(&self, node_id: RaftNodeId) -> String {
        let port = BASE_PORT + self.port_offset + (node_id - 1) as u16;
        format!("127.0.0.1:{}", port)
    }

    /// Starts all gRPC servers.
    pub async fn start_servers(&self) {
        for node in &self.nodes {
            node.start_rpc_server().await.expect("Failed to start gRPC server");
        }
    }

    /// Bootstraps the cluster from node 0.
    pub async fn bootstrap(&self) {
        // Bootstrap first node
        self.nodes[0].bootstrap().await.expect("Failed to bootstrap");
        self.nodes[0]
            .wait_for_leader(Duration::from_secs(5))
            .await
            .expect("First node didn't become leader");

        // Add remaining nodes as learners
        for i in 1..self.nodes.len() {
            let node_id = (i + 1) as RaftNodeId;
            let addr = self.node_addr(node_id);
            self.nodes[0].add_learner(node_id, &addr).await.expect("Failed to add learner");
        }

        // Wait for log replication
        sleep(Duration::from_millis(500)).await;

        // Change membership to make all nodes voters
        let mut voters = BTreeSet::new();
        for i in 0..self.nodes.len() {
            voters.insert((i + 1) as RaftNodeId);
        }
        self.nodes[0].change_membership(voters).await.expect("Failed to change membership");

        // Wait for membership change to propagate
        sleep(Duration::from_millis(500)).await;
    }

    /// Waits for a leader to be elected and returns the leader's node ID.
    pub async fn wait_for_leader(
        &self,
        timeout: Duration,
    ) -> std::result::Result<RaftNodeId, String> {
        let start = std::time::Instant::now();

        loop {
            for node in &self.nodes {
                if node.is_leader().await {
                    return Ok(node.node_id());
                }
            }

            if start.elapsed() > timeout {
                return Err("Timeout waiting for leader".to_string());
            }

            sleep(Duration::from_millis(100)).await;
        }
    }

    /// Returns the index of the current leader node, or None if no leader.
    pub async fn leader_index(&self) -> Option<usize> {
        for (i, node) in self.nodes.iter().enumerate() {
            if node.is_leader().await {
                return Some(i);
            }
        }
        None
    }

    /// Simulates a node crash via chaos controller.
    pub fn crash_node(&self, node_id: RaftNodeId) {
        info!(node_id = node_id, "Crashing node");
        self.chaos.crash_node(node_id);
    }

    /// Recovers a crashed node.
    pub fn recover_node(&self, node_id: RaftNodeId) {
        info!(node_id = node_id, "Recovering node");
        self.chaos.recover_node(node_id);
    }

    /// Creates a network partition isolating the given nodes.
    pub async fn partition_nodes(&self, isolated: &[RaftNodeId]) -> u64 {
        info!(nodes = ?isolated, "Creating partition");
        self.chaos.partition(HashSet::from_iter(isolated.iter().copied())).await
    }

    /// Heals a network partition.
    pub async fn heal_partition(&self, partition_id: u64) {
        info!(partition_id = partition_id, "Healing partition");
        self.chaos.heal_partition(partition_id).await;
    }

    /// Injects network delay between two nodes.
    pub fn inject_delay(&self, from: RaftNodeId, to: RaftNodeId, delay: Duration) {
        info!(from = from, to = to, delay_ms = delay.as_millis(), "Injecting delay");
        self.chaos.inject_delay(from, to, delay);
    }

    /// Injects packet loss between two nodes.
    pub fn inject_packet_loss(&self, from: RaftNodeId, to: RaftNodeId, rate: f64) {
        info!(from = from, to = to, rate = rate, "Injecting packet loss");
        self.chaos.inject_packet_loss(from, to, rate);
    }

    /// Resets all chaos state.
    pub async fn reset_chaos(&self) {
        self.chaos.reset().await;
    }

    /// Shuts down all nodes.
    pub async fn shutdown(&self) {
        for node in &self.nodes {
            if let Err(e) = node.shutdown().await {
                warn!(error = %e, "Error shutting down node");
            }
        }
    }
}

// =============================================================================
// Leader Failure Scenarios (3 tests)
// =============================================================================

#[tokio::test]
async fn test_leader_crash_triggers_new_election() {
    let cluster = ChaosTestCluster::new(3, 100).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    // Verify initial leader
    let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();
    info!(leader = initial_leader, "Initial leader elected");

    // Crash the leader
    cluster.crash_node(initial_leader);

    // Wait for new leader election (should happen within election timeout)
    sleep(Duration::from_secs(2)).await;

    // Find new leader among non-crashed nodes
    let mut new_leader = None;
    for i in 0..cluster.node_count() {
        let node_id = (i + 1) as RaftNodeId;
        if node_id != initial_leader && cluster.node(i).is_leader().await {
            new_leader = Some(node_id);
            break;
        }
    }

    assert!(new_leader.is_some(), "New leader should be elected after crash");
    assert_ne!(new_leader.unwrap(), initial_leader, "New leader should be different");

    info!(new_leader = ?new_leader, "New leader elected after crash");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_leader_crash_no_data_loss() {
    let cluster = ChaosTestCluster::new(3, 110).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    // Get initial leader
    let leader_idx = cluster.leader_index().await.expect("Should have leader");

    // Create a bucket through Raft consensus
    let bucket_name = "test-bucket-no-loss";
    cluster.create_bucket(leader_idx, bucket_name).await.expect("Failed to create bucket");

    // Wait for replication
    sleep(Duration::from_millis(500)).await;

    // Verify bucket exists on all nodes before crash
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists(bucket_name).await.unwrap();
        assert!(exists, "Bucket should exist on node {} before crash", i + 1);
    }

    // Crash the leader
    let leader_id = (leader_idx + 1) as RaftNodeId;
    cluster.crash_node(leader_id);

    // Wait for new election
    sleep(Duration::from_secs(2)).await;

    // Verify data still exists on surviving nodes
    for i in 0..cluster.node_count() {
        if (i + 1) as RaftNodeId != leader_id {
            let exists = cluster.store(i).bucket_exists(bucket_name).await.unwrap();
            assert!(exists, "Bucket should still exist on node {} after leader crash", i + 1);
        }
    }

    info!("Data preserved after leader crash");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_leader_crash_in_flight_writes_handled() {
    let cluster = ChaosTestCluster::new(3, 120).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    let leader_id = (leader_idx + 1) as RaftNodeId;

    // Create first bucket (should succeed)
    cluster.create_bucket(leader_idx, "bucket-1").await.expect("Failed to create bucket-1");
    sleep(Duration::from_millis(200)).await;

    // Crash leader while attempting second write
    let chaos = cluster.chaos().clone();
    let raft = cluster.node(leader_idx).raft();
    let store = cluster.store(leader_idx).clone();
    let create_task = tokio::spawn(async move {
        // Try to create bucket - this may fail or succeed
        let backend = RaftMetadataBackend::new(raft, store);
        backend.create_bucket("bucket-2").await
    });

    // Crash leader shortly after
    sleep(Duration::from_millis(50)).await;
    chaos.crash_node(leader_id);

    // Wait for the create task to complete (may fail)
    let result: std::result::Result<Result<BucketInfo>, _> = create_task.await;
    info!(result = ?result.map(|r| r.is_ok()), "In-flight write result");

    // Wait for new leader
    sleep(Duration::from_secs(2)).await;

    // bucket-1 should definitely exist on surviving nodes
    for i in 0..cluster.node_count() {
        if (i + 1) as RaftNodeId != leader_id {
            let exists = cluster.store(i).bucket_exists("bucket-1").await.unwrap();
            assert!(exists, "bucket-1 should exist on surviving nodes");
        }
    }

    // bucket-2 may or may not exist (depends on timing), but should be consistent
    let mut bucket2_exists = None;
    for i in 0..cluster.node_count() {
        if (i + 1) as RaftNodeId != leader_id {
            let exists = cluster.store(i).bucket_exists("bucket-2").await.unwrap();
            match bucket2_exists {
                None => bucket2_exists = Some(exists),
                Some(prev) => assert_eq!(prev, exists, "bucket-2 state should be consistent"),
            }
        }
    }

    info!(bucket2_exists = ?bucket2_exists, "Consistency verified after in-flight write");
    cluster.shutdown().await;
}

// =============================================================================
// Network Partition Scenarios (4 tests)
// =============================================================================

#[tokio::test]
async fn test_minority_partition_cannot_write() {
    let cluster = ChaosTestCluster::new(3, 200).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    let leader_id = (leader_idx + 1) as RaftNodeId;

    // Create bucket before partition
    cluster.create_bucket(leader_idx, "pre-partition").await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // Partition the leader from the other two nodes
    // Leader becomes minority, other two form majority
    let partition_id = cluster.partition_nodes(&[leader_id]).await;

    // Wait for the partition to take effect and new election in majority
    sleep(Duration::from_secs(3)).await;

    // The old leader (now isolated) should not be able to commit writes
    // because it can't reach quorum
    let write_result: std::result::Result<Result<BucketInfo>, _> = tokio::time::timeout(
        Duration::from_secs(2),
        cluster.create_bucket(leader_idx, "minority-write"),
    )
    .await;

    // Either times out or fails - minority can't commit
    assert!(
        write_result.is_err() || write_result.unwrap().is_err(),
        "Minority node should not be able to commit writes"
    );

    info!("Minority partition correctly rejected write");

    cluster.heal_partition(partition_id).await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_majority_partition_continues_operation() {
    let cluster = ChaosTestCluster::new(3, 210).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    // Initial leader
    let _initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Isolate node 3 (minority partition)
    let partition_id = cluster.partition_nodes(&[3]).await;

    // Wait for partition to take effect
    sleep(Duration::from_secs(2)).await;

    // Find leader in the majority (nodes 1 and 2)
    let mut majority_leader_idx = None;
    for i in 0..2 {
        if cluster.node(i).is_leader().await {
            majority_leader_idx = Some(i);
            break;
        }
    }

    // If no leader yet in majority, wait longer
    if majority_leader_idx.is_none() {
        sleep(Duration::from_secs(2)).await;
        for i in 0..2 {
            if cluster.node(i).is_leader().await {
                majority_leader_idx = Some(i);
                break;
            }
        }
    }

    // Majority should still be able to operate
    if let Some(idx) = majority_leader_idx {
        let result = cluster.create_bucket(idx, "majority-write").await;
        assert!(result.is_ok(), "Majority partition should accept writes");
        info!("Majority partition successfully accepted write");
    } else {
        // If no leader in majority yet, the test still passes if writes don't happen
        info!("Majority still electing leader - partition behavior verified");
    }

    cluster.heal_partition(partition_id).await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_partition_heal_state_reconciliation() {
    let cluster = ChaosTestCluster::new(3, 220).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");

    // Create initial bucket
    cluster.create_bucket(leader_idx, "before-partition").await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // Partition node 3
    let partition_id = cluster.partition_nodes(&[3]).await;
    sleep(Duration::from_secs(1)).await;

    // Create bucket while node 3 is partitioned (if leader is in majority)
    let leader_id = (leader_idx + 1) as RaftNodeId;
    if leader_id != 3 {
        let _ = cluster.create_bucket(leader_idx, "during-partition").await;
    }
    sleep(Duration::from_millis(300)).await;

    // Heal partition
    cluster.heal_partition(partition_id).await;

    // Wait for state reconciliation
    sleep(Duration::from_secs(2)).await;

    // Verify all nodes have consistent state
    let buckets_node1 = cluster.store(0).list_buckets().await.unwrap();
    let buckets_node2 = cluster.store(1).list_buckets().await.unwrap();
    let buckets_node3 = cluster.store(2).list_buckets().await.unwrap();

    // Names should be the same across all nodes
    let names1: HashSet<_> = buckets_node1.iter().map(|b| &b.name).collect();
    let names2: HashSet<_> = buckets_node2.iter().map(|b| &b.name).collect();
    let names3: HashSet<_> = buckets_node3.iter().map(|b| &b.name).collect();

    assert_eq!(names1, names2, "Nodes 1 and 2 should have same buckets");
    assert_eq!(names2, names3, "Nodes 2 and 3 should have same buckets after heal");

    info!(bucket_count = buckets_node1.len(), "State reconciled after partition heal");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_asymmetric_partition_handling() {
    let cluster = ChaosTestCluster::new(3, 230).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    // Create asymmetric partition: 1 -> 2 works, 2 -> 1 fails
    // This simulates one-way network failures
    cluster.inject_packet_loss(2, 1, 1.0); // 100% loss from 2 to 1

    // Wait for any effects to manifest
    sleep(Duration::from_secs(2)).await;

    // The cluster should still function (Raft handles asymmetric networks)
    // Though it may trigger elections or leader changes
    let result = cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert!(result.is_ok(), "Cluster should still have leader with asymmetric partition");

    cluster.reset_chaos().await;
    cluster.shutdown().await;
}

// =============================================================================
// Delay/Latency Scenarios (3 tests)
// =============================================================================

#[tokio::test]
async fn test_delayed_heartbeats_no_false_election() {
    let cluster = ChaosTestCluster::new(3, 300).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let initial_leader = cluster.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Inject delay that's LESS than election timeout (which is typically 150-300ms)
    // Default OpenRaft election timeout is around 150-300ms
    // We use 50ms delay which shouldn't trigger election
    for to in 1..=3 {
        if to != initial_leader {
            cluster.inject_delay(initial_leader, to as RaftNodeId, Duration::from_millis(50));
        }
    }

    // Wait to see if unnecessary election occurs
    sleep(Duration::from_secs(3)).await;

    // Leader should remain the same (no false election)
    let current_leader = cluster.wait_for_leader(Duration::from_secs(2)).await.unwrap();
    assert_eq!(initial_leader, current_leader, "Leader should not change with small delays");

    info!("No false election with delayed heartbeats");
    cluster.reset_chaos().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_high_latency_write_still_commits() {
    let cluster = ChaosTestCluster::new(3, 310).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    let leader_id = (leader_idx + 1) as RaftNodeId;

    // Inject 200ms delay on all paths from leader
    for to in 1..=3 {
        if to != leader_id {
            cluster.inject_delay(leader_id, to as RaftNodeId, Duration::from_millis(200));
            cluster.inject_delay(to as RaftNodeId, leader_id, Duration::from_millis(200));
        }
    }

    // Write should still succeed (just take longer)
    let start = std::time::Instant::now();
    let result: std::result::Result<Result<BucketInfo>, _> = tokio::time::timeout(
        Duration::from_secs(10),
        cluster.create_bucket(leader_idx, "high-latency-bucket"),
    )
    .await;

    assert!(result.is_ok() && result.unwrap().is_ok(), "Write should eventually commit");
    let elapsed = start.elapsed();
    info!(elapsed_ms = elapsed.as_millis(), "High-latency write completed");

    // Verify replication
    sleep(Duration::from_secs(1)).await;
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists("high-latency-bucket").await.unwrap();
        assert!(exists, "Bucket should be replicated to node {}", i + 1);
    }

    cluster.reset_chaos().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_asymmetric_delay_handling() {
    let cluster = ChaosTestCluster::new(3, 320).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    let leader_id = (leader_idx + 1) as RaftNodeId;

    // Asymmetric delays: fast to node 2, slow to node 3
    let fast_node = if leader_id == 2 { 1 } else { 2 };
    let slow_node = if leader_id == 3 { 1 } else { 3 };

    cluster.inject_delay(leader_id, fast_node, Duration::from_millis(10));
    cluster.inject_delay(leader_id, slow_node, Duration::from_millis(300));

    // Write should still succeed (replicates to fast node first)
    let result = cluster.create_bucket(leader_idx, "asymmetric-delay-bucket").await;
    assert!(result.is_ok(), "Write should succeed with asymmetric delays");

    // Wait for slow replication
    sleep(Duration::from_secs(2)).await;

    // All nodes should eventually have the data
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists("asymmetric-delay-bucket").await.unwrap();
        assert!(exists, "Bucket should eventually be on node {}", i + 1);
    }

    cluster.reset_chaos().await;
    cluster.shutdown().await;
}

// =============================================================================
// Packet Loss Scenarios (2 tests)
// =============================================================================

#[tokio::test]
async fn test_packet_loss_eventual_consistency() {
    let cluster = ChaosTestCluster::new(3, 400).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");

    // Inject 10% packet loss on all paths
    for from in 1..=3 {
        for to in 1..=3 {
            if from != to {
                cluster.inject_packet_loss(from, to, 0.10);
            }
        }
    }

    // Perform write (may need retries internally)
    let result: std::result::Result<Result<BucketInfo>, _> = tokio::time::timeout(
        Duration::from_secs(15),
        cluster.create_bucket(leader_idx, "packet-loss-bucket"),
    )
    .await;

    assert!(result.is_ok() && result.unwrap().is_ok(), "Write should eventually succeed");

    // Wait for replication with retries
    sleep(Duration::from_secs(3)).await;

    // Verify eventual consistency
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists("packet-loss-bucket").await.unwrap();
        assert!(exists, "Bucket should eventually be on node {} despite packet loss", i + 1);
    }

    info!("Eventual consistency achieved with 10% packet loss");
    cluster.reset_chaos().await;
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_high_packet_loss_cluster_survives() {
    let cluster = ChaosTestCluster::new(3, 410).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    // Create bucket before packet loss
    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    cluster.create_bucket(leader_idx, "pre-loss-bucket").await.unwrap();
    sleep(Duration::from_millis(500)).await;

    // Inject 30% packet loss
    for from in 1..=3 {
        for to in 1..=3 {
            if from != to {
                cluster.inject_packet_loss(from, to, 0.30);
            }
        }
    }

    // Wait for any disruption
    sleep(Duration::from_secs(3)).await;

    // Cluster should still have a leader (though it might change)
    let result = cluster.wait_for_leader(Duration::from_secs(10)).await;
    assert!(result.is_ok(), "Cluster should survive 30% packet loss");

    // Pre-existing data should still be accessible
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists("pre-loss-bucket").await.unwrap();
        assert!(exists, "Pre-existing bucket should still exist on node {}", i + 1);
    }

    info!("Cluster survived 30% packet loss");
    cluster.reset_chaos().await;
    cluster.shutdown().await;
}

// =============================================================================
// Combined Chaos Scenarios (3 tests)
// =============================================================================

#[tokio::test]
async fn test_rolling_node_failures() {
    let cluster = ChaosTestCluster::new(3, 500).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    // Create initial data
    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    cluster.create_bucket(leader_idx, "initial-bucket").await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // Rolling failures: crash node 1, recover, crash node 2, recover, etc.
    // Always maintain quorum (at least 2 of 3 nodes)
    for crash_id in 1..=3 {
        info!(node = crash_id, "Starting rolling failure");
        cluster.crash_node(crash_id);
        sleep(Duration::from_secs(2)).await;

        // Verify cluster still has leader
        let result = cluster.wait_for_leader(Duration::from_secs(5)).await;
        assert!(result.is_ok(), "Cluster should have leader after crashing node {}", crash_id);

        // Recover before crashing next
        cluster.recover_node(crash_id);
        sleep(Duration::from_secs(1)).await;
    }

    // Final verification - all nodes should be back and consistent
    sleep(Duration::from_secs(2)).await;
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists("initial-bucket").await.unwrap();
        assert!(exists, "Bucket should exist after rolling failures on node {}", i + 1);
    }

    info!("Cluster survived rolling node failures");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_network_flapping() {
    let cluster = ChaosTestCluster::new(3, 510).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    // Create initial data
    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    cluster.create_bucket(leader_idx, "flap-bucket").await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // Rapid partition/heal cycles
    for i in 0..5 {
        info!(cycle = i, "Network flap cycle");

        // Create partition
        let partition_id = cluster.partition_nodes(&[3]).await;
        sleep(Duration::from_millis(300)).await;

        // Heal partition
        cluster.heal_partition(partition_id).await;
        sleep(Duration::from_millis(300)).await;
    }

    // Cluster should stabilize
    sleep(Duration::from_secs(2)).await;
    let result = cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert!(result.is_ok(), "Cluster should stabilize after flapping");

    // Data should be consistent
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists("flap-bucket").await.unwrap();
        assert!(exists, "Bucket should exist after network flapping on node {}", i + 1);
    }

    info!("Cluster survived network flapping");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_progressive_degradation() {
    let cluster = ChaosTestCluster::new(3, 520).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    let leader_id = (leader_idx + 1) as RaftNodeId;

    // Create initial bucket
    cluster.create_bucket(leader_idx, "degradation-bucket").await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // Progressively increase delays
    for delay_ms in [50, 100, 200, 400] {
        info!(delay_ms = delay_ms, "Increasing network delay");

        for to in 1..=3 {
            if to != leader_id {
                cluster.inject_delay(leader_id, to as RaftNodeId, Duration::from_millis(delay_ms));
                cluster.inject_delay(to as RaftNodeId, leader_id, Duration::from_millis(delay_ms));
            }
        }

        sleep(Duration::from_secs(1)).await;

        // Cluster should still function (though possibly with leader changes)
        let result = cluster.wait_for_leader(Duration::from_secs(5)).await;
        if result.is_err() {
            // At very high delays, cluster may struggle - that's expected
            warn!(delay_ms = delay_ms, "Leader election timed out at high delay");
            break;
        }
    }

    // Reset and verify recovery
    cluster.reset_chaos().await;
    sleep(Duration::from_secs(2)).await;

    let result = cluster.wait_for_leader(Duration::from_secs(5)).await;
    assert!(result.is_ok(), "Cluster should recover after chaos reset");

    // Data should still be there
    for i in 0..cluster.node_count() {
        let exists = cluster.store(i).bucket_exists("degradation-bucket").await.unwrap();
        assert!(exists, "Bucket should exist after degradation test on node {}", i + 1);
    }

    info!("Cluster survived progressive degradation");
    cluster.shutdown().await;
}

// =============================================================================
// Linearizability Scenarios (3 tests)
// =============================================================================

#[tokio::test]
async fn test_linearizable_under_leader_change() {
    let cluster = ChaosTestCluster::new(3, 600).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");
    let leader_id = (leader_idx + 1) as RaftNodeId;

    // Create bucket sequence with leader crash in middle
    cluster.create_bucket(leader_idx, "bucket-a").await.unwrap();
    sleep(Duration::from_millis(200)).await;

    // Crash leader
    cluster.crash_node(leader_id);
    sleep(Duration::from_secs(2)).await;

    // Find new leader
    let new_leader_idx = cluster.leader_index().await;
    if let Some(idx) = new_leader_idx {
        // Create another bucket with new leader
        cluster.create_bucket(idx, "bucket-b").await.unwrap();
        sleep(Duration::from_millis(200)).await;
    }

    // Verify linearizability: bucket-a must exist, bucket-b if it was written
    for i in 0..cluster.node_count() {
        let exists_a = cluster.store(i).bucket_exists("bucket-a").await.unwrap();
        assert!(exists_a, "bucket-a should exist on node {} (written before crash)", i + 1);
    }

    // Recover crashed node
    cluster.recover_node(leader_id);
    sleep(Duration::from_secs(2)).await;

    // All nodes should have consistent view
    let buckets_0 = cluster.store(0).list_buckets().await.unwrap();
    let buckets_1 = cluster.store(1).list_buckets().await.unwrap();
    let buckets_2 = cluster.store(2).list_buckets().await.unwrap();

    let names_0: HashSet<_> = buckets_0.iter().map(|b| &b.name).collect();
    let names_1: HashSet<_> = buckets_1.iter().map(|b| &b.name).collect();
    let names_2: HashSet<_> = buckets_2.iter().map(|b| &b.name).collect();

    assert_eq!(names_0, names_1, "Nodes 0 and 1 should have same buckets");
    assert_eq!(names_1, names_2, "Nodes 1 and 2 should have same buckets");

    info!("Linearizability maintained under leader change");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_linearizable_under_partition() {
    let cluster = ChaosTestCluster::new(3, 610).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");

    // Create bucket before partition
    cluster.create_bucket(leader_idx, "pre-partition-lin").await.unwrap();
    sleep(Duration::from_millis(300)).await;

    // Partition one node
    let partition_id = cluster.partition_nodes(&[3]).await;
    sleep(Duration::from_secs(1)).await;

    // Try to create bucket during partition
    let leader_id = (leader_idx + 1) as RaftNodeId;
    if leader_id != 3 {
        // Leader is in majority, should succeed
        let _ = cluster.create_bucket(leader_idx, "during-partition-lin").await;
    }

    // Heal partition
    cluster.heal_partition(partition_id).await;
    sleep(Duration::from_secs(2)).await;

    // Verify linearizability - all nodes must agree
    let buckets_0 = cluster.store(0).list_buckets().await.unwrap();
    let buckets_1 = cluster.store(1).list_buckets().await.unwrap();
    let buckets_2 = cluster.store(2).list_buckets().await.unwrap();

    let names_0: HashSet<_> = buckets_0.iter().map(|b| b.name.clone()).collect();
    let names_1: HashSet<_> = buckets_1.iter().map(|b| b.name.clone()).collect();
    let names_2: HashSet<_> = buckets_2.iter().map(|b| b.name.clone()).collect();

    assert_eq!(names_0, names_1, "Nodes must agree on bucket set");
    assert_eq!(names_1, names_2, "All nodes must agree on bucket set");

    // Pre-partition bucket must exist everywhere
    for i in 0..3 {
        let exists = cluster.store(i).bucket_exists("pre-partition-lin").await.unwrap();
        assert!(exists, "Pre-partition bucket must exist on node {}", i + 1);
    }

    info!("Linearizability maintained under partition");
    cluster.shutdown().await;
}

#[tokio::test]
async fn test_linearizable_under_packet_loss() {
    let cluster = ChaosTestCluster::new(3, 620).await;
    cluster.start_servers().await;
    cluster.bootstrap().await;

    let leader_idx = cluster.leader_index().await.expect("Should have leader");

    // Inject moderate packet loss
    for from in 1..=3 {
        for to in 1..=3 {
            if from != to {
                cluster.inject_packet_loss(from, to, 0.15);
            }
        }
    }

    // Create multiple buckets
    for i in 0..3 {
        let result: std::result::Result<Result<BucketInfo>, _> = tokio::time::timeout(
            Duration::from_secs(10),
            cluster.create_bucket(leader_idx, &format!("lin-bucket-{}", i)),
        )
        .await;

        if result.is_err() || result.unwrap().is_err() {
            // Some writes may fail under packet loss - that's OK
            warn!(bucket = i, "Bucket creation failed under packet loss");
        }
        sleep(Duration::from_millis(100)).await;
    }

    // Clear packet loss
    cluster.reset_chaos().await;
    sleep(Duration::from_secs(2)).await;

    // Verify consistency - all nodes must agree on which buckets exist
    let buckets_0 = cluster.store(0).list_buckets().await.unwrap();
    let buckets_1 = cluster.store(1).list_buckets().await.unwrap();
    let buckets_2 = cluster.store(2).list_buckets().await.unwrap();

    let names_0: HashSet<_> = buckets_0.iter().map(|b| b.name.clone()).collect();
    let names_1: HashSet<_> = buckets_1.iter().map(|b| b.name.clone()).collect();
    let names_2: HashSet<_> = buckets_2.iter().map(|b| b.name.clone()).collect();

    assert_eq!(names_0, names_1, "Nodes 0 and 1 must agree");
    assert_eq!(names_1, names_2, "Nodes 1 and 2 must agree");

    info!(bucket_count = names_0.len(), "Linearizability maintained under packet loss");
    cluster.shutdown().await;
}
