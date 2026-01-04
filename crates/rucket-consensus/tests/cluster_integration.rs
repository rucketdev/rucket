//! Integration tests for the Raft cluster.
//!
//! These tests verify:
//! - Single-node cluster bootstrap
//! - Leader election
//! - Adding learners and changing membership
//! - Basic write replication
//! - Metadata replication through RaftMetadataBackend

use std::sync::Arc;
use std::time::Duration;

use rucket_consensus::{
    ClusterConfig, ClusterManager, MetadataStateMachine, RaftMetadataBackend, RedbLogStorage,
};
use rucket_core::config::SyncStrategy;
use rucket_storage::metadata::{MetadataBackend, RedbMetadataStore};
use tempfile::TempDir;
use tokio::time::sleep;

/// Creates a test cluster config for a given node.
fn create_test_config(node_id: u64, port: u16) -> ClusterConfig {
    ClusterConfig::new(node_id).with_bind_addr(format!("127.0.0.1:{}", port).parse().unwrap())
}

/// Creates a test metadata backend and returns the concrete type for direct access.
fn create_metadata_store(dir: &TempDir, node_id: u64) -> Arc<RedbMetadataStore> {
    let db_path = dir.path().join(format!("metadata_{}.db", node_id));
    Arc::new(RedbMetadataStore::open(&db_path, SyncStrategy::Always).unwrap())
}

/// Creates a complete node with all components.
async fn create_node(
    node_id: u64,
    port: u16,
    dir: &TempDir,
) -> Result<ClusterManager, Box<dyn std::error::Error>> {
    let config = create_test_config(node_id, port);

    // Create log storage
    let log_path = dir.path().join(format!("raft_log_{}.db", node_id));
    let log_storage = RedbLogStorage::open(&log_path)?;
    log_storage.load_state().await?;

    // Create metadata backend and state machine
    let metadata = create_metadata_store(dir, node_id);
    let state_machine = MetadataStateMachine::new(metadata as Arc<dyn MetadataBackend>);

    // Create cluster manager
    let manager = ClusterManager::new(config, log_storage, state_machine).await?;

    Ok(manager)
}

/// Creates a node and returns both the manager and the underlying metadata store.
/// This allows tests to verify data replication by reading directly from the local store.
async fn create_node_with_store(
    node_id: u64,
    port: u16,
    dir: &TempDir,
) -> Result<(ClusterManager, Arc<RedbMetadataStore>), Box<dyn std::error::Error>> {
    let config = create_test_config(node_id, port);

    // Create log storage
    let log_path = dir.path().join(format!("raft_log_{}.db", node_id));
    let log_storage = RedbLogStorage::open(&log_path)?;
    log_storage.load_state().await?;

    // Create metadata backend - keep a reference to the concrete type
    let metadata_store = create_metadata_store(dir, node_id);
    let state_machine =
        MetadataStateMachine::new(metadata_store.clone() as Arc<dyn MetadataBackend>);

    // Create cluster manager
    let manager = ClusterManager::new(config, log_storage, state_machine).await?;

    Ok((manager, metadata_store))
}

#[tokio::test]
async fn test_single_node_bootstrap() {
    let tmp_dir = TempDir::new().unwrap();

    // Create node 1
    let node1 = create_node(1, 19001, &tmp_dir).await.unwrap();

    // Start gRPC server
    node1.start_rpc_server().await.unwrap();

    // Bootstrap the cluster
    node1.bootstrap().await.unwrap();

    // Wait for leader election
    node1.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Verify this node is the leader
    assert!(node1.is_leader().await);

    // Verify metrics
    let metrics = node1.metrics().await;
    assert_eq!(metrics.current_leader, Some(1));
    assert_eq!(metrics.id, 1);

    // Shutdown
    node1.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_cluster_state_transitions() {
    use rucket_consensus::ClusterState;

    let tmp_dir = TempDir::new().unwrap();

    // Create node
    let node = create_node(1, 19002, &tmp_dir).await.unwrap();

    // Initially should be Initialized
    assert_eq!(node.state().await, ClusterState::Initialized);

    // Start server
    node.start_rpc_server().await.unwrap();

    // Still Initialized before bootstrap
    assert_eq!(node.state().await, ClusterState::Initialized);

    // Bootstrap
    node.bootstrap().await.unwrap();

    // Now should be Running
    assert_eq!(node.state().await, ClusterState::Running);

    // Shutdown
    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_three_node_cluster() {
    let tmp_dir = TempDir::new().unwrap();

    // Create three nodes with unique ports
    let node1 = create_node(1, 19011, &tmp_dir).await.unwrap();
    let node2 = create_node(2, 19012, &tmp_dir).await.unwrap();
    let node3 = create_node(3, 19013, &tmp_dir).await.unwrap();

    // Start gRPC servers for all nodes
    node1.start_rpc_server().await.unwrap();
    node2.start_rpc_server().await.unwrap();
    node3.start_rpc_server().await.unwrap();

    // Bootstrap node 1
    node1.bootstrap().await.unwrap();
    node1.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    assert!(node1.is_leader().await);

    // Add node 2 as a learner from node 1 (the leader)
    node1.add_learner(2, "127.0.0.1:19012").await.unwrap();

    // Add node 3 as a learner
    node1.add_learner(3, "127.0.0.1:19013").await.unwrap();

    // Give some time for logs to replicate
    sleep(Duration::from_millis(500)).await;

    // Change membership to make all nodes voters
    let mut voters = std::collections::BTreeSet::new();
    voters.insert(1);
    voters.insert(2);
    voters.insert(3);
    node1.change_membership(voters).await.unwrap();

    // Give time for membership change to propagate
    sleep(Duration::from_millis(500)).await;

    // Verify all nodes know about the leader
    let leader1 = node1.current_leader().await;
    let leader2 = node2.current_leader().await;
    let leader3 = node3.current_leader().await;

    assert!(leader1.is_some());
    assert_eq!(leader1, leader2);
    assert_eq!(leader2, leader3);

    // Shutdown all nodes
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_metrics_reporting() {
    let tmp_dir = TempDir::new().unwrap();

    let node = create_node(1, 19021, &tmp_dir).await.unwrap();
    node.start_rpc_server().await.unwrap();
    node.bootstrap().await.unwrap();
    node.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    let metrics = node.metrics().await;

    // Check basic metrics
    assert_eq!(metrics.id, 1);
    assert_eq!(metrics.current_leader, Some(1));
    assert!(metrics.current_term > 0);

    // Check membership
    let voters = &metrics.membership_config.membership().get_joint_config()[0];
    assert!(voters.contains(&1));

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_add_learner_from_non_leader() {
    let tmp_dir = TempDir::new().unwrap();

    let node1 = create_node(1, 19031, &tmp_dir).await.unwrap();
    let node2 = create_node(2, 19032, &tmp_dir).await.unwrap();

    node1.start_rpc_server().await.unwrap();
    node2.start_rpc_server().await.unwrap();

    // Bootstrap node 1
    node1.bootstrap().await.unwrap();
    node1.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Try to add a learner from node 2 (not the leader) - should fail
    // Note: This test verifies that only the leader can modify membership
    let result = node2.add_learner(3, "127.0.0.1:19033").await;

    // The error should indicate that node2 is not the leader
    // (exact error type depends on OpenRaft version)
    assert!(result.is_err());

    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_cannot_bootstrap_twice() {
    let tmp_dir = TempDir::new().unwrap();

    let node = create_node(1, 19041, &tmp_dir).await.unwrap();
    node.start_rpc_server().await.unwrap();

    // First bootstrap should succeed
    node.bootstrap().await.unwrap();
    node.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Second bootstrap should fail (already Running)
    let result = node.bootstrap().await;
    assert!(result.is_err());

    node.shutdown().await.unwrap();
}

/// Tests that bucket creation through RaftMetadataBackend is replicated to all nodes.
///
/// This test verifies the complete data path:
/// 1. Create a 3-node cluster
/// 2. Use RaftMetadataBackend on the leader to create a bucket
/// 3. Verify the bucket is visible on all nodes' local metadata stores
#[tokio::test]
async fn test_bucket_replication_through_raft() {
    let tmp_dir = TempDir::new().unwrap();

    // Create three nodes with their underlying metadata stores
    let (node1, store1) = create_node_with_store(1, 19051, &tmp_dir).await.unwrap();
    let (node2, store2) = create_node_with_store(2, 19052, &tmp_dir).await.unwrap();
    let (node3, store3) = create_node_with_store(3, 19053, &tmp_dir).await.unwrap();

    // Start gRPC servers for all nodes
    node1.start_rpc_server().await.unwrap();
    node2.start_rpc_server().await.unwrap();
    node3.start_rpc_server().await.unwrap();

    // Bootstrap node 1
    node1.bootstrap().await.unwrap();
    node1.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    assert!(node1.is_leader().await);

    // Add nodes 2 and 3 as learners
    node1.add_learner(2, "127.0.0.1:19052").await.unwrap();
    node1.add_learner(3, "127.0.0.1:19053").await.unwrap();

    // Give time for logs to replicate
    sleep(Duration::from_millis(500)).await;

    // Change membership to make all nodes voters
    let mut voters = std::collections::BTreeSet::new();
    voters.insert(1);
    voters.insert(2);
    voters.insert(3);
    node1.change_membership(voters).await.unwrap();

    // Give time for membership change to propagate
    sleep(Duration::from_millis(500)).await;

    // Create RaftMetadataBackend on the leader node
    // This routes write operations through Raft consensus
    let raft_backend = RaftMetadataBackend::new(node1.raft(), store1.clone());

    // Create a bucket through RaftMetadataBackend
    let bucket_name = "test-replicated-bucket";
    let bucket_info = raft_backend.create_bucket(bucket_name).await.unwrap();
    assert_eq!(bucket_info.name, bucket_name);

    // Give time for the bucket creation to replicate to all nodes
    sleep(Duration::from_millis(500)).await;

    // Verify the bucket exists on the leader's local store
    let exists_on_leader = store1.bucket_exists(bucket_name).await.unwrap();
    assert!(exists_on_leader, "Bucket should exist on leader's local store");

    // Verify the bucket exists on follower node 2's local store
    let exists_on_node2 = store2.bucket_exists(bucket_name).await.unwrap();
    assert!(exists_on_node2, "Bucket should be replicated to node 2's local store");

    // Verify the bucket exists on follower node 3's local store
    let exists_on_node3 = store3.bucket_exists(bucket_name).await.unwrap();
    assert!(exists_on_node3, "Bucket should be replicated to node 3's local store");

    // Verify we can list buckets from any node and see the bucket
    let buckets_on_leader = store1.list_buckets().await.unwrap();
    assert_eq!(buckets_on_leader.len(), 1);
    assert_eq!(buckets_on_leader[0].name, bucket_name);

    let buckets_on_node2 = store2.list_buckets().await.unwrap();
    assert_eq!(buckets_on_node2.len(), 1);
    assert_eq!(buckets_on_node2[0].name, bucket_name);

    let buckets_on_node3 = store3.list_buckets().await.unwrap();
    assert_eq!(buckets_on_node3.len(), 1);
    assert_eq!(buckets_on_node3[0].name, bucket_name);

    // Shutdown all nodes
    node1.shutdown().await.unwrap();
    node2.shutdown().await.unwrap();
    node3.shutdown().await.unwrap();
}

/// Tests that bucket deletion through RaftMetadataBackend is replicated to all nodes.
#[tokio::test]
async fn test_bucket_deletion_replication_through_raft() {
    let tmp_dir = TempDir::new().unwrap();

    // Create a single-node cluster for simplicity
    let (node, store) = create_node_with_store(1, 19061, &tmp_dir).await.unwrap();

    node.start_rpc_server().await.unwrap();
    node.bootstrap().await.unwrap();
    node.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    // Create RaftMetadataBackend
    let raft_backend = RaftMetadataBackend::new(node.raft(), store.clone());

    // Create a bucket
    let bucket_name = "bucket-to-delete";
    raft_backend.create_bucket(bucket_name).await.unwrap();

    // Verify it exists
    assert!(store.bucket_exists(bucket_name).await.unwrap());

    // Delete the bucket through Raft
    raft_backend.delete_bucket(bucket_name).await.unwrap();

    // Verify it no longer exists
    assert!(!store.bucket_exists(bucket_name).await.unwrap());

    node.shutdown().await.unwrap();
}

/// Tests bucket versioning configuration through RaftMetadataBackend.
#[tokio::test]
async fn test_bucket_versioning_through_raft() {
    use rucket_core::types::VersioningStatus;

    let tmp_dir = TempDir::new().unwrap();

    let (node, store) = create_node_with_store(1, 19071, &tmp_dir).await.unwrap();

    node.start_rpc_server().await.unwrap();
    node.bootstrap().await.unwrap();
    node.wait_for_leader(Duration::from_secs(5)).await.unwrap();

    let raft_backend = RaftMetadataBackend::new(node.raft(), store.clone());

    // Create a bucket
    let bucket_name = "versioned-bucket";
    raft_backend.create_bucket(bucket_name).await.unwrap();

    // Verify initial versioning status (should be None = never enabled)
    let bucket = store.get_bucket(bucket_name).await.unwrap();
    assert_eq!(bucket.versioning_status, None);

    // Enable versioning through Raft
    raft_backend.set_bucket_versioning(bucket_name, VersioningStatus::Enabled).await.unwrap();

    // Verify versioning is now enabled
    let bucket = store.get_bucket(bucket_name).await.unwrap();
    assert_eq!(bucket.versioning_status, Some(VersioningStatus::Enabled));

    // Suspend versioning through Raft
    raft_backend.set_bucket_versioning(bucket_name, VersioningStatus::Suspended).await.unwrap();

    // Verify versioning is now suspended
    let bucket = store.get_bucket(bucket_name).await.unwrap();
    assert_eq!(bucket.versioning_status, Some(VersioningStatus::Suspended));

    node.shutdown().await.unwrap();
}
