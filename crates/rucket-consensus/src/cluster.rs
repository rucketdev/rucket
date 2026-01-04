//! Cluster management.
//!
//! This module handles cluster formation, membership changes,
//! and node lifecycle.

use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use openraft::{BasicNode, ChangeMembers, Config, Raft};
use tokio::sync::RwLock;
use tonic::transport::Server;
use tracing::{error, info, warn};

use crate::config::ClusterConfig;
use crate::log_storage::{LogStorageError, RedbLogStorage};
use crate::network::{GrpcNetworkFactory, GrpcRaftServer};
use crate::state_machine::MetadataStateMachine;
use crate::types::{RaftNodeId, RucketRaft};

/// Error type for cluster operations.
#[derive(Debug, thiserror::Error)]
pub enum ClusterError {
    /// Raft error.
    #[error("raft error: {0}")]
    Raft(String),

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// Network error.
    #[error("network error: {0}")]
    Network(String),

    /// Storage error.
    #[error("storage error: {0}")]
    Storage(String),

    /// Bootstrap error.
    #[error("bootstrap error: {0}")]
    Bootstrap(String),

    /// Join error.
    #[error("join error: {0}")]
    Join(String),
}

impl From<LogStorageError> for ClusterError {
    fn from(e: LogStorageError) -> Self {
        ClusterError::Storage(e.to_string())
    }
}

/// State of the cluster manager.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterState {
    /// Not yet initialized.
    Uninitialized,
    /// Raft instance created, waiting to join or bootstrap.
    Initialized,
    /// Part of a cluster (leader or follower).
    Running,
    /// Shutting down.
    ShuttingDown,
}

/// Manages cluster formation and membership.
///
/// The `ClusterManager` handles:
/// - Bootstrapping a new cluster
/// - Joining an existing cluster
/// - Adding/removing nodes
/// - Graceful shutdown and drain operations
///
/// # Usage
///
/// ```ignore
/// use rucket_consensus::{ClusterConfig, ClusterManager, RedbLogStorage, MetadataStateMachine};
///
/// // Create components
/// let log_storage = RedbLogStorage::open("./data/raft/log.db")?;
/// let state_machine = MetadataStateMachine::new(backend);
///
/// // Create manager
/// let manager = ClusterManager::new(config, log_storage, state_machine)?;
///
/// // Start gRPC server
/// manager.start_rpc_server().await?;
///
/// // Bootstrap or join
/// if config.should_bootstrap() {
///     manager.bootstrap().await?;
/// } else {
///     manager.join_cluster().await?;
/// }
/// ```
pub struct ClusterManager {
    /// Cluster configuration.
    config: ClusterConfig,

    /// The Raft instance.
    raft: Arc<RucketRaft>,

    /// Current cluster state.
    state: RwLock<ClusterState>,

    /// gRPC server shutdown handle.
    shutdown_tx: RwLock<Option<tokio::sync::oneshot::Sender<()>>>,
}

impl ClusterManager {
    /// Creates a new cluster manager with the given components.
    ///
    /// This creates the Raft instance but does not start the gRPC server
    /// or join/bootstrap the cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The Raft configuration is invalid
    /// - The Raft instance cannot be created
    pub async fn new(
        config: ClusterConfig,
        log_storage: RedbLogStorage,
        state_machine: MetadataStateMachine,
    ) -> Result<Self, ClusterError> {
        // Build OpenRaft config
        let raft_config = Config {
            cluster_name: "rucket".to_string(),
            heartbeat_interval: config.raft.heartbeat_interval_ms,
            election_timeout_min: config.raft.election_timeout_min_ms,
            election_timeout_max: config.raft.election_timeout_max_ms,
            max_payload_entries: config.raft.max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(config.raft.snapshot_interval),
            ..Default::default()
        };

        let raft_config = Arc::new(
            raft_config
                .validate()
                .map_err(|e| ClusterError::Config(format!("invalid raft config: {}", e)))?,
        );

        // Create network factory
        let network = GrpcNetworkFactory::new();

        // Create Raft instance
        let raft = Raft::new(config.node_id, raft_config, network, log_storage, state_machine)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;

        info!(
            node_id = config.node_id,
            bind_addr = %config.bind_cluster,
            "Created Raft instance"
        );

        Ok(Self {
            config,
            raft: Arc::new(raft),
            state: RwLock::new(ClusterState::Initialized),
            shutdown_tx: RwLock::new(None),
        })
    }

    /// Returns the Raft instance.
    #[must_use]
    pub fn raft(&self) -> Arc<RucketRaft> {
        self.raft.clone()
    }

    /// Returns the current cluster state.
    pub async fn state(&self) -> ClusterState {
        *self.state.read().await
    }

    /// Returns this node's ID.
    #[must_use]
    pub fn node_id(&self) -> RaftNodeId {
        self.config.node_id
    }

    /// Returns the cluster bind address.
    #[must_use]
    pub fn bind_addr(&self) -> SocketAddr {
        self.config.bind_cluster
    }

    /// Starts the gRPC server for Raft RPC communication.
    ///
    /// This must be called before bootstrapping or joining a cluster.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot bind to the configured address.
    pub async fn start_rpc_server(&self) -> Result<(), ClusterError> {
        let addr = self.config.bind_cluster;
        let raft = (*self.raft).clone();

        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        *self.shutdown_tx.write().await = Some(shutdown_tx);

        let server = GrpcRaftServer::new(raft);
        let service = server.into_service();

        info!(addr = %addr, "Starting Raft gRPC server");

        tokio::spawn(async move {
            if let Err(e) = Server::builder()
                .add_service(service)
                .serve_with_shutdown(addr, async {
                    let _ = shutdown_rx.await;
                })
                .await
            {
                error!(error = %e, "gRPC server error");
            }
        });

        // Give the server a moment to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        Ok(())
    }

    /// Bootstraps a new single-node cluster.
    ///
    /// This should only be called on the first node of a new cluster.
    /// Subsequent nodes should use `join_cluster()`.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The cluster is already initialized
    /// - Bootstrap fails
    pub async fn bootstrap(&self) -> Result<(), ClusterError> {
        let state = *self.state.read().await;
        if state != ClusterState::Initialized {
            return Err(ClusterError::Bootstrap(format!("cannot bootstrap in state {:?}", state)));
        }

        info!(node_id = self.config.node_id, "Bootstrapping new cluster");

        // Create initial membership with just this node
        let mut members = BTreeMap::new();
        members
            .insert(self.config.node_id, BasicNode { addr: self.config.bind_cluster.to_string() });

        // Initialize Raft with single-node membership
        self.raft.initialize(members).await.map_err(|e| ClusterError::Bootstrap(e.to_string()))?;

        *self.state.write().await = ClusterState::Running;
        info!(node_id = self.config.node_id, "Cluster bootstrapped successfully");

        Ok(())
    }

    /// Joins an existing cluster.
    ///
    /// This contacts the configured peers and requests to join the cluster.
    /// The leader will add this node to the membership.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - No peers are configured
    /// - Cannot connect to any peer
    /// - The join request is rejected
    pub async fn join_cluster(&self) -> Result<(), ClusterError> {
        let state = *self.state.read().await;
        if state != ClusterState::Initialized {
            return Err(ClusterError::Join(format!("cannot join in state {:?}", state)));
        }

        if self.config.peers.is_empty() {
            return Err(ClusterError::Join("no peers configured".to_string()));
        }

        info!(
            node_id = self.config.node_id,
            peers = ?self.config.peers,
            "Joining existing cluster"
        );

        // For now, we don't implement automatic join.
        // The node starts and waits to be added via add_learner/change_membership.
        // In a production system, you'd implement a join RPC that contacts the leader.

        *self.state.write().await = ClusterState::Running;
        warn!(
            "Automatic join not yet implemented. \
             Add this node manually via the leader: add_learner({}, \"{}\")",
            self.config.node_id, self.config.bind_cluster
        );

        Ok(())
    }

    /// Adds a learner (non-voting member) to the cluster.
    ///
    /// Only the leader can add learners. Learners receive log entries
    /// but do not participate in voting.
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not the leader.
    pub async fn add_learner(&self, node_id: RaftNodeId, addr: &str) -> Result<(), ClusterError> {
        info!(node_id = node_id, addr = addr, "Adding learner");

        self.raft
            .add_learner(node_id, BasicNode { addr: addr.to_string() }, true)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;

        Ok(())
    }

    /// Promotes learners to voters and updates membership.
    ///
    /// The provided node IDs must already be learners.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - This node is not the leader
    /// - Any of the node IDs are not learners
    pub async fn change_membership(
        &self,
        voter_ids: BTreeSet<RaftNodeId>,
    ) -> Result<(), ClusterError> {
        info!(voter_ids = ?voter_ids, "Changing membership");

        self.raft
            .change_membership(voter_ids, false)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;

        Ok(())
    }

    /// Sets the full membership with node addresses.
    ///
    /// This is useful when adding new nodes with their addresses.
    ///
    /// # Errors
    ///
    /// Returns an error if this node is not the leader.
    pub async fn set_membership(
        &self,
        members: BTreeMap<RaftNodeId, BasicNode>,
    ) -> Result<(), ClusterError> {
        info!(members = ?members.keys().collect::<Vec<_>>(), "Setting membership");

        self.raft
            .change_membership(ChangeMembers::SetNodes(members), false)
            .await
            .map_err(|e| ClusterError::Raft(e.to_string()))?;

        Ok(())
    }

    /// Returns current cluster metrics.
    pub async fn metrics(&self) -> openraft::RaftMetrics<RaftNodeId, BasicNode> {
        self.raft.metrics().borrow().clone()
    }

    /// Returns true if this node is the current leader.
    pub async fn is_leader(&self) -> bool {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader == Some(self.config.node_id)
    }

    /// Returns the current leader's node ID, if known.
    pub async fn current_leader(&self) -> Option<RaftNodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// Shuts down the cluster manager gracefully.
    ///
    /// This stops the gRPC server and shuts down the Raft instance.
    pub async fn shutdown(&self) -> Result<(), ClusterError> {
        info!(node_id = self.config.node_id, "Shutting down cluster manager");

        *self.state.write().await = ClusterState::ShuttingDown;

        // Signal gRPC server to stop
        if let Some(tx) = self.shutdown_tx.write().await.take() {
            let _ = tx.send(());
        }

        // Shutdown Raft
        if let Err(e) = self.raft.shutdown().await {
            warn!(error = %e, "Error during Raft shutdown");
        }

        Ok(())
    }

    /// Waits for this node to become the leader.
    ///
    /// Returns immediately if already leader.
    /// Useful after bootstrap to ensure the node is ready to accept writes.
    pub async fn wait_for_leader(&self, timeout: Duration) -> Result<(), ClusterError> {
        let start = std::time::Instant::now();

        loop {
            if self.is_leader().await {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(ClusterError::Raft("timeout waiting to become leader".to_string()));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Waits until a leader is known (this node or another).
    ///
    /// Useful after joining to ensure the cluster is operational.
    pub async fn wait_for_any_leader(&self, timeout: Duration) -> Result<RaftNodeId, ClusterError> {
        let start = std::time::Instant::now();

        loop {
            if let Some(leader_id) = self.current_leader().await {
                return Ok(leader_id);
            }

            if start.elapsed() > timeout {
                return Err(ClusterError::Raft("timeout waiting for leader election".to_string()));
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_state_default() {
        // Just verify enum variants exist
        assert_ne!(ClusterState::Uninitialized, ClusterState::Running);
        assert_ne!(ClusterState::Initialized, ClusterState::ShuttingDown);
    }

    #[test]
    fn test_cluster_error_display() {
        let err = ClusterError::Config("test".to_string());
        assert!(err.to_string().contains("test"));

        let err = ClusterError::Bootstrap("bootstrap failed".to_string());
        assert!(err.to_string().contains("bootstrap"));
    }
}
