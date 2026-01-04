//! Chaos network wrapper for fault injection testing.
//!
//! This module provides [`ChaosNetwork`] which wraps any [`RaftNetwork`] implementation
//! and injects faults based on a [`ChaosController`].

use std::sync::Arc;

use openraft::error::{NetworkError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tracing::{debug, instrument};

use crate::testing::{ChaosController, ChaosError};
use crate::types::{RaftNodeId, RaftTypeConfig};

/// A network wrapper that injects chaos faults based on a [`ChaosController`].
///
/// Wraps any [`RaftNetwork`] implementation and intercepts all RPCs to apply
/// configured faults (delays, packet loss, partitions, crashes).
///
/// # Example
///
/// ```ignore
/// use std::sync::Arc;
/// use rucket_consensus::network::ChaosNetworkFactory;
/// use rucket_consensus::testing::chaos::ChaosController;
///
/// let chaos = Arc::new(ChaosController::new());
/// let factory = ChaosNetworkFactory::new(inner_factory, chaos.clone(), node_id);
///
/// // Inject faults via the controller
/// chaos.inject_delay(1, 2, Duration::from_millis(100));
/// chaos.crash_node(3);
/// ```
#[derive(Debug)]
pub struct ChaosNetwork<N> {
    /// The underlying network implementation.
    inner: N,
    /// Chaos controller for fault injection.
    controller: Arc<ChaosController>,
    /// This node's ID (source of RPCs).
    self_id: RaftNodeId,
    /// Target node's ID (destination of RPCs).
    target_id: RaftNodeId,
}

impl<N> ChaosNetwork<N> {
    /// Creates a new chaos network wrapper.
    ///
    /// # Arguments
    /// * `inner` - The underlying network to wrap
    /// * `controller` - Shared chaos controller for fault configuration
    /// * `self_id` - This node's ID (RPC source)
    /// * `target_id` - Target node's ID (RPC destination)
    pub fn new(
        inner: N,
        controller: Arc<ChaosController>,
        self_id: RaftNodeId,
        target_id: RaftNodeId,
    ) -> Self {
        Self { inner, controller, self_id, target_id }
    }

    /// Returns a reference to the underlying network.
    pub fn inner(&self) -> &N {
        &self.inner
    }

    /// Returns a mutable reference to the underlying network.
    pub fn inner_mut(&mut self) -> &mut N {
        &mut self.inner
    }

    /// Returns the chaos controller.
    pub fn controller(&self) -> &Arc<ChaosController> {
        &self.controller
    }

    /// Returns this node's ID.
    pub fn self_id(&self) -> RaftNodeId {
        self.self_id
    }

    /// Returns the target node's ID.
    pub fn target_id(&self) -> RaftNodeId {
        self.target_id
    }
}

/// Result type for vote and append_entries RPCs.
type RpcResult<T> = std::result::Result<T, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>;

/// Result type for install_snapshot RPC.
type SnapshotRpcResult<T> = std::result::Result<
    T,
    RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, openraft::error::InstallSnapshotError>>,
>;

/// Result type for full_snapshot streaming.
type StreamingResult<T> = std::result::Result<
    T,
    openraft::error::StreamingError<RaftTypeConfig, openraft::error::Fatal<RaftNodeId>>,
>;

/// Converts a ChaosError to an RPC error.
fn chaos_to_rpc_error(e: ChaosError) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>> {
    match e {
        ChaosError::NodeCrashed(_) => RPCError::Unreachable(Unreachable::new(&e)),
        ChaosError::Partitioned(_, _) => RPCError::Unreachable(Unreachable::new(&e)),
        ChaosError::PacketDropped(_) => RPCError::Network(NetworkError::new(&e)),
        ChaosError::Timeout(_) => RPCError::Unreachable(Unreachable::new(&e)),
    }
}

/// Converts a ChaosError to a snapshot RPC error.
fn chaos_to_snapshot_error(
    e: ChaosError,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, openraft::error::InstallSnapshotError>> {
    match e {
        ChaosError::NodeCrashed(_) => RPCError::Unreachable(Unreachable::new(&e)),
        ChaosError::Partitioned(_, _) => RPCError::Unreachable(Unreachable::new(&e)),
        ChaosError::PacketDropped(_) => RPCError::Network(NetworkError::new(&e)),
        ChaosError::Timeout(_) => RPCError::Unreachable(Unreachable::new(&e)),
    }
}

/// Converts a ChaosError to a streaming error.
fn chaos_to_streaming_error(
    e: ChaosError,
) -> openraft::error::StreamingError<RaftTypeConfig, openraft::error::Fatal<RaftNodeId>> {
    match e {
        ChaosError::NodeCrashed(_) | ChaosError::Partitioned(_, _) | ChaosError::Timeout(_) => {
            openraft::error::StreamingError::Unreachable(Unreachable::new(&e))
        }
        ChaosError::PacketDropped(_) => {
            openraft::error::StreamingError::Network(NetworkError::new(&e))
        }
    }
}

impl<N> RaftNetwork<RaftTypeConfig> for ChaosNetwork<N>
where
    N: RaftNetwork<RaftTypeConfig>,
{
    #[instrument(level = "trace", skip_all, fields(from = self.self_id, to = self.target_id))]
    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<RaftNodeId>> {
        // Apply chaos interception
        match self.controller.intercept(self.self_id, self.target_id).await {
            Err(e) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    error = %e,
                    "Chaos: vote RPC blocked"
                );
                return Err(chaos_to_rpc_error(e));
            }
            Ok(Some(delay)) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    delay_ms = delay.as_millis(),
                    "Chaos: delaying vote RPC"
                );
                tokio::time::sleep(delay).await;
            }
            Ok(None) => {}
        }

        // Delegate to inner network
        self.inner.vote(rpc, option).await
    }

    #[instrument(level = "trace", skip_all, fields(from = self.self_id, to = self.target_id))]
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RaftTypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<RaftNodeId>> {
        // Apply chaos interception
        match self.controller.intercept(self.self_id, self.target_id).await {
            Err(e) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    error = %e,
                    "Chaos: append_entries RPC blocked"
                );
                return Err(chaos_to_rpc_error(e));
            }
            Ok(Some(delay)) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    delay_ms = delay.as_millis(),
                    "Chaos: delaying append_entries RPC"
                );
                tokio::time::sleep(delay).await;
            }
            Ok(None) => {}
        }

        // Delegate to inner network
        self.inner.append_entries(rpc, option).await
    }

    #[instrument(level = "trace", skip_all, fields(from = self.self_id, to = self.target_id))]
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<RaftTypeConfig>,
        option: RPCOption,
    ) -> SnapshotRpcResult<InstallSnapshotResponse<RaftNodeId>> {
        // Apply chaos interception
        match self.controller.intercept(self.self_id, self.target_id).await {
            Err(e) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    error = %e,
                    "Chaos: install_snapshot RPC blocked"
                );
                return Err(chaos_to_snapshot_error(e));
            }
            Ok(Some(delay)) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    delay_ms = delay.as_millis(),
                    "Chaos: delaying install_snapshot RPC"
                );
                tokio::time::sleep(delay).await;
            }
            Ok(None) => {}
        }

        // Delegate to inner network
        self.inner.install_snapshot(rpc, option).await
    }

    #[instrument(level = "trace", skip_all, fields(from = self.self_id, to = self.target_id))]
    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<RaftNodeId>,
        snapshot: openraft::Snapshot<RaftTypeConfig>,
        cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
        option: RPCOption,
    ) -> StreamingResult<SnapshotResponse<RaftNodeId>> {
        // Apply chaos interception
        match self.controller.intercept(self.self_id, self.target_id).await {
            Err(e) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    error = %e,
                    "Chaos: full_snapshot RPC blocked"
                );
                return Err(chaos_to_streaming_error(e));
            }
            Ok(Some(delay)) => {
                debug!(
                    from = self.self_id,
                    to = self.target_id,
                    delay_ms = delay.as_millis(),
                    "Chaos: delaying full_snapshot RPC"
                );
                tokio::time::sleep(delay).await;
            }
            Ok(None) => {}
        }

        // Delegate to inner network
        self.inner.full_snapshot(vote, snapshot, cancel, option).await
    }
}

/// A network factory that wraps another factory with chaos fault injection.
///
/// Creates [`ChaosNetwork`] instances that wrap the networks created by the inner factory.
#[derive(Clone)]
pub struct ChaosNetworkFactory<F> {
    /// The underlying network factory.
    inner: F,
    /// Shared chaos controller.
    controller: Arc<ChaosController>,
    /// This node's ID.
    self_id: RaftNodeId,
}

impl<F> ChaosNetworkFactory<F> {
    /// Creates a new chaos network factory.
    ///
    /// # Arguments
    /// * `inner` - The underlying network factory to wrap
    /// * `controller` - Shared chaos controller for fault configuration
    /// * `self_id` - This node's ID (will be the source of all RPCs)
    pub fn new(inner: F, controller: Arc<ChaosController>, self_id: RaftNodeId) -> Self {
        Self { inner, controller, self_id }
    }

    /// Returns a reference to the chaos controller.
    pub fn controller(&self) -> &Arc<ChaosController> {
        &self.controller
    }
}

impl<F> RaftNetworkFactory<RaftTypeConfig> for ChaosNetworkFactory<F>
where
    F: RaftNetworkFactory<RaftTypeConfig>,
{
    type Network = ChaosNetwork<F::Network>;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        let inner_network = self.inner.new_client(target, node).await;
        ChaosNetwork::new(inner_network, self.controller.clone(), self.self_id, target)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// A mock network for testing that tracks call counts.
    #[derive(Debug, Default)]
    struct MockNetwork {
        vote_count: std::sync::atomic::AtomicU32,
    }

    impl RaftNetwork<RaftTypeConfig> for MockNetwork {
        async fn vote(
            &mut self,
            _rpc: VoteRequest<RaftNodeId>,
            _option: RPCOption,
        ) -> RpcResult<VoteResponse<RaftNodeId>> {
            self.vote_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(VoteResponse {
                vote: openraft::Vote::new(1, 1),
                vote_granted: true,
                last_log_id: None,
            })
        }

        async fn append_entries(
            &mut self,
            _rpc: AppendEntriesRequest<RaftTypeConfig>,
            _option: RPCOption,
        ) -> RpcResult<AppendEntriesResponse<RaftNodeId>> {
            // Return Success variant - exact structure depends on openraft version
            unimplemented!("not needed for these tests")
        }

        async fn install_snapshot(
            &mut self,
            _rpc: InstallSnapshotRequest<RaftTypeConfig>,
            _option: RPCOption,
        ) -> SnapshotRpcResult<InstallSnapshotResponse<RaftNodeId>> {
            unimplemented!("not needed for these tests")
        }

        async fn full_snapshot(
            &mut self,
            _vote: openraft::Vote<RaftNodeId>,
            _snapshot: openraft::Snapshot<RaftTypeConfig>,
            _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
                + openraft::OptionalSend
                + 'static,
            _option: RPCOption,
        ) -> StreamingResult<SnapshotResponse<RaftNodeId>> {
            unimplemented!("not needed for these tests")
        }
    }

    fn make_rpc_option() -> RPCOption {
        RPCOption::new(Duration::from_secs(5))
    }

    #[tokio::test]
    async fn test_chaos_network_passthrough() {
        let controller = Arc::new(ChaosController::new());
        let mock = MockNetwork::default();
        let mut chaos = ChaosNetwork::new(mock, controller, 1, 2);

        // No chaos configured - should pass through
        let vote_req = VoteRequest { vote: openraft::Vote::new(1, 1), last_log_id: None };
        let result = chaos.vote(vote_req, make_rpc_option()).await;
        assert!(result.is_ok());
        assert_eq!(chaos.inner().vote_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_chaos_network_crash_blocks_rpc() {
        let controller = Arc::new(ChaosController::new());
        let mock = MockNetwork::default();
        let mut chaos = ChaosNetwork::new(mock, controller.clone(), 1, 2);

        // Crash the target node
        controller.crash_node(2);

        let vote_req = VoteRequest { vote: openraft::Vote::new(1, 1), last_log_id: None };
        let result = chaos.vote(vote_req, make_rpc_option()).await;

        // Should fail due to crash
        assert!(result.is_err());
        // Inner network should not have been called
        assert_eq!(chaos.inner().vote_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_chaos_network_partition_blocks_rpc() {
        let controller = Arc::new(ChaosController::new());
        let mock = MockNetwork::default();
        let mut chaos = ChaosNetwork::new(mock, controller.clone(), 1, 2);

        // Create partition isolating node 2
        use std::collections::HashSet;
        controller.partition(HashSet::from([2])).await;

        let vote_req = VoteRequest { vote: openraft::Vote::new(1, 1), last_log_id: None };
        let result = chaos.vote(vote_req, make_rpc_option()).await;

        // Should fail due to partition
        assert!(result.is_err());
        // Inner network should not have been called
        assert_eq!(chaos.inner().vote_count.load(std::sync::atomic::Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn test_chaos_network_delay() {
        let controller = Arc::new(ChaosController::new());
        let mock = MockNetwork::default();
        let mut chaos = ChaosNetwork::new(mock, controller.clone(), 1, 2);

        // Inject 50ms delay
        controller.inject_delay(1, 2, Duration::from_millis(50));

        let start = std::time::Instant::now();
        let vote_req = VoteRequest { vote: openraft::Vote::new(1, 1), last_log_id: None };
        let result = chaos.vote(vote_req, make_rpc_option()).await;
        let elapsed = start.elapsed();

        // Should succeed but with delay
        assert!(result.is_ok());
        assert!(elapsed >= Duration::from_millis(50));
        assert_eq!(chaos.inner().vote_count.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
