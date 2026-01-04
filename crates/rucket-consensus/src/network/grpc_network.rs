//! gRPC-based network implementation for Raft communication.
//!
//! This module provides the network layer for OpenRaft, implementing both
//! the client side (sending RPCs to other nodes) and server side (receiving
//! RPCs from other nodes).

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::Arc;

use openraft::error::{
    Fatal, InstallSnapshotError, NetworkError, RPCError, RaftError, Unreachable,
};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use tokio::sync::RwLock;
use tonic::transport::Channel;
use tonic::{Request, Response, Status};
use tracing::{debug, error, instrument, warn};

use crate::types::{RaftNodeId, RaftTypeConfig, RucketRaft};

/// Generated gRPC client and server code.
pub mod proto {
    tonic::include_proto!("rucket.raft.v1");
}

use proto::raft_service_client::RaftServiceClient;
use proto::raft_service_server::{RaftService, RaftServiceServer};

/// Result type for vote and append_entries RPCs.
type RpcResult<T> = std::result::Result<T, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>;

/// Result type for install_snapshot RPC.
type SnapshotRpcResult<T> = std::result::Result<
    T,
    RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
>;

/// Result type for full_snapshot streaming.
type StreamingResult<T> =
    std::result::Result<T, openraft::error::StreamingError<RaftTypeConfig, Fatal<RaftNodeId>>>;

// ----------------------------------------------------------------------------
// Client Side: RaftNetwork Implementation
// ----------------------------------------------------------------------------

/// gRPC-based Raft network factory.
///
/// Creates connections to peer nodes for sending Raft RPCs.
#[derive(Clone)]
pub struct GrpcNetworkFactory {
    /// Cache of gRPC clients keyed by node address.
    clients: Arc<RwLock<HashMap<String, RaftServiceClient<Channel>>>>,
}

impl GrpcNetworkFactory {
    /// Creates a new gRPC network factory.
    pub fn new() -> Self {
        Self { clients: Arc::new(RwLock::new(HashMap::new())) }
    }
}

impl Default for GrpcNetworkFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl RaftNetworkFactory<RaftTypeConfig> for GrpcNetworkFactory {
    type Network = GrpcNetwork;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        GrpcNetwork { target, target_addr: node.addr.clone(), clients: self.clients.clone() }
    }
}

/// gRPC-based Raft network connection to a specific node.
pub struct GrpcNetwork {
    /// Target node ID.
    target: RaftNodeId,
    /// Target node address.
    target_addr: String,
    /// Shared client cache.
    clients: Arc<RwLock<HashMap<String, RaftServiceClient<Channel>>>>,
}

/// Error type for client connection.
#[derive(Debug)]
pub enum ConnectionError {
    /// Invalid endpoint URI.
    InvalidUri(String),
    /// Transport error.
    Transport(tonic::transport::Error),
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionError::InvalidUri(uri) => write!(f, "Invalid endpoint URI: {}", uri),
            ConnectionError::Transport(e) => write!(f, "Transport error: {}", e),
        }
    }
}

impl std::error::Error for ConnectionError {}

impl GrpcNetwork {
    /// Gets or creates a gRPC client for the target node.
    async fn get_client(&self) -> std::result::Result<RaftServiceClient<Channel>, ConnectionError> {
        // Check cache first
        {
            let clients = self.clients.read().await;
            if let Some(client) = clients.get(&self.target_addr) {
                return Ok(client.clone());
            }
        }

        // Create new client
        let endpoint = format!("http://{}", self.target_addr);
        debug!(endpoint = %endpoint, "Creating new gRPC client connection");

        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|_| ConnectionError::InvalidUri(endpoint))?
            .connect()
            .await
            .map_err(ConnectionError::Transport)?;

        let client = RaftServiceClient::new(channel);

        // Cache it
        {
            let mut clients = self.clients.write().await;
            clients.insert(self.target_addr.clone(), client.clone());
        }

        Ok(client)
    }
}

impl RaftNetwork<RaftTypeConfig> for GrpcNetwork {
    #[instrument(level = "debug", skip_all, fields(target = self.target))]
    async fn vote(
        &mut self,
        rpc: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> RpcResult<VoteResponse<RaftNodeId>> {
        let mut client =
            self.get_client().await.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let data =
            bincode::serialize(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let response =
            client.vote(Request::new(proto::VoteRequest { data })).await.map_err(to_rpc_error)?;

        let vote_response: VoteResponse<RaftNodeId> =
            bincode::deserialize(&response.into_inner().data)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(vote_response)
    }

    #[instrument(level = "debug", skip_all, fields(target = self.target))]
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<RaftTypeConfig>,
        _option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<RaftNodeId>> {
        let mut client =
            self.get_client().await.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let data =
            bincode::serialize(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let response = client
            .append_entries(Request::new(proto::AppendEntriesRequest { data }))
            .await
            .map_err(to_rpc_error)?;

        let append_response: AppendEntriesResponse<RaftNodeId> =
            bincode::deserialize(&response.into_inner().data)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(append_response)
    }

    #[instrument(level = "debug", skip_all, fields(target = self.target))]
    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<RaftTypeConfig>,
        _option: RPCOption,
    ) -> SnapshotRpcResult<InstallSnapshotResponse<RaftNodeId>> {
        let mut client =
            self.get_client().await.map_err(|e| RPCError::Unreachable(Unreachable::new(&e)))?;

        let data =
            bincode::serialize(&rpc).map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        let response = client
            .install_snapshot(Request::new(proto::InstallSnapshotRequest { data }))
            .await
            .map_err(to_snapshot_rpc_error)?;

        let install_response: InstallSnapshotResponse<RaftNodeId> =
            bincode::deserialize(&response.into_inner().data)
                .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        Ok(install_response)
    }

    #[instrument(level = "debug", skip_all, fields(target = self.target))]
    async fn full_snapshot(
        &mut self,
        vote: openraft::Vote<RaftNodeId>,
        snapshot: openraft::Snapshot<RaftTypeConfig>,
        _cancel: impl std::future::Future<Output = openraft::error::ReplicationClosed>
            + openraft::OptionalSend
            + 'static,
        _option: RPCOption,
    ) -> StreamingResult<SnapshotResponse<RaftNodeId>> {
        let mut client = self
            .get_client()
            .await
            .map_err(|e| openraft::error::StreamingError::Unreachable(Unreachable::new(&e)))?;

        // Build the InstallSnapshotRequest
        let snapshot_data = snapshot.snapshot.into_inner();

        let install_request: InstallSnapshotRequest<RaftTypeConfig> = InstallSnapshotRequest {
            vote,
            meta: snapshot.meta,
            offset: 0,
            data: snapshot_data,
            done: true,
        };

        let data = bincode::serialize(&install_request)
            .map_err(|e| openraft::error::StreamingError::Network(NetworkError::new(&e)))?;

        let response = client
            .install_snapshot(Request::new(proto::InstallSnapshotRequest { data }))
            .await
            .map_err(|e| openraft::error::StreamingError::Unreachable(Unreachable::new(&e)))?;

        let install_response: InstallSnapshotResponse<RaftNodeId> =
            bincode::deserialize(&response.into_inner().data)
                .map_err(|e| openraft::error::StreamingError::Network(NetworkError::new(&e)))?;

        Ok(SnapshotResponse { vote: install_response.vote })
    }
}

/// Convert tonic Status to RPC error.
fn to_rpc_error(e: Status) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>> {
    match e.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            RPCError::Unreachable(Unreachable::new(&e))
        }
        _ => RPCError::Network(NetworkError::new(&e)),
    }
}

/// Convert tonic Status to snapshot RPC error.
fn to_snapshot_rpc_error(
    e: Status,
) -> RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>> {
    match e.code() {
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded => {
            RPCError::Unreachable(Unreachable::new(&e))
        }
        _ => RPCError::Network(NetworkError::new(&e)),
    }
}

// ----------------------------------------------------------------------------
// Server Side: RaftService Implementation
// ----------------------------------------------------------------------------

/// gRPC server implementation for Raft RPCs.
///
/// Receives RPCs from other nodes and forwards them to the local Raft instance.
pub struct GrpcRaftServer {
    /// The local Raft instance.
    raft: RucketRaft,
}

impl GrpcRaftServer {
    /// Creates a new gRPC Raft server.
    pub fn new(raft: RucketRaft) -> Self {
        Self { raft }
    }

    /// Creates a tonic service from this server.
    pub fn into_service(self) -> RaftServiceServer<Self> {
        RaftServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl RaftService for GrpcRaftServer {
    #[instrument(level = "debug", skip_all)]
    async fn vote(
        &self,
        request: Request<proto::VoteRequest>,
    ) -> std::result::Result<Response<proto::VoteResponse>, Status> {
        let req: VoteRequest<RaftNodeId> = bincode::deserialize(&request.into_inner().data)
            .map_err(|e| {
                error!(error = %e, "Failed to deserialize VoteRequest");
                Status::invalid_argument(format!("Failed to deserialize: {}", e))
            })?;

        let response = self.raft.vote(req).await.map_err(|e| {
            warn!(error = %e, "Vote RPC failed");
            Status::internal(format!("Vote failed: {}", e))
        })?;

        let data = bincode::serialize(&response).map_err(|e| {
            error!(error = %e, "Failed to serialize VoteResponse");
            Status::internal(format!("Failed to serialize: {}", e))
        })?;

        Ok(Response::new(proto::VoteResponse { data }))
    }

    #[instrument(level = "debug", skip_all)]
    async fn append_entries(
        &self,
        request: Request<proto::AppendEntriesRequest>,
    ) -> std::result::Result<Response<proto::AppendEntriesResponse>, Status> {
        let req: AppendEntriesRequest<RaftTypeConfig> =
            bincode::deserialize(&request.into_inner().data).map_err(|e| {
                error!(error = %e, "Failed to deserialize AppendEntriesRequest");
                Status::invalid_argument(format!("Failed to deserialize: {}", e))
            })?;

        let response = self.raft.append_entries(req).await.map_err(|e| {
            warn!(error = %e, "AppendEntries RPC failed");
            Status::internal(format!("AppendEntries failed: {}", e))
        })?;

        let data = bincode::serialize(&response).map_err(|e| {
            error!(error = %e, "Failed to serialize AppendEntriesResponse");
            Status::internal(format!("Failed to serialize: {}", e))
        })?;

        Ok(Response::new(proto::AppendEntriesResponse { data }))
    }

    #[instrument(level = "debug", skip_all)]
    async fn install_snapshot(
        &self,
        request: Request<proto::InstallSnapshotRequest>,
    ) -> std::result::Result<Response<proto::InstallSnapshotResponse>, Status> {
        let req: InstallSnapshotRequest<RaftTypeConfig> =
            bincode::deserialize(&request.into_inner().data).map_err(|e| {
                error!(error = %e, "Failed to deserialize InstallSnapshotRequest");
                Status::invalid_argument(format!("Failed to deserialize: {}", e))
            })?;

        // Wrap the data in a Cursor to match the SnapshotData type
        let snapshot =
            openraft::Snapshot { meta: req.meta, snapshot: Box::new(Cursor::new(req.data)) };

        let response = self.raft.install_full_snapshot(req.vote, snapshot).await.map_err(|e| {
            warn!(error = %e, "InstallSnapshot RPC failed");
            Status::internal(format!("InstallSnapshot failed: {}", e))
        })?;

        let data = bincode::serialize(&response).map_err(|e| {
            error!(error = %e, "Failed to serialize InstallSnapshotResponse");
            Status::internal(format!("Failed to serialize: {}", e))
        })?;

        Ok(Response::new(proto::InstallSnapshotResponse { data }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_grpc_network_factory_creation() {
        let factory = GrpcNetworkFactory::new();
        // Just verify it can be created
        assert!(Arc::strong_count(&factory.clients) == 1);
    }
}
