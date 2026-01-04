//! Raft network implementation.
//!
//! This module provides the gRPC-based network layer for Raft
//! communication between cluster nodes.

mod grpc_network;

pub use grpc_network::{proto, GrpcNetwork, GrpcNetworkFactory, GrpcRaftServer};
