//! Raft network implementation.
//!
//! This module provides the gRPC-based network layer for Raft
//! communication between cluster nodes.
//!
//! For chaos testing, use [`ChaosNetworkFactory`](crate::testing::ChaosNetworkFactory)
//! from the testing module to wrap the gRPC network with fault injection capabilities.

mod grpc_network;

pub use grpc_network::{proto, GrpcNetwork, GrpcNetworkFactory, GrpcRaftServer};
