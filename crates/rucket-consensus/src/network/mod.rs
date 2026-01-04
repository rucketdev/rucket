//! Raft network implementation.
//!
//! This module provides the gRPC-based network layer for Raft
//! communication between cluster nodes.
//!
//! For chaos testing, use [`ChaosNetworkFactory`] to wrap the gRPC network
//! with fault injection capabilities.

mod grpc_network;

#[cfg(feature = "chaos-testing")]
mod chaos_network;

#[cfg(feature = "chaos-testing")]
pub use chaos_network::{ChaosNetwork, ChaosNetworkFactory};
pub use grpc_network::{proto, GrpcNetwork, GrpcNetworkFactory, GrpcRaftServer};
