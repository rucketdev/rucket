//! Cluster configuration for Raft consensus.
//!
//! This module re-exports cluster configuration types from `rucket-core`.

// Re-export all cluster config types from rucket-core
pub use rucket_core::config::{BootstrapConfig, ClusterConfig, DiscoveryConfig, RaftTimingConfig};
