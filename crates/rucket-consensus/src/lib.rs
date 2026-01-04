//! Rucket Consensus - Raft-based distributed consensus for metadata operations.
//!
//! This crate implements a single Raft group for linearizable metadata operations
//! across a cluster of Rucket nodes. Object data is replicated separately via
//! Primary-Backup replication (see `rucket-replication`).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     S3 API Layer                             │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!              ┌───────────────┼───────────────┐
//!              │               │               │
//!              ▼               ▼               ▼
//!       ┌───────────┐   ┌───────────┐   ┌───────────┐
//!       │  Bucket   │   │  Object   │   │   Data    │
//!       │Operations │   │ Metadata  │   │   I/O     │
//!       │(Strong)   │   │ (Strong)  │   │ (Local)   │
//!       └───────────┘   └───────────┘   └───────────┘
//!              │               │               │
//!              └───────┬───────┘               │
//!                      ▼                       │
//!       ┌─────────────────────────────┐       │
//!       │    RaftMetadataBackend      │       │
//!       │         (Raft)              │       │
//!       └─────────────────────────────┘       │
//!                      │                       │
//!                      └───────────────────────┘
//!                                 │
//!                                 ▼
//!       ┌─────────────────────────────────────────────────────────┐
//!       │                    LocalStorage                          │
//!       └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```ignore
//! use rucket_consensus::{ClusterConfig, ClusterManager};
//!
//! let config = ClusterConfig {
//!     enabled: true,
//!     node_id: 1,
//!     raft_addr: "127.0.0.1:9001".parse()?,
//!     peers: vec!["node2:9001".to_string(), "node3:9001".to_string()],
//!     ..Default::default()
//! };
//!
//! let cluster = ClusterManager::new(config, log_storage, state_machine).await?;
//! if config.bootstrap.is_some() {
//!     cluster.bootstrap().await?;
//! }
//! ```

#![warn(missing_docs)]

pub mod command;
pub mod config;
pub mod discovery;
pub mod response;
pub mod types;

pub mod log_storage;
pub mod network;
pub mod state_machine;

#[cfg(feature = "chaos-testing")]
pub mod testing;

mod backend;
mod cluster;

pub use backend::RaftMetadataBackend;
pub use cluster::{ClusterError, ClusterManager, ClusterState};
pub use command::MetadataCommand;
pub use config::ClusterConfig;
// Discovery exports
pub use discovery::{
    CloudConfig, CloudDiscovery, CloudProvider, DiscoveredPeer, Discovery, DiscoveryError,
    DiscoveryEvent, DiscoveryManager, DiscoveryManagerConfig, DiscoveryOptions, DnsDiscovery,
    MetadataEndpoints, StaticDiscovery,
};
pub use log_storage::{LogStorageError, RedbLogReader, RedbLogStorage};
pub use network::{GrpcNetworkFactory, GrpcRaftServer};
pub use response::MetadataResponse;
pub use state_machine::{MetadataSnapshotBuilder, MetadataStateMachine, SnapshotData};
pub use types::{RaftNodeId, RaftTypeConfig, RucketRaft};
