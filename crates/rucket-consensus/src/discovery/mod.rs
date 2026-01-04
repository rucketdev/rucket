//! Service discovery for cluster peer discovery.
//!
//! This module provides pluggable peer discovery mechanisms for Rucket clusters.
//! It supports both traditional discovery methods (DNS, Kubernetes) and SPOF-free
//! approaches (gossip-based SWIM protocol).
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Discovery Backends                        │
//! ├──────────┬──────────┬──────────┬──────────┬─────────────────┤
//! │  Static  │   DNS    │   K8s    │  Gossip  │     mDNS        │
//! └────┬─────┴────┬─────┴────┬─────┴────┬─────┴────┬────────────┘
//!      │          │          │          │          │
//!      └──────────┴──────────┴──────────┴──────────┘
//!                              │
//!                              ▼
//!                 ┌─────────────────────────┐
//!                 │    Discovery Trait      │
//!                 │  discover() -> Peers    │
//!                 │  watch() -> Events      │
//!                 └───────────┬─────────────┘
//!                             │
//!                             ▼
//!                 ┌─────────────────────────┐
//!                 │   DiscoveryManager      │
//!                 │   Feeds into Raft       │
//!                 │   Auto-add learners     │
//!                 └─────────────────────────┘
//! ```
//!
//! # Key Principle
//!
//! **Gossip for discovery, Raft for authority:**
//! - Gossip answers "who exists?" (fast, distributed, eventually consistent)
//! - Raft answers "who is IN the cluster?" (slow, centralized, strongly consistent)

mod cloud;
mod dns;
mod manager;
mod static_peers;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;

use async_trait::async_trait;
pub use cloud::{CloudConfig, CloudDiscovery, CloudProvider, MetadataEndpoints};
pub use dns::DnsDiscovery;
pub use manager::{DiscoveryManager, DiscoveryManagerConfig};
use serde::{Deserialize, Serialize};
pub use static_peers::{parse_peers, StaticDiscovery};
use thiserror::Error;
use tokio::sync::mpsc::Receiver;

/// Errors that can occur during service discovery.
#[derive(Debug, Error)]
pub enum DiscoveryError {
    /// DNS resolution failed.
    #[error("DNS resolution failed: {0}")]
    DnsResolution(String),

    /// Network error during discovery.
    #[error("Network error: {0}")]
    Network(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Timeout waiting for peers.
    #[error("Discovery timeout: expected {expected} peers, found {found}")]
    Timeout {
        /// Expected number of peers.
        expected: u32,
        /// Number of peers actually found.
        found: u32,
    },

    /// Backend-specific error.
    #[error("Backend error ({backend}): {message}")]
    Backend {
        /// Name of the discovery backend.
        backend: &'static str,
        /// Error message from the backend.
        message: String,
    },

    /// Discovery not supported.
    #[error("Discovery method not supported: {0}")]
    NotSupported(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Information about a discovered peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DiscoveredPeer {
    /// The peer's Raft node ID (may be unknown initially).
    pub node_id: Option<u64>,

    /// Address for Raft RPC communication.
    pub raft_addr: SocketAddr,

    /// Optional metadata about the peer.
    pub metadata: PeerMetadata,
}

impl DiscoveredPeer {
    /// Creates a new discovered peer with just an address.
    #[must_use]
    pub fn new(raft_addr: SocketAddr) -> Self {
        Self { node_id: None, raft_addr, metadata: PeerMetadata::default() }
    }

    /// Creates a new discovered peer with a known node ID.
    #[must_use]
    pub fn with_node_id(node_id: u64, raft_addr: SocketAddr) -> Self {
        Self { node_id: Some(node_id), raft_addr, metadata: PeerMetadata::default() }
    }

    /// Adds metadata to this peer.
    #[must_use]
    pub fn with_metadata(mut self, metadata: PeerMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

/// Optional metadata about a discovered peer.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerMetadata {
    /// Hostname or human-readable name.
    pub hostname: Option<String>,

    /// Data center or availability zone.
    pub datacenter: Option<String>,

    /// Rack or failure domain within datacenter.
    pub rack: Option<String>,

    /// Version of the Rucket software.
    pub version: Option<String>,

    /// Additional key-value tags.
    pub tags: Vec<(String, String)>,
}

/// Events emitted by discovery backends that support watching.
#[derive(Debug, Clone)]
pub enum DiscoveryEvent {
    /// A new peer was discovered.
    PeerJoined(DiscoveredPeer),

    /// A peer is no longer available.
    PeerLeft(SocketAddr),

    /// A peer's information was updated.
    PeerUpdated(DiscoveredPeer),

    /// Discovery refresh completed (for backends that poll).
    RefreshCompleted {
        /// Current set of known peers.
        peers: HashSet<DiscoveredPeer>,
    },

    /// An error occurred during discovery.
    Error(String),
}

/// Options for discovery operations.
#[derive(Debug, Clone)]
pub struct DiscoveryOptions {
    /// How often to refresh peer list (for polling-based backends).
    pub refresh_interval: Duration,

    /// Timeout for individual discovery operations.
    pub timeout: Duration,

    /// Whether to include self in discovered peers.
    pub include_self: bool,
}

impl Default for DiscoveryOptions {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(30),
            timeout: Duration::from_secs(5),
            include_self: false,
        }
    }
}

/// Trait for service discovery backends.
///
/// Implementations provide different methods for discovering cluster peers:
/// - Static: Fixed list of peers from configuration
/// - DNS: SRV or A record resolution
/// - Kubernetes: Headless service discovery via K8s API
/// - Gossip: SWIM protocol for decentralized discovery
/// - mDNS: Zero-configuration LAN discovery
///
/// # Design
///
/// The trait is designed to be:
/// - **Async**: All operations are async for network I/O
/// - **Pluggable**: Easy to add new backends
/// - **Observable**: Supports both one-shot discovery and continuous watching
/// - **Self-registering**: Backends can optionally register/deregister themselves
#[async_trait]
pub trait Discovery: Send + Sync + 'static {
    /// Returns the name of this discovery backend.
    fn name(&self) -> &'static str;

    /// Performs a one-shot discovery of peers.
    ///
    /// Returns the current set of known peers. This is a point-in-time
    /// snapshot and may not reflect the latest cluster state.
    async fn discover(&self) -> Result<HashSet<DiscoveredPeer>, DiscoveryError>;

    /// Starts watching for peer changes.
    ///
    /// Returns a channel that receives discovery events. The channel
    /// is closed when the discovery backend is stopped.
    ///
    /// Not all backends support watching - some only support polling
    /// via `discover()`. Check `supports_watching()` first.
    async fn watch(
        &self,
        options: DiscoveryOptions,
    ) -> Result<Receiver<DiscoveryEvent>, DiscoveryError>;

    /// Registers this node with the discovery backend.
    ///
    /// For backends that support active membership (like gossip or Consul),
    /// this makes the current node visible to other peers.
    ///
    /// For passive backends (like DNS), this is a no-op.
    async fn register(&self, self_info: DiscoveredPeer) -> Result<(), DiscoveryError> {
        let _ = self_info;
        Ok(())
    }

    /// Deregisters this node from the discovery backend.
    ///
    /// Called during graceful shutdown to remove this node from
    /// the discovery pool.
    async fn deregister(&self) -> Result<(), DiscoveryError> {
        Ok(())
    }

    /// Whether this backend supports continuous watching.
    ///
    /// If false, use periodic `discover()` calls instead of `watch()`.
    fn supports_watching(&self) -> bool {
        false
    }

    /// Whether this backend supports active membership (register/deregister).
    fn supports_active_membership(&self) -> bool {
        false
    }
}

/// Result type alias for discovery operations.
pub type DiscoveryResult<T> = Result<T, DiscoveryError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_discovered_peer_creation() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let peer = DiscoveredPeer::new(addr);

        assert_eq!(peer.raft_addr, addr);
        assert!(peer.node_id.is_none());
        assert!(peer.metadata.hostname.is_none());
    }

    #[test]
    fn test_discovered_peer_with_node_id() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let peer = DiscoveredPeer::with_node_id(1, addr);

        assert_eq!(peer.node_id, Some(1));
        assert_eq!(peer.raft_addr, addr);
    }

    #[test]
    fn test_discovered_peer_with_metadata() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let metadata = PeerMetadata {
            hostname: Some("node1.example.com".to_string()),
            datacenter: Some("us-east-1".to_string()),
            ..Default::default()
        };
        let peer = DiscoveredPeer::new(addr).with_metadata(metadata.clone());

        assert_eq!(peer.metadata.hostname, Some("node1.example.com".to_string()));
        assert_eq!(peer.metadata.datacenter, Some("us-east-1".to_string()));
    }

    #[test]
    fn test_discovered_peer_equality() {
        let addr1: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let addr2: SocketAddr = "127.0.0.1:9002".parse().unwrap();

        let peer1 = DiscoveredPeer::new(addr1);
        let peer2 = DiscoveredPeer::new(addr1);
        let peer3 = DiscoveredPeer::new(addr2);

        assert_eq!(peer1, peer2);
        assert_ne!(peer1, peer3);
    }

    #[test]
    fn test_discovery_options_default() {
        let options = DiscoveryOptions::default();

        assert_eq!(options.refresh_interval, Duration::from_secs(30));
        assert_eq!(options.timeout, Duration::from_secs(5));
        assert!(!options.include_self);
    }
}
