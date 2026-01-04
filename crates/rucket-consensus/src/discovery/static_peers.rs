//! Static peer discovery from configuration.
//!
//! This is the simplest discovery backend - it uses a fixed list of
//! peer addresses from configuration. No network calls are made;
//! the peer list is returned immediately.
//!
//! # Use Cases
//!
//! - Small, stable clusters with known addresses
//! - Development and testing
//! - Environments without DNS or service discovery

use std::collections::HashSet;
use std::net::SocketAddr;

use async_trait::async_trait;
use tokio::sync::mpsc::{self, Receiver};
use tokio::time::interval;
use tracing::debug;

use super::{
    DiscoveredPeer, Discovery, DiscoveryError, DiscoveryEvent, DiscoveryOptions, DiscoveryResult,
};

/// Static peer discovery using a fixed list of addresses.
///
/// # Example
///
/// ```ignore
/// let discovery = StaticDiscovery::new(vec![
///     "node1:9001".parse().unwrap(),
///     "node2:9001".parse().unwrap(),
///     "node3:9001".parse().unwrap(),
/// ]);
///
/// let peers = discovery.discover().await?;
/// ```
#[derive(Debug, Clone)]
pub struct StaticDiscovery {
    /// The fixed list of peer addresses.
    peers: Vec<SocketAddr>,

    /// Optional self address to exclude from discovery results.
    self_addr: Option<SocketAddr>,
}

impl StaticDiscovery {
    /// Creates a new static discovery with the given peer addresses.
    #[must_use]
    pub fn new(peers: Vec<SocketAddr>) -> Self {
        Self { peers, self_addr: None }
    }

    /// Creates a static discovery from string addresses.
    ///
    /// # Errors
    ///
    /// Returns an error if any address cannot be parsed.
    pub fn from_strings(addrs: &[String]) -> DiscoveryResult<Self> {
        let peers = addrs
            .iter()
            .map(|s| {
                s.parse::<SocketAddr>()
                    .map_err(|e| DiscoveryError::Config(format!("Invalid address '{}': {}", s, e)))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self::new(peers))
    }

    /// Sets the self address to exclude from discovery results.
    #[must_use]
    pub fn with_self_addr(mut self, addr: SocketAddr) -> Self {
        self.self_addr = Some(addr);
        self
    }

    /// Returns the configured peer addresses.
    #[must_use]
    pub fn peer_addrs(&self) -> &[SocketAddr] {
        &self.peers
    }
}

#[async_trait]
impl Discovery for StaticDiscovery {
    fn name(&self) -> &'static str {
        "static"
    }

    async fn discover(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        debug!(peer_count = self.peers.len(), "Returning static peer list");

        let peers = self
            .peers
            .iter()
            .filter(|addr| self.self_addr.as_ref() != Some(*addr))
            .map(|addr| DiscoveredPeer::new(*addr))
            .collect();

        Ok(peers)
    }

    async fn watch(&self, options: DiscoveryOptions) -> DiscoveryResult<Receiver<DiscoveryEvent>> {
        let (tx, rx) = mpsc::channel(16);
        let peers = self.peers.clone();
        let self_addr = self.self_addr;
        let refresh_interval = options.refresh_interval;

        // Spawn a task that periodically emits the peer list
        tokio::spawn(async move {
            let mut interval = interval(refresh_interval);

            loop {
                interval.tick().await;

                let discovered: HashSet<DiscoveredPeer> = peers
                    .iter()
                    .filter(|addr| self_addr.as_ref() != Some(*addr))
                    .map(|addr| DiscoveredPeer::new(*addr))
                    .collect();

                if tx.send(DiscoveryEvent::RefreshCompleted { peers: discovered }).await.is_err() {
                    // Receiver dropped, stop watching
                    break;
                }
            }
        });

        Ok(rx)
    }

    fn supports_watching(&self) -> bool {
        // Static discovery "watching" is just periodic refresh
        true
    }
}

/// Creates a static discovery from CLI peer arguments.
///
/// Parses peer addresses in the format "host:port" or "host:port,host:port,...".
///
/// # Errors
///
/// Returns an error if any address cannot be parsed.
pub fn parse_peers(peers_arg: &str) -> DiscoveryResult<Vec<SocketAddr>> {
    peers_arg
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| {
            s.parse::<SocketAddr>()
                .map_err(|e| DiscoveryError::Config(format!("Invalid peer address '{}': {}", s, e)))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_discovery_new() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap(), "127.0.0.1:9002".parse().unwrap()];
        let discovery = StaticDiscovery::new(peers.clone());

        assert_eq!(discovery.peer_addrs(), &peers);
    }

    #[test]
    fn test_static_discovery_from_strings() {
        let addrs = vec!["127.0.0.1:9001".to_string(), "127.0.0.1:9002".to_string()];
        let discovery = StaticDiscovery::from_strings(&addrs).unwrap();

        assert_eq!(discovery.peer_addrs().len(), 2);
    }

    #[test]
    fn test_static_discovery_from_strings_invalid() {
        let addrs = vec!["invalid-address".to_string()];
        let result = StaticDiscovery::from_strings(&addrs);

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_static_discovery_discover() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap(), "127.0.0.1:9002".parse().unwrap()];
        let discovery = StaticDiscovery::new(peers);

        let discovered = discovery.discover().await.unwrap();

        assert_eq!(discovered.len(), 2);
    }

    #[tokio::test]
    async fn test_static_discovery_excludes_self() {
        let peers = vec![
            "127.0.0.1:9001".parse().unwrap(),
            "127.0.0.1:9002".parse().unwrap(),
            "127.0.0.1:9003".parse().unwrap(),
        ];
        let discovery =
            StaticDiscovery::new(peers).with_self_addr("127.0.0.1:9002".parse().unwrap());

        let discovered = discovery.discover().await.unwrap();

        assert_eq!(discovered.len(), 2);
        assert!(!discovered.iter().any(|p| p.raft_addr.port() == 9002));
    }

    #[test]
    fn test_parse_peers() {
        let peers = parse_peers("127.0.0.1:9001,127.0.0.1:9002").unwrap();
        assert_eq!(peers.len(), 2);
        assert_eq!(peers[0].port(), 9001);
        assert_eq!(peers[1].port(), 9002);
    }

    #[test]
    fn test_parse_peers_with_spaces() {
        let peers = parse_peers("127.0.0.1:9001, 127.0.0.1:9002 , 127.0.0.1:9003").unwrap();
        assert_eq!(peers.len(), 3);
    }

    #[test]
    fn test_parse_peers_empty() {
        let peers = parse_peers("").unwrap();
        assert!(peers.is_empty());
    }

    #[test]
    fn test_parse_peers_invalid() {
        let result = parse_peers("invalid:addr,127.0.0.1:9001");
        assert!(result.is_err());
    }

    #[test]
    fn test_static_discovery_name() {
        let discovery = StaticDiscovery::new(vec![]);
        assert_eq!(discovery.name(), "static");
    }
}
