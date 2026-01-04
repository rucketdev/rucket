//! Discovery manager for Raft cluster integration.
//!
//! The `DiscoveryManager` bridges service discovery with Raft membership:
//! - Monitors discovered peers via the `Discovery` trait
//! - Coordinates cluster bootstrap when expected nodes are found
//! - Automatically adds discovered peers as Raft learners
//! - Handles graceful node departure
//!
//! # Key Principle
//!
//! **Gossip for discovery, Raft for authority:**
//! - Discovery tells us "who exists?" (fast, eventually consistent)
//! - Raft tells us "who is IN the cluster?" (slow, strongly consistent)
//!
//! New nodes are discovered, then proposed to Raft. Raft is the source of
//! truth for cluster membership.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, RwLock};
use tokio::time::{interval, timeout};
use tracing::{debug, error, info, warn};

use super::{
    DiscoveredPeer, Discovery, DiscoveryError, DiscoveryEvent, DiscoveryOptions, DiscoveryResult,
};
use crate::types::RucketRaft;

/// Configuration for the discovery manager.
#[derive(Debug, Clone)]
pub struct DiscoveryManagerConfig {
    /// How often to check for new peers.
    pub refresh_interval: Duration,

    /// Timeout for discovery operations.
    pub discovery_timeout: Duration,

    /// Expected number of nodes for bootstrap (0 = no bootstrap coordination).
    pub expect_nodes: u32,

    /// Timeout for waiting for expected nodes during bootstrap.
    pub bootstrap_timeout: Duration,

    /// Whether to automatically add discovered peers as learners.
    pub auto_add_learners: bool,
}

impl Default for DiscoveryManagerConfig {
    fn default() -> Self {
        Self {
            refresh_interval: Duration::from_secs(30),
            discovery_timeout: Duration::from_secs(5),
            expect_nodes: 0,
            bootstrap_timeout: Duration::from_secs(60),
            auto_add_learners: true,
        }
    }
}

/// Manages service discovery and Raft membership integration.
///
/// # Lifecycle
///
/// 1. Create with a discovery backend and optional Raft handle
/// 2. Call `wait_for_bootstrap_peers()` if coordinating cluster formation
/// 3. Call `start()` to begin background discovery
/// 4. Call `stop()` during graceful shutdown
pub struct DiscoveryManager {
    /// The discovery backend.
    discovery: Arc<dyn Discovery>,

    /// Configuration.
    config: DiscoveryManagerConfig,

    /// Current set of discovered peers.
    discovered_peers: Arc<RwLock<HashSet<DiscoveredPeer>>>,

    /// This node's information for self-registration.
    self_info: Option<DiscoveredPeer>,

    /// Handle for stopping the background task.
    stop_tx: Option<mpsc::Sender<()>>,
}

impl DiscoveryManager {
    /// Creates a new discovery manager.
    pub fn new(discovery: Arc<dyn Discovery>, config: DiscoveryManagerConfig) -> Self {
        Self {
            discovery,
            config,
            discovered_peers: Arc::new(RwLock::new(HashSet::new())),
            self_info: None,
            stop_tx: None,
        }
    }

    /// Sets this node's information for self-registration.
    #[must_use]
    pub fn with_self_info(mut self, info: DiscoveredPeer) -> Self {
        self.self_info = Some(info);
        self
    }

    /// Returns the current set of discovered peers.
    pub async fn peers(&self) -> HashSet<DiscoveredPeer> {
        self.discovered_peers.read().await.clone()
    }

    /// Performs a one-shot discovery and updates the peer set.
    pub async fn refresh(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        let peers = timeout(self.config.discovery_timeout, self.discovery.discover())
            .await
            .map_err(|_| DiscoveryError::Timeout { expected: 0, found: 0 })??;

        *self.discovered_peers.write().await = peers.clone();
        Ok(peers)
    }

    /// Waits for the expected number of peers to be discovered.
    ///
    /// Used for coordinated cluster bootstrap. Returns when the expected
    /// number of peers (including self) are discovered.
    ///
    /// # Errors
    ///
    /// Returns `DiscoveryError::Timeout` if the expected peers aren't
    /// found within the configured timeout.
    pub async fn wait_for_bootstrap_peers(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        if self.config.expect_nodes == 0 {
            return self.refresh().await;
        }

        let expected = self.config.expect_nodes as usize;
        let deadline = tokio::time::Instant::now() + self.config.bootstrap_timeout;

        info!(
            expected_nodes = expected,
            timeout_secs = self.config.bootstrap_timeout.as_secs(),
            "Waiting for bootstrap peers"
        );

        let mut poll_interval = interval(Duration::from_secs(2));

        loop {
            poll_interval.tick().await;

            if tokio::time::Instant::now() > deadline {
                let found = self.discovered_peers.read().await.len();
                return Err(DiscoveryError::Timeout {
                    expected: self.config.expect_nodes,
                    found: found as u32,
                });
            }

            match self.refresh().await {
                Ok(peers) => {
                    // Count includes self if self_info is set
                    let count =
                        if self.self_info.is_some() { peers.len() + 1 } else { peers.len() };

                    debug!(discovered = peers.len(), expected = expected, "Checking peer count");

                    if count >= expected {
                        info!(
                            discovered = peers.len(),
                            expected = expected,
                            "Found expected number of peers"
                        );
                        return Ok(peers);
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Discovery failed, will retry");
                }
            }
        }
    }

    /// Starts background discovery.
    ///
    /// If Raft handle is provided and `auto_add_learners` is enabled,
    /// newly discovered peers will be automatically proposed as learners.
    pub async fn start(&mut self, raft: Option<Arc<RucketRaft>>) -> DiscoveryResult<()> {
        // Register self if supported
        if let Some(ref info) = self.self_info {
            if self.discovery.supports_active_membership() {
                self.discovery.register(info.clone()).await?;
            }
        }

        // Initial discovery
        self.refresh().await?;

        // Start background watcher
        let (stop_tx, mut stop_rx) = mpsc::channel::<()>(1);
        self.stop_tx = Some(stop_tx);

        let discovery = Arc::clone(&self.discovery);
        let discovered_peers = Arc::clone(&self.discovered_peers);
        let config = self.config.clone();

        tokio::spawn(async move {
            let options = DiscoveryOptions {
                refresh_interval: config.refresh_interval,
                timeout: config.discovery_timeout,
                include_self: false,
            };

            // Try to use watch if supported, otherwise poll
            if discovery.supports_watching() {
                match discovery.watch(options.clone()).await {
                    Ok(mut rx) => loop {
                        tokio::select! {
                            _ = stop_rx.recv() => {
                                debug!("Discovery manager stopping");
                                break;
                            }
                            event = rx.recv() => {
                                match event {
                                    Some(DiscoveryEvent::PeerJoined(peer)) => {
                                        info!(peer = ?peer.raft_addr, "Peer joined");
                                        discovered_peers.write().await.insert(peer.clone());

                                        if config.auto_add_learners {
                                            if let Some(ref raft) = raft {
                                                if let Some(node_id) = peer.node_id {
                                                    let node = openraft::BasicNode {
                                                        addr: peer.raft_addr.to_string(),
                                                    };
                                                    if let Err(e) = raft.add_learner(node_id, node, true).await {
                                                        warn!(
                                                            node_id = node_id,
                                                            error = %e,
                                                            "Failed to add discovered peer as learner"
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    Some(DiscoveryEvent::PeerLeft(addr)) => {
                                        info!(peer = ?addr, "Peer left");
                                        discovered_peers.write().await.retain(|p| p.raft_addr != addr);
                                    }
                                    Some(DiscoveryEvent::PeerUpdated(peer)) => {
                                        debug!(peer = ?peer.raft_addr, "Peer updated");
                                        let mut peers = discovered_peers.write().await;
                                        peers.retain(|p| p.raft_addr != peer.raft_addr);
                                        peers.insert(peer);
                                    }
                                    Some(DiscoveryEvent::RefreshCompleted { peers }) => {
                                        *discovered_peers.write().await = peers;
                                    }
                                    Some(DiscoveryEvent::Error(e)) => {
                                        warn!(error = %e, "Discovery error");
                                    }
                                    None => {
                                        debug!("Discovery channel closed");
                                        break;
                                    }
                                }
                            }
                        }
                    },
                    Err(e) => {
                        error!(error = %e, "Failed to start discovery watch, falling back to polling");
                        // Fall through to polling
                    }
                }
            }

            // Polling fallback
            let mut poll_interval = interval(config.refresh_interval);
            loop {
                tokio::select! {
                    _ = stop_rx.recv() => {
                        debug!("Discovery manager stopping");
                        break;
                    }
                    _ = poll_interval.tick() => {
                        match discovery.discover().await {
                            Ok(peers) => {
                                *discovered_peers.write().await = peers;
                            }
                            Err(e) => {
                                warn!(error = %e, "Discovery refresh failed");
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    /// Stops background discovery and deregisters from the backend.
    pub async fn stop(&mut self) -> DiscoveryResult<()> {
        if let Some(tx) = self.stop_tx.take() {
            let _ = tx.send(()).await;
        }

        if self.discovery.supports_active_membership() {
            self.discovery.deregister().await?;
        }

        Ok(())
    }

    /// Returns the discovery backend name.
    pub fn backend_name(&self) -> &'static str {
        self.discovery.name()
    }
}

impl std::fmt::Debug for DiscoveryManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DiscoveryManager")
            .field("backend", &self.discovery.name())
            .field("config", &self.config)
            .field("self_info", &self.self_info)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::discovery::StaticDiscovery;

    #[tokio::test]
    async fn test_discovery_manager_new() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap()];
        let discovery = Arc::new(StaticDiscovery::new(peers));
        let config = DiscoveryManagerConfig::default();

        let manager = DiscoveryManager::new(discovery, config);
        assert_eq!(manager.backend_name(), "static");
    }

    #[tokio::test]
    async fn test_discovery_manager_refresh() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap(), "127.0.0.1:9002".parse().unwrap()];
        let discovery = Arc::new(StaticDiscovery::new(peers));
        let config = DiscoveryManagerConfig::default();

        let manager = DiscoveryManager::new(discovery, config);
        let discovered = manager.refresh().await.unwrap();

        assert_eq!(discovered.len(), 2);
    }

    #[tokio::test]
    async fn test_discovery_manager_peers() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap()];
        let discovery = Arc::new(StaticDiscovery::new(peers));
        let config = DiscoveryManagerConfig::default();

        let manager = DiscoveryManager::new(discovery, config);

        // Initially empty
        assert!(manager.peers().await.is_empty());

        // After refresh
        manager.refresh().await.unwrap();
        assert_eq!(manager.peers().await.len(), 1);
    }

    #[tokio::test]
    async fn test_discovery_manager_with_self_info() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap()];
        let discovery = Arc::new(StaticDiscovery::new(peers));
        let config = DiscoveryManagerConfig::default();

        let self_info = DiscoveredPeer::with_node_id(1, "127.0.0.1:9000".parse().unwrap());
        let manager = DiscoveryManager::new(discovery, config).with_self_info(self_info.clone());

        assert!(manager.self_info.is_some());
        assert_eq!(manager.self_info.unwrap().node_id, Some(1));
    }

    #[tokio::test]
    async fn test_discovery_manager_wait_for_bootstrap_peers() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap(), "127.0.0.1:9002".parse().unwrap()];
        let discovery = Arc::new(StaticDiscovery::new(peers));
        let config = DiscoveryManagerConfig {
            expect_nodes: 2,
            bootstrap_timeout: Duration::from_secs(5),
            ..Default::default()
        };

        let manager = DiscoveryManager::new(discovery, config);
        let discovered = manager.wait_for_bootstrap_peers().await.unwrap();

        assert_eq!(discovered.len(), 2);
    }

    #[tokio::test]
    async fn test_discovery_manager_wait_for_bootstrap_peers_timeout() {
        let peers = vec!["127.0.0.1:9001".parse().unwrap()];
        let discovery = Arc::new(StaticDiscovery::new(peers));
        let config = DiscoveryManagerConfig {
            expect_nodes: 5, // Expecting more peers than available
            bootstrap_timeout: Duration::from_millis(100),
            ..Default::default()
        };

        let manager = DiscoveryManager::new(discovery, config);
        let result = manager.wait_for_bootstrap_peers().await;

        assert!(matches!(result, Err(DiscoveryError::Timeout { .. })));
    }
}
