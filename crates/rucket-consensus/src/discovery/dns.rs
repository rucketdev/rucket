//! DNS-based peer discovery.
//!
//! Discovers cluster peers by querying DNS SRV or A records.
//! This is suitable for environments with reliable DNS infrastructure.
//!
//! # SRV Record Format
//!
//! For SRV records, the format is:
//! ```text
//! _rucket._tcp.example.com.  300  IN  SRV  10  0  9001  node1.example.com.
//! _rucket._tcp.example.com.  300  IN  SRV  10  0  9001  node2.example.com.
//! ```
//!
//! # A Record Format
//!
//! For A records, all resolved IPs use the configured port:
//! ```text
//! rucket.example.com.  300  IN  A  10.0.0.1
//! rucket.example.com.  300  IN  A  10.0.0.2
//! ```
//!
//! # Use Cases
//!
//! - Kubernetes headless services (resolved via CoreDNS)
//! - Traditional DNS-based service discovery
//! - Cloud provider internal DNS

use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use hickory_resolver::Resolver;
use tokio::net::lookup_host;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info, warn};

use super::{
    DiscoveredPeer, Discovery, DiscoveryError, DiscoveryEvent, DiscoveryOptions, DiscoveryResult,
    PeerMetadata,
};

/// DNS-based peer discovery.
///
/// Resolves peer addresses by querying DNS A or SRV records.
///
/// # Example
///
/// ```ignore
/// // Resolve A records for a hostname
/// let discovery = DnsDiscovery::new("rucket.example.com", 9001);
/// let peers = discovery.discover().await?;
///
/// // Or use SRV records for full service discovery
/// let discovery = DnsDiscovery::with_srv("_rucket._tcp.example.com");
/// let peers = discovery.discover().await?;
/// ```
pub struct DnsDiscovery {
    /// The hostname to resolve.
    hostname: String,

    /// Port to use for discovered peers (for A records).
    port: u16,

    /// Whether to use SRV records (requires hickory-resolver).
    use_srv: bool,

    /// Optional self address to exclude.
    self_addr: Option<SocketAddr>,

    /// Cache of last discovered peers.
    cache: Arc<RwLock<HashSet<DiscoveredPeer>>>,
}

impl DnsDiscovery {
    /// Creates a new DNS discovery for A record resolution.
    #[must_use]
    pub fn new(hostname: impl Into<String>, port: u16) -> Self {
        Self {
            hostname: hostname.into(),
            port,
            use_srv: false,
            self_addr: None,
            cache: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Creates a new DNS discovery for SRV record resolution.
    ///
    /// SRV records include both the target hostname and port, making them
    /// ideal for service discovery where ports may vary.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Resolve SRV records for _rucket._tcp.example.com
    /// let discovery = DnsDiscovery::with_srv("_rucket._tcp.example.com");
    /// let peers = discovery.discover().await?;
    /// ```
    #[must_use]
    pub fn with_srv(hostname: impl Into<String>) -> Self {
        Self {
            hostname: hostname.into(),
            port: 0, // Port comes from SRV record
            use_srv: true,
            self_addr: None,
            cache: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Sets the self address to exclude from discovery results.
    #[must_use]
    pub fn with_self_addr(mut self, addr: SocketAddr) -> Self {
        self.self_addr = Some(addr);
        self
    }

    /// Resolves A records for the hostname.
    async fn resolve_a_records(&self) -> DiscoveryResult<Vec<IpAddr>> {
        let lookup_target = format!("{}:{}", self.hostname, self.port);

        debug!(hostname = %self.hostname, port = self.port, "Resolving DNS A records");

        let addrs: Vec<SocketAddr> = lookup_host(&lookup_target)
            .await
            .map_err(|e| DiscoveryError::DnsResolution(format!("{}: {}", self.hostname, e)))?
            .collect();

        if addrs.is_empty() {
            return Err(DiscoveryError::DnsResolution(format!(
                "No addresses found for {}",
                self.hostname
            )));
        }

        Ok(addrs.into_iter().map(|a| a.ip()).collect())
    }

    /// Resolves SRV records for the hostname.
    ///
    /// Returns a list of (hostname, port) pairs from the SRV records.
    async fn resolve_srv_records(&self) -> DiscoveryResult<Vec<(String, u16)>> {
        debug!(hostname = %self.hostname, "Resolving DNS SRV records");

        let resolver = Resolver::builder_tokio()
            .map_err(|e| DiscoveryError::DnsResolution(format!("Failed to create resolver: {e}")))?
            .build();

        let srv_lookup = resolver.srv_lookup(&self.hostname).await.map_err(|e| {
            DiscoveryError::DnsResolution(format!("SRV lookup failed for {}: {}", self.hostname, e))
        })?;

        let records: Vec<(String, u16)> = srv_lookup
            .iter()
            .map(|srv| {
                let target = srv.target().to_string().trim_end_matches('.').to_string();
                let port = srv.port();
                (target, port)
            })
            .collect();

        if records.is_empty() {
            return Err(DiscoveryError::DnsResolution(format!(
                "No SRV records found for {}",
                self.hostname
            )));
        }

        info!(
            hostname = %self.hostname,
            count = records.len(),
            "Resolved DNS SRV records"
        );

        Ok(records)
    }

    /// Resolves SRV records and returns DiscoveredPeer instances.
    async fn resolve_srv_to_peers(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        let srv_records = self.resolve_srv_records().await?;

        let resolver = Resolver::builder_tokio()
            .map_err(|e| DiscoveryError::DnsResolution(format!("Failed to create resolver: {e}")))?
            .build();

        let mut peers = HashSet::new();

        for (target, port) in srv_records {
            // Resolve the A/AAAA records for each SRV target
            match resolver.lookup_ip(&target).await {
                Ok(ips) => {
                    for ip in ips.iter() {
                        let addr = SocketAddr::new(ip, port);
                        if self.self_addr.as_ref() != Some(&addr) {
                            peers.insert(DiscoveredPeer::new(addr).with_metadata(PeerMetadata {
                                hostname: Some(target.clone()),
                                ..Default::default()
                            }));
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        target = %target,
                        error = %e,
                        "Failed to resolve SRV target to IP"
                    );
                }
            }
        }

        if peers.is_empty() {
            return Err(DiscoveryError::DnsResolution(format!(
                "No resolvable peers found from SRV records for {}",
                self.hostname
            )));
        }

        Ok(peers)
    }
}

#[async_trait]
impl Discovery for DnsDiscovery {
    fn name(&self) -> &'static str {
        "dns"
    }

    async fn discover(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        let peers = if self.use_srv {
            debug!(hostname = %self.hostname, "Using SRV record resolution");
            self.resolve_srv_to_peers().await?
        } else {
            let ips = self.resolve_a_records().await?;

            debug!(
                hostname = %self.hostname,
                count = ips.len(),
                "Resolved DNS addresses"
            );

            ips.into_iter()
                .map(|ip| {
                    let addr = SocketAddr::new(ip, self.port);
                    DiscoveredPeer::new(addr).with_metadata(PeerMetadata {
                        hostname: Some(self.hostname.clone()),
                        ..Default::default()
                    })
                })
                .filter(|peer| self.self_addr.as_ref() != Some(&peer.raft_addr))
                .collect()
        };

        // Update cache
        *self.cache.write().await = peers.clone();

        Ok(peers)
    }

    async fn watch(&self, options: DiscoveryOptions) -> DiscoveryResult<Receiver<DiscoveryEvent>> {
        let (tx, rx) = mpsc::channel(16);

        let hostname = self.hostname.clone();
        let port = self.port;
        let use_srv = self.use_srv;
        let self_addr = self.self_addr;
        let cache = Arc::clone(&self.cache);
        let refresh_interval = options.refresh_interval;

        tokio::spawn(async move {
            let mut interval = interval(refresh_interval);

            loop {
                interval.tick().await;

                // Create a temporary discovery for resolution
                let discovery = if use_srv {
                    DnsDiscovery::with_srv(&hostname)
                } else {
                    DnsDiscovery::new(&hostname, port)
                };
                let discovery = if let Some(addr) = self_addr {
                    discovery.with_self_addr(addr)
                } else {
                    discovery
                };

                match discovery.discover().await {
                    Ok(new_peers) => {
                        let old_peers = cache.read().await.clone();

                        // Detect joins and leaves
                        for peer in &new_peers {
                            if !old_peers.contains(peer)
                                && tx.send(DiscoveryEvent::PeerJoined(peer.clone())).await.is_err()
                            {
                                return;
                            }
                        }

                        for peer in &old_peers {
                            if !new_peers.contains(peer)
                                && tx.send(DiscoveryEvent::PeerLeft(peer.raft_addr)).await.is_err()
                            {
                                return;
                            }
                        }

                        // Update cache
                        *cache.write().await = new_peers.clone();

                        if tx
                            .send(DiscoveryEvent::RefreshCompleted { peers: new_peers })
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    Err(e) => {
                        if tx.send(DiscoveryEvent::Error(e.to_string())).await.is_err() {
                            return;
                        }
                    }
                }
            }
        });

        Ok(rx)
    }

    fn supports_watching(&self) -> bool {
        true
    }
}

impl std::fmt::Debug for DnsDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsDiscovery")
            .field("hostname", &self.hostname)
            .field("port", &self.port)
            .field("use_srv", &self.use_srv)
            .field("self_addr", &self.self_addr)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dns_discovery_new() {
        let discovery = DnsDiscovery::new("example.com", 9001);
        assert_eq!(discovery.hostname, "example.com");
        assert_eq!(discovery.port, 9001);
        assert!(!discovery.use_srv);
    }

    #[test]
    fn test_dns_discovery_with_srv() {
        let discovery = DnsDiscovery::with_srv("_rucket._tcp.example.com");
        assert!(discovery.use_srv);
    }

    #[test]
    fn test_dns_discovery_with_self_addr() {
        let addr: SocketAddr = "127.0.0.1:9001".parse().unwrap();
        let discovery = DnsDiscovery::new("example.com", 9001).with_self_addr(addr);
        assert_eq!(discovery.self_addr, Some(addr));
    }

    #[test]
    fn test_dns_discovery_name() {
        let discovery = DnsDiscovery::new("example.com", 9001);
        assert_eq!(discovery.name(), "dns");
    }

    // Note: Actual DNS resolution tests would require network access
    // or mocking, which is beyond the scope of unit tests.
}
