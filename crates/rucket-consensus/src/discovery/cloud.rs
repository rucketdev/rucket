//! Auto-detecting cloud discovery.
//!
//! Automatically detects which cloud provider the node is running on
//! (AWS, GCP, or Azure) and uses the appropriate discovery mechanism.
//!
//! # Detection Method
//!
//! Each cloud provider has a metadata service at a well-known address:
//! - AWS: `http://169.254.169.254/latest/meta-data/`
//! - GCP: `http://metadata.google.internal/` (requires `Metadata-Flavor: Google` header)
//! - Azure: `http://169.254.169.254/metadata/instance` (requires `Metadata: true` header)
//!
//! # Discovery Method
//!
//! Once the cloud is detected:
//! - **AWS**: Uses EC2 tags to find instances with matching cluster tag
//! - **GCP**: Uses instance labels to find VMs with matching cluster label
//! - **Azure**: Uses resource tags to find VMs with matching cluster tag
//!
//! # Example
//!
//! ```ignore
//! let discovery = CloudDiscovery::auto_detect(CloudConfig {
//!     cluster_tag: "rucket-cluster".to_string(),
//!     cluster_value: "production".to_string(),
//!     raft_port: 9001,
//! }).await?;
//!
//! let peers = discovery.discover().await?;
//! ```

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::mpsc::{self, Receiver};
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{debug, info};

#[cfg(feature = "aws")]
use super::PeerMetadata;
use super::{
    DiscoveredPeer, Discovery, DiscoveryError, DiscoveryEvent, DiscoveryOptions, DiscoveryResult,
};

/// The detected cloud provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CloudProvider {
    /// Amazon Web Services (EC2).
    Aws,
    /// Google Cloud Platform (Compute Engine).
    Gcp,
    /// Microsoft Azure (Virtual Machines).
    Azure,
    /// Not running on a known cloud provider.
    Unknown,
}

impl std::fmt::Display for CloudProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Aws => write!(f, "AWS"),
            Self::Gcp => write!(f, "GCP"),
            Self::Azure => write!(f, "Azure"),
            Self::Unknown => write!(f, "Unknown"),
        }
    }
}

/// Metadata endpoint configuration for cloud providers.
///
/// Used internally for testing with mock servers. In production,
/// default endpoints are used.
#[derive(Debug, Clone)]
pub struct MetadataEndpoints {
    /// AWS Instance Metadata Service base URL.
    /// Default: `http://169.254.169.254`
    pub aws_base_url: String,

    /// GCP Compute Engine metadata base URL.
    /// Default: `http://metadata.google.internal`
    pub gcp_base_url: String,

    /// Azure Instance Metadata Service base URL.
    /// Default: `http://169.254.169.254`
    pub azure_base_url: String,
}

impl Default for MetadataEndpoints {
    fn default() -> Self {
        Self {
            aws_base_url: "http://169.254.169.254".to_string(),
            gcp_base_url: "http://metadata.google.internal".to_string(),
            azure_base_url: "http://169.254.169.254".to_string(),
        }
    }
}

impl MetadataEndpoints {
    /// Creates endpoints for testing with a mock server.
    ///
    /// All cloud providers will use the same mock server URL.
    #[must_use]
    pub fn for_testing(mock_server_url: &str) -> Self {
        let url = mock_server_url.trim_end_matches('/').to_string();
        Self { aws_base_url: url.clone(), gcp_base_url: url.clone(), azure_base_url: url }
    }

    /// Creates endpoints for testing with separate mock servers per cloud.
    #[must_use]
    pub fn for_testing_separate(aws_url: &str, gcp_url: &str, azure_url: &str) -> Self {
        Self {
            aws_base_url: aws_url.trim_end_matches('/').to_string(),
            gcp_base_url: gcp_url.trim_end_matches('/').to_string(),
            azure_base_url: azure_url.trim_end_matches('/').to_string(),
        }
    }
}

/// Configuration for cloud discovery.
#[derive(Debug, Clone)]
pub struct CloudConfig {
    /// The tag/label key to identify cluster members.
    pub cluster_tag: String,

    /// The tag/label value to match.
    pub cluster_value: String,

    /// Port for Raft RPC communication.
    pub raft_port: u16,

    /// Timeout for metadata service requests.
    pub metadata_timeout: Duration,

    /// Optional: Force a specific cloud provider (skip auto-detection).
    pub force_provider: Option<CloudProvider>,

    /// Metadata service endpoints (for testing).
    pub endpoints: MetadataEndpoints,

    // AWS-specific settings
    /// Use IMDSv2 for AWS metadata access (more secure). Default: true.
    pub aws_use_imdsv2: bool,
    /// Override auto-detected AWS region.
    pub aws_region: Option<String>,

    // GCP-specific settings
    /// Override auto-detected GCP project ID.
    pub gcp_project_id: Option<String>,
    /// Filter discovery to specific GCP zone.
    pub gcp_zone: Option<String>,

    // Azure-specific settings
    /// Override auto-detected Azure resource group.
    pub azure_resource_group: Option<String>,
    /// Override auto-detected Azure subscription ID.
    pub azure_subscription_id: Option<String>,
}

impl Default for CloudConfig {
    fn default() -> Self {
        Self {
            cluster_tag: "rucket:cluster".to_string(),
            cluster_value: "default".to_string(),
            raft_port: 9001,
            metadata_timeout: Duration::from_secs(2),
            force_provider: None,
            endpoints: MetadataEndpoints::default(),
            aws_use_imdsv2: true,
            aws_region: None,
            gcp_project_id: None,
            gcp_zone: None,
            azure_resource_group: None,
            azure_subscription_id: None,
        }
    }
}

/// Cloud-based peer discovery with auto-detection.
pub struct CloudDiscovery {
    /// Configuration.
    config: CloudConfig,

    /// Detected cloud provider.
    provider: CloudProvider,

    /// Optional self address to exclude.
    self_addr: Option<SocketAddr>,

    /// Cache of discovered peers.
    cache: Arc<RwLock<HashSet<DiscoveredPeer>>>,
}

impl CloudDiscovery {
    /// Creates a new cloud discovery with auto-detection.
    ///
    /// Probes metadata endpoints to determine the cloud provider.
    pub async fn auto_detect(config: CloudConfig) -> DiscoveryResult<Self> {
        let provider = if let Some(forced) = config.force_provider {
            info!(provider = %forced, "Using forced cloud provider");
            forced
        } else {
            detect_cloud_provider(&config).await
        };

        if provider == CloudProvider::Unknown {
            return Err(DiscoveryError::NotSupported(
                "Not running on a supported cloud provider (AWS, GCP, Azure)".to_string(),
            ));
        }

        info!(provider = %provider, "Detected cloud provider");

        Ok(Self { config, provider, self_addr: None, cache: Arc::new(RwLock::new(HashSet::new())) })
    }

    /// Creates a cloud discovery for a specific provider.
    pub fn for_provider(provider: CloudProvider, config: CloudConfig) -> Self {
        Self { config, provider, self_addr: None, cache: Arc::new(RwLock::new(HashSet::new())) }
    }

    /// Sets the self address to exclude from discovery results.
    #[must_use]
    pub fn with_self_addr(mut self, addr: SocketAddr) -> Self {
        self.self_addr = Some(addr);
        self
    }

    /// Returns the detected cloud provider.
    pub fn provider(&self) -> CloudProvider {
        self.provider
    }

    /// Discovers peers using the AWS EC2 API.
    async fn discover_aws(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        // AWS discovery using EC2 describe-instances with tag filters
        // This requires the aws-sdk-ec2 crate (optional feature)
        //
        // In a full implementation:
        // 1. Get region from instance metadata
        // 2. Use EC2 client to describe-instances with tag filter
        // 3. Return private IPs of matching instances

        #[cfg(feature = "aws")]
        {
            use aws_config::meta::region::RegionProviderChain;
            use aws_config::BehaviorVersion;
            use aws_sdk_ec2::types::Filter;

            // Get region from metadata or environment
            let region_provider = RegionProviderChain::default_provider();
            let config = aws_config::defaults(BehaviorVersion::latest())
                .region(region_provider)
                .load()
                .await;
            let client = aws_sdk_ec2::Client::new(&config);

            // Filter by cluster tag
            let filter = Filter::builder()
                .name(format!("tag:{}", self.config.cluster_tag))
                .values(&self.config.cluster_value)
                .build();

            let resp =
                client.describe_instances().filters(filter).send().await.map_err(|e| {
                    DiscoveryError::Backend { backend: "aws", message: e.to_string() }
                })?;

            let mut peers = HashSet::new();

            for reservation in resp.reservations() {
                for instance in reservation.instances() {
                    if let Some(private_ip) = instance.private_ip_address() {
                        let addr: SocketAddr = format!("{}:{}", private_ip, self.config.raft_port)
                            .parse()
                            .map_err(|e| DiscoveryError::Backend {
                                backend: "aws",
                                message: format!("Invalid IP: {}", e),
                            })?;

                        if self.self_addr.as_ref() != Some(&addr) {
                            let metadata = PeerMetadata {
                                hostname: instance.private_dns_name().map(|s| s.to_string()),
                                datacenter: instance
                                    .placement()
                                    .and_then(|p| p.availability_zone().map(|az| az.to_string())),
                                ..Default::default()
                            };
                            peers.insert(DiscoveredPeer::new(addr).with_metadata(metadata));
                        }
                    }
                }
            }

            Ok(peers)
        }

        #[cfg(not(feature = "aws"))]
        {
            Err(DiscoveryError::NotSupported(
                "AWS discovery requires the 'aws' feature".to_string(),
            ))
        }
    }

    /// Discovers peers using the GCP Compute Engine API.
    async fn discover_gcp(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        // GCP discovery using Compute Engine API
        // This requires the gcp-compute crate or direct API calls
        //
        // In a full implementation:
        // 1. Get project and zone from metadata
        // 2. List instances with label filter
        // 3. Return internal IPs of matching instances

        #[cfg(feature = "gcp")]
        {
            // GCP implementation would go here
            // For now, return an error indicating the feature is not fully implemented
            Err(DiscoveryError::NotSupported(
                "GCP discovery is not yet fully implemented".to_string(),
            ))
        }

        #[cfg(not(feature = "gcp"))]
        {
            Err(DiscoveryError::NotSupported(
                "GCP discovery requires the 'gcp' feature".to_string(),
            ))
        }
    }

    /// Discovers peers using the Azure Resource Manager API.
    async fn discover_azure(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        // Azure discovery using Azure Resource Manager API
        // This requires the azure_identity and azure_mgmt_compute crates
        //
        // In a full implementation:
        // 1. Get subscription/resource group from metadata
        // 2. List VMs with tag filter
        // 3. Get network interfaces for private IPs
        // 4. Return private IPs of matching VMs

        #[cfg(feature = "azure")]
        {
            // Azure implementation would go here
            Err(DiscoveryError::NotSupported(
                "Azure discovery is not yet fully implemented".to_string(),
            ))
        }

        #[cfg(not(feature = "azure"))]
        {
            Err(DiscoveryError::NotSupported(
                "Azure discovery requires the 'azure' feature".to_string(),
            ))
        }
    }
}

#[async_trait]
impl Discovery for CloudDiscovery {
    fn name(&self) -> &'static str {
        match self.provider {
            CloudProvider::Aws => "cloud:aws",
            CloudProvider::Gcp => "cloud:gcp",
            CloudProvider::Azure => "cloud:azure",
            CloudProvider::Unknown => "cloud:unknown",
        }
    }

    async fn discover(&self) -> DiscoveryResult<HashSet<DiscoveredPeer>> {
        let peers = match self.provider {
            CloudProvider::Aws => self.discover_aws().await?,
            CloudProvider::Gcp => self.discover_gcp().await?,
            CloudProvider::Azure => self.discover_azure().await?,
            CloudProvider::Unknown => {
                return Err(DiscoveryError::NotSupported("Unknown cloud provider".to_string()));
            }
        };

        debug!(
            provider = %self.provider,
            count = peers.len(),
            "Discovered cloud peers"
        );

        // Update cache
        *self.cache.write().await = peers.clone();

        Ok(peers)
    }

    async fn watch(&self, options: DiscoveryOptions) -> DiscoveryResult<Receiver<DiscoveryEvent>> {
        let (tx, rx) = mpsc::channel(16);

        let provider = self.provider;
        let config = self.config.clone();
        let self_addr = self.self_addr;
        let cache = Arc::clone(&self.cache);
        let refresh_interval = options.refresh_interval;

        tokio::spawn(async move {
            let mut interval = interval(refresh_interval);

            loop {
                interval.tick().await;

                let discovery = CloudDiscovery::for_provider(provider, config.clone());
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

impl std::fmt::Debug for CloudDiscovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CloudDiscovery")
            .field("provider", &self.provider)
            .field("config", &self.config)
            .field("self_addr", &self.self_addr)
            .finish()
    }
}

/// Detects the cloud provider by probing metadata endpoints.
async fn detect_cloud_provider(config: &CloudConfig) -> CloudProvider {
    let timeout_duration = config.metadata_timeout;
    let endpoints = &config.endpoints;
    let use_imdsv2 = config.aws_use_imdsv2;

    // Try all providers in parallel
    let (aws_result, gcp_result, azure_result) = tokio::join!(
        probe_aws_metadata(&endpoints.aws_base_url, timeout_duration, use_imdsv2),
        probe_gcp_metadata(&endpoints.gcp_base_url, timeout_duration),
        probe_azure_metadata(&endpoints.azure_base_url, timeout_duration),
    );

    if aws_result {
        return CloudProvider::Aws;
    }
    if gcp_result {
        return CloudProvider::Gcp;
    }
    if azure_result {
        return CloudProvider::Azure;
    }

    CloudProvider::Unknown
}

/// Probes AWS IMDS endpoint.
///
/// Supports both IMDSv1 (simple GET) and IMDSv2 (token-based).
/// IMDSv2 is more secure and recommended for production use.
async fn probe_aws_metadata(base_url: &str, timeout_duration: Duration, use_imdsv2: bool) -> bool {
    use tokio::time::timeout;

    let result = timeout(timeout_duration, async {
        let client = reqwest::Client::new();

        if use_imdsv2 {
            // IMDSv2: Get session token first, then use it for metadata access
            let token_url = format!("{}/latest/api/token", base_url);
            let token_result = client
                .put(&token_url)
                .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
                .send()
                .await;

            match token_result {
                Ok(resp) if resp.status().is_success() => {
                    if let Ok(token) = resp.text().await {
                        // Use token to access metadata
                        let metadata_url = format!("{}/latest/meta-data/instance-id", base_url);
                        client
                            .get(&metadata_url)
                            .header("X-aws-ec2-metadata-token", &token)
                            .send()
                            .await
                            .map(|r| r.status().is_success())
                            .unwrap_or(false)
                    } else {
                        false
                    }
                }
                _ => {
                    // IMDSv2 token request failed, try IMDSv1 as fallback
                    debug!("IMDSv2 token request failed, trying IMDSv1 fallback");
                    let metadata_url = format!("{}/latest/meta-data/instance-id", base_url);
                    client
                        .get(&metadata_url)
                        .send()
                        .await
                        .map(|r| r.status().is_success())
                        .unwrap_or(false)
                }
            }
        } else {
            // IMDSv1: Simple GET request
            let metadata_url = format!("{}/latest/meta-data/instance-id", base_url);
            client.get(&metadata_url).send().await.map(|r| r.status().is_success()).unwrap_or(false)
        }
    })
    .await;

    match result {
        Ok(true) => {
            debug!("AWS metadata endpoint detected");
            true
        }
        _ => false,
    }
}

/// Probes GCP metadata endpoint.
async fn probe_gcp_metadata(base_url: &str, timeout_duration: Duration) -> bool {
    use tokio::time::timeout;

    let result = timeout(timeout_duration, async {
        let client = reqwest::Client::new();
        let url = format!("{}/computeMetadata/v1/instance/zone", base_url);
        client
            .get(&url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    })
    .await;

    match result {
        Ok(true) => {
            debug!("GCP metadata endpoint detected");
            true
        }
        _ => false,
    }
}

/// Probes Azure IMDS endpoint.
async fn probe_azure_metadata(base_url: &str, timeout_duration: Duration) -> bool {
    use tokio::time::timeout;

    let result = timeout(timeout_duration, async {
        let client = reqwest::Client::new();
        let url = format!("{}/metadata/instance?api-version=2021-02-01", base_url);
        client
            .get(&url)
            .header("Metadata", "true")
            .send()
            .await
            .map(|r| r.status().is_success())
            .unwrap_or(false)
    })
    .await;

    match result {
        Ok(true) => {
            debug!("Azure metadata endpoint detected");
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cloud_provider_display() {
        assert_eq!(CloudProvider::Aws.to_string(), "AWS");
        assert_eq!(CloudProvider::Gcp.to_string(), "GCP");
        assert_eq!(CloudProvider::Azure.to_string(), "Azure");
        assert_eq!(CloudProvider::Unknown.to_string(), "Unknown");
    }

    #[test]
    fn test_cloud_config_default() {
        let config = CloudConfig::default();
        assert_eq!(config.cluster_tag, "rucket:cluster");
        assert_eq!(config.cluster_value, "default");
        assert_eq!(config.raft_port, 9001);
        assert!(config.aws_use_imdsv2);
        assert!(config.aws_region.is_none());
        assert!(config.gcp_project_id.is_none());
        assert!(config.azure_resource_group.is_none());
    }

    #[test]
    fn test_metadata_endpoints_default() {
        let endpoints = MetadataEndpoints::default();
        assert_eq!(endpoints.aws_base_url, "http://169.254.169.254");
        assert_eq!(endpoints.gcp_base_url, "http://metadata.google.internal");
        assert_eq!(endpoints.azure_base_url, "http://169.254.169.254");
    }

    #[test]
    fn test_metadata_endpoints_for_testing() {
        let endpoints = MetadataEndpoints::for_testing("http://localhost:8080");
        assert_eq!(endpoints.aws_base_url, "http://localhost:8080");
        assert_eq!(endpoints.gcp_base_url, "http://localhost:8080");
        assert_eq!(endpoints.azure_base_url, "http://localhost:8080");
    }

    #[test]
    fn test_metadata_endpoints_for_testing_strips_trailing_slash() {
        let endpoints = MetadataEndpoints::for_testing("http://localhost:8080/");
        assert_eq!(endpoints.aws_base_url, "http://localhost:8080");
    }

    #[test]
    fn test_metadata_endpoints_for_testing_separate() {
        let endpoints = MetadataEndpoints::for_testing_separate(
            "http://aws:8080",
            "http://gcp:8081",
            "http://azure:8082",
        );
        assert_eq!(endpoints.aws_base_url, "http://aws:8080");
        assert_eq!(endpoints.gcp_base_url, "http://gcp:8081");
        assert_eq!(endpoints.azure_base_url, "http://azure:8082");
    }

    #[test]
    fn test_cloud_discovery_for_provider() {
        let config = CloudConfig::default();
        let discovery = CloudDiscovery::for_provider(CloudProvider::Aws, config);
        assert_eq!(discovery.provider(), CloudProvider::Aws);
        assert_eq!(discovery.name(), "cloud:aws");
    }

    #[test]
    fn test_cloud_discovery_with_self_addr() {
        let config = CloudConfig::default();
        let addr: SocketAddr = "10.0.0.1:9001".parse().unwrap();
        let discovery =
            CloudDiscovery::for_provider(CloudProvider::Aws, config).with_self_addr(addr);
        assert_eq!(discovery.self_addr, Some(addr));
    }
}
