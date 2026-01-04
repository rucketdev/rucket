//! Configuration management for Rucket.
//!
//! Following 12-factor app principles, configuration can be loaded from:
//! 1. Compiled-in defaults
//! 2. TOML configuration file
//! 3. Environment variables (override everything)
//!
//! # Environment Variables
//!
//! All configuration can be overridden via environment variables using the pattern:
//! `RUCKET__<SECTION>__<FIELD>` (double underscore for nesting).
//!
//! Examples:
//! - `RUCKET__SERVER__BIND=0.0.0.0:9000`
//! - `RUCKET__STORAGE__DATA_DIR=/var/lib/rucket`
//! - `RUCKET__AUTH__ACCESS_KEY=mykey`
//! - `RUCKET__AUTH__SECRET_KEY=mysecret`
//! - `RUCKET__LOGGING__LEVEL=debug`
//! - `RUCKET__METRICS__ENABLED=false`
//! - `RUCKET__API__COMPATIBILITY_MODE=ceph`
//! - `RUCKET__STORAGE__SYNC__DATA=always`
//! - `RUCKET__STORAGE__WAL__ENABLED=false`
//! - `RUCKET__STORAGE__ENCRYPTION__ENABLED=true`
//! - `RUCKET__STORAGE__ENCRYPTION__MASTER_KEY=<64-char hex key>`

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use serde::{Deserialize, Serialize};

/// Main configuration for Rucket server.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    /// Server configuration.
    pub server: ServerConfig,
    /// Storage configuration.
    pub storage: StorageConfig,
    /// Authentication configuration.
    pub auth: AuthConfig,
    /// Bucket configuration.
    pub bucket: BucketConfig,
    /// Logging configuration.
    pub logging: LoggingConfig,
    /// Metrics configuration.
    pub metrics: MetricsConfig,
    /// API configuration.
    pub api: ApiConfig,
    /// Cluster configuration for distributed mode.
    pub cluster: ClusterConfig,
}

impl Config {
    /// Load configuration from optional TOML file with environment variable overrides.
    ///
    /// Configuration is loaded in this order (later sources override earlier):
    /// 1. Compiled-in defaults
    /// 2. TOML file (if path provided, or found at default locations)
    /// 3. Environment variables with `RUCKET__` prefix
    ///
    /// # Environment Variable Naming
    ///
    /// Variables use uppercase with double underscore for nesting:
    /// - `RUCKET__SERVER__BIND` → `server.bind`
    /// - `RUCKET__STORAGE__DATA_DIR` → `storage.data_dir`
    /// - `RUCKET__AUTH__ACCESS_KEY` → `auth.access_key`
    /// - `RUCKET__STORAGE__SYNC__DATA` → `storage.sync.data`
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration cannot be loaded or parsed.
    pub fn load(path: Option<&Path>) -> crate::Result<Self> {
        // Start with defaults
        let mut figment = Figment::from(Serialized::defaults(Config::default()));

        // Add TOML file if specified or found
        if let Some(p) = path {
            figment = figment.merge(Toml::file(p));
        } else {
            // Try default locations
            for p in &["rucket.toml", "/etc/rucket/rucket.toml"] {
                if Path::new(p).exists() {
                    figment = figment.merge(Toml::file(p));
                    break;
                }
            }
        }

        // Environment variables override everything
        // Uses double underscore (__) for nesting: RUCKET__SERVER__BIND → server.bind
        figment = figment.merge(Env::prefixed("RUCKET__").split("__"));

        figment.extract().map_err(|e| crate::Error::Config(e.to_string()))
    }

    /// Load configuration from a TOML file (without environment variable overrides).
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn from_file(path: &Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(crate::Error::Io)?;
        toml::from_str(&content).map_err(|e| crate::Error::Config(e.to_string()))
    }

    /// Load configuration from a TOML string (without environment variable overrides).
    ///
    /// # Errors
    ///
    /// Returns an error if the string cannot be parsed.
    pub fn parse(content: &str) -> crate::Result<Self> {
        toml::from_str(content).map_err(|e| crate::Error::Config(e.to_string()))
    }
}

/// Server configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ServerConfig {
    /// Address to bind the server to.
    pub bind: SocketAddr,
    /// Path to TLS certificate file (optional).
    pub tls_cert: Option<PathBuf>,
    /// Path to TLS private key file (optional).
    pub tls_key: Option<PathBuf>,
    /// Maximum request body size in bytes.
    /// S3 supports up to 5GB for single PUT, but this can be tuned.
    /// Default: 5 GiB (5368709120 bytes).
    /// Set to 0 for unlimited.
    pub max_body_size: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9000".parse().expect("valid default address"),
            tls_cert: None,
            tls_key: None,
            max_body_size: 5 * 1024 * 1024 * 1024, // 5 GiB (S3 max single PUT)
        }
    }
}

/// Storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct StorageConfig {
    /// Directory to store object data.
    pub data_dir: PathBuf,
    /// Directory for temporary files (defaults to data_dir/.tmp).
    pub temp_dir: Option<PathBuf>,
    /// Sync configuration for durability vs performance tuning.
    pub sync: SyncConfig,
    /// redb database configuration.
    pub redb: RedbConfig,
    /// Write-ahead log configuration.
    pub wal: WalConfig,
    /// Server-side encryption configuration.
    pub encryption: EncryptionConfig,
}

/// Server-side encryption (SSE-S3) configuration.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct EncryptionConfig {
    /// Enable server-side encryption at rest.
    /// When enabled, all objects are encrypted using AES-256-GCM.
    pub enabled: bool,
    /// Master encryption key (hex-encoded, 64 characters for 32 bytes).
    /// Required when encryption is enabled.
    /// Generate with: `openssl rand -hex 32`
    pub master_key: Option<String>,
}

/// redb database configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RedbConfig {
    /// Cache size in bytes for the database.
    /// Default: 64 MiB (67108864 bytes).
    pub cache_size_bytes: u64,
}

impl Default for RedbConfig {
    fn default() -> Self {
        Self {
            cache_size_bytes: 64 * 1024 * 1024, // 64 MiB
        }
    }
}

impl RedbConfig {
    /// Configuration optimized for maximum durability.
    /// Uses immediate sync (fsync every commit).
    #[must_use]
    pub fn durable() -> Self {
        Self::default()
    }

    /// Configuration optimized for maximum performance.
    #[must_use]
    pub fn fast() -> Self {
        Self {
            cache_size_bytes: 128 * 1024 * 1024, // 128 MiB cache
        }
    }
}

/// Write-Ahead Log (WAL) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct WalConfig {
    /// Enable WAL for crash recovery.
    pub enabled: bool,
    /// Sync mode for WAL writes.
    pub sync_mode: WalSyncMode,
    /// Recovery mode for startup crash recovery.
    pub recovery_mode: RecoveryMode,
    /// Checkpoint configuration.
    pub checkpoint: CheckpointConfig,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            sync_mode: WalSyncMode::Fdatasync,
            recovery_mode: RecoveryMode::Light,
            checkpoint: CheckpointConfig::default(),
        }
    }
}

impl WalConfig {
    /// Configuration with WAL disabled.
    #[must_use]
    pub fn disabled() -> Self {
        Self { enabled: false, ..Default::default() }
    }

    /// Configuration for maximum durability.
    #[must_use]
    pub fn durable() -> Self {
        Self { enabled: true, sync_mode: WalSyncMode::Fsync, ..Default::default() }
    }
}

/// WAL sync mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum WalSyncMode {
    /// No explicit sync - rely on OS flush.
    None,
    /// Use fdatasync (faster, doesn't sync file metadata).
    #[default]
    Fdatasync,
    /// Use full fsync (slower, syncs all metadata).
    Fsync,
}

/// Recovery mode for startup crash recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryMode {
    /// Light recovery: WAL-based recovery only (fast).
    ///
    /// Replays WAL to identify incomplete operations and rolls them back.
    /// Does not scan filesystem or verify checksums.
    #[default]
    Light,

    /// Full recovery: WAL + orphan scan + checksum verification (thorough).
    ///
    /// In addition to WAL recovery:
    /// - Scans all data files for orphans (files without metadata)
    /// - Verifies CRC32C checksums of all objects
    /// - Reports corrupted files (does not delete them)
    ///
    /// This mode is slower but catches corruption that occurred outside
    /// of normal operations (e.g., disk errors, manual file edits).
    Full,
}

/// Checkpoint configuration for WAL.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct CheckpointConfig {
    /// Checkpoint after this many WAL entries.
    pub entries_threshold: u64,
    /// Checkpoint after this many bytes written to WAL.
    pub bytes_threshold: u64,
    /// Checkpoint after this duration (milliseconds).
    pub interval_ms: u64,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            entries_threshold: 10_000,
            bytes_threshold: 64 * 1024 * 1024, // 64 MB
            interval_ms: 60_000,               // 1 minute
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            temp_dir: None,
            sync: SyncConfig::default(),
            redb: RedbConfig::default(),
            wal: WalConfig::default(),
            encryption: EncryptionConfig::default(),
        }
    }
}

impl StorageConfig {
    /// Returns the temporary directory path.
    #[must_use]
    pub fn temp_dir(&self) -> PathBuf {
        self.temp_dir.clone().unwrap_or_else(|| self.data_dir.join(".tmp"))
    }
}

/// Named durability presets for simple configuration.
///
/// These presets provide a simple way to configure durability vs performance trade-offs.
/// Use these instead of fine-grained `SyncConfig` settings for most use cases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum DurabilityPreset {
    /// Maximum performance, may lose recent writes on crash.
    ///
    /// - No fsync (relies on OS ~30s flush)
    /// - No checksums verified on read
    /// - Aggressive write batching
    /// - Best for: Caching, temporary data, development
    Performance,

    /// Balanced mode (default) - bounded data loss window.
    ///
    /// - fdatasync every 1s or 10MB or 100 ops
    /// - Checksums computed on write (not verified on read)
    /// - Moderate write batching
    /// - Best for: General purpose, most production workloads
    #[default]
    Balanced,

    /// Maximum durability, slower writes.
    ///
    /// - fdatasync every write
    /// - Checksums verified on every read
    /// - Directory fsync after rename
    /// - No write batching
    /// - Best for: Critical data, financial records, compliance
    Durable,
}

impl DurabilityPreset {
    /// Convert this preset to a `SyncConfig`.
    #[must_use]
    pub fn to_sync_config(self) -> SyncConfig {
        match self {
            Self::Performance => SyncConfig {
                data: SyncStrategy::None,
                metadata: SyncStrategy::Periodic,
                interval_ms: 5000,
                batch: BatchConfig::aggressive(),
                verify_checksums_on_read: false,
                ..Default::default()
            },
            Self::Balanced => {
                SyncConfig { verify_checksums_on_read: false, ..SyncConfig::default() }
            }
            Self::Durable => SyncConfig {
                data: SyncStrategy::Always,
                metadata: SyncStrategy::Always,
                batch: BatchConfig::disabled(),
                verify_checksums_on_read: true, // Verify on read in Durable mode
                ..Default::default()
            },
        }
    }

    /// Whether checksums should be verified on read.
    #[must_use]
    pub fn verify_checksums_on_read(self) -> bool {
        matches!(self, Self::Durable)
    }

    /// Whether directory fsync should be performed after rename.
    #[must_use]
    pub fn sync_directory_after_rename(self) -> bool {
        matches!(self, Self::Durable)
    }
}

/// Sync strategy for controlling when data is flushed to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum SyncStrategy {
    /// Never explicitly sync - rely on OS to flush.
    /// Fastest, but data may be lost on crash.
    None,
    /// Sync periodically based on time interval OR when thresholds are reached.
    /// Syncs when: time interval elapses, OR bytes threshold reached, OR ops threshold reached.
    /// Good balance of performance and durability with bounded data loss window.
    #[default]
    Periodic,
    /// Sync after a threshold of bytes written or operations completed.
    /// Similar to Periodic but without time-based background sync.
    Threshold,
    /// Sync after every write operation.
    /// Slowest, but guarantees durability.
    Always,
}

/// Sync configuration for fine-grained durability control.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SyncConfig {
    /// Sync strategy for object data files.
    pub data: SyncStrategy,
    /// Sync strategy for metadata (redb database).
    pub metadata: SyncStrategy,
    /// Interval in milliseconds for periodic sync strategy.
    /// Only used when strategy is `Periodic`.
    pub interval_ms: u64,
    /// Bytes threshold for threshold-based sync.
    /// Sync after this many bytes are written.
    pub bytes_threshold: u64,
    /// Operations threshold for threshold-based sync.
    /// Sync after this many write operations complete.
    pub ops_threshold: u32,
    /// Maximum idle time in milliseconds for threshold-based sync.
    /// If no writes occur within this window, dirty data is flushed.
    /// Prevents data from staying dirty forever if activity stops.
    /// Only used when strategy is `Threshold`.
    pub max_idle_ms: u64,
    /// Minimum file size in bytes to use direct I/O (O_DIRECT).
    /// Set to 0 to disable. Only effective on Linux.
    /// Direct I/O bypasses the page cache for large sequential writes.
    pub direct_io_min_size: u64,
    /// Batch configuration for group commit.
    /// Batching multiple writes with a single fsync improves throughput.
    pub batch: BatchConfig,
    /// Verify CRC32C checksums on read operations.
    ///
    /// When enabled, reads will compute the CRC32C of the data and compare
    /// it to the stored checksum. If they don't match, an error is returned.
    ///
    /// Default: `false` (only enabled in Durable preset).
    /// Enable this for maximum data integrity at the cost of read performance.
    pub verify_checksums_on_read: bool,
}

/// Configuration for batched write operations (group commit).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BatchConfig {
    /// Enable batched writes (group commit).
    pub enabled: bool,
    /// Maximum number of writes to batch before flushing.
    pub max_batch_size: usize,
    /// Maximum bytes to accumulate before flushing.
    pub max_batch_bytes: u64,
    /// Maximum delay in milliseconds before flushing.
    pub max_batch_delay_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_batch_size: 64,
            max_batch_bytes: 16 * 1024 * 1024, // 16 MB
            max_batch_delay_ms: 10,
        }
    }
}

impl BatchConfig {
    /// Configuration for aggressive batching (maximum performance).
    #[must_use]
    pub fn aggressive() -> Self {
        Self {
            enabled: true,
            max_batch_size: 256,
            max_batch_bytes: 64 * 1024 * 1024, // 64 MB
            max_batch_delay_ms: 50,
        }
    }

    /// Disable batching (sync each write individually).
    #[must_use]
    pub fn disabled() -> Self {
        Self { enabled: false, max_batch_size: 1, max_batch_bytes: 0, max_batch_delay_ms: 0 }
    }
}

impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            // Default: periodic sync for data, always sync metadata
            data: SyncStrategy::Periodic,
            metadata: SyncStrategy::Always,
            interval_ms: 1000,                 // 1 second
            bytes_threshold: 10 * 1024 * 1024, // 10 MB
            ops_threshold: 100,
            max_idle_ms: 5000,     // 5 seconds
            direct_io_min_size: 0, // Disabled by default
            batch: BatchConfig::default(),
            verify_checksums_on_read: false, // Only enabled in Durable preset
        }
    }
}

impl SyncConfig {
    /// Configuration that never explicitly syncs - relies on OS.
    /// Maximum performance but risk of data loss on crash.
    #[must_use]
    pub fn never() -> Self {
        Self {
            data: SyncStrategy::None,
            metadata: SyncStrategy::Periodic,
            interval_ms: 5000,
            ..Default::default()
        }
    }

    /// Configuration with periodic sync.
    /// Syncs when: time interval elapses (1s), bytes threshold reached (10MB), or ops threshold reached (100).
    /// Good balance for typical workloads with bounded data loss window.
    #[must_use]
    pub fn periodic() -> Self {
        Self::default()
    }

    /// Configuration that always syncs after every operation.
    /// Slowest but maximum durability.
    #[must_use]
    pub fn always() -> Self {
        Self {
            data: SyncStrategy::Always,
            metadata: SyncStrategy::Always,
            batch: BatchConfig::disabled(), // No batching for maximum durability
            ..Default::default()
        }
    }
}

/// Authentication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AuthConfig {
    /// Access key ID.
    pub access_key: String,
    /// Secret access key.
    pub secret_key: String,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self { access_key: "rucket".to_string(), secret_key: "rucket123".to_string() }
    }
}

/// Bucket naming rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum BucketNamingRules {
    /// Strict S3-compatible naming (DNS-compatible).
    Strict,
    /// Relaxed naming rules allowing more characters.
    #[default]
    Relaxed,
}

/// Bucket configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct BucketConfig {
    /// Bucket naming rules.
    pub naming_rules: BucketNamingRules,
}

impl Default for BucketConfig {
    fn default() -> Self {
        Self { naming_rules: BucketNamingRules::Relaxed }
    }
}

/// Log output format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable format.
    #[default]
    Pretty,
    /// JSON format.
    Json,
}

/// Logging configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LoggingConfig {
    /// Log level (trace, debug, info, warn, error).
    pub level: String,
    /// Log output format.
    pub format: LogFormat,
    /// Include HTTP request/response logging.
    pub log_requests: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self { level: "info".to_string(), format: LogFormat::Pretty, log_requests: true }
    }
}

/// Metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MetricsConfig {
    /// Enable metrics collection and endpoint.
    pub enabled: bool,
    /// Port for the metrics endpoint (separate from main server).
    pub port: u16,
    /// Bind address for metrics server.
    pub bind: String,
    /// Include storage metrics (bucket counts, object counts, bytes).
    /// These require periodic metadata scans.
    pub include_storage_metrics: bool,
    /// Interval in seconds for storage metrics refresh.
    pub storage_metrics_interval_secs: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9001,
            bind: "0.0.0.0".to_string(),
            include_storage_metrics: true,
            storage_metrics_interval_secs: 60,
        }
    }
}

/// API compatibility mode.
///
/// Controls which API endpoints are available.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "kebab-case")]
pub enum ApiCompatibilityMode {
    /// Strict S3 API compatibility only.
    ///
    /// Only standard S3 API endpoints are available.
    /// Use this for maximum compatibility with S3 clients.
    S3Strict,

    /// S3 API plus MinIO-specific extensions.
    ///
    /// Includes additional MinIO endpoints:
    /// - `/minio/health/live` - Liveness probe
    /// - `/minio/health/ready` - Readiness probe
    ///
    /// Use this for compatibility with MinIO clients and tools.
    #[default]
    Minio,

    /// S3 API with Ceph RGW compatibility extensions.
    ///
    /// Enables Ceph-specific behaviors and endpoints for compatibility
    /// with Ceph RGW clients and the ceph/s3-tests test suite.
    ///
    /// Includes:
    /// - Full versioning support with list_object_versions
    /// - Delete markers handling
    /// - Ceph-compatible response formats
    Ceph,
}

/// API configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ApiConfig {
    /// API compatibility mode.
    pub compatibility_mode: ApiCompatibilityMode,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self { compatibility_mode: ApiCompatibilityMode::Minio }
    }
}

/// Cluster configuration for distributed mode.
///
/// When `enabled` is `true`, Rucket operates as part of a distributed cluster
/// using Raft consensus for metadata operations.
///
/// # Example Configuration
///
/// ```toml
/// [cluster]
/// enabled = true
/// node_id = 1
/// bind_cluster = "0.0.0.0:9001"
/// peers = ["node2:9001", "node3:9001"]
///
/// [cluster.raft]
/// heartbeat_interval_ms = 150
/// election_timeout_min_ms = 300
/// election_timeout_max_ms = 600
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ClusterConfig {
    /// Whether clustering is enabled.
    ///
    /// When `false`, Rucket operates in single-node mode (default).
    pub enabled: bool,

    /// This node's unique ID.
    ///
    /// Must be unique across the cluster and stable across restarts.
    /// IDs are u64 values; common practice is to use 1, 2, 3, etc.
    pub node_id: u64,

    /// Address for Raft RPC communication.
    ///
    /// This is the address other nodes use to reach this node for
    /// Raft messages (vote requests, append entries, snapshots).
    pub bind_cluster: SocketAddr,

    /// Initial peer addresses for cluster formation.
    ///
    /// Specify as `["host1:port", "host2:port"]`.
    /// Used when discovery is not configured.
    pub peers: Vec<String>,

    /// Service discovery configuration.
    ///
    /// Alternative to static `peers` list. Supports DNS and Kubernetes
    /// discovery methods.
    pub discover: Option<DiscoveryConfig>,

    /// Bootstrap configuration for new cluster formation.
    ///
    /// Only the first node in a new cluster should have `bootstrap.enabled = true`.
    pub bootstrap: Option<BootstrapConfig>,

    /// Raft timing configuration.
    pub raft: RaftTimingConfig,

    /// Path for Raft log storage.
    ///
    /// The Raft log is stored separately from the metadata database
    /// for better performance and isolation.
    pub raft_log_dir: PathBuf,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            node_id: 1,
            bind_cluster: "127.0.0.1:9001".parse().expect("valid default address"),
            peers: Vec::new(),
            discover: None,
            bootstrap: None,
            raft: RaftTimingConfig::default(),
            raft_log_dir: PathBuf::from("./data/raft"),
        }
    }
}

impl ClusterConfig {
    /// Creates a new cluster config with the given node ID.
    #[must_use]
    pub fn new(node_id: u64) -> Self {
        Self { node_id, enabled: true, ..Default::default() }
    }

    /// Sets the bind address for cluster communication.
    #[must_use]
    pub fn with_bind_addr(mut self, addr: SocketAddr) -> Self {
        self.bind_cluster = addr;
        self
    }

    /// Sets the peer addresses.
    #[must_use]
    pub fn with_peers(mut self, peers: Vec<String>) -> Self {
        self.peers = peers;
        self
    }

    /// Enables bootstrap mode.
    #[must_use]
    pub fn with_bootstrap(mut self, expect_nodes: u32) -> Self {
        self.bootstrap = Some(BootstrapConfig { enabled: true, expect_nodes, timeout_secs: 60 });
        self
    }

    /// Returns whether this node should bootstrap a new cluster.
    #[must_use]
    pub fn should_bootstrap(&self) -> bool {
        self.bootstrap.as_ref().is_some_and(|b| b.enabled)
    }

    /// Returns the expected number of nodes for bootstrap.
    #[must_use]
    pub fn expected_nodes(&self) -> u32 {
        self.bootstrap.as_ref().map_or(1, |b| b.expect_nodes)
    }
}

/// Bootstrap configuration for initial cluster formation.
///
/// Bootstrap mode is used when forming a new cluster. The first node
/// waits for the expected number of nodes to be available before
/// initializing the Raft group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapConfig {
    /// Whether this node should bootstrap a new cluster.
    ///
    /// Only ONE node in the cluster should have this set to `true`.
    pub enabled: bool,

    /// Expected number of nodes before forming the cluster.
    ///
    /// The bootstrap node waits until this many nodes are discovered
    /// before initializing Raft. For a 3-node cluster, set to 3.
    pub expect_nodes: u32,

    /// Timeout in seconds for waiting for expected nodes.
    ///
    /// If the expected nodes aren't discovered within this time,
    /// bootstrap fails.
    pub timeout_secs: u64,
}

/// Service discovery configuration.
///
/// Provides automatic peer discovery as an alternative to static
/// peer lists. Supports multiple discovery mechanisms from traditional
/// infrastructure-based (DNS, Kubernetes) to SPOF-free (gossip).
///
/// # Examples
///
/// ```toml
/// # DNS-based discovery
/// [cluster.discover]
/// type = "dns"
/// hostname = "rucket.local"
/// port = 9001
///
/// # Gossip-based discovery (SPOF-free)
/// [cluster.discover]
/// type = "gossip"
/// bind = "0.0.0.0:9002"
/// bootstrap_peers = ["seed1:9002", "seed2:9002"]
///
/// # Auto-detect cloud provider with AWS-specific settings
/// [cluster.discover]
/// type = "cloud"
/// cluster_tag = "rucket:cluster"
/// cluster_value = "production"
/// aws_use_imdsv2 = true
/// aws_region = "us-west-2"
///
/// # Explicit AWS (skip auto-detection)
/// [cluster.discover]
/// type = "aws"
/// cluster_tag = "rucket:cluster"
/// cluster_value = "production"
/// use_imdsv2 = true
/// region = "us-west-2"
///
/// # Explicit GCP
/// [cluster.discover]
/// type = "gcp"
/// cluster_label = "rucket-cluster"
/// cluster_value = "production"
/// project_id = "my-project"
///
/// # Explicit Azure
/// [cluster.discover]
/// type = "azure"
/// cluster_tag = "rucket:cluster"
/// cluster_value = "production"
/// resource_group = "my-rg"
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DiscoveryConfig {
    /// DNS-based discovery using SRV or A records.
    ///
    /// Queries the specified hostname and uses the results as peer
    /// addresses. Works with any DNS infrastructure including
    /// Kubernetes CoreDNS.
    Dns {
        /// DNS hostname to query (e.g., "rucket.local").
        hostname: String,
        /// Port to use for discovered hosts (for A records).
        port: u16,
        /// Whether to use SRV records (port comes from record).
        #[serde(default)]
        use_srv: bool,
    },

    /// Kubernetes headless service discovery.
    ///
    /// Uses the Kubernetes API to discover pod IPs from a headless
    /// service.
    Kubernetes {
        /// Service name.
        service: String,
        /// Namespace (defaults to current namespace from env).
        namespace: Option<String>,
        /// Port name or number.
        port: String,
    },

    /// Gossip-based discovery using SWIM protocol.
    ///
    /// Decentralized, SPOF-free discovery where nodes discover each
    /// other through epidemic protocol. Requires at least one bootstrap
    /// peer to join an existing cluster.
    Gossip {
        /// Address to bind the gossip listener.
        bind: SocketAddr,
        /// Initial peers to contact for joining the gossip network.
        bootstrap_peers: Vec<String>,
        /// Whether to encrypt gossip traffic.
        #[serde(default)]
        encrypt: bool,
        /// Encryption key (32-byte hex string) if encrypt is true.
        secret_key: Option<String>,
    },

    /// Auto-detecting cloud provider discovery.
    ///
    /// Automatically detects whether running on AWS, GCP, or Azure
    /// and uses the appropriate discovery mechanism (EC2 tags,
    /// instance labels, resource tags). Provider-specific settings
    /// are prefixed with `aws_`, `gcp_`, or `azure_`.
    Cloud {
        /// Tag/label key to identify cluster members.
        #[serde(default = "default_cluster_tag")]
        cluster_tag: String,
        /// Tag/label value to match.
        cluster_value: String,
        /// Port for Raft RPC communication.
        #[serde(default = "default_raft_port")]
        raft_port: u16,
        /// Force a specific cloud provider instead of auto-detecting.
        /// Values: "aws", "gcp", "azure"
        force_provider: Option<String>,

        // AWS-specific settings (apply when AWS is detected)
        /// Use IMDSv2 for metadata access (more secure). Default: true.
        #[serde(default)]
        aws_use_imdsv2: Option<bool>,
        /// Override auto-detected AWS region.
        #[serde(default)]
        aws_region: Option<String>,

        // GCP-specific settings (apply when GCP is detected)
        /// Override auto-detected GCP project ID.
        #[serde(default)]
        gcp_project_id: Option<String>,
        /// Filter discovery to specific GCP zone.
        #[serde(default)]
        gcp_zone: Option<String>,

        // Azure-specific settings (apply when Azure is detected)
        /// Override auto-detected Azure resource group.
        #[serde(default)]
        azure_resource_group: Option<String>,
        /// Override auto-detected Azure subscription ID.
        #[serde(default)]
        azure_subscription_id: Option<String>,
    },

    /// AWS EC2 discovery using instance tags.
    ///
    /// Explicitly targets AWS without auto-detection. Discovers
    /// cluster members by querying EC2 instances with matching tags.
    Aws {
        /// EC2 tag key to identify cluster members.
        #[serde(default = "default_cluster_tag")]
        cluster_tag: String,
        /// EC2 tag value to match.
        cluster_value: String,
        /// Port for Raft RPC communication.
        #[serde(default = "default_raft_port")]
        raft_port: u16,
        /// Use IMDSv2 for metadata access (more secure). Default: true.
        #[serde(default = "default_true")]
        use_imdsv2: bool,
        /// Override auto-detected AWS region.
        #[serde(default)]
        region: Option<String>,
    },

    /// GCP Compute Engine discovery using instance labels.
    ///
    /// Explicitly targets GCP without auto-detection. Discovers
    /// cluster members by querying VM instances with matching labels.
    Gcp {
        /// Instance label key to identify cluster members.
        #[serde(default = "default_cluster_tag")]
        cluster_label: String,
        /// Instance label value to match.
        cluster_value: String,
        /// Port for Raft RPC communication.
        #[serde(default = "default_raft_port")]
        raft_port: u16,
        /// Override auto-detected GCP project ID.
        #[serde(default)]
        project_id: Option<String>,
        /// Filter discovery to specific GCP zone.
        #[serde(default)]
        zone: Option<String>,
    },

    /// Azure VM discovery using resource tags.
    ///
    /// Explicitly targets Azure without auto-detection. Discovers
    /// cluster members by querying VMs with matching tags.
    Azure {
        /// Resource tag key to identify cluster members.
        #[serde(default = "default_cluster_tag")]
        cluster_tag: String,
        /// Resource tag value to match.
        cluster_value: String,
        /// Port for Raft RPC communication.
        #[serde(default = "default_raft_port")]
        raft_port: u16,
        /// Override auto-detected Azure resource group.
        #[serde(default)]
        resource_group: Option<String>,
        /// Override auto-detected Azure subscription ID.
        #[serde(default)]
        subscription_id: Option<String>,
    },

    /// mDNS/DNS-SD discovery for local networks.
    ///
    /// Zero-configuration discovery using multicast DNS. Suitable
    /// for development, IoT, and LAN deployments.
    #[serde(rename = "mdns")]
    MDns {
        /// Service name to advertise/discover.
        #[serde(default = "default_mdns_service")]
        service_name: String,
        /// Port for Raft RPC communication.
        #[serde(default = "default_raft_port")]
        raft_port: u16,
    },
}

fn default_cluster_tag() -> String {
    "rucket:cluster".to_string()
}

fn default_raft_port() -> u16 {
    9001
}

fn default_true() -> bool {
    true
}

fn default_mdns_service() -> String {
    "_rucket._tcp.local".to_string()
}

/// Raft timing parameters.
///
/// These values control leader election and heartbeat behavior.
/// The defaults are suitable for most deployments.
///
/// # Tuning Guidelines
///
/// - `heartbeat_interval_ms`: Should be much less than election timeout.
///   Lower values detect failures faster but increase network traffic.
///
/// - Election timeouts: Should be 2-3x the heartbeat interval to avoid
///   unnecessary elections during brief network hiccups.
///
/// - For high-latency networks (cross-region), increase all values
///   proportionally.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RaftTimingConfig {
    /// Heartbeat interval in milliseconds.
    ///
    /// The leader sends heartbeats to followers at this interval.
    /// Default: 150ms
    pub heartbeat_interval_ms: u64,

    /// Minimum election timeout in milliseconds.
    ///
    /// A follower starts an election if it hasn't heard from the leader
    /// within a random timeout between min and max.
    /// Default: 300ms
    pub election_timeout_min_ms: u64,

    /// Maximum election timeout in milliseconds.
    ///
    /// Default: 600ms
    pub election_timeout_max_ms: u64,

    /// Maximum entries per AppendEntries RPC.
    ///
    /// Limits the batch size for log replication. Higher values can
    /// improve throughput but increase memory usage.
    /// Default: 300
    pub max_payload_entries: u64,

    /// Snapshot interval in number of log entries.
    ///
    /// A snapshot is taken every N committed entries to compact the log.
    /// Default: 10000
    pub snapshot_interval: u64,
}

impl Default for RaftTimingConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval_ms: 150,
            election_timeout_min_ms: 300,
            election_timeout_max_ms: 600,
            max_payload_entries: 300,
            snapshot_interval: 10000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.bind.port(), 9000);
        assert_eq!(config.auth.access_key, "rucket");
        assert_eq!(config.bucket.naming_rules, BucketNamingRules::Relaxed);
        assert_eq!(config.api.compatibility_mode, ApiCompatibilityMode::Minio);
    }

    #[test]
    fn test_parse_config() {
        let toml = r#"
[server]
bind = "0.0.0.0:8080"

[storage]
data_dir = "/var/lib/rucket"

[auth]
access_key = "mykey"
secret_key = "mysecret"

[bucket]
naming_rules = "strict"

[logging]
level = "debug"
format = "json"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.server.bind.port(), 8080);
        assert_eq!(config.auth.access_key, "mykey");
        assert_eq!(config.bucket.naming_rules, BucketNamingRules::Strict);
        assert_eq!(config.logging.format, LogFormat::Json);
    }

    #[test]
    fn test_durability_preset_performance() {
        let preset = DurabilityPreset::Performance;
        let config = preset.to_sync_config();

        assert_eq!(config.data, SyncStrategy::None);
        assert!(config.batch.enabled);
        assert_eq!(config.batch.max_batch_size, 256); // Aggressive batching
        assert!(!preset.verify_checksums_on_read());
        assert!(!preset.sync_directory_after_rename());
    }

    #[test]
    fn test_durability_preset_balanced() {
        let preset = DurabilityPreset::Balanced;
        let config = preset.to_sync_config();

        assert_eq!(config.data, SyncStrategy::Periodic);
        assert!(config.batch.enabled);
        assert!(!preset.verify_checksums_on_read());
        assert!(!preset.sync_directory_after_rename());
    }

    #[test]
    fn test_durability_preset_durable() {
        let preset = DurabilityPreset::Durable;
        let config = preset.to_sync_config();

        assert_eq!(config.data, SyncStrategy::Always);
        assert!(!config.batch.enabled); // No batching for durability
        assert!(preset.verify_checksums_on_read());
        assert!(preset.sync_directory_after_rename());
    }

    #[test]
    fn test_default_durability_preset() {
        let preset = DurabilityPreset::default();
        assert_eq!(preset, DurabilityPreset::Balanced);
    }

    #[test]
    fn test_api_compatibility_mode_s3_strict() {
        let toml = r#"
[api]
compatibility_mode = "s3-strict"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.api.compatibility_mode, ApiCompatibilityMode::S3Strict);
    }

    #[test]
    fn test_api_compatibility_mode_minio() {
        let toml = r#"
[api]
compatibility_mode = "minio"
"#;
        let config = Config::parse(toml).unwrap();
        assert_eq!(config.api.compatibility_mode, ApiCompatibilityMode::Minio);
    }

    mod env_override_tests {
        use std::sync::Mutex;

        use super::*;

        // Use a mutex to prevent parallel env var tests from interfering
        static ENV_MUTEX: Mutex<()> = Mutex::new(());

        fn with_env_var<F, R>(key: &str, value: &str, f: F) -> R
        where
            F: FnOnce() -> R,
        {
            let _guard = ENV_MUTEX.lock().unwrap();
            let original = std::env::var(key).ok();
            std::env::set_var(key, value);
            let result = f();
            match original {
                Some(v) => std::env::set_var(key, v),
                None => std::env::remove_var(key),
            }
            result
        }

        fn with_env_vars<F, R>(vars: &[(&str, &str)], f: F) -> R
        where
            F: FnOnce() -> R,
        {
            let _guard = ENV_MUTEX.lock().unwrap();
            let originals: Vec<_> = vars.iter().map(|(k, _)| (*k, std::env::var(k).ok())).collect();

            for (key, value) in vars {
                std::env::set_var(key, value);
            }

            let result = f();

            for (key, original) in originals {
                match original {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
            result
        }

        #[test]
        fn test_env_override_bind() {
            with_env_var("RUCKET__SERVER__BIND", "0.0.0.0:8080", || {
                let config = Config::load(None).unwrap();
                assert_eq!(config.server.bind.to_string(), "0.0.0.0:8080");
            });
        }

        #[test]
        fn test_env_override_data_dir() {
            with_env_var("RUCKET__STORAGE__DATA_DIR", "/custom/data", || {
                let config = Config::load(None).unwrap();
                assert_eq!(config.storage.data_dir, PathBuf::from("/custom/data"));
            });
        }

        #[test]
        fn test_env_override_access_key() {
            with_env_var("RUCKET__AUTH__ACCESS_KEY", "myaccesskey", || {
                let config = Config::load(None).unwrap();
                assert_eq!(config.auth.access_key, "myaccesskey");
            });
        }

        #[test]
        fn test_env_override_secret_key() {
            with_env_var("RUCKET__AUTH__SECRET_KEY", "mysecretkey", || {
                let config = Config::load(None).unwrap();
                assert_eq!(config.auth.secret_key, "mysecretkey");
            });
        }

        #[test]
        fn test_env_override_logging_level() {
            with_env_var("RUCKET__LOGGING__LEVEL", "debug", || {
                let config = Config::load(None).unwrap();
                assert_eq!(config.logging.level, "debug");
            });
        }

        #[test]
        fn test_env_override_metrics_enabled() {
            with_env_var("RUCKET__METRICS__ENABLED", "false", || {
                let config = Config::load(None).unwrap();
                assert!(!config.metrics.enabled);
            });
        }

        #[test]
        fn test_env_override_nested_sync_data() {
            with_env_var("RUCKET__STORAGE__SYNC__DATA", "always", || {
                let config = Config::load(None).unwrap();
                assert_eq!(config.storage.sync.data, SyncStrategy::Always);
            });
        }

        #[test]
        fn test_env_override_nested_wal_enabled() {
            with_env_var("RUCKET__STORAGE__WAL__ENABLED", "false", || {
                let config = Config::load(None).unwrap();
                assert!(!config.storage.wal.enabled);
            });
        }

        #[test]
        fn test_env_override_api_compatibility_mode() {
            with_env_var("RUCKET__API__COMPATIBILITY_MODE", "ceph", || {
                let config = Config::load(None).unwrap();
                assert_eq!(config.api.compatibility_mode, ApiCompatibilityMode::Ceph);
            });
        }

        #[test]
        fn test_env_override_preserves_other_fields() {
            with_env_var("RUCKET__AUTH__ACCESS_KEY", "newkey", || {
                let config = Config::load(None).unwrap();
                // Other fields should remain at defaults
                assert_eq!(config.server.bind.port(), 9000);
                assert_eq!(config.auth.secret_key, "rucket123");
                assert_eq!(config.storage.data_dir, PathBuf::from("./data"));
            });
        }

        #[test]
        fn test_env_override_multiple_vars() {
            with_env_vars(
                &[
                    ("RUCKET__SERVER__BIND", "0.0.0.0:8888"),
                    ("RUCKET__AUTH__ACCESS_KEY", "envkey"),
                    ("RUCKET__LOGGING__FORMAT", "json"),
                ],
                || {
                    let config = Config::load(None).unwrap();
                    assert_eq!(config.server.bind.to_string(), "0.0.0.0:8888");
                    assert_eq!(config.auth.access_key, "envkey");
                    assert_eq!(config.logging.format, LogFormat::Json);
                },
            );
        }
    }
}
