//! Configuration management for Rucket.

use std::net::SocketAddr;
use std::path::PathBuf;

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
}

impl Config {
    /// Load configuration from a TOML file.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be read or parsed.
    pub fn from_file(path: &std::path::Path) -> crate::Result<Self> {
        let content = std::fs::read_to_string(path).map_err(crate::Error::Io)?;
        toml::from_str(&content).map_err(|e| crate::Error::Config(e.to_string()))
    }

    /// Load configuration from a TOML string.
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
            Self::Balanced => SyncConfig {
                verify_checksums_on_read: false,
                ..SyncConfig::default()
            },
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
    /// Sync strategy for metadata (SQLite database).
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
}
