// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1:9000".parse().expect("valid default address"),
            tls_cert: None,
            tls_key: None,
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

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("./data"),
            temp_dir: None,
            sync: SyncConfig::default(),
            redb: RedbConfig::default(),
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
        Self { data: SyncStrategy::Always, metadata: SyncStrategy::Always, ..Default::default() }
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
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self { level: "info".to_string(), format: LogFormat::Pretty }
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
}
