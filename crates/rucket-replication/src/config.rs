//! Configuration for the replication subsystem.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::level::ReplicationLevel;

/// Default replication factor (number of copies including primary).
pub const DEFAULT_REPLICATION_FACTOR: usize = 3;

/// Default async replication queue size.
pub const DEFAULT_QUEUE_SIZE: usize = 10_000;

/// Default replication timeout in milliseconds.
pub const DEFAULT_TIMEOUT_MS: u64 = 5_000;

/// Default lag threshold before a replica is considered unhealthy (ms).
pub const DEFAULT_LAG_THRESHOLD_MS: u64 = 30_000;

/// Default batch size for async replication.
pub const DEFAULT_BATCH_SIZE: usize = 100;

/// Default interval between replication lag checks (ms).
pub const DEFAULT_LAG_CHECK_INTERVAL_MS: u64 = 1_000;

/// Configuration for the replication subsystem.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ReplicationConfig {
    /// Whether replication is enabled.
    pub enabled: bool,

    /// Default replication level for writes.
    pub default_level: ReplicationLevel,

    /// Replication factor (total number of copies including primary).
    ///
    /// With RF=3, data is stored on 3 nodes (1 primary + 2 backups).
    pub replication_factor: usize,

    /// Size of the async replication queue per replica.
    pub queue_size: usize,

    /// Timeout for sync replication operations in milliseconds.
    pub timeout_ms: u64,

    /// Lag threshold in milliseconds before a replica is considered unhealthy.
    pub lag_threshold_ms: u64,

    /// Number of items to batch together for async replication.
    pub batch_size: usize,

    /// Interval between replication lag checks in milliseconds.
    pub lag_check_interval_ms: u64,

    /// Whether to allow writes when quorum cannot be achieved.
    ///
    /// When true, writes with `Durable` level will fall back to
    /// `Replicated` level if quorum is unavailable.
    pub allow_degraded_writes: bool,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            default_level: ReplicationLevel::Replicated,
            replication_factor: DEFAULT_REPLICATION_FACTOR,
            queue_size: DEFAULT_QUEUE_SIZE,
            timeout_ms: DEFAULT_TIMEOUT_MS,
            lag_threshold_ms: DEFAULT_LAG_THRESHOLD_MS,
            batch_size: DEFAULT_BATCH_SIZE,
            lag_check_interval_ms: DEFAULT_LAG_CHECK_INTERVAL_MS,
            allow_degraded_writes: false,
        }
    }
}

impl ReplicationConfig {
    /// Creates a new replication configuration with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether replication is enabled.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Sets the default replication level.
    pub fn default_level(mut self, level: ReplicationLevel) -> Self {
        self.default_level = level;
        self
    }

    /// Sets the replication factor.
    pub fn replication_factor(mut self, factor: usize) -> Self {
        self.replication_factor = factor;
        self
    }

    /// Sets the queue size for async replication.
    pub fn queue_size(mut self, size: usize) -> Self {
        self.queue_size = size;
        self
    }

    /// Sets the timeout for sync replication.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout_ms = timeout.as_millis() as u64;
        self
    }

    /// Sets the lag threshold.
    pub fn lag_threshold(mut self, threshold: Duration) -> Self {
        self.lag_threshold_ms = threshold.as_millis() as u64;
        self
    }

    /// Sets the batch size for async replication.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Sets whether degraded writes are allowed.
    pub fn allow_degraded_writes(mut self, allow: bool) -> Self {
        self.allow_degraded_writes = allow;
        self
    }

    /// Returns the timeout as a Duration.
    pub fn timeout_duration(&self) -> Duration {
        Duration::from_millis(self.timeout_ms)
    }

    /// Returns the lag threshold as a Duration.
    pub fn lag_threshold_duration(&self) -> Duration {
        Duration::from_millis(self.lag_threshold_ms)
    }

    /// Returns the lag check interval as a Duration.
    pub fn lag_check_interval(&self) -> Duration {
        Duration::from_millis(self.lag_check_interval_ms)
    }

    /// Calculates the quorum size for this replication factor.
    ///
    /// Quorum is defined as RF/2 + 1 (majority).
    pub fn quorum_size(&self) -> usize {
        self.replication_factor / 2 + 1
    }

    /// Returns the number of backup replicas (RF - 1).
    pub fn backup_count(&self) -> usize {
        self.replication_factor.saturating_sub(1)
    }

    /// Validates the configuration.
    pub fn validate(&self) -> Result<(), ConfigValidationError> {
        if self.replication_factor == 0 {
            return Err(ConfigValidationError::InvalidReplicationFactor);
        }
        if self.queue_size == 0 {
            return Err(ConfigValidationError::InvalidQueueSize);
        }
        if self.timeout_ms == 0 {
            return Err(ConfigValidationError::InvalidTimeout);
        }
        if self.batch_size == 0 {
            return Err(ConfigValidationError::InvalidBatchSize);
        }
        Ok(())
    }
}

/// Errors from configuration validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ConfigValidationError {
    /// Replication factor must be at least 1.
    #[error("replication factor must be at least 1")]
    InvalidReplicationFactor,

    /// Queue size must be at least 1.
    #[error("queue size must be at least 1")]
    InvalidQueueSize,

    /// Timeout must be positive.
    #[error("timeout must be positive")]
    InvalidTimeout,

    /// Batch size must be at least 1.
    #[error("batch size must be at least 1")]
    InvalidBatchSize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ReplicationConfig::default();
        assert!(config.enabled);
        assert_eq!(config.default_level, ReplicationLevel::Replicated);
        assert_eq!(config.replication_factor, 3);
        assert_eq!(config.queue_size, 10_000);
        assert_eq!(config.timeout_ms, 5_000);
        assert_eq!(config.lag_threshold_ms, 30_000);
        assert_eq!(config.batch_size, 100);
        assert!(!config.allow_degraded_writes);
    }

    #[test]
    fn test_builder_pattern() {
        let config = ReplicationConfig::new()
            .enabled(false)
            .default_level(ReplicationLevel::Durable)
            .replication_factor(5)
            .queue_size(1000)
            .timeout(Duration::from_secs(10))
            .lag_threshold(Duration::from_secs(60))
            .batch_size(50)
            .allow_degraded_writes(true);

        assert!(!config.enabled);
        assert_eq!(config.default_level, ReplicationLevel::Durable);
        assert_eq!(config.replication_factor, 5);
        assert_eq!(config.queue_size, 1000);
        assert_eq!(config.timeout_ms, 10_000);
        assert_eq!(config.lag_threshold_ms, 60_000);
        assert_eq!(config.batch_size, 50);
        assert!(config.allow_degraded_writes);
    }

    #[test]
    fn test_quorum_size() {
        // RF=1 -> quorum=1
        assert_eq!(ReplicationConfig::new().replication_factor(1).quorum_size(), 1);
        // RF=2 -> quorum=2
        assert_eq!(ReplicationConfig::new().replication_factor(2).quorum_size(), 2);
        // RF=3 -> quorum=2
        assert_eq!(ReplicationConfig::new().replication_factor(3).quorum_size(), 2);
        // RF=4 -> quorum=3
        assert_eq!(ReplicationConfig::new().replication_factor(4).quorum_size(), 3);
        // RF=5 -> quorum=3
        assert_eq!(ReplicationConfig::new().replication_factor(5).quorum_size(), 3);
    }

    #[test]
    fn test_backup_count() {
        assert_eq!(ReplicationConfig::new().replication_factor(1).backup_count(), 0);
        assert_eq!(ReplicationConfig::new().replication_factor(3).backup_count(), 2);
        assert_eq!(ReplicationConfig::new().replication_factor(5).backup_count(), 4);
    }

    #[test]
    fn test_duration_conversions() {
        let config = ReplicationConfig::new()
            .timeout(Duration::from_secs(5))
            .lag_threshold(Duration::from_secs(30));

        assert_eq!(config.timeout_duration(), Duration::from_secs(5));
        assert_eq!(config.lag_threshold_duration(), Duration::from_secs(30));
    }

    #[test]
    fn test_validation() {
        assert!(ReplicationConfig::default().validate().is_ok());
        assert!(ReplicationConfig::new().replication_factor(0).validate().is_err());
        assert!(ReplicationConfig::new().queue_size(0).validate().is_err());

        let config = ReplicationConfig { timeout_ms: 0, ..Default::default() };
        assert!(config.validate().is_err());

        let config = ReplicationConfig { batch_size: 0, ..Default::default() };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_serialize_deserialize() {
        let config =
            ReplicationConfig::new().default_level(ReplicationLevel::Durable).replication_factor(5);

        let json = serde_json::to_string(&config).unwrap();
        let parsed: ReplicationConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.default_level, ReplicationLevel::Durable);
        assert_eq!(parsed.replication_factor, 5);
    }
}
