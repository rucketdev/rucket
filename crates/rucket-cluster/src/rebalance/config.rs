// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Configuration for the rebalance manager.

use std::time::Duration;

/// Configuration for the rebalance manager.
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Delay before starting rebalance after node join.
    /// Allows cluster to stabilize.
    pub join_delay: Duration,

    /// Delay before starting rebalance after node leave.
    /// Allows for transient failures to recover.
    pub leave_delay: Duration,

    /// Maximum concurrent shard migrations.
    pub max_concurrent_migrations: usize,

    /// Timeout for individual shard migration.
    pub migration_timeout: Duration,

    /// Interval between rebalance status checks.
    pub check_interval: Duration,

    /// Maximum shards to migrate per second (rate limiting).
    pub max_migrations_per_second: u32,

    /// Whether to enable automatic rebalancing.
    pub auto_rebalance: bool,

    /// Minimum imbalance threshold to trigger rebalancing (0.0 to 1.0).
    /// 0.1 means rebalance if any node has 10% more/less than average.
    pub imbalance_threshold: f64,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            join_delay: Duration::from_secs(30),
            leave_delay: Duration::from_secs(60),
            max_concurrent_migrations: 4,
            migration_timeout: Duration::from_secs(600),
            check_interval: Duration::from_secs(10),
            max_migrations_per_second: 50,
            auto_rebalance: true,
            imbalance_threshold: 0.1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rebalance_config_defaults() {
        let config = RebalanceConfig::default();
        assert_eq!(config.join_delay, Duration::from_secs(30));
        assert_eq!(config.leave_delay, Duration::from_secs(60));
        assert_eq!(config.max_concurrent_migrations, 4);
        assert!(config.auto_rebalance);
    }
}
