// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Region-aware placement for geo-distributed storage.
//!
//! This module provides geo-aware placement that:
//! - Routes writes to the appropriate region based on bucket configuration
//! - Routes reads to the nearest region for low latency
//! - Supports regional consistency levels (local, regional, global)
//!
//! # Regional Consistency Levels
//!
//! - **Local**: Writes are considered durable after local region acknowledgment.
//!   Provides lowest latency but risks data loss if the region fails before replication.
//!
//! - **Regional**: Writes are considered durable after acknowledgment from the
//!   home region AND at least one replica region. Provides a balance of latency
//!   and durability.
//!
//! - **Global**: Writes are considered durable only after all configured replica
//!   regions acknowledge. Provides maximum durability but highest latency.
//!
//! # Example
//!
//! ```ignore
//! use rucket_geo::placement::{GeoPlacement, GeoPlacementConfig, ConsistencyLevel};
//!
//! let config = GeoPlacementConfig {
//!     home_region: "us-east-1".into(),
//!     consistency_level: ConsistencyLevel::Regional,
//!     ..Default::default()
//! };
//!
//! let placement = GeoPlacement::new(config);
//!
//! // Route a read to the nearest region
//! let region = placement.route_read("bucket", "key", Some("eu-west-1"));
//!
//! // Route a write to the home region
//! let write_targets = placement.route_write("bucket", "key");
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::error::{GeoError, GeoResult};
use crate::region::{Region, RegionId, RegionRegistry};

/// Consistency level for geo-distributed operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ConsistencyLevel {
    /// Write is durable after local region acknowledgment only.
    /// Lowest latency, but data may be lost if region fails before replication.
    Local,

    /// Write is durable after home region + at least one replica region.
    /// Balanced latency and durability.
    #[default]
    Regional,

    /// Write is durable only after all replica regions acknowledge.
    /// Maximum durability, highest latency.
    Global,
}

impl ConsistencyLevel {
    /// Returns the minimum number of regions that must acknowledge a write.
    #[must_use]
    pub fn required_acks(&self, replica_count: usize) -> usize {
        match self {
            Self::Local => 1,
            Self::Regional => 2.min(replica_count),
            Self::Global => replica_count,
        }
    }

    /// Check if the given number of acknowledgments satisfies this consistency level.
    #[must_use]
    pub fn is_satisfied(&self, acks: usize, replica_count: usize) -> bool {
        acks >= self.required_acks(replica_count)
    }
}

/// Read routing preference for geo-distributed reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReadPreference {
    /// Always read from the nearest region (lowest latency).
    #[default]
    Nearest,

    /// Always read from the home region (strongest consistency).
    HomeRegion,

    /// Prefer local region, fall back to home if unavailable.
    LocalFirst,

    /// Read from any available region.
    Any,
}

/// Configuration for geo-aware placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeoPlacementConfig {
    /// The home region for this cluster node.
    pub home_region: RegionId,

    /// Default consistency level for writes.
    pub consistency_level: ConsistencyLevel,

    /// Default read preference.
    pub read_preference: ReadPreference,

    /// Maximum acceptable staleness for reads (in milliseconds).
    /// If a remote region's data is older than this, prefer home region.
    pub max_staleness_ms: u64,

    /// Whether to enable read-your-writes consistency.
    /// When enabled, reads will go to home region if the client recently wrote.
    pub read_your_writes: bool,

    /// Regions to exclude from read routing (e.g., degraded regions).
    pub excluded_regions: HashSet<RegionId>,
}

impl Default for GeoPlacementConfig {
    fn default() -> Self {
        Self {
            home_region: RegionId::new("local"),
            consistency_level: ConsistencyLevel::Regional,
            read_preference: ReadPreference::Nearest,
            max_staleness_ms: 5000, // 5 seconds
            read_your_writes: true,
            excluded_regions: HashSet::new(),
        }
    }
}

/// Result of a read routing decision.
#[derive(Debug, Clone)]
pub struct ReadRoute {
    /// The primary region to read from.
    pub region: RegionId,

    /// Fallback regions if the primary is unavailable.
    pub fallbacks: Vec<RegionId>,

    /// Whether this read might return stale data.
    pub may_be_stale: bool,
}

/// Result of a write routing decision.
#[derive(Debug, Clone)]
pub struct WriteRoute {
    /// The primary region for the write.
    pub primary: RegionId,

    /// Regions that must acknowledge before the write is considered durable.
    pub required_regions: Vec<RegionId>,

    /// Additional regions for async replication.
    pub async_regions: Vec<RegionId>,

    /// Minimum acknowledgments required.
    pub min_acks: usize,
}

/// Per-bucket geo placement configuration.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BucketGeoConfig {
    /// Home region for this bucket (where writes go first).
    pub home_region: Option<RegionId>,

    /// Replica regions for this bucket.
    pub replica_regions: Vec<RegionId>,

    /// Override consistency level for this bucket.
    pub consistency_level: Option<ConsistencyLevel>,

    /// Override read preference for this bucket.
    pub read_preference: Option<ReadPreference>,
}

/// Geo-aware placement for routing reads and writes across regions.
#[derive(Debug)]
pub struct GeoPlacement {
    /// Global configuration.
    config: GeoPlacementConfig,

    /// Registry of known regions.
    region_registry: Arc<RegionRegistry>,

    /// Per-bucket configuration.
    bucket_configs: HashMap<String, BucketGeoConfig>,

    /// Region latency estimates (region_id -> latency_ms).
    latency_estimates: HashMap<RegionId, u64>,
}

impl GeoPlacement {
    /// Create a new geo placement policy.
    pub fn new(config: GeoPlacementConfig, region_registry: Arc<RegionRegistry>) -> Self {
        Self {
            config,
            region_registry,
            bucket_configs: HashMap::new(),
            latency_estimates: HashMap::new(),
        }
    }

    /// Set bucket-specific geo configuration.
    pub fn set_bucket_config(&mut self, bucket: impl Into<String>, config: BucketGeoConfig) {
        self.bucket_configs.insert(bucket.into(), config);
    }

    /// Get bucket-specific geo configuration.
    #[must_use]
    pub fn get_bucket_config(&self, bucket: &str) -> Option<&BucketGeoConfig> {
        self.bucket_configs.get(bucket)
    }

    /// Update latency estimate for a region.
    pub fn update_latency(&mut self, region: RegionId, latency_ms: u64) {
        self.latency_estimates.insert(region, latency_ms);
    }

    /// Get the home region for a bucket.
    #[must_use]
    pub fn home_region(&self, bucket: &str) -> RegionId {
        self.bucket_configs
            .get(bucket)
            .and_then(|c| c.home_region.clone())
            .unwrap_or_else(|| self.config.home_region.clone())
    }

    /// Get the consistency level for a bucket.
    #[must_use]
    pub fn consistency_level(&self, bucket: &str) -> ConsistencyLevel {
        self.bucket_configs
            .get(bucket)
            .and_then(|c| c.consistency_level)
            .unwrap_or(self.config.consistency_level)
    }

    /// Get the read preference for a bucket.
    #[must_use]
    pub fn read_preference(&self, bucket: &str) -> ReadPreference {
        self.bucket_configs
            .get(bucket)
            .and_then(|c| c.read_preference)
            .unwrap_or(self.config.read_preference)
    }

    /// Route a read operation to the appropriate region.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `_key` - The object key (reserved for future use)
    /// * `client_region` - The region where the client is located (for nearest routing)
    #[must_use]
    pub fn route_read(
        &self,
        bucket: &str,
        _key: &str,
        client_region: Option<&RegionId>,
    ) -> ReadRoute {
        let home = self.home_region(bucket);
        let preference = self.read_preference(bucket);

        let (primary, may_be_stale) = match preference {
            ReadPreference::HomeRegion => (home.clone(), false),
            ReadPreference::Nearest => {
                if let Some(client) = client_region {
                    // If client is in home region, use it
                    if client == &home {
                        (home.clone(), false)
                    } else {
                        // Use client's region if available
                        (client.clone(), true)
                    }
                } else {
                    // No client region info, use home
                    (home.clone(), false)
                }
            }
            ReadPreference::LocalFirst => {
                if let Some(client) = client_region {
                    if self.is_region_available(client) && !self.is_region_excluded(client) {
                        (client.clone(), client != &home)
                    } else {
                        (home.clone(), false)
                    }
                } else {
                    (home.clone(), false)
                }
            }
            ReadPreference::Any => {
                // Find the region with lowest latency
                if let Some(best) = self.lowest_latency_region() {
                    let is_stale = best != home;
                    (best, is_stale)
                } else {
                    (home.clone(), false)
                }
            }
        };

        // Build fallback list
        let mut fallbacks = Vec::new();
        if primary != home {
            fallbacks.push(home.clone());
        }

        // Add replica regions as fallbacks
        if let Some(bucket_config) = self.bucket_configs.get(bucket) {
            for region in &bucket_config.replica_regions {
                if region != &primary && !fallbacks.contains(region) {
                    fallbacks.push(region.clone());
                }
            }
        }

        ReadRoute { region: primary, fallbacks, may_be_stale }
    }

    /// Route a write operation to the appropriate regions.
    ///
    /// Returns the primary region and required acknowledgment regions based
    /// on the bucket's consistency level.
    #[must_use]
    pub fn route_write(&self, bucket: &str, _key: &str) -> WriteRoute {
        let home = self.home_region(bucket);
        let consistency = self.consistency_level(bucket);

        // Get replica regions for this bucket
        let replicas: Vec<RegionId> =
            self.bucket_configs.get(bucket).map(|c| c.replica_regions.clone()).unwrap_or_default();

        let total_regions = 1 + replicas.len(); // home + replicas
        let min_acks = consistency.required_acks(total_regions);

        // Determine which regions require sync acknowledgment
        let (required, async_regions) = match consistency {
            ConsistencyLevel::Local => {
                // Only home region required
                (vec![home.clone()], replicas)
            }
            ConsistencyLevel::Regional => {
                // Home + one replica required
                if replicas.is_empty() {
                    (vec![home.clone()], Vec::new())
                } else {
                    let required = vec![home.clone(), replicas[0].clone()];
                    let async_regions = replicas[1..].to_vec();
                    (required, async_regions)
                }
            }
            ConsistencyLevel::Global => {
                // All regions required
                let mut required = vec![home.clone()];
                required.extend(replicas.clone());
                (required, Vec::new())
            }
        };

        WriteRoute { primary: home, required_regions: required, async_regions, min_acks }
    }

    /// Check if a region is available for read/write operations.
    #[must_use]
    pub fn is_region_available(&self, region: &RegionId) -> bool {
        self.region_registry.contains(region)
    }

    /// Check if a region is excluded from routing.
    #[must_use]
    pub fn is_region_excluded(&self, region: &RegionId) -> bool {
        self.config.excluded_regions.contains(region)
    }

    /// Find the region with the lowest latency.
    fn lowest_latency_region(&self) -> Option<RegionId> {
        self.latency_estimates
            .iter()
            .filter(|(r, _)| !self.is_region_excluded(r))
            .min_by_key(|(_, lat)| *lat)
            .map(|(r, _)| r.clone())
    }

    /// Validate that a bucket's geo configuration is valid.
    pub fn validate_bucket_config(&self, bucket: &str) -> GeoResult<()> {
        let config = match self.bucket_configs.get(bucket) {
            Some(c) => c,
            None => return Ok(()), // No config = valid
        };

        // Validate home region exists
        if let Some(ref home) = config.home_region {
            if !self.region_registry.contains(home) {
                return Err(GeoError::InvalidConfig(format!(
                    "Home region '{}' not found in registry",
                    home
                )));
            }
        }

        // Validate replica regions exist
        for region in &config.replica_regions {
            if !self.region_registry.contains(region) {
                return Err(GeoError::InvalidConfig(format!(
                    "Replica region '{}' not found in registry",
                    region
                )));
            }
        }

        Ok(())
    }
}

/// Builder for geo placement configuration.
#[derive(Debug, Default)]
pub struct GeoPlacementBuilder {
    config: GeoPlacementConfig,
    regions: Vec<Region>,
    bucket_configs: HashMap<String, BucketGeoConfig>,
}

impl GeoPlacementBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the home region.
    pub fn home_region(mut self, region: impl Into<RegionId>) -> Self {
        self.config.home_region = region.into();
        self
    }

    /// Set the default consistency level.
    pub fn consistency_level(mut self, level: ConsistencyLevel) -> Self {
        self.config.consistency_level = level;
        self
    }

    /// Set the default read preference.
    pub fn read_preference(mut self, preference: ReadPreference) -> Self {
        self.config.read_preference = preference;
        self
    }

    /// Set maximum staleness for reads.
    pub fn max_staleness_ms(mut self, ms: u64) -> Self {
        self.config.max_staleness_ms = ms;
        self
    }

    /// Enable or disable read-your-writes consistency.
    pub fn read_your_writes(mut self, enabled: bool) -> Self {
        self.config.read_your_writes = enabled;
        self
    }

    /// Add a region to the registry.
    pub fn add_region(mut self, region: Region) -> Self {
        self.regions.push(region);
        self
    }

    /// Add bucket-specific configuration.
    pub fn bucket_config(mut self, bucket: impl Into<String>, config: BucketGeoConfig) -> Self {
        self.bucket_configs.insert(bucket.into(), config);
        self
    }

    /// Exclude a region from routing.
    pub fn exclude_region(mut self, region: impl Into<RegionId>) -> Self {
        self.config.excluded_regions.insert(region.into());
        self
    }

    /// Build the geo placement.
    pub fn build(self) -> GeoResult<GeoPlacement> {
        // Create region registry
        let registry = RegionRegistry::new();
        for region in self.regions {
            registry.register(region)?;
        }

        let mut placement = GeoPlacement::new(self.config, Arc::new(registry));
        placement.bucket_configs = self.bucket_configs;

        Ok(placement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::region::RegionEndpoint;

    fn create_test_registry() -> Arc<RegionRegistry> {
        let registry = RegionRegistry::new();

        let us_east = Region::new(
            "us-east-1",
            "US East",
            RegionEndpoint::new("https://us-east-1.example.com"),
        )
        .as_local();
        registry.register(us_east).unwrap();

        let eu_west = Region::new(
            "eu-west-1",
            "EU West",
            RegionEndpoint::new("https://eu-west-1.example.com"),
        );
        registry.register(eu_west).unwrap();

        let ap_south = Region::new(
            "ap-south-1",
            "Asia Pacific South",
            RegionEndpoint::new("https://ap-south-1.example.com"),
        );
        registry.register(ap_south).unwrap();

        Arc::new(registry)
    }

    #[test]
    fn test_consistency_level_required_acks() {
        assert_eq!(ConsistencyLevel::Local.required_acks(3), 1);
        assert_eq!(ConsistencyLevel::Regional.required_acks(3), 2);
        assert_eq!(ConsistencyLevel::Regional.required_acks(1), 1);
        assert_eq!(ConsistencyLevel::Global.required_acks(3), 3);
    }

    #[test]
    fn test_consistency_level_satisfaction() {
        assert!(ConsistencyLevel::Local.is_satisfied(1, 3));
        assert!(ConsistencyLevel::Regional.is_satisfied(2, 3));
        assert!(!ConsistencyLevel::Regional.is_satisfied(1, 3));
        assert!(ConsistencyLevel::Global.is_satisfied(3, 3));
        assert!(!ConsistencyLevel::Global.is_satisfied(2, 3));
    }

    #[test]
    fn test_read_route_home_region() {
        let registry = create_test_registry();
        let config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            read_preference: ReadPreference::HomeRegion,
            ..Default::default()
        };

        let placement = GeoPlacement::new(config, registry);
        let route = placement.route_read("test-bucket", "key", None);

        assert_eq!(route.region, RegionId::new("us-east-1"));
        assert!(!route.may_be_stale);
    }

    #[test]
    fn test_read_route_nearest() {
        let registry = create_test_registry();
        let config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            read_preference: ReadPreference::Nearest,
            ..Default::default()
        };

        let placement = GeoPlacement::new(config, registry);

        // Client in EU should read from EU
        let client_region = RegionId::new("eu-west-1");
        let route = placement.route_read("test-bucket", "key", Some(&client_region));
        assert_eq!(route.region, client_region);
        assert!(route.may_be_stale);
        assert_eq!(route.fallbacks[0], RegionId::new("us-east-1"));
    }

    #[test]
    fn test_write_route_local_consistency() {
        let registry = create_test_registry();
        let config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            consistency_level: ConsistencyLevel::Local,
            ..Default::default()
        };

        let mut placement = GeoPlacement::new(config, registry);
        placement.set_bucket_config(
            "test-bucket",
            BucketGeoConfig {
                replica_regions: vec![RegionId::new("eu-west-1"), RegionId::new("ap-south-1")],
                ..Default::default()
            },
        );

        let route = placement.route_write("test-bucket", "key");

        assert_eq!(route.primary, RegionId::new("us-east-1"));
        assert_eq!(route.required_regions.len(), 1);
        assert_eq!(route.async_regions.len(), 2);
        assert_eq!(route.min_acks, 1);
    }

    #[test]
    fn test_write_route_regional_consistency() {
        let registry = create_test_registry();
        let config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            consistency_level: ConsistencyLevel::Regional,
            ..Default::default()
        };

        let mut placement = GeoPlacement::new(config, registry);
        placement.set_bucket_config(
            "test-bucket",
            BucketGeoConfig {
                replica_regions: vec![RegionId::new("eu-west-1"), RegionId::new("ap-south-1")],
                ..Default::default()
            },
        );

        let route = placement.route_write("test-bucket", "key");

        assert_eq!(route.primary, RegionId::new("us-east-1"));
        assert_eq!(route.required_regions.len(), 2);
        assert_eq!(route.async_regions.len(), 1);
        assert_eq!(route.min_acks, 2);
    }

    #[test]
    fn test_write_route_global_consistency() {
        let registry = create_test_registry();
        let config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            consistency_level: ConsistencyLevel::Global,
            ..Default::default()
        };

        let mut placement = GeoPlacement::new(config, registry);
        placement.set_bucket_config(
            "test-bucket",
            BucketGeoConfig {
                replica_regions: vec![RegionId::new("eu-west-1"), RegionId::new("ap-south-1")],
                ..Default::default()
            },
        );

        let route = placement.route_write("test-bucket", "key");

        assert_eq!(route.primary, RegionId::new("us-east-1"));
        assert_eq!(route.required_regions.len(), 3);
        assert!(route.async_regions.is_empty());
        assert_eq!(route.min_acks, 3);
    }

    #[test]
    fn test_bucket_specific_config() {
        let registry = create_test_registry();
        let config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            consistency_level: ConsistencyLevel::Local,
            ..Default::default()
        };

        let mut placement = GeoPlacement::new(config, registry);
        placement.set_bucket_config(
            "critical-bucket",
            BucketGeoConfig {
                home_region: Some(RegionId::new("eu-west-1")),
                consistency_level: Some(ConsistencyLevel::Global),
                ..Default::default()
            },
        );

        // Default bucket uses global config
        assert_eq!(placement.home_region("default-bucket"), RegionId::new("us-east-1"));
        assert_eq!(placement.consistency_level("default-bucket"), ConsistencyLevel::Local);

        // Critical bucket uses bucket-specific config
        assert_eq!(placement.home_region("critical-bucket"), RegionId::new("eu-west-1"));
        assert_eq!(placement.consistency_level("critical-bucket"), ConsistencyLevel::Global);
    }

    #[test]
    fn test_builder() {
        let placement = GeoPlacementBuilder::new()
            .home_region("us-east-1")
            .consistency_level(ConsistencyLevel::Regional)
            .read_preference(ReadPreference::Nearest)
            .max_staleness_ms(10000)
            .add_region(Region::new(
                "us-east-1",
                "US East",
                RegionEndpoint::new("https://us-east-1.example.com"),
            ))
            .add_region(Region::new(
                "eu-west-1",
                "EU West",
                RegionEndpoint::new("https://eu-west-1.example.com"),
            ))
            .build()
            .unwrap();

        assert_eq!(placement.config.home_region, RegionId::new("us-east-1"));
        assert_eq!(placement.config.consistency_level, ConsistencyLevel::Regional);
        assert_eq!(placement.config.max_staleness_ms, 10000);
    }

    #[test]
    fn test_excluded_regions() {
        let registry = create_test_registry();
        let mut config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            read_preference: ReadPreference::Any,
            ..Default::default()
        };
        config.excluded_regions.insert(RegionId::new("eu-west-1"));

        let mut placement = GeoPlacement::new(config, registry);
        placement.update_latency(RegionId::new("us-east-1"), 10);
        placement.update_latency(RegionId::new("eu-west-1"), 5); // Lower but excluded
        placement.update_latency(RegionId::new("ap-south-1"), 50);

        // Should not return the excluded region even if it has lowest latency
        let best = placement.lowest_latency_region();
        assert_eq!(best, Some(RegionId::new("us-east-1")));

        assert!(placement.is_region_excluded(&RegionId::new("eu-west-1")));
        assert!(!placement.is_region_excluded(&RegionId::new("us-east-1")));
    }

    #[test]
    fn test_read_route_local_first() {
        let registry = create_test_registry();
        let config = GeoPlacementConfig {
            home_region: RegionId::new("us-east-1"),
            read_preference: ReadPreference::LocalFirst,
            ..Default::default()
        };

        let placement = GeoPlacement::new(config, registry);

        // Client in available region should use local
        let client_region = RegionId::new("eu-west-1");
        let route = placement.route_read("test-bucket", "key", Some(&client_region));
        assert_eq!(route.region, client_region);
        assert!(route.may_be_stale);

        // Client in unknown region should fallback to home
        let unknown_region = RegionId::new("unknown");
        let route = placement.route_read("test-bucket", "key", Some(&unknown_region));
        assert_eq!(route.region, RegionId::new("us-east-1"));
        assert!(!route.may_be_stale);
    }
}
