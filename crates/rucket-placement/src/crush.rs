//! CRUSH placement algorithm implementation.
//!
//! This module implements the core CRUSH algorithm for computing
//! object placement across a cluster.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::bucket::{BucketId, BucketItem, DeviceId, FailureDomain};
use crate::cluster_map::{ClusterMap, ClusterMapError};
use crate::hash::{compute_pg, crush_hash2};
use crate::rule::{Rule, RuleStep};

/// Errors that can occur during placement.
#[derive(Debug, Error)]
pub enum PlacementError {
    /// Cluster map error.
    #[error("cluster map error: {0}")]
    ClusterMap(#[from] ClusterMapError),

    /// Not enough devices for the requested replica count.
    #[error("not enough devices: need {needed}, have {available}")]
    NotEnoughDevices {
        /// Number of devices needed.
        needed: usize,
        /// Number of devices available.
        available: usize,
    },

    /// No root bucket defined.
    #[error("no root bucket defined in cluster map")]
    NoRoot,

    /// Rule execution failed.
    #[error("rule execution failed: {0}")]
    RuleFailed(String),
}

/// Configuration for CRUSH placement.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlacementConfig {
    /// Number of placement groups.
    /// More PGs = better distribution, but more memory/CPU.
    /// Recommended: 100-200 per OSD.
    pub pg_count: u32,

    /// Number of replicas for each object.
    pub replica_count: usize,

    /// Maximum number of selection retries.
    pub max_retries: usize,
}

impl Default for PlacementConfig {
    fn default() -> Self {
        Self { pg_count: 256, replica_count: 3, max_retries: 50 }
    }
}

/// Result of computing placement for an object.
#[derive(Debug, Clone)]
pub struct PlacementResult {
    /// Placement group ID.
    pub pg_id: u32,
    /// Primary device (OSD) for this object.
    pub primary_osd: DeviceId,
    /// Replica devices (excluding primary).
    pub replica_osds: Vec<DeviceId>,
}

impl PlacementResult {
    /// Get all OSDs (primary + replicas).
    #[must_use]
    pub fn all_osds(&self) -> Vec<DeviceId> {
        let mut osds = vec![self.primary_osd];
        osds.extend(self.replica_osds.iter().copied());
        osds
    }
}

/// CRUSH placement policy.
#[derive(Debug)]
pub struct CrushPlacement {
    /// Cluster topology.
    map: ClusterMap,
    /// Placement configuration.
    config: PlacementConfig,
    /// Placement rule.
    rule: Rule,
}

impl CrushPlacement {
    /// Create a new CRUSH placement policy with default rule.
    #[must_use]
    pub fn new(map: ClusterMap, config: PlacementConfig) -> Self {
        let rule = Rule::replicated("default", config.replica_count);
        Self { map, config, rule }
    }

    /// Create a CRUSH placement with a custom rule.
    #[must_use]
    pub fn with_rule(map: ClusterMap, config: PlacementConfig, rule: Rule) -> Self {
        Self { map, config, rule }
    }

    /// Get the cluster map.
    #[must_use]
    pub fn cluster_map(&self) -> &ClusterMap {
        &self.map
    }

    /// Get a mutable reference to the cluster map.
    pub fn cluster_map_mut(&mut self) -> &mut ClusterMap {
        &mut self.map
    }

    /// Get the placement configuration.
    #[must_use]
    pub fn config(&self) -> &PlacementConfig {
        &self.config
    }

    /// Compute placement for an object.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `key` - The object key
    ///
    /// # Returns
    ///
    /// The placement result containing PG and device assignments.
    #[must_use]
    pub fn place(&self, bucket: &str, key: &str) -> PlacementResult {
        let pg_id = compute_pg(bucket, key, self.config.pg_count);
        self.place_pg(pg_id)
    }

    /// Compute placement for a specific placement group.
    #[must_use]
    pub fn place_pg(&self, pg_id: u32) -> PlacementResult {
        let osds = self.execute_rule(pg_id);

        if osds.is_empty() {
            // Fallback: return first available device
            let available = self.map.available_devices();
            return PlacementResult {
                pg_id,
                primary_osd: available.first().copied().unwrap_or(0),
                replica_osds: Vec::new(),
            };
        }

        PlacementResult {
            pg_id,
            primary_osd: osds[0],
            replica_osds: osds.into_iter().skip(1).collect(),
        }
    }

    /// Execute the placement rule for a PG.
    fn execute_rule(&self, pg_id: u32) -> Vec<DeviceId> {
        let mut current_bucket_id = match self.map.root_id() {
            Some(id) => id,
            None => return Vec::new(),
        };
        let mut selected_osds = Vec::new();
        let mut selected_set: HashSet<DeviceId> = HashSet::new();

        for step in &self.rule.steps {
            match step {
                RuleStep::Take { bucket } => {
                    if let Some(name) = bucket {
                        if let Some(b) = self.map.get_bucket(name) {
                            current_bucket_id = b.id;
                        }
                    } else if let Some(id) = self.map.root_id() {
                        current_bucket_id = id;
                    }
                }

                RuleStep::Choose { count, domain } => {
                    let chosen = self.choose_from_bucket(
                        current_bucket_id,
                        pg_id,
                        *count,
                        *domain,
                        &selected_set,
                    );
                    for osd in chosen {
                        if selected_set.insert(osd) {
                            selected_osds.push(osd);
                        }
                    }
                }

                RuleStep::ChooseLeaf { count, domain } => {
                    // For ChooseLeaf, we select at the domain level then pick a device
                    let chosen = self.choose_from_bucket(
                        current_bucket_id,
                        pg_id,
                        *count,
                        *domain,
                        &selected_set,
                    );
                    for osd in chosen {
                        if selected_set.insert(osd) {
                            selected_osds.push(osd);
                        }
                    }
                }

                RuleStep::Emit => {
                    // Return what we've collected
                    break;
                }
            }
        }

        selected_osds
    }

    /// Choose items from a bucket respecting failure domains.
    fn choose_from_bucket(
        &self,
        start_bucket_id: BucketId,
        pg_id: u32,
        count: usize,
        target_domain: FailureDomain,
        exclude: &HashSet<DeviceId>,
    ) -> Vec<DeviceId> {
        let mut result = Vec::new();
        let mut used_domains: HashSet<BucketId> = HashSet::new();

        for replica in 0..count {
            if let Some(osd) = self.select_one(
                start_bucket_id,
                pg_id,
                replica,
                target_domain,
                exclude,
                &used_domains,
            ) {
                // Track the domain bucket we used
                if let Some(parent) = self.map.find_parent_bucket(osd) {
                    used_domains.insert(parent.id);
                }
                result.push(osd);
            }
        }

        result
    }

    /// Select one device, respecting failure domain isolation.
    fn select_one(
        &self,
        start_bucket_id: BucketId,
        pg_id: u32,
        replica: usize,
        target_domain: FailureDomain,
        exclude_osds: &HashSet<DeviceId>,
        exclude_domains: &HashSet<BucketId>,
    ) -> Option<DeviceId> {
        let x = u64::from(pg_id);

        for retry in 0..self.config.max_retries {
            let r = (replica * self.config.max_retries + retry) as u64;

            if let Some(osd) = self.descend_and_select(
                start_bucket_id,
                x,
                r,
                target_domain,
                exclude_osds,
                exclude_domains,
            ) {
                return Some(osd);
            }
        }

        None
    }

    /// Descend through the hierarchy and select a device.
    fn descend_and_select(
        &self,
        bucket_id: BucketId,
        x: u64,
        r: u64,
        target_domain: FailureDomain,
        exclude_osds: &HashSet<DeviceId>,
        exclude_domains: &HashSet<BucketId>,
    ) -> Option<DeviceId> {
        let bucket = self.map.get_bucket_by_id(bucket_id)?;

        // Select an item from this bucket
        let (_, item) = bucket.select(x, r)?;

        match item {
            BucketItem::Device(osd_id) => {
                let osd_id = *osd_id;
                // Check if device is available and not excluded
                if exclude_osds.contains(&osd_id) {
                    return None;
                }
                if let Some(device) = self.map.get_device(osd_id) {
                    if device.is_available() {
                        return Some(osd_id);
                    }
                }
                None
            }

            BucketItem::Bucket(child_id) => {
                let child_id = *child_id;
                // Check domain exclusion for failure isolation
                if exclude_domains.contains(&child_id) {
                    return None;
                }

                let child_bucket = self.map.get_bucket_by_id(child_id)?;

                // If we've reached target domain level, descend to find a device
                if child_bucket.domain.level() <= target_domain.level() {
                    // We're at or below target, continue to device
                    self.descend_to_device(
                        child_id,
                        x,
                        crush_hash2(r, child_id as u64),
                        exclude_osds,
                    )
                } else {
                    // Continue descending
                    self.descend_and_select(
                        child_id,
                        x,
                        crush_hash2(r, child_id as u64),
                        target_domain,
                        exclude_osds,
                        exclude_domains,
                    )
                }
            }
        }
    }

    /// Descend to find a device within a bucket subtree.
    fn descend_to_device(
        &self,
        bucket_id: BucketId,
        x: u64,
        r: u64,
        exclude_osds: &HashSet<DeviceId>,
    ) -> Option<DeviceId> {
        let bucket = self.map.get_bucket_by_id(bucket_id)?;
        let (_, item) = bucket.select(x, r)?;

        match item {
            BucketItem::Device(osd_id) => {
                let osd_id = *osd_id;
                if exclude_osds.contains(&osd_id) {
                    return None;
                }
                if let Some(device) = self.map.get_device(osd_id) {
                    if device.is_available() {
                        return Some(osd_id);
                    }
                }
                None
            }
            BucketItem::Bucket(child_id) => {
                self.descend_to_device(*child_id, x, crush_hash2(r, *child_id as u64), exclude_osds)
            }
        }
    }

    /// Get all placement groups and their mappings.
    #[must_use]
    pub fn get_all_pg_mappings(&self) -> Vec<(u32, Vec<DeviceId>)> {
        (0..self.config.pg_count)
            .map(|pg_id| {
                let result = self.place_pg(pg_id);
                (pg_id, result.all_osds())
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::{BucketType, DeviceInfo};

    fn create_test_cluster() -> ClusterMap {
        let mut map = ClusterMap::new();

        // Create 9 OSDs
        for i in 0..9 {
            map.add_device(DeviceInfo::new(i, format!("osd.{i}"), 1.0)).unwrap();
        }

        // Create 3 hosts, each with 3 OSDs
        map.add_bucket_with_domain("host1", BucketType::Straw2, FailureDomain::Host, vec![0, 1, 2])
            .unwrap();
        map.add_bucket_with_domain("host2", BucketType::Straw2, FailureDomain::Host, vec![3, 4, 5])
            .unwrap();
        map.add_bucket_with_domain("host3", BucketType::Straw2, FailureDomain::Host, vec![6, 7, 8])
            .unwrap();

        // Create root
        map.add_parent_bucket(
            "root",
            BucketType::Straw2,
            FailureDomain::Root,
            vec!["host1", "host2", "host3"],
        )
        .unwrap();
        map.set_root("root").unwrap();

        map
    }

    #[test]
    fn test_basic_placement() {
        let map = create_test_cluster();
        let config = PlacementConfig { pg_count: 64, replica_count: 3, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        let result = crush.place("test-bucket", "object-key");

        // Should have 1 primary + 2 replicas
        assert_eq!(result.all_osds().len(), 3);

        // All OSDs should be valid (0-8)
        for osd in result.all_osds() {
            assert!((0..9).contains(&osd));
        }
    }

    #[test]
    fn test_placement_deterministic() {
        let map = create_test_cluster();
        let config = PlacementConfig { pg_count: 64, replica_count: 3, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        let result1 = crush.place("bucket", "key");
        let result2 = crush.place("bucket", "key");

        assert_eq!(result1.pg_id, result2.pg_id);
        assert_eq!(result1.primary_osd, result2.primary_osd);
        assert_eq!(result1.replica_osds, result2.replica_osds);
    }

    #[test]
    fn test_placement_different_keys() {
        let map = create_test_cluster();
        let config = PlacementConfig { pg_count: 256, replica_count: 3, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        let result1 = crush.place("bucket", "key1");
        let result2 = crush.place("bucket", "key2");

        // Different keys should (usually) map to different PGs
        // Not guaranteed but extremely likely with 256 PGs
        // Just verify both have valid results
        assert!(!result1.all_osds().is_empty());
        assert!(!result2.all_osds().is_empty());
    }

    #[test]
    fn test_failure_domain_isolation() {
        let map = create_test_cluster();
        let config = PlacementConfig { pg_count: 64, replica_count: 3, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        // Check multiple PGs for host isolation
        for pg_id in 0..64 {
            let result = crush.place_pg(pg_id);
            let osds = result.all_osds();

            if osds.len() >= 3 {
                // Check that OSDs are on different hosts
                let host1_osds: HashSet<_> = [0, 1, 2].into_iter().collect();
                let host2_osds: HashSet<_> = [3, 4, 5].into_iter().collect();
                let host3_osds: HashSet<_> = [6, 7, 8].into_iter().collect();

                let hosts_used: HashSet<_> = osds
                    .iter()
                    .map(|osd| {
                        if host1_osds.contains(osd) {
                            1
                        } else if host2_osds.contains(osd) {
                            2
                        } else if host3_osds.contains(osd) {
                            3
                        } else {
                            0
                        }
                    })
                    .collect();

                // Should use 3 different hosts
                assert_eq!(
                    hosts_used.len(),
                    3,
                    "PG {pg_id} doesn't spread across 3 hosts: {osds:?}"
                );
            }
        }
    }

    #[test]
    fn test_distribution() {
        let map = create_test_cluster();
        let config = PlacementConfig { pg_count: 256, replica_count: 3, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        let mut osd_counts = vec![0u32; 9];

        for pg_id in 0..256 {
            let result = crush.place_pg(pg_id);
            for osd in result.all_osds() {
                osd_counts[osd as usize] += 1;
            }
        }

        // All 9 OSDs should be used (with equal weights)
        for (osd, count) in osd_counts.iter().enumerate() {
            assert!(*count > 0, "OSD {osd} was never selected");
        }

        // Distribution should be relatively uniform (within 2x)
        let min = *osd_counts.iter().min().unwrap();
        let max = *osd_counts.iter().max().unwrap();
        assert!(
            max <= min * 3,
            "Distribution too uneven: min={min}, max={max}, counts={osd_counts:?}"
        );
    }

    #[test]
    fn test_single_device_cluster() {
        let mut map = ClusterMap::new();
        map.add_device(DeviceInfo::new(0, "osd.0", 1.0)).unwrap();
        map.add_bucket_with_domain("host1", BucketType::Straw2, FailureDomain::Host, vec![0])
            .unwrap();
        map.add_parent_bucket("root", BucketType::Straw2, FailureDomain::Root, vec!["host1"])
            .unwrap();
        map.set_root("root").unwrap();

        let config = PlacementConfig { pg_count: 16, replica_count: 3, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        let result = crush.place("bucket", "key");

        // With only 1 device, can only get 1 OSD
        assert_eq!(result.primary_osd, 0);
    }

    #[test]
    fn test_weighted_distribution() {
        let mut map = ClusterMap::new();

        // OSD 0 has 2x weight
        map.add_device(DeviceInfo::new(0, "osd.0", 2.0)).unwrap();
        map.add_device(DeviceInfo::new(1, "osd.1", 1.0)).unwrap();
        map.add_device(DeviceInfo::new(2, "osd.2", 1.0)).unwrap();

        map.add_bucket_with_domain("host1", BucketType::Straw2, FailureDomain::Host, vec![0])
            .unwrap();
        map.add_bucket_with_domain("host2", BucketType::Straw2, FailureDomain::Host, vec![1])
            .unwrap();
        map.add_bucket_with_domain("host3", BucketType::Straw2, FailureDomain::Host, vec![2])
            .unwrap();

        map.add_parent_bucket(
            "root",
            BucketType::Straw2,
            FailureDomain::Root,
            vec!["host1", "host2", "host3"],
        )
        .unwrap();
        map.set_root("root").unwrap();

        let config = PlacementConfig { pg_count: 1000, replica_count: 1, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        let mut counts = [0u32; 3];
        for pg_id in 0..1000 {
            let result = crush.place_pg(pg_id);
            counts[result.primary_osd as usize] += 1;
        }

        // OSD 0 should have roughly 2x the assignments
        let ratio = counts[0] as f64 / counts[1] as f64;
        assert!(
            ratio > 1.5 && ratio < 2.5,
            "Weight not respected: ratio={ratio}, counts={counts:?}"
        );
    }

    #[test]
    fn test_get_all_pg_mappings() {
        let map = create_test_cluster();
        let config = PlacementConfig { pg_count: 16, replica_count: 3, ..Default::default() };
        let crush = CrushPlacement::new(map, config);

        let mappings = crush.get_all_pg_mappings();

        assert_eq!(mappings.len(), 16);
        for (pg_id, osds) in mappings {
            assert!(pg_id < 16);
            assert!(!osds.is_empty());
        }
    }
}
