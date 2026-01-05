//! Bucket types for CRUSH placement hierarchy.
//!
//! CRUSH uses a hierarchy of buckets to represent the cluster topology.
//! Buckets can contain either devices (leaf nodes) or other buckets (interior nodes).
//!
//! # Bucket Types
//!
//! - **Uniform**: All items have equal weight (fastest, but inflexible)
//! - **Straw2**: Weighted selection with minimal disruption on topology changes
//!
//! Straw2 is recommended for production use as it provides good distribution
//! and handles weight changes gracefully.

use serde::{Deserialize, Serialize};

use crate::hash::crush_hash2;

/// Unique identifier for a device (OSD).
pub type DeviceId = i32;

/// Unique identifier for a bucket.
pub type BucketId = i32;

/// Status of a device in the cluster.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum DeviceStatus {
    /// Device is up and accepting I/O.
    #[default]
    Up,
    /// Device is down (failed or maintenance).
    Down,
    /// Device is being drained (no new data, existing data migrating out).
    Draining,
}

/// Information about a storage device (OSD).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeviceInfo {
    /// Unique device ID (must be >= 0).
    pub id: DeviceId,
    /// Human-readable name.
    pub name: String,
    /// Weight for placement (higher = more data).
    /// Typically corresponds to capacity in TB.
    pub weight: f64,
    /// Current device status.
    pub status: DeviceStatus,
    /// Network address for the device.
    pub address: Option<String>,
}

impl DeviceInfo {
    /// Create a new device.
    #[must_use]
    pub fn new(id: DeviceId, name: impl Into<String>, weight: f64) -> Self {
        Self { id, name: name.into(), weight, status: DeviceStatus::Up, address: None }
    }

    /// Create a device with an address.
    #[must_use]
    pub fn with_address(mut self, address: impl Into<String>) -> Self {
        self.address = Some(address.into());
        self
    }

    /// Set the device status.
    #[must_use]
    pub fn with_status(mut self, status: DeviceStatus) -> Self {
        self.status = status;
        self
    }

    /// Returns true if the device is available for placement.
    #[must_use]
    pub fn is_available(&self) -> bool {
        self.status == DeviceStatus::Up
    }
}

/// Type of failure domain for a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FailureDomain {
    /// Root of the cluster (no specific domain).
    Root,
    /// Datacenter/region level.
    Datacenter,
    /// Availability zone within a datacenter.
    Zone,
    /// Physical rack.
    Rack,
    /// Physical host/server.
    Host,
    /// Storage device (OSD) - leaf level.
    Device,
}

impl FailureDomain {
    /// Returns the numeric level (higher = closer to root).
    #[must_use]
    pub fn level(self) -> u8 {
        match self {
            Self::Root => 5,
            Self::Datacenter => 4,
            Self::Zone => 3,
            Self::Rack => 2,
            Self::Host => 1,
            Self::Device => 0,
        }
    }
}

/// Bucket selection algorithm.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketType {
    /// All items have equal weight.
    /// Fastest but doesn't support variable weights.
    Uniform,
    /// Straw2 algorithm for weighted selection.
    /// Recommended for production use.
    #[default]
    Straw2,
}

/// An item that can be in a bucket (device or sub-bucket).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BucketItem {
    /// A storage device (leaf node).
    Device(DeviceId),
    /// A sub-bucket (interior node).
    Bucket(BucketId),
}

impl BucketItem {
    /// Get the ID of this item.
    #[must_use]
    pub fn id(&self) -> i32 {
        match self {
            Self::Device(id) | Self::Bucket(id) => *id,
        }
    }
}

/// A bucket in the CRUSH hierarchy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bucket {
    /// Unique bucket ID (must be < 0 to distinguish from devices).
    pub id: BucketId,
    /// Human-readable name.
    pub name: String,
    /// The type of failure domain this bucket represents.
    pub domain: FailureDomain,
    /// Selection algorithm.
    pub bucket_type: BucketType,
    /// Items in this bucket.
    pub items: Vec<BucketItem>,
    /// Weights for each item (same order as items).
    pub weights: Vec<f64>,
}

impl Bucket {
    /// Create a new bucket.
    ///
    /// # Arguments
    ///
    /// * `id` - Unique bucket ID (must be negative to distinguish from devices)
    /// * `name` - Human-readable name
    /// * `domain` - Failure domain level
    /// * `bucket_type` - Selection algorithm
    #[must_use]
    pub fn new(
        id: BucketId,
        name: impl Into<String>,
        domain: FailureDomain,
        bucket_type: BucketType,
    ) -> Self {
        Self { id, name: name.into(), domain, bucket_type, items: Vec::new(), weights: Vec::new() }
    }

    /// Add a device to this bucket.
    pub fn add_device(&mut self, device_id: DeviceId, weight: f64) {
        self.items.push(BucketItem::Device(device_id));
        self.weights.push(weight);
    }

    /// Add a sub-bucket to this bucket.
    pub fn add_bucket(&mut self, bucket_id: BucketId, weight: f64) {
        self.items.push(BucketItem::Bucket(bucket_id));
        self.weights.push(weight);
    }

    /// Get the total weight of this bucket.
    #[must_use]
    pub fn total_weight(&self) -> f64 {
        self.weights.iter().sum()
    }

    /// Select an item from this bucket using the given hash.
    ///
    /// Returns the index and item selected.
    #[must_use]
    pub fn select(&self, x: u64, r: u64) -> Option<(usize, &BucketItem)> {
        if self.items.is_empty() {
            return None;
        }

        match self.bucket_type {
            BucketType::Uniform => self.select_uniform(x, r),
            BucketType::Straw2 => self.select_straw2(x, r),
        }
    }

    /// Uniform selection (equal weights).
    fn select_uniform(&self, x: u64, r: u64) -> Option<(usize, &BucketItem)> {
        let hash = crush_hash2(x, r);
        let idx = (hash as usize) % self.items.len();
        Some((idx, &self.items[idx]))
    }

    /// Straw2 selection (weighted).
    ///
    /// The straw2 algorithm assigns each item a "straw length" based on:
    /// - A hash of (input, item_id)
    /// - The item's weight
    ///
    /// The item with the longest straw is selected. This provides:
    /// - Weighted distribution proportional to item weights
    /// - Minimal disruption when items are added/removed
    fn select_straw2(&self, x: u64, r: u64) -> Option<(usize, &BucketItem)> {
        let mut best_idx = 0;
        let mut best_draw = f64::NEG_INFINITY;

        for (idx, (item, &weight)) in self.items.iter().zip(self.weights.iter()).enumerate() {
            if weight <= 0.0 {
                continue;
            }

            // Hash the input with the item ID
            let item_id = item.id();
            let hash = crush_hash2(crush_hash2(x, r), item_id as u64);

            // Convert hash to a value in (0, 1)
            let u = (hash as f64) / (u64::MAX as f64);

            // Compute the straw length using the inverse of -ln(u)/weight
            // This gives us weighted exponential distribution
            let draw = weight * (-u.ln()).recip();

            if draw > best_draw {
                best_draw = draw;
                best_idx = idx;
            }
        }

        if best_draw.is_finite() {
            Some((best_idx, &self.items[best_idx]))
        } else {
            // All items have zero weight
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_device_info() {
        let dev = DeviceInfo::new(1, "osd.1", 1.0)
            .with_address("192.168.1.10:6789")
            .with_status(DeviceStatus::Up);

        assert_eq!(dev.id, 1);
        assert_eq!(dev.name, "osd.1");
        assert_eq!(dev.weight, 1.0);
        assert!(dev.is_available());
        assert_eq!(dev.address, Some("192.168.1.10:6789".to_string()));
    }

    #[test]
    fn test_device_status() {
        let mut dev = DeviceInfo::new(1, "osd.1", 1.0);
        assert!(dev.is_available());

        dev.status = DeviceStatus::Down;
        assert!(!dev.is_available());

        dev.status = DeviceStatus::Draining;
        assert!(!dev.is_available());
    }

    #[test]
    fn test_failure_domain_levels() {
        assert!(FailureDomain::Root.level() > FailureDomain::Datacenter.level());
        assert!(FailureDomain::Datacenter.level() > FailureDomain::Zone.level());
        assert!(FailureDomain::Zone.level() > FailureDomain::Rack.level());
        assert!(FailureDomain::Rack.level() > FailureDomain::Host.level());
        assert!(FailureDomain::Host.level() > FailureDomain::Device.level());
    }

    #[test]
    fn test_bucket_uniform_selection() {
        let mut bucket = Bucket::new(-1, "host1", FailureDomain::Host, BucketType::Uniform);
        bucket.add_device(1, 1.0);
        bucket.add_device(2, 1.0);
        bucket.add_device(3, 1.0);

        // Selection should be deterministic
        let (idx1, _) = bucket.select(42, 0).unwrap();
        let (idx2, _) = bucket.select(42, 0).unwrap();
        assert_eq!(idx1, idx2);

        // Different inputs should (usually) select different items
        let mut selected = std::collections::HashSet::new();
        for i in 0..100 {
            let (_, item) = bucket.select(i, 0).unwrap();
            selected.insert(item.id());
        }
        // Should have selected multiple devices
        assert!(selected.len() > 1);
    }

    #[test]
    fn test_bucket_straw2_selection() {
        let mut bucket = Bucket::new(-1, "host1", FailureDomain::Host, BucketType::Straw2);
        bucket.add_device(1, 1.0);
        bucket.add_device(2, 2.0); // Double weight
        bucket.add_device(3, 1.0);

        // Selection should be deterministic
        let (idx1, _) = bucket.select(42, 0).unwrap();
        let (idx2, _) = bucket.select(42, 0).unwrap();
        assert_eq!(idx1, idx2);

        // Over many selections, device 2 should be selected ~2x as often
        let mut counts = [0u32; 3];
        for i in 0..10000 {
            let (_, item) = bucket.select(i, 0).unwrap();
            match item {
                BucketItem::Device(1) => counts[0] += 1,
                BucketItem::Device(2) => counts[1] += 1,
                BucketItem::Device(3) => counts[2] += 1,
                _ => panic!("unexpected item"),
            }
        }

        // Device 2 should have roughly 2x the selections
        let ratio = counts[1] as f64 / counts[0] as f64;
        assert!(ratio > 1.5 && ratio < 2.5, "Weight ratio not respected: {ratio}");
    }

    #[test]
    fn test_bucket_empty() {
        let bucket = Bucket::new(-1, "empty", FailureDomain::Host, BucketType::Straw2);
        assert!(bucket.select(42, 0).is_none());
    }

    #[test]
    fn test_bucket_total_weight() {
        let mut bucket = Bucket::new(-1, "host1", FailureDomain::Host, BucketType::Straw2);
        bucket.add_device(1, 1.0);
        bucket.add_device(2, 2.0);
        bucket.add_device(3, 1.5);

        assert!((bucket.total_weight() - 4.5).abs() < f64::EPSILON);
    }
}
