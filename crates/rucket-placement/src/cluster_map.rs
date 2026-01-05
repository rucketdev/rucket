//! Cluster map for CRUSH topology.
//!
//! The cluster map represents the complete topology of the storage cluster,
//! including all devices, buckets, and their hierarchical relationships.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::bucket::{
    Bucket, BucketId, BucketItem, BucketType, DeviceId, DeviceInfo, FailureDomain,
};

/// Errors that can occur when building or modifying a cluster map.
#[derive(Debug, Error)]
pub enum ClusterMapError {
    /// Device ID already exists.
    #[error("device {0} already exists")]
    DuplicateDevice(DeviceId),

    /// Bucket name already exists.
    #[error("bucket '{0}' already exists")]
    DuplicateBucket(String),

    /// Device not found.
    #[error("device {0} not found")]
    DeviceNotFound(DeviceId),

    /// Bucket not found.
    #[error("bucket '{0}' not found")]
    BucketNotFound(String),

    /// Invalid device ID (must be >= 0).
    #[error("device ID must be >= 0, got {0}")]
    InvalidDeviceId(DeviceId),

    /// No root bucket set.
    #[error("no root bucket set")]
    NoRoot,

    /// Item already in a bucket.
    #[error("item {0} is already in a bucket")]
    ItemAlreadyAssigned(i32),
}

/// The complete cluster topology.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterMap {
    /// All devices in the cluster, indexed by ID.
    devices: HashMap<DeviceId, DeviceInfo>,
    /// All buckets in the cluster, indexed by ID.
    buckets: HashMap<BucketId, Bucket>,
    /// Bucket name to ID mapping.
    bucket_names: HashMap<String, BucketId>,
    /// Root bucket ID.
    root_id: Option<BucketId>,
    /// Next bucket ID to assign (starts at -1, decrements).
    next_bucket_id: BucketId,
}

impl Default for ClusterMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ClusterMap {
    /// Create a new empty cluster map.
    #[must_use]
    pub fn new() -> Self {
        Self {
            devices: HashMap::new(),
            buckets: HashMap::new(),
            bucket_names: HashMap::new(),
            root_id: None,
            next_bucket_id: -1,
        }
    }

    /// Add a device to the cluster.
    ///
    /// Devices must be added before they can be placed in buckets.
    pub fn add_device(&mut self, device: DeviceInfo) -> Result<(), ClusterMapError> {
        if device.id < 0 {
            return Err(ClusterMapError::InvalidDeviceId(device.id));
        }
        if self.devices.contains_key(&device.id) {
            return Err(ClusterMapError::DuplicateDevice(device.id));
        }
        self.devices.insert(device.id, device);
        Ok(())
    }

    /// Get a device by ID.
    #[must_use]
    pub fn get_device(&self, id: DeviceId) -> Option<&DeviceInfo> {
        self.devices.get(&id)
    }

    /// Get a mutable reference to a device.
    #[must_use]
    pub fn get_device_mut(&mut self, id: DeviceId) -> Option<&mut DeviceInfo> {
        self.devices.get_mut(&id)
    }

    /// Get all devices.
    #[must_use]
    pub fn devices(&self) -> &HashMap<DeviceId, DeviceInfo> {
        &self.devices
    }

    /// Add a bucket containing the specified device IDs.
    ///
    /// The bucket will have a weight equal to the sum of its device weights.
    pub fn add_bucket(
        &mut self,
        name: impl Into<String>,
        bucket_type: BucketType,
        device_ids: Vec<DeviceId>,
    ) -> Result<BucketId, ClusterMapError> {
        self.add_bucket_with_domain(name, bucket_type, FailureDomain::Host, device_ids)
    }

    /// Add a bucket with a specific failure domain.
    pub fn add_bucket_with_domain(
        &mut self,
        name: impl Into<String>,
        bucket_type: BucketType,
        domain: FailureDomain,
        device_ids: Vec<DeviceId>,
    ) -> Result<BucketId, ClusterMapError> {
        let name = name.into();
        if self.bucket_names.contains_key(&name) {
            return Err(ClusterMapError::DuplicateBucket(name));
        }

        let bucket_id = self.next_bucket_id;
        self.next_bucket_id -= 1;

        let mut bucket = Bucket::new(bucket_id, &name, domain, bucket_type);

        for device_id in device_ids {
            let device =
                self.devices.get(&device_id).ok_or(ClusterMapError::DeviceNotFound(device_id))?;
            bucket.add_device(device_id, device.weight);
        }

        self.bucket_names.insert(name, bucket_id);
        self.buckets.insert(bucket_id, bucket);

        Ok(bucket_id)
    }

    /// Add a hierarchical bucket containing other buckets.
    pub fn add_parent_bucket(
        &mut self,
        name: impl Into<String>,
        bucket_type: BucketType,
        domain: FailureDomain,
        child_names: Vec<&str>,
    ) -> Result<BucketId, ClusterMapError> {
        let name = name.into();
        if self.bucket_names.contains_key(&name) {
            return Err(ClusterMapError::DuplicateBucket(name));
        }

        let bucket_id = self.next_bucket_id;
        self.next_bucket_id -= 1;

        let mut bucket = Bucket::new(bucket_id, &name, domain, bucket_type);

        for child_name in child_names {
            let child_id = self
                .bucket_names
                .get(child_name)
                .ok_or_else(|| ClusterMapError::BucketNotFound(child_name.to_string()))?;
            let child_bucket = self
                .buckets
                .get(child_id)
                .ok_or(ClusterMapError::BucketNotFound(child_name.to_string()))?;
            bucket.add_bucket(*child_id, child_bucket.total_weight());
        }

        self.bucket_names.insert(name, bucket_id);
        self.buckets.insert(bucket_id, bucket);

        Ok(bucket_id)
    }

    /// Get a bucket by name.
    #[must_use]
    pub fn get_bucket(&self, name: &str) -> Option<&Bucket> {
        self.bucket_names.get(name).and_then(|id| self.buckets.get(id))
    }

    /// Get a bucket by ID.
    #[must_use]
    pub fn get_bucket_by_id(&self, id: BucketId) -> Option<&Bucket> {
        self.buckets.get(&id)
    }

    /// Set the root bucket.
    pub fn set_root(&mut self, name: &str) -> Result<(), ClusterMapError> {
        let id = self
            .bucket_names
            .get(name)
            .copied()
            .ok_or_else(|| ClusterMapError::BucketNotFound(name.to_string()))?;
        self.root_id = Some(id);
        Ok(())
    }

    /// Get the root bucket.
    #[must_use]
    pub fn root(&self) -> Option<&Bucket> {
        self.root_id.and_then(|id| self.buckets.get(&id))
    }

    /// Get the root bucket ID.
    #[must_use]
    pub fn root_id(&self) -> Option<BucketId> {
        self.root_id
    }

    /// Get the total number of devices.
    #[must_use]
    pub fn device_count(&self) -> usize {
        self.devices.len()
    }

    /// Get the total number of buckets.
    #[must_use]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }

    /// Recursively get all device IDs under a bucket.
    #[must_use]
    pub fn get_devices_under_bucket(&self, bucket_id: BucketId) -> Vec<DeviceId> {
        let mut devices = Vec::new();
        self.collect_devices_recursive(bucket_id, &mut devices);
        devices
    }

    fn collect_devices_recursive(&self, bucket_id: BucketId, devices: &mut Vec<DeviceId>) {
        if let Some(bucket) = self.buckets.get(&bucket_id) {
            for item in &bucket.items {
                match item {
                    BucketItem::Device(device_id) => devices.push(*device_id),
                    BucketItem::Bucket(child_id) => {
                        self.collect_devices_recursive(*child_id, devices);
                    }
                }
            }
        }
    }

    /// Get all available (up) devices.
    #[must_use]
    pub fn available_devices(&self) -> Vec<DeviceId> {
        self.devices.iter().filter(|(_, d)| d.is_available()).map(|(&id, _)| id).collect()
    }

    /// Find the bucket containing a specific item.
    #[must_use]
    pub fn find_parent_bucket(&self, item_id: i32) -> Option<&Bucket> {
        for bucket in self.buckets.values() {
            for item in &bucket.items {
                if item.id() == item_id {
                    return Some(bucket);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_map() -> ClusterMap {
        let mut map = ClusterMap::new();

        // Add 9 devices (3 per host)
        for i in 0..9 {
            map.add_device(DeviceInfo::new(i, format!("osd.{i}"), 1.0)).unwrap();
        }

        // Create 3 hosts
        map.add_bucket("host1", BucketType::Straw2, vec![0, 1, 2]).unwrap();
        map.add_bucket("host2", BucketType::Straw2, vec![3, 4, 5]).unwrap();
        map.add_bucket("host3", BucketType::Straw2, vec![6, 7, 8]).unwrap();

        // Create root bucket containing hosts
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
    fn test_add_device() {
        let mut map = ClusterMap::new();
        map.add_device(DeviceInfo::new(0, "osd.0", 1.0)).unwrap();

        assert_eq!(map.device_count(), 1);
        assert!(map.get_device(0).is_some());
    }

    #[test]
    fn test_duplicate_device() {
        let mut map = ClusterMap::new();
        map.add_device(DeviceInfo::new(0, "osd.0", 1.0)).unwrap();

        let result = map.add_device(DeviceInfo::new(0, "osd.0-dup", 1.0));
        assert!(matches!(result, Err(ClusterMapError::DuplicateDevice(0))));
    }

    #[test]
    fn test_add_bucket() {
        let mut map = ClusterMap::new();
        map.add_device(DeviceInfo::new(0, "osd.0", 1.0)).unwrap();
        map.add_device(DeviceInfo::new(1, "osd.1", 2.0)).unwrap();

        let bucket_id = map.add_bucket("host1", BucketType::Straw2, vec![0, 1]).unwrap();
        assert!(bucket_id < 0);

        let bucket = map.get_bucket("host1").unwrap();
        assert_eq!(bucket.items.len(), 2);
        assert!((bucket.total_weight() - 3.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_hierarchical_buckets() {
        let map = create_test_map();

        assert_eq!(map.device_count(), 9);
        assert_eq!(map.bucket_count(), 4); // 3 hosts + 1 root

        let root = map.root().unwrap();
        assert_eq!(root.items.len(), 3);
        assert!((root.total_weight() - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_get_devices_under_bucket() {
        let map = create_test_map();
        let root_id = map.root_id().unwrap();

        let devices = map.get_devices_under_bucket(root_id);
        assert_eq!(devices.len(), 9);

        let host1_id = map.get_bucket("host1").unwrap().id;
        let host1_devices = map.get_devices_under_bucket(host1_id);
        assert_eq!(host1_devices.len(), 3);
        assert!(host1_devices.contains(&0));
        assert!(host1_devices.contains(&1));
        assert!(host1_devices.contains(&2));
    }

    #[test]
    fn test_available_devices() {
        let mut map = ClusterMap::new();
        map.add_device(DeviceInfo::new(0, "osd.0", 1.0)).unwrap();
        map.add_device(
            DeviceInfo::new(1, "osd.1", 1.0).with_status(crate::bucket::DeviceStatus::Down),
        )
        .unwrap();
        map.add_device(DeviceInfo::new(2, "osd.2", 1.0)).unwrap();

        let available = map.available_devices();
        assert_eq!(available.len(), 2);
        assert!(available.contains(&0));
        assert!(available.contains(&2));
        assert!(!available.contains(&1));
    }

    #[test]
    fn test_find_parent_bucket() {
        let map = create_test_map();

        // Device 0 should be in host1
        let parent = map.find_parent_bucket(0).unwrap();
        assert_eq!(parent.name, "host1");

        // Device 5 should be in host2
        let parent = map.find_parent_bucket(5).unwrap();
        assert_eq!(parent.name, "host2");

        // Host1 bucket should be in root
        let host1_id = map.get_bucket("host1").unwrap().id;
        let parent = map.find_parent_bucket(host1_id).unwrap();
        assert_eq!(parent.name, "root");
    }
}
