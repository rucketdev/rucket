//! CRUSH (Controlled Replication Under Scalable Hashing) placement algorithm.
//!
//! This crate provides a pure Rust implementation of the CRUSH algorithm
//! for deterministic, failure-domain-aware data placement in distributed systems.
//!
//! # Overview
//!
//! CRUSH is a pseudo-random placement algorithm that:
//! - Distributes data uniformly across storage devices
//! - Respects failure domains (zones, racks, hosts)
//! - Is deterministic given the same cluster map and input
//! - Handles node failures gracefully with minimal data movement
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────┐
//! │                   ClusterMap                         │
//! ├─────────────────────────────────────────────────────┤
//! │  ┌─────────┐  ┌─────────┐  ┌─────────┐              │
//! │  │  Zone A │  │  Zone B │  │  Zone C │   (zones)    │
//! │  └────┬────┘  └────┬────┘  └────┬────┘              │
//! │       │            │            │                    │
//! │  ┌────┴────┐  ┌────┴────┐  ┌────┴────┐              │
//! │  │ Rack 1  │  │ Rack 2  │  │ Rack 3  │   (racks)    │
//! │  └────┬────┘  └────┬────┘  └────┬────┘              │
//! │       │            │            │                    │
//! │  ┌────┴────┐  ┌────┴────┐  ┌────┴────┐              │
//! │  │ Host A  │  │ Host B  │  │ Host C  │   (hosts)    │
//! │  └────┬────┘  └────┬────┘  └────┬────┘              │
//! │       │            │            │                    │
//! │  ┌────┴────┐  ┌────┴────┐  ┌────┴────┐              │
//! │  │OSD 1,2,3│  │OSD 4,5,6│  │OSD 7,8,9│   (devices)  │
//! │  └─────────┘  └─────────┘  └─────────┘              │
//! └─────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```
//! use rucket_placement::{ClusterMap, CrushPlacement, PlacementConfig};
//! use rucket_placement::bucket::{BucketType, DeviceInfo};
//!
//! // Create a simple cluster map
//! let mut map = ClusterMap::new();
//!
//! // Add devices (OSDs)
//! let osd1 = DeviceInfo::new(1, "osd1", 1.0);
//! let osd2 = DeviceInfo::new(2, "osd2", 1.0);
//! let osd3 = DeviceInfo::new(3, "osd3", 1.0);
//!
//! map.add_device(osd1).unwrap();
//! map.add_device(osd2).unwrap();
//! map.add_device(osd3).unwrap();
//!
//! // Create a host bucket containing the devices
//! map.add_bucket("host1", BucketType::Straw2, vec![1, 2, 3]).unwrap();
//!
//! // Create root bucket
//! map.set_root("host1").unwrap();
//!
//! // Create CRUSH placement policy
//! let config = PlacementConfig {
//!     replica_count: 3,
//!     pg_count: 256,
//!     ..Default::default()
//! };
//! let crush = CrushPlacement::new(map, config);
//!
//! // Compute placement for an object
//! let placement = crush.place("bucket", "key");
//! println!("Primary: {:?}, Replicas: {:?}",
//!          placement.primary_osd, placement.replica_osds);
//! ```

#![warn(missing_docs)]

pub mod bucket;
pub mod cluster_map;
pub mod crush;
pub mod hash;
pub mod rule;

pub use bucket::{Bucket, BucketType, DeviceInfo, DeviceStatus};
pub use cluster_map::{ClusterMap, ClusterMapError};
pub use crush::{CrushPlacement, PlacementConfig, PlacementError, PlacementResult};
pub use hash::crush_hash;
pub use rule::{Rule, RuleStep};
