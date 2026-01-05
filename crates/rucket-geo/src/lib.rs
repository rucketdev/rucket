// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Cross-Region Replication (CRR) for Rucket object storage.
//!
//! This crate provides S3-compatible cross-region replication functionality:
//!
//! - **Replication Configuration**: S3-compatible replication rules with filters
//! - **Event-Sourced Replication**: Stream storage events to remote regions
//! - **Conflict Resolution**: Last-Write-Wins (LWW) using HLC timestamps
//! - **Status Tracking**: Per-object replication status monitoring
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
//! │  Source Region  │────▶│ ReplicationStream │────▶│  Dest Region   │
//! │  (Primary)      │     │  (Event-based)   │     │  (Replica)     │
//! └─────────────────┘     └─────────────────┘     └─────────────────┘
//!        │                        │                        │
//!        ▼                        ▼                        ▼
//!   StorageEvent            HLC Timestamp            ConflictResolver
//!   (create/delete)         (causality)             (LWW + region ID)
//! ```
//!
//! # S3 CRR Compatibility
//!
//! Supports the following S3 replication configuration features:
//! - Replication rules with filters (prefix, tags)
//! - Replication time control (RTC) metrics
//! - Delete marker replication
//! - Replica modification sync
//!
//! # Example
//!
//! ```ignore
//! use rucket_geo::{ReplicationConfig, ReplicationRule, RegionEndpoint};
//!
//! // Create replication configuration
//! let config = ReplicationConfig::builder()
//!     .role("arn:aws:iam::123456789:role/replication")
//!     .add_rule(
//!         ReplicationRule::builder()
//!             .id("replicate-all")
//!             .status(RuleStatus::Enabled)
//!             .destination(RegionEndpoint::new("us-west-2", "https://west.example.com"))
//!             .build()
//!     )
//!     .build();
//! ```

#![warn(missing_docs)]

pub mod config;
pub mod conflict;
pub mod error;
pub mod placement;
pub mod region;
pub mod stream;
pub mod types;

pub use config::{
    DeleteMarkerReplication, Destination, ExistingObjectReplication, Filter, Metrics,
    ReplicaModifications, ReplicationConfig, ReplicationRule, ReplicationTimeControl, RuleStatus,
    SourceSelectionCriteria, SseKmsEncryptedObjects, TagFilter,
};
pub use conflict::{ConflictResolution, ConflictResolver, LastWriteWinsResolver};
pub use error::{GeoError, GeoResult};
pub use placement::{
    BucketGeoConfig, ConsistencyLevel, GeoPlacement, GeoPlacementBuilder, GeoPlacementConfig,
    ReadPreference, ReadRoute, WriteRoute,
};
pub use region::{Region, RegionEndpoint, RegionId, RegionRegistry};
pub use stream::{
    NoOpReplicationSink, ReplicationEntry, ReplicationSink, ReplicationSource, ReplicationStream,
    ReplicationStreamConfig,
};
pub use types::{ObjectReplicationStatus, ReplicationProgress, ReplicationStatusInfo};
