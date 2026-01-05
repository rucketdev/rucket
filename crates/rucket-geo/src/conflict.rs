// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Conflict resolution for cross-region replication.
//!
//! When the same object is modified in multiple regions simultaneously,
//! conflicts can occur. This module provides conflict resolution strategies.
//!
//! # Supported Strategies
//!
//! - **Last-Write-Wins (LWW)**: The write with the highest HLC timestamp wins.
//!   Ties are broken by region ID (lexicographic order).
//!
//! # Example
//!
//! ```ignore
//! use rucket_geo::conflict::{ConflictResolver, LastWriteWinsResolver};
//!
//! let resolver = LastWriteWinsResolver::new();
//!
//! let local = ObjectVersion { hlc: 100, region: "us-east-1", .. };
//! let remote = ObjectVersion { hlc: 110, region: "eu-west-1", .. };
//!
//! let resolution = resolver.resolve(&local, &remote);
//! assert_eq!(resolution, ConflictResolution::AcceptRemote);
//! ```

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::region::RegionId;

/// The result of conflict resolution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConflictResolution {
    /// Accept the local version (reject remote).
    AcceptLocal,
    /// Accept the remote version (overwrite local).
    AcceptRemote,
    /// Merge both versions (requires custom merge logic).
    Merge,
    /// Conflict could not be resolved automatically.
    Unresolved,
}

impl ConflictResolution {
    /// Returns true if the local version wins.
    pub fn is_local(&self) -> bool {
        matches!(self, Self::AcceptLocal)
    }

    /// Returns true if the remote version wins.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::AcceptRemote)
    }
}

/// Information about an object version for conflict resolution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectVersion {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID (if versioned).
    pub version_id: Option<String>,
    /// HLC timestamp.
    pub hlc_timestamp: u64,
    /// Region where this version was created.
    pub region: RegionId,
    /// ETag (content hash).
    pub etag: String,
    /// Size in bytes.
    pub size: u64,
    /// Whether this is a delete marker.
    pub is_delete_marker: bool,
}

impl ObjectVersion {
    /// Create a new object version.
    pub fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        hlc_timestamp: u64,
        region: RegionId,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            version_id: None,
            hlc_timestamp,
            region,
            etag: String::new(),
            size: 0,
            is_delete_marker: false,
        }
    }

    /// Set the version ID.
    pub fn with_version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    /// Set the ETag.
    pub fn with_etag(mut self, etag: impl Into<String>) -> Self {
        self.etag = etag.into();
        self
    }

    /// Set the size.
    pub fn with_size(mut self, size: u64) -> Self {
        self.size = size;
        self
    }

    /// Mark as a delete marker.
    pub fn as_delete_marker(mut self) -> Self {
        self.is_delete_marker = true;
        self
    }
}

/// Trait for resolving conflicts between object versions.
pub trait ConflictResolver: Send + Sync {
    /// Resolve a conflict between local and remote versions.
    ///
    /// Returns the resolution strategy to apply.
    fn resolve(&self, local: &ObjectVersion, remote: &ObjectVersion) -> ConflictResolution;

    /// Get the name of this resolver.
    fn name(&self) -> &'static str;
}

/// Last-Write-Wins conflict resolver.
///
/// This resolver uses HLC timestamps to determine the winner:
/// - Higher HLC timestamp wins
/// - Ties are broken by region ID (lexicographic order for determinism)
///
/// This is a simple and deterministic strategy suitable for most use cases.
#[derive(Debug, Default)]
pub struct LastWriteWinsResolver {
    /// Whether to prefer deletes over puts when timestamps are equal.
    prefer_deletes: bool,
}

impl LastWriteWinsResolver {
    /// Create a new LWW resolver.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a resolver that prefers deletes when timestamps are equal.
    pub fn prefer_deletes() -> Self {
        Self { prefer_deletes: true }
    }
}

impl ConflictResolver for LastWriteWinsResolver {
    fn resolve(&self, local: &ObjectVersion, remote: &ObjectVersion) -> ConflictResolution {
        // First, compare HLC timestamps
        match local.hlc_timestamp.cmp(&remote.hlc_timestamp) {
            Ordering::Greater => ConflictResolution::AcceptLocal,
            Ordering::Less => ConflictResolution::AcceptRemote,
            Ordering::Equal => {
                // Timestamps are equal - use tiebreakers

                // If one is a delete and we prefer deletes, accept that one
                if self.prefer_deletes {
                    match (local.is_delete_marker, remote.is_delete_marker) {
                        (true, false) => return ConflictResolution::AcceptLocal,
                        (false, true) => return ConflictResolution::AcceptRemote,
                        _ => {}
                    }
                }

                // Final tiebreaker: lexicographic comparison of region IDs
                // This ensures deterministic resolution across all nodes
                match local.region.as_str().cmp(remote.region.as_str()) {
                    Ordering::Greater => ConflictResolution::AcceptLocal,
                    Ordering::Less => ConflictResolution::AcceptRemote,
                    Ordering::Equal => {
                        // Same region, same timestamp - this shouldn't happen
                        // but if it does, we check if they're actually the same version
                        if local.etag == remote.etag {
                            ConflictResolution::AcceptLocal // They're the same
                        } else {
                            ConflictResolution::Unresolved
                        }
                    }
                }
            }
        }
    }

    fn name(&self) -> &'static str {
        "last-write-wins"
    }
}

/// Conflict information for logging and debugging.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictInfo {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Local version info.
    pub local_hlc: u64,
    /// Local region.
    pub local_region: RegionId,
    /// Remote version info.
    pub remote_hlc: u64,
    /// Remote region.
    pub remote_region: RegionId,
    /// Resolution applied.
    pub resolution: ConflictResolution,
    /// Resolver name.
    pub resolver: String,
}

impl ConflictInfo {
    /// Create conflict info from versions and resolution.
    pub fn new(
        local: &ObjectVersion,
        remote: &ObjectVersion,
        resolution: ConflictResolution,
        resolver_name: &str,
    ) -> Self {
        Self {
            bucket: local.bucket.clone(),
            key: local.key.clone(),
            local_hlc: local.hlc_timestamp,
            local_region: local.region.clone(),
            remote_hlc: remote.hlc_timestamp,
            remote_region: remote.region.clone(),
            resolution,
            resolver: resolver_name.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_version(hlc: u64, region: &str) -> ObjectVersion {
        ObjectVersion::new("bucket", "key", hlc, RegionId::new(region))
    }

    #[test]
    fn test_lww_higher_timestamp_wins() {
        let resolver = LastWriteWinsResolver::new();

        let local = create_version(100, "us-east-1");
        let remote = create_version(200, "eu-west-1");

        let resolution = resolver.resolve(&local, &remote);
        assert_eq!(resolution, ConflictResolution::AcceptRemote);

        let resolution = resolver.resolve(&remote, &local);
        assert_eq!(resolution, ConflictResolution::AcceptLocal);
    }

    #[test]
    fn test_lww_tiebreaker_by_region() {
        let resolver = LastWriteWinsResolver::new();

        let local = create_version(100, "us-east-1");
        let remote = create_version(100, "eu-west-1");

        // eu-west-1 < us-east-1 lexicographically
        let resolution = resolver.resolve(&local, &remote);
        assert_eq!(resolution, ConflictResolution::AcceptLocal);

        let resolution = resolver.resolve(&remote, &local);
        assert_eq!(resolution, ConflictResolution::AcceptRemote);
    }

    #[test]
    fn test_lww_same_region_same_etag() {
        let resolver = LastWriteWinsResolver::new();

        let local = create_version(100, "us-east-1").with_etag("abc123");
        let remote = create_version(100, "us-east-1").with_etag("abc123");

        let resolution = resolver.resolve(&local, &remote);
        assert_eq!(resolution, ConflictResolution::AcceptLocal);
    }

    #[test]
    fn test_lww_same_region_different_etag() {
        let resolver = LastWriteWinsResolver::new();

        let local = create_version(100, "us-east-1").with_etag("abc123");
        let remote = create_version(100, "us-east-1").with_etag("def456");

        let resolution = resolver.resolve(&local, &remote);
        assert_eq!(resolution, ConflictResolution::Unresolved);
    }

    #[test]
    fn test_lww_prefer_deletes() {
        let resolver = LastWriteWinsResolver::prefer_deletes();

        let local = create_version(100, "us-east-1");
        let remote = create_version(100, "eu-west-1").as_delete_marker();

        let resolution = resolver.resolve(&local, &remote);
        assert_eq!(resolution, ConflictResolution::AcceptRemote);

        let local = create_version(100, "us-east-1").as_delete_marker();
        let remote = create_version(100, "eu-west-1");

        let resolution = resolver.resolve(&local, &remote);
        assert_eq!(resolution, ConflictResolution::AcceptLocal);
    }

    #[test]
    fn test_resolution_helpers() {
        assert!(ConflictResolution::AcceptLocal.is_local());
        assert!(!ConflictResolution::AcceptLocal.is_remote());

        assert!(ConflictResolution::AcceptRemote.is_remote());
        assert!(!ConflictResolution::AcceptRemote.is_local());
    }

    #[test]
    fn test_conflict_info() {
        let local = create_version(100, "us-east-1");
        let remote = create_version(200, "eu-west-1");
        let resolution = ConflictResolution::AcceptRemote;

        let info = ConflictInfo::new(&local, &remote, resolution, "last-write-wins");

        assert_eq!(info.bucket, "bucket");
        assert_eq!(info.key, "key");
        assert_eq!(info.local_hlc, 100);
        assert_eq!(info.remote_hlc, 200);
        assert_eq!(info.resolution, ConflictResolution::AcceptRemote);
    }

    #[test]
    fn test_resolver_name() {
        let resolver = LastWriteWinsResolver::new();
        assert_eq!(resolver.name(), "last-write-wins");
    }

    #[test]
    fn test_object_version_builders() {
        let version = ObjectVersion::new("bucket", "key", 100, RegionId::new("us-east-1"))
            .with_version_id("v123")
            .with_etag("abc123")
            .with_size(1024)
            .as_delete_marker();

        assert_eq!(version.version_id, Some("v123".to_string()));
        assert_eq!(version.etag, "abc123");
        assert_eq!(version.size, 1024);
        assert!(version.is_delete_marker);
    }
}
