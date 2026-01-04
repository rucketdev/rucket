//! Public Access Block configuration for buckets.
//!
//! This module provides types for S3's Public Access Block feature,
//! which controls whether buckets and objects can be made public.

use serde::{Deserialize, Serialize};

/// Public Access Block configuration for a bucket.
///
/// All fields default to `false` (allowing public access).
/// Set fields to `true` to block the corresponding type of public access.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicAccessBlockConfiguration {
    /// Block public ACLs: Reject PUT requests that include public ACL grants.
    /// Also prevents existing objects from being made public via ACLs.
    #[serde(default)]
    pub block_public_acls: bool,

    /// Ignore public ACLs: Ignore all public ACLs on buckets and objects.
    /// Even if ACLs grant public access, requests still require authentication.
    #[serde(default)]
    pub ignore_public_acls: bool,

    /// Block public policy: Reject bucket policies that grant public access.
    /// Prevents creation of policies that allow public access.
    #[serde(default)]
    pub block_public_policy: bool,

    /// Restrict public buckets: Only allow authenticated principals to access
    /// this bucket if it has a public policy. Cross-account access requires
    /// explicit permission.
    #[serde(default)]
    pub restrict_public_buckets: bool,
}

impl PublicAccessBlockConfiguration {
    /// Creates a new configuration with all blocks disabled (default).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a configuration that blocks all public access.
    #[must_use]
    pub fn block_all() -> Self {
        Self {
            block_public_acls: true,
            ignore_public_acls: true,
            block_public_policy: true,
            restrict_public_buckets: true,
        }
    }

    /// Returns true if any public access block is enabled.
    #[must_use]
    pub fn has_any_block(&self) -> bool {
        self.block_public_acls
            || self.ignore_public_acls
            || self.block_public_policy
            || self.restrict_public_buckets
    }

    /// Returns true if all public access is blocked.
    #[must_use]
    pub fn blocks_all(&self) -> bool {
        self.block_public_acls
            && self.ignore_public_acls
            && self.block_public_policy
            && self.restrict_public_buckets
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_allows_public() {
        let config = PublicAccessBlockConfiguration::new();
        assert!(!config.block_public_acls);
        assert!(!config.ignore_public_acls);
        assert!(!config.block_public_policy);
        assert!(!config.restrict_public_buckets);
        assert!(!config.has_any_block());
        assert!(!config.blocks_all());
    }

    #[test]
    fn test_block_all() {
        let config = PublicAccessBlockConfiguration::block_all();
        assert!(config.block_public_acls);
        assert!(config.ignore_public_acls);
        assert!(config.block_public_policy);
        assert!(config.restrict_public_buckets);
        assert!(config.has_any_block());
        assert!(config.blocks_all());
    }

    #[test]
    fn test_partial_block() {
        let config = PublicAccessBlockConfiguration {
            block_public_acls: true,
            ignore_public_acls: false,
            block_public_policy: true,
            restrict_public_buckets: false,
        };
        assert!(config.has_any_block());
        assert!(!config.blocks_all());
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = PublicAccessBlockConfiguration {
            block_public_acls: true,
            ignore_public_acls: false,
            block_public_policy: true,
            restrict_public_buckets: true,
        };
        let json = serde_json::to_string(&config).unwrap();
        let parsed: PublicAccessBlockConfiguration = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_backward_compat_empty_json() {
        // Old data without any fields should deserialize to defaults
        let config: PublicAccessBlockConfiguration = serde_json::from_str("{}").unwrap();
        assert_eq!(config, PublicAccessBlockConfiguration::default());
    }
}
