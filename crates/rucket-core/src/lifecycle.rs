//! Lifecycle configuration for S3 buckets.
//!
//! This module provides types for S3 lifecycle rules that control automatic
//! object transitions and expirations.

use serde::{Deserialize, Serialize};

/// Lifecycle configuration for a bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleConfiguration {
    /// List of lifecycle rules.
    pub rules: Vec<LifecycleRule>,
}

/// A single lifecycle rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleRule {
    /// Unique identifier for the rule (max 255 characters).
    pub id: Option<String>,

    /// Status of the rule (Enabled or Disabled).
    pub status: RuleStatus,

    /// Filter to select objects this rule applies to.
    #[serde(default)]
    pub filter: Option<LifecycleFilter>,

    /// Expiration settings for current versions.
    #[serde(default)]
    pub expiration: Option<Expiration>,

    /// Expiration settings for noncurrent versions.
    #[serde(default)]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpiration>,

    /// Settings for aborting incomplete multipart uploads.
    #[serde(default)]
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUpload>,
}

/// Status of a lifecycle rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleStatus {
    /// Rule is active.
    Enabled,
    /// Rule is inactive.
    Disabled,
}

impl RuleStatus {
    /// Parse from string representation.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "enabled" => Some(Self::Enabled),
            "disabled" => Some(Self::Disabled),
            _ => None,
        }
    }

    /// Convert to string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::Disabled => "Disabled",
        }
    }
}

/// Filter to select objects for a lifecycle rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleFilter {
    /// Key name prefix that selects objects.
    #[serde(default)]
    pub prefix: Option<String>,

    /// Tag that selects objects.
    #[serde(default)]
    pub tag: Option<LifecycleTag>,

    /// Minimum object size in bytes.
    #[serde(default)]
    pub object_size_greater_than: Option<u64>,

    /// Maximum object size in bytes.
    #[serde(default)]
    pub object_size_less_than: Option<u64>,

    /// Logical AND of multiple filters.
    #[serde(default)]
    pub and: Option<LifecycleFilterAnd>,
}

/// Logical AND filter for lifecycle rules.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleFilterAnd {
    /// Key name prefix.
    #[serde(default)]
    pub prefix: Option<String>,

    /// Tags to match.
    #[serde(default)]
    pub tags: Vec<LifecycleTag>,

    /// Minimum object size in bytes.
    #[serde(default)]
    pub object_size_greater_than: Option<u64>,

    /// Maximum object size in bytes.
    #[serde(default)]
    pub object_size_less_than: Option<u64>,
}

/// A tag used in lifecycle filtering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LifecycleTag {
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// Expiration settings for current object versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Expiration {
    /// Number of days after object creation when the object expires.
    #[serde(default)]
    pub days: Option<u32>,

    /// Date when objects expire (ISO 8601 format).
    #[serde(default)]
    pub date: Option<String>,

    /// Whether to remove expired delete markers.
    #[serde(default)]
    pub expired_object_delete_marker: Option<bool>,
}

/// Expiration settings for noncurrent object versions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoncurrentVersionExpiration {
    /// Number of days after an object becomes noncurrent when it expires.
    pub noncurrent_days: u32,

    /// Number of noncurrent versions to retain.
    #[serde(default)]
    pub newer_noncurrent_versions: Option<u32>,
}

/// Settings for aborting incomplete multipart uploads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AbortIncompleteMultipartUpload {
    /// Number of days after initiation when incomplete uploads are aborted.
    pub days_after_initiation: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_status_parsing() {
        assert_eq!(RuleStatus::parse("Enabled"), Some(RuleStatus::Enabled));
        assert_eq!(RuleStatus::parse("enabled"), Some(RuleStatus::Enabled));
        assert_eq!(RuleStatus::parse("Disabled"), Some(RuleStatus::Disabled));
        assert_eq!(RuleStatus::parse("disabled"), Some(RuleStatus::Disabled));
        assert_eq!(RuleStatus::parse("invalid"), None);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = LifecycleConfiguration {
            rules: vec![LifecycleRule {
                id: Some("test-rule".to_string()),
                status: RuleStatus::Enabled,
                filter: Some(LifecycleFilter {
                    prefix: Some("logs/".to_string()),
                    tag: None,
                    object_size_greater_than: None,
                    object_size_less_than: None,
                    and: None,
                }),
                expiration: Some(Expiration {
                    days: Some(30),
                    date: None,
                    expired_object_delete_marker: None,
                }),
                noncurrent_version_expiration: None,
                abort_incomplete_multipart_upload: Some(AbortIncompleteMultipartUpload {
                    days_after_initiation: 7,
                }),
            }],
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: LifecycleConfiguration = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }
}
