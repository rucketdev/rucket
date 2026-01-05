// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! S3 bucket replication configuration types.
//!
//! This module provides types for S3 Cross-Region Replication (CRR) and
//! Same-Region Replication (SRR) configurations.
//!
//! # Example XML
//!
//! ```xml
//! <ReplicationConfiguration>
//!     <Role>arn:aws:iam::123456789:role/replication</Role>
//!     <Rule>
//!         <ID>rule-1</ID>
//!         <Status>Enabled</Status>
//!         <Priority>1</Priority>
//!         <Filter>
//!             <Prefix>logs/</Prefix>
//!         </Filter>
//!         <Destination>
//!             <Bucket>arn:aws:s3:::dest-bucket</Bucket>
//!         </Destination>
//!         <DeleteMarkerReplication>
//!             <Status>Enabled</Status>
//!         </DeleteMarkerReplication>
//!     </Rule>
//! </ReplicationConfiguration>
//! ```

use serde::{Deserialize, Serialize};

/// Replication configuration for a bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationConfiguration {
    /// IAM role ARN for replication.
    pub role: String,
    /// List of replication rules.
    pub rules: Vec<ReplicationRule>,
}

impl ReplicationConfiguration {
    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ReplicationConfigError> {
        if self.role.is_empty() {
            return Err(ReplicationConfigError::MissingRole);
        }

        if self.rules.is_empty() {
            return Err(ReplicationConfigError::NoRules);
        }

        // Check for duplicate rule IDs
        let mut ids = std::collections::HashSet::new();
        for rule in &self.rules {
            if let Some(ref id) = rule.id {
                if !ids.insert(id.as_str()) {
                    return Err(ReplicationConfigError::DuplicateRuleId(id.clone()));
                }
            }
        }

        // Validate each rule
        for rule in &self.rules {
            rule.validate()?;
        }

        Ok(())
    }

    /// Get all enabled rules.
    pub fn enabled_rules(&self) -> impl Iterator<Item = &ReplicationRule> {
        self.rules.iter().filter(|r| r.status == RuleStatus::Enabled)
    }
}

/// A single replication rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationRule {
    /// Unique identifier for the rule (optional).
    #[serde(default)]
    pub id: Option<String>,

    /// Status of the rule (Enabled or Disabled).
    pub status: RuleStatus,

    /// Priority of the rule (lower = higher priority).
    #[serde(default)]
    pub priority: Option<u32>,

    /// Filter to select objects this rule applies to.
    #[serde(default)]
    pub filter: Option<ReplicationFilter>,

    /// Destination bucket for replicated objects.
    pub destination: ReplicationDestination,

    /// Whether to replicate delete markers.
    #[serde(default)]
    pub delete_marker_replication: Option<DeleteMarkerReplication>,

    /// Criteria for selecting source objects.
    #[serde(default)]
    pub source_selection_criteria: Option<SourceSelectionCriteria>,

    /// Whether to replicate existing objects.
    #[serde(default)]
    pub existing_object_replication: Option<ExistingObjectReplication>,
}

impl ReplicationRule {
    /// Validate the rule.
    pub fn validate(&self) -> Result<(), ReplicationConfigError> {
        // Destination bucket is required
        if self.destination.bucket.is_empty() {
            return Err(ReplicationConfigError::MissingDestination);
        }
        Ok(())
    }
}

/// Status of a replication rule.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RuleStatus {
    /// Rule is active.
    #[default]
    Enabled,
    /// Rule is inactive.
    Disabled,
}

impl RuleStatus {
    /// Parse from string representation.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(Self::Enabled),
            "Disabled" => Some(Self::Disabled),
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

/// Filter to select objects for replication.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationFilter {
    /// Key name prefix that selects objects.
    #[serde(default)]
    pub prefix: Option<String>,

    /// Tag that selects objects.
    #[serde(default)]
    pub tag: Option<ReplicationTag>,

    /// Logical AND of multiple filter conditions.
    #[serde(default)]
    pub and: Option<ReplicationFilterAnd>,
}

/// Logical AND filter for replication rules.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationFilterAnd {
    /// Key name prefix.
    #[serde(default)]
    pub prefix: Option<String>,

    /// Tags to match.
    #[serde(default)]
    pub tags: Vec<ReplicationTag>,
}

/// A tag used in replication filtering.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationTag {
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

/// Destination for replicated objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationDestination {
    /// Destination bucket ARN or name.
    pub bucket: String,

    /// Optional account ID for cross-account replication.
    #[serde(default)]
    pub account: Option<String>,

    /// Storage class for replicated objects.
    #[serde(default)]
    pub storage_class: Option<String>,

    /// Access control translation for cross-account replication.
    #[serde(default)]
    pub access_control_translation: Option<AccessControlTranslation>,

    /// Encryption configuration for replicated objects.
    #[serde(default)]
    pub encryption_configuration: Option<EncryptionConfiguration>,

    /// Replication time control settings.
    #[serde(default)]
    pub replication_time: Option<ReplicationTime>,

    /// Metrics configuration.
    #[serde(default)]
    pub metrics: Option<ReplicationMetrics>,
}

/// Access control translation for cross-account replication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessControlTranslation {
    /// Owner override (Destination).
    pub owner: String,
}

/// Encryption configuration for destination objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionConfiguration {
    /// KMS key ID for destination encryption.
    #[serde(default)]
    pub replica_kms_key_id: Option<String>,
}

/// Replication time control settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationTime {
    /// Status of replication time control.
    pub status: RuleStatus,
    /// Time threshold in minutes.
    #[serde(default)]
    pub time: Option<ReplicationTimeValue>,
}

/// Time value for replication time control.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationTimeValue {
    /// Minutes threshold.
    pub minutes: u32,
}

/// Metrics configuration for replication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationMetrics {
    /// Status of metrics.
    pub status: RuleStatus,
    /// Event threshold for metrics.
    #[serde(default)]
    pub event_threshold: Option<ReplicationTimeValue>,
}

/// Delete marker replication settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteMarkerReplication {
    /// Status of delete marker replication.
    pub status: RuleStatus,
}

/// Source selection criteria for replication.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSelectionCriteria {
    /// Settings for replicating SSE-KMS encrypted objects.
    #[serde(default)]
    pub sse_kms_encrypted_objects: Option<SseKmsEncryptedObjects>,

    /// Replica modifications settings.
    #[serde(default)]
    pub replica_modifications: Option<ReplicaModifications>,
}

/// Settings for replicating SSE-KMS encrypted objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseKmsEncryptedObjects {
    /// Status of SSE-KMS replication.
    pub status: RuleStatus,
}

/// Replica modifications settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaModifications {
    /// Status of replica modifications replication.
    pub status: RuleStatus,
}

/// Existing object replication settings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExistingObjectReplication {
    /// Status of existing object replication.
    pub status: RuleStatus,
}

/// Errors from replication configuration validation.
#[derive(Debug, Clone, thiserror::Error)]
pub enum ReplicationConfigError {
    /// Role ARN is required.
    #[error("Role ARN is required")]
    MissingRole,

    /// At least one rule is required.
    #[error("At least one rule is required")]
    NoRules,

    /// Duplicate rule ID.
    #[error("Duplicate rule ID: {0}")]
    DuplicateRuleId(String),

    /// Destination bucket is required.
    #[error("Destination bucket is required")]
    MissingDestination,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_status_parsing() {
        assert_eq!(RuleStatus::parse("Enabled"), Some(RuleStatus::Enabled));
        assert_eq!(RuleStatus::parse("Disabled"), Some(RuleStatus::Disabled));
        assert_eq!(RuleStatus::parse("enabled"), None); // Case-sensitive
        assert_eq!(RuleStatus::parse("invalid"), None);
    }

    #[test]
    fn test_validate_empty_role() {
        let config = ReplicationConfiguration {
            role: String::new(),
            rules: vec![ReplicationRule {
                id: Some("rule1".to_string()),
                status: RuleStatus::Enabled,
                priority: None,
                filter: None,
                destination: ReplicationDestination {
                    bucket: "dest-bucket".to_string(),
                    account: None,
                    storage_class: None,
                    access_control_translation: None,
                    encryption_configuration: None,
                    replication_time: None,
                    metrics: None,
                },
                delete_marker_replication: None,
                source_selection_criteria: None,
                existing_object_replication: None,
            }],
        };
        assert!(matches!(config.validate(), Err(ReplicationConfigError::MissingRole)));
    }

    #[test]
    fn test_validate_no_rules() {
        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![],
        };
        assert!(matches!(config.validate(), Err(ReplicationConfigError::NoRules)));
    }

    #[test]
    fn test_validate_duplicate_rule_ids() {
        let rule = ReplicationRule {
            id: Some("rule1".to_string()),
            status: RuleStatus::Enabled,
            priority: None,
            filter: None,
            destination: ReplicationDestination {
                bucket: "dest-bucket".to_string(),
                account: None,
                storage_class: None,
                access_control_translation: None,
                encryption_configuration: None,
                replication_time: None,
                metrics: None,
            },
            delete_marker_replication: None,
            source_selection_criteria: None,
            existing_object_replication: None,
        };

        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![rule.clone(), rule],
        };
        assert!(matches!(config.validate(), Err(ReplicationConfigError::DuplicateRuleId(_))));
    }

    #[test]
    fn test_valid_config() {
        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![ReplicationRule {
                id: Some("rule1".to_string()),
                status: RuleStatus::Enabled,
                priority: Some(1),
                filter: Some(ReplicationFilter {
                    prefix: Some("logs/".to_string()),
                    tag: None,
                    and: None,
                }),
                destination: ReplicationDestination {
                    bucket: "dest-bucket".to_string(),
                    account: None,
                    storage_class: Some("STANDARD".to_string()),
                    access_control_translation: None,
                    encryption_configuration: None,
                    replication_time: None,
                    metrics: None,
                },
                delete_marker_replication: Some(DeleteMarkerReplication {
                    status: RuleStatus::Enabled,
                }),
                source_selection_criteria: None,
                existing_object_replication: None,
            }],
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![ReplicationRule {
                id: Some("rule1".to_string()),
                status: RuleStatus::Enabled,
                priority: Some(1),
                filter: Some(ReplicationFilter {
                    prefix: Some("logs/".to_string()),
                    tag: None,
                    and: None,
                }),
                destination: ReplicationDestination {
                    bucket: "dest-bucket".to_string(),
                    account: None,
                    storage_class: None,
                    access_control_translation: None,
                    encryption_configuration: None,
                    replication_time: None,
                    metrics: None,
                },
                delete_marker_replication: None,
                source_selection_criteria: None,
                existing_object_replication: None,
            }],
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: ReplicationConfiguration = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }

    #[test]
    fn test_enabled_rules() {
        let config = ReplicationConfiguration {
            role: "arn:aws:iam::123456789:role/replication".to_string(),
            rules: vec![
                ReplicationRule {
                    id: Some("enabled".to_string()),
                    status: RuleStatus::Enabled,
                    priority: None,
                    filter: None,
                    destination: ReplicationDestination {
                        bucket: "dest".to_string(),
                        account: None,
                        storage_class: None,
                        access_control_translation: None,
                        encryption_configuration: None,
                        replication_time: None,
                        metrics: None,
                    },
                    delete_marker_replication: None,
                    source_selection_criteria: None,
                    existing_object_replication: None,
                },
                ReplicationRule {
                    id: Some("disabled".to_string()),
                    status: RuleStatus::Disabled,
                    priority: None,
                    filter: None,
                    destination: ReplicationDestination {
                        bucket: "dest".to_string(),
                        account: None,
                        storage_class: None,
                        access_control_translation: None,
                        encryption_configuration: None,
                        replication_time: None,
                        metrics: None,
                    },
                    delete_marker_replication: None,
                    source_selection_criteria: None,
                    existing_object_replication: None,
                },
            ],
        };

        let enabled: Vec<_> = config.enabled_rules().collect();
        assert_eq!(enabled.len(), 1);
        assert_eq!(enabled[0].id, Some("enabled".to_string()));
    }
}
