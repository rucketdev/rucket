// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! S3-compatible replication configuration types.
//!
//! Implements the AWS S3 Replication Configuration schema with support for:
//! - Replication rules with filters
//! - Delete marker replication
//! - Replication time control (RTC)
//! - Replica modifications sync

use serde::{Deserialize, Serialize};

use crate::error::{GeoError, GeoResult};

/// Status of a replication rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "PascalCase")]
pub enum RuleStatus {
    /// Rule is enabled.
    #[default]
    Enabled,
    /// Rule is disabled.
    Disabled,
}

impl RuleStatus {
    /// Check if the rule is enabled.
    pub fn is_enabled(&self) -> bool {
        matches!(self, Self::Enabled)
    }
}

/// Tag-based filter for replication rules.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct TagFilter {
    /// Tag key.
    pub key: String,
    /// Tag value.
    pub value: String,
}

impl TagFilter {
    /// Create a new tag filter.
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { key: key.into(), value: value.into() }
    }

    /// Check if an object's tags match this filter.
    pub fn matches(&self, tags: &[(String, String)]) -> bool {
        tags.iter().any(|(k, v)| k == &self.key && v == &self.value)
    }
}

/// Filter for selecting objects to replicate.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Filter {
    /// Prefix filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Tag filter (must match ALL tags).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<TagFilter>,
    /// Combined filter with AND logic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub and: Option<AndFilter>,
}

impl Filter {
    /// Create a new filter with prefix.
    pub fn with_prefix(prefix: impl Into<String>) -> Self {
        Self { prefix: Some(prefix.into()), tag: None, and: None }
    }

    /// Create a new filter with tag.
    pub fn with_tag(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { prefix: None, tag: Some(TagFilter::new(key, value)), and: None }
    }

    /// Check if an object matches this filter.
    pub fn matches(&self, key: &str, tags: &[(String, String)]) -> bool {
        // If AND filter is present, use it
        if let Some(ref and) = self.and {
            return and.matches(key, tags);
        }

        // Check prefix
        if let Some(ref prefix) = self.prefix {
            if !key.starts_with(prefix) {
                return false;
            }
        }

        // Check tag
        if let Some(ref tag) = self.tag {
            if !tag.matches(tags) {
                return false;
            }
        }

        true
    }
}

/// Combined filter with AND logic.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AndFilter {
    /// Prefix filter.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Tags that must all match.
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub tags: Vec<TagFilter>,
}

impl AndFilter {
    /// Check if an object matches this filter.
    pub fn matches(&self, key: &str, tags: &[(String, String)]) -> bool {
        // Check prefix
        if let Some(ref prefix) = self.prefix {
            if !key.starts_with(prefix) {
                return false;
            }
        }

        // Check all tags
        for tag_filter in &self.tags {
            if !tag_filter.matches(tags) {
                return false;
            }
        }

        true
    }
}

/// Delete marker replication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct DeleteMarkerReplication {
    /// Status of delete marker replication.
    pub status: RuleStatus,
}

impl Default for DeleteMarkerReplication {
    fn default() -> Self {
        Self { status: RuleStatus::Disabled }
    }
}

/// SSE-KMS encrypted objects replication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SseKmsEncryptedObjects {
    /// Status of SSE-KMS replication.
    pub status: RuleStatus,
}

impl Default for SseKmsEncryptedObjects {
    fn default() -> Self {
        Self { status: RuleStatus::Disabled }
    }
}

/// Replica modifications sync configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicaModifications {
    /// Status of replica modifications sync.
    pub status: RuleStatus,
}

impl Default for ReplicaModifications {
    fn default() -> Self {
        Self { status: RuleStatus::Disabled }
    }
}

/// Source selection criteria for replication.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct SourceSelectionCriteria {
    /// SSE-KMS encrypted objects settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sse_kms_encrypted_objects: Option<SseKmsEncryptedObjects>,
    /// Replica modifications settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica_modifications: Option<ReplicaModifications>,
}

/// Replication metrics configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Metrics {
    /// Status of metrics.
    pub status: RuleStatus,
    /// Event threshold configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_threshold: Option<EventThreshold>,
}

impl Default for Metrics {
    fn default() -> Self {
        Self { status: RuleStatus::Disabled, event_threshold: None }
    }
}

/// Event threshold for replication metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EventThreshold {
    /// Threshold in minutes.
    pub minutes: u32,
}

/// Replication Time Control (RTC) configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationTimeControl {
    /// Status of RTC.
    pub status: RuleStatus,
    /// Time threshold for replication.
    pub time: ReplicationTimeValue,
}

impl Default for ReplicationTimeControl {
    fn default() -> Self {
        Self { status: RuleStatus::Disabled, time: ReplicationTimeValue { minutes: 15 } }
    }
}

/// Replication time value.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationTimeValue {
    /// Time in minutes.
    pub minutes: u32,
}

/// Existing object replication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ExistingObjectReplication {
    /// Status of existing object replication.
    pub status: RuleStatus,
}

impl Default for ExistingObjectReplication {
    fn default() -> Self {
        Self { status: RuleStatus::Disabled }
    }
}

/// Destination configuration for replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct Destination {
    /// Destination bucket ARN or name.
    pub bucket: String,
    /// Optional destination account ID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// Storage class for replicated objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Access control translation settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub access_control_translation: Option<AccessControlTranslation>,
    /// Encryption configuration for destination.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<EncryptionConfiguration>,
    /// Replication time control settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replication_time: Option<ReplicationTimeControl>,
    /// Metrics configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<Metrics>,
}

impl Destination {
    /// Create a new destination.
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            account: None,
            storage_class: None,
            access_control_translation: None,
            encryption_configuration: None,
            replication_time: None,
            metrics: None,
        }
    }

    /// Set the storage class.
    pub fn with_storage_class(mut self, storage_class: impl Into<String>) -> Self {
        self.storage_class = Some(storage_class.into());
        self
    }

    /// Enable replication time control.
    pub fn with_rtc(mut self, minutes: u32) -> Self {
        self.replication_time = Some(ReplicationTimeControl {
            status: RuleStatus::Enabled,
            time: ReplicationTimeValue { minutes },
        });
        self
    }

    /// Enable metrics.
    pub fn with_metrics(mut self) -> Self {
        self.metrics = Some(Metrics { status: RuleStatus::Enabled, event_threshold: None });
        self
    }
}

/// Access control translation settings.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct AccessControlTranslation {
    /// Owner override setting.
    pub owner: String,
}

/// Encryption configuration for destination.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct EncryptionConfiguration {
    /// KMS key ID for encryption.
    pub replica_kms_key_id: Option<String>,
}

/// A single replication rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationRule {
    /// Unique identifier for this rule.
    #[serde(rename = "ID")]
    pub id: String,
    /// Priority of this rule (lower = higher priority).
    #[serde(default)]
    pub priority: u32,
    /// Rule status (enabled/disabled).
    pub status: RuleStatus,
    /// Filter for selecting objects.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub filter: Option<Filter>,
    /// Destination configuration.
    pub destination: Destination,
    /// Delete marker replication settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delete_marker_replication: Option<DeleteMarkerReplication>,
    /// Source selection criteria.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_selection_criteria: Option<SourceSelectionCriteria>,
    /// Existing object replication settings.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub existing_object_replication: Option<ExistingObjectReplication>,
}

impl ReplicationRule {
    /// Create a new replication rule builder.
    pub fn builder() -> ReplicationRuleBuilder {
        ReplicationRuleBuilder::new()
    }

    /// Check if this rule applies to an object.
    pub fn applies_to(&self, key: &str, tags: &[(String, String)]) -> bool {
        if !self.status.is_enabled() {
            return false;
        }

        match &self.filter {
            Some(filter) => filter.matches(key, tags),
            None => true, // No filter means all objects
        }
    }

    /// Check if delete markers should be replicated.
    pub fn replicate_delete_markers(&self) -> bool {
        self.delete_marker_replication.as_ref().map(|d| d.status.is_enabled()).unwrap_or(false)
    }
}

/// Builder for replication rules.
#[derive(Debug, Default)]
pub struct ReplicationRuleBuilder {
    id: Option<String>,
    priority: u32,
    status: RuleStatus,
    filter: Option<Filter>,
    destination: Option<Destination>,
    delete_marker_replication: Option<DeleteMarkerReplication>,
    source_selection_criteria: Option<SourceSelectionCriteria>,
    existing_object_replication: Option<ExistingObjectReplication>,
}

impl ReplicationRuleBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the rule ID.
    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Set the rule priority.
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the rule status.
    pub fn status(mut self, status: RuleStatus) -> Self {
        self.status = status;
        self
    }

    /// Set the filter.
    pub fn filter(mut self, filter: Filter) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Set the destination.
    pub fn destination(mut self, destination: Destination) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Enable delete marker replication.
    pub fn replicate_delete_markers(mut self) -> Self {
        self.delete_marker_replication =
            Some(DeleteMarkerReplication { status: RuleStatus::Enabled });
        self
    }

    /// Enable existing object replication.
    pub fn replicate_existing_objects(mut self) -> Self {
        self.existing_object_replication =
            Some(ExistingObjectReplication { status: RuleStatus::Enabled });
        self
    }

    /// Build the rule.
    pub fn build(self) -> GeoResult<ReplicationRule> {
        let id =
            self.id.ok_or_else(|| GeoError::InvalidConfig("Rule ID is required".to_string()))?;
        let destination = self
            .destination
            .ok_or_else(|| GeoError::InvalidConfig("Destination is required".to_string()))?;

        Ok(ReplicationRule {
            id,
            priority: self.priority,
            status: self.status,
            filter: self.filter,
            destination,
            delete_marker_replication: self.delete_marker_replication,
            source_selection_criteria: self.source_selection_criteria,
            existing_object_replication: self.existing_object_replication,
        })
    }
}

/// Complete replication configuration for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct ReplicationConfiguration {
    /// IAM role ARN for replication.
    pub role: String,
    /// List of replication rules.
    pub rules: Vec<ReplicationRule>,
}

/// Alias for ReplicationConfiguration (S3 API uses both names).
pub type ReplicationConfig = ReplicationConfiguration;

impl ReplicationConfiguration {
    /// Create a new configuration builder.
    pub fn builder() -> ReplicationConfigBuilder {
        ReplicationConfigBuilder::new()
    }

    /// Get all enabled rules.
    pub fn enabled_rules(&self) -> impl Iterator<Item = &ReplicationRule> {
        self.rules.iter().filter(|r| r.status.is_enabled())
    }

    /// Find the first matching rule for an object.
    pub fn find_rule(&self, key: &str, tags: &[(String, String)]) -> Option<&ReplicationRule> {
        // Rules are sorted by priority (lower = higher priority)
        self.enabled_rules().filter(|r| r.applies_to(key, tags)).min_by_key(|r| r.priority)
    }

    /// Get a rule by ID.
    pub fn get_rule(&self, id: &str) -> Option<&ReplicationRule> {
        self.rules.iter().find(|r| r.id == id)
    }

    /// Validate the configuration.
    pub fn validate(&self) -> GeoResult<()> {
        if self.role.is_empty() {
            return Err(GeoError::InvalidConfig("Role ARN is required".to_string()));
        }

        if self.rules.is_empty() {
            return Err(GeoError::InvalidConfig("At least one rule is required".to_string()));
        }

        // Check for duplicate rule IDs
        let mut ids = std::collections::HashSet::new();
        for rule in &self.rules {
            if !ids.insert(&rule.id) {
                return Err(GeoError::InvalidConfig(format!("Duplicate rule ID: {}", rule.id)));
            }
        }

        Ok(())
    }
}

/// Builder for replication configuration.
#[derive(Debug, Default)]
pub struct ReplicationConfigBuilder {
    role: Option<String>,
    rules: Vec<ReplicationRule>,
}

impl ReplicationConfigBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the IAM role ARN.
    pub fn role(mut self, role: impl Into<String>) -> Self {
        self.role = Some(role.into());
        self
    }

    /// Add a replication rule.
    pub fn add_rule(mut self, rule: ReplicationRule) -> Self {
        self.rules.push(rule);
        self
    }

    /// Build the configuration.
    pub fn build(mut self) -> GeoResult<ReplicationConfiguration> {
        let role =
            self.role.ok_or_else(|| GeoError::InvalidConfig("Role ARN is required".to_string()))?;

        // Sort rules by priority
        self.rules.sort_by_key(|r| r.priority);

        let config = ReplicationConfiguration { role, rules: self.rules };
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_prefix() {
        let filter = Filter::with_prefix("logs/");
        assert!(filter.matches("logs/access.log", &[]));
        assert!(!filter.matches("data/file.txt", &[]));
    }

    #[test]
    fn test_filter_tag() {
        let filter = Filter::with_tag("env", "prod");
        assert!(filter.matches("any/key", &[("env".to_string(), "prod".to_string())]));
        assert!(!filter.matches("any/key", &[("env".to_string(), "dev".to_string())]));
        assert!(!filter.matches("any/key", &[]));
    }

    #[test]
    fn test_rule_builder() {
        let rule = ReplicationRule::builder()
            .id("test-rule")
            .priority(1)
            .status(RuleStatus::Enabled)
            .filter(Filter::with_prefix("data/"))
            .destination(Destination::new("dest-bucket"))
            .replicate_delete_markers()
            .build()
            .unwrap();

        assert_eq!(rule.id, "test-rule");
        assert_eq!(rule.priority, 1);
        assert!(rule.status.is_enabled());
        assert!(rule.replicate_delete_markers());
    }

    #[test]
    fn test_config_builder() {
        let config = ReplicationConfiguration::builder()
            .role("arn:aws:iam::123456789:role/replication")
            .add_rule(
                ReplicationRule::builder()
                    .id("rule1")
                    .priority(1)
                    .destination(Destination::new("bucket1"))
                    .build()
                    .unwrap(),
            )
            .add_rule(
                ReplicationRule::builder()
                    .id("rule2")
                    .priority(2)
                    .destination(Destination::new("bucket2"))
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        assert_eq!(config.rules.len(), 2);
        assert_eq!(config.rules[0].id, "rule1"); // Lower priority first
    }

    #[test]
    fn test_config_validation() {
        // Missing role
        let result = ReplicationConfiguration::builder()
            .add_rule(
                ReplicationRule::builder()
                    .id("rule1")
                    .destination(Destination::new("bucket"))
                    .build()
                    .unwrap(),
            )
            .build();
        assert!(result.is_err());

        // Missing rules
        let result = ReplicationConfiguration::builder()
            .role("arn:aws:iam::123456789:role/replication")
            .build();
        assert!(result.is_err());
    }

    #[test]
    fn test_find_matching_rule() {
        let config = ReplicationConfiguration::builder()
            .role("arn:aws:iam::123456789:role/replication")
            .add_rule(
                ReplicationRule::builder()
                    .id("logs-rule")
                    .priority(1)
                    .filter(Filter::with_prefix("logs/"))
                    .destination(Destination::new("logs-bucket"))
                    .build()
                    .unwrap(),
            )
            .add_rule(
                ReplicationRule::builder()
                    .id("default-rule")
                    .priority(100)
                    .destination(Destination::new("default-bucket"))
                    .build()
                    .unwrap(),
            )
            .build()
            .unwrap();

        // logs/ prefix matches logs-rule
        let rule = config.find_rule("logs/access.log", &[]).unwrap();
        assert_eq!(rule.id, "logs-rule");

        // Other keys match default-rule
        let rule = config.find_rule("data/file.txt", &[]).unwrap();
        assert_eq!(rule.id, "default-rule");
    }

    #[test]
    fn test_destination_builder() {
        let dest = Destination::new("dest-bucket")
            .with_storage_class("STANDARD_IA")
            .with_rtc(15)
            .with_metrics();

        assert_eq!(dest.bucket, "dest-bucket");
        assert_eq!(dest.storage_class, Some("STANDARD_IA".to_string()));
        assert!(dest.replication_time.is_some());
        assert!(dest.metrics.is_some());
    }
}
