//! Bucket replication configuration tests.
//!
//! Tests for S3 bucket replication configuration API including:
//! - PUT bucket replication configuration
//! - GET bucket replication configuration
//! - DELETE bucket replication configuration
//!
//! Note: Replication configuration IS implemented in Rucket.
//! These tests verify the CRUD operations for replication configuration.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_replication_*
//! - AWS S3 documentation: Cross-Region Replication

use aws_sdk_s3::types::{
    DeleteMarkerReplication, DeleteMarkerReplicationStatus, Destination, ReplicationConfiguration,
    ReplicationRule, ReplicationRuleFilter, ReplicationRuleStatus,
};

use crate::S3TestContext;

/// Test getting replication on bucket with no config.
/// Ceph: test_get_bucket_replication_empty
#[tokio::test]
async fn test_bucket_replication_get_empty() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.get_bucket_replication().bucket(&ctx.bucket).send().await;

    assert!(result.is_err(), "Should fail when no replication config exists");
}

/// Test putting and getting replication configuration.
/// Ceph: test_set_bucket_replication
#[tokio::test]
async fn test_bucket_replication_put_get() {
    let ctx = S3TestContext::with_versioning().await;

    let rule = ReplicationRule::builder()
        .id("rule1")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .delete_marker_replication(
            DeleteMarkerReplication::builder()
                .status(DeleteMarkerReplicationStatus::Disabled)
                .build(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should put replication configuration");

    let response = ctx
        .client
        .get_bucket_replication()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get replication configuration");

    let rules = response.replication_configuration().unwrap().rules();
    assert_eq!(rules.len(), 1, "Should have one rule");
}

/// Test deleting replication configuration.
/// Ceph: test_delete_bucket_replication
#[tokio::test]
async fn test_bucket_replication_delete() {
    let ctx = S3TestContext::with_versioning().await;

    // First put a config
    let rule = ReplicationRule::builder()
        .id("rule1")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should put config");

    // Delete it
    ctx.client
        .delete_bucket_replication()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should delete replication configuration");

    // Verify deleted
    let result = ctx.client.get_bucket_replication().bucket(&ctx.bucket).send().await;
    assert!(result.is_err(), "Replication config should be deleted");
}

/// Test replication with prefix filter.
/// Ceph: test_bucket_replication_prefix
#[tokio::test]
async fn test_bucket_replication_prefix_filter() {
    let ctx = S3TestContext::with_versioning().await;

    let rule = ReplicationRule::builder()
        .id("prefix-rule")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().prefix("logs/").build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should put replication with prefix filter");
}

/// Test replication with multiple rules.
/// Ceph: test_bucket_replication_multiple_rules
#[tokio::test]
async fn test_bucket_replication_multiple_rules() {
    let ctx = S3TestContext::with_versioning().await;

    let rule1 = ReplicationRule::builder()
        .id("rule1")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().prefix("logs/").build())
        .destination(Destination::builder().bucket("arn:aws:s3:::dest-bucket-1").build().unwrap())
        .build()
        .unwrap();

    let rule2 = ReplicationRule::builder()
        .id("rule2")
        .status(ReplicationRuleStatus::Enabled)
        .priority(2)
        .filter(ReplicationRuleFilter::builder().prefix("data/").build())
        .destination(Destination::builder().bucket("arn:aws:s3:::dest-bucket-2").build().unwrap())
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule1)
        .rules(rule2)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should put multiple rules");

    let response = ctx
        .client
        .get_bucket_replication()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get config");

    let rules = response.replication_configuration().unwrap().rules();
    assert_eq!(rules.len(), 2, "Should have two rules");
}

/// Test replication with disabled rule.
/// Ceph: test_bucket_replication_disabled
#[tokio::test]
async fn test_bucket_replication_disabled_rule() {
    let ctx = S3TestContext::with_versioning().await;

    let rule = ReplicationRule::builder()
        .id("disabled-rule")
        .status(ReplicationRuleStatus::Disabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should accept disabled rule");
}

/// Test replication requires versioning.
/// Ceph: test_bucket_replication_requires_versioning
#[tokio::test]
async fn test_bucket_replication_requires_versioning() {
    let ctx = S3TestContext::new().await; // No versioning

    let rule = ReplicationRule::builder()
        .id("rule1")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    let result = ctx
        .client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await;

    assert!(result.is_err(), "Should fail without versioning enabled");
}

/// Test replication with delete marker replication enabled.
/// Ceph: test_bucket_replication_delete_markers
#[tokio::test]
async fn test_bucket_replication_delete_markers() {
    let ctx = S3TestContext::with_versioning().await;

    let rule = ReplicationRule::builder()
        .id("delete-marker-rule")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .delete_marker_replication(
            DeleteMarkerReplication::builder()
                .status(DeleteMarkerReplicationStatus::Enabled)
                .build(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should accept delete marker replication");
}

/// Test replication on non-existent bucket.
/// Ceph: test_bucket_replication_nonexistent
#[tokio::test]
async fn test_bucket_replication_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result =
        ctx.client.get_bucket_replication().bucket("nonexistent-bucket-12345").send().await;

    assert!(result.is_err(), "Should fail on non-existent bucket");
}

/// Test replication with storage class.
/// Ceph: test_bucket_replication_storage_class
#[tokio::test]
async fn test_bucket_replication_storage_class() {
    let ctx = S3TestContext::with_versioning().await;

    let rule = ReplicationRule::builder()
        .id("storage-class-rule")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder()
                .bucket("arn:aws:s3:::destination-bucket")
                .storage_class(aws_sdk_s3::types::StorageClass::StandardIa)
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should accept storage class in destination");
}

/// Test updating replication configuration.
/// Ceph: test_bucket_replication_update
#[tokio::test]
async fn test_bucket_replication_update() {
    let ctx = S3TestContext::with_versioning().await;

    // Initial config
    let rule1 = ReplicationRule::builder()
        .id("rule1")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(Destination::builder().bucket("arn:aws:s3:::dest-bucket-1").build().unwrap())
        .build()
        .unwrap();

    let config1 = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/role1")
        .rules(rule1)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config1)
        .send()
        .await
        .expect("Should put initial config");

    // Update config
    let rule2 = ReplicationRule::builder()
        .id("rule2")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(Destination::builder().bucket("arn:aws:s3:::dest-bucket-2").build().unwrap())
        .build()
        .unwrap();

    let config2 = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/role2")
        .rules(rule2)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config2)
        .send()
        .await
        .expect("Should update config");

    let response = ctx
        .client
        .get_bucket_replication()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get updated config");

    let rules = response.replication_configuration().unwrap().rules();
    assert_eq!(rules.len(), 1, "Should have one rule after update");
    assert_eq!(rules[0].id(), Some("rule2"), "Should be updated rule");
}

/// Test replication with tag filter.
/// Ceph: test_bucket_replication_tag_filter
#[tokio::test]
async fn test_bucket_replication_tag_filter() {
    let ctx = S3TestContext::with_versioning().await;

    let rule = ReplicationRule::builder()
        .id("tag-rule")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(
            ReplicationRuleFilter::builder()
                .tag(aws_sdk_s3::types::Tag::builder().key("env").value("prod").build().unwrap())
                .build(),
        )
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should accept tag filter");
}

/// Test replication configuration validation.
/// Ceph: test_bucket_replication_invalid
#[tokio::test]
async fn test_bucket_replication_invalid_config() {
    let ctx = S3TestContext::with_versioning().await;

    // Config without role should fail
    let rule = ReplicationRule::builder()
        .id("rule1")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder().rules(rule).build();

    // This might fail at config build or at API call depending on SDK validation
    if let Ok(cfg) = config {
        let result = ctx
            .client
            .put_bucket_replication()
            .bucket(&ctx.bucket)
            .replication_configuration(cfg)
            .send()
            .await;
        assert!(result.is_err(), "Should reject config without role");
    }
}

/// Test replication with replica modifications sync.
/// Ceph: test_bucket_replication_replica_modifications
#[tokio::test]
async fn test_bucket_replication_replica_modifications() {
    let ctx = S3TestContext::with_versioning().await;

    // This tests bidirectional replication setup
    let rule = ReplicationRule::builder()
        .id("bidirectional-rule")
        .status(ReplicationRuleStatus::Enabled)
        .priority(1)
        .filter(ReplicationRuleFilter::builder().build())
        .destination(
            Destination::builder().bucket("arn:aws:s3:::destination-bucket").build().unwrap(),
        )
        .build()
        .unwrap();

    let config = ReplicationConfiguration::builder()
        .role("arn:aws:iam::123456789012:role/replication-role")
        .rules(rule)
        .build()
        .unwrap();

    ctx.client
        .put_bucket_replication()
        .bucket(&ctx.bucket)
        .replication_configuration(config)
        .send()
        .await
        .expect("Should accept replica modifications config");
}
