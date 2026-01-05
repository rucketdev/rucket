//! Bucket lifecycle configuration tests.
//!
//! Tests for S3 bucket lifecycle configuration API including:
//! - PUT bucket lifecycle configuration
//! - GET bucket lifecycle configuration
//! - DELETE bucket lifecycle configuration
//!
//! Note: Lifecycle policies ARE implemented in Rucket (see rucket-api handlers).
//! These tests verify the CRUD operations for lifecycle configuration.

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::types::{
    BucketLifecycleConfiguration, ExpirationStatus, LifecycleExpiration, LifecycleRule,
    LifecycleRuleFilter,
};

use crate::S3TestContext;

/// Helper to create a lifecycle rule filter with prefix.
fn prefix_filter(prefix: &str) -> LifecycleRuleFilter {
    LifecycleRuleFilter::builder().prefix(prefix).build()
}

// =============================================================================
// Basic Lifecycle CRUD Tests
// =============================================================================

/// Test putting lifecycle configuration on a bucket.
#[tokio::test]
async fn test_bucket_lifecycle_put() {
    let ctx = S3TestContext::new().await;

    // Create a simple lifecycle rule with expiration
    let rule = LifecycleRule::builder()
        .id("expire-after-30-days")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter("")) // Apply to all objects
        .expiration(LifecycleExpiration::builder().days(30).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build lifecycle configuration");

    // Put the lifecycle configuration
    let result = ctx
        .client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config)
        .send()
        .await;

    assert!(result.is_ok(), "PUT lifecycle configuration should succeed");
}

/// Test getting lifecycle configuration from a bucket.
#[tokio::test]
async fn test_bucket_lifecycle_get() {
    let ctx = S3TestContext::new().await;

    // First, put a lifecycle configuration
    let rule = LifecycleRule::builder()
        .id("test-rule")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter("logs/"))
        .expiration(LifecycleExpiration::builder().days(7).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config)
        .send()
        .await
        .expect("PUT lifecycle should succeed");

    // Now get the configuration
    let result = ctx
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET lifecycle should succeed");

    // Verify the configuration
    let rules = result.rules();
    assert_eq!(rules.len(), 1, "Should have one rule");

    let retrieved_rule = &rules[0];
    assert_eq!(retrieved_rule.id(), Some("test-rule"));
    assert_eq!(retrieved_rule.status(), &ExpirationStatus::Enabled);

    // Verify expiration days
    if let Some(expiration) = retrieved_rule.expiration() {
        assert_eq!(expiration.days(), Some(7));
    } else {
        panic!("Rule should have expiration");
    }
}

/// Test deleting lifecycle configuration from a bucket.
#[tokio::test]
async fn test_bucket_lifecycle_delete() {
    let ctx = S3TestContext::new().await;

    // First, put a lifecycle configuration
    let rule = LifecycleRule::builder()
        .id("delete-test-rule")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter(""))
        .expiration(LifecycleExpiration::builder().days(1).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config)
        .send()
        .await
        .expect("PUT lifecycle should succeed");

    // Verify it exists
    ctx.client
        .get_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET lifecycle should succeed before delete");

    // Delete the lifecycle configuration
    ctx.client
        .delete_bucket_lifecycle()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("DELETE lifecycle should succeed");

    // Verify it's deleted - should return NoSuchLifecycleConfiguration error
    let result = ctx.client.get_bucket_lifecycle_configuration().bucket(&ctx.bucket).send().await;

    assert!(result.is_err(), "GET lifecycle should fail after delete");

    // Verify it's the expected error type
    if let Err(SdkError::ServiceError(err)) = &result {
        let code = err.err().meta().code();
        assert_eq!(
            code,
            Some("NoSuchLifecycleConfiguration"),
            "Error should be NoSuchLifecycleConfiguration"
        );
    }
}

// =============================================================================
// Lifecycle Rule Configuration Tests
// =============================================================================

/// Test lifecycle expiration rule with days.
#[tokio::test]
async fn test_bucket_lifecycle_expiration() {
    let ctx = S3TestContext::new().await;

    let rule = LifecycleRule::builder()
        .id("expire-30-days")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter(""))
        .expiration(LifecycleExpiration::builder().days(30).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config)
        .send()
        .await
        .expect("PUT lifecycle should succeed");

    let result = ctx
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET lifecycle should succeed");

    let rules = result.rules();
    assert_eq!(rules.len(), 1);

    let expiration = rules[0].expiration().expect("Should have expiration");
    assert_eq!(expiration.days(), Some(30));
}

/// Test lifecycle with prefix filter.
#[tokio::test]
async fn test_bucket_lifecycle_prefix() {
    let ctx = S3TestContext::new().await;

    let rule = LifecycleRule::builder()
        .id("expire-logs")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter("logs/"))
        .expiration(LifecycleExpiration::builder().days(14).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config)
        .send()
        .await
        .expect("PUT lifecycle should succeed");

    let result = ctx
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET lifecycle should succeed");

    let rules = result.rules();
    assert_eq!(rules.len(), 1);

    // Verify the prefix filter
    if let Some(filter) = rules[0].filter() {
        assert_eq!(filter.prefix(), Some("logs/"));
    } else {
        panic!("Rule should have prefix filter");
    }
}

/// Test lifecycle with multiple rules.
#[tokio::test]
async fn test_bucket_lifecycle_multiple_rules() {
    let ctx = S3TestContext::new().await;

    let rule1 = LifecycleRule::builder()
        .id("expire-logs")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter("logs/"))
        .expiration(LifecycleExpiration::builder().days(7).build())
        .build()
        .expect("Failed to build lifecycle rule 1");

    let rule2 = LifecycleRule::builder()
        .id("expire-temp")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter("temp/"))
        .expiration(LifecycleExpiration::builder().days(1).build())
        .build()
        .expect("Failed to build lifecycle rule 2");

    let config = BucketLifecycleConfiguration::builder()
        .rules(rule1)
        .rules(rule2)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config)
        .send()
        .await
        .expect("PUT lifecycle should succeed");

    let result = ctx
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET lifecycle should succeed");

    let rules = result.rules();
    assert_eq!(rules.len(), 2, "Should have two rules");

    // Verify both rules exist (order may vary)
    let rule_ids: Vec<_> = rules.iter().filter_map(|r| r.id()).collect();
    assert!(rule_ids.contains(&"expire-logs"));
    assert!(rule_ids.contains(&"expire-temp"));
}

/// Test lifecycle overwriting existing configuration.
#[tokio::test]
async fn test_bucket_lifecycle_overwrite() {
    let ctx = S3TestContext::new().await;

    // Put initial configuration
    let rule1 = LifecycleRule::builder()
        .id("initial-rule")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter(""))
        .expiration(LifecycleExpiration::builder().days(30).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config1 = BucketLifecycleConfiguration::builder()
        .rules(rule1)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config1)
        .send()
        .await
        .expect("First PUT should succeed");

    // Overwrite with new configuration
    let rule2 = LifecycleRule::builder()
        .id("new-rule")
        .status(ExpirationStatus::Enabled)
        .filter(prefix_filter(""))
        .expiration(LifecycleExpiration::builder().days(7).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config2 = BucketLifecycleConfiguration::builder()
        .rules(rule2)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config2)
        .send()
        .await
        .expect("Second PUT should succeed");

    // Verify the new configuration replaced the old one
    let result = ctx
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET should succeed");

    let rules = result.rules();
    assert_eq!(rules.len(), 1, "Should have only one rule after overwrite");
    assert_eq!(rules[0].id(), Some("new-rule"), "Should have the new rule");
}

/// Test lifecycle with disabled status.
#[tokio::test]
async fn test_bucket_lifecycle_status_disabled() {
    let ctx = S3TestContext::new().await;

    let rule = LifecycleRule::builder()
        .id("disabled-rule")
        .status(ExpirationStatus::Disabled)
        .filter(prefix_filter(""))
        .expiration(LifecycleExpiration::builder().days(30).build())
        .build()
        .expect("Failed to build lifecycle rule");

    let config = BucketLifecycleConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build lifecycle configuration");

    ctx.client
        .put_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .lifecycle_configuration(config)
        .send()
        .await
        .expect("PUT lifecycle should succeed");

    let result = ctx
        .client
        .get_bucket_lifecycle_configuration()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET lifecycle should succeed");

    let rules = result.rules();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0].status(), &ExpirationStatus::Disabled);
}

/// Test getting lifecycle configuration from bucket without one.
#[tokio::test]
async fn test_bucket_lifecycle_get_nonexistent() {
    let ctx = S3TestContext::new().await;

    // Try to get lifecycle configuration from a bucket that doesn't have one
    let result = ctx.client.get_bucket_lifecycle_configuration().bucket(&ctx.bucket).send().await;

    // Should return NoSuchLifecycleConfiguration error
    assert!(result.is_err(), "GET lifecycle should fail for bucket without configuration");

    if let Err(SdkError::ServiceError(err)) = &result {
        let code = err.err().meta().code();
        assert_eq!(
            code,
            Some("NoSuchLifecycleConfiguration"),
            "Error should be NoSuchLifecycleConfiguration"
        );
    }
}
