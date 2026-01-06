//! Bucket logging tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_logging_*
//! - AWS S3 documentation: Bucket logging

use aws_sdk_s3::types::{BucketLoggingStatus, LoggingEnabled};

use crate::S3TestContext;

/// Test getting logging configuration on bucket with no config.
/// Should return empty logging status (not an error like other configs).
#[tokio::test]
async fn test_bucket_logging_get_empty() {
    let ctx = S3TestContext::new().await;

    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get logging configuration (empty)");

    // Logging returns empty status instead of error when not configured
    assert!(response.logging_enabled().is_none(), "Should have no logging enabled");
}

/// Test putting and getting basic logging configuration.
/// Ceph: test_logging_set
#[tokio::test]
async fn test_bucket_logging_put_get() {
    let ctx = S3TestContext::new().await;

    // Create target bucket for logs
    let log_bucket = format!("{}-logs", ctx.bucket);
    ctx.client.create_bucket().bucket(&log_bucket).send().await.unwrap();

    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(&log_bucket)
        .target_prefix("logs/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should put logging configuration");

    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get logging configuration");

    let enabled = response.logging_enabled().expect("Should have logging enabled");
    assert_eq!(enabled.target_bucket(), &log_bucket, "Target bucket should match");
    assert_eq!(enabled.target_prefix(), "logs/", "Target prefix should match");
}

/// Test logging with target bucket.
/// Ceph: test_logging_target_bucket
#[tokio::test]
async fn test_bucket_logging_target_bucket() {
    let ctx = S3TestContext::new().await;

    // Create target bucket for logs
    let log_bucket = format!("{}-logs", ctx.bucket);
    ctx.client.create_bucket().bucket(&log_bucket).send().await.unwrap();

    let logging_enabled =
        LoggingEnabled::builder().target_bucket(&log_bucket).target_prefix("").build().unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should put logging configuration with target bucket");

    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get logging configuration");

    let enabled = response.logging_enabled().expect("Should have logging enabled");
    assert_eq!(enabled.target_bucket(), &log_bucket);
}

/// Test logging with target prefix.
/// Ceph: test_logging_target_prefix
#[tokio::test]
async fn test_bucket_logging_target_prefix() {
    let ctx = S3TestContext::new().await;

    // Create target bucket for logs
    let log_bucket = format!("{}-logs", ctx.bucket);
    ctx.client.create_bucket().bucket(&log_bucket).send().await.unwrap();

    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(&log_bucket)
        .target_prefix("my-prefix/access-logs/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should put logging configuration with prefix");

    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get logging configuration");

    let enabled = response.logging_enabled().expect("Should have logging enabled");
    assert_eq!(enabled.target_prefix(), "my-prefix/access-logs/");
}

/// Test logging to same bucket.
/// Ceph: test_logging_same_bucket
#[tokio::test]
async fn test_bucket_logging_same_bucket() {
    let ctx = S3TestContext::new().await;

    // Log to same bucket - this is allowed by S3
    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(&ctx.bucket)
        .target_prefix("logs/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should allow logging to same bucket");
}

/// Test logging to non-existent bucket.
/// Ceph: test_logging_nonexistent_target
#[tokio::test]
async fn test_bucket_logging_nonexistent_target() {
    let ctx = S3TestContext::new().await;

    let logging_enabled = LoggingEnabled::builder()
        .target_bucket("nonexistent-bucket-12345")
        .target_prefix("logs/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    let result = ctx
        .client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await;

    assert!(result.is_err(), "Should fail when target bucket does not exist");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(
        error_str.contains("InvalidTargetBucketForLogging") || error_str.contains("NoSuchBucket"),
        "Expected InvalidTargetBucketForLogging or NoSuchBucket error, got: {}",
        error_str
    );
}

/// Test disabling logging (empty config).
/// Ceph: test_logging_delete
#[tokio::test]
async fn test_bucket_logging_disable() {
    let ctx = S3TestContext::new().await;

    // First enable logging
    let log_bucket = format!("{}-logs", ctx.bucket);
    ctx.client.create_bucket().bucket(&log_bucket).send().await.unwrap();

    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(&log_bucket)
        .target_prefix("logs/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should put logging configuration");

    // Disable by sending empty config
    let empty_config = BucketLoggingStatus::builder().build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(empty_config)
        .send()
        .await
        .expect("Should disable logging");

    // Verify it's disabled
    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get logging configuration");

    assert!(response.logging_enabled().is_none(), "Logging should be disabled");
}

/// Test logging configuration on non-existent bucket.
#[tokio::test]
async fn test_bucket_logging_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.get_bucket_logging().bucket("nonexistent-bucket-12345").send().await;

    assert!(result.is_err(), "Should fail on non-existent bucket");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(error_str.contains("NoSuchBucket"), "Expected NoSuchBucket error, got: {}", error_str);
}

/// Test updating logging configuration.
#[tokio::test]
async fn test_bucket_logging_update() {
    let ctx = S3TestContext::new().await;

    // Create two target buckets
    let log_bucket1 = format!("{}-logs1", ctx.bucket);
    let log_bucket2 = format!("{}-logs2", ctx.bucket);
    ctx.client.create_bucket().bucket(&log_bucket1).send().await.unwrap();
    ctx.client.create_bucket().bucket(&log_bucket2).send().await.unwrap();

    // Initial config
    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(&log_bucket1)
        .target_prefix("logs1/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should put initial logging configuration");

    // Update config
    let logging_enabled2 = LoggingEnabled::builder()
        .target_bucket(&log_bucket2)
        .target_prefix("logs2/")
        .build()
        .unwrap();

    let config2 = BucketLoggingStatus::builder().logging_enabled(logging_enabled2).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config2)
        .send()
        .await
        .expect("Should update logging configuration");

    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get updated logging configuration");

    let enabled = response.logging_enabled().expect("Should have logging enabled");
    assert_eq!(enabled.target_bucket(), &log_bucket2, "Target bucket should be updated");
    assert_eq!(enabled.target_prefix(), "logs2/", "Target prefix should be updated");
}

/// Test logging XML format.
/// Ceph: test_logging_xml
#[tokio::test]
async fn test_bucket_logging_xml_format() {
    let ctx = S3TestContext::new().await;

    // Create target bucket for logs
    let log_bucket = format!("{}-logs", ctx.bucket);
    ctx.client.create_bucket().bucket(&log_bucket).send().await.unwrap();

    let logging_enabled = LoggingEnabled::builder()
        .target_bucket(&log_bucket)
        .target_prefix("prefix/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should put logging configuration");

    // Verify we can retrieve it back correctly
    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get logging configuration");

    let enabled = response.logging_enabled().expect("Should have logging enabled");
    assert_eq!(enabled.target_bucket(), &log_bucket);
    assert_eq!(enabled.target_prefix(), "prefix/");
}

/// Test logging with empty prefix.
#[tokio::test]
async fn test_bucket_logging_empty_prefix() {
    let ctx = S3TestContext::new().await;

    // Create target bucket for logs
    let log_bucket = format!("{}-logs", ctx.bucket);
    ctx.client.create_bucket().bucket(&log_bucket).send().await.unwrap();

    let logging_enabled =
        LoggingEnabled::builder().target_bucket(&log_bucket).target_prefix("").build().unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    ctx.client
        .put_bucket_logging()
        .bucket(&ctx.bucket)
        .bucket_logging_status(config)
        .send()
        .await
        .expect("Should put logging configuration with empty prefix");

    let response = ctx
        .client
        .get_bucket_logging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get logging configuration");

    let enabled = response.logging_enabled().expect("Should have logging enabled");
    assert_eq!(enabled.target_prefix(), "");
}

/// Test PUT logging on non-existent bucket.
#[tokio::test]
async fn test_bucket_logging_put_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let logging_enabled = LoggingEnabled::builder()
        .target_bucket("some-target-bucket")
        .target_prefix("logs/")
        .build()
        .unwrap();

    let config = BucketLoggingStatus::builder().logging_enabled(logging_enabled).build();

    let result = ctx
        .client
        .put_bucket_logging()
        .bucket("nonexistent-bucket-12345")
        .bucket_logging_status(config)
        .send()
        .await;

    assert!(result.is_err(), "Should fail on non-existent bucket");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(error_str.contains("NoSuchBucket"), "Expected NoSuchBucket error, got: {}", error_str);
}
