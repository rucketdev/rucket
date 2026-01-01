//! Bucket versioning tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_versioning_*
//! - MinIO Mint: versioning tests

use crate::S3TestContext;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};

/// Test enabling versioning on a bucket.
/// Ceph: test_versioning_bucket_create_versioned
#[tokio::test]
async fn test_bucket_versioning_enable() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Should enable versioning");

    let response = ctx
        .client
        .get_bucket_versioning()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get versioning status");

    assert_eq!(
        response.status(),
        Some(&BucketVersioningStatus::Enabled),
        "Versioning should be enabled"
    );
}

/// Test suspending versioning on a bucket.
/// Ceph: test_versioning_suspend
#[tokio::test]
async fn test_bucket_versioning_suspend() {
    let ctx = S3TestContext::new().await;

    // First enable versioning
    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Should enable versioning");

    // Then suspend it
    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .expect("Should suspend versioning");

    let response = ctx
        .client
        .get_bucket_versioning()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get versioning status");

    assert_eq!(
        response.status(),
        Some(&BucketVersioningStatus::Suspended),
        "Versioning should be suspended"
    );
}

/// Test getting versioning status on unversioned bucket.
/// Ceph: test_versioning_get_unversioned
#[tokio::test]
async fn test_bucket_versioning_get_unversioned() {
    let ctx = S3TestContext::new().await;

    let response = ctx
        .client
        .get_bucket_versioning()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get versioning status");

    // Unversioned bucket returns None for status
    assert!(
        response.status().is_none(),
        "Unversioned bucket should have no versioning status"
    );
}

/// Test re-enabling versioning after suspension.
/// Ceph: test_versioning_re_enable
#[tokio::test]
async fn test_bucket_versioning_re_enable() {
    let ctx = S3TestContext::new().await;

    // Enable
    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Should enable versioning");

    // Suspend
    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .expect("Should suspend versioning");

    // Re-enable
    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Should re-enable versioning");

    let response = ctx
        .client
        .get_bucket_versioning()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get versioning status");

    assert_eq!(
        response.status(),
        Some(&BucketVersioningStatus::Enabled),
        "Versioning should be enabled"
    );
}

/// Test versioning on non-existent bucket fails.
#[tokio::test]
async fn test_bucket_versioning_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx
        .client
        .get_bucket_versioning()
        .bucket("nonexistent-bucket-xyz")
        .send()
        .await;

    assert!(result.is_err(), "Should fail on non-existent bucket");
}

/// Test putting versioning with empty bucket name fails.
#[tokio::test]
async fn test_bucket_versioning_empty_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx
        .client
        .put_bucket_versioning()
        .bucket("")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await;

    assert!(result.is_err(), "Should fail with empty bucket name");
}
