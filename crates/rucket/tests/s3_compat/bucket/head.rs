//! Bucket HEAD tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_head_*
//! - MinIO Mint: HeadBucket tests

use crate::{random_bucket_name, S3TestContext};

/// Test HEAD on an existing bucket.
/// Ceph: test_bucket_head
#[tokio::test]
async fn test_bucket_head_exists() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_ok(), "HEAD on existing bucket should succeed");
}

/// Test HEAD on a non-existent bucket.
/// Ceph: test_bucket_head_notfound
#[tokio::test]
async fn test_bucket_head_not_found() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.head_bucket().bucket("nonexistent-bucket-xyz").send().await;

    assert!(result.is_err(), "HEAD on non-existent bucket should fail");
}

/// Test HEAD returns appropriate headers.
#[tokio::test]
async fn test_bucket_head_headers() {
    let ctx = S3TestContext::new().await;

    // HeadBucket doesn't return much in the response body,
    // but it should succeed for an existing bucket
    let result = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_ok(), "HEAD should succeed");
}

/// Test HEAD on bucket just after creation.
#[tokio::test]
async fn test_bucket_head_immediately_after_create() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    // HEAD immediately after creation
    let result = ctx.client.head_bucket().bucket(&bucket).send().await;

    assert!(result.is_ok(), "HEAD should succeed immediately after creation");
}

/// Test HEAD on bucket after deletion fails.
#[tokio::test]
async fn test_bucket_head_after_delete() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    ctx.client.delete_bucket().bucket(&bucket).send().await.expect("Should delete bucket");

    let result = ctx.client.head_bucket().bucket(&bucket).send().await;

    assert!(result.is_err(), "HEAD should fail after deletion");
}

/// Test HEAD with empty bucket name fails.
#[tokio::test]
async fn test_bucket_head_empty_name() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.head_bucket().bucket("").send().await;

    assert!(result.is_err(), "HEAD with empty bucket name should fail");
}
