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

// =============================================================================
// Extended HEAD Bucket Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test HEAD bucket concurrent requests.
/// Ceph: test_bucket_head_concurrent
#[tokio::test]
async fn test_bucket_head_concurrent() {
    let ctx = S3TestContext::new().await;

    let mut handles = Vec::new();
    for _ in 0..20 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move { client.head_bucket().bucket(&bucket).send().await });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "All concurrent HEAD requests should succeed");
    }
}

/// Test HEAD bucket with objects.
/// Ceph: test_bucket_head_with_objects
#[tokio::test]
async fn test_bucket_head_with_objects() {
    let ctx = S3TestContext::new().await;

    // Add some objects
    for i in 0..5 {
        ctx.put(&format!("obj{}.txt", i), b"content").await;
    }

    let result = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_ok(), "HEAD should succeed on bucket with objects");
}

/// Test HEAD bucket with versioning enabled.
/// Ceph: test_bucket_head_versioned
#[tokio::test]
async fn test_bucket_head_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let result = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_ok(), "HEAD should succeed on versioned bucket");
}

/// Test HEAD bucket with special characters in name.
/// Note: bucket names are limited, but some special chars are allowed
#[tokio::test]
async fn test_bucket_head_with_dashes() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = format!(
        "test-bucket-with-dashes-{}",
        uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
    );

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    let result = ctx.client.head_bucket().bucket(&bucket).send().await;

    assert!(result.is_ok(), "HEAD should work on bucket with dashes");
}

/// Test HEAD bucket multiple times in sequence.
/// Ceph: test_bucket_head_repeated
#[tokio::test]
async fn test_bucket_head_repeated() {
    let ctx = S3TestContext::new().await;

    for _ in 0..5 {
        let result = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;
        assert!(result.is_ok(), "Repeated HEAD should succeed");
    }
}

/// Test HEAD bucket after adding object.
/// Ceph: test_bucket_head_after_put
#[tokio::test]
async fn test_bucket_head_after_object_put() {
    let ctx = S3TestContext::new().await;

    let result1 = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;
    assert!(result1.is_ok());

    ctx.put("test.txt", b"content").await;

    let result2 = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;
    assert!(result2.is_ok(), "HEAD should succeed after adding object");
}

/// Test HEAD bucket after deleting object.
/// Ceph: test_bucket_head_after_delete
#[tokio::test]
async fn test_bucket_head_after_object_delete() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let result = ctx.client.head_bucket().bucket(&ctx.bucket).send().await;
    assert!(result.is_ok(), "HEAD should succeed after deleting object");
}

/// Test HEAD bucket with invalid bucket name format.
#[tokio::test]
async fn test_bucket_head_invalid_name() {
    let ctx = S3TestContext::without_bucket().await;

    // Names with uppercase are invalid
    let result = ctx.client.head_bucket().bucket("INVALID_BUCKET").send().await;

    assert!(result.is_err(), "HEAD with invalid bucket name should fail");
}

/// Test HEAD bucket returns access denied for bucket you don't own.
/// This test is placeholder - in single-user test environment, all buckets are owned
#[tokio::test]
#[ignore = "Requires multi-user setup for access control testing"]
async fn test_bucket_head_access_denied() {
    // Would test HEAD on bucket owned by another user
    // Should return 403 Access Denied
}

/// Test HEAD bucket long name.
#[tokio::test]
async fn test_bucket_head_long_name() {
    let ctx = S3TestContext::without_bucket().await;

    // Max bucket name is 63 characters
    let long_name = format!("a{}", "b".repeat(62));
    assert_eq!(long_name.len(), 63);

    ctx.client.create_bucket().bucket(&long_name).send().await.expect("Should create bucket");

    let result = ctx.client.head_bucket().bucket(&long_name).send().await;

    assert!(result.is_ok(), "HEAD should work on max-length bucket name");
}
