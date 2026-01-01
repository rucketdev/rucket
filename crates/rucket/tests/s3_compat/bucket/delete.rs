//! Bucket deletion tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_delete_*
//! - MinIO Mint: bucket deletion tests

use crate::{random_bucket_name, S3TestContext};
use aws_sdk_s3::primitives::ByteStream;

/// Test deleting an empty bucket.
/// Ceph: test_bucket_delete_empty
#[tokio::test]
async fn test_bucket_delete_empty() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should create bucket");

    ctx.client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should delete empty bucket");

    // Verify bucket no longer exists
    let result = ctx.client.head_bucket().bucket(&bucket).send().await;
    assert!(result.is_err(), "Bucket should not exist after deletion");
}

/// Test that deleting a non-empty bucket fails.
/// Ceph: test_bucket_delete_nonempty
#[tokio::test]
async fn test_bucket_delete_non_empty_fails() {
    let ctx = S3TestContext::new().await;

    // Put an object in the bucket
    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Should put object");

    // Try to delete non-empty bucket
    let result = ctx.client.delete_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_err(), "Should not delete non-empty bucket");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(
        error_str.contains("BucketNotEmpty"),
        "Expected BucketNotEmpty error, got: {}",
        error_str
    );
}

/// Test that deleting a non-existent bucket fails.
/// Ceph: test_bucket_delete_nonexistent
#[tokio::test]
async fn test_bucket_delete_nonexistent() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx
        .client
        .delete_bucket()
        .bucket("nonexistent-bucket-12345")
        .send()
        .await;

    assert!(result.is_err(), "Should not delete non-existent bucket");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(
        error_str.contains("NoSuchBucket"),
        "Expected NoSuchBucket error, got: {}",
        error_str
    );
}

/// Test that bucket can be deleted after removing all objects.
/// Ceph: test_bucket_delete_after_objects_removed
#[tokio::test]
async fn test_bucket_delete_after_objects_removed() {
    let ctx = S3TestContext::new().await;

    // Put objects
    for i in 0..3 {
        let body = ByteStream::from_static(b"content");
        ctx.client
            .put_object()
            .bucket(&ctx.bucket)
            .key(format!("file{}.txt", i))
            .body(body)
            .send()
            .await
            .expect("Should put object");
    }

    // Delete all objects
    for i in 0..3 {
        ctx.client
            .delete_object()
            .bucket(&ctx.bucket)
            .key(format!("file{}.txt", i))
            .send()
            .await
            .expect("Should delete object");
    }

    // Now bucket should be deletable
    ctx.client
        .delete_bucket()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should delete bucket after removing objects");
}

/// Test that deleting a bucket is idempotent within a short window.
/// Note: After first successful delete, subsequent deletes return NoSuchBucket.
#[tokio::test]
async fn test_bucket_delete_twice() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should create bucket");

    // First delete
    ctx.client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("First delete should succeed");

    // Second delete should fail with NoSuchBucket
    let result = ctx.client.delete_bucket().bucket(&bucket).send().await;
    assert!(result.is_err(), "Second delete should fail");
}

/// Test deleting a bucket that was just created and is empty.
/// This tests the create-delete lifecycle.
#[tokio::test]
async fn test_bucket_create_then_delete() {
    let ctx = S3TestContext::without_bucket().await;

    for _ in 0..3 {
        let bucket = random_bucket_name();

        ctx.client
            .create_bucket()
            .bucket(&bucket)
            .send()
            .await
            .expect("Should create bucket");

        ctx.client
            .delete_bucket()
            .bucket(&bucket)
            .send()
            .await
            .expect("Should delete bucket");
    }
}
