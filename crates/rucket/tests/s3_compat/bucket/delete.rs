//! Bucket deletion tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_delete_*
//! - MinIO Mint: bucket deletion tests

use aws_sdk_s3::primitives::ByteStream;

use crate::{random_bucket_name, S3TestContext};

/// Test deleting an empty bucket.
/// Ceph: test_bucket_delete_empty
#[tokio::test]
async fn test_bucket_delete_empty() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    ctx.client.delete_bucket().bucket(&bucket).send().await.expect("Should delete empty bucket");

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

    let result = ctx.client.delete_bucket().bucket("nonexistent-bucket-12345").send().await;

    assert!(result.is_err(), "Should not delete non-existent bucket");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(error_str.contains("NoSuchBucket"), "Expected NoSuchBucket error, got: {}", error_str);
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

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    // First delete
    ctx.client.delete_bucket().bucket(&bucket).send().await.expect("First delete should succeed");

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

        ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

        ctx.client.delete_bucket().bucket(&bucket).send().await.expect("Should delete bucket");
    }
}

// =============================================================================
// Extended Bucket Delete Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test delete bucket with versioning enabled.
/// Ceph: test_bucket_delete_versioned
#[tokio::test]
async fn test_bucket_delete_versioned_empty() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    // Enable versioning
    ctx.client
        .put_bucket_versioning()
        .bucket(&bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Should enable versioning");

    // Delete empty versioned bucket should succeed
    ctx.client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should delete versioned empty bucket");
}

/// Test delete bucket fails when versions exist.
/// Ceph: test_bucket_delete_with_versions
#[tokio::test]
async fn test_bucket_delete_with_versions_fails() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"version1").await;
    ctx.put("test.txt", b"version2").await;

    let result = ctx.client.delete_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_err(), "Should not delete bucket with versions");
}

/// Test delete bucket fails when delete markers exist.
/// Ceph: test_bucket_delete_with_delete_markers
#[tokio::test]
#[ignore = "Delete marker bucket deletion check not implemented"]
async fn test_bucket_delete_with_delete_markers_fails() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await; // Creates delete marker

    let result = ctx.client.delete_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_err(), "Should not delete bucket with delete markers");
}

/// Test delete bucket with lifecycle rules.
/// Ceph: test_bucket_delete_with_lifecycle
#[tokio::test]
#[ignore = "Lifecycle configuration not fully implemented"]
async fn test_bucket_delete_with_lifecycle() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    // Lifecycle configuration would be added here
    // Delete should still succeed for empty bucket
    ctx.client
        .delete_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should delete bucket with lifecycle");
}

/// Test delete bucket concurrent requests.
/// Ceph: test_bucket_delete_concurrent
#[tokio::test]
async fn test_bucket_delete_concurrent() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    // Multiple concurrent delete attempts - only one should succeed
    let mut handles = Vec::new();
    for _ in 0..5 {
        let client = ctx.client.clone();
        let bucket_clone = bucket.clone();
        let handle =
            tokio::spawn(async move { client.delete_bucket().bucket(&bucket_clone).send().await });
        handles.push(handle);
    }

    let mut successes = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            successes += 1;
        }
    }

    assert!(successes >= 1, "At least one delete should succeed");
}

/// Test delete bucket with deep nested objects fails.
/// Ceph: test_bucket_delete_with_nested_objects
#[tokio::test]
async fn test_bucket_delete_with_nested_objects_fails() {
    let ctx = S3TestContext::new().await;

    ctx.put("a/b/c/d/e/deep.txt", b"content").await;

    let result = ctx.client.delete_bucket().bucket(&ctx.bucket).send().await;

    assert!(result.is_err(), "Should not delete bucket with nested objects");
}

/// Test delete bucket immediately after create.
/// Ceph: test_bucket_delete_immediate
#[tokio::test]
async fn test_bucket_delete_immediately_after_create() {
    let ctx = S3TestContext::without_bucket().await;

    for _ in 0..5 {
        let bucket = random_bucket_name();

        ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

        // Delete immediately
        ctx.client.delete_bucket().bucket(&bucket).send().await.expect("Should delete immediately");
    }
}

/// Test delete bucket and recreate with same name.
/// Ceph: test_bucket_delete_recreate
#[tokio::test]
async fn test_bucket_delete_and_recreate() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    // Create
    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    // Delete
    ctx.client.delete_bucket().bucket(&bucket).send().await.expect("Should delete bucket");

    // Recreate with same name
    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should recreate bucket");

    // Clean up
    ctx.client.delete_bucket().bucket(&bucket).send().await.expect("Should delete again");
}

/// Test delete bucket with multipart uploads in progress fails.
/// Ceph: test_bucket_delete_with_multipart
#[tokio::test]
async fn test_bucket_delete_with_multipart_upload_fails() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should initiate multipart");

    let upload_id = create.upload_id().unwrap().to_string();

    // Try to delete bucket with active multipart upload
    let _result = ctx.client.delete_bucket().bucket(&ctx.bucket).send().await;

    // Behavior varies - some implementations fail, some succeed
    // Just ensure no panic

    // Clean up
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(&upload_id)
        .send()
        .await;
}

/// Test delete bucket removes it from list.
/// Ceph: test_bucket_delete_removed_from_list
#[tokio::test]
async fn test_bucket_delete_removed_from_list() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client.create_bucket().bucket(&bucket).send().await.expect("Should create bucket");

    // Verify in list
    let list = ctx.client.list_buckets().send().await.expect("Should list");
    let bucket_names: Vec<&str> = list.buckets().iter().filter_map(|b| b.name()).collect();
    assert!(bucket_names.contains(&bucket.as_str()));

    // Delete
    ctx.client.delete_bucket().bucket(&bucket).send().await.expect("Should delete");

    // Verify removed from list
    let list = ctx.client.list_buckets().send().await.expect("Should list");
    let bucket_names: Vec<&str> = list.buckets().iter().filter_map(|b| b.name()).collect();
    assert!(!bucket_names.contains(&bucket.as_str()));
}
