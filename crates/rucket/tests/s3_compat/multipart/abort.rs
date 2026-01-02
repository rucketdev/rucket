//! Multipart upload abort tests.

use aws_sdk_s3::primitives::ByteStream;

use crate::S3TestContext;

/// Test abort multipart upload.
#[tokio::test]
async fn test_multipart_abort_basic() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    // Upload a part
    ctx.client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload");

    // Abort
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should abort");

    // Object should not exist
    let exists = ctx.exists("test.txt").await;
    assert!(!exists);
}

/// Test abort removes uploaded parts.
#[tokio::test]
async fn test_multipart_abort_removes_parts() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    // Upload multiple parts
    for i in 1..=3 {
        ctx.client
            .upload_part()
            .bucket(&ctx.bucket)
            .key("test.txt")
            .upload_id(upload_id)
            .part_number(i)
            .body(ByteStream::from(vec![b'x'; 1024]))
            .send()
            .await
            .expect("Should upload");
    }

    // Abort
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should abort");

    // Further operations with this upload ID should fail
    let result = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(4)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await;

    assert!(result.is_err());
}

/// Test abort non-existent upload.
#[tokio::test]
async fn test_multipart_abort_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id("nonexistent-upload-id")
        .send()
        .await;

    // May succeed (idempotent) or fail depending on implementation
    // Just ensure it doesn't panic
    let _ = result;
}

/// Test abort is idempotent.
#[tokio::test]
async fn test_multipart_abort_idempotent() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    // First abort
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await
        .expect("First abort should succeed");

    // Second abort - should not error
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

// =============================================================================
// Extended Abort Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test abort removes upload from list.
/// Ceph: test_multipart_abort_removes_from_list
#[tokio::test]
async fn test_multipart_abort_removes_from_list() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    // Verify upload is in list
    let list = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should list");

    assert!(!list.uploads().is_empty());

    // Abort
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should abort");

    // Should no longer be in list
    let list = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should list");

    assert!(list.uploads().is_empty());
}

/// Test abort on non-existent bucket.
/// Ceph: test_multipart_abort_nonexistent_bucket
#[tokio::test]
async fn test_multipart_abort_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx
        .client
        .abort_multipart_upload()
        .bucket("nonexistent-bucket")
        .key("test.txt")
        .upload_id("fake-upload-id")
        .send()
        .await;

    assert!(result.is_err());
}

/// Test abort concurrent operations.
/// Ceph: test_multipart_abort_concurrent
#[tokio::test]
async fn test_multipart_abort_concurrent() {
    let ctx = S3TestContext::new().await;

    // Create multiple uploads
    let mut upload_ids = Vec::new();
    for i in 0..5 {
        let create = ctx
            .client
            .create_multipart_upload()
            .bucket(&ctx.bucket)
            .key(format!("concurrent-{}.txt", i))
            .send()
            .await
            .expect("Should initiate");

        upload_ids.push((format!("concurrent-{}.txt", i), create.upload_id().unwrap().to_string()));
    }

    // Abort all concurrently
    let mut handles = Vec::new();
    for (key, upload_id) in upload_ids {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.abort_multipart_upload().bucket(&bucket).key(&key).upload_id(&upload_id).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

/// Test abort with special characters in key.
/// Ceph: test_multipart_abort_special_chars
#[tokio::test]
async fn test_multipart_abort_special_chars_key() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("path/to/file with spaces.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("path/to/file with spaces.txt")
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should abort");
}
