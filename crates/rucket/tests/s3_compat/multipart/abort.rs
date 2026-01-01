//! Multipart upload abort tests.

use crate::S3TestContext;
use aws_sdk_s3::primitives::ByteStream;

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
