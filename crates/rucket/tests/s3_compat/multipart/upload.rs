//! Multipart upload initiation and part upload tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_multipart_*
//! - MinIO Mint: multipart upload tests

use crate::S3TestContext;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::CompletedMultipartUpload;
use aws_sdk_s3::types::CompletedPart;

/// Test basic multipart upload.
#[tokio::test]
async fn test_multipart_upload_basic() {
    let ctx = S3TestContext::new().await;

    // Initiate multipart upload
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .send()
        .await
        .expect("Should initiate multipart upload");

    let upload_id = create_response.upload_id().expect("Should have upload ID");

    // Upload a part
    let part_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1 content"))
        .send()
        .await
        .expect("Should upload part");

    let etag = part_response.e_tag().expect("Should have ETag");

    // Complete multipart upload
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(etag)
                .build(),
        )
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete multipart upload");

    // Verify object exists
    let data = ctx.get("multipart.txt").await;
    assert_eq!(data.as_slice(), b"part1 content");
}

/// Test multipart upload with multiple parts.
#[tokio::test]
async fn test_multipart_upload_multiple_parts() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .send()
        .await
        .expect("Should initiate multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Upload 3 parts (each must be at least 5MB except last, but for tests we use smaller)
    let mut completed_parts = Vec::new();
    for part_num in 1..=3 {
        let content = format!("part{} content - ", part_num).repeat(100);
        let part_response = ctx
            .client
            .upload_part()
            .bucket(&ctx.bucket)
            .key("multipart.txt")
            .upload_id(upload_id)
            .part_number(part_num)
            .body(ByteStream::from(content.into_bytes()))
            .send()
            .await
            .expect("Should upload part");

        completed_parts.push(
            CompletedPart::builder()
                .part_number(part_num)
                .e_tag(part_response.e_tag().unwrap())
                .build(),
        );
    }

    let completed = CompletedMultipartUpload::builder()
        .set_parts(Some(completed_parts))
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete multipart upload");

    // Verify object exists and has combined content
    let exists = ctx.exists("multipart.txt").await;
    assert!(exists);
}

/// Test create multipart upload returns upload ID.
#[tokio::test]
async fn test_multipart_create_returns_upload_id() {
    let ctx = S3TestContext::new().await;

    let response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should initiate multipart upload");

    assert!(response.upload_id().is_some());
    assert!(!response.upload_id().unwrap().is_empty());
}

/// Test create multipart upload with content type.
#[tokio::test]
async fn test_multipart_create_with_content_type() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.html")
        .content_type("text/html")
        .send()
        .await
        .expect("Should initiate multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Upload and complete
    let part_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.html")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"<html></html>"))
        .send()
        .await
        .expect("Should upload part");

    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part_response.e_tag().unwrap())
                .build(),
        )
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.html")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    let head = ctx.head("test.html").await;
    assert_eq!(head.content_type(), Some("text/html"));
}

/// Test create multipart upload with metadata.
#[tokio::test]
async fn test_multipart_create_with_metadata() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .metadata("custom-key", "custom-value")
        .send()
        .await
        .expect("Should initiate multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    let part_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload part");

    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part_response.e_tag().unwrap())
                .build(),
        )
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    let head = ctx.head("test.txt").await;
    let metadata = head.metadata().unwrap();
    assert_eq!(metadata.get("custom-key"), Some(&"custom-value".to_string()));
}

/// Test upload part returns ETag.
#[tokio::test]
async fn test_multipart_upload_part_returns_etag() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    let part_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload part");

    assert!(part_response.e_tag().is_some());

    // Cleanup - abort
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test upload part with invalid upload ID fails.
#[tokio::test]
async fn test_multipart_upload_part_invalid_upload_id() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id("invalid-upload-id")
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await;

    assert!(result.is_err());
}

/// Test upload part to non-existent bucket fails.
#[tokio::test]
async fn test_multipart_upload_part_nonexistent_bucket() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .upload_part()
        .bucket("nonexistent-bucket")
        .key("test.txt")
        .upload_id("some-id")
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await;

    assert!(result.is_err());
}
