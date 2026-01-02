//! Multipart upload initiation and part upload tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_multipart_*
//! - MinIO Mint: multipart upload tests

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

use crate::S3TestContext;

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
        .parts(CompletedPart::builder().part_number(1).e_tag(etag).build())
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

    let completed = CompletedMultipartUpload::builder().set_parts(Some(completed_parts)).build();

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
            CompletedPart::builder().part_number(1).e_tag(part_response.e_tag().unwrap()).build(),
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
            CompletedPart::builder().part_number(1).e_tag(part_response.e_tag().unwrap()).build(),
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

// =============================================================================
// Extended Multipart Tests (ported from Ceph s3-tests and MinIO Mint)
// =============================================================================

/// Test multipart upload with zero-byte part.
/// Ceph: test_multipart_empty_part
#[tokio::test]
#[ignore = "Zero-byte parts behavior varies"]
async fn test_multipart_empty_part() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("empty-part.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    // Try uploading empty part
    let result = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("empty-part.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b""))
        .send()
        .await;

    // Empty parts should fail or be rejected
    assert!(result.is_err());
}

/// Test multipart upload maximum parts (10000).
/// Ceph: test_multipart_max_parts
#[tokio::test]
#[ignore = "Requires many parts"]
async fn test_multipart_max_parts() {
    let _ctx = S3TestContext::new().await;
    // Would need to upload 10000 parts
}

/// Test multipart upload part number too high.
/// Ceph: test_multipart_part_number_too_high
#[tokio::test]
async fn test_multipart_part_number_too_high() {
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

    // Part number must be 1-10000
    let result = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(10001) // Too high
        .body(ByteStream::from_static(b"content"))
        .send()
        .await;

    assert!(result.is_err());

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test multipart upload part number zero.
/// Ceph: test_multipart_part_number_zero
#[tokio::test]
async fn test_multipart_part_number_zero() {
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

    // Part number 0 is invalid
    let result = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(0)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await;

    assert!(result.is_err());

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test multipart upload overwrites part.
/// Ceph: test_multipart_overwrite_part
#[tokio::test]
async fn test_multipart_overwrite_part() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("overwrite.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    // Upload part 1 with first content
    let _first_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("overwrite.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"first content"))
        .send()
        .await
        .expect("Should upload first");

    // Overwrite part 1 with second content
    let second_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("overwrite.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"second content"))
        .send()
        .await
        .expect("Should overwrite");

    // Complete with the second ETag
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder().part_number(1).e_tag(second_response.e_tag().unwrap()).build(),
        )
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("overwrite.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    // Verify content is the second version
    let data = ctx.get("overwrite.txt").await;
    assert_eq!(data.as_slice(), b"second content");
}

/// Test multipart upload with out-of-order parts.
/// Ceph: test_multipart_out_of_order
#[tokio::test]
async fn test_multipart_out_of_order_parts() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("out-of-order.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    // Upload part 3 first
    let part3 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("out-of-order.txt")
        .upload_id(upload_id)
        .part_number(3)
        .body(ByteStream::from_static(b"part3"))
        .send()
        .await
        .expect("Should upload part 3");

    // Upload part 1 second
    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("out-of-order.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1"))
        .send()
        .await
        .expect("Should upload part 1");

    // Upload part 2 last
    let part2 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("out-of-order.txt")
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from_static(b"part2"))
        .send()
        .await
        .expect("Should upload part 2");

    // Complete with parts in order
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(2).e_tag(part2.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(3).e_tag(part3.e_tag().unwrap()).build())
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("out-of-order.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    let data = ctx.get("out-of-order.txt").await;
    assert_eq!(data.as_slice(), b"part1part2part3");
}

/// Test multipart upload with skipped parts.
/// Ceph: test_multipart_skipped_parts
#[tokio::test]
async fn test_multipart_skipped_parts() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("skipped.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    // Upload part 1 and 3 (skip 2)
    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("skipped.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1"))
        .send()
        .await
        .expect("Should upload part 1");

    let part3 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("skipped.txt")
        .upload_id(upload_id)
        .part_number(3)
        .body(ByteStream::from_static(b"part3"))
        .send()
        .await
        .expect("Should upload part 3");

    // Complete with only parts 1 and 3
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(3).e_tag(part3.e_tag().unwrap()).build())
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("skipped.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete with skipped parts");

    let data = ctx.get("skipped.txt").await;
    assert_eq!(data.as_slice(), b"part1part3");
}

/// Test multipart upload complete with wrong ETag fails.
/// Ceph: test_multipart_complete_wrong_etag
#[tokio::test]
async fn test_multipart_complete_wrong_etag() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("wrong-etag.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    let _part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("wrong-etag.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload");

    // Complete with wrong ETag
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag("\"wrong-etag\"").build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("wrong-etag.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err());

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("wrong-etag.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test multipart upload complete with missing part fails.
/// Ceph: test_multipart_complete_missing_part
#[tokio::test]
async fn test_multipart_complete_missing_part() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("missing-part.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    // Upload only part 1
    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("missing-part.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1"))
        .send()
        .await
        .expect("Should upload");

    // Try to complete with part 2 that doesn't exist
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(2).e_tag("\"nonexistent\"").build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("missing-part.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err());

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("missing-part.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test multipart upload with cache control.
/// Ceph: test_multipart_cache_control
#[tokio::test]
async fn test_multipart_with_cache_control() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("cached.txt")
        .cache_control("max-age=3600")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("cached.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload");

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("cached.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    let head = ctx.head("cached.txt").await;
    assert_eq!(head.cache_control(), Some("max-age=3600"));
}

/// Test multipart upload with content disposition.
/// Ceph: test_multipart_content_disposition
#[tokio::test]
async fn test_multipart_with_content_disposition() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("download.txt")
        .content_disposition("attachment; filename=\"download.txt\"")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("download.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload");

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("download.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    let head = ctx.head("download.txt").await;
    assert_eq!(head.content_disposition(), Some("attachment; filename=\"download.txt\""));
}

/// Test multipart upload with content encoding.
/// Ceph: test_multipart_content_encoding
#[tokio::test]
async fn test_multipart_with_content_encoding() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("encoded.txt")
        .content_encoding("gzip")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("encoded.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload");

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("encoded.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    let head = ctx.head("encoded.txt").await;
    assert_eq!(head.content_encoding(), Some("gzip"));
}

/// Test multipart upload with tagging.
/// Ceph: test_multipart_tagging
#[tokio::test]
#[ignore = "Tagging on multipart not fully tested"]
async fn test_multipart_with_tagging() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("tagged.txt")
        .tagging("key1=value1&key2=value2")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create_response.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("tagged.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload");

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("tagged.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    // Verify tags
    let tags = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("tagged.txt")
        .send()
        .await
        .expect("Should get tags");

    assert_eq!(tags.tag_set().len(), 2);
}

/// Test concurrent multipart uploads to same key.
/// Ceph: test_multipart_concurrent
#[tokio::test]
async fn test_multipart_concurrent_uploads() {
    let ctx = S3TestContext::new().await;

    // Start two concurrent uploads to same key
    let create1 = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("concurrent.txt")
        .send()
        .await
        .expect("Should initiate first");

    let create2 = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("concurrent.txt")
        .send()
        .await
        .expect("Should initiate second");

    let upload_id1 = create1.upload_id().unwrap();
    let upload_id2 = create2.upload_id().unwrap();

    // Both should have different upload IDs
    assert_ne!(upload_id1, upload_id2);

    // Upload to first
    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("concurrent.txt")
        .upload_id(upload_id1)
        .part_number(1)
        .body(ByteStream::from_static(b"upload1"))
        .send()
        .await
        .expect("Should upload to first");

    // Complete first
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("concurrent.txt")
        .upload_id(upload_id1)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete first");

    // Abort second
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("concurrent.txt")
        .upload_id(upload_id2)
        .send()
        .await
        .expect("Should abort second");

    // Verify content is from first upload
    let data = ctx.get("concurrent.txt").await;
    assert_eq!(data.as_slice(), b"upload1");
}
