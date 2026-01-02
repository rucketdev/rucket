//! Multipart upload completion tests.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

use crate::S3TestContext;

/// Test complete multipart with wrong part order fails.
#[tokio::test]
async fn test_multipart_complete_wrong_order() {
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

    // Upload two parts
    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1"))
        .send()
        .await
        .expect("Should upload part 1");

    let part2 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from_static(b"part2"))
        .send()
        .await
        .expect("Should upload part 2");

    // Complete with wrong order (part 2 before part 1)
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(2).e_tag(part2.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err(), "Should fail with wrong part order");
}

/// Test complete multipart with invalid ETag fails.
#[tokio::test]
async fn test_multipart_complete_invalid_etag() {
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
    let _ = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .expect("Should upload");

    // Complete with wrong ETag
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag("\"invalid-etag\"").build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err(), "Should fail with invalid ETag");
}

/// Test complete multipart returns ETag.
#[tokio::test]
async fn test_multipart_complete_returns_etag() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    let response = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    assert!(response.e_tag().is_some());
    // Multipart ETags have format "etag-N" where N is part count
    let etag = response.e_tag().unwrap();
    assert!(etag.contains("-"), "Multipart ETag should contain dash");
}

/// Test complete multipart with missing part fails.
#[tokio::test]
async fn test_multipart_complete_missing_part() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    // Upload part 1
    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1"))
        .send()
        .await
        .unwrap();

    // Complete with parts 1 and 2, but we never uploaded part 2
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(2).e_tag("\"some-etag\"").build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err());
}

// =============================================================================
// Extended Complete Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test complete multipart returns location.
/// Ceph: test_multipart_complete_location
#[tokio::test]
async fn test_multipart_complete_returns_location() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    let response = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    assert!(response.location().is_some());
}

/// Test complete multipart returns bucket.
/// Ceph: test_multipart_complete_bucket
#[tokio::test]
async fn test_multipart_complete_returns_bucket() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    let response = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    assert_eq!(response.bucket(), Some(ctx.bucket.as_str()));
}

/// Test complete multipart returns key.
/// Ceph: test_multipart_complete_key
#[tokio::test]
async fn test_multipart_complete_returns_key() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("my/key.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("my/key.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    let response = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("my/key.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    assert_eq!(response.key(), Some("my/key.txt"));
}

/// Test complete with non-existent upload ID fails.
/// Ceph: test_multipart_complete_nonexistent_upload
#[tokio::test]
async fn test_multipart_complete_nonexistent_upload() {
    let ctx = S3TestContext::new().await;

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag("\"etag\"").build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id("nonexistent-upload-id")
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err());
}

/// Test complete multipart on versioned bucket returns version ID.
/// Ceph: test_multipart_complete_versioned
#[tokio::test]
async fn test_multipart_complete_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    let response = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    assert!(response.version_id().is_some());
}

/// Test complete multipart with empty parts list fails.
/// Ceph: test_multipart_complete_empty
#[tokio::test]
async fn test_multipart_complete_empty_parts() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    // Try to complete with no parts
    let completed = CompletedMultipartUpload::builder().build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err(), "Should fail with empty parts list");
}

/// Test complete multipart with duplicate part numbers fails.
/// Ceph: test_multipart_complete_duplicate_parts
#[tokio::test]
async fn test_multipart_complete_duplicate_parts() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    // Complete with duplicate part numbers
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_err(), "Should fail with duplicate part numbers");
}

/// Test complete multipart with gap in part numbers.
/// Ceph: test_multipart_complete_gap
#[tokio::test]
async fn test_multipart_complete_skipped_part_numbers() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    // Upload part 1 and part 3 (skip 2)
    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1"))
        .send()
        .await
        .unwrap();

    let part3 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(3)
        .body(ByteStream::from_static(b"part3"))
        .send()
        .await
        .unwrap();

    // Complete with only parts 1 and 3 (skipping 2 is allowed)
    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part1.e_tag().unwrap()).build())
        .parts(CompletedPart::builder().part_number(3).e_tag(part3.e_tag().unwrap()).build())
        .build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_ok(), "Should allow gaps in part numbers");
}

/// Test complete multipart object is accessible.
/// Ceph: test_multipart_complete_accessible
#[tokio::test]
async fn test_multipart_complete_object_accessible() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"test content"))
        .send()
        .await
        .unwrap();

    let completed = CompletedMultipartUpload::builder()
        .parts(CompletedPart::builder().part_number(1).e_tag(part.e_tag().unwrap()).build())
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

    // Verify object is accessible
    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"test content");
}

/// Test complete multipart with many parts.
/// Ceph: test_multipart_complete_many_parts
#[tokio::test]
async fn test_multipart_complete_many_parts() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();

    let upload_id = create.upload_id().unwrap();

    let mut parts = Vec::new();
    for i in 1..=10 {
        let part = ctx
            .client
            .upload_part()
            .bucket(&ctx.bucket)
            .key("test.txt")
            .upload_id(upload_id)
            .part_number(i)
            .body(ByteStream::from(format!("part{}", i).into_bytes()))
            .send()
            .await
            .unwrap();

        parts.push(CompletedPart::builder().part_number(i).e_tag(part.e_tag().unwrap()).build());
    }

    let mut builder = CompletedMultipartUpload::builder();
    for part in parts {
        builder = builder.parts(part);
    }
    let completed = builder.build();

    let result = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await;

    assert!(result.is_ok());

    // ETag should indicate multiple parts
    let etag = result.unwrap().e_tag().unwrap().to_string();
    assert!(etag.contains("-10"), "ETag should show 10 parts: {}", etag);
}
