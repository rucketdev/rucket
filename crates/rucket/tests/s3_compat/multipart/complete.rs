//! Multipart upload completion tests.

use crate::S3TestContext;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

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
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(part2.e_tag().unwrap())
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part1.e_tag().unwrap())
                .build(),
        )
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
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag("\"invalid-etag\"")
                .build(),
        )
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
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part.e_tag().unwrap())
                .build(),
        )
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
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part1.e_tag().unwrap())
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag("\"some-etag\"")
                .build(),
        )
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
