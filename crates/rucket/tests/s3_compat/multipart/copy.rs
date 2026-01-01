//! Multipart copy tests.

use crate::S3TestContext;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};

/// Test upload part copy.
#[tokio::test]
async fn test_multipart_upload_part_copy() {
    let ctx = S3TestContext::new().await;

    // Create source object
    ctx.put("source.txt", b"source content for copy").await;

    // Initiate multipart upload
    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    // Upload part by copying from source
    let copy_response = ctx
        .client
        .upload_part_copy()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy part");

    let etag = copy_response
        .copy_part_result()
        .unwrap()
        .e_tag()
        .unwrap();

    // Complete
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
        .key("dest.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    // Verify
    let data = ctx.get("dest.txt").await;
    assert_eq!(data.as_slice(), b"source content for copy");
}

/// Test upload part copy with range.
#[tokio::test]
async fn test_multipart_upload_part_copy_range() {
    let ctx = S3TestContext::new().await;

    // Create source object
    ctx.put("source.txt", b"0123456789abcdef").await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    // Copy only bytes 5-9
    let copy_response = ctx
        .client
        .upload_part_copy()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .copy_source_range("bytes=5-9")
        .send()
        .await
        .expect("Should copy part range");

    let etag = copy_response
        .copy_part_result()
        .unwrap()
        .e_tag()
        .unwrap();

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
        .key("dest.txt")
        .upload_id(upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .expect("Should complete");

    let data = ctx.get("dest.txt").await;
    assert_eq!(data.as_slice(), b"56789");
}

/// Test upload part copy from non-existent source fails.
#[tokio::test]
async fn test_multipart_upload_part_copy_nonexistent_source() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .send()
        .await
        .expect("Should initiate");

    let upload_id = create.upload_id().unwrap();

    let result = ctx
        .client
        .upload_part_copy()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/nonexistent.txt", ctx.bucket))
        .send()
        .await;

    assert!(result.is_err());

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .upload_id(upload_id)
        .send()
        .await;
}
