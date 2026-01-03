//! SSE-S3 (Server-Side Encryption with S3-managed keys) integration tests.
//!
//! Tests for SSE-S3 encryption functionality including:
//! - Basic encrypt/decrypt (PUT/GET)
//! - Encryption headers in responses
//! - Multipart upload encryption
//! - Versioning with encryption
//! - Range requests on encrypted objects

use crate::harness::{random_bytes, random_key, S3TestContext};

// =============================================================================
// Basic SSE-S3 Tests
// =============================================================================

/// Test that objects are encrypted and decrypted correctly (basic roundtrip).
#[tokio::test]
async fn test_sse_s3_put_get_roundtrip() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();
    let data = b"Hello, encrypted world!";

    // PUT the object
    ctx.put(&key, data).await;

    // GET the object and verify content matches
    let retrieved = ctx.get(&key).await;
    assert_eq!(retrieved.as_slice(), data, "Decrypted content should match original");
}

/// Test SSE-S3 with larger data.
#[tokio::test]
async fn test_sse_s3_large_object() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();
    let data = random_bytes(1024 * 1024); // 1 MB

    ctx.put(&key, &data).await;
    let retrieved = ctx.get(&key).await;

    assert_eq!(retrieved.len(), data.len(), "Size should match");
    assert_eq!(retrieved.as_slice(), data.as_slice(), "Content should match");
}

/// Test SSE-S3 with empty object.
#[tokio::test]
async fn test_sse_s3_empty_object() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();
    let data = b"";

    ctx.put(&key, data).await;
    let retrieved = ctx.get(&key).await;

    assert!(retrieved.is_empty(), "Empty object should decrypt to empty");
}

/// Test HEAD returns encryption header.
#[tokio::test]
async fn test_sse_s3_head_shows_encryption() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();
    ctx.put(&key, b"encrypted data").await;

    let head = ctx.head(&key).await;

    // Server-side encryption should be indicated in the response
    assert!(head.server_side_encryption().is_some(), "HEAD should show server-side encryption");
}

/// Test GET returns encryption header.
#[tokio::test]
async fn test_sse_s3_get_shows_encryption() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();
    ctx.put(&key, b"encrypted data").await;

    let get_output = ctx.get_object(&key).await;

    // Server-side encryption should be indicated in the response
    assert!(
        get_output.server_side_encryption().is_some(),
        "GET should show server-side encryption"
    );
}

// =============================================================================
// SSE-S3 with Versioning Tests
// =============================================================================

/// Test SSE-S3 with versioning enabled.
#[tokio::test]
async fn test_sse_s3_versioning() {
    let ctx = S3TestContext::with_encryption().await;
    ctx.enable_versioning().await;

    let key = random_key();
    let data1 = b"version 1 content";
    let data2 = b"version 2 content";

    // Create two versions
    let put1 = ctx.put(&key, data1).await;
    let version1 = put1.version_id().expect("Should have version ID");

    let put2 = ctx.put(&key, data2).await;
    let version2 = put2.version_id().expect("Should have version ID");

    assert_ne!(version1, version2, "Versions should be different");

    // Retrieve specific versions and verify content
    let response1 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(&key)
        .version_id(version1)
        .send()
        .await
        .expect("Failed to get version 1");
    let content1 = response1.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(content1.as_slice(), data1);

    let response2 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(&key)
        .version_id(version2)
        .send()
        .await
        .expect("Failed to get version 2");
    let content2 = response2.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(content2.as_slice(), data2);
}

// =============================================================================
// SSE-S3 Multipart Upload Tests
// =============================================================================

/// Test SSE-S3 with multipart upload.
#[tokio::test]
async fn test_sse_s3_multipart() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();

    // Initiate multipart upload
    let create_output = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(&key)
        .send()
        .await
        .expect("Failed to create multipart upload");

    let upload_id = create_output.upload_id().expect("Should have upload ID");

    // Upload two parts (each 5MB minimum in real S3, but we can use smaller for tests)
    let part1_data = random_bytes(1024 * 1024); // 1 MB
    let part2_data = random_bytes(1024 * 1024); // 1 MB

    let part1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key(&key)
        .upload_id(upload_id)
        .part_number(1)
        .body(part1_data.clone().into())
        .send()
        .await
        .expect("Failed to upload part 1");

    let part2 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key(&key)
        .upload_id(upload_id)
        .part_number(2)
        .body(part2_data.clone().into())
        .send()
        .await
        .expect("Failed to upload part 2");

    // Complete the multipart upload
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let completed_parts = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(part1.e_tag().unwrap_or_default())
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(part2.e_tag().unwrap_or_default())
                .build(),
        )
        .build();

    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key(&key)
        .upload_id(upload_id)
        .multipart_upload(completed_parts)
        .send()
        .await
        .expect("Failed to complete multipart upload");

    // Verify the complete object
    let retrieved = ctx.get(&key).await;
    let mut expected = part1_data.clone();
    expected.extend_from_slice(&part2_data);

    assert_eq!(retrieved.len(), expected.len(), "Size should match");
    assert_eq!(retrieved.as_slice(), expected.as_slice(), "Content should match");
}

// =============================================================================
// SSE-S3 Range Request Tests
// =============================================================================

/// Test range request on encrypted object.
#[tokio::test]
async fn test_sse_s3_range_request() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();
    let data = b"0123456789ABCDEF0123456789ABCDEF";

    ctx.put(&key, data).await;

    // Request a specific range
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(&key)
        .range("bytes=5-14")
        .send()
        .await
        .expect("Failed to get range");

    let content = response.body.collect().await.unwrap().into_bytes().to_vec();
    assert_eq!(content.as_slice(), &data[5..15], "Range content should match");
}

// =============================================================================
// SSE-S3 Delete Tests
// =============================================================================

/// Test deleting encrypted object.
#[tokio::test]
async fn test_sse_s3_delete() {
    let ctx = S3TestContext::with_encryption().await;

    let key = random_key();
    ctx.put(&key, b"encrypted data to delete").await;

    // Verify object exists
    assert!(ctx.exists(&key).await, "Object should exist before delete");

    // Delete the object
    ctx.delete(&key).await;

    // Verify object is deleted
    assert!(!ctx.exists(&key).await, "Object should not exist after delete");
}

// =============================================================================
// SSE-S3 Copy Tests
// =============================================================================

/// Test copying encrypted object.
#[tokio::test]
async fn test_sse_s3_copy() {
    let ctx = S3TestContext::with_encryption().await;

    let source_key = random_key();
    let dest_key = random_key();
    let data = b"encrypted data to copy";

    ctx.put(&source_key, data).await;

    // Copy the object
    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key(&dest_key)
        .copy_source(format!("{}/{}", &ctx.bucket, &source_key))
        .send()
        .await
        .expect("Failed to copy object");

    // Verify both objects have correct content
    let source_content = ctx.get(&source_key).await;
    let dest_content = ctx.get(&dest_key).await;

    assert_eq!(source_content.as_slice(), data);
    assert_eq!(dest_content.as_slice(), data);
}

// =============================================================================
// SSE-S3 List Tests
// =============================================================================

/// Test listing encrypted bucket.
#[tokio::test]
async fn test_sse_s3_list() {
    let ctx = S3TestContext::with_encryption().await;

    let key1 = format!("{}/file1", random_key());
    let key2 = format!("{}/file2", random_key());

    ctx.put(&key1, b"content1").await;
    ctx.put(&key2, b"content2").await;

    let list = ctx.list_objects().await;
    let contents = list.contents();

    assert!(contents.len() >= 2, "Should list at least 2 objects");
}
