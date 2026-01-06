//! SSE-C (customer-provided key) encryption tests.

use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use md5::{Digest, Md5};

use crate::S3TestContext;

/// Generate a 256-bit (32-byte) customer key for SSE-C.
fn generate_sse_c_key() -> [u8; 32] {
    // Use a fixed key for test reproducibility
    [
        0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
        0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d,
        0x1e, 0x1f,
    ]
}

/// Generate a different key for testing wrong key scenarios.
fn generate_wrong_key() -> [u8; 32] {
    [
        0xff, 0xfe, 0xfd, 0xfc, 0xfb, 0xfa, 0xf9, 0xf8, 0xf7, 0xf6, 0xf5, 0xf4, 0xf3, 0xf2, 0xf1,
        0xf0, 0xef, 0xee, 0xed, 0xec, 0xeb, 0xea, 0xe9, 0xe8, 0xe7, 0xe6, 0xe5, 0xe4, 0xe3, 0xe2,
        0xe1, 0xe0,
    ]
}

/// Encode a key as base64 for the SSE-C header.
fn key_to_base64(key: &[u8; 32]) -> String {
    base64::engine::general_purpose::STANDARD.encode(key)
}

/// Compute the MD5 hash of a key and encode as base64.
fn key_to_md5_base64(key: &[u8; 32]) -> String {
    let mut hasher = Md5::new();
    hasher.update(key);
    let md5 = hasher.finalize();
    base64::engine::general_purpose::STANDARD.encode(md5)
}

/// Test SSE-C PUT and GET roundtrip.
#[tokio::test]
async fn test_sse_c_put_get_roundtrip() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    let content = b"Hello, SSE-C encrypted world!";

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c.txt")
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT with SSE-C should succeed");

    // GET with SSE-C
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET with SSE-C should succeed");

    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), content, "Decrypted content should match");
}

/// Test SSE-C GET requires key.
#[tokio::test]
async fn test_sse_c_get_requires_key() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-required.txt")
        .body(ByteStream::from(b"encrypted content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT should succeed");

    // GET without SSE-C headers should fail
    let result = ctx.client.get_object().bucket(&ctx.bucket).key("sse-c-required.txt").send().await;

    assert!(result.is_err(), "GET without SSE-C headers should fail");
}

/// Test SSE-C wrong key fails.
#[tokio::test]
async fn test_sse_c_wrong_key() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-wrong.txt")
        .body(ByteStream::from(b"encrypted content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT should succeed");

    // GET with wrong key should fail
    let wrong_key = generate_wrong_key();
    let wrong_key_base64 = key_to_base64(&wrong_key);
    let wrong_key_md5 = key_to_md5_base64(&wrong_key);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-wrong.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&wrong_key_base64)
        .sse_customer_key_md5(&wrong_key_md5)
        .send()
        .await;

    assert!(result.is_err(), "GET with wrong key should fail");
}

/// Test SSE-C HEAD returns encryption headers.
#[tokio::test]
async fn test_sse_c_head() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-head.txt")
        .body(ByteStream::from(b"encrypted content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT should succeed");

    // HEAD with SSE-C headers
    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("sse-c-head.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("HEAD should succeed");

    // Check response has SSE-C headers
    assert_eq!(response.sse_customer_algorithm(), Some("AES256"), "Should return SSE-C algorithm");
    assert!(response.sse_customer_key_md5().is_some(), "Should return SSE-C key MD5");
}

/// Test SSE-C HEAD without key fails.
#[tokio::test]
async fn test_sse_c_head_requires_key() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-head-required.txt")
        .body(ByteStream::from(b"encrypted content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT should succeed");

    // HEAD without SSE-C headers should fail
    let result =
        ctx.client.head_object().bucket(&ctx.bucket).key("sse-c-head-required.txt").send().await;

    assert!(result.is_err(), "HEAD without SSE-C headers should fail");
}

/// Test SSE-C missing key header.
#[tokio::test]
async fn test_sse_c_missing_key() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_md5 = key_to_md5_base64(&key);

    // Try PUT with missing key (only algorithm and MD5)
    // The SDK may not allow this, so we use the raw client
    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-missing.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .sse_customer_algorithm("AES256")
        // Intentionally not providing sse_customer_key
        .sse_customer_key_md5(&key_md5)
        .send()
        .await;

    assert!(result.is_err(), "PUT without SSE-C key should fail");
}

/// Test SSE-C invalid algorithm.
#[tokio::test]
async fn test_sse_c_invalid_algorithm() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // Try PUT with invalid algorithm
    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-invalid-alg.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .sse_customer_algorithm("DES") // Invalid - only AES256 is supported
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await;

    assert!(result.is_err(), "PUT with invalid algorithm should fail");
}

/// Test SSE-C key size validation.
#[tokio::test]
async fn test_sse_c_key_size() {
    let ctx = S3TestContext::new().await;

    // Use a key that's too short (16 bytes instead of 32)
    let short_key = [0u8; 16];
    let key_base64 = base64::engine::general_purpose::STANDARD.encode(short_key);

    let mut hasher = Md5::new();
    hasher.update(short_key);
    let key_md5 = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-short-key.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await;

    assert!(result.is_err(), "PUT with wrong key size should fail");
}

/// Test SSE-C key MD5 mismatch.
#[tokio::test]
async fn test_sse_c_key_md5_mismatch() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    // Use wrong MD5
    let wrong_md5 = base64::engine::general_purpose::STANDARD.encode([0u8; 16]);

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-md5-mismatch.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&wrong_md5)
        .send()
        .await;

    assert!(result.is_err(), "PUT with MD5 mismatch should fail");
}

/// Test SSE-C delete encrypted object.
#[tokio::test]
async fn test_sse_c_delete() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-delete.txt")
        .body(ByteStream::from(b"encrypted content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT should succeed");

    // DELETE does not require SSE-C headers
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("sse-c-delete.txt")
        .send()
        .await
        .expect("DELETE should succeed without SSE-C headers");

    // Verify object is deleted
    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("sse-c-delete.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await;

    assert!(result.is_err(), "Object should be deleted");
}

/// Test SSE-C content size is correct.
#[tokio::test]
async fn test_sse_c_content_size() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    let content = b"Test content with specific size";
    let content_len = content.len() as i64;

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-size.txt")
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT should succeed");

    // HEAD to check content length
    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("sse-c-size.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("HEAD should succeed");

    assert_eq!(
        response.content_length(),
        Some(content_len),
        "Content-Length should match original plaintext size"
    );
}

// =============================================================================
// Tests that require additional features not yet implemented
// =============================================================================

/// Test SSE-C copy - copy encrypted object with same key.
#[tokio::test]
async fn test_sse_c_copy() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    let content = b"Content to copy with SSE-C";

    // PUT source object with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-source.txt")
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT source with SSE-C should succeed");

    // Copy with same key (source and dest both use same key)
    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("sse-c-dest.txt")
        .copy_source(format!("{}/sse-c-source.txt", ctx.bucket))
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&key_base64)
        .copy_source_sse_customer_key_md5(&key_md5)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("Copy with SSE-C should succeed");

    // Verify we can read the copied object
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-dest.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET copied object should succeed");

    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), content, "Copied content should match");
}

/// Test SSE-C copy requires source key when source is encrypted.
#[tokio::test]
async fn test_sse_c_copy_source_key() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT source object with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-requires-key.txt")
        .body(ByteStream::from(b"encrypted content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT source with SSE-C should succeed");

    // Try to copy without providing source SSE-C headers - should fail
    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("sse-c-dest-no-key.txt")
        .copy_source(format!("{}/sse-c-requires-key.txt", ctx.bucket))
        .send()
        .await;

    assert!(result.is_err(), "Copy without source SSE-C headers should fail");
}

/// Test SSE-C copy with wrong source key fails.
#[tokio::test]
async fn test_sse_c_copy_wrong_source_key() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    let wrong_key = generate_wrong_key();
    let wrong_key_base64 = key_to_base64(&wrong_key);
    let wrong_key_md5 = key_to_md5_base64(&wrong_key);

    // PUT source object with correct key
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-wrong-key.txt")
        .body(ByteStream::from(b"encrypted content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT source with SSE-C should succeed");

    // Try to copy with wrong source key - should fail
    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("sse-c-dest-wrong-key.txt")
        .copy_source(format!("{}/sse-c-wrong-key.txt", ctx.bucket))
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&wrong_key_base64)
        .copy_source_sse_customer_key_md5(&wrong_key_md5)
        .send()
        .await;

    assert!(result.is_err(), "Copy with wrong source key should fail");
}

/// Test SSE-C copy to new encryption key.
#[tokio::test]
async fn test_sse_c_copy_new_key() {
    let ctx = S3TestContext::new().await;

    let source_key = generate_sse_c_key();
    let source_key_base64 = key_to_base64(&source_key);
    let source_key_md5 = key_to_md5_base64(&source_key);

    // Generate a different destination key
    let dest_key = generate_wrong_key(); // Reuse as a different key
    let dest_key_base64 = key_to_base64(&dest_key);
    let dest_key_md5 = key_to_md5_base64(&dest_key);

    let content = b"Content to re-encrypt";

    // PUT source object with source key
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-rekey-source.txt")
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&source_key_base64)
        .sse_customer_key_md5(&source_key_md5)
        .send()
        .await
        .expect("PUT source with SSE-C should succeed");

    // Copy with different destination key (re-encrypt)
    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("sse-c-rekey-dest.txt")
        .copy_source(format!("{}/sse-c-rekey-source.txt", ctx.bucket))
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&source_key_base64)
        .copy_source_sse_customer_key_md5(&source_key_md5)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&dest_key_base64)
        .sse_customer_key_md5(&dest_key_md5)
        .send()
        .await
        .expect("Copy with different SSE-C key should succeed");

    // Verify we can read the copied object with the NEW key
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-rekey-dest.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&dest_key_base64)
        .sse_customer_key_md5(&dest_key_md5)
        .send()
        .await
        .expect("GET with new key should succeed");

    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), content, "Content should match after re-encryption");

    // Verify old key doesn't work on the destination
    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-rekey-dest.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&source_key_base64)
        .sse_customer_key_md5(&source_key_md5)
        .send()
        .await;

    assert!(result.is_err(), "GET with old key should fail on re-encrypted object");
}

/// Test SSE-C with range request.
#[tokio::test]
async fn test_sse_c_range() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // Content with known pattern for range verification
    let content = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";

    // PUT with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-range.txt")
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT with SSE-C should succeed");

    // GET with range and SSE-C (bytes 10-19 = "ABCDEFGHIJ")
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-range.txt")
        .range("bytes=10-19")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET with range and SSE-C should succeed");

    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), b"ABCDEFGHIJ", "Range should return correct bytes");

    // GET with suffix range (last 5 bytes = "VWXYZ")
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-range.txt")
        .range("bytes=-5")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET with suffix range and SSE-C should succeed");

    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), b"VWXYZ", "Suffix range should return last 5 bytes");
}

/// Test SSE-C multipart.
#[tokio::test]
async fn test_sse_c_multipart() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // Create multipart upload with SSE-C
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("CreateMultipartUpload should succeed");
    let upload_id = create_response.upload_id().expect("Should have upload ID");

    // Upload parts with SSE-C
    let part1_content = b"This is part 1 of the multipart SSE-C upload. ";
    let part1_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(part1_content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("UploadPart 1 should succeed");
    let etag1 = part1_response.e_tag().expect("Should have ETag");

    let part2_content = b"This is part 2 of the multipart SSE-C upload.";
    let part2_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart.txt")
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from(part2_content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("UploadPart 2 should succeed");
    let etag2 = part2_response.e_tag().expect("Should have ETag");

    // Complete multipart upload with SSE-C
    let complete_response = ctx
        .client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart.txt")
        .upload_id(upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder().part_number(1).e_tag(etag1).build(),
                )
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder().part_number(2).e_tag(etag2).build(),
                )
                .build(),
        )
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("CompleteMultipartUpload should succeed");

    // Verify the object was created
    assert!(complete_response.e_tag().is_some(), "Should have ETag");

    // GET the object with SSE-C
    let get_response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET should succeed");

    let data = get_response.body.collect().await.unwrap().into_bytes();
    let expected: Vec<u8> = [part1_content.as_slice(), part2_content.as_slice()].concat();
    assert_eq!(data.as_ref(), expected.as_slice(), "Content should match concatenated parts");
}

/// Test SSE-C multipart upload different keys per part fails.
#[tokio::test]
async fn test_sse_c_multipart_different_keys() {
    let ctx = S3TestContext::new().await;

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    let wrong_key = generate_wrong_key();
    let wrong_key_base64 = key_to_base64(&wrong_key);
    let wrong_key_md5 = key_to_md5_base64(&wrong_key);

    // Create multipart upload with SSE-C
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart-wrongkey.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("CreateMultipartUpload should succeed");
    let upload_id = create_response.upload_id().expect("Should have upload ID");

    // Upload part 1 with correct key
    let part1_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart-wrongkey.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(b"Part 1 content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await;
    assert!(part1_response.is_ok(), "UploadPart 1 with correct key should succeed");

    // Upload part 2 with wrong key - should fail
    let part2_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart-wrongkey.txt")
        .upload_id(upload_id)
        .part_number(2)
        .body(ByteStream::from(b"Part 2 content".to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&wrong_key_base64)
        .sse_customer_key_md5(&wrong_key_md5)
        .send()
        .await;
    assert!(part2_response.is_err(), "UploadPart 2 with wrong key should fail");

    // Clean up - abort the upload
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("sse-c-multipart-wrongkey.txt")
        .upload_id(upload_id)
        .send()
        .await
        .expect("AbortMultipartUpload should succeed");
}

/// Test SSE-C with presigned URLs.
/// This test verifies that SSE-C encryption works with presigned PUT and GET URLs.
#[tokio::test]
async fn test_sse_c_presigned() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "sse-c-presigned-object.txt";
    let content = b"SSE-C presigned content - secret data!";

    let sse_key = generate_sse_c_key();
    let key_base64 = key_to_base64(&sse_key);
    let key_md5 = key_to_md5_base64(&sse_key);

    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    // Generate a presigned PUT URL with SSE-C headers
    let presigned_put = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .presigned(presigning_config.clone())
        .await
        .expect("Failed to generate presigned PUT URL");

    // Use reqwest to upload the object with SSE-C headers
    let client = reqwest::Client::new();
    let response = client
        .put(presigned_put.uri())
        .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
        .header("x-amz-server-side-encryption-customer-key", &key_base64)
        .header("x-amz-server-side-encryption-customer-key-MD5", &key_md5)
        .body(content.to_vec())
        .send()
        .await
        .expect("Failed to make presigned PUT request");

    assert!(
        response.status().is_success(),
        "PUT should succeed, got {} - {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    // Generate a presigned GET URL with SSE-C headers
    let presigned_get = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .presigned(presigning_config.clone())
        .await
        .expect("Failed to generate presigned GET URL");

    // Use reqwest to download the object with SSE-C headers
    let response = client
        .get(presigned_get.uri())
        .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
        .header("x-amz-server-side-encryption-customer-key", &key_base64)
        .header("x-amz-server-side-encryption-customer-key-MD5", &key_md5)
        .send()
        .await
        .expect("Failed to make presigned GET request");

    assert!(
        response.status().is_success(),
        "GET should succeed, got {} - {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    let body = response.bytes().await.expect("Failed to read body");
    assert_eq!(body.as_ref(), content, "Decrypted content should match original");
}

/// Test SSE-C presigned URL fails with wrong key.
#[tokio::test]
async fn test_sse_c_presigned_wrong_key() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "sse-c-presigned-wrong-key.txt";
    let content = b"SSE-C presigned wrong key test";

    let correct_key = generate_sse_c_key();
    let correct_key_base64 = key_to_base64(&correct_key);
    let correct_key_md5 = key_to_md5_base64(&correct_key);

    let wrong_key = generate_wrong_key();
    let wrong_key_base64 = key_to_base64(&wrong_key);
    let wrong_key_md5 = key_to_md5_base64(&wrong_key);

    // Upload with correct key using SDK
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&correct_key_base64)
        .sse_customer_key_md5(&correct_key_md5)
        .send()
        .await
        .expect("PUT should succeed");

    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    // Generate a presigned GET URL with wrong key
    let presigned_get = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&wrong_key_base64)
        .sse_customer_key_md5(&wrong_key_md5)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned GET URL");

    // Try to download with the wrong key headers
    let client = reqwest::Client::new();
    let response = client
        .get(presigned_get.uri())
        .header("x-amz-server-side-encryption-customer-algorithm", "AES256")
        .header("x-amz-server-side-encryption-customer-key", &wrong_key_base64)
        .header("x-amz-server-side-encryption-customer-key-MD5", &wrong_key_md5)
        .send()
        .await
        .expect("Failed to make presigned GET request");

    // Should fail with 403 because the key doesn't match
    assert!(
        response.status().as_u16() == 403 || response.status().as_u16() == 400,
        "GET with wrong key should fail with 400 or 403, got {}",
        response.status()
    );
}

/// Test SSE-C with versioning - upload multiple versions and retrieve them.
#[tokio::test]
async fn test_sse_c_versioning() {
    let ctx = S3TestContext::new().await;

    // Enable versioning on the bucket
    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Enable versioning should succeed");

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT version 1 with SSE-C
    let v1_content = b"Version 1 content with SSE-C";
    let v1_response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-versioned.txt")
        .body(ByteStream::from(v1_content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT v1 should succeed");
    let v1_id = v1_response.version_id().expect("Should have version ID");

    // PUT version 2 with same SSE-C key
    let v2_content = b"Version 2 - updated content with SSE-C";
    let v2_response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("sse-c-versioned.txt")
        .body(ByteStream::from(v2_content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT v2 should succeed");
    let v2_id = v2_response.version_id().expect("Should have version ID");

    // GET latest version (v2) with SSE-C
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-versioned.txt")
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET latest should succeed");
    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), v2_content, "Latest version should be v2");

    // GET specific version (v1) with SSE-C
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-versioned.txt")
        .version_id(v1_id)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET v1 should succeed");
    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), v1_content, "Version 1 content should match");

    // GET specific version (v2) with SSE-C
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("sse-c-versioned.txt")
        .version_id(v2_id)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("GET v2 should succeed");
    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), v2_content, "Version 2 content should match");

    // HEAD specific version with SSE-C
    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("sse-c-versioned.txt")
        .version_id(v1_id)
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("HEAD v1 should succeed");
    assert_eq!(response.sse_customer_algorithm(), Some("AES256"), "Should return SSE-C algorithm");
}

/// Test SSE-C upload part copy.
/// UploadPartCopy with SSE-C source requires decrypting the source object
/// with x-amz-copy-source-server-side-encryption-customer-* headers.
#[tokio::test]
async fn test_sse_c_upload_part_copy() {
    let ctx = S3TestContext::new().await;
    let src_key = "sse-c-source-for-upload-part-copy.txt";
    let dst_key = "sse-c-dest-multipart-copy.txt";

    // Content large enough for a single part
    let content =
        b"SSE-C source content for UploadPartCopy test - needs to be decrypted from source!";

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT source object with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(src_key)
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT source object should succeed");

    // Create multipart upload for destination (non-encrypted for simplicity)
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .send()
        .await
        .expect("CreateMultipartUpload should succeed");

    let upload_id = create_response.upload_id().expect("Should have upload ID");

    // UploadPartCopy from SSE-C encrypted source
    let copy_response = ctx
        .client
        .upload_part_copy()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/{}", ctx.bucket, src_key))
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&key_base64)
        .copy_source_sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("UploadPartCopy should succeed");

    let etag = copy_response
        .copy_part_result()
        .expect("Should have copy result")
        .e_tag()
        .expect("Should have ETag")
        .to_string();

    // Complete multipart upload
    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .upload_id(upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder().part_number(1).e_tag(etag).build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("CompleteMultipartUpload should succeed");

    // GET the destination object (not encrypted) and verify content
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .send()
        .await
        .expect("GET should succeed");

    let data = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(data.as_ref(), content, "Content should match original");
}

/// Test SSE-C upload part copy fails without source SSE-C headers.
#[tokio::test]
async fn test_sse_c_upload_part_copy_missing_source_headers() {
    let ctx = S3TestContext::new().await;
    let src_key = "sse-c-source-missing-headers.txt";
    let dst_key = "sse-c-dest-missing-headers.txt";
    let content = b"SSE-C source content - headers required for copy!";

    let key = generate_sse_c_key();
    let key_base64 = key_to_base64(&key);
    let key_md5 = key_to_md5_base64(&key);

    // PUT source object with SSE-C
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(src_key)
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&key_base64)
        .sse_customer_key_md5(&key_md5)
        .send()
        .await
        .expect("PUT source object should succeed");

    // Create multipart upload
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .send()
        .await
        .expect("CreateMultipartUpload should succeed");

    let upload_id = create_response.upload_id().expect("Should have upload ID");

    // UploadPartCopy WITHOUT source SSE-C headers should fail
    let result = ctx
        .client
        .upload_part_copy()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/{}", ctx.bucket, src_key))
        // No copy_source_sse_customer_* headers
        .send()
        .await;

    assert!(result.is_err(), "UploadPartCopy without SSE-C headers should fail");

    // Cleanup: abort the multipart upload
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("AbortMultipartUpload should succeed");
}

/// Test SSE-C upload part copy fails with wrong source key.
#[tokio::test]
async fn test_sse_c_upload_part_copy_wrong_source_key() {
    let ctx = S3TestContext::new().await;
    let src_key = "sse-c-source-wrong-key.txt";
    let dst_key = "sse-c-dest-wrong-key.txt";
    let content = b"SSE-C source content - wrong key test!";

    let correct_key = generate_sse_c_key();
    let correct_key_base64 = key_to_base64(&correct_key);
    let correct_key_md5 = key_to_md5_base64(&correct_key);

    let wrong_key = generate_wrong_key();
    let wrong_key_base64 = key_to_base64(&wrong_key);
    let wrong_key_md5 = key_to_md5_base64(&wrong_key);

    // PUT source object with correct key
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(src_key)
        .body(ByteStream::from(content.to_vec()))
        .sse_customer_algorithm("AES256")
        .sse_customer_key(&correct_key_base64)
        .sse_customer_key_md5(&correct_key_md5)
        .send()
        .await
        .expect("PUT source object should succeed");

    // Create multipart upload
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .send()
        .await
        .expect("CreateMultipartUpload should succeed");

    let upload_id = create_response.upload_id().expect("Should have upload ID");

    // UploadPartCopy with wrong source SSE-C key should fail
    let result = ctx
        .client
        .upload_part_copy()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .upload_id(upload_id)
        .part_number(1)
        .copy_source(format!("{}/{}", ctx.bucket, src_key))
        .copy_source_sse_customer_algorithm("AES256")
        .copy_source_sse_customer_key(&wrong_key_base64)
        .copy_source_sse_customer_key_md5(&wrong_key_md5)
        .send()
        .await;

    assert!(result.is_err(), "UploadPartCopy with wrong SSE-C key should fail");

    // Cleanup: abort the multipart upload
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("AbortMultipartUpload should succeed");
}
