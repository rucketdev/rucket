//! SSE-C (customer-provided key) encryption tests.

use aws_sdk_s3::primitives::ByteStream;
use base64::Engine;
use md5::{Digest, Md5};

// Allow unused for tests that are ignored
#[allow(unused_imports)]
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

/// Test SSE-C copy.
/// Requires SSE-C support in copy_object handler.
#[tokio::test]
#[ignore = "SSE-C copy not implemented"]
async fn test_sse_c_copy() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C copy requires source key.
#[tokio::test]
#[ignore = "SSE-C copy not implemented"]
async fn test_sse_c_copy_source_key() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C copy with wrong source key.
#[tokio::test]
#[ignore = "SSE-C copy not implemented"]
async fn test_sse_c_copy_wrong_source_key() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C copy to new key.
#[tokio::test]
#[ignore = "SSE-C copy not implemented"]
async fn test_sse_c_copy_new_key() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C with range request.
#[tokio::test]
#[ignore = "SSE-C range not implemented"]
async fn test_sse_c_range() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C multipart.
#[tokio::test]
#[ignore = "SSE-C multipart not implemented"]
async fn test_sse_c_multipart() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C multipart upload different keys per part fails.
#[tokio::test]
#[ignore = "SSE-C multipart not implemented"]
async fn test_sse_c_multipart_different_keys() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C presigned URL.
#[tokio::test]
#[ignore = "SSE-C presigned not implemented"]
async fn test_sse_c_presigned() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C with versioning.
#[tokio::test]
#[ignore = "SSE-C versioning not fully tested"]
async fn test_sse_c_versioning() {
    let _ctx = S3TestContext::new().await;
}

/// Test SSE-C upload part copy.
#[tokio::test]
#[ignore = "SSE-C multipart not implemented"]
async fn test_sse_c_upload_part_copy() {
    let _ctx = S3TestContext::new().await;
}
