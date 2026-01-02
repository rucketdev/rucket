//! Object GET tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_get_*
//! - MinIO Mint: GetObject tests

use aws_sdk_s3::primitives::ByteStream;

use crate::{random_bytes, S3TestContext};

/// Test basic object GET.
/// Ceph: test_object_read
#[tokio::test]
async fn test_object_get_simple() {
    let ctx = S3TestContext::new().await;

    let content = b"Hello, World!";
    ctx.put("test.txt", content).await;

    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), content);
}

/// Test GET non-existent object returns 404.
/// Ceph: test_object_read_notexist
#[tokio::test]
async fn test_object_get_not_found() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.get_object().bucket(&ctx.bucket).key("nonexistent.txt").send().await;

    assert!(result.is_err(), "Should return error for non-existent object");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(error_str.contains("NoSuchKey"), "Should be NoSuchKey error, got: {}", error_str);
}

/// Test GET from non-existent bucket returns error.
#[tokio::test]
async fn test_object_get_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.get_object().bucket("nonexistent-bucket").key("test.txt").send().await;

    assert!(result.is_err(), "Should return error for non-existent bucket");
}

/// Test GET returns correct content type.
#[tokio::test]
async fn test_object_get_content_type() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"<html></html>");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("index.html")
        .body(body)
        .content_type("text/html")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.get_object("index.html").await;
    assert_eq!(response.content_type(), Some("text/html"));
}

/// Test GET returns correct content length.
#[tokio::test]
async fn test_object_get_content_length() {
    let ctx = S3TestContext::new().await;

    let content = b"Exactly 21 bytes.....";
    ctx.put("test.txt", content).await;

    let response = ctx.get_object("test.txt").await;
    assert_eq!(response.content_length(), Some(21));
}

/// Test GET returns ETag.
#[tokio::test]
async fn test_object_get_etag() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    assert!(response.e_tag().is_some(), "Should return ETag");
}

/// Test GET empty object.
#[tokio::test]
async fn test_object_get_empty() {
    let ctx = S3TestContext::new().await;

    ctx.put("empty.txt", b"").await;

    let data = ctx.get("empty.txt").await;
    assert!(data.is_empty(), "Empty object should return empty data");
}

/// Test GET large object.
#[tokio::test]
async fn test_object_get_large() {
    let ctx = S3TestContext::new().await;

    let content = random_bytes(1024 * 1024); // 1 MB
    ctx.put("large.bin", &content).await;

    let data = ctx.get("large.bin").await;
    assert_eq!(data.len(), 1024 * 1024);
    assert_eq!(data.as_slice(), content.as_slice());
}

/// Test GET returns user metadata.
#[tokio::test]
async fn test_object_get_user_metadata() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("x-custom", "value")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.get_object("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("x-custom"), Some(&"value".to_string()));
}

/// Test GET with If-Match succeeds.
#[tokio::test]
async fn test_object_get_if_match_success() {
    let ctx = S3TestContext::new().await;

    let put_response = ctx.put("test.txt", b"content").await;
    let etag = put_response.e_tag().unwrap().to_string();

    let result =
        ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").if_match(&etag).send().await;

    assert!(result.is_ok(), "GET with matching If-Match should succeed");
}

/// Test GET with If-Match fails on mismatch.
#[tokio::test]
async fn test_object_get_if_match_fails() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_match("\"wrong-etag\"")
        .send()
        .await;

    assert!(result.is_err(), "GET with wrong If-Match should fail");
}

/// Test GET with If-None-Match succeeds when no match.
#[tokio::test]
async fn test_object_get_if_none_match_success() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match("\"different-etag\"")
        .send()
        .await;

    assert!(result.is_ok(), "GET with non-matching If-None-Match should succeed");
}

/// Test GET with If-None-Match returns 304 when matching.
#[tokio::test]
async fn test_object_get_if_none_match_not_modified() {
    let ctx = S3TestContext::new().await;

    let put_response = ctx.put("test.txt", b"content").await;
    let etag = put_response.e_tag().unwrap().to_string();

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match(&etag)
        .send()
        .await;

    // Should return 304 Not Modified (which AWS SDK treats as an error)
    assert!(result.is_err(), "GET with matching If-None-Match should return 304");
}

/// Test GET after overwrite.
#[tokio::test]
async fn test_object_get_after_overwrite() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Version 1").await;
    ctx.put("test.txt", b"Version 2").await;

    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"Version 2");
}

/// Test GET binary content.
#[tokio::test]
async fn test_object_get_binary() {
    let ctx = S3TestContext::new().await;

    let content: Vec<u8> = (0..=255).collect();
    ctx.put("binary.bin", &content).await;

    let data = ctx.get("binary.bin").await;
    assert_eq!(data.as_slice(), content.as_slice());
}

/// Test concurrent GETs.
#[tokio::test]
async fn test_object_get_concurrent() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"shared content").await;

    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.get_object().bucket(&bucket).key("test.txt").send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent GET should succeed");
    }
}

/// Test GET with response content type override.
#[tokio::test]
async fn test_object_get_response_content_type() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_content_type("application/json")
        .send()
        .await
        .expect("Should get object");

    assert_eq!(response.content_type(), Some("application/json"));
}

/// Test GET with response content disposition override.
#[tokio::test]
async fn test_object_get_response_content_disposition() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_content_disposition("attachment; filename=\"download.txt\"")
        .send()
        .await
        .expect("Should get object");

    assert_eq!(response.content_disposition(), Some("attachment; filename=\"download.txt\""));
}

/// Test GET with deep path key.
#[tokio::test]
async fn test_object_get_deep_path() {
    let ctx = S3TestContext::new().await;

    let key = "a/b/c/d/e/file.txt";
    ctx.put(key, b"deep content").await;

    let data = ctx.get(key).await;
    assert_eq!(data.as_slice(), b"deep content");
}
