//! Object HEAD tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_head_*
//! - MinIO Mint: HeadObject tests

use aws_sdk_s3::primitives::ByteStream;

use crate::S3TestContext;

/// Test basic object HEAD.
/// Ceph: test_object_head
#[tokio::test]
async fn test_object_head_simple() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello, World!").await;

    let response = ctx.head("test.txt").await;

    assert_eq!(response.content_length(), Some(13));
    assert!(response.e_tag().is_some());
}

/// Test HEAD non-existent object returns 404.
/// Ceph: test_object_head_notexist
#[tokio::test]
async fn test_object_head_not_found() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.head_object().bucket(&ctx.bucket).key("nonexistent.txt").send().await;

    assert!(result.is_err(), "Should return error for non-existent object");
}

/// Test HEAD returns content type.
#[tokio::test]
async fn test_object_head_content_type() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.html")
        .body(body)
        .content_type("text/html")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("test.html").await;
    assert_eq!(response.content_type(), Some("text/html"));
}

/// Test HEAD returns cache control.
#[tokio::test]
async fn test_object_head_cache_control() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("cached.txt")
        .body(body)
        .cache_control("max-age=3600")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("cached.txt").await;
    assert_eq!(response.cache_control(), Some("max-age=3600"));
}

/// Test HEAD returns content encoding.
#[tokio::test]
async fn test_object_head_content_encoding() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("encoded.gz")
        .body(body)
        .content_encoding("gzip")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("encoded.gz").await;
    assert_eq!(response.content_encoding(), Some("gzip"));
}

/// Test HEAD returns content disposition.
#[tokio::test]
async fn test_object_head_content_disposition() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("download.txt")
        .body(body)
        .content_disposition("attachment")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("download.txt").await;
    assert_eq!(response.content_disposition(), Some("attachment"));
}

/// Test HEAD returns content language.
#[tokio::test]
async fn test_object_head_content_language() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("french.txt")
        .body(body)
        .content_language("fr")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("french.txt").await;
    assert_eq!(response.content_language(), Some("fr"));
}

/// Test HEAD returns user metadata.
#[tokio::test]
async fn test_object_head_user_metadata() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("custom-key", "custom-value")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("custom-key"), Some(&"custom-value".to_string()));
}

/// Test HEAD empty object.
#[tokio::test]
async fn test_object_head_empty() {
    let ctx = S3TestContext::new().await;

    ctx.put("empty.txt", b"").await;

    let response = ctx.head("empty.txt").await;
    assert_eq!(response.content_length(), Some(0));
}

/// Test HEAD with If-Match succeeds.
#[tokio::test]
async fn test_object_head_if_match_success() {
    let ctx = S3TestContext::new().await;

    let put_response = ctx.put("test.txt", b"content").await;
    let etag = put_response.e_tag().unwrap().to_string();

    let result =
        ctx.client.head_object().bucket(&ctx.bucket).key("test.txt").if_match(&etag).send().await;

    assert!(result.is_ok(), "HEAD with matching If-Match should succeed");
}

/// Test HEAD with If-Match fails on mismatch.
#[tokio::test]
async fn test_object_head_if_match_fails() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_match("\"wrong-etag\"")
        .send()
        .await;

    assert!(result.is_err(), "HEAD with wrong If-Match should fail");
}

/// Test HEAD returns last modified.
#[tokio::test]
async fn test_object_head_last_modified() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.head("test.txt").await;
    assert!(response.last_modified().is_some(), "Should return last modified");
}

/// Test HEAD after overwrite shows new metadata.
#[tokio::test]
async fn test_object_head_after_overwrite() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Version 1").await;
    let head1 = ctx.head("test.txt").await;
    let etag1 = head1.e_tag().unwrap().to_string();

    ctx.put("test.txt", b"Version 2 longer").await;
    let head2 = ctx.head("test.txt").await;
    let etag2 = head2.e_tag().unwrap().to_string();

    assert_ne!(etag1, etag2, "ETag should change after overwrite");
    assert_eq!(head2.content_length(), Some(16), "Length should reflect new content");
}

/// Test concurrent HEADs.
#[tokio::test]
async fn test_object_head_concurrent() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.head_object().bucket(&bucket).key("test.txt").send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent HEAD should succeed");
    }
}
