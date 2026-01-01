//! If-Match conditional tests.

use crate::S3TestContext;
use aws_sdk_s3::primitives::ByteStream;

/// Test If-Match with matching ETag succeeds.
#[tokio::test]
async fn test_if_match_success() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"new content"))
        .if_match(etag)
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-Match with non-matching ETag fails.
#[tokio::test]
async fn test_if_match_fails() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"new content"))
        .if_match("\"wrong-etag\"")
        .send()
        .await;

    assert!(result.is_err());
}

/// Test If-Match * succeeds when object exists.
#[tokio::test]
async fn test_if_match_star_exists() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"new content"))
        .if_match("*")
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-Match * fails when object doesn't exist.
#[tokio::test]
async fn test_if_match_star_not_exists() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("nonexistent.txt")
        .body(ByteStream::from_static(b"content"))
        .if_match("*")
        .send()
        .await;

    assert!(result.is_err());
}

/// Test If-Match on GET.
#[tokio::test]
async fn test_if_match_get() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_match(etag)
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-Match on HEAD.
#[tokio::test]
async fn test_if_match_head() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_match(etag)
        .send()
        .await;

    assert!(result.is_ok());
}
