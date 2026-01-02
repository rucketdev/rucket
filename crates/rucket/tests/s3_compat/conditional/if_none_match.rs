//! If-None-Match conditional tests.

use aws_sdk_s3::primitives::ByteStream;

use crate::S3TestContext;

/// Test If-None-Match * allows new object.
#[tokio::test]
async fn test_if_none_match_star_new() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("new.txt")
        .body(ByteStream::from_static(b"content"))
        .if_none_match("*")
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-None-Match * prevents overwrite.
#[tokio::test]
async fn test_if_none_match_star_exists() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"original").await;

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"new"))
        .if_none_match("*")
        .send()
        .await;

    assert!(result.is_err());

    // Original content preserved
    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"original");
}

/// Test If-None-Match with matching ETag fails.
#[tokio::test]
async fn test_if_none_match_matching() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"new"))
        .if_none_match(etag)
        .send()
        .await;

    assert!(result.is_err());
}

/// Test If-None-Match with different ETag succeeds.
#[tokio::test]
async fn test_if_none_match_different() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"new"))
        .if_none_match("\"different-etag\"")
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-None-Match on GET returns 304.
#[tokio::test]
async fn test_if_none_match_get_not_modified() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match(etag)
        .send()
        .await;

    // 304 Not Modified is treated as error by SDK
    assert!(result.is_err());
}

/// Test If-None-Match on GET with different ETag succeeds.
#[tokio::test]
async fn test_if_none_match_get_different() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match("\"different\"")
        .send()
        .await;

    assert!(result.is_ok());
}
