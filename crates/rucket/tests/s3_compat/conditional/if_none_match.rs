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

// =============================================================================
// Extended If-None-Match Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test If-None-Match on HEAD returns 304 when matching.
/// Ceph: test_if_none_match_head
#[tokio::test]
#[ignore = "If-None-Match on HEAD not implemented"]
async fn test_if_none_match_head_matching() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match(etag)
        .send()
        .await;

    // 304 is returned as error
    assert!(result.is_err());
}

/// Test If-None-Match on HEAD succeeds with different ETag.
/// Ceph: test_if_none_match_head_different
#[tokio::test]
async fn test_if_none_match_head_different() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match("\"different\"")
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-None-Match with multiple ETags.
/// Ceph: test_if_none_match_multiple
#[tokio::test]
async fn test_if_none_match_multiple_etags() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    // Multiple ETags in If-None-Match
    let multi_etag = format!("\"etag1\", {}, \"etag2\"", etag);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match(multi_etag)
        .send()
        .await;

    // Should match one of them, return 304
    assert!(result.is_err());
}

/// Test If-None-Match on versioned object.
/// Ceph: test_if_none_match_versioned
#[tokio::test]
async fn test_if_none_match_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"content").await;
    let vid = put.version_id().unwrap();
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid)
        .if_none_match(etag)
        .send()
        .await;

    // 304 when matching
    assert!(result.is_err());
}

/// Test If-None-Match combined with If-Modified-Since.
/// Ceph: test_if_none_match_with_modified
#[tokio::test]
async fn test_if_none_match_with_if_modified() {
    use aws_smithy_types::DateTime;

    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match(etag)
        .if_modified_since(old_date)
        .send()
        .await;

    // If-None-Match takes precedence, returns 304
    assert!(result.is_err());
}

/// Test If-None-Match * on non-existent object.
/// Ceph: test_if_none_match_star_nonexistent
#[tokio::test]
async fn test_if_none_match_star_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("new-object.txt")
        .body(ByteStream::from_static(b"content"))
        .if_none_match("*")
        .send()
        .await;

    // Should succeed for new objects
    assert!(result.is_ok());
}

/// Test weak ETag comparison.
/// Ceph: test_if_none_match_weak
#[tokio::test]
#[ignore = "Weak ETags not implemented"]
async fn test_if_none_match_weak_etag() {
    let _ctx = S3TestContext::new().await;
    // Weak ETags use W/ prefix
}
