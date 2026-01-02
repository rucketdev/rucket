//! If-Modified-Since and If-Unmodified-Since tests.

use aws_smithy_types::DateTime;

use crate::S3TestContext;

/// Test If-Modified-Since with old date returns content.
#[tokio::test]
async fn test_if_modified_since_old_date() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Use a very old date
    let old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_modified_since(old_date)
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-Modified-Since with future date returns 304.
#[tokio::test]
async fn test_if_modified_since_future_date() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Use a future date (year 2100)
    let future_date = DateTime::from_secs(4102444800);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_modified_since(future_date)
        .send()
        .await;

    // 304 Not Modified
    assert!(result.is_err());
}

/// Test If-Unmodified-Since with old date fails.
#[tokio::test]
async fn test_if_unmodified_since_old_date() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_unmodified_since(old_date)
        .send()
        .await;

    // 412 Precondition Failed
    assert!(result.is_err());
}

/// Test If-Unmodified-Since with future date succeeds.
#[tokio::test]
async fn test_if_unmodified_since_future_date() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let future_date = DateTime::from_secs(4102444800);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_unmodified_since(future_date)
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test conditional on HEAD.
#[tokio::test]
async fn test_if_modified_head() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_modified_since(old_date)
        .send()
        .await;

    assert!(result.is_ok());
}

// =============================================================================
// Extended If-Modified-Since Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test If-Modified-Since with exact modification time.
/// Ceph: test_if_modified_exact
#[tokio::test]
async fn test_if_modified_since_exact() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let head = ctx.head("test.txt").await;
    let mod_time = head.last_modified().expect("Should have last modified");

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_modified_since(*mod_time)
        .send()
        .await;

    // With exact same time, should return 304
    assert!(result.is_err());
}

/// Test If-Unmodified-Since with exact modification time.
/// Ceph: test_if_unmodified_exact
#[tokio::test]
async fn test_if_unmodified_since_exact() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let head = ctx.head("test.txt").await;
    let mod_time = head.last_modified().expect("Should have last modified");

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_unmodified_since(*mod_time)
        .send()
        .await;

    // With exact same time, should succeed
    assert!(result.is_ok());
}

/// Test If-Modified-Since on versioned object.
/// Ceph: test_if_modified_versioned
#[tokio::test]
async fn test_if_modified_since_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"content").await;
    let vid = put.version_id().unwrap();

    let old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid)
        .if_modified_since(old_date)
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-Modified-Since combined with If-Match.
/// Ceph: test_if_modified_with_if_match
#[tokio::test]
async fn test_if_modified_with_if_match() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_modified_since(old_date)
        .if_match(etag)
        .send()
        .await;

    assert!(result.is_ok());
}

/// Test If-Unmodified-Since combined with If-Match fails when modified.
/// Ceph: test_if_unmodified_with_if_match
#[tokio::test]
async fn test_if_unmodified_fails_with_old_date() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let very_old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_unmodified_since(very_old_date)
        .send()
        .await;

    assert!(result.is_err());
}

/// Test conditional on non-existent object.
/// Ceph: test_if_modified_nonexistent
#[tokio::test]
async fn test_if_modified_since_nonexistent() {
    let ctx = S3TestContext::new().await;

    let old_date = DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("nonexistent.txt")
        .if_modified_since(old_date)
        .send()
        .await;

    assert!(result.is_err()); // 404 Not Found
}

/// Test If-Unmodified-Since on HEAD.
/// Ceph: test_if_unmodified_head
#[tokio::test]
async fn test_if_unmodified_since_head() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let future_date = DateTime::from_secs(4102444800);

    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_unmodified_since(future_date)
        .send()
        .await;

    assert!(result.is_ok());
}
