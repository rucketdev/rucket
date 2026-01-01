//! If-Modified-Since and If-Unmodified-Since tests.

use crate::S3TestContext;
use aws_smithy_types::DateTime;

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
