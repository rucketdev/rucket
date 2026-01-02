//! Versioned GET tests.

use crate::S3TestContext;

/// Test GET specific version.
#[tokio::test]
async fn test_versioning_get_specific_version() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"version1").await;
    let v2 = ctx.put("test.txt", b"version2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    // GET specific versions
    let response1 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1");

    let body1 = response1.body.collect().await.unwrap().into_bytes();
    assert_eq!(body1.as_ref(), b"version1");

    let response2 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2");

    let body2 = response2.body.collect().await.unwrap().into_bytes();
    assert_eq!(body2.as_ref(), b"version2");
}

/// Test GET without version ID returns latest.
#[tokio::test]
async fn test_versioning_get_latest() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"version1").await;
    ctx.put("test.txt", b"version2").await;
    ctx.put("test.txt", b"version3").await;

    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"version3");
}

/// Test GET returns version ID in response.
#[tokio::test]
async fn test_versioning_get_returns_version_id() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"content").await;
    let put_vid = put.version_id().unwrap();

    let response = ctx.get_object("test.txt").await;
    let get_vid = response.version_id().unwrap();

    assert_eq!(put_vid, get_vid);
}

/// Test GET non-existent version fails.
#[tokio::test]
async fn test_versioning_get_nonexistent_version() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id("nonexistent-version-id")
        .send()
        .await;

    assert!(result.is_err());
}

/// Test GET old version after overwrite.
#[tokio::test]
async fn test_versioning_get_old_after_overwrite() {
    let ctx = S3TestContext::with_versioning().await;

    let old = ctx.put("test.txt", b"old content").await;
    let old_vid = old.version_id().unwrap();

    ctx.put("test.txt", b"new content").await;

    // Old version should still be accessible
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(old_vid)
        .send()
        .await
        .expect("Should get old version");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"old content");
}
