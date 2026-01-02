//! Versioned DELETE tests.

use crate::S3TestContext;

/// Test DELETE creates delete marker.
#[tokio::test]
async fn test_versioning_delete_creates_marker() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    let delete_response = ctx
        .client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should delete");

    // Should return delete marker version ID
    assert!(delete_response.version_id().is_some());
    assert!(delete_response.delete_marker().unwrap_or(false));
}

/// Test GET after DELETE returns 404.
#[tokio::test]
async fn test_versioning_delete_then_get() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let result = ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").send().await;

    assert!(result.is_err());
}

/// Test DELETE specific version permanently removes it.
#[tokio::test]
async fn test_versioning_delete_specific_version() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"version1").await;
    let vid1 = v1.version_id().unwrap();

    ctx.put("test.txt", b"version2").await;

    // Delete specific version
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should delete specific version");

    // That version should be gone
    let result =
        ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").version_id(vid1).send().await;

    assert!(result.is_err());
}

/// Test can still GET old version after delete marker.
#[tokio::test]
async fn test_versioning_get_version_after_delete_marker() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"original").await;
    let vid = put.version_id().unwrap();

    // Create delete marker
    ctx.delete("test.txt").await;

    // Can still get the old version by ID
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid)
        .send()
        .await
        .expect("Should get old version");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"original");
}

/// Test delete delete marker restores object.
#[tokio::test]
async fn test_versioning_delete_delete_marker() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    // Create delete marker
    let delete = ctx
        .client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should delete");

    let marker_vid = delete.version_id().unwrap();

    // Now delete the delete marker
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(marker_vid)
        .send()
        .await
        .expect("Should delete marker");

    // Object should be accessible again
    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"content");
}
