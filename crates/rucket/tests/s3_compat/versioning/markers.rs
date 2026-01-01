//! Delete marker tests.

use crate::S3TestContext;

/// Test list versions shows delete markers.
#[tokio::test]
async fn test_versioning_list_delete_markers() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let response = ctx.list_versions().await;

    assert_eq!(response.versions().len(), 1);
    assert_eq!(response.delete_markers().len(), 1);
}

/// Test delete marker is latest.
#[tokio::test]
async fn test_versioning_delete_marker_is_latest() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let response = ctx.list_versions().await;

    let markers = response.delete_markers();
    assert_eq!(markers.len(), 1);
    assert!(markers[0].is_latest().unwrap_or(false), "Delete marker should be latest");

    let versions = response.versions();
    assert!(!versions[0].is_latest().unwrap_or(true), "Old version should not be latest");
}

/// Test multiple delete markers.
#[tokio::test]
async fn test_versioning_multiple_delete_markers() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"v1").await;
    ctx.delete("test.txt").await;
    ctx.put("test.txt", b"v2").await;
    ctx.delete("test.txt").await;

    let response = ctx.list_versions().await;

    // 2 versions + 2 delete markers
    assert_eq!(response.versions().len(), 2);
    assert_eq!(response.delete_markers().len(), 2);
}

/// Test delete marker has key and version ID.
#[tokio::test]
async fn test_versioning_delete_marker_info() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let response = ctx.list_versions().await;
    let marker = &response.delete_markers()[0];

    assert_eq!(marker.key(), Some("test.txt"));
    assert!(marker.version_id().is_some());
    assert!(marker.last_modified().is_some());
}

/// Test head on delete marker returns 404.
#[tokio::test]
async fn test_versioning_head_delete_marker() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let result = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await;

    assert!(result.is_err());
}
