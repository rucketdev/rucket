//! ListObjectVersions tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_list_versions_*
//! - MinIO Mint: ListObjectVersions tests

use crate::S3TestContext;

/// Test list object versions basic.
/// Ceph: test_bucket_list_versions
#[tokio::test]
async fn test_list_versions_basic() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("file.txt", b"version1").await;
    ctx.put("file.txt", b"version2").await;
    ctx.put("file.txt", b"version3").await;

    let response = ctx.list_versions().await;

    let versions = response.versions();
    assert_eq!(versions.len(), 3, "Should have 3 versions");
}

/// Test list versions on unversioned bucket.
#[tokio::test]
async fn test_list_versions_unversioned_bucket() {
    let ctx = S3TestContext::new().await;

    ctx.put("file1.txt", b"content").await;
    ctx.put("file2.txt", b"content").await;

    let response = ctx.list_versions().await;

    // Unversioned bucket still returns objects with null version
    let versions = response.versions();
    assert_eq!(versions.len(), 2);
}

/// Test list versions with delete markers.
#[tokio::test]
async fn test_list_versions_with_delete_markers() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("file.txt", b"content").await;
    ctx.delete("file.txt").await;

    let response = ctx.list_versions().await;

    // Should have 1 version + 1 delete marker
    let versions = response.versions();
    let delete_markers = response.delete_markers();

    assert_eq!(versions.len(), 1, "Should have 1 version");
    assert_eq!(delete_markers.len(), 1, "Should have 1 delete marker");
}

/// Test list versions with prefix.
#[tokio::test]
async fn test_list_versions_prefix() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("docs/file1.txt", b"content").await;
    ctx.put("docs/file2.txt", b"content").await;
    ctx.put("src/main.rs", b"content").await;

    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("Should list versions");

    let versions = response.versions();
    assert_eq!(versions.len(), 2);
    for v in versions {
        assert!(v.key().unwrap().starts_with("docs/"));
    }
}

/// Test list versions with max-keys.
#[tokio::test]
async fn test_list_versions_max_keys() {
    let ctx = S3TestContext::with_versioning().await;

    for i in 0..5 {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .max_keys(2)
        .send()
        .await
        .expect("Should list versions");

    assert_eq!(response.versions().len(), 2);
    assert!(response.is_truncated().unwrap_or(false));
}

/// Test list versions with key-marker.
#[tokio::test]
async fn test_list_versions_key_marker() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("a.txt", b"content").await;
    ctx.put("b.txt", b"content").await;
    ctx.put("c.txt", b"content").await;

    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .key_marker("a.txt")
        .send()
        .await
        .expect("Should list versions");

    let keys: Vec<&str> = response.versions().iter().filter_map(|v| v.key()).collect();
    assert_eq!(keys, vec!["b.txt", "c.txt"]);
}

/// Test list versions includes version id.
#[tokio::test]
async fn test_list_versions_includes_version_id() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("file.txt", b"content").await;

    let response = ctx.list_versions().await;

    let version = &response.versions()[0];
    assert!(version.version_id().is_some(), "Should include version ID");
}

/// Test list versions includes is_latest.
#[tokio::test]
async fn test_list_versions_is_latest() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("file.txt", b"version1").await;
    ctx.put("file.txt", b"version2").await;

    let response = ctx.list_versions().await;

    let versions = response.versions();
    let latest_count = versions.iter().filter(|v| v.is_latest().unwrap_or(false)).count();
    assert_eq!(latest_count, 1, "Only one version should be latest");
}

/// Test list versions sorted by key then version.
#[tokio::test]
async fn test_list_versions_sorted() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("b.txt", b"content").await;
    ctx.put("a.txt", b"content").await;
    ctx.put("c.txt", b"content").await;

    let response = ctx.list_versions().await;

    let keys: Vec<&str> = response.versions().iter().filter_map(|v| v.key()).collect();
    assert_eq!(keys, vec!["a.txt", "b.txt", "c.txt"]);
}

/// Test list versions empty bucket.
#[tokio::test]
async fn test_list_versions_empty() {
    let ctx = S3TestContext::with_versioning().await;

    let response = ctx.list_versions().await;

    assert!(response.versions().is_empty());
    assert!(response.delete_markers().is_empty());
}

/// Test list versions after all versions deleted.
#[tokio::test]
async fn test_list_versions_all_deleted() {
    let ctx = S3TestContext::with_versioning().await;

    let put_response = ctx.put("file.txt", b"content").await;
    let version_id = put_response.version_id().unwrap();

    // Delete specific version
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("file.txt")
        .version_id(version_id)
        .send()
        .await
        .expect("Should delete version");

    let response = ctx.list_versions().await;

    // Should be empty after deleting the only version
    assert!(response.versions().is_empty());
}
