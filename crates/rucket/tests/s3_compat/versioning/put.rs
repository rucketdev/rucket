//! Versioned PUT tests.

use crate::S3TestContext;

/// Test PUT returns version ID when versioning enabled.
#[tokio::test]
async fn test_versioning_put_returns_version_id() {
    let ctx = S3TestContext::with_versioning().await;

    let response = ctx.put("test.txt", b"content").await;
    assert!(response.version_id().is_some());
}

/// Test multiple PUTs create multiple versions.
#[tokio::test]
async fn test_versioning_put_multiple_versions() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"version1").await;
    let v2 = ctx.put("test.txt", b"version2").await;
    let v3 = ctx.put("test.txt", b"version3").await;

    // All should have different version IDs
    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();
    let vid3 = v3.version_id().unwrap();

    assert_ne!(vid1, vid2);
    assert_ne!(vid2, vid3);
    assert_ne!(vid1, vid3);
}

/// Test PUT without versioning returns null version.
#[tokio::test]
async fn test_versioning_put_unversioned() {
    let ctx = S3TestContext::new().await;

    let response = ctx.put("test.txt", b"content").await;
    // Unversioned buckets may return None or "null" for version ID
    let vid = response.version_id();
    assert!(vid.is_none() || vid == Some("null"));
}

/// Test list versions shows all PUT versions.
#[tokio::test]
async fn test_versioning_put_list_versions() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"v1").await;
    ctx.put("test.txt", b"v2").await;
    ctx.put("test.txt", b"v3").await;

    let response = ctx.list_versions().await;
    let versions = response.versions();

    assert_eq!(versions.len(), 3);
}

/// Test PUT after suspend versioning.
#[tokio::test]
async fn test_versioning_put_after_suspend() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"versioned").await;
    ctx.suspend_versioning().await;
    let response = ctx.put("test.txt", b"unversioned").await;

    // After suspend, new objects get null version
    let vid = response.version_id();
    assert!(vid.is_none() || vid == Some("null"));
}
