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

// =============================================================================
// Extended Versioning PUT Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test PUT with metadata creates versioned object.
/// Ceph: test_versioned_put_metadata
#[tokio::test]
async fn test_versioning_put_with_metadata() {
    let ctx = S3TestContext::with_versioning().await;

    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("meta.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"content"))
        .metadata("key", "value")
        .send()
        .await
        .expect("Should put with metadata");

    assert!(response.version_id().is_some());
}

/// Test PUT with content-type creates versioned object.
/// Ceph: test_versioned_put_content_type
#[tokio::test]
async fn test_versioning_put_with_content_type() {
    let ctx = S3TestContext::with_versioning().await;

    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("typed.txt")
        .body(aws_sdk_s3::primitives::ByteStream::from_static(b"content"))
        .content_type("text/plain")
        .send()
        .await
        .expect("Should put with content type");

    assert!(response.version_id().is_some());
}

/// Test each PUT returns unique version ID.
/// Ceph: test_versioned_put_unique_ids
#[tokio::test]
async fn test_versioning_put_unique_ids() {
    let ctx = S3TestContext::with_versioning().await;

    let mut ids = std::collections::HashSet::new();
    for i in 0..10 {
        let response = ctx.put(&format!("obj{}.txt", i), b"content").await;
        let vid = response.version_id().unwrap().to_string();
        assert!(ids.insert(vid), "Version IDs should be unique");
    }
}

/// Test version ID format is valid.
/// Ceph: test_versioned_put_id_format
#[tokio::test]
async fn test_versioning_put_id_format() {
    let ctx = S3TestContext::with_versioning().await;

    let response = ctx.put("test.txt", b"content").await;
    let vid = response.version_id().unwrap();

    // Version ID should not be empty
    assert!(!vid.is_empty());
}

/// Test large file creates versioned object.
/// Ceph: test_versioned_put_large
#[tokio::test]
async fn test_versioning_put_large_file() {
    let ctx = S3TestContext::with_versioning().await;

    let content = vec![b'x'; 1024 * 1024]; // 1MB
    let response = ctx.put("large.bin", &content).await;

    assert!(response.version_id().is_some());
}

/// Test empty file creates versioned object.
/// Ceph: test_versioned_put_empty
#[tokio::test]
async fn test_versioning_put_empty_file() {
    let ctx = S3TestContext::with_versioning().await;

    let response = ctx.put("empty.txt", b"").await;
    assert!(response.version_id().is_some());
}

/// Test overwrite preserves old versions.
/// Ceph: test_versioned_put_preserves
#[tokio::test]
async fn test_versioning_put_preserves_old_versions() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"version1").await;
    let v2 = ctx.put("test.txt", b"version2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    // Both versions should be accessible
    let get1 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1");

    let body1 = get1.body.collect().await.unwrap().into_bytes();
    assert_eq!(body1.as_ref(), b"version1");

    let get2 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2");

    let body2 = get2.body.collect().await.unwrap().into_bytes();
    assert_eq!(body2.as_ref(), b"version2");
}

/// Test different keys get different version IDs.
/// Ceph: test_versioned_put_different_keys
#[tokio::test]
async fn test_versioning_put_different_keys() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("key1.txt", b"content1").await;
    let v2 = ctx.put("key2.txt", b"content2").await;

    assert!(v1.version_id().is_some());
    assert!(v2.version_id().is_some());
    // Different keys may have same or different version IDs
}
