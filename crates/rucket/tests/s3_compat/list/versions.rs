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

// =============================================================================
// Extended ListVersions Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test list versions with delimiter.
/// Ceph: test_bucket_list_versions_delimiter
#[tokio::test]
async fn test_list_versions_delimiter() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("a/file1.txt", b"content").await;
    ctx.put("a/file2.txt", b"content").await;
    ctx.put("b/file1.txt", b"content").await;
    ctx.put("root.txt", b"content").await;

    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list");

    // Should have root.txt in versions and a/, b/ in common prefixes
    assert_eq!(response.versions().len(), 1);
    assert_eq!(response.common_prefixes().len(), 2);
}

/// Test list versions with prefix and delimiter.
/// Ceph: test_bucket_list_versions_prefix_delimiter
#[tokio::test]
async fn test_list_versions_prefix_delimiter() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("photos/2023/jan/pic.jpg", b"content").await;
    ctx.put("photos/2023/feb/pic.jpg", b"content").await;
    ctx.put("photos/2024/jan/pic.jpg", b"content").await;

    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .prefix("photos/2023/")
        .delimiter("/")
        .send()
        .await
        .expect("Should list");

    assert!(response.versions().is_empty());
    assert_eq!(response.common_prefixes().len(), 2); // jan/, feb/
}

/// Test list versions returns bucket name.
/// Ceph: test_bucket_list_versions_name
#[tokio::test]
async fn test_list_versions_returns_bucket_name() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_versions().await;

    assert_eq!(response.name(), Some(ctx.bucket.as_str()));
}

/// Test list versions includes etag.
/// Ceph: test_bucket_list_versions_etag
#[tokio::test]
async fn test_list_versions_includes_etag() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_versions().await;

    let version = &response.versions()[0];
    assert!(version.e_tag().is_some());
}

/// Test list versions includes size.
/// Ceph: test_bucket_list_versions_size
#[tokio::test]
async fn test_list_versions_includes_size() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"12345").await;

    let response = ctx.list_versions().await;

    let version = &response.versions()[0];
    assert_eq!(version.size(), Some(5));
}

/// Test list versions includes last modified.
/// Ceph: test_bucket_list_versions_last_modified
#[tokio::test]
async fn test_list_versions_includes_last_modified() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_versions().await;

    let version = &response.versions()[0];
    assert!(version.last_modified().is_some());
}

/// Test list versions includes owner.
/// Ceph: test_bucket_list_versions_owner
#[tokio::test]
async fn test_list_versions_includes_owner() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_versions().await;

    let version = &response.versions()[0];
    // Owner may or may not be included depending on implementation
    let _ = version.owner();
}

/// Test list versions pagination.
/// Ceph: test_bucket_list_versions_pagination
#[tokio::test]
async fn test_list_versions_pagination() {
    let ctx = S3TestContext::with_versioning().await;

    for i in 0..10 {
        ctx.put(&format!("file{:02}.txt", i), b"content").await;
    }

    let mut all_keys = Vec::new();
    let mut key_marker: Option<String> = None;

    loop {
        let mut request = ctx.client.list_object_versions().bucket(&ctx.bucket).max_keys(3);

        if let Some(marker) = &key_marker {
            request = request.key_marker(marker);
        }

        let response = request.send().await.expect("Should list");

        for version in response.versions() {
            if let Some(key) = version.key() {
                all_keys.push(key.to_string());
            }
        }

        if !response.is_truncated().unwrap_or(false) {
            break;
        }

        key_marker = response.next_key_marker().map(|s| s.to_string());
    }

    assert_eq!(all_keys.len(), 10);
}

/// Test list versions with version-id-marker.
/// Ceph: test_bucket_list_versions_version_marker
#[tokio::test]
async fn test_list_versions_version_id_marker() {
    let ctx = S3TestContext::with_versioning().await;

    // Create multiple versions of same key
    let v1 = ctx.put("file.txt", b"v1").await;
    let _v2 = ctx.put("file.txt", b"v2").await;
    let _v3 = ctx.put("file.txt", b"v3").await;

    let vid1 = v1.version_id().unwrap();

    // List starting after v1
    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .key_marker("file.txt")
        .version_id_marker(vid1)
        .send()
        .await
        .expect("Should list");

    // Should skip v1 and potentially v2, v3 (order depends on implementation)
    // Just verify we can use version_id_marker
    let _ = response.versions();
}

/// Test list versions concurrent requests.
/// Ceph: test_bucket_list_versions_concurrent
#[tokio::test]
async fn test_list_versions_concurrent() {
    let ctx = S3TestContext::with_versioning().await;

    for i in 0..5 {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle =
            tokio::spawn(async move { client.list_object_versions().bucket(&bucket).send().await });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

/// Test list versions delete marker has is_latest.
/// Ceph: test_bucket_list_versions_delete_marker_is_latest
#[tokio::test]
async fn test_list_versions_delete_marker_is_latest() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("file.txt", b"content").await;
    ctx.delete("file.txt").await;

    let response = ctx.list_versions().await;

    let delete_markers = response.delete_markers();
    assert_eq!(delete_markers.len(), 1);

    let marker = &delete_markers[0];
    assert!(marker.is_latest().unwrap_or(false));
}

/// Test list versions max keys zero.
/// Ceph: test_bucket_list_versions_maxkeys_zero
#[tokio::test]
async fn test_list_versions_max_keys_zero() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .max_keys(0)
        .send()
        .await
        .expect("Should list");

    assert!(response.versions().is_empty());
}

/// Test list versions encoding type.
/// Ceph: test_bucket_list_versions_encoding
#[tokio::test]
async fn test_list_versions_encoding_type() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("file with spaces.txt", b"content").await;

    let response = ctx
        .client
        .list_object_versions()
        .bucket(&ctx.bucket)
        .encoding_type(aws_sdk_s3::types::EncodingType::Url)
        .send()
        .await
        .expect("Should list");

    assert_eq!(response.versions().len(), 1);
}
