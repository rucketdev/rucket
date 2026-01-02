//! Object DELETE tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_delete_*
//! - MinIO Mint: DeleteObject tests

use crate::S3TestContext;

/// Test basic object DELETE.
/// Ceph: test_object_delete
#[tokio::test]
async fn test_object_delete_simple() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    ctx.delete("test.txt").await;

    let exists = ctx.exists("test.txt").await;
    assert!(!exists, "Object should not exist after deletion");
}

/// Test DELETE non-existent object is idempotent.
/// Ceph: test_object_delete_nonexistent
#[tokio::test]
async fn test_object_delete_nonexistent_idempotent() {
    let ctx = S3TestContext::new().await;

    // DELETE on non-existent object should succeed (idempotent)
    let result = ctx.client.delete_object().bucket(&ctx.bucket).key("nonexistent.txt").send().await;

    assert!(result.is_ok(), "DELETE on non-existent object should succeed");
}

/// Test DELETE from non-existent bucket fails.
#[tokio::test]
async fn test_object_delete_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result =
        ctx.client.delete_object().bucket("nonexistent-bucket").key("test.txt").send().await;

    assert!(result.is_err(), "DELETE from non-existent bucket should fail");
}

/// Test double DELETE is idempotent.
#[tokio::test]
async fn test_object_delete_twice_idempotent() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    ctx.delete("test.txt").await;

    // Second delete should also succeed
    let result = ctx.client.delete_object().bucket(&ctx.bucket).key("test.txt").send().await;

    assert!(result.is_ok(), "Second DELETE should succeed");
}

/// Test DELETE then GET returns 404.
#[tokio::test]
async fn test_object_delete_then_get() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let result = ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").send().await;

    assert!(result.is_err(), "GET after DELETE should return error");
}

/// Test DELETE then HEAD returns 404.
#[tokio::test]
async fn test_object_delete_then_head() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await;

    let result = ctx.client.head_object().bucket(&ctx.bucket).key("test.txt").send().await;

    assert!(result.is_err(), "HEAD after DELETE should return error");
}

/// Test DELETE removes object from LIST.
#[tokio::test]
async fn test_object_delete_removed_from_list() {
    let ctx = S3TestContext::new().await;

    ctx.put("file1.txt", b"content").await;
    ctx.put("file2.txt", b"content").await;

    ctx.delete("file1.txt").await;

    let response = ctx.list_objects().await;
    let keys: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();

    assert_eq!(keys.len(), 1);
    assert!(keys.contains(&"file2.txt"));
    assert!(!keys.contains(&"file1.txt"));
}

/// Test DELETE with deep path.
#[tokio::test]
async fn test_object_delete_deep_path() {
    let ctx = S3TestContext::new().await;

    let key = "a/b/c/d/file.txt";
    ctx.put(key, b"content").await;
    ctx.delete(key).await;

    let exists = ctx.exists(key).await;
    assert!(!exists, "Deep path object should be deleted");
}

/// Test DELETE many objects.
#[tokio::test]
async fn test_object_delete_many() {
    let ctx = S3TestContext::new().await;

    let count = 20;
    for i in 0..count {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    for i in 0..count {
        ctx.delete(&format!("file{}.txt", i)).await;
    }

    let response = ctx.list_objects().await;
    assert!(response.contents().is_empty(), "All objects should be deleted");
}

/// Test concurrent DELETEs.
#[tokio::test]
async fn test_object_delete_concurrent() {
    let ctx = S3TestContext::new().await;

    // Create objects
    for i in 0..10 {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    // Delete concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.delete_object().bucket(&bucket).key(format!("file{}.txt", i)).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent DELETE should succeed");
    }

    let response = ctx.list_objects().await;
    assert!(response.contents().is_empty(), "All objects should be deleted");
}

/// Test DELETE then re-PUT same key.
#[tokio::test]
async fn test_object_delete_then_put() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Version 1").await;
    ctx.delete("test.txt").await;
    ctx.put("test.txt", b"Version 2").await;

    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"Version 2");
}

// =============================================================================
// Extended Delete Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test DELETE versioned object creates delete marker.
/// Ceph: test_object_delete_versioned
#[tokio::test]
async fn test_object_delete_versioned_creates_marker() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"content").await;
    let version_id = put.version_id().unwrap();

    // Delete without version ID creates delete marker
    let delete_result =
        ctx.client.delete_object().bucket(&ctx.bucket).key("test.txt").send().await.unwrap();

    assert!(delete_result.delete_marker().unwrap_or(false), "Should create delete marker");
    assert!(delete_result.version_id().is_some(), "Delete marker should have version ID");
    assert_ne!(delete_result.version_id(), Some(version_id));

    // Object should appear deleted
    let result = ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").send().await;
    assert!(result.is_err());
}

/// Test DELETE specific version.
/// Ceph: test_object_delete_specific_version
#[tokio::test]
async fn test_object_delete_specific_version() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"version1").await;
    let v2 = ctx.put("test.txt", b"version2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    // Delete v1
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should delete specific version");

    // v2 should still exist and be current
    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"version2");

    // v1 should be gone
    let result =
        ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").version_id(vid1).send().await;
    assert!(result.is_err());

    // v2 should still be accessible by version ID
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("v2 should exist");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"version2");
}

/// Test DELETE delete marker removes it.
/// Ceph: test_object_delete_delete_marker
#[tokio::test]
async fn test_object_delete_removes_delete_marker() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    // Create delete marker
    let delete1 =
        ctx.client.delete_object().bucket(&ctx.bucket).key("test.txt").send().await.unwrap();

    let marker_id = delete1.version_id().unwrap();

    // Delete the delete marker
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(marker_id)
        .send()
        .await
        .expect("Should delete delete marker");

    // Original object should be accessible again
    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"content");
}

/// Test DELETE with special characters in key.
/// Ceph: test_object_delete_special_chars
#[tokio::test]
async fn test_object_delete_special_chars_key() {
    let ctx = S3TestContext::new().await;

    let keys = ["file with spaces.txt", "file+plus.txt", "path/to/deep/file.txt"];

    for key in &keys {
        ctx.put(key, b"content").await;
        ctx.delete(key).await;
        let exists = ctx.exists(key).await;
        assert!(!exists, "Object {} should be deleted", key);
    }
}

/// Test DELETE empty object.
/// Ceph: test_object_delete_empty
#[tokio::test]
async fn test_object_delete_empty() {
    let ctx = S3TestContext::new().await;

    ctx.put("empty.txt", b"").await;
    ctx.delete("empty.txt").await;

    let exists = ctx.exists("empty.txt").await;
    assert!(!exists);
}

/// Test DELETE returns version ID on versioned bucket.
/// Ceph: test_object_delete_returns_version_id
#[tokio::test]
async fn test_object_delete_returns_version_id() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let delete_result =
        ctx.client.delete_object().bucket(&ctx.bucket).key("test.txt").send().await.unwrap();

    assert!(delete_result.version_id().is_some());
}

/// Test DELETE all versions.
/// Ceph: test_object_delete_all_versions
#[tokio::test]
async fn test_object_delete_all_versions() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"v1").await;
    let v2 = ctx.put("test.txt", b"v2").await;
    let v3 = ctx.put("test.txt", b"v3").await;

    // Delete all versions
    for vid in [v1.version_id().unwrap(), v2.version_id().unwrap(), v3.version_id().unwrap()] {
        ctx.client
            .delete_object()
            .bucket(&ctx.bucket)
            .key("test.txt")
            .version_id(vid)
            .send()
            .await
            .expect("Should delete version");
    }

    // No versions should exist
    let versions = ctx.list_versions().await;
    assert!(versions.versions().is_empty());
}

/// Test DELETE concurrent on versioned bucket.
/// Ceph: test_object_delete_concurrent_versioned
#[tokio::test]
async fn test_object_delete_concurrent_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    // Create multiple versions
    for i in 0..5 {
        ctx.put("test.txt", format!("version{}", i).as_bytes()).await;
    }

    // Delete concurrently (creates delete markers)
    let mut handles = Vec::new();
    for _ in 0..5 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.delete_object().bucket(&bucket).key("test.txt").send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}
