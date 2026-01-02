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
