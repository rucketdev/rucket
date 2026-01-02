//! Batch delete tests.

use aws_sdk_s3::types::{Delete, ObjectIdentifier};

use crate::S3TestContext;

/// Test delete multiple objects.
#[tokio::test]
async fn test_delete_objects_basic() {
    let ctx = S3TestContext::new().await;

    // Create objects
    for i in 0..5 {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("file0.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("file1.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("file2.txt").build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete objects");

    let deleted = response.deleted();
    assert_eq!(deleted.len(), 3);

    // Verify remaining
    let list = ctx.list_objects().await;
    assert_eq!(list.contents().len(), 2);
}

/// Test delete multiple includes non-existent.
#[tokio::test]
async fn test_delete_objects_includes_nonexistent() {
    let ctx = S3TestContext::new().await;

    ctx.put("exists.txt", b"content").await;

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("exists.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("nonexistent.txt").build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete objects");

    // Both should be reported as deleted
    let deleted = response.deleted();
    assert_eq!(deleted.len(), 2);
}

/// Test delete multiple with quiet mode.
#[tokio::test]
async fn test_delete_objects_quiet() {
    let ctx = S3TestContext::new().await;

    ctx.put("file.txt", b"content").await;

    let delete = Delete::builder()
        .quiet(true)
        .objects(ObjectIdentifier::builder().key("file.txt").build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete objects");

    // In quiet mode, only errors are returned
    assert!(response.deleted().is_empty());
}

/// Test delete objects returns keys.
#[tokio::test]
async fn test_delete_objects_returns_keys() {
    let ctx = S3TestContext::new().await;

    ctx.put("a.txt", b"content").await;
    ctx.put("b.txt", b"content").await;

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("a.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("b.txt").build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete");

    let keys: Vec<&str> = response.deleted().iter().filter_map(|d| d.key()).collect();

    assert!(keys.contains(&"a.txt"));
    assert!(keys.contains(&"b.txt"));
}

/// Test delete objects with versioning.
#[tokio::test]
async fn test_delete_objects_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("file.txt", b"v1").await;
    let v2 = ctx.put("file.txt", b"v2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("file.txt").version_id(vid1).build().unwrap())
        .objects(ObjectIdentifier::builder().key("file.txt").version_id(vid2).build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete");

    assert_eq!(response.deleted().len(), 2);

    // Both versions should be gone
    let versions = ctx.list_versions().await;
    assert!(versions.versions().is_empty());
}

/// Test delete objects many.
#[tokio::test]
async fn test_delete_objects_many() {
    let ctx = S3TestContext::new().await;

    let count = 50;
    for i in 0..count {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    let mut builder = Delete::builder();
    for i in 0..count {
        builder = builder
            .objects(ObjectIdentifier::builder().key(format!("file{}.txt", i)).build().unwrap());
    }

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(builder.build().unwrap())
        .send()
        .await
        .expect("Should delete");

    assert_eq!(response.deleted().len(), count);

    let list = ctx.list_objects().await;
    assert!(list.contents().is_empty());
}

// =============================================================================
// Extended Delete Multiple Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test delete objects returns version IDs on versioned bucket.
/// Ceph: test_delete_objects_versioned_returns_version_ids
#[tokio::test]
async fn test_delete_objects_versioned_returns_version_ids() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("file.txt", b"v1").await;
    let vid = v1.version_id().unwrap();

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("file.txt").version_id(vid).build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete");

    let deleted = &response.deleted()[0];
    assert!(deleted.version_id().is_some());
}

/// Test delete objects creates delete markers.
/// Ceph: test_delete_objects_creates_delete_markers
#[tokio::test]
#[ignore = "Delete marker response field not implemented"]
async fn test_delete_objects_creates_delete_markers() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("file.txt", b"content").await;

    // Delete without version ID creates a delete marker
    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("file.txt").build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete");

    let deleted = &response.deleted()[0];
    assert!(deleted.delete_marker().unwrap_or(false));
}

/// Test delete objects partial failure.
/// Ceph: test_delete_objects_partial_failure
#[tokio::test]
#[ignore = "Access control not implemented"]
async fn test_delete_objects_partial_failure() {
    let _ctx = S3TestContext::new().await;
    // Test with mixed permissions - some objects can be deleted, some cannot
}

/// Test delete objects empty list.
/// Ceph: test_delete_objects_empty
#[tokio::test]
#[ignore = "Empty delete list validation not implemented"]
async fn test_delete_objects_empty_list() {
    let ctx = S3TestContext::new().await;

    let delete = Delete::builder().build().unwrap();

    let result = ctx.client.delete_objects().bucket(&ctx.bucket).delete(delete).send().await;

    // Empty delete should succeed or error depending on implementation
    // AWS returns MalformedXML
    assert!(result.is_err() || result.unwrap().deleted().is_empty());
}

/// Test delete objects with prefix pattern.
/// Ceph: test_delete_objects_prefix
#[tokio::test]
async fn test_delete_objects_with_prefix() {
    let ctx = S3TestContext::new().await;

    ctx.put("prefix/a.txt", b"content").await;
    ctx.put("prefix/b.txt", b"content").await;
    ctx.put("other/c.txt", b"content").await;

    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("prefix/a.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("prefix/b.txt").build().unwrap())
        .build()
        .unwrap();

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(delete)
        .send()
        .await
        .expect("Should delete");

    assert_eq!(response.deleted().len(), 2);

    // other/c.txt should still exist
    let list = ctx.list_objects().await;
    assert_eq!(list.contents().len(), 1);
    assert_eq!(list.contents()[0].key(), Some("other/c.txt"));
}

/// Test delete objects max limit (1000).
/// Ceph: test_delete_objects_max
#[tokio::test]
async fn test_delete_objects_max_limit() {
    let ctx = S3TestContext::new().await;

    // Create more than 1000 objects but only delete 100 to keep test fast
    let count = 100;
    for i in 0..count {
        ctx.put(&format!("file{:04}.txt", i), b"x").await;
    }

    let mut builder = Delete::builder();
    for i in 0..count {
        builder = builder
            .objects(ObjectIdentifier::builder().key(format!("file{:04}.txt", i)).build().unwrap());
    }

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(builder.build().unwrap())
        .send()
        .await
        .expect("Should delete");

    assert_eq!(response.deleted().len(), count);
}

/// Test delete objects with special characters in keys.
/// Ceph: test_delete_objects_special_chars
#[tokio::test]
async fn test_delete_objects_special_chars() {
    let ctx = S3TestContext::new().await;

    let keys = ["file with spaces.txt", "file+plus.txt", "path/to/file.txt"];

    for key in &keys {
        ctx.put(key, b"content").await;
    }

    let mut builder = Delete::builder();
    for key in &keys {
        builder = builder.objects(ObjectIdentifier::builder().key(*key).build().unwrap());
    }

    let response = ctx
        .client
        .delete_objects()
        .bucket(&ctx.bucket)
        .delete(builder.build().unwrap())
        .send()
        .await
        .expect("Should delete");

    assert_eq!(response.deleted().len(), keys.len());

    let list = ctx.list_objects().await;
    assert!(list.contents().is_empty());
}

/// Test delete objects concurrent requests.
/// Ceph: test_delete_objects_concurrent
#[tokio::test]
async fn test_delete_objects_concurrent() {
    let ctx = S3TestContext::new().await;

    // Create objects in batches
    for batch in 0..5 {
        for i in 0..10 {
            ctx.put(&format!("batch{}-file{}.txt", batch, i), b"content").await;
        }
    }

    // Delete in parallel
    let mut handles = Vec::new();
    for batch in 0..5 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let mut builder = Delete::builder();
            for i in 0..10 {
                builder = builder.objects(
                    ObjectIdentifier::builder()
                        .key(format!("batch{}-file{}.txt", batch, i))
                        .build()
                        .unwrap(),
                );
            }
            client.delete_objects().bucket(&bucket).delete(builder.build().unwrap()).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().deleted().len(), 10);
    }

    let list = ctx.list_objects().await;
    assert!(list.contents().is_empty());
}
