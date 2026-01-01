//! Batch delete tests.

use crate::S3TestContext;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};

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
        .objects(
            ObjectIdentifier::builder()
                .key("nonexistent.txt")
                .build()
                .unwrap(),
        )
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

    let keys: Vec<&str> = response
        .deleted()
        .iter()
        .filter_map(|d| d.key())
        .collect();

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
        .objects(
            ObjectIdentifier::builder()
                .key("file.txt")
                .version_id(vid1)
                .build()
                .unwrap(),
        )
        .objects(
            ObjectIdentifier::builder()
                .key("file.txt")
                .version_id(vid2)
                .build()
                .unwrap(),
        )
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
        builder = builder.objects(
            ObjectIdentifier::builder()
                .key(format!("file{}.txt", i))
                .build()
                .unwrap(),
        );
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
