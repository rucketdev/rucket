//! Versioned GET tests.

use crate::S3TestContext;

/// Test GET specific version.
#[tokio::test]
async fn test_versioning_get_specific_version() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"version1").await;
    let v2 = ctx.put("test.txt", b"version2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    // GET specific versions
    let response1 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1");

    let body1 = response1.body.collect().await.unwrap().into_bytes();
    assert_eq!(body1.as_ref(), b"version1");

    let response2 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2");

    let body2 = response2.body.collect().await.unwrap().into_bytes();
    assert_eq!(body2.as_ref(), b"version2");
}

/// Test GET without version ID returns latest.
#[tokio::test]
async fn test_versioning_get_latest() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"version1").await;
    ctx.put("test.txt", b"version2").await;
    ctx.put("test.txt", b"version3").await;

    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"version3");
}

/// Test GET returns version ID in response.
#[tokio::test]
async fn test_versioning_get_returns_version_id() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"content").await;
    let put_vid = put.version_id().unwrap();

    let response = ctx.get_object("test.txt").await;
    let get_vid = response.version_id().unwrap();

    assert_eq!(put_vid, get_vid);
}

/// Test GET non-existent version fails.
#[tokio::test]
async fn test_versioning_get_nonexistent_version() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id("nonexistent-version-id")
        .send()
        .await;

    assert!(result.is_err());
}

/// Test GET old version after overwrite.
#[tokio::test]
async fn test_versioning_get_old_after_overwrite() {
    let ctx = S3TestContext::with_versioning().await;

    let old = ctx.put("test.txt", b"old content").await;
    let old_vid = old.version_id().unwrap();

    ctx.put("test.txt", b"new content").await;

    // Old version should still be accessible
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(old_vid)
        .send()
        .await
        .expect("Should get old version");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"old content");
}

// =============================================================================
// Extended Versioning GET Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test GET version with range.
/// Ceph: test_versioned_get_range
#[tokio::test]
async fn test_versioning_get_version_with_range() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"0123456789").await;
    let vid1 = v1.version_id().unwrap();

    ctx.put("test.txt", b"abcdefghij").await;

    // Get range from old version
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .range("bytes=0-4")
        .send()
        .await
        .expect("Should get with range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"01234");
}

/// Test GET deleted version fails.
/// Ceph: test_versioned_get_deleted
#[tokio::test]
async fn test_versioning_get_deleted_version() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"content").await;
    let vid1 = v1.version_id().unwrap();

    // Delete the specific version
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should delete");

    // GET deleted version should fail
    let result =
        ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").version_id(vid1).send().await;

    assert!(result.is_err());
}

/// Test GET version metadata is version-specific.
/// Ceph: test_versioned_get_metadata
#[tokio::test]
async fn test_versioning_get_version_metadata() {
    use aws_sdk_s3::primitives::ByteStream;

    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"v1"))
        .metadata("version", "1")
        .send()
        .await
        .expect("Should put v1");
    let vid1 = v1.version_id().unwrap();

    let v2 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"v2"))
        .metadata("version", "2")
        .send()
        .await
        .expect("Should put v2");
    let vid2 = v2.version_id().unwrap();

    // Get v1 metadata
    let response1 = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should head v1");

    assert_eq!(response1.metadata().unwrap().get("version"), Some(&"1".to_string()));

    // Get v2 metadata
    let response2 = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should head v2");

    assert_eq!(response2.metadata().unwrap().get("version"), Some(&"2".to_string()));
}

/// Test GET version ETag is version-specific.
/// Ceph: test_versioned_get_etag
#[tokio::test]
async fn test_versioning_get_version_etag() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"content1").await;
    let v2 = ctx.put("test.txt", b"content2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();
    let etag1 = v1.e_tag().unwrap();
    let etag2 = v2.e_tag().unwrap();

    // ETags should be different
    assert_ne!(etag1, etag2);

    // Get versions should return correct ETags
    let response1 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1");

    assert_eq!(response1.e_tag(), Some(etag1));

    let response2 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2");

    assert_eq!(response2.e_tag(), Some(etag2));
}

/// Test GET version content-length is correct.
/// Ceph: test_versioned_get_content_length
#[tokio::test]
async fn test_versioning_get_version_content_length() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"short").await;
    let v2 = ctx.put("test.txt", b"longer content here").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    let response1 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1");

    assert_eq!(response1.content_length(), Some(5));

    let response2 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2");

    assert_eq!(response2.content_length(), Some(19));
}

/// Test concurrent GET of different versions.
/// Ceph: test_versioned_get_concurrent
#[tokio::test]
async fn test_versioning_get_concurrent() {
    let ctx = S3TestContext::with_versioning().await;

    // Create versions
    let mut version_ids = Vec::new();
    for i in 0..5 {
        let response = ctx.put("test.txt", format!("version{}", i).as_bytes()).await;
        version_ids.push(response.version_id().unwrap().to_string());
    }

    // GET all versions concurrently
    let mut handles = Vec::new();
    for vid in version_ids.clone() {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.get_object().bucket(&bucket).key("test.txt").version_id(&vid).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent GET should succeed");
    }
}

/// Test GET version after delete marker.
/// Ceph: test_versioned_get_after_delete_marker
#[tokio::test]
async fn test_versioning_get_version_after_delete_marker() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"content").await;
    let vid1 = v1.version_id().unwrap();

    // Create delete marker
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should delete");

    // v1 should still be accessible by version ID
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1 after delete marker");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"content");
}

/// Test GET latest after restoring from delete marker.
/// Ceph: test_versioned_get_after_restore
#[tokio::test]
async fn test_versioning_get_after_restore() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"content").await;
    let _vid1 = v1.version_id().unwrap();

    // Create delete marker
    let delete = ctx
        .client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should delete");
    let delete_marker_id = delete.version_id().unwrap();

    // GET latest should fail (delete marker)
    let result = ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").send().await;
    assert!(result.is_err());

    // Remove delete marker
    ctx.client
        .delete_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(delete_marker_id)
        .send()
        .await
        .expect("Should remove delete marker");

    // GET latest should now work
    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"content");
}

/// Test GET version content-type is preserved.
/// Ceph: test_versioned_get_content_type
#[tokio::test]
async fn test_versioning_get_version_content_type() {
    use aws_sdk_s3::primitives::ByteStream;

    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"text"))
        .content_type("text/plain")
        .send()
        .await
        .expect("Should put v1");
    let vid1 = v1.version_id().unwrap();

    let v2 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"json"))
        .content_type("application/json")
        .send()
        .await
        .expect("Should put v2");
    let vid2 = v2.version_id().unwrap();

    let response1 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1");

    assert_eq!(response1.content_type(), Some("text/plain"));

    let response2 = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2");

    assert_eq!(response2.content_type(), Some("application/json"));
}
