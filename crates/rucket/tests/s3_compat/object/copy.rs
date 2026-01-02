//! Object COPY tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_copy_*
//! - MinIO Mint: CopyObject tests

use aws_sdk_s3::primitives::ByteStream;

use crate::{random_bucket_name, S3TestContext};

/// Test basic object copy within same bucket.
/// Ceph: test_object_copy_same_bucket
#[tokio::test]
async fn test_object_copy_same_bucket() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"original content").await;

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let data = ctx.get("dest.txt").await;
    assert_eq!(data.as_slice(), b"original content");

    // Source should still exist
    let source_data = ctx.get("source.txt").await;
    assert_eq!(source_data.as_slice(), b"original content");
}

/// Test copy object between buckets.
/// Ceph: test_object_copy_different_bucket
#[tokio::test]
async fn test_object_copy_different_bucket() {
    let source_bucket = random_bucket_name();
    let dest_bucket = random_bucket_name();

    let ctx = S3TestContext::with_buckets(&[&source_bucket, &dest_bucket]).await;

    // Put in source bucket
    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&source_bucket)
        .key("source.txt")
        .body(body)
        .send()
        .await
        .expect("Should put object");

    // Copy to dest bucket
    ctx.client
        .copy_object()
        .bucket(&dest_bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", source_bucket))
        .send()
        .await
        .expect("Should copy object");

    // Verify in dest bucket
    let response = ctx
        .client
        .get_object()
        .bucket(&dest_bucket)
        .key("dest.txt")
        .send()
        .await
        .expect("Should get copied object");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"content");
}

/// Test copy preserves content type.
#[tokio::test]
async fn test_object_copy_preserves_content_type() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"<html></html>");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.html")
        .body(body)
        .content_type("text/html")
        .send()
        .await
        .expect("Should put object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.html")
        .copy_source(format!("{}/source.html", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let response = ctx.head("dest.html").await;
    assert_eq!(response.content_type(), Some("text/html"));
}

/// Test copy preserves user metadata.
#[tokio::test]
async fn test_object_copy_preserves_metadata() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(body)
        .metadata("custom", "value")
        .send()
        .await
        .expect("Should put object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let response = ctx.head("dest.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("custom"), Some(&"value".to_string()));
}

/// Test copy from non-existent source fails.
/// Ceph: test_object_copy_noexistent_source
#[tokio::test]
async fn test_object_copy_nonexistent_source() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/nonexistent.txt", ctx.bucket))
        .send()
        .await;

    assert!(result.is_err(), "Copy from non-existent source should fail");
}

/// Test copy from non-existent bucket fails.
#[tokio::test]
async fn test_object_copy_nonexistent_source_bucket() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source("nonexistent-bucket/source.txt")
        .send()
        .await;

    assert!(result.is_err(), "Copy from non-existent bucket should fail");
}

/// Test copy to non-existent bucket fails.
#[tokio::test]
async fn test_object_copy_nonexistent_dest_bucket() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content").await;

    let result = ctx
        .client
        .copy_object()
        .bucket("nonexistent-bucket")
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await;

    assert!(result.is_err(), "Copy to non-existent bucket should fail");
}

/// Test copy overwrites existing object.
/// Ceph: test_object_copy_overwrite
#[tokio::test]
async fn test_object_copy_overwrite() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"new content").await;
    ctx.put("dest.txt", b"old content").await;

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy and overwrite");

    let data = ctx.get("dest.txt").await;
    assert_eq!(data.as_slice(), b"new content");
}

/// Test copy to same key (in-place copy).
#[tokio::test]
async fn test_object_copy_same_key() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("original", "value")
        .send()
        .await
        .expect("Should put object");

    // Copy to same key with new metadata
    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .copy_source(format!("{}/test.txt", ctx.bucket))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("new", "metadata")
        .send()
        .await
        .expect("Should copy in place");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("new"), Some(&"metadata".to_string()));
}

/// Test copy returns ETag.
#[tokio::test]
async fn test_object_copy_returns_etag() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content").await;

    let response = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let result = response.copy_object_result().unwrap();
    assert!(result.e_tag().is_some(), "Copy should return ETag");
}

/// Test copy large object.
#[tokio::test]
async fn test_object_copy_large() {
    let ctx = S3TestContext::new().await;

    let content = vec![b'x'; 5 * 1024 * 1024]; // 5 MB
    let body = ByteStream::from(content.clone());
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("large.bin")
        .body(body)
        .send()
        .await
        .expect("Should put large object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("large-copy.bin")
        .copy_source(format!("{}/large.bin", ctx.bucket))
        .send()
        .await
        .expect("Should copy large object");

    let response = ctx.head("large-copy.bin").await;
    assert_eq!(response.content_length(), Some(5 * 1024 * 1024));
}

/// Test copy with metadata directive REPLACE.
#[tokio::test]
async fn test_object_copy_metadata_replace() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(body)
        .metadata("original", "value")
        .content_type("text/plain")
        .send()
        .await
        .expect("Should put object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("replaced", "newvalue")
        .content_type("application/json")
        .send()
        .await
        .expect("Should copy with replaced metadata");

    let response = ctx.head("dest.txt").await;
    let metadata = response.metadata().unwrap();
    assert!(metadata.get("original").is_none(), "Original metadata should not be present");
    assert_eq!(metadata.get("replaced"), Some(&"newvalue".to_string()));
    assert_eq!(response.content_type(), Some("application/json"));
}

/// Test copy with conditional If-Match.
#[tokio::test]
async fn test_object_copy_if_match() {
    let ctx = S3TestContext::new().await;

    let put_response = ctx.put("source.txt", b"content").await;
    let etag = put_response.e_tag().unwrap().to_string();

    // Copy with matching ETag
    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .copy_source_if_match(&etag)
        .send()
        .await;

    assert!(result.is_ok(), "Copy with matching If-Match should succeed");
}

/// Test copy with conditional If-Match fails on mismatch.
#[tokio::test]
async fn test_object_copy_if_match_fails() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content").await;

    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .copy_source_if_match("\"wrong-etag\"")
        .send()
        .await;

    assert!(result.is_err(), "Copy with wrong If-Match should fail");
}

// =============================================================================
// Extended Copy Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test copy with If-None-Match condition.
/// Ceph: test_object_copy_if_none_match
#[tokio::test]
async fn test_object_copy_if_none_match() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content").await;

    // Copy with non-matching ETag should succeed
    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .copy_source_if_none_match("\"different-etag\"")
        .send()
        .await;

    assert!(result.is_ok(), "Copy with non-matching If-None-Match should succeed");
}

/// Test copy with If-None-Match matching ETag fails.
/// Ceph: test_object_copy_if_none_match_fails
#[tokio::test]
#[ignore = "Copy If-None-Match not implemented"]
async fn test_object_copy_if_none_match_fails() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("source.txt", b"content").await;
    let etag = put.e_tag().unwrap();

    let result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .copy_source_if_none_match(etag)
        .send()
        .await;

    assert!(result.is_err(), "Copy with matching If-None-Match should fail");
}

/// Test copy versioned object.
/// Ceph: test_object_copy_versioned
#[tokio::test]
#[ignore = "Copy versioned object not implemented"]
async fn test_object_copy_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("source.txt", b"version1").await;
    let _v2 = ctx.put("source.txt", b"version2").await;

    let vid1 = v1.version_id().unwrap();

    // Copy specific version
    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt?versionId={}", ctx.bucket, vid1))
        .send()
        .await
        .expect("Should copy specific version");

    let data = ctx.get("dest.txt").await;
    assert_eq!(data.as_slice(), b"version1");
}

/// Test copy to versioned bucket creates version.
/// Ceph: test_object_copy_to_versioned
#[tokio::test]
#[ignore = "Copy to versioned bucket not implemented"]
async fn test_object_copy_to_versioned_bucket() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("source.txt", b"content").await;

    let copy_result = ctx
        .client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    assert!(
        copy_result.version_id().is_some(),
        "Copy to versioned bucket should return version ID"
    );
}

/// Test copy preserves cache control.
/// Ceph: test_object_copy_cache_control
#[tokio::test]
async fn test_object_copy_preserves_cache_control() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(body)
        .cache_control("max-age=3600")
        .send()
        .await
        .expect("Should put object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let response = ctx.head("dest.txt").await;
    assert_eq!(response.cache_control(), Some("max-age=3600"));
}

/// Test copy preserves content encoding.
/// Ceph: test_object_copy_content_encoding
#[tokio::test]
async fn test_object_copy_preserves_content_encoding() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.gz")
        .body(body)
        .content_encoding("gzip")
        .send()
        .await
        .expect("Should put object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.gz")
        .copy_source(format!("{}/source.gz", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let response = ctx.head("dest.gz").await;
    assert_eq!(response.content_encoding(), Some("gzip"));
}

/// Test copy preserves content disposition.
/// Ceph: test_object_copy_content_disposition
#[tokio::test]
async fn test_object_copy_preserves_content_disposition() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(body)
        .content_disposition("attachment")
        .send()
        .await
        .expect("Should put object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let response = ctx.head("dest.txt").await;
    assert_eq!(response.content_disposition(), Some("attachment"));
}

/// Test copy with storage class.
/// Ceph: test_object_copy_storage_class
#[tokio::test]
async fn test_object_copy_storage_class() {
    use aws_sdk_s3::types::StorageClass;

    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content").await;

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .storage_class(StorageClass::Standard)
        .send()
        .await
        .expect("Should copy with storage class");

    let exists = ctx.exists("dest.txt").await;
    assert!(exists);
}

/// Test copy with tagging.
/// Ceph: test_object_copy_tagging
#[tokio::test]
#[ignore = "Copy with tagging not implemented"]
async fn test_object_copy_with_tagging() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content").await;

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .tagging("key1=value1&key2=value2")
        .tagging_directive(aws_sdk_s3::types::TaggingDirective::Replace)
        .send()
        .await
        .expect("Should copy with tagging");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .send()
        .await
        .expect("Should get tagging");

    assert_eq!(response.tag_set().len(), 2);
}

/// Test copy concurrent operations.
/// Ceph: test_object_copy_concurrent
#[tokio::test]
async fn test_object_copy_concurrent() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content for concurrent copy").await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .copy_object()
                .bucket(&bucket)
                .key(format!("copy-{}.txt", i))
                .copy_source(format!("{}/source.txt", bucket))
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    let list = ctx.list_objects().await;
    assert_eq!(list.contents().len(), 11); // source + 10 copies
}

/// Test copy empty object.
/// Ceph: test_object_copy_empty
#[tokio::test]
async fn test_object_copy_empty() {
    let ctx = S3TestContext::new().await;

    ctx.put("empty.txt", b"").await;

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("empty-copy.txt")
        .copy_source(format!("{}/empty.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy empty object");

    let response = ctx.head("empty-copy.txt").await;
    assert_eq!(response.content_length(), Some(0));
}

/// Test copy with special characters in key.
/// Ceph: test_object_copy_special_chars
#[tokio::test]
async fn test_object_copy_special_chars_key() {
    let ctx = S3TestContext::new().await;

    ctx.put("source with spaces.txt", b"content").await;

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest with spaces.txt")
        .copy_source(format!("{}/source with spaces.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object with special chars");

    let exists = ctx.exists("dest with spaces.txt").await;
    assert!(exists);
}
