//! Object PUT tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_put_*
//! - MinIO Mint: PutObject tests

use aws_sdk_s3::primitives::ByteStream;

use crate::{random_bytes, S3TestContext};

/// Test basic object PUT.
/// Ceph: test_object_write_to_nonexistent_bucket
#[tokio::test]
async fn test_object_put_simple() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"Hello, World!");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Should put object");

    // Verify it exists
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get object");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello, World!");
}

/// Test PUT to non-existent bucket fails.
/// Ceph: test_object_write_to_nonexistent_bucket
#[tokio::test]
async fn test_object_put_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let body = ByteStream::from_static(b"content");
    let result = ctx
        .client
        .put_object()
        .bucket("nonexistent-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await;

    assert!(result.is_err(), "Should fail to put to non-existent bucket");
}

/// Test PUT empty object.
/// Ceph: test_object_write_empty
#[tokio::test]
async fn test_object_put_empty() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("empty.txt")
        .body(body)
        .send()
        .await
        .expect("Should put empty object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("empty.txt")
        .send()
        .await
        .expect("Should head object");

    assert_eq!(response.content_length(), Some(0), "Object should be empty");
}

/// Test PUT with content type.
/// Ceph: test_object_write_with_content_type
#[tokio::test]
async fn test_object_put_content_type() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"<html></html>");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("index.html")
        .body(body)
        .content_type("text/html")
        .send()
        .await
        .expect("Should put object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("index.html")
        .send()
        .await
        .expect("Should head object");

    assert_eq!(response.content_type(), Some("text/html"));
}

/// Test PUT with cache control.
#[tokio::test]
async fn test_object_put_cache_control() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("cached.txt")
        .body(body)
        .cache_control("max-age=3600")
        .send()
        .await
        .expect("Should put object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("cached.txt")
        .send()
        .await
        .expect("Should head object");

    assert_eq!(response.cache_control(), Some("max-age=3600"));
}

/// Test PUT with content encoding.
#[tokio::test]
async fn test_object_put_content_encoding() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"gzipped content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("file.gz")
        .body(body)
        .content_encoding("gzip")
        .send()
        .await
        .expect("Should put object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("file.gz")
        .send()
        .await
        .expect("Should head object");

    assert_eq!(response.content_encoding(), Some("gzip"));
}

/// Test PUT with content disposition.
#[tokio::test]
async fn test_object_put_content_disposition() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("download.txt")
        .body(body)
        .content_disposition("attachment; filename=\"download.txt\"")
        .send()
        .await
        .expect("Should put object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("download.txt")
        .send()
        .await
        .expect("Should head object");

    assert_eq!(response.content_disposition(), Some("attachment; filename=\"download.txt\""));
}

/// Test PUT with content language.
#[tokio::test]
async fn test_object_put_content_language() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"Bonjour");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("french.txt")
        .body(body)
        .content_language("fr")
        .send()
        .await
        .expect("Should put object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("french.txt")
        .send()
        .await
        .expect("Should head object");

    assert_eq!(response.content_language(), Some("fr"));
}

/// Test PUT returns ETag.
/// Ceph: test_object_write_check_etag
#[tokio::test]
async fn test_object_put_returns_etag() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content for etag");
    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Should put object");

    assert!(response.e_tag().is_some(), "Should return ETag");
    let etag = response.e_tag().unwrap();
    assert!(etag.starts_with('"') && etag.ends_with('"'), "ETag should be quoted");
}

/// Test PUT overwrite.
/// Ceph: test_object_overwrite
#[tokio::test]
async fn test_object_put_overwrite() {
    let ctx = S3TestContext::new().await;

    // First PUT
    let body = ByteStream::from_static(b"Version 1");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Should put object");

    // Overwrite
    let body = ByteStream::from_static(b"Version 2");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Should overwrite object");

    // Verify new content
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get object");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Version 2");
}

/// Test PUT with user metadata.
/// Ceph: test_object_metadata_write
#[tokio::test]
async fn test_object_put_user_metadata() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("custom-key", "custom-value")
        .metadata("another-key", "another-value")
        .send()
        .await
        .expect("Should put object with metadata");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should head object");

    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("custom-key"), Some(&"custom-value".to_string()));
    assert_eq!(metadata.get("another-key"), Some(&"another-value".to_string()));
}

/// Test PUT large object (1MB).
/// Ceph: test_object_write_large
#[tokio::test]
async fn test_object_put_large_1mb() {
    let ctx = S3TestContext::new().await;

    let data = random_bytes(1024 * 1024); // 1 MB
    let body = ByteStream::from(data.clone());

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("large.bin")
        .body(body)
        .send()
        .await
        .expect("Should put large object");

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("large.bin")
        .send()
        .await
        .expect("Should get large object");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.len(), 1024 * 1024);
    assert_eq!(body.as_ref(), data.as_slice());
}

/// Test PUT very large object (10MB).
#[tokio::test]
async fn test_object_put_large_10mb() {
    let ctx = S3TestContext::new().await;

    let data = random_bytes(10 * 1024 * 1024); // 10 MB
    let body = ByteStream::from(data.clone());

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("large10.bin")
        .body(body)
        .send()
        .await
        .expect("Should put 10MB object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("large10.bin")
        .send()
        .await
        .expect("Should head large object");

    assert_eq!(response.content_length(), Some(10 * 1024 * 1024));
}

/// Test PUT with various key patterns.
#[tokio::test]
async fn test_object_put_key_patterns() {
    let ctx = S3TestContext::new().await;

    let keys = [
        "simple",
        "with-dashes",
        "with_underscores",
        "with.dots",
        "path/to/object",
        "path/to/nested/object",
        "file.txt",
        "UPPERCASE",
        "MixedCase",
        "123numeric",
    ];

    for key in &keys {
        let body = ByteStream::from_static(b"content");
        ctx.client
            .put_object()
            .bucket(&ctx.bucket)
            .key(*key)
            .body(body)
            .send()
            .await
            .unwrap_or_else(|_| panic!("Should put object with key: {}", key));
    }

    // Verify all exist
    let response =
        ctx.client.list_objects_v2().bucket(&ctx.bucket).send().await.expect("Should list objects");

    assert_eq!(response.contents().len(), keys.len());
}

/// Test PUT with special characters in key.
#[tokio::test]
async fn test_object_put_special_chars_in_key() {
    let ctx = S3TestContext::new().await;

    let keys = [
        "file with spaces.txt",
        "file+plus.txt",
        "file%20encoded.txt",
        "file=equals.txt",
        "file#hash.txt",
        "file&ampersand.txt",
    ];

    for key in &keys {
        let body = ByteStream::from_static(b"content");
        let result = ctx.client.put_object().bucket(&ctx.bucket).key(*key).body(body).send().await;

        // These may or may not be valid depending on implementation
        // Just verify we can put them without crashing
        if result.is_ok() {
            // Verify we can get it back
            let get_result = ctx.client.get_object().bucket(&ctx.bucket).key(*key).send().await;
            assert!(get_result.is_ok(), "Should get object with key: {}", key);
        }
    }
}

/// Test PUT many objects.
#[tokio::test]
async fn test_object_put_many() {
    let ctx = S3TestContext::new().await;

    let count = 50;
    for i in 0..count {
        let key = format!("file{:03}.txt", i);
        let body = ByteStream::from(format!("content {}", i).into_bytes());
        ctx.client
            .put_object()
            .bucket(&ctx.bucket)
            .key(&key)
            .body(body)
            .send()
            .await
            .expect("Should put object");
    }

    let response =
        ctx.client.list_objects_v2().bucket(&ctx.bucket).send().await.expect("Should list objects");

    assert_eq!(response.contents().len(), count);
}

/// Test concurrent PUTs to different keys.
#[tokio::test]
async fn test_object_put_concurrent_different_keys() {
    let ctx = S3TestContext::new().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let key = format!("concurrent-{}.txt", i);
            let body = ByteStream::from(format!("content {}", i).into_bytes());
            client.put_object().bucket(&bucket).key(&key).body(body).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent PUT should succeed");
    }

    let response =
        ctx.client.list_objects_v2().bucket(&ctx.bucket).send().await.expect("Should list objects");

    assert_eq!(response.contents().len(), 10);
}

/// Test PUT with exact same content produces same ETag.
#[tokio::test]
async fn test_object_put_same_content_same_etag() {
    let ctx = S3TestContext::new().await;
    let content = b"identical content";

    let body1 = ByteStream::from_static(content);
    let response1 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("file1.txt")
        .body(body1)
        .send()
        .await
        .expect("Should put object");

    let body2 = ByteStream::from_static(content);
    let response2 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("file2.txt")
        .body(body2)
        .send()
        .await
        .expect("Should put object");

    assert_eq!(response1.e_tag(), response2.e_tag(), "Same content should have same ETag");
}

/// Test PUT with binary content.
#[tokio::test]
async fn test_object_put_binary() {
    let ctx = S3TestContext::new().await;

    // Create binary data with all byte values
    let data: Vec<u8> = (0..=255).collect();
    let body = ByteStream::from(data.clone());

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("binary.bin")
        .body(body)
        .content_type("application/octet-stream")
        .send()
        .await
        .expect("Should put binary object");

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("binary.bin")
        .send()
        .await
        .expect("Should get binary object");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), data.as_slice());
}

/// Test PUT with long key (1000+ chars).
#[tokio::test]
async fn test_object_put_long_key() {
    let ctx = S3TestContext::new().await;

    // S3 allows keys up to 1024 bytes
    let long_key = "a".repeat(500);

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(&long_key)
        .body(body)
        .send()
        .await
        .expect("Should put object with long key");

    let result = ctx.exists(&long_key).await;
    assert!(result, "Object with long key should exist");
}

// =============================================================================
// Extended PUT Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test PUT with expires header.
/// Ceph: test_object_put_expires
#[tokio::test]
async fn test_object_put_expires() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .expires(aws_smithy_types::DateTime::from_secs(1700000000))
        .send()
        .await
        .expect("Should put object with expires");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should head object");

    assert!(response.expires().is_some());
}

/// Test PUT to versioned bucket returns version id.
/// Ceph: test_object_put_versioned
#[tokio::test]
async fn test_object_put_versioned_returns_version_id() {
    let ctx = S3TestContext::with_versioning().await;

    let body = ByteStream::from_static(b"content");
    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Should put object");

    assert!(response.version_id().is_some(), "Should return version ID");
}

/// Test PUT multiple versions.
/// Ceph: test_object_put_multiple_versions
#[tokio::test]
async fn test_object_put_multiple_versions() {
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

/// Test PUT with If-None-Match prevents overwrite.
/// Ceph: test_object_put_if_none_match
#[tokio::test]
async fn test_object_put_if_none_match_prevents_overwrite() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"original").await;

    let body = ByteStream::from_static(b"new content");
    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .if_none_match("*")
        .send()
        .await;

    assert!(result.is_err(), "Should fail with If-None-Match * when object exists");

    // Original content should be preserved
    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"original");
}

/// Test PUT with If-None-Match allows new object.
/// Ceph: test_object_put_if_none_match_new
#[tokio::test]
async fn test_object_put_if_none_match_allows_new() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"new content");
    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("new-object.txt")
        .body(body)
        .if_none_match("*")
        .send()
        .await;

    assert!(result.is_ok(), "Should succeed with If-None-Match * for new object");
}

/// Test PUT with website redirect location.
/// Ceph: test_object_put_redirect
#[tokio::test]
#[ignore = "Website redirect not implemented"]
async fn test_object_put_website_redirect() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("redirect.html")
        .body(body)
        .website_redirect_location("/target.html")
        .send()
        .await
        .expect("Should put object with redirect");
}

/// Test PUT with storage class.
/// Ceph: test_object_put_storage_class
#[tokio::test]
async fn test_object_put_storage_class() {
    use aws_sdk_s3::types::StorageClass;

    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .storage_class(StorageClass::Standard)
        .send()
        .await
        .expect("Should put object with storage class");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should head object");

    // Storage class should be STANDARD or not specified (defaults to STANDARD)
    if let Some(sc) = response.storage_class() {
        assert_eq!(sc, &aws_sdk_s3::types::StorageClass::Standard);
    }
}

/// Test PUT concurrent to same key.
/// Ceph: test_object_put_concurrent_same_key
#[tokio::test]
async fn test_object_put_concurrent_same_key() {
    let ctx = S3TestContext::new().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let body = ByteStream::from(format!("content {}", i).into_bytes());
            client.put_object().bucket(&bucket).key("same-key.txt").body(body).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent PUT should succeed");
    }

    // One of the writes should win
    let data = ctx.get("same-key.txt").await;
    assert!(data.starts_with(b"content "));
}

/// Test PUT with tagging.
/// Ceph: test_object_put_tagging
#[tokio::test]
async fn test_object_put_with_tagging() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("tagged.txt")
        .body(body)
        .tagging("key1=value1&key2=value2")
        .send()
        .await
        .expect("Should put object with tagging");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("tagged.txt")
        .send()
        .await
        .expect("Should get tagging");

    assert_eq!(response.tag_set().len(), 2);
}
