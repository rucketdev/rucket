//! Object metadata tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_metadata_*
//! - MinIO Mint: object metadata tests

use aws_sdk_s3::primitives::ByteStream;

use crate::S3TestContext;

/// Test putting object with user metadata.
/// Ceph: test_object_set_get_metadata
#[tokio::test]
async fn test_object_metadata_put_get() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("x-custom-key", "custom-value")
        .send()
        .await
        .expect("Should put object with metadata");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("x-custom-key"), Some(&"custom-value".to_string()));
}

/// Test putting object with multiple metadata entries.
#[tokio::test]
async fn test_object_metadata_multiple() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("key1", "value1")
        .metadata("key2", "value2")
        .metadata("key3", "value3")
        .send()
        .await
        .expect("Should put object with multiple metadata");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("key1"), Some(&"value1".to_string()));
    assert_eq!(metadata.get("key2"), Some(&"value2".to_string()));
    assert_eq!(metadata.get("key3"), Some(&"value3".to_string()));
}

/// Test metadata persists after overwrite with same metadata.
#[tokio::test]
async fn test_object_metadata_overwrite_preserve() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content1");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("key", "value1")
        .send()
        .await
        .expect("Should put object");

    // Overwrite without metadata - metadata should NOT be preserved
    let body = ByteStream::from_static(b"content2");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Should overwrite object");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata();
    // After overwrite without metadata, old metadata should be gone
    assert!(
        metadata.is_none() || metadata.unwrap().get("key").is_none(),
        "Metadata should not persist after overwrite without metadata"
    );
}

/// Test metadata with special characters in value.
#[tokio::test]
async fn test_object_metadata_special_chars() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("key", "value with spaces")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("key"), Some(&"value with spaces".to_string()));
}

/// Test metadata with long value.
#[tokio::test]
async fn test_object_metadata_long_value() {
    let ctx = S3TestContext::new().await;

    let long_value = "x".repeat(1000);

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("key", &long_value)
        .send()
        .await
        .expect("Should put object with long metadata value");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("key"), Some(&long_value));
}

/// Test metadata case handling.
#[tokio::test]
async fn test_object_metadata_case() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("MyKey", "MyValue")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    // Metadata keys are typically lowercased by S3
    assert!(
        metadata.get("mykey").is_some() || metadata.get("MyKey").is_some(),
        "Metadata should be retrievable"
    );
}

/// Test empty metadata value.
#[tokio::test]
async fn test_object_metadata_empty_value() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("empty-key", "")
        .send()
        .await
        .expect("Should put object with empty metadata value");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("empty-key"), Some(&"".to_string()));
}

/// Test metadata via GET response.
#[tokio::test]
async fn test_object_metadata_in_get() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("key", "value")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.get_object("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("key"), Some(&"value".to_string()));
}

/// Test many metadata entries.
#[tokio::test]
async fn test_object_metadata_many_entries() {
    let ctx = S3TestContext::new().await;

    let mut builder = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"content"));

    // Add 10 metadata entries
    for i in 0..10 {
        builder = builder.metadata(format!("key{}", i), format!("value{}", i));
    }

    builder.send().await.expect("Should put object with many metadata");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.len(), 10);
}

// =============================================================================
// Extended Metadata Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test metadata preserved in copy.
/// Ceph: test_object_metadata_copy
#[tokio::test]
async fn test_object_metadata_preserved_in_copy() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(body)
        .metadata("original", "value")
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
    assert_eq!(metadata.get("original"), Some(&"value".to_string()));
}

/// Test metadata replaced in copy with REPLACE directive.
/// Ceph: test_object_metadata_copy_replace
#[tokio::test]
async fn test_object_metadata_replaced_in_copy() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(body)
        .metadata("original", "value")
        .send()
        .await
        .expect("Should put object");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .metadata_directive(aws_sdk_s3::types::MetadataDirective::Replace)
        .metadata("new", "metadata")
        .send()
        .await
        .expect("Should copy with replaced metadata");

    let response = ctx.head("dest.txt").await;
    let metadata = response.metadata().unwrap();
    assert!(metadata.get("original").is_none());
    assert_eq!(metadata.get("new"), Some(&"metadata".to_string()));
}

/// Test metadata with numeric values.
/// Ceph: test_object_metadata_numeric
#[tokio::test]
async fn test_object_metadata_numeric_values() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("count", "12345")
        .metadata("version", "1.2.3")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("count"), Some(&"12345".to_string()));
    assert_eq!(metadata.get("version"), Some(&"1.2.3".to_string()));
}

/// Test metadata with dashes and underscores in key.
/// Ceph: test_object_metadata_key_chars
#[tokio::test]
async fn test_object_metadata_key_special_chars() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("key-with-dashes", "value1")
        .metadata("key_with_underscores", "value2")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("key-with-dashes"), Some(&"value1".to_string()));
    assert_eq!(metadata.get("key_with_underscores"), Some(&"value2".to_string()));
}

/// Test metadata on versioned object.
/// Ceph: test_object_metadata_versioned
#[tokio::test]
async fn test_object_metadata_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    // Put v1 with metadata
    let body = ByteStream::from_static(b"v1");
    let v1 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("version", "1")
        .send()
        .await
        .expect("Should put v1");

    // Put v2 with different metadata
    let body = ByteStream::from_static(b"v2");
    let v2 = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("version", "2")
        .send()
        .await
        .expect("Should put v2");

    // Get v1 metadata
    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(v1.version_id().unwrap())
        .send()
        .await
        .expect("Should head v1");

    assert_eq!(response.metadata().unwrap().get("version"), Some(&"1".to_string()));

    // Get v2 metadata
    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(v2.version_id().unwrap())
        .send()
        .await
        .expect("Should head v2");

    assert_eq!(response.metadata().unwrap().get("version"), Some(&"2".to_string()));
}

/// Test metadata concurrent updates.
/// Ceph: test_object_metadata_concurrent
#[tokio::test]
async fn test_object_metadata_concurrent_updates() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .put_object()
                .bucket(&bucket)
                .key("test.txt")
                .body(ByteStream::from(format!("content{}", i).into_bytes()))
                .metadata("iteration", format!("{}", i))
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

/// Test metadata total size limit.
/// Ceph: test_object_metadata_size_limit
#[tokio::test]
async fn test_object_metadata_size_limit() {
    let ctx = S3TestContext::new().await;

    // Total metadata header size should be under 2KB typically
    let value = "x".repeat(500);

    let body = ByteStream::from_static(b"content");
    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("key1", &value)
        .metadata("key2", &value)
        .metadata("key3", &value)
        .send()
        .await;

    assert!(result.is_ok(), "Should accept metadata within size limit");
}

/// Test metadata keys are lowercased.
/// Ceph: test_object_metadata_lowercase
#[tokio::test]
async fn test_object_metadata_keys_lowercased() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("UPPERCASE", "value")
        .metadata("MixedCase", "value")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.head("test.txt").await;
    let metadata = response.metadata().unwrap();

    // Keys should be lowercase
    let keys: Vec<&str> = metadata.keys().map(|s| s.as_str()).collect();
    assert!(keys.iter().all(|k| *k == k.to_lowercase()));
}
