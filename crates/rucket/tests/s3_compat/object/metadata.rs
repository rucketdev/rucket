//! Object metadata tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_metadata_*
//! - MinIO Mint: object metadata tests

use crate::S3TestContext;
use aws_sdk_s3::primitives::ByteStream;

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
    assert_eq!(
        metadata.get("x-custom-key"),
        Some(&"custom-value".to_string())
    );
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
    assert_eq!(
        metadata.get("key"),
        Some(&"value with spaces".to_string())
    );
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
