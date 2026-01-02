//! Object tagging tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_tagging_*
//! - MinIO Mint: object tagging tests

use aws_sdk_s3::types::{Tag, Tagging};

use crate::S3TestContext;

/// Test putting and getting object tags.
/// Ceph: test_put_object_tagging
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_put_get() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("env").value("test").build().unwrap())
        .tag_set(Tag::builder().key("project").value("rucket").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await
        .expect("Should put object tagging");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get object tagging");

    let tags = response.tag_set();
    assert_eq!(tags.len(), 2, "Should have 2 tags");

    let tag_map: std::collections::HashMap<&str, &str> =
        tags.iter().map(|t| (t.key(), t.value())).collect();

    assert_eq!(tag_map.get("env"), Some(&"test"));
    assert_eq!(tag_map.get("project"), Some(&"rucket"));
}

/// Test deleting object tags.
/// Ceph: test_delete_object_tagging
#[tokio::test]
async fn test_object_tagging_delete() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Put tags
    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("env").value("test").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await
        .expect("Should put object tagging");

    // Delete tags
    ctx.client
        .delete_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should delete object tagging");

    // Get tags should return empty
    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get object tagging");

    assert!(response.tag_set().is_empty(), "Tags should be empty after delete");
}

/// Test getting tags on object with no tags.
#[tokio::test]
async fn test_object_tagging_get_empty() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get object tagging");

    assert!(response.tag_set().is_empty(), "Should have no tags");
}

/// Test tagging on non-existent object fails.
#[tokio::test]
async fn test_object_tagging_nonexistent_object() {
    let ctx = S3TestContext::new().await;

    let result =
        ctx.client.get_object_tagging().bucket(&ctx.bucket).key("nonexistent.txt").send().await;

    assert!(result.is_err(), "Should fail on non-existent object");
}

/// Test putting many tags.
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_many_tags() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // S3 allows up to 10 tags per object
    let mut tagging_builder = Tagging::builder();
    for i in 0..10 {
        tagging_builder = tagging_builder.tag_set(
            Tag::builder().key(format!("key{}", i)).value(format!("value{}", i)).build().unwrap(),
        );
    }
    let tagging = tagging_builder.build().unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await
        .expect("Should put many tags");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get tags");

    assert_eq!(response.tag_set().len(), 10, "Should have 10 tags");
}

/// Test replacing object tags.
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_replace() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Put initial tags
    let tagging1 = Tagging::builder()
        .tag_set(Tag::builder().key("old").value("value").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging1)
        .send()
        .await
        .expect("Should put initial tags");

    // Replace with new tags
    let tagging2 = Tagging::builder()
        .tag_set(Tag::builder().key("new").value("value").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging2)
        .send()
        .await
        .expect("Should replace tags");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get tags");

    let tags = response.tag_set();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0].key(), "new");
}

/// Test tag key max length.
#[tokio::test]
async fn test_object_tagging_max_key_length() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Max key length is 128 characters
    let long_key = "k".repeat(128);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key(&long_key).value("value").build().unwrap())
        .build()
        .unwrap();

    let result = ctx
        .client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await;

    assert!(result.is_ok(), "Should accept 128-char tag key");
}

/// Test tag value max length.
#[tokio::test]
async fn test_object_tagging_max_value_length() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Max value length is 256 characters
    let long_value = "v".repeat(256);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("key").value(&long_value).build().unwrap())
        .build()
        .unwrap();

    let result = ctx
        .client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await;

    assert!(result.is_ok(), "Should accept 256-char tag value");
}

// =============================================================================
// Extended Object Tagging Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test tagging on versioned object.
/// Ceph: test_object_tagging_versioned
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"v1").await;
    let v2 = ctx.put("test.txt", b"v2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    // Tag v1
    let tagging1 = Tagging::builder()
        .tag_set(Tag::builder().key("version").value("1").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .tagging(tagging1)
        .send()
        .await
        .expect("Should tag v1");

    // Tag v2 differently
    let tagging2 = Tagging::builder()
        .tag_set(Tag::builder().key("version").value("2").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .tagging(tagging2)
        .send()
        .await
        .expect("Should tag v2");

    // Get v1 tags
    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1 tags");

    assert_eq!(response.tag_set()[0].value(), "1");

    // Get v2 tags
    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2 tags");

    assert_eq!(response.tag_set()[0].value(), "2");
}

/// Test tagging with special characters in value.
/// Ceph: test_object_tagging_special_chars
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_special_chars_value() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("key").value("value with spaces").build().unwrap())
        .tag_set(Tag::builder().key("dash-key").value("dash-value").build().unwrap())
        .tag_set(Tag::builder().key("under_key").value("under_value").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await
        .expect("Should put tags with special chars");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get tags");

    assert_eq!(response.tag_set().len(), 3);
}

/// Test concurrent tagging operations.
/// Ceph: test_object_tagging_concurrent
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_concurrent() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let tagging = Tagging::builder()
                .tag_set(Tag::builder().key("iteration").value(format!("{}", i)).build().unwrap())
                .build()
                .unwrap();

            client
                .put_object_tagging()
                .bucket(&bucket)
                .key("test.txt")
                .tagging(tagging)
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

/// Test put object with tagging header.
/// Ceph: test_put_object_with_tagging
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_put_with_tagging() {
    use aws_sdk_s3::primitives::ByteStream;

    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(ByteStream::from_static(b"content"))
        .tagging("key1=value1&key2=value2")
        .send()
        .await
        .expect("Should put object with tagging");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .expect("Should get tags");

    assert_eq!(response.tag_set().len(), 2);
}

/// Test tagging key too long.
/// Ceph: test_object_tagging_key_too_long
#[tokio::test]
async fn test_object_tagging_key_too_long() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Key over 128 characters should fail
    let too_long_key = "k".repeat(129);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key(&too_long_key).value("value").build().unwrap())
        .build()
        .unwrap();

    let result = ctx
        .client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await;

    assert!(result.is_err(), "Should reject tag key > 128 chars");
}

/// Test tagging value too long.
/// Ceph: test_object_tagging_value_too_long
#[tokio::test]
async fn test_object_tagging_value_too_long() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Value over 256 characters should fail
    let too_long_value = "v".repeat(257);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("key").value(&too_long_value).build().unwrap())
        .build()
        .unwrap();

    let result = ctx
        .client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await;

    assert!(result.is_err(), "Should reject tag value > 256 chars");
}

/// Test too many tags.
/// Ceph: test_object_tagging_too_many
#[tokio::test]
async fn test_object_tagging_too_many() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // S3 allows max 10 tags, trying 11 should fail
    let mut tagging_builder = Tagging::builder();
    for i in 0..11 {
        tagging_builder = tagging_builder.tag_set(
            Tag::builder().key(format!("key{}", i)).value(format!("value{}", i)).build().unwrap(),
        );
    }
    let tagging = tagging_builder.build().unwrap();

    let result = ctx
        .client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .tagging(tagging)
        .send()
        .await;

    assert!(result.is_err(), "Should reject more than 10 tags");
}

/// Test delete tagging on object with no tags.
/// Ceph: test_object_delete_tagging_empty
#[tokio::test]
async fn test_object_tagging_delete_empty() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Delete tagging on object with no tags should succeed
    let result =
        ctx.client.delete_object_tagging().bucket(&ctx.bucket).key("test.txt").send().await;

    assert!(result.is_ok(), "Delete tagging on untagged object should succeed");
}

/// Test tagging with empty key fails.
/// Ceph: test_object_tagging_empty_key
#[tokio::test]
async fn test_object_tagging_empty_key() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Empty key should fail at builder level or server side
    let tag_result = Tag::builder().key("").value("value").build();

    // The builder itself might reject empty keys
    if let Ok(tag) = tag_result {
        let tagging = Tagging::builder().tag_set(tag).build().unwrap();

        let result = ctx
            .client
            .put_object_tagging()
            .bucket(&ctx.bucket)
            .key("test.txt")
            .tagging(tagging)
            .send()
            .await;

        assert!(result.is_err(), "Should reject empty tag key");
    }
}

/// Test copy object preserves tags by default.
/// Ceph: test_object_copy_preserves_tags
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_preserved_in_copy() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"content").await;

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("original").value("tag").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .tagging(tagging)
        .send()
        .await
        .expect("Should tag source");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .send()
        .await
        .expect("Should get dest tags");

    assert_eq!(response.tag_set().len(), 1);
    assert_eq!(response.tag_set()[0].key(), "original");
}

/// Test tagging on deep path key.
/// Ceph: test_object_tagging_deep_path
#[tokio::test]
#[ignore = "Object tagging storage not implemented"]
async fn test_object_tagging_deep_path() {
    let ctx = S3TestContext::new().await;

    let key = "a/b/c/d/deep.txt";
    ctx.put(key, b"content").await;

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("deep").value("path").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_object_tagging()
        .bucket(&ctx.bucket)
        .key(key)
        .tagging(tagging)
        .send()
        .await
        .expect("Should tag deep path object");

    let response = ctx
        .client
        .get_object_tagging()
        .bucket(&ctx.bucket)
        .key(key)
        .send()
        .await
        .expect("Should get tags");

    assert_eq!(response.tag_set().len(), 1);
}
