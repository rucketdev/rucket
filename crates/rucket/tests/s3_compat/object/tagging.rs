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
