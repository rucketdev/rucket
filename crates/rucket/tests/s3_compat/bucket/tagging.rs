//! Bucket tagging tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_tagging_*
//! - MinIO Mint: bucket tagging tests

use aws_sdk_s3::types::{Tag, Tagging};

use crate::S3TestContext;

/// Test putting and getting bucket tags.
/// Ceph: test_set_bucket_tagging
#[tokio::test]
#[ignore = "Bucket tagging storage not implemented"]
async fn test_bucket_tagging_put_get() {
    let ctx = S3TestContext::new().await;

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("env").value("test").build().unwrap())
        .tag_set(Tag::builder().key("project").value("rucket").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging)
        .send()
        .await
        .expect("Should put bucket tagging");

    let response = ctx
        .client
        .get_bucket_tagging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get bucket tagging");

    let tags = response.tag_set();
    assert_eq!(tags.len(), 2, "Should have 2 tags");

    let tag_map: std::collections::HashMap<&str, &str> =
        tags.iter().map(|t| (t.key(), t.value())).collect();

    assert_eq!(tag_map.get("env"), Some(&"test"));
    assert_eq!(tag_map.get("project"), Some(&"rucket"));
}

/// Test deleting bucket tags.
/// Ceph: test_delete_bucket_tagging
#[tokio::test]
async fn test_bucket_tagging_delete() {
    let ctx = S3TestContext::new().await;

    // First put some tags
    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("env").value("test").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging)
        .send()
        .await
        .expect("Should put bucket tagging");

    // Delete tags
    ctx.client
        .delete_bucket_tagging()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should delete bucket tagging");

    // Get tags should fail or return empty
    let result = ctx.client.get_bucket_tagging().bucket(&ctx.bucket).send().await;

    // Either returns empty tag set or NoSuchTagSet error
    match result {
        Ok(response) => {
            assert!(response.tag_set().is_empty(), "Tags should be empty after delete");
        }
        Err(e) => {
            let error_str = format!("{:?}", e);
            assert!(error_str.contains("NoSuchTagSet"), "Expected NoSuchTagSet error");
        }
    }
}

/// Test getting tags on bucket with no tags.
/// Ceph: test_get_bucket_tagging_empty
#[tokio::test]
async fn test_bucket_tagging_get_empty() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.get_bucket_tagging().bucket(&ctx.bucket).send().await;

    // Should return NoSuchTagSet or empty tag set
    match result {
        Ok(response) => {
            assert!(response.tag_set().is_empty(), "Should have no tags");
        }
        Err(e) => {
            let error_str = format!("{:?}", e);
            assert!(
                error_str.contains("NoSuchTagSet"),
                "Expected NoSuchTagSet error, got: {}",
                error_str
            );
        }
    }
}

/// Test putting tags with maximum key length.
#[tokio::test]
async fn test_bucket_tagging_max_key_length() {
    let ctx = S3TestContext::new().await;

    // Max key length is 128 characters
    let long_key = "k".repeat(128);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key(&long_key).value("value").build().unwrap())
        .build()
        .unwrap();

    let result = ctx.client.put_bucket_tagging().bucket(&ctx.bucket).tagging(tagging).send().await;

    assert!(result.is_ok(), "Should accept 128-char tag key");
}

/// Test putting many tags.
#[tokio::test]
#[ignore = "Bucket tagging storage not implemented"]
async fn test_bucket_tagging_many_tags() {
    let ctx = S3TestContext::new().await;

    // S3 allows up to 50 tags per bucket
    let mut tagging_builder = Tagging::builder();
    for i in 0..10 {
        tagging_builder = tagging_builder.tag_set(
            Tag::builder().key(format!("key{}", i)).value(format!("value{}", i)).build().unwrap(),
        );
    }
    let tagging = tagging_builder.build().unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging)
        .send()
        .await
        .expect("Should put many tags");

    let response =
        ctx.client.get_bucket_tagging().bucket(&ctx.bucket).send().await.expect("Should get tags");

    assert_eq!(response.tag_set().len(), 10, "Should have 10 tags");
}
