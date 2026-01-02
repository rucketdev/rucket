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

// =============================================================================
// Extended Bucket Tagging Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test putting tags with maximum value length.
/// Ceph: test_bucket_tagging_max_value_length
#[tokio::test]
async fn test_bucket_tagging_max_value_length() {
    let ctx = S3TestContext::new().await;

    // Max value length is 256 characters
    let long_value = "v".repeat(256);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("key").value(&long_value).build().unwrap())
        .build()
        .unwrap();

    let result = ctx.client.put_bucket_tagging().bucket(&ctx.bucket).tagging(tagging).send().await;

    assert!(result.is_ok(), "Should accept 256-char tag value");
}

/// Test putting tags with invalid key (too long).
/// Ceph: test_bucket_tagging_invalid_key
#[tokio::test]
#[ignore = "Tag key length validation not implemented"]
async fn test_bucket_tagging_key_too_long() {
    let ctx = S3TestContext::new().await;

    // Key longer than 128 chars should fail
    let too_long_key = "k".repeat(129);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key(&too_long_key).value("value").build().unwrap())
        .build()
        .unwrap();

    let result = ctx.client.put_bucket_tagging().bucket(&ctx.bucket).tagging(tagging).send().await;

    assert!(result.is_err(), "Should reject key longer than 128 chars");
}

/// Test putting tags with invalid value (too long).
/// Ceph: test_bucket_tagging_invalid_value
#[tokio::test]
#[ignore = "Tag value length validation not implemented"]
async fn test_bucket_tagging_value_too_long() {
    let ctx = S3TestContext::new().await;

    // Value longer than 256 chars should fail
    let too_long_value = "v".repeat(257);

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("key").value(&too_long_value).build().unwrap())
        .build()
        .unwrap();

    let result = ctx.client.put_bucket_tagging().bucket(&ctx.bucket).tagging(tagging).send().await;

    assert!(result.is_err(), "Should reject value longer than 256 chars");
}

/// Test bucket tagging on non-existent bucket.
/// Ceph: test_bucket_tagging_nonexistent
#[tokio::test]
async fn test_bucket_tagging_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("key").value("value").build().unwrap())
        .build()
        .unwrap();

    let result = ctx
        .client
        .put_bucket_tagging()
        .bucket("nonexistent-bucket-xyz")
        .tagging(tagging)
        .send()
        .await;

    assert!(result.is_err(), "Should fail for non-existent bucket");
}

/// Test bucket tagging replaces existing tags.
/// Ceph: test_bucket_tagging_replace
#[tokio::test]
#[ignore = "Bucket tagging storage not implemented"]
async fn test_bucket_tagging_replace() {
    let ctx = S3TestContext::new().await;

    // Put first set of tags
    let tagging1 = Tagging::builder()
        .tag_set(Tag::builder().key("env").value("dev").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging1)
        .send()
        .await
        .expect("Should put first tags");

    // Put second set (should replace)
    let tagging2 = Tagging::builder()
        .tag_set(Tag::builder().key("env").value("prod").build().unwrap())
        .tag_set(Tag::builder().key("team").value("backend").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging2)
        .send()
        .await
        .expect("Should put second tags");

    let response =
        ctx.client.get_bucket_tagging().bucket(&ctx.bucket).send().await.expect("Should get tags");

    assert_eq!(response.tag_set().len(), 2);
}

/// Test bucket tagging with unicode characters.
/// Ceph: test_bucket_tagging_unicode
#[tokio::test]
#[ignore = "Bucket tagging storage not implemented"]
async fn test_bucket_tagging_unicode() {
    let ctx = S3TestContext::new().await;

    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("名前").value("値").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging)
        .send()
        .await
        .expect("Should put unicode tags");

    let response =
        ctx.client.get_bucket_tagging().bucket(&ctx.bucket).send().await.expect("Should get tags");

    let tag = &response.tag_set()[0];
    assert_eq!(tag.key(), "名前");
    assert_eq!(tag.value(), "値");
}

/// Test bucket tagging with special characters.
/// Ceph: test_bucket_tagging_special_chars
#[tokio::test]
#[ignore = "Bucket tagging storage not implemented"]
async fn test_bucket_tagging_special_chars() {
    let ctx = S3TestContext::new().await;

    let tagging = Tagging::builder()
        .tag_set(
            Tag::builder().key("key-with-dash").value("value_with_underscore").build().unwrap(),
        )
        .tag_set(Tag::builder().key("key.with.dots").value("value:with:colons").build().unwrap())
        .build()
        .unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging)
        .send()
        .await
        .expect("Should put tags with special chars");
}

/// Test bucket tagging empty tag set.
/// Ceph: test_bucket_tagging_empty
#[tokio::test]
#[ignore = "Empty tag set validation not implemented"]
async fn test_bucket_tagging_empty_set() {
    let ctx = S3TestContext::new().await;

    let tagging = Tagging::builder().build().unwrap();

    let result = ctx.client.put_bucket_tagging().bucket(&ctx.bucket).tagging(tagging).send().await;

    // Empty tag set may be rejected or accepted depending on implementation
    // AWS rejects empty tag sets with MalformedXML
    if result.is_ok() {
        // If accepted, should clear tags
        let get_result = ctx.client.get_bucket_tagging().bucket(&ctx.bucket).send().await;
        // NoSuchTagSet is also acceptable
        if let Ok(r) = get_result {
            assert!(r.tag_set().is_empty());
        }
    }
}

/// Test bucket tagging maximum 50 tags.
/// Ceph: test_bucket_tagging_max_50
#[tokio::test]
#[ignore = "Bucket tagging storage not implemented"]
async fn test_bucket_tagging_max_50() {
    let ctx = S3TestContext::new().await;

    // S3 allows exactly 50 tags
    let mut tagging_builder = Tagging::builder();
    for i in 0..50 {
        tagging_builder = tagging_builder.tag_set(
            Tag::builder()
                .key(format!("key{:02}", i))
                .value(format!("value{}", i))
                .build()
                .unwrap(),
        );
    }
    let tagging = tagging_builder.build().unwrap();

    ctx.client
        .put_bucket_tagging()
        .bucket(&ctx.bucket)
        .tagging(tagging)
        .send()
        .await
        .expect("Should accept 50 tags");

    let response =
        ctx.client.get_bucket_tagging().bucket(&ctx.bucket).send().await.expect("Should get tags");

    assert_eq!(response.tag_set().len(), 50);
}

/// Test bucket tagging more than 50 tags fails.
/// Ceph: test_bucket_tagging_over_50
#[tokio::test]
#[ignore = "Tag count validation not implemented"]
async fn test_bucket_tagging_over_50_fails() {
    let ctx = S3TestContext::new().await;

    // More than 50 tags should fail
    let mut tagging_builder = Tagging::builder();
    for i in 0..51 {
        tagging_builder = tagging_builder.tag_set(
            Tag::builder()
                .key(format!("key{:02}", i))
                .value(format!("value{}", i))
                .build()
                .unwrap(),
        );
    }
    let tagging = tagging_builder.build().unwrap();

    let result = ctx.client.put_bucket_tagging().bucket(&ctx.bucket).tagging(tagging).send().await;

    assert!(result.is_err(), "Should reject more than 50 tags");
}
