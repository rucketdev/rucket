//! ListObjects V1 tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_list_*
//! - MinIO Mint: ListObjects tests

use crate::S3TestContext;

/// Test basic list objects v1.
/// Ceph: test_bucket_list_objects
#[tokio::test]
async fn test_list_objects_v1_basic() {
    let ctx = S3TestContext::new().await;

    ctx.put("file1.txt", b"content").await;
    ctx.put("file2.txt", b"content").await;
    ctx.put("file3.txt", b"content").await;

    let response =
        ctx.client.list_objects().bucket(&ctx.bucket).send().await.expect("Should list objects");

    let contents = response.contents();
    assert_eq!(contents.len(), 3);
}

/// Test list objects v1 empty bucket.
/// Ceph: test_bucket_list_empty
#[tokio::test]
async fn test_list_objects_v1_empty() {
    let ctx = S3TestContext::new().await;

    let response =
        ctx.client.list_objects().bucket(&ctx.bucket).send().await.expect("Should list objects");

    assert!(response.contents().is_empty());
}

/// Test list objects v1 with prefix.
/// Ceph: test_bucket_list_prefix
#[tokio::test]
async fn test_list_objects_v1_prefix() {
    let ctx = S3TestContext::new().await;

    ctx.put("docs/readme.md", b"content").await;
    ctx.put("docs/guide.md", b"content").await;
    ctx.put("src/main.rs", b"content").await;

    let response = ctx
        .client
        .list_objects()
        .bucket(&ctx.bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("Should list objects");

    let contents = response.contents();
    assert_eq!(contents.len(), 2);
    for obj in contents {
        assert!(obj.key().unwrap().starts_with("docs/"));
    }
}

/// Test list objects v1 with max-keys.
/// Ceph: test_bucket_list_maxkeys
#[tokio::test]
async fn test_list_objects_v1_max_keys() {
    let ctx = S3TestContext::new().await;

    for i in 0..10 {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    let response = ctx
        .client
        .list_objects()
        .bucket(&ctx.bucket)
        .max_keys(3)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 3);
    assert!(response.is_truncated().unwrap_or(false));
}

/// Test list objects v1 with marker.
/// Ceph: test_bucket_list_marker
#[tokio::test]
async fn test_list_objects_v1_marker() {
    let ctx = S3TestContext::new().await;

    ctx.put("a.txt", b"content").await;
    ctx.put("b.txt", b"content").await;
    ctx.put("c.txt", b"content").await;
    ctx.put("d.txt", b"content").await;

    let response = ctx
        .client
        .list_objects()
        .bucket(&ctx.bucket)
        .marker("b.txt")
        .send()
        .await
        .expect("Should list objects");

    let keys: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["c.txt", "d.txt"]);
}

/// Test list objects v1 returns sorted keys.
/// Ceph: test_bucket_list_ordered
#[tokio::test]
async fn test_list_objects_v1_sorted() {
    let ctx = S3TestContext::new().await;

    // Create in random order
    ctx.put("zebra.txt", b"content").await;
    ctx.put("apple.txt", b"content").await;
    ctx.put("mango.txt", b"content").await;

    let response =
        ctx.client.list_objects().bucket(&ctx.bucket).send().await.expect("Should list objects");

    let keys: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["apple.txt", "mango.txt", "zebra.txt"]);
}

/// Test list objects v1 includes size.
#[tokio::test]
async fn test_list_objects_v1_includes_size() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"12345").await;

    let response =
        ctx.client.list_objects().bucket(&ctx.bucket).send().await.expect("Should list objects");

    let obj = &response.contents()[0];
    assert_eq!(obj.size(), Some(5));
}

/// Test list objects v1 includes etag.
#[tokio::test]
async fn test_list_objects_v1_includes_etag() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response =
        ctx.client.list_objects().bucket(&ctx.bucket).send().await.expect("Should list objects");

    let obj = &response.contents()[0];
    assert!(obj.e_tag().is_some());
}

/// Test list objects v1 includes last modified.
#[tokio::test]
async fn test_list_objects_v1_includes_last_modified() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response =
        ctx.client.list_objects().bucket(&ctx.bucket).send().await.expect("Should list objects");

    let obj = &response.contents()[0];
    assert!(obj.last_modified().is_some());
}

/// Test list objects v1 pagination.
#[tokio::test]
async fn test_list_objects_v1_pagination() {
    let ctx = S3TestContext::new().await;

    for i in 0..10 {
        ctx.put(&format!("file{:02}.txt", i), b"content").await;
    }

    // First page
    let response1 = ctx
        .client
        .list_objects()
        .bucket(&ctx.bucket)
        .max_keys(3)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response1.contents().len(), 3);
    assert!(response1.is_truncated().unwrap_or(false));

    // Second page using marker
    let marker = response1.contents().last().unwrap().key().unwrap();
    let response2 = ctx
        .client
        .list_objects()
        .bucket(&ctx.bucket)
        .max_keys(3)
        .marker(marker)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response2.contents().len(), 3);

    // No duplicate keys between pages
    let keys1: Vec<&str> = response1.contents().iter().filter_map(|o| o.key()).collect();
    let keys2: Vec<&str> = response2.contents().iter().filter_map(|o| o.key()).collect();
    for key in &keys1 {
        assert!(!keys2.contains(key));
    }
}

/// Test list objects v1 nonexistent bucket.
#[tokio::test]
async fn test_list_objects_v1_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.list_objects().bucket("nonexistent-bucket").send().await;

    assert!(result.is_err());
}
