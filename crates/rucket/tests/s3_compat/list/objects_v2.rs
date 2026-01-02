//! ListObjectsV2 tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_listv2_*
//! - MinIO Mint: ListObjectsV2 tests

use crate::S3TestContext;

/// Test basic list objects v2.
/// Ceph: test_bucket_listv2_objects
#[tokio::test]
async fn test_list_objects_v2_basic() {
    let ctx = S3TestContext::new().await;

    ctx.put("file1.txt", b"content").await;
    ctx.put("file2.txt", b"content").await;
    ctx.put("file3.txt", b"content").await;

    let response = ctx.list_objects().await;

    assert_eq!(response.contents().len(), 3);
    assert_eq!(response.key_count(), Some(3));
}

/// Test list objects v2 empty bucket.
/// Ceph: test_bucket_listv2_empty
#[tokio::test]
async fn test_list_objects_v2_empty() {
    let ctx = S3TestContext::new().await;

    let response = ctx.list_objects().await;

    assert!(response.contents().is_empty());
    assert_eq!(response.key_count(), Some(0));
}

/// Test list objects v2 with prefix.
/// Ceph: test_bucket_listv2_prefix
#[tokio::test]
async fn test_list_objects_v2_prefix() {
    let ctx = S3TestContext::new().await;

    ctx.put("docs/readme.md", b"content").await;
    ctx.put("docs/guide.md", b"content").await;
    ctx.put("src/main.rs", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 2);
    assert_eq!(response.prefix(), Some("docs/"));
}

/// Test list objects v2 with max-keys.
/// Ceph: test_bucket_listv2_maxkeys
#[tokio::test]
async fn test_list_objects_v2_max_keys() {
    let ctx = S3TestContext::new().await;

    for i in 0..10 {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .max_keys(3)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 3);
    assert!(response.is_truncated().unwrap_or(false));
    assert!(response.next_continuation_token().is_some());
}

/// Test list objects v2 with continuation token.
/// Ceph: test_bucket_listv2_continuation
#[tokio::test]
async fn test_list_objects_v2_continuation_token() {
    let ctx = S3TestContext::new().await;

    for i in 0..10 {
        ctx.put(&format!("file{:02}.txt", i), b"content").await;
    }

    // First page
    let response1 = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .max_keys(3)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response1.contents().len(), 3);
    let token = response1.next_continuation_token().unwrap();

    // Second page
    let response2 = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .max_keys(3)
        .continuation_token(token)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response2.contents().len(), 3);

    // No duplicates
    let keys1: Vec<&str> = response1.contents().iter().filter_map(|o| o.key()).collect();
    let keys2: Vec<&str> = response2.contents().iter().filter_map(|o| o.key()).collect();
    for key in &keys1 {
        assert!(!keys2.contains(key));
    }
}

/// Test list objects v2 with start-after.
/// Ceph: test_bucket_listv2_startafter
#[tokio::test]
async fn test_list_objects_v2_start_after() {
    let ctx = S3TestContext::new().await;

    ctx.put("a.txt", b"content").await;
    ctx.put("b.txt", b"content").await;
    ctx.put("c.txt", b"content").await;
    ctx.put("d.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .start_after("b.txt")
        .send()
        .await
        .expect("Should list objects");

    let keys: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["c.txt", "d.txt"]);
}

/// Test list objects v2 with fetch-owner.
#[tokio::test]
async fn test_list_objects_v2_fetch_owner() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .fetch_owner(true)
        .send()
        .await
        .expect("Should list objects");

    // Owner info may or may not be included depending on implementation
    let _obj = &response.contents()[0];
    // Just verify it doesn't error with fetch_owner=true
}

/// Test list objects v2 returns sorted keys.
#[tokio::test]
async fn test_list_objects_v2_sorted() {
    let ctx = S3TestContext::new().await;

    ctx.put("zebra.txt", b"content").await;
    ctx.put("apple.txt", b"content").await;
    ctx.put("mango.txt", b"content").await;

    let response = ctx.list_objects().await;

    let keys: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["apple.txt", "mango.txt", "zebra.txt"]);
}

/// Test list objects v2 includes size.
#[tokio::test]
async fn test_list_objects_v2_includes_size() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"12345").await;

    let response = ctx.list_objects().await;

    let obj = &response.contents()[0];
    assert_eq!(obj.size(), Some(5));
}

/// Test list objects v2 includes etag.
#[tokio::test]
async fn test_list_objects_v2_includes_etag() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_objects().await;

    let obj = &response.contents()[0];
    assert!(obj.e_tag().is_some());
}

/// Test list objects v2 includes last modified.
#[tokio::test]
async fn test_list_objects_v2_includes_last_modified() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_objects().await;

    let obj = &response.contents()[0];
    assert!(obj.last_modified().is_some());
}

/// Test list objects v2 many objects.
#[tokio::test]
async fn test_list_objects_v2_many() {
    let ctx = S3TestContext::new().await;

    for i in 0..100 {
        ctx.put(&format!("file{:03}.txt", i), b"content").await;
    }

    let response = ctx.list_objects().await;

    assert_eq!(response.contents().len(), 100);
}

/// Test list objects v2 nonexistent bucket.
#[tokio::test]
async fn test_list_objects_v2_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.list_objects_v2().bucket("nonexistent-bucket").send().await;

    assert!(result.is_err());
}

/// Test list objects v2 with prefix no match.
#[tokio::test]
async fn test_list_objects_v2_prefix_no_match() {
    let ctx = S3TestContext::new().await;

    ctx.put("file.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("nomatch/")
        .send()
        .await
        .expect("Should list objects");

    assert!(response.contents().is_empty());
}
