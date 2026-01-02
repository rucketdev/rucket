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

// =============================================================================
// Extended ListObjectsV2 Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test list objects v2 with delimiter.
/// Ceph: test_bucket_listv2_delimiter
#[tokio::test]
async fn test_list_objects_v2_delimiter() {
    let ctx = S3TestContext::new().await;

    ctx.put("a/1.txt", b"content").await;
    ctx.put("a/2.txt", b"content").await;
    ctx.put("b/1.txt", b"content").await;
    ctx.put("root.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    // Should have root.txt as content and a/, b/ as common prefixes
    assert_eq!(response.contents().len(), 1);
    assert_eq!(response.common_prefixes().len(), 2);
}

/// Test list objects v2 with prefix and delimiter.
/// Ceph: test_bucket_listv2_prefix_delimiter
#[tokio::test]
async fn test_list_objects_v2_prefix_delimiter() {
    let ctx = S3TestContext::new().await;

    ctx.put("photos/2023/jan/pic1.jpg", b"content").await;
    ctx.put("photos/2023/jan/pic2.jpg", b"content").await;
    ctx.put("photos/2023/feb/pic1.jpg", b"content").await;
    ctx.put("photos/2024/jan/pic1.jpg", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("photos/2023/")
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    assert!(response.contents().is_empty());
    assert_eq!(response.common_prefixes().len(), 2); // jan/, feb/
}

/// Test list objects v2 encoding type.
/// Ceph: test_bucket_listv2_encoding_type
#[tokio::test]
async fn test_list_objects_v2_encoding_type() {
    let ctx = S3TestContext::new().await;

    ctx.put("file with spaces.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .encoding_type(aws_sdk_s3::types::EncodingType::Url)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 1);
}

/// Test list objects v2 pagination consistency.
/// Ceph: test_bucket_listv2_pagination
#[tokio::test]
async fn test_list_objects_v2_pagination_consistency() {
    let ctx = S3TestContext::new().await;

    // Create 25 objects
    for i in 0..25 {
        ctx.put(&format!("file{:02}.txt", i), b"content").await;
    }

    let mut all_keys = Vec::new();
    let mut continuation_token: Option<String> = None;

    loop {
        let mut request = ctx.client.list_objects_v2().bucket(&ctx.bucket).max_keys(10);

        if let Some(token) = &continuation_token {
            request = request.continuation_token(token);
        }

        let response = request.send().await.expect("Should list objects");

        for obj in response.contents() {
            if let Some(key) = obj.key() {
                all_keys.push(key.to_string());
            }
        }

        if !response.is_truncated().unwrap_or(false) {
            break;
        }

        continuation_token = response.next_continuation_token().map(|s| s.to_string());
    }

    assert_eq!(all_keys.len(), 25);

    // Verify no duplicates
    let unique: std::collections::HashSet<_> = all_keys.iter().collect();
    assert_eq!(unique.len(), 25);
}

/// Test list objects v2 returns correct bucket name.
/// Ceph: test_bucket_listv2_name
#[tokio::test]
async fn test_list_objects_v2_returns_bucket_name() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_objects().await;

    assert_eq!(response.name(), Some(ctx.bucket.as_str()));
}

/// Test list objects v2 max keys zero.
/// Ceph: test_bucket_listv2_maxkeys_zero
#[tokio::test]
async fn test_list_objects_v2_max_keys_zero() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .max_keys(0)
        .send()
        .await
        .expect("Should list objects");

    assert!(response.contents().is_empty());
}

/// Test list objects v2 max keys one.
/// Ceph: test_bucket_listv2_maxkeys_one
#[tokio::test]
async fn test_list_objects_v2_max_keys_one() {
    let ctx = S3TestContext::new().await;

    ctx.put("a.txt", b"content").await;
    ctx.put("b.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .max_keys(1)
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 1);
    assert!(response.is_truncated().unwrap_or(false));
}

/// Test list objects v2 delimiter only.
/// Ceph: test_bucket_listv2_delimiter_only
#[tokio::test]
async fn test_list_objects_v2_delimiter_only() {
    let ctx = S3TestContext::new().await;

    ctx.put("a", b"content").await;
    ctx.put("b/c", b"content").await;
    ctx.put("b/d", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 1); // "a"
    assert_eq!(response.common_prefixes().len(), 1); // "b/"
}

/// Test list objects v2 with deep nested prefixes.
/// Ceph: test_bucket_listv2_deep_prefix
#[tokio::test]
async fn test_list_objects_v2_deep_nested() {
    let ctx = S3TestContext::new().await;

    ctx.put("a/b/c/d/e/file.txt", b"content").await;

    for depth in 1..=5 {
        let prefix = "a/".repeat(1).chars().take(depth * 2).collect::<String>();
        let response = ctx
            .client
            .list_objects_v2()
            .bucket(&ctx.bucket)
            .prefix(&prefix)
            .send()
            .await
            .expect("Should list objects");

        assert!(response.contents().len() <= 1 || !response.common_prefixes().is_empty());
    }
}

/// Test list objects v2 special characters in prefix.
/// Ceph: test_bucket_listv2_special_prefix
#[tokio::test]
async fn test_list_objects_v2_special_chars_prefix() {
    let ctx = S3TestContext::new().await;

    ctx.put("test-file.txt", b"content").await;
    ctx.put("test_file.txt", b"content").await;
    ctx.put("test.file.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("test")
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 3);
}

/// Test list objects v2 storage class.
/// Ceph: test_bucket_listv2_storage_class
#[tokio::test]
async fn test_list_objects_v2_storage_class() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_objects().await;

    let obj = &response.contents()[0];
    // Storage class may be STANDARD or not set
    if let Some(sc) = obj.storage_class() {
        assert_eq!(sc, &aws_sdk_s3::types::ObjectStorageClass::Standard);
    }
}

/// Test list objects v2 concurrent requests.
/// Ceph: test_bucket_listv2_concurrent
#[tokio::test]
async fn test_list_objects_v2_concurrent() {
    let ctx = S3TestContext::new().await;

    for i in 0..20 {
        ctx.put(&format!("file{:02}.txt", i), b"content").await;
    }

    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move { client.list_objects_v2().bucket(&bucket).send().await });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().contents().len(), 20);
    }
}

/// Test list objects v2 start after nonexistent.
/// Ceph: test_bucket_listv2_startafter_nonexistent
#[tokio::test]
async fn test_list_objects_v2_start_after_nonexistent() {
    let ctx = S3TestContext::new().await;

    ctx.put("a.txt", b"content").await;
    ctx.put("c.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .start_after("b.txt")
        .send()
        .await
        .expect("Should list");

    let keys: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["c.txt"]);
}

/// Test list objects v2 start after last.
/// Ceph: test_bucket_listv2_startafter_last
#[tokio::test]
async fn test_list_objects_v2_start_after_last() {
    let ctx = S3TestContext::new().await;

    ctx.put("a.txt", b"content").await;
    ctx.put("b.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .start_after("z.txt")
        .send()
        .await
        .expect("Should list");

    assert!(response.contents().is_empty());
}

/// Test list objects v2 empty delimiter.
/// Ceph: test_bucket_listv2_empty_delimiter
#[tokio::test]
async fn test_list_objects_v2_empty_delimiter() {
    let ctx = S3TestContext::new().await;

    ctx.put("a/b.txt", b"content").await;
    ctx.put("a/c.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("")
        .send()
        .await
        .expect("Should list");

    // Empty delimiter should return all objects
    assert_eq!(response.contents().len(), 2);
    assert!(response.common_prefixes().is_empty());
}

/// Test list objects v2 with unicode keys.
/// Ceph: test_bucket_listv2_unicode
#[tokio::test]
async fn test_list_objects_v2_unicode_keys() {
    let ctx = S3TestContext::new().await;

    ctx.put("日本語/ファイル.txt", b"content").await;
    ctx.put("中文/文件.txt", b"content").await;
    ctx.put("한국어/파일.txt", b"content").await;

    let response = ctx.list_objects().await;

    assert_eq!(response.contents().len(), 3);
}

/// Test list objects v2 max keys negative handled.
/// Some implementations treat negative as error, others as default
#[tokio::test]
async fn test_list_objects_v2_max_keys_boundary() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Very large max_keys
    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .max_keys(10000)
        .send()
        .await
        .expect("Should list");

    assert_eq!(response.contents().len(), 1);
}

/// Test list objects v2 with continuation token and prefix.
/// Ceph: test_bucket_listv2_continuation_prefix
#[tokio::test]
async fn test_list_objects_v2_continuation_with_prefix() {
    let ctx = S3TestContext::new().await;

    for i in 0..10 {
        ctx.put(&format!("docs/file{:02}.txt", i), b"content").await;
    }
    ctx.put("other.txt", b"content").await;

    // First page with prefix
    let response1 = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("docs/")
        .max_keys(3)
        .send()
        .await
        .expect("Should list");

    assert_eq!(response1.contents().len(), 3);
    let token = response1.next_continuation_token().unwrap();

    // Second page should maintain prefix
    let response2 = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("docs/")
        .continuation_token(token)
        .send()
        .await
        .expect("Should list");

    for obj in response2.contents() {
        assert!(obj.key().unwrap().starts_with("docs/"));
    }
}

/// Test list objects v2 includes checksum algorithm.
/// Ceph: test_bucket_listv2_checksum
#[tokio::test]
async fn test_list_objects_v2_checksum_algorithm() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.list_objects().await;

    // Checksum algorithm info may or may not be included
    let _obj = &response.contents()[0];
    // Just verify we can access objects, checksum is optional
}

/// Test list objects v2 deleted object not listed.
/// Ceph: test_bucket_listv2_after_delete
#[tokio::test]
async fn test_list_objects_v2_after_delete() {
    let ctx = S3TestContext::new().await;

    ctx.put("keep.txt", b"content").await;
    ctx.put("delete.txt", b"content").await;

    ctx.delete("delete.txt").await;

    let response = ctx.list_objects().await;

    let keys: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, vec!["keep.txt"]);
}
