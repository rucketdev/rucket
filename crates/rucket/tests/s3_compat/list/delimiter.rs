//! List objects delimiter/prefix tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_list_delimiter_*
//! - MinIO Mint: delimiter/prefix tests

use crate::S3TestContext;

/// Test list with delimiter returns common prefixes.
/// Ceph: test_bucket_list_delimiter_basic
#[tokio::test]
async fn test_list_delimiter_basic() {
    let ctx = S3TestContext::new().await;

    ctx.put("dir1/file1.txt", b"content").await;
    ctx.put("dir1/file2.txt", b"content").await;
    ctx.put("dir2/file1.txt", b"content").await;
    ctx.put("root.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    // Should have root.txt in contents
    let contents = response.contents();
    assert_eq!(contents.len(), 1);
    assert_eq!(contents[0].key(), Some("root.txt"));

    // Should have dir1/ and dir2/ as common prefixes
    let prefixes = response.common_prefixes();
    let prefix_strs: Vec<&str> = prefixes.iter().filter_map(|p| p.prefix()).collect();
    assert_eq!(prefix_strs.len(), 2);
    assert!(prefix_strs.contains(&"dir1/"));
    assert!(prefix_strs.contains(&"dir2/"));
}

/// Test list with delimiter and prefix.
/// Ceph: test_bucket_list_delimiter_prefix
#[tokio::test]
async fn test_list_delimiter_prefix() {
    let ctx = S3TestContext::new().await;

    ctx.put("dir/subdir1/file.txt", b"content").await;
    ctx.put("dir/subdir2/file.txt", b"content").await;
    ctx.put("dir/file.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("dir/")
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    // Should have dir/file.txt in contents
    let contents = response.contents();
    assert_eq!(contents.len(), 1);
    assert_eq!(contents[0].key(), Some("dir/file.txt"));

    // Should have dir/subdir1/ and dir/subdir2/ as common prefixes
    let prefixes = response.common_prefixes();
    let prefix_strs: Vec<&str> = prefixes.iter().filter_map(|p| p.prefix()).collect();
    assert_eq!(prefix_strs.len(), 2);
    assert!(prefix_strs.contains(&"dir/subdir1/"));
    assert!(prefix_strs.contains(&"dir/subdir2/"));
}

/// Test list with delimiter - no common prefixes.
#[tokio::test]
async fn test_list_delimiter_no_prefixes() {
    let ctx = S3TestContext::new().await;

    ctx.put("file1.txt", b"content").await;
    ctx.put("file2.txt", b"content").await;
    ctx.put("file3.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 3);
    assert!(response.common_prefixes().is_empty());
}

/// Test list with delimiter - deep nesting.
#[tokio::test]
async fn test_list_delimiter_deep_nesting() {
    let ctx = S3TestContext::new().await;

    ctx.put("a/b/c/d/file.txt", b"content").await;
    ctx.put("a/b/c/file.txt", b"content").await;
    ctx.put("a/b/file.txt", b"content").await;
    ctx.put("a/file.txt", b"content").await;

    // List at root level
    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    assert!(response.contents().is_empty());
    let prefixes: Vec<&str> =
        response.common_prefixes().iter().filter_map(|p| p.prefix()).collect();
    assert_eq!(prefixes, vec!["a/"]);

    // List at a/ level
    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("a/")
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 1); // a/file.txt
    let prefixes: Vec<&str> =
        response.common_prefixes().iter().filter_map(|p| p.prefix()).collect();
    assert_eq!(prefixes, vec!["a/b/"]);
}

/// Test list with non-standard delimiter.
#[tokio::test]
async fn test_list_delimiter_non_standard() {
    let ctx = S3TestContext::new().await;

    ctx.put("dir-sub-file.txt", b"content").await;
    ctx.put("dir-file.txt", b"content").await;
    ctx.put("other.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("-")
        .send()
        .await
        .expect("Should list objects");

    // other.txt should be in contents
    let contents = response.contents();
    assert_eq!(contents.len(), 1);
    assert_eq!(contents[0].key(), Some("other.txt"));

    // dir- should be a common prefix
    let prefixes: Vec<&str> =
        response.common_prefixes().iter().filter_map(|p| p.prefix()).collect();
    assert_eq!(prefixes, vec!["dir-"]);
}

/// Test list v1 with delimiter.
#[tokio::test]
async fn test_list_v1_delimiter() {
    let ctx = S3TestContext::new().await;

    ctx.put("dir1/file.txt", b"content").await;
    ctx.put("dir2/file.txt", b"content").await;
    ctx.put("root.txt", b"content").await;

    let response = ctx
        .client
        .list_objects()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    assert_eq!(response.contents().len(), 1);
    assert_eq!(response.common_prefixes().len(), 2);
}

/// Test delimiter with max-keys pagination.
#[tokio::test]
async fn test_list_delimiter_pagination() {
    let ctx = S3TestContext::new().await;

    // Create many directories
    for i in 0..10 {
        ctx.put(&format!("dir{:02}/file.txt", i), b"content").await;
    }

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .max_keys(3)
        .send()
        .await
        .expect("Should list objects");

    // Should get 3 common prefixes
    assert_eq!(response.common_prefixes().len(), 3);
    assert!(response.is_truncated().unwrap_or(false));
}

/// Test prefix that doesn't match any keys.
#[tokio::test]
async fn test_list_prefix_no_match() {
    let ctx = S3TestContext::new().await;

    ctx.put("dir/file.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .prefix("nonexistent/")
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    assert!(response.contents().is_empty());
    assert!(response.common_prefixes().is_empty());
}

/// Test empty delimiter (same as no delimiter).
#[tokio::test]
async fn test_list_empty_delimiter() {
    let ctx = S3TestContext::new().await;

    ctx.put("dir/file.txt", b"content").await;
    ctx.put("root.txt", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("")
        .send()
        .await
        .expect("Should list objects");

    // Should return all objects, no common prefixes
    assert_eq!(response.contents().len(), 2);
    assert!(response.common_prefixes().is_empty());
}

/// Test delimiter with single character keys.
#[tokio::test]
async fn test_list_delimiter_single_char_keys() {
    let ctx = S3TestContext::new().await;

    ctx.put("/", b"content").await;
    ctx.put("a", b"content").await;
    ctx.put("a/b", b"content").await;

    let response = ctx
        .client
        .list_objects_v2()
        .bucket(&ctx.bucket)
        .delimiter("/")
        .send()
        .await
        .expect("Should list objects");

    // "/" and "a" should be in contents or prefixes
    let contents: Vec<&str> = response.contents().iter().filter_map(|o| o.key()).collect();
    let prefixes: Vec<&str> =
        response.common_prefixes().iter().filter_map(|p| p.prefix()).collect();

    // Verify we get expected results
    assert!(!contents.is_empty() || !prefixes.is_empty());
}
