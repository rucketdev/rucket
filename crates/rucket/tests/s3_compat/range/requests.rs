//! Range request tests.

use crate::S3TestContext;

/// Test range request first bytes.
#[tokio::test]
async fn test_range_first_bytes() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello, World!").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello");
}

/// Test range request last bytes.
#[tokio::test]
async fn test_range_last_bytes() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello, World!").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=-6")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"World!");
}

/// Test range request from offset to end.
#[tokio::test]
async fn test_range_offset_to_end() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello, World!").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=7-")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"World!");
}

/// Test range request middle bytes.
#[tokio::test]
async fn test_range_middle() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"0123456789").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=3-6")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"3456");
}

/// Test range returns 206 partial content.
#[tokio::test]
async fn test_range_returns_partial() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello, World!").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Should get range");

    // Content-Length should be the range size
    assert_eq!(response.content_length(), Some(5));
}

/// Test range returns content-range header.
#[tokio::test]
async fn test_range_content_range_header() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello, World!").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Should get range");

    assert!(response.content_range().is_some());
    let range = response.content_range().unwrap();
    assert!(range.contains("bytes 0-4/13"));
}

/// Test range single byte.
#[tokio::test]
async fn test_range_single_byte() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"ABC").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=1-1")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"B");
}

/// Test range entire file.
#[tokio::test]
async fn test_range_entire_file() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello");
}

/// Test range beyond file size.
#[tokio::test]
async fn test_range_beyond_end() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello").await;

    // Request beyond file size - should return up to end
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-100")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello");
}

/// Test invalid range.
#[tokio::test]
async fn test_range_invalid() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Hello").await;

    // Start beyond file size
    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=100-200")
        .send()
        .await;

    // Should return 416 Range Not Satisfiable
    assert!(result.is_err());
}

/// Test range on large file.
#[tokio::test]
async fn test_range_large_file() {
    let ctx = S3TestContext::new().await;

    let content = vec![b'x'; 1024 * 1024]; // 1MB
    ctx.put("large.bin", &content).await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("large.bin")
        .range("bytes=1000-1999")
        .send()
        .await
        .expect("Should get range");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.len(), 1000);
}

// =============================================================================
// Extended Range Request Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test multiple ranges in a single request.
/// Ceph: test_range_multiple
#[tokio::test]
#[ignore = "Multiple ranges not implemented"]
async fn test_range_multiple() {
    let _ctx = S3TestContext::new().await;
    // Would return multipart/byteranges response
}

/// Test range request on versioned object.
/// Ceph: test_range_versioned
#[tokio::test]
#[ignore = "Range on versioned object not fully implemented"]
async fn test_range_versioned() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"Hello, World!").await;
    let vid = put.version_id().unwrap();

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid)
        .range("bytes=0-4")
        .send()
        .await
        .expect("Should get range from version");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello");
}

/// Test range with If-Match.
/// Ceph: test_range_if_match
#[tokio::test]
async fn test_range_if_match() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"Hello, World!").await;
    let etag = put.e_tag().unwrap();

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-4")
        .if_match(etag)
        .send()
        .await
        .expect("Should get range with If-Match");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello");
}

/// Test range request preserves ETag.
/// Ceph: test_range_preserves_etag
#[tokio::test]
async fn test_range_preserves_etag() {
    let ctx = S3TestContext::new().await;

    let put = ctx.put("test.txt", b"Hello, World!").await;
    let put_etag = put.e_tag().unwrap();

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Should get range");

    let get_etag = response.e_tag().unwrap();
    assert_eq!(put_etag, get_etag);
}

/// Test range on empty file.
/// Ceph: test_range_empty_file
#[tokio::test]
async fn test_range_empty_file() {
    let ctx = S3TestContext::new().await;

    ctx.put("empty.txt", b"").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("empty.txt")
        .range("bytes=0-10")
        .send()
        .await;

    // Empty file range should fail
    assert!(result.is_err());
}

/// Test range start equals end.
/// Ceph: test_range_start_equals_end
#[tokio::test]
async fn test_range_start_equals_end() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"ABC").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=1-1")
        .send()
        .await
        .expect("Should get single byte");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"B");
}

/// Test range with zero offset.
/// Ceph: test_range_zero_offset
#[tokio::test]
async fn test_range_zero_offset() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"ABCDEF").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=0-0")
        .send()
        .await
        .expect("Should get first byte");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"A");
}

/// Test range last byte.
/// Ceph: test_range_last_byte
#[tokio::test]
async fn test_range_suffix_one() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"ABCDEF").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=-1")
        .send()
        .await
        .expect("Should get last byte");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"F");
}

/// Test range suffix larger than file.
/// Ceph: test_range_suffix_large
#[tokio::test]
async fn test_range_suffix_larger_than_file() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"ABC").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .range("bytes=-100")
        .send()
        .await
        .expect("Should get entire file");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"ABC");
}
