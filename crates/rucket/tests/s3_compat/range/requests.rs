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
