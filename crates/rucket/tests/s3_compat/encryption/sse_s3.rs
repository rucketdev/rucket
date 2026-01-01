//! SSE-S3 encryption tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test SSE-S3 PUT.
#[tokio::test]
#[ignore = "SSE-S3 not implemented"]
async fn test_sse_s3_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-S3 not implemented");
}

/// Test SSE-S3 GET.
#[tokio::test]
#[ignore = "SSE-S3 not implemented"]
async fn test_sse_s3_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-S3 not implemented");
}

/// Test SSE-S3 copy.
#[tokio::test]
#[ignore = "SSE-S3 not implemented"]
async fn test_sse_s3_copy() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-S3 not implemented");
}

/// Test SSE-S3 multipart.
#[tokio::test]
#[ignore = "SSE-S3 not implemented"]
async fn test_sse_s3_multipart() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-S3 not implemented");
}
