//! SSE-C (customer-provided key) encryption tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test SSE-C PUT.
#[tokio::test]
#[ignore = "SSE-C not implemented"]
async fn test_sse_c_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-C not implemented");
}

/// Test SSE-C GET requires key.
#[tokio::test]
#[ignore = "SSE-C not implemented"]
async fn test_sse_c_get_requires_key() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-C not implemented");
}

/// Test SSE-C wrong key fails.
#[tokio::test]
#[ignore = "SSE-C not implemented"]
async fn test_sse_c_wrong_key() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-C not implemented");
}

/// Test SSE-C copy.
#[tokio::test]
#[ignore = "SSE-C not implemented"]
async fn test_sse_c_copy() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-C not implemented");
}

/// Test SSE-C multipart.
#[tokio::test]
#[ignore = "SSE-C not implemented"]
async fn test_sse_c_multipart() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-C not implemented");
}
