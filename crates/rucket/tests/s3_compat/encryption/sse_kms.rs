//! SSE-KMS encryption tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test SSE-KMS PUT.
#[tokio::test]
#[ignore = "SSE-KMS not implemented"]
async fn test_sse_kms_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-KMS not implemented");
}

/// Test SSE-KMS GET.
#[tokio::test]
#[ignore = "SSE-KMS not implemented"]
async fn test_sse_kms_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-KMS not implemented");
}

/// Test SSE-KMS with key ID.
#[tokio::test]
#[ignore = "SSE-KMS not implemented"]
async fn test_sse_kms_key_id() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-KMS not implemented");
}

/// Test SSE-KMS copy.
#[tokio::test]
#[ignore = "SSE-KMS not implemented"]
async fn test_sse_kms_copy() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("SSE-KMS not implemented");
}
