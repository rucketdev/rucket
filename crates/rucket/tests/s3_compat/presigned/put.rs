//! Presigned PUT URL tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test presigned PUT URL works.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_basic() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT with expiry.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_expiry() {
    let _ctx = S3TestContext::new().await;
}

/// Test expired presigned PUT fails.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_expired() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT with content type.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_content_type() {
    let _ctx = S3TestContext::new().await;
}
