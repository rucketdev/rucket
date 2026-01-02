//! Presigned POST (browser upload) tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test POST object policy.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_presigned_post_policy() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

/// Test POST object with conditions.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_presigned_post_conditions() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

/// Test POST object expires.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_presigned_post_expiry() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}
