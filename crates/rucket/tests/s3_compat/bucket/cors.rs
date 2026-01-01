//! Bucket CORS tests.
//!
//! These tests are for CORS (Cross-Origin Resource Sharing) which is not yet
//! implemented in Rucket. All tests are marked with #[ignore].
//!
//! Ported from:
//! - Ceph s3-tests: test_cors_*

#![allow(dead_code)]

use crate::S3TestContext;

/// Test putting CORS configuration.
/// Ceph: test_set_cors
#[tokio::test]
#[ignore = "CORS not implemented"]
async fn test_bucket_cors_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("CORS not implemented");
}

/// Test getting CORS configuration.
/// Ceph: test_get_cors
#[tokio::test]
#[ignore = "CORS not implemented"]
async fn test_bucket_cors_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("CORS not implemented");
}

/// Test deleting CORS configuration.
/// Ceph: test_delete_cors
#[tokio::test]
#[ignore = "CORS not implemented"]
async fn test_bucket_cors_delete() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("CORS not implemented");
}

/// Test CORS preflight request.
/// Ceph: test_cors_origin_response
#[tokio::test]
#[ignore = "CORS not implemented"]
async fn test_bucket_cors_preflight() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("CORS not implemented");
}

/// Test CORS with wildcard origin.
/// Ceph: test_cors_origin_wildcard
#[tokio::test]
#[ignore = "CORS not implemented"]
async fn test_bucket_cors_wildcard_origin() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("CORS not implemented");
}
