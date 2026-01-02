//! Bucket lifecycle tests.
//!
//! These tests are for lifecycle policies which are not yet implemented in Rucket.
//! All tests are marked with #[ignore].
//!
//! Ported from:
//! - Ceph s3-tests: test_lifecycle_*

#![allow(dead_code)]

use crate::S3TestContext;

/// Test putting lifecycle configuration.
/// Ceph: test_lifecycle_set
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}

/// Test getting lifecycle configuration.
/// Ceph: test_lifecycle_get
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}

/// Test deleting lifecycle configuration.
/// Ceph: test_lifecycle_delete
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_delete() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}

/// Test lifecycle expiration rule.
/// Ceph: test_lifecycle_expiration
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_expiration() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}

/// Test lifecycle transition rule.
/// Ceph: test_lifecycle_transition
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_transition() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}

/// Test lifecycle with multiple rules.
/// Ceph: test_lifecycle_rules_multiple
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_multiple_rules() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}

/// Test lifecycle with prefix filter.
/// Ceph: test_lifecycle_prefix
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_prefix() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}

/// Test lifecycle non-current version expiration.
/// Ceph: test_lifecycle_noncur_expiration
#[tokio::test]
#[ignore = "Lifecycle not implemented"]
async fn test_bucket_lifecycle_noncurrent_expiration() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Lifecycle not implemented");
}
