//! Bucket policy tests.
//!
//! These tests are for bucket policies which are not yet implemented in Rucket.
//! All tests are marked with #[ignore].
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_policy_*

#![allow(dead_code)]

use crate::S3TestContext;

/// Test putting a bucket policy.
/// Ceph: test_bucket_policy_put
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}

/// Test getting a bucket policy.
/// Ceph: test_bucket_policy_get
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}

/// Test deleting a bucket policy.
/// Ceph: test_bucket_policy_delete
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_delete() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}

/// Test bucket policy with conditions.
/// Ceph: test_bucket_policy_condition
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_condition() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}

/// Test bucket policy Allow.
/// Ceph: test_bucket_policy_allow
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_allow() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}

/// Test bucket policy Deny.
/// Ceph: test_bucket_policy_deny
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_deny() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}

/// Test bucket policy with public access.
/// Ceph: test_bucket_policy_public
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_public() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}

/// Test bucket policy with IP condition.
/// Ceph: test_bucket_policy_ip
#[tokio::test]
#[ignore = "Bucket policy not implemented"]
async fn test_bucket_policy_ip_condition() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket policy not implemented");
}
