//! Bucket ACL tests.
//!
//! These tests are for ACL (Access Control List) features which are not yet
//! implemented in Rucket. All tests are marked with #[ignore].
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_acl_*

#![allow(dead_code)]

use crate::S3TestContext;

/// Test getting bucket ACL.
/// Ceph: test_bucket_acl_default
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_get_default() {
    let _ctx = S3TestContext::new().await;
    // TODO: Implement when ACL support is added
    unimplemented!("ACL support not implemented");
}

/// Test putting bucket ACL.
/// Ceph: test_bucket_acl_put
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL with canned ACL private.
/// Ceph: test_bucket_acl_canned_private
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_canned_private() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL with canned ACL public-read.
/// Ceph: test_bucket_acl_canned_publicread
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_canned_public_read() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL with canned ACL public-read-write.
/// Ceph: test_bucket_acl_canned_publicreadwrite
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_canned_public_read_write() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL with canned ACL authenticated-read.
/// Ceph: test_bucket_acl_canned_authenticatedread
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_canned_authenticated_read() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL grant read.
/// Ceph: test_bucket_acl_grant_read
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_grant_read() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL grant write.
/// Ceph: test_bucket_acl_grant_write
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_grant_write() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL grant read-acp.
/// Ceph: test_bucket_acl_grant_readacp
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_grant_read_acp() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL grant write-acp.
/// Ceph: test_bucket_acl_grant_writeacp
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_grant_write_acp() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}

/// Test bucket ACL grant full-control.
/// Ceph: test_bucket_acl_grant_fullcontrol
#[tokio::test]
#[ignore = "ACL not implemented"]
async fn test_bucket_acl_grant_full_control() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("ACL support not implemented");
}
