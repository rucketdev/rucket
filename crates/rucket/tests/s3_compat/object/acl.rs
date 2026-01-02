//! Object ACL tests.
//!
//! These tests are for ACL features which are not yet implemented in Rucket.
//! All tests are marked with #[ignore].

#![allow(dead_code)]

use crate::S3TestContext;

/// Test getting object ACL.
#[tokio::test]
#[ignore = "Object ACL not implemented"]
async fn test_object_acl_get_default() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object ACL not implemented");
}

/// Test putting object ACL.
#[tokio::test]
#[ignore = "Object ACL not implemented"]
async fn test_object_acl_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object ACL not implemented");
}

/// Test object ACL canned private.
#[tokio::test]
#[ignore = "Object ACL not implemented"]
async fn test_object_acl_canned_private() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object ACL not implemented");
}

/// Test object ACL canned public-read.
#[tokio::test]
#[ignore = "Object ACL not implemented"]
async fn test_object_acl_canned_public_read() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object ACL not implemented");
}

/// Test object ACL grant read.
#[tokio::test]
#[ignore = "Object ACL not implemented"]
async fn test_object_acl_grant_read() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object ACL not implemented");
}
