//! Permission tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test anonymous access denied.
#[tokio::test]
#[ignore = "Multi-user access control not implemented"]
async fn test_access_anonymous_denied() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Multi-user access control not implemented");
}

/// Test wrong credentials denied.
#[tokio::test]
#[ignore = "Multi-user access control not implemented"]
async fn test_access_wrong_credentials() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Multi-user access control not implemented");
}

/// Test cross-account access.
#[tokio::test]
#[ignore = "Multi-user access control not implemented"]
async fn test_access_cross_account() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Multi-user access control not implemented");
}

/// Test IAM policy.
#[tokio::test]
#[ignore = "Multi-user access control not implemented"]
async fn test_access_iam_policy() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Multi-user access control not implemented");
}
