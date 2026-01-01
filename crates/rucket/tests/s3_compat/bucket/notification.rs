//! Bucket notification tests.
//!
//! These tests are for bucket notifications which are not yet implemented in Rucket.
//! All tests are marked with #[ignore].
//!
//! Ported from:
//! - Ceph s3-tests: test_notification_*

#![allow(dead_code)]

use crate::S3TestContext;

/// Test putting notification configuration.
/// Ceph: test_notification_set
#[tokio::test]
#[ignore = "Notifications not implemented"]
async fn test_bucket_notification_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Notifications not implemented");
}

/// Test getting notification configuration.
/// Ceph: test_notification_get
#[tokio::test]
#[ignore = "Notifications not implemented"]
async fn test_bucket_notification_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Notifications not implemented");
}

/// Test deleting notification configuration.
/// Ceph: test_notification_delete
#[tokio::test]
#[ignore = "Notifications not implemented"]
async fn test_bucket_notification_delete() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Notifications not implemented");
}

/// Test SNS notification.
/// Ceph: test_notification_sns
#[tokio::test]
#[ignore = "Notifications not implemented"]
async fn test_bucket_notification_sns() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Notifications not implemented");
}

/// Test SQS notification.
/// Ceph: test_notification_sqs
#[tokio::test]
#[ignore = "Notifications not implemented"]
async fn test_bucket_notification_sqs() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Notifications not implemented");
}
