//! Object lock tests.
//!
//! These tests are for object lock which is not yet implemented in Rucket.
//! All tests are marked with #[ignore].

#![allow(dead_code)]

use crate::S3TestContext;

/// Test getting object lock configuration.
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_get_configuration() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object lock not implemented");
}

/// Test putting object lock configuration.
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_put_configuration() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object lock not implemented");
}

/// Test object lock with default retention.
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_default_retention() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object lock not implemented");
}

/// Test deleting locked object fails.
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_delete_fails() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object lock not implemented");
}
