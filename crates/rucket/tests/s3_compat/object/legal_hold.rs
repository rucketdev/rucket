//! Object legal hold tests.
//!
//! These tests are for object legal hold which is not yet implemented in Rucket.
//! All tests are marked with #[ignore].

#![allow(dead_code)]

use crate::S3TestContext;

/// Test putting object legal hold.
#[tokio::test]
#[ignore = "Object legal hold not implemented"]
async fn test_object_legal_hold_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object legal hold not implemented");
}

/// Test getting object legal hold.
#[tokio::test]
#[ignore = "Object legal hold not implemented"]
async fn test_object_legal_hold_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object legal hold not implemented");
}

/// Test object legal hold on status.
#[tokio::test]
#[ignore = "Object legal hold not implemented"]
async fn test_object_legal_hold_on() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object legal hold not implemented");
}

/// Test object legal hold off status.
#[tokio::test]
#[ignore = "Object legal hold not implemented"]
async fn test_object_legal_hold_off() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object legal hold not implemented");
}
