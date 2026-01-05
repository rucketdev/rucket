//! Object legal hold tests (stub file).
//!
//! Note: Legal hold IS implemented in Rucket and tested in `lock.rs`.
//! These are additional stub tests for future comprehensive coverage.

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
