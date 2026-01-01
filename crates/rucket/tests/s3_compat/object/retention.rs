//! Object retention tests.
//!
//! These tests are for object retention which is not yet implemented in Rucket.
//! All tests are marked with #[ignore].

#![allow(dead_code)]

use crate::S3TestContext;

/// Test putting object retention.
#[tokio::test]
#[ignore = "Object retention not implemented"]
async fn test_object_retention_put() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object retention not implemented");
}

/// Test getting object retention.
#[tokio::test]
#[ignore = "Object retention not implemented"]
async fn test_object_retention_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object retention not implemented");
}

/// Test object retention governance mode.
#[tokio::test]
#[ignore = "Object retention not implemented"]
async fn test_object_retention_governance() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object retention not implemented");
}

/// Test object retention compliance mode.
#[tokio::test]
#[ignore = "Object retention not implemented"]
async fn test_object_retention_compliance() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Object retention not implemented");
}
