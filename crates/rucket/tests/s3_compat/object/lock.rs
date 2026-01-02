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

// =============================================================================
// Extended Object Lock Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test object lock governance mode.
/// Ceph: test_object_lock_governance_mode
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_governance_mode() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock compliance mode.
/// Ceph: test_object_lock_compliance_mode
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_compliance_mode() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock retention put.
/// Ceph: test_object_lock_retention_put
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_retention_put() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock retention get.
/// Ceph: test_object_lock_retention_get
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_retention_get() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock retention bypass governance.
/// Ceph: test_object_lock_retention_bypass
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_retention_bypass() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock legal hold on.
/// Ceph: test_object_lock_legal_hold_on
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_legal_hold_on() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock legal hold off.
/// Ceph: test_object_lock_legal_hold_off
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_legal_hold_off() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock legal hold get.
/// Ceph: test_object_lock_legal_hold_get
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_legal_hold_get() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock with versioning.
/// Ceph: test_object_lock_versioning
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_with_versioning() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock delete protected object.
/// Ceph: test_object_lock_delete_protected
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_delete_protected() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock overwrite protected object.
/// Ceph: test_object_lock_overwrite_protected
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_overwrite_protected() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock extend retention.
/// Ceph: test_object_lock_extend_retention
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_extend_retention() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock shorten retention fails.
/// Ceph: test_object_lock_shorten_retention
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_shorten_retention_fails() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock expired retention allows delete.
/// Ceph: test_object_lock_expired_retention
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_expired_retention() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock on copy.
/// Ceph: test_object_lock_copy
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_copy() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock on multipart upload.
/// Ceph: test_object_lock_multipart
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_multipart() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock invalid retention period.
/// Ceph: test_object_lock_invalid_retention
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_invalid_retention() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock bucket requires versioning.
/// Ceph: test_object_lock_requires_versioning
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_requires_versioning() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock retention date format.
/// Ceph: test_object_lock_retention_date
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_retention_date_format() {
    let _ctx = S3TestContext::new().await;
}

/// Test object lock with both retention and legal hold.
/// Ceph: test_object_lock_both
#[tokio::test]
#[ignore = "Object lock not implemented"]
async fn test_object_lock_both_retention_and_legal_hold() {
    let _ctx = S3TestContext::new().await;
}
