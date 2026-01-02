//! Object restore tests (Glacier/Deep Archive).
//!
//! These tests are for Glacier restore functionality which is not yet implemented.
//! All tests are marked with #[ignore].
//!
//! Ported from:
//! - Ceph s3-tests: test_restore_*
//! - AWS S3 restore object documentation

#![allow(dead_code)]

use crate::S3TestContext;

/// Test restore object request.
/// Ceph: test_restore_object
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_basic() {
    let _ctx = S3TestContext::new().await;
    // RestoreObject API not implemented
}

/// Test restore object with days parameter.
/// Ceph: test_restore_object_days
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_days() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object tier expedited.
/// Ceph: test_restore_object_expedited
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_tier_expedited() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object tier standard.
/// Ceph: test_restore_object_standard
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_tier_standard() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object tier bulk.
/// Ceph: test_restore_object_bulk
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_tier_bulk() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object already in progress.
/// Ceph: test_restore_object_in_progress
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_already_in_progress() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object already restored.
/// Ceph: test_restore_object_already_restored
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_already_restored() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object on non-archived object.
/// Ceph: test_restore_object_not_archived
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_not_archived() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object non-existent.
/// Ceph: test_restore_object_nonexistent
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_nonexistent() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object head shows restore status.
/// Ceph: test_restore_object_head_status
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_head_status() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object expiration.
/// Ceph: test_restore_object_expiration
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_expiration() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore versioned object.
/// Ceph: test_restore_object_versioned
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_versioned() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object with output location (S3 Batch).
/// Ceph: test_restore_object_output_location
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_output_location() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object select (restore with query).
/// Ceph: test_restore_object_select
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_select() {
    let _ctx = S3TestContext::new().await;
}

/// Test restore object from Deep Archive.
/// Ceph: test_restore_object_deep_archive
#[tokio::test]
#[ignore = "Glacier restore not implemented"]
async fn test_object_restore_deep_archive() {
    let _ctx = S3TestContext::new().await;
}
