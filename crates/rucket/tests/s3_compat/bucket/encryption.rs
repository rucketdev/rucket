//! Bucket encryption tests.
//!
//! These tests are for default bucket encryption which is not yet implemented in Rucket.
//! All tests are marked with #[ignore].
//!
//! Ported from:
//! - Ceph s3-tests: test_encryption_*

#![allow(dead_code)]

use crate::S3TestContext;

/// Test putting bucket encryption (SSE-S3).
/// Ceph: test_sse_s3_bucket_default_encryption
#[tokio::test]
#[ignore = "Bucket encryption not implemented"]
async fn test_bucket_encryption_put_sse_s3() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket encryption not implemented");
}

/// Test getting bucket encryption.
/// Ceph: test_get_bucket_encryption
#[tokio::test]
#[ignore = "Bucket encryption not implemented"]
async fn test_bucket_encryption_get() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket encryption not implemented");
}

/// Test deleting bucket encryption.
/// Ceph: test_delete_bucket_encryption
#[tokio::test]
#[ignore = "Bucket encryption not implemented"]
async fn test_bucket_encryption_delete() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket encryption not implemented");
}

/// Test putting bucket encryption (SSE-KMS).
/// Ceph: test_sse_kms_bucket_default_encryption
#[tokio::test]
#[ignore = "Bucket encryption not implemented"]
async fn test_bucket_encryption_put_sse_kms() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("Bucket encryption not implemented");
}
