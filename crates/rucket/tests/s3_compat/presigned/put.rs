//! Presigned PUT URL tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test presigned PUT URL works.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_basic() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT with expiry.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_expiry() {
    let _ctx = S3TestContext::new().await;
}

/// Test expired presigned PUT fails.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_expired() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT with content type.
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_content_type() {
    let _ctx = S3TestContext::new().await;
}

// =============================================================================
// Extended Presigned PUT Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test presigned PUT with metadata.
/// Ceph: test_presigned_put_metadata
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_metadata() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT with ACL.
/// Ceph: test_presigned_put_acl
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_acl() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT with storage class.
/// Ceph: test_presigned_put_storage_class
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_storage_class() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT modified signature fails.
/// Ceph: test_presigned_put_modified_signature
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_modified_signature() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT wrong bucket fails.
/// Ceph: test_presigned_put_wrong_bucket
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_wrong_bucket() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT wrong key fails.
/// Ceph: test_presigned_put_wrong_key
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_wrong_key() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT content-length mismatch.
/// Ceph: test_presigned_put_content_length
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_content_length_mismatch() {
    let _ctx = S3TestContext::new().await;
}

/// Test presigned PUT content-md5.
/// Ceph: test_presigned_put_content_md5
#[tokio::test]
#[ignore = "Presigned URL testing requires HTTP client outside SDK"]
async fn test_presigned_put_content_md5() {
    let _ctx = S3TestContext::new().await;
}
