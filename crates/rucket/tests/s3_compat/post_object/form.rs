//! POST object form upload tests.

#![allow(dead_code)]

use crate::S3TestContext;

/// Test POST object basic.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_basic() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

/// Test POST object with policy.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_policy() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

/// Test POST object with key prefix.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_key_prefix() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

/// Test POST object expired policy.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_expired() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

/// Test POST object invalid signature.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_invalid_signature() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

/// Test POST object content length limit.
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_size_limit() {
    let _ctx = S3TestContext::new().await;
    unimplemented!("POST object not implemented");
}

// =============================================================================
// Extended POST Object Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test POST object with ACL.
/// Ceph: test_post_object_acl
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_acl() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with success action redirect.
/// Ceph: test_post_object_redirect
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_success_redirect() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with success action status.
/// Ceph: test_post_object_status
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_success_status() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with content-type condition.
/// Ceph: test_post_object_content_type
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_content_type() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object starts-with condition.
/// Ceph: test_post_object_starts_with
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_starts_with() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object eq condition.
/// Ceph: test_post_object_eq
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_eq_condition() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with metadata.
/// Ceph: test_post_object_metadata
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_metadata() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object key variable ${filename}.
/// Ceph: test_post_object_filename
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_filename_variable() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object missing required field.
/// Ceph: test_post_object_missing_field
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_missing_field() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with tagging.
/// Ceph: test_post_object_tagging
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_tagging() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with storage class.
/// Ceph: test_post_object_storage_class
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_storage_class() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with SSE-S3.
/// Ceph: test_post_object_sse_s3
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_sse_s3() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object with SSE-C.
/// Ceph: test_post_object_sse_c
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_sse_c() {
    let _ctx = S3TestContext::new().await;
}

/// Test POST object to versioned bucket.
/// Ceph: test_post_object_versioned
#[tokio::test]
#[ignore = "POST object not implemented"]
async fn test_post_object_versioned() {
    let _ctx = S3TestContext::new().await;
}
