//! Anonymous access tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_*_anon_*
//! - AWS documentation: Public access settings

use crate::S3TestContext;

/// Test anonymous GET on public object.
/// Ceph: test_object_anon_get_public
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_get_public_object() {
    let _ctx = S3TestContext::new().await;
    // Would need unauthenticated client
}

/// Test anonymous GET on private object fails.
/// Ceph: test_object_anon_get_private
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_get_private_object() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous PUT on public bucket.
/// Ceph: test_object_anon_put
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_put_public_bucket() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous list on public bucket.
/// Ceph: test_bucket_anon_list
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_list_public_bucket() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous HEAD on public object.
/// Ceph: test_object_anon_head
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_head_public_object() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous DELETE is denied.
/// Ceph: test_object_anon_delete
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_delete_denied() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous multipart upload.
/// Ceph: test_multipart_anon
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_multipart() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous access with bucket policy.
/// Ceph: test_anon_bucket_policy
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_bucket_policy() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous access with ACL.
/// Ceph: test_anon_acl
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_acl() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous access to versioned object.
/// Ceph: test_anon_versioned
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_versioned_object() {
    let _ctx = S3TestContext::new().await;
}

/// Test public-read canned ACL allows anonymous GET.
/// Ceph: test_public_read_acl
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_public_read_acl() {
    let _ctx = S3TestContext::new().await;
}

/// Test public-read-write canned ACL.
/// Ceph: test_public_read_write_acl
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_public_read_write_acl() {
    let _ctx = S3TestContext::new().await;
}

/// Test authenticated-read canned ACL.
/// Ceph: test_authenticated_read_acl
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_authenticated_read_acl() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous access blocked by block public access.
/// Ceph: test_block_public_access
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_block_public_access() {
    let _ctx = S3TestContext::new().await;
}

/// Test anonymous presigned URL access.
/// Ceph: test_anon_presigned
#[tokio::test]
#[ignore = "Anonymous access not implemented"]
async fn test_anonymous_presigned_url() {
    let _ctx = S3TestContext::new().await;
}
