//! Bucket ACL tests.
//!
//! Tests for S3 bucket Access Control List operations including:
//! - GET bucket ACL (returns owner with FULL_CONTROL)
//! - PUT bucket ACL (accepts ACL changes)
//!
//! Note: Rucket implements a simplified ACL model in single-tenant mode where
//! the owner always has FULL_CONTROL. PUT operations are accepted but ACLs
//! are not actually enforced.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_acl_*

use aws_sdk_s3::types::{BucketCannedAcl, Permission};

use crate::S3TestContext;

// =============================================================================
// Basic Bucket ACL Tests
// =============================================================================

/// Test getting bucket ACL returns owner with FULL_CONTROL.
/// Ceph: test_bucket_acl_default
#[tokio::test]
async fn test_bucket_acl_get_default() {
    let ctx = S3TestContext::new().await;

    // Get the bucket ACL
    let result = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed");

    // Verify owner is set
    let owner = result.owner().expect("ACL should have owner");
    assert!(owner.id().is_some(), "Owner should have ID");

    // Verify grants include FULL_CONTROL for owner
    let grants = result.grants();
    assert!(!grants.is_empty(), "ACL should have at least one grant");

    // Find the FULL_CONTROL grant
    let full_control = grants.iter().find(|g| g.permission() == Some(&Permission::FullControl));
    assert!(full_control.is_some(), "Should have FULL_CONTROL grant");
}

/// Test putting bucket ACL is accepted.
/// Ceph: test_bucket_acl_put
#[tokio::test]
async fn test_bucket_acl_put() {
    let ctx = S3TestContext::new().await;

    // Put bucket ACL with canned ACL
    let result =
        ctx.client.put_bucket_acl().bucket(&ctx.bucket).acl(BucketCannedAcl::Private).send().await;

    // PUT ACL should succeed (even in single-tenant mode where it's a no-op)
    assert!(result.is_ok(), "PUT bucket ACL should succeed");

    // Verify ACL can still be retrieved
    let acl = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed after PUT");

    assert!(acl.owner().is_some(), "ACL should have owner");
}

// =============================================================================
// Canned ACL Tests
// =============================================================================

/// Test bucket ACL with canned ACL private.
/// Ceph: test_bucket_acl_canned_private
#[tokio::test]
async fn test_bucket_acl_canned_private() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .expect("PUT bucket ACL with private should succeed");

    // Verify ACL
    let result = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed");

    assert!(result.owner().is_some(), "ACL should have owner");
    assert!(!result.grants().is_empty(), "ACL should have grants");
}

/// Test bucket ACL with canned ACL public-read.
/// Ceph: test_bucket_acl_canned_publicread
///
/// Note: In single-tenant mode, public-read is accepted but not enforced.
#[tokio::test]
async fn test_bucket_acl_canned_public_read() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .expect("PUT bucket ACL with public-read should succeed");

    // Verify ACL can be retrieved
    let result = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed");

    assert!(result.owner().is_some(), "ACL should have owner");
}

/// Test bucket ACL with canned ACL public-read-write.
/// Ceph: test_bucket_acl_canned_publicreadwrite
///
/// Note: In single-tenant mode, public-read-write is accepted but not enforced.
#[tokio::test]
async fn test_bucket_acl_canned_public_read_write() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::PublicReadWrite)
        .send()
        .await
        .expect("PUT bucket ACL with public-read-write should succeed");

    // Verify ACL can be retrieved
    let result = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed");

    assert!(result.owner().is_some(), "ACL should have owner");
}

/// Test bucket ACL with canned ACL authenticated-read.
/// Ceph: test_bucket_acl_canned_authenticatedread
///
/// Note: In single-tenant mode, authenticated-read is accepted but not enforced.
#[tokio::test]
async fn test_bucket_acl_canned_authenticated_read() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::AuthenticatedRead)
        .send()
        .await
        .expect("PUT bucket ACL with authenticated-read should succeed");

    // Verify ACL can be retrieved
    let result = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed");

    assert!(result.owner().is_some(), "ACL should have owner");
}

// =============================================================================
// Error Handling Tests
// =============================================================================

/// Test GET bucket ACL for non-existent bucket returns error.
#[tokio::test]
async fn test_bucket_acl_nonexistent_bucket() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.get_bucket_acl().bucket("nonexistent-bucket-for-acl-test").send().await;

    assert!(result.is_err(), "GET ACL for non-existent bucket should fail");
}

/// Test PUT bucket ACL for non-existent bucket returns error.
#[tokio::test]
async fn test_bucket_acl_put_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .put_bucket_acl()
        .bucket("nonexistent-bucket-for-put-acl")
        .acl(BucketCannedAcl::Private)
        .send()
        .await;

    assert!(result.is_err(), "PUT ACL for non-existent bucket should fail");
}

// =============================================================================
// Multiple ACL Operations Tests
// =============================================================================

/// Test changing bucket ACL multiple times.
#[tokio::test]
async fn test_bucket_acl_change_multiple() {
    let ctx = S3TestContext::new().await;

    // Set to private
    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .expect("PUT private should succeed");

    // Change to public-read
    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::PublicRead)
        .send()
        .await
        .expect("PUT public-read should succeed");

    // Change back to private
    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .expect("PUT private again should succeed");

    // Verify final ACL
    let result = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed");

    assert!(result.owner().is_some(), "ACL should have owner");
}

/// Test bucket ACL persists across operations.
#[tokio::test]
async fn test_bucket_acl_persist() {
    let ctx = S3TestContext::new().await;

    // Set ACL
    ctx.client
        .put_bucket_acl()
        .bucket(&ctx.bucket)
        .acl(BucketCannedAcl::Private)
        .send()
        .await
        .expect("PUT should succeed");

    // Do some other operation (put object)
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test-object.txt")
        .body(Vec::from("test content").into())
        .send()
        .await
        .expect("PUT object should succeed");

    // ACL should still be retrievable
    let result = ctx
        .client
        .get_bucket_acl()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET bucket ACL should succeed");

    assert!(result.owner().is_some(), "ACL should still have owner");
    assert!(!result.grants().is_empty(), "ACL should still have grants");
}
