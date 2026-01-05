//! Object ACL tests.
//!
//! Tests for S3 object Access Control List operations including:
//! - GET object ACL (returns owner with FULL_CONTROL)
//! - PUT object ACL (accepts ACL changes)
//!
//! Note: Rucket implements a simplified ACL model in single-tenant mode where
//! the owner always has FULL_CONTROL. PUT operations are accepted but ACLs
//! are not actually enforced.

use aws_sdk_s3::types::{ObjectCannedAcl, Permission};

use crate::S3TestContext;

// =============================================================================
// Basic Object ACL Tests
// =============================================================================

/// Test getting object ACL returns owner with FULL_CONTROL.
#[tokio::test]
async fn test_object_acl_get_default() {
    let ctx = S3TestContext::new().await;

    // Create an object first
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test-acl-object.txt")
        .body(Vec::from("test content").into())
        .send()
        .await
        .expect("PUT object should succeed");

    // Get the object ACL
    let result = ctx
        .client
        .get_object_acl()
        .bucket(&ctx.bucket)
        .key("test-acl-object.txt")
        .send()
        .await
        .expect("GET object ACL should succeed");

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

/// Test putting object ACL is accepted.
#[tokio::test]
async fn test_object_acl_put() {
    let ctx = S3TestContext::new().await;

    // Create an object first
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test-put-acl.txt")
        .body(Vec::from("test content").into())
        .send()
        .await
        .expect("PUT object should succeed");

    // Put object ACL with canned ACL (using header-based approach)
    let result = ctx
        .client
        .put_object_acl()
        .bucket(&ctx.bucket)
        .key("test-put-acl.txt")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;

    // PUT ACL should succeed (even in single-tenant mode where it's a no-op)
    assert!(result.is_ok(), "PUT object ACL should succeed");
}

/// Test object ACL with canned private ACL.
#[tokio::test]
async fn test_object_acl_canned_private() {
    let ctx = S3TestContext::new().await;

    // Create object with private ACL
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("private-object.txt")
        .body(Vec::from("private content").into())
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .expect("PUT object with private ACL should succeed");

    // Get ACL to verify
    let result = ctx
        .client
        .get_object_acl()
        .bucket(&ctx.bucket)
        .key("private-object.txt")
        .send()
        .await
        .expect("GET object ACL should succeed");

    // Owner should exist with FULL_CONTROL
    assert!(result.owner().is_some(), "ACL should have owner");
    assert!(!result.grants().is_empty(), "ACL should have grants");
}

/// Test object ACL with canned public-read ACL.
///
/// Note: In single-tenant mode, public-read is accepted but not enforced.
#[tokio::test]
async fn test_object_acl_canned_public_read() {
    let ctx = S3TestContext::new().await;

    // Create object with public-read ACL
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("public-object.txt")
        .body(Vec::from("public content").into())
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .expect("PUT object with public-read ACL should succeed");

    // Get ACL to verify the object was created
    let result = ctx
        .client
        .get_object_acl()
        .bucket(&ctx.bucket)
        .key("public-object.txt")
        .send()
        .await
        .expect("GET object ACL should succeed");

    // Owner should exist
    assert!(result.owner().is_some(), "ACL should have owner");
}

/// Test PUT object ACL using x-amz-acl header.
#[tokio::test]
async fn test_object_acl_header() {
    let ctx = S3TestContext::new().await;

    // Create an object
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("acl-header-test.txt")
        .body(Vec::from("test").into())
        .send()
        .await
        .expect("PUT object should succeed");

    // Set ACL using the acl method (maps to x-amz-acl header)
    ctx.client
        .put_object_acl()
        .bucket(&ctx.bucket)
        .key("acl-header-test.txt")
        .acl(ObjectCannedAcl::BucketOwnerFullControl)
        .send()
        .await
        .expect("PUT object ACL should succeed");

    // Verify ACL can be retrieved
    let result = ctx
        .client
        .get_object_acl()
        .bucket(&ctx.bucket)
        .key("acl-header-test.txt")
        .send()
        .await
        .expect("GET object ACL should succeed");

    assert!(result.owner().is_some(), "ACL should have owner");
}

/// Test GET object ACL for non-existent object returns error.
#[tokio::test]
async fn test_object_acl_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result =
        ctx.client.get_object_acl().bucket(&ctx.bucket).key("nonexistent-object.txt").send().await;

    assert!(result.is_err(), "GET ACL for non-existent object should fail");
}

/// Test PUT object ACL for non-existent object returns error.
#[tokio::test]
async fn test_object_acl_put_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .put_object_acl()
        .bucket(&ctx.bucket)
        .key("nonexistent-for-acl.txt")
        .acl(ObjectCannedAcl::Private)
        .send()
        .await;

    assert!(result.is_err(), "PUT ACL for non-existent object should fail");
}
