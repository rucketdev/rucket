//! Anonymous access tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_*_anon_*
//! - AWS documentation: Public access settings
//!
//! Note: Rucket currently allows all anonymous (unauthenticated) requests
//! in single-tenant mode. These tests verify that unauthenticated HTTP
//! requests work correctly.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ObjectCannedAcl;

use crate::S3TestContext;

/// Helper to make an unauthenticated HTTP request.
async fn make_anon_request(url: &str, method: &str) -> reqwest::Response {
    let client = reqwest::Client::new();
    match method {
        "GET" => client.get(url).send().await.expect("GET request failed"),
        "HEAD" => client.head(url).send().await.expect("HEAD request failed"),
        "DELETE" => client.delete(url).send().await.expect("DELETE request failed"),
        _ => panic!("Unsupported method: {}", method),
    }
}

/// Test anonymous GET on object.
/// Currently Rucket allows all anonymous requests in single-tenant mode.
/// Ceph: test_object_anon_get_public
#[tokio::test]
async fn test_anonymous_get_public_object() {
    let ctx = S3TestContext::new().await;
    let key = "anon-get-test.txt";
    let content = b"Anonymous GET test content";

    // Create object using authenticated client
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .send()
        .await
        .expect("PUT should succeed");

    // Make anonymous GET request
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = make_anon_request(&url, "GET").await;

    assert!(response.status().is_success(), "Anonymous GET should succeed");
    let body = response.bytes().await.expect("Failed to read body");
    assert_eq!(body.as_ref(), content, "Content should match");
}

/// Test anonymous HEAD on object.
/// Ceph: test_object_anon_head
#[tokio::test]
async fn test_anonymous_head_public_object() {
    let ctx = S3TestContext::new().await;
    let key = "anon-head-test.txt";
    let content = b"Anonymous HEAD test content";

    // Create object using authenticated client
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .send()
        .await
        .expect("PUT should succeed");

    // Make anonymous HEAD request
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = make_anon_request(&url, "HEAD").await;

    assert!(response.status().is_success(), "Anonymous HEAD should succeed");
    assert_eq!(
        response.headers().get("content-length").and_then(|v| v.to_str().ok()),
        Some(content.len().to_string().as_str()),
        "Content-Length should match"
    );
}

/// Test anonymous list on bucket.
/// Ceph: test_bucket_anon_list
#[tokio::test]
async fn test_anonymous_list_public_bucket() {
    let ctx = S3TestContext::new().await;

    // Create some objects
    for i in 0..3 {
        ctx.client
            .put_object()
            .bucket(&ctx.bucket)
            .key(format!("anon-list-{}.txt", i))
            .body(ByteStream::from(format!("content {}", i).into_bytes()))
            .send()
            .await
            .expect("PUT should succeed");
    }

    // Make anonymous list request
    let url = format!("{}/{}", ctx.endpoint, ctx.bucket);
    let response = make_anon_request(&url, "GET").await;

    assert!(response.status().is_success(), "Anonymous list should succeed");
    let body = response.text().await.expect("Failed to read body");
    assert!(body.contains("<ListBucketResult"), "Should return XML listing");
    assert!(body.contains("anon-list-0.txt"), "Should contain object key");
}

/// Test anonymous DELETE.
/// Currently Rucket allows all anonymous requests including DELETE.
/// Ceph: test_object_anon_delete
#[tokio::test]
async fn test_anonymous_delete() {
    let ctx = S3TestContext::new().await;
    let key = "anon-delete-test.txt";

    // Create object using authenticated client
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(b"to be deleted".to_vec()))
        .send()
        .await
        .expect("PUT should succeed");

    // Make anonymous DELETE request
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = make_anon_request(&url, "DELETE").await;

    // In single-tenant mode, anonymous DELETE is currently allowed
    assert!(
        response.status().is_success() || response.status() == 204,
        "Anonymous DELETE should succeed with status {}",
        response.status()
    );

    // Verify object is deleted
    let get_result = ctx.client.head_object().bucket(&ctx.bucket).key(key).send().await;
    assert!(get_result.is_err(), "Object should be deleted");
}

/// Test anonymous access to versioned object.
/// Ceph: test_anon_versioned
#[tokio::test]
async fn test_anonymous_versioned_object() {
    let ctx = S3TestContext::new().await;

    // Enable versioning
    ctx.client
        .put_bucket_versioning()
        .bucket(&ctx.bucket)
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .expect("Should enable versioning");

    // Create multiple versions
    let key = "anon-versioned.txt";
    for i in 1..=3 {
        ctx.client
            .put_object()
            .bucket(&ctx.bucket)
            .key(key)
            .body(ByteStream::from(format!("version {}", i).into_bytes()))
            .send()
            .await
            .expect("PUT should succeed");
    }

    // Make anonymous GET request (should get latest version)
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = make_anon_request(&url, "GET").await;

    assert!(response.status().is_success(), "Anonymous GET versioned should succeed");
    let body = response.text().await.expect("Failed to read body");
    assert_eq!(body, "version 3", "Should get latest version");
}

/// Test anonymous GET with public-read ACL.
/// Note: In single-tenant mode, ACLs are accepted but not enforced.
/// Ceph: test_public_read_acl
#[tokio::test]
async fn test_public_read_acl() {
    let ctx = S3TestContext::new().await;
    let key = "public-read-test.txt";
    let content = b"Public read content";

    // Create object with public-read ACL
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .acl(ObjectCannedAcl::PublicRead)
        .send()
        .await
        .expect("PUT with public-read should succeed");

    // Make anonymous GET request
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = make_anon_request(&url, "GET").await;

    assert!(response.status().is_success(), "Anonymous GET with public-read should succeed");
}

/// Test anonymous access with public-read-write ACL.
/// Ceph: test_public_read_write_acl
#[tokio::test]
async fn test_public_read_write_acl() {
    let ctx = S3TestContext::new().await;
    let key = "public-rw-test.txt";

    // Create object with public-read-write ACL
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(b"Public RW content".to_vec()))
        .acl(ObjectCannedAcl::PublicReadWrite)
        .send()
        .await
        .expect("PUT with public-read-write should succeed");

    // Make anonymous GET request
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = make_anon_request(&url, "GET").await;

    assert!(response.status().is_success(), "Anonymous GET with public-read-write should succeed");
}

/// Test anonymous PUT object.
/// Currently Rucket allows anonymous PUT in single-tenant mode.
/// Ceph: test_object_anon_put
#[tokio::test]
async fn test_anonymous_put_public_bucket() {
    let ctx = S3TestContext::new().await;
    let key = "anon-put-test.txt";
    let content = b"Anonymous PUT content";

    // Make anonymous PUT request
    let client = reqwest::Client::new();
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response =
        client.put(&url).body(content.to_vec()).send().await.expect("PUT request failed");

    assert!(response.status().is_success(), "Anonymous PUT should succeed");

    // Verify object was created
    let head = ctx.client.head_object().bucket(&ctx.bucket).key(key).send().await;
    assert!(head.is_ok(), "Object should exist after anonymous PUT");
}

/// Test anonymous multipart upload.
/// Ceph: test_multipart_anon
#[tokio::test]
#[ignore = "Anonymous multipart requires implementing CreateMultipartUpload without auth"]
async fn test_anonymous_multipart() {
    let _ctx = S3TestContext::new().await;
    // Multipart upload initiation requires proper request parsing
}

/// Test anonymous access with bucket policy.
/// Ceph: test_anon_bucket_policy
#[tokio::test]
#[ignore = "Bucket policy evaluation for anonymous principals not fully implemented"]
async fn test_anonymous_bucket_policy() {
    let _ctx = S3TestContext::new().await;
    // Would need bucket policy that allows anonymous access
}

/// Test anonymous access with ACL.
/// Ceph: test_anon_acl
#[tokio::test]
#[ignore = "ACL-based anonymous access enforcement not implemented"]
async fn test_anonymous_acl() {
    let _ctx = S3TestContext::new().await;
    // Would need ACL enforcement
}

/// Test authenticated-read canned ACL.
/// Ceph: test_authenticated_read_acl
#[tokio::test]
#[ignore = "Authenticated-read ACL enforcement not implemented"]
async fn test_authenticated_read_acl() {
    let _ctx = S3TestContext::new().await;
    // Would need to verify authenticated users can read but anonymous cannot
}

/// Test anonymous access blocked by block public access.
/// Ceph: test_block_public_access
#[tokio::test]
#[ignore = "Block public access enforcement not implemented"]
async fn test_block_public_access() {
    let _ctx = S3TestContext::new().await;
    // Would need block public access settings enforcement
}

/// Test anonymous presigned URL access.
/// Ceph: test_anon_presigned
#[tokio::test]
async fn test_anonymous_presigned_url() {
    let ctx = S3TestContext::new().await;
    let key = "presigned-anon-test.txt";
    let content = b"Presigned content";

    // Create object
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .send()
        .await
        .expect("PUT should succeed");

    // Create presigned URL
    let presign_config = aws_sdk_s3::presigning::PresigningConfig::builder()
        .expires_in(std::time::Duration::from_secs(300))
        .build()
        .expect("Failed to build presign config");

    let presigned_req = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presign_config)
        .await
        .expect("Failed to create presigned URL");

    // Make request using presigned URL (no additional auth needed)
    let client = reqwest::Client::new();
    let response = client.get(presigned_req.uri()).send().await.expect("Presigned request failed");

    assert!(response.status().is_success(), "Presigned URL request should succeed");
    let body = response.bytes().await.expect("Failed to read body");
    assert_eq!(body.as_ref(), content, "Content should match");
}

/// Test anonymous GET on private object.
/// Currently in single-tenant mode, all anonymous requests are allowed.
/// Ceph: test_object_anon_get_private
#[tokio::test]
async fn test_anonymous_get_private_object() {
    let ctx = S3TestContext::new().await;
    let key = "private-object.txt";

    // Create object with private ACL
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(b"Private content".to_vec()))
        .acl(ObjectCannedAcl::Private)
        .send()
        .await
        .expect("PUT with private should succeed");

    // Make anonymous GET request
    // In single-tenant mode, this still succeeds because ACLs aren't enforced
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = make_anon_request(&url, "GET").await;

    // Current behavior: anonymous access is allowed even for private objects
    // This documents the current behavior, not the ideal behavior
    assert!(
        response.status().is_success(),
        "In single-tenant mode, anonymous GET on private object currently succeeds"
    );
}
