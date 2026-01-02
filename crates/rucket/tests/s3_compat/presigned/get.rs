//! Presigned GET URL tests.
//!
//! Tests for presigned URL validation on GET operations.
//! These tests generate presigned URLs using the AWS SDK and then use
//! reqwest to make the actual HTTP request (to avoid re-signing).

use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;

use crate::S3TestContext;

/// Test presigned GET URL works.
#[tokio::test]
async fn test_presigned_get_basic() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "presigned-test-object";
    let content = b"Hello from presigned URL!";

    // Put an object using the SDK (which handles auth)
    ctx.put(key, content).await;

    // Generate a presigned URL for GET
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Use reqwest to fetch the object using the presigned URL
    let client = reqwest::Client::new();
    let response =
        client.get(presigned_request.uri()).send().await.expect("Failed to make presigned request");

    assert!(response.status().is_success(), "Expected success, got {}", response.status());

    let body = response.bytes().await.expect("Failed to read body");
    assert_eq!(body.as_ref(), content, "Content mismatch");
}

/// Test presigned GET with custom expiry.
#[tokio::test]
async fn test_presigned_get_custom_expiry() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "presigned-expiry-test";
    let content = b"Expiry test content";

    ctx.put(key, content).await;

    // Short expiry (60 seconds)
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(60))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Should still work immediately
    let client = reqwest::Client::new();
    let response =
        client.get(presigned_request.uri()).send().await.expect("Failed to make presigned request");

    assert!(response.status().is_success(), "Expected success, got {}", response.status());
}

/// Test presigned GET for non-existent object returns 404.
#[tokio::test]
async fn test_presigned_get_nonexistent() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "nonexistent-presigned-object";

    // Generate a presigned URL for a non-existent object
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // The request should fail with 404
    let client = reqwest::Client::new();
    let response =
        client.get(presigned_request.uri()).send().await.expect("Failed to make presigned request");

    assert_eq!(response.status().as_u16(), 404, "Expected 404 for non-existent object");
}

/// Test presigned GET with modified signature fails.
#[tokio::test]
async fn test_presigned_get_modified_signature() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "presigned-signature-test";
    let content = b"Signature test content";

    ctx.put(key, content).await;

    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Tamper with the signature
    let url = presigned_request.uri().to_string();
    let tampered_url = url.replace("X-Amz-Signature=", "X-Amz-Signature=00000000");

    let client = reqwest::Client::new();
    let response =
        client.get(&tampered_url).send().await.expect("Failed to make presigned request");

    assert_eq!(response.status().as_u16(), 403, "Expected 403 for invalid signature");
}

/// Test presigned GET with range header.
#[tokio::test]
async fn test_presigned_get_range() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "presigned-range-test";
    let content = b"0123456789";

    ctx.put(key, content).await;

    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Use reqwest with range header
    let client = reqwest::Client::new();
    let response = client
        .get(presigned_request.uri())
        .header("Range", "bytes=0-4")
        .send()
        .await
        .expect("Failed to make presigned request");

    assert!(
        response.status().is_success() || response.status().as_u16() == 206,
        "Expected success or 206, got {}",
        response.status()
    );

    let body = response.bytes().await.expect("Failed to read body");
    assert_eq!(body.as_ref(), b"01234", "Range content mismatch");
}

/// Test that requests without auth to an auth-enabled server are still allowed
/// (anonymous access fallback for requests without auth headers).
#[tokio::test]
async fn test_unauthenticated_request_allowed() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "auth-test-object";
    let content = b"Auth test content";

    // Put using SDK (which handles auth)
    ctx.put(key, content).await;

    // Make an unauthenticated request directly
    let client = reqwest::Client::new();
    let url = format!("{}/{}/{}", ctx.endpoint, ctx.bucket, key);
    let response = client.get(&url).send().await.expect("Failed to make request");

    // Currently, we allow anonymous access (no auth header = pass through)
    // This test documents the current behavior
    assert!(
        response.status().is_success(),
        "Expected success for anonymous request, got {}",
        response.status()
    );
}
