//! Presigned PUT URL tests.
//!
//! Tests for presigned URL validation on PUT operations.
//! These tests generate presigned URLs using the AWS SDK and then use
//! reqwest to make the actual HTTP request (to avoid re-signing).

use std::time::Duration;

use aws_sdk_s3::presigning::PresigningConfig;

use crate::S3TestContext;

/// Test presigned PUT URL works.
#[tokio::test]
async fn test_presigned_put_basic() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "presigned-put-object";
    let content = b"Uploaded via presigned PUT!";

    // Generate a presigned URL for PUT
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Use reqwest to upload the object using the presigned URL
    let client = reqwest::Client::new();
    let response = client
        .put(presigned_request.uri())
        .body(content.to_vec())
        .send()
        .await
        .expect("Failed to make presigned request");

    assert!(response.status().is_success(), "Expected success, got {}", response.status());

    // Verify the object was uploaded correctly
    let downloaded = ctx.get(key).await;
    assert_eq!(downloaded.as_slice(), content, "Content mismatch");
}

/// Test presigned PUT with custom expiry.
#[tokio::test]
async fn test_presigned_put_expiry() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "presigned-put-expiry";
    let content = b"Expiry test content";

    // Short expiry (60 seconds)
    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(60))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Should still work immediately
    let client = reqwest::Client::new();
    let response = client
        .put(presigned_request.uri())
        .body(content.to_vec())
        .send()
        .await
        .expect("Failed to make presigned request");

    assert!(response.status().is_success(), "Expected success, got {}", response.status());

    // Verify the object was uploaded correctly
    let downloaded = ctx.get(key).await;
    assert_eq!(downloaded.as_slice(), content, "Content mismatch");
}

/// Test presigned PUT with modified signature fails.
#[tokio::test]
async fn test_presigned_put_modified_signature() {
    let ctx = S3TestContext::new_with_auth().await;
    let key = "presigned-put-signature-test";
    let content = b"Signature test content";

    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Tamper with the signature
    let url = presigned_request.uri().to_string();
    let tampered_url = url.replace("X-Amz-Signature=", "X-Amz-Signature=00000000");

    let client = reqwest::Client::new();
    let response = client
        .put(&tampered_url)
        .body(content.to_vec())
        .send()
        .await
        .expect("Failed to make presigned request");

    assert_eq!(response.status().as_u16(), 403, "Expected 403 for invalid signature");
}

/// Test presigned PUT to wrong key fails.
/// The presigned URL is for a specific key, so using it for another key should fail.
#[tokio::test]
async fn test_presigned_put_wrong_key() {
    let ctx = S3TestContext::new_with_auth().await;
    let original_key = "presigned-original-key";
    let wrong_key = "presigned-wrong-key";
    let content = b"Wrong key test content";

    let presigning_config = PresigningConfig::builder()
        .expires_in(Duration::from_secs(3600))
        .build()
        .expect("valid presigning config");

    let presigned_request = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(original_key)
        .presigned(presigning_config)
        .await
        .expect("Failed to generate presigned URL");

    // Modify the URL to use a different key
    let url = presigned_request.uri().to_string();
    let tampered_url = url.replace(original_key, wrong_key);

    let client = reqwest::Client::new();
    let response = client
        .put(&tampered_url)
        .body(content.to_vec())
        .send()
        .await
        .expect("Failed to make presigned request");

    assert_eq!(
        response.status().as_u16(),
        403,
        "Expected 403 for wrong key, got {}",
        response.status()
    );
}
