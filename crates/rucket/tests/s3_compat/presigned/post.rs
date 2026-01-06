//! Presigned POST (browser upload) tests with signature verification.
//!
//! Tests for POST Object with AWS Signature V4 authentication.
//! These tests generate signed POST forms using HMAC-SHA256.

use base64::Engine;
use chrono::{Duration, Utc};
use hmac::{Hmac, Mac};
use reqwest::multipart::{Form, Part};
use serde_json::json;
use sha2::Sha256;

use crate::S3TestContext;

// Test credentials (must match harness.rs)
const ACCESS_KEY: &str = "rucket";
const SECRET_KEY: &str = "rucket123";
const REGION: &str = "us-east-1";
const SERVICE: &str = "s3";

type HmacSha256 = Hmac<Sha256>;

/// Compute AWS Signature V4 signing key.
fn get_signing_key(secret_key: &str, date: &str, region: &str, service: &str) -> Vec<u8> {
    // DateKey = HMAC-SHA256("AWS4" + secret_key, date)
    let date_key = {
        let mut mac = HmacSha256::new_from_slice(format!("AWS4{}", secret_key).as_bytes()).unwrap();
        mac.update(date.as_bytes());
        mac.finalize().into_bytes().to_vec()
    };

    // DateRegionKey = HMAC-SHA256(DateKey, region)
    let date_region_key = {
        let mut mac = HmacSha256::new_from_slice(&date_key).unwrap();
        mac.update(region.as_bytes());
        mac.finalize().into_bytes().to_vec()
    };

    // DateRegionServiceKey = HMAC-SHA256(DateRegionKey, service)
    let date_region_service_key = {
        let mut mac = HmacSha256::new_from_slice(&date_region_key).unwrap();
        mac.update(service.as_bytes());
        mac.finalize().into_bytes().to_vec()
    };

    // SigningKey = HMAC-SHA256(DateRegionServiceKey, "aws4_request")
    let mut mac = HmacSha256::new_from_slice(&date_region_service_key).unwrap();
    mac.update(b"aws4_request");
    mac.finalize().into_bytes().to_vec()
}

/// Sign a POST policy using AWS Signature V4.
fn sign_policy(policy_b64: &str, date: &str, region: &str) -> String {
    let signing_key = get_signing_key(SECRET_KEY, date, region, SERVICE);
    let mut mac = HmacSha256::new_from_slice(&signing_key).unwrap();
    mac.update(policy_b64.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

/// Build a signed POST policy.
fn build_signed_policy(
    bucket: &str,
    key_condition: serde_json::Value,
    extra_conditions: Vec<serde_json::Value>,
) -> (String, String, String, String) {
    let now = Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let expiration = (now + Duration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let credential = format!("{}/{}/{}/{}/aws4_request", ACCESS_KEY, date, REGION, SERVICE);

    let mut conditions = vec![
        json!({"bucket": bucket}),
        key_condition,
        json!({"x-amz-algorithm": "AWS4-HMAC-SHA256"}),
        json!({"x-amz-credential": credential}),
        json!({"x-amz-date": amz_date}),
    ];
    conditions.extend(extra_conditions);

    let policy = json!({
        "expiration": expiration,
        "conditions": conditions
    });

    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.to_string());
    let signature = sign_policy(&policy_b64, &date, REGION);

    (policy_b64, credential, amz_date, signature)
}

/// Test presigned POST with valid policy and signature.
#[tokio::test]
async fn test_presigned_post_policy() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    let key = "presigned/test-file.txt";
    let content = b"Presigned POST content";

    let (policy, credential, amz_date, signature) =
        build_signed_policy(&ctx.bucket, json!(["starts-with", "$key", "presigned/"]), vec![]);

    let form = Form::new()
        .text("key", key.to_string())
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(content.to_vec()).file_name("test-file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "Presigned POST failed: {} - {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    // Verify the object was uploaded
    let data = ctx.get(key).await;
    assert_eq!(data, content);
}

/// Test presigned POST with various policy conditions.
#[tokio::test]
async fn test_presigned_post_conditions() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    let key = "conditions/exact-match.txt";
    let content = b"Condition test content";
    let content_type = "text/plain";

    // Policy with exact key match and content-type condition
    let (policy, credential, amz_date, signature) = build_signed_policy(
        &ctx.bucket,
        json!(["eq", "$key", key]),
        vec![json!(["starts-with", "$Content-Type", "text/"])],
    );

    let form = Form::new()
        .text("key", key.to_string())
        .text("Content-Type", content_type.to_string())
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(content.to_vec()).file_name("exact-match.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "Presigned POST with conditions failed: {} - {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    // Verify the object was uploaded with correct content type
    let head = ctx.head(key).await;
    assert_eq!(head.content_type().unwrap_or_default(), content_type);
}

/// Test presigned POST with content-length-range condition.
#[tokio::test]
async fn test_presigned_post_content_length() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    let key = "size-limited/small-file.txt";
    let content = b"Small content";

    // Policy allowing files 1-1000 bytes
    let (policy, credential, amz_date, signature) = build_signed_policy(
        &ctx.bucket,
        json!(["starts-with", "$key", "size-limited/"]),
        vec![json!(["content-length-range", 1, 1000])],
    );

    let form = Form::new()
        .text("key", key.to_string())
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(content.to_vec()).file_name("small.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Now try uploading a file that's too large
    let (policy, credential, amz_date, signature) = build_signed_policy(
        &ctx.bucket,
        json!(["starts-with", "$key", "size-limited/"]),
        vec![json!(["content-length-range", 1, 100])],
    );

    let large_content = vec![b'x'; 200];
    let form = Form::new()
        .text("key", "size-limited/large-file.txt")
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(large_content).file_name("large.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 400, "Expected 400 for file too large");
}

/// Test presigned POST with expired policy.
#[tokio::test]
async fn test_presigned_post_expiry() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    // Create an expired policy
    let now = Utc::now();
    let date = now.format("%Y%m%d").to_string();
    let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
    let expiration = (now - Duration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let credential = format!("{}/{}/{}/{}/aws4_request", ACCESS_KEY, date, REGION, SERVICE);

    let policy = json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": ctx.bucket},
            ["starts-with", "$key", ""],
            {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
            {"x-amz-credential": credential},
            {"x-amz-date": amz_date}
        ]
    });

    let policy_b64 = base64::engine::general_purpose::STANDARD.encode(policy.to_string());
    let signature = sign_policy(&policy_b64, &date, REGION);

    let form = Form::new()
        .text("key", "expired-upload.txt")
        .text("policy", policy_b64)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 400, "Expected 400 for expired policy");
    let body = response.text().await.unwrap_or_default();
    assert!(
        body.contains("ExpiredToken") || body.contains("expired"),
        "Expected expiration error, got: {}",
        body
    );
}

/// Test presigned POST with invalid signature.
#[tokio::test]
async fn test_presigned_post_invalid_signature() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    let (policy, credential, amz_date, _signature) =
        build_signed_policy(&ctx.bucket, json!(["starts-with", "$key", ""]), vec![]);

    // Use an invalid signature
    let invalid_signature = "0000000000000000000000000000000000000000000000000000000000000000";

    let form = Form::new()
        .text("key", "test.txt")
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", invalid_signature.to_string())
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 403, "Expected 403 for invalid signature");
}

/// Test presigned POST with key mismatch (policy says starts-with "uploads/" but key is different).
#[tokio::test]
async fn test_presigned_post_key_mismatch() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    let (policy, credential, amz_date, signature) =
        build_signed_policy(&ctx.bucket, json!(["starts-with", "$key", "uploads/"]), vec![]);

    // Try uploading with a key that doesn't match the policy
    let form = Form::new()
        .text("key", "other/invalid-key.txt")
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 403, "Expected 403 for key mismatch");
}

/// Test presigned POST with success_action_redirect.
#[tokio::test]
async fn test_presigned_post_redirect() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Failed to build client");

    let key = "redirect-presigned.txt";
    let redirect_url = "http://example.com/upload-success";

    let (policy, credential, amz_date, signature) = build_signed_policy(
        &ctx.bucket,
        json!(["eq", "$key", key]),
        vec![json!(["starts-with", "$success_action_redirect", "http://"])],
    );

    let form = Form::new()
        .text("key", key.to_string())
        .text("success_action_redirect", redirect_url.to_string())
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 303);
    let location = response.headers().get("location").expect("Missing Location header");
    assert!(location.to_str().unwrap().starts_with(redirect_url));
}

/// Test presigned POST with metadata.
#[tokio::test]
async fn test_presigned_post_metadata() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    let key = "metadata-presigned.txt";

    let (policy, credential, amz_date, signature) = build_signed_policy(
        &ctx.bucket,
        json!(["eq", "$key", key]),
        vec![
            json!(["starts-with", "$x-amz-meta-custom", ""]),
            json!(["starts-with", "$x-amz-meta-author", ""]),
        ],
    );

    let form = Form::new()
        .text("key", key.to_string())
        .text("x-amz-meta-custom", "custom-value")
        .text("x-amz-meta-author", "test-author")
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(b"metadata content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify metadata was stored
    let head = ctx.head(key).await;
    let metadata = head.metadata().unwrap();
    assert_eq!(metadata.get("custom"), Some(&"custom-value".to_string()));
    assert_eq!(metadata.get("author"), Some(&"test-author".to_string()));
}

/// Test presigned POST with success_action_status 201 (XML response).
#[tokio::test]
async fn test_presigned_post_status_201() {
    let ctx = S3TestContext::new_with_auth().await;
    let client = reqwest::Client::new();

    let key = "status-201-presigned.txt";

    let (policy, credential, amz_date, signature) = build_signed_policy(
        &ctx.bucket,
        json!(["eq", "$key", key]),
        vec![json!(["eq", "$success_action_status", "201"])],
    );

    let form = Form::new()
        .text("key", key.to_string())
        .text("success_action_status", "201")
        .text("policy", policy)
        .text("x-amz-algorithm", "AWS4-HMAC-SHA256")
        .text("x-amz-credential", credential)
        .text("x-amz-date", amz_date)
        .text("x-amz-signature", signature)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 201);
    let body = response.text().await.unwrap_or_default();
    assert!(body.contains("<PostResponse>"));
    assert!(body.contains("<Location>"));
    assert!(body.contains("<Bucket>"));
    assert!(body.contains("<Key>"));
    assert!(body.contains("<ETag>"));
}
