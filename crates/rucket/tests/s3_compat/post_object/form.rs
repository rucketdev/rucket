//! POST object form upload tests.

use base64::Engine;
use chrono::{Duration, Utc};
use reqwest::multipart::{Form, Part};
use serde_json::json;

use crate::S3TestContext;

/// Helper to build a POST policy JSON document.
fn build_policy(bucket: &str, key_prefix: &str, conditions: Vec<serde_json::Value>) -> String {
    let expiration = (Utc::now() + Duration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ").to_string();

    let mut all_conditions =
        vec![json!({"bucket": bucket}), json!(["starts-with", "$key", key_prefix])];
    all_conditions.extend(conditions);

    let policy = json!({
        "expiration": expiration,
        "conditions": all_conditions
    });

    policy.to_string()
}

/// Encode policy as base64.
fn encode_policy(policy: &str) -> String {
    base64::engine::general_purpose::STANDARD.encode(policy)
}

/// Test POST object basic upload without policy (anonymous).
#[tokio::test]
async fn test_post_object_basic() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "test-file.txt";
    let content = b"Hello, World!";

    let form = Form::new()
        .text("key", key.to_string())
        .part("file", Part::bytes(content.to_vec()).file_name("test-file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "POST failed with status: {} - {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    // Verify the object was uploaded
    let data = ctx.get(key).await;
    assert_eq!(data, content);
}

/// Test POST object with policy.
#[tokio::test]
async fn test_post_object_policy() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "uploads/test-file.txt";
    let content = b"Policy protected content";

    let policy_json = build_policy(&ctx.bucket, "uploads/", vec![]);
    let policy = encode_policy(&policy_json);

    let form = Form::new()
        .text("key", key.to_string())
        .text("policy", policy)
        .part("file", Part::bytes(content.to_vec()).file_name("test-file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(
        response.status().is_success(),
        "POST with policy failed: {} - {}",
        response.status(),
        response.text().await.unwrap_or_default()
    );

    let data = ctx.get(key).await;
    assert_eq!(data, content);
}

/// Test POST object with key prefix using starts-with.
#[tokio::test]
async fn test_post_object_key_prefix() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "user/123/document.pdf";
    let content = b"PDF content";

    let policy_json = build_policy(&ctx.bucket, "user/", vec![]);
    let policy = encode_policy(&policy_json);

    let form = Form::new()
        .text("key", key.to_string())
        .text("policy", policy)
        .part("file", Part::bytes(content.to_vec()).file_name("document.pdf"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());
    let data = ctx.get(key).await;
    assert_eq!(data, content);
}

/// Test POST object with expired policy.
#[tokio::test]
async fn test_post_object_expired() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    // Create an expired policy
    let expiration = (Utc::now() - Duration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let policy = json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": ctx.bucket},
            ["starts-with", "$key", ""]
        ]
    });
    let policy_b64 = encode_policy(&policy.to_string());

    let form = Form::new()
        .text("key", "expired-upload.txt")
        .text("policy", policy_b64)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 400);
    let body = response.text().await.unwrap_or_default();
    assert!(body.contains("ExpiredToken") || body.contains("expired"));
}

/// Test POST object content length limit.
#[tokio::test]
async fn test_post_object_size_limit() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    // Policy with content-length-range: 1 to 100 bytes
    let expiration = (Utc::now() + Duration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let policy = json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": ctx.bucket},
            ["starts-with", "$key", ""],
            ["content-length-range", 1, 100]
        ]
    });
    let policy_b64 = encode_policy(&policy.to_string());

    // Upload content larger than 100 bytes
    let large_content = vec![b'x'; 200];
    let form = Form::new()
        .text("key", "large-file.txt")
        .text("policy", policy_b64)
        .part("file", Part::bytes(large_content).file_name("large.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 400);
    let body = response.text().await.unwrap_or_default();
    assert!(body.contains("EntityTooSmall") || body.contains("size") || body.contains("range"));
}

/// Test POST object with success action redirect.
#[tokio::test]
async fn test_post_object_success_redirect() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .expect("Failed to build client");

    let key = "redirect-test.txt";
    let redirect_url = "http://example.com/success";

    let form = Form::new()
        .text("key", key.to_string())
        .text("success_action_redirect", redirect_url.to_string())
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 303);
    let location = response.headers().get("location").expect("Missing Location header");
    let location_str = location.to_str().unwrap();
    assert!(location_str.starts_with(redirect_url));
    assert!(location_str.contains("bucket="));
    assert!(location_str.contains("key="));
    assert!(location_str.contains("etag="));
}

/// Test POST object with success action status 200.
#[tokio::test]
async fn test_post_object_success_status_200() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "status-200.txt";
    let form = Form::new()
        .text("key", key.to_string())
        .text("success_action_status", "200")
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 200);
}

/// Test POST object with success action status 201.
#[tokio::test]
async fn test_post_object_success_status_201() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "status-201.txt";
    let form = Form::new()
        .text("key", key.to_string())
        .text("success_action_status", "201")
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

/// Test POST object with success action status 204 (default).
#[tokio::test]
async fn test_post_object_success_status_204() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "status-204.txt";
    let form = Form::new()
        .text("key", key.to_string())
        .text("success_action_status", "204")
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 204);
}

/// Test POST object with content-type.
#[tokio::test]
async fn test_post_object_content_type() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "typed-file.json";
    let content_type = "application/json";

    let form = Form::new()
        .text("key", key.to_string())
        .text("Content-Type", content_type.to_string())
        .part("file", Part::bytes(b"{}".to_vec()).file_name("file.json"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify content type was stored
    let head = ctx.head(key).await;
    assert_eq!(head.content_type().unwrap_or_default(), content_type);
}

/// Test POST object starts-with condition.
#[tokio::test]
async fn test_post_object_starts_with() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    // Valid: key starts with "prefix/"
    let policy_json = build_policy(&ctx.bucket, "prefix/", vec![]);
    let policy = encode_policy(&policy_json);

    let form = Form::new()
        .text("key", "prefix/valid-key.txt")
        .text("policy", policy)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Invalid: key does not start with "prefix/"
    let policy_json = build_policy(&ctx.bucket, "prefix/", vec![]);
    let policy = encode_policy(&policy_json);

    let form = Form::new()
        .text("key", "other/invalid-key.txt")
        .text("policy", policy)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 403);
}

/// Test POST object eq condition.
#[tokio::test]
async fn test_post_object_eq_condition() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let expiration = (Utc::now() + Duration::hours(1)).format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let policy = json!({
        "expiration": expiration,
        "conditions": [
            {"bucket": ctx.bucket},
            ["eq", "$key", "exact-key.txt"]
        ]
    });
    let policy_b64 = encode_policy(&policy.to_string());

    // Valid: exact key match
    let form = Form::new()
        .text("key", "exact-key.txt")
        .text("policy", policy_b64.clone())
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Invalid: different key
    let form = Form::new()
        .text("key", "different-key.txt")
        .text("policy", policy_b64)
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 403);
}

/// Test POST object with metadata.
#[tokio::test]
async fn test_post_object_metadata() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "metadata-file.txt";

    let form = Form::new()
        .text("key", key.to_string())
        .text("x-amz-meta-custom", "custom-value")
        .text("x-amz-meta-author", "test-author")
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

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

/// Test POST object key variable ${filename}.
#[tokio::test]
async fn test_post_object_filename_variable() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    // Use ${filename} in key
    let form = Form::new()
        .text("key", "uploads/${filename}")
        .part("file", Part::bytes(b"content".to_vec()).file_name("my-document.pdf"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify the filename was substituted
    assert!(ctx.exists("uploads/my-document.pdf").await);
}

/// Test POST object missing required field.
#[tokio::test]
async fn test_post_object_missing_field() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    // Missing key field
    let form = Form::new().part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 400);

    // Missing file field
    let form = Form::new().text("key", "test.txt");

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 400);
}

/// Test POST object with storage class.
#[tokio::test]
async fn test_post_object_storage_class() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "cold-storage.txt";

    let form = Form::new()
        .text("key", key.to_string())
        .text("x-amz-storage-class", "GLACIER")
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify storage class was set
    let head = ctx.head(key).await;
    assert_eq!(head.storage_class().map(|sc| sc.as_str()), Some("GLACIER"));
}

/// Test POST object to versioned bucket.
#[tokio::test]
async fn test_post_object_versioned() {
    let ctx = S3TestContext::with_versioning().await;
    let client = reqwest::Client::new();

    let key = "versioned-file.txt";

    // First upload
    let form = Form::new()
        .text("key", key.to_string())
        .part("file", Part::bytes(b"version 1".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Second upload (new version)
    let form = Form::new()
        .text("key", key.to_string())
        .part("file", Part::bytes(b"version 2".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify we have 2 versions
    let versions = ctx.list_versions().await;
    let file_versions: Vec<_> =
        versions.versions().iter().filter(|v| v.key() == Some(key)).collect();
    assert_eq!(file_versions.len(), 2);
}

/// Test POST object to non-existent bucket.
#[tokio::test]
async fn test_post_object_no_bucket() {
    let ctx = S3TestContext::without_bucket().await;
    let client = reqwest::Client::new();

    let form = Form::new()
        .text("key", "test.txt")
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/nonexistent-bucket", ctx.endpoint))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 404);
}

/// Test POST object with Cache-Control header.
#[tokio::test]
async fn test_post_object_cache_control() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "cached-file.txt";
    let cache_control = "max-age=3600, public";

    let form = Form::new()
        .text("key", key.to_string())
        .text("Cache-Control", cache_control.to_string())
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify Cache-Control was stored
    let head = ctx.head(key).await;
    assert_eq!(head.cache_control().unwrap_or_default(), cache_control);
}

/// Test POST object with Content-Disposition header.
#[tokio::test]
async fn test_post_object_content_disposition() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "download-file.txt";
    let content_disposition = "attachment; filename=\"download.txt\"";

    let form = Form::new()
        .text("key", key.to_string())
        .text("Content-Disposition", content_disposition.to_string())
        .part("file", Part::bytes(b"content".to_vec()).file_name("file.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify Content-Disposition was stored
    let head = ctx.head(key).await;
    assert_eq!(head.content_disposition().unwrap_or_default(), content_disposition);
}

/// Test POST object invalid Content-Type (not multipart/form-data).
#[tokio::test]
async fn test_post_object_invalid_content_type() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Content-Type", "application/json")
        .body("{}")
        .send()
        .await
        .expect("Request failed");

    assert_eq!(response.status().as_u16(), 400);
}

/// Test POST object with empty file.
#[tokio::test]
async fn test_post_object_empty_file() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "empty-file.txt";
    let form = Form::new()
        .text("key", key.to_string())
        .part("file", Part::bytes(Vec::new()).file_name("empty.txt"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify empty file was created
    let data = ctx.get(key).await;
    assert!(data.is_empty());
}

/// Test POST object with large file.
#[tokio::test]
async fn test_post_object_large_file() {
    let ctx = S3TestContext::new().await;
    let client = reqwest::Client::new();

    let key = "large-file.bin";
    let content = vec![0u8; 1024 * 1024]; // 1 MB

    let form = Form::new()
        .text("key", key.to_string())
        .part("file", Part::bytes(content.clone()).file_name("large.bin"));

    let response = client
        .post(format!("{}/{}", ctx.endpoint, ctx.bucket))
        .multipart(form)
        .send()
        .await
        .expect("Request failed");

    assert!(response.status().is_success());

    // Verify content
    let data = ctx.get(key).await;
    assert_eq!(data.len(), content.len());
    assert_eq!(data, content);
}
