//! Bucket CORS tests.
//!
//! These tests cover CORS (Cross-Origin Resource Sharing) functionality.
//!
//! Ported from:
//! - Ceph s3-tests: test_cors_*

use reqwest::Client;

use crate::S3TestContext;

/// Create an HTTP client for CORS testing.
fn http_client() -> Client {
    Client::builder().build().expect("Failed to create HTTP client")
}

/// Test putting and getting CORS configuration.
/// Ceph: test_set_cors, test_get_cors
#[tokio::test]
async fn test_bucket_cors_put_get() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS configuration
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>http://example.com</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
        <AllowedMethod>PUT</AllowedMethod>
        <AllowedHeader>*</AllowedHeader>
        <ExposeHeader>x-amz-request-id</ExposeHeader>
        <MaxAgeSeconds>3000</MaxAgeSeconds>
    </CORSRule>
</CORSConfiguration>"#;

    let put_response = client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    assert!(put_response.status().is_success(), "PUT CORS failed: {:?}", put_response.status());

    // Get CORS configuration
    let get_response = client
        .get(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .send()
        .await
        .expect("Failed to get CORS");

    assert!(get_response.status().is_success(), "GET CORS failed: {:?}", get_response.status());

    let body = get_response.text().await.expect("Failed to read response");
    assert!(body.contains("http://example.com"), "Response should contain origin");
    assert!(body.contains("GET"), "Response should contain GET method");
    assert!(body.contains("PUT"), "Response should contain PUT method");
    assert!(body.contains("x-amz-request-id"), "Response should contain expose header");
    assert!(body.contains("3000"), "Response should contain max age");
}

/// Test deleting CORS configuration.
/// Ceph: test_delete_cors
#[tokio::test]
async fn test_bucket_cors_delete() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // First set CORS
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>*</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Delete CORS
    let delete_response = client
        .delete(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .send()
        .await
        .expect("Failed to delete CORS");

    assert_eq!(delete_response.status(), 204, "DELETE CORS should return 204");

    // Verify CORS is deleted (should return 404 NoSuchCORSConfiguration)
    let get_response = client
        .get(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .send()
        .await
        .expect("Failed to get CORS");

    assert_eq!(get_response.status(), 404, "GET CORS after delete should return 404");
}

/// Test CORS preflight request (OPTIONS).
/// Ceph: test_cors_origin_response
#[tokio::test]
async fn test_bucket_cors_preflight() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS configuration
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>http://example.com</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
        <AllowedMethod>PUT</AllowedMethod>
        <AllowedHeader>content-type</AllowedHeader>
        <AllowedHeader>authorization</AllowedHeader>
        <ExposeHeader>x-amz-request-id</ExposeHeader>
        <MaxAgeSeconds>3600</MaxAgeSeconds>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Send OPTIONS preflight request
    let options_response = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "PUT")
        .header("Access-Control-Request-Headers", "content-type, authorization")
        .send()
        .await
        .expect("Failed to send OPTIONS request");

    assert!(
        options_response.status().is_success(),
        "OPTIONS request failed: {:?}",
        options_response.status()
    );

    // Check CORS headers in response
    let headers = options_response.headers();
    assert_eq!(
        headers.get("Access-Control-Allow-Origin").and_then(|v| v.to_str().ok()),
        Some("http://example.com"),
        "Should have correct Access-Control-Allow-Origin"
    );
    assert!(
        headers.get("Access-Control-Allow-Methods").is_some(),
        "Should have Access-Control-Allow-Methods"
    );
    assert!(headers.get("Access-Control-Max-Age").is_some(), "Should have Access-Control-Max-Age");
}

/// Test CORS with wildcard origin.
/// Ceph: test_cors_origin_wildcard
#[tokio::test]
async fn test_bucket_cors_wildcard_origin() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS with wildcard origin
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>*</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
        <AllowedMethod>HEAD</AllowedMethod>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Test with any origin
    let options_response = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://any-origin.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .expect("Failed to send OPTIONS request");

    assert!(options_response.status().is_success(), "OPTIONS request failed");

    let headers = options_response.headers();
    assert!(
        headers.get("Access-Control-Allow-Origin").is_some(),
        "Should have Access-Control-Allow-Origin for wildcard"
    );
}

/// Test CORS no matching rule returns 403.
/// Ceph: test_cors_no_match
#[tokio::test]
async fn test_bucket_cors_no_match() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS with specific origin
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>http://allowed.com</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Test with non-matching origin
    let options_response = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://not-allowed.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .expect("Failed to send OPTIONS request");

    assert_eq!(options_response.status(), 403, "Non-matching origin should return 403");
}

/// Test CORS multiple rules.
/// Ceph: test_cors_multiple_rules
#[tokio::test]
async fn test_bucket_cors_multiple_rules() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS with multiple rules
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <ID>rule1</ID>
        <AllowedOrigin>http://first.com</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
    </CORSRule>
    <CORSRule>
        <ID>rule2</ID>
        <AllowedOrigin>http://second.com</AllowedOrigin>
        <AllowedMethod>PUT</AllowedMethod>
        <AllowedMethod>POST</AllowedMethod>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Test first rule
    let response1 = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://first.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .expect("Failed to send OPTIONS");

    assert!(response1.status().is_success(), "First rule should match");

    // Test second rule
    let response2 = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://second.com")
        .header("Access-Control-Request-Method", "PUT")
        .send()
        .await
        .expect("Failed to send OPTIONS");

    assert!(response2.status().is_success(), "Second rule should match");
}

/// Test CORS OPTIONS on object path.
/// Ceph: test_cors_options
#[tokio::test]
async fn test_bucket_cors_options_object() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS configuration
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>http://example.com</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
        <AllowedMethod>PUT</AllowedMethod>
        <AllowedHeader>*</AllowedHeader>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Put an object
    ctx.put("test-object", b"test data").await;

    // Send OPTIONS request to object path
    let options_response = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}/test-object", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .expect("Failed to send OPTIONS request");

    assert!(options_response.status().is_success(), "OPTIONS on object path should succeed");
    assert!(
        options_response.headers().get("Access-Control-Allow-Origin").is_some(),
        "Should have CORS headers on object path"
    );
}

/// Test CORS on non-existent bucket returns 404.
/// Ceph: test_cors_nonexistent
#[tokio::test]
async fn test_bucket_cors_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;
    let client = http_client();

    let response = client
        .get(format!("{}/nonexistent-bucket-12345?cors", ctx.endpoint))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .send()
        .await
        .expect("Failed to send request");

    assert_eq!(response.status(), 404, "Should return 404 for non-existent bucket");
}

/// Test CORS with wildcard header matching.
#[tokio::test]
async fn test_bucket_cors_wildcard_headers() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS with wildcard headers
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>http://example.com</AllowedOrigin>
        <AllowedMethod>PUT</AllowedMethod>
        <AllowedHeader>*</AllowedHeader>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Test with custom headers
    let options_response = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "PUT")
        .header("Access-Control-Request-Headers", "x-custom-header, x-another-header")
        .send()
        .await
        .expect("Failed to send OPTIONS request");

    assert!(options_response.status().is_success(), "Wildcard headers should match any header");
}

/// Test CORS preflight fails for non-matching method.
#[tokio::test]
async fn test_bucket_cors_method_mismatch() {
    let ctx = S3TestContext::new().await;
    let client = http_client();

    // Set CORS with only GET allowed
    let cors_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
    <CORSRule>
        <AllowedOrigin>http://example.com</AllowedOrigin>
        <AllowedMethod>GET</AllowedMethod>
    </CORSRule>
</CORSConfiguration>"#;

    client
        .put(format!("{}/{}?cors", ctx.endpoint, ctx.bucket))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .header("Content-Type", "application/xml")
        .body(cors_xml)
        .send()
        .await
        .expect("Failed to set CORS");

    // Test with non-allowed method
    let options_response = client
        .request(reqwest::Method::OPTIONS, format!("{}/{}", ctx.endpoint, ctx.bucket))
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "DELETE")
        .send()
        .await
        .expect("Failed to send OPTIONS request");

    assert_eq!(options_response.status(), 403, "Non-matching method should return 403");
}

// =============================================================================
// Tests that require additional features (marked ignore)
// =============================================================================

/// Test CORS Access-Control-Allow-Credentials.
/// Ceph: test_cors_credentials
#[tokio::test]
#[ignore = "Credentials support not implemented"]
async fn test_bucket_cors_credentials() {
    let _ctx = S3TestContext::new().await;
}

/// Test CORS with presigned URL.
/// Ceph: test_cors_presigned
#[tokio::test]
#[ignore = "CORS headers on presigned URLs not implemented"]
async fn test_bucket_cors_presigned() {
    let _ctx = S3TestContext::new().await;
}
