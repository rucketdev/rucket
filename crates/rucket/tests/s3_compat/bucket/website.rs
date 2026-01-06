//! Bucket website configuration tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_s3_website.py
//! - AWS S3 documentation: Static website hosting

use aws_sdk_s3::types::{
    Condition, ErrorDocument, IndexDocument, Redirect, RedirectAllRequestsTo, RoutingRule,
    WebsiteConfiguration,
};

use crate::S3TestContext;

/// Test getting website configuration on bucket with no config.
/// Ceph: test_get_bucket_website_none
#[tokio::test]
async fn test_bucket_website_get_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.get_bucket_website().bucket(&ctx.bucket).send().await;

    // Should return NoSuchWebsiteConfiguration error
    assert!(result.is_err(), "Should fail when no website config exists");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(
        error_str.contains("NoSuchWebsiteConfiguration"),
        "Expected NoSuchWebsiteConfiguration error, got: {}",
        error_str
    );
}

/// Test putting and getting basic website configuration.
/// Ceph: test_set_bucket_website_index
#[tokio::test]
async fn test_bucket_website_put_get_index() {
    let ctx = S3TestContext::new().await;

    let config = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration");

    let response = ctx
        .client
        .get_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get website configuration");

    let suffix = response.index_document().map(|d| d.suffix());
    assert_eq!(suffix, Some("index.html"), "Index document should be index.html");
}

/// Test website configuration with error document.
/// Ceph: test_set_bucket_website_error
#[tokio::test]
async fn test_bucket_website_error_document() {
    let ctx = S3TestContext::new().await;

    let config = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .error_document(ErrorDocument::builder().key("error.html").build().unwrap())
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration");

    let response = ctx
        .client
        .get_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get website configuration");

    let key = response.error_document().map(|d| d.key());
    assert_eq!(key, Some("error.html"), "Error document should be error.html");
}

/// Test deleting website configuration.
/// Ceph: test_delete_bucket_website
#[tokio::test]
async fn test_bucket_website_delete() {
    let ctx = S3TestContext::new().await;

    // First put a website config
    let config = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration");

    // Delete the config
    ctx.client
        .delete_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should delete website configuration");

    // Verify it's gone
    let result = ctx.client.get_bucket_website().bucket(&ctx.bucket).send().await;
    assert!(result.is_err(), "Website config should be deleted");
}

/// Test website configuration with redirect all requests.
/// Ceph: test_set_bucket_website_redirect_all
#[tokio::test]
async fn test_bucket_website_redirect_all() {
    let ctx = S3TestContext::new().await;

    let config = WebsiteConfiguration::builder()
        .redirect_all_requests_to(
            RedirectAllRequestsTo::builder()
                .host_name("example.com")
                .protocol(aws_sdk_s3::types::Protocol::Https)
                .build()
                .unwrap(),
        )
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration with redirect");

    let response = ctx
        .client
        .get_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get website configuration");

    let redirect = response.redirect_all_requests_to().expect("Should have redirect config");
    assert_eq!(redirect.host_name(), "example.com");
}

/// Test website configuration with routing rules.
/// Ceph: test_set_bucket_website_routing_rules
#[tokio::test]
async fn test_bucket_website_routing_rules() {
    let ctx = S3TestContext::new().await;

    let rule = RoutingRule::builder()
        .condition(Condition::builder().key_prefix_equals("docs/").build())
        .redirect(Redirect::builder().replace_key_prefix_with("documents/").build())
        .build();

    let config = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .routing_rules(rule)
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration with routing rules");

    let response = ctx
        .client
        .get_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get website configuration");

    let rules = response.routing_rules();
    assert_eq!(rules.len(), 1, "Should have one routing rule");
}

/// Test website configuration with HTTP error code condition.
/// Ceph: test_set_bucket_website_routing_http_error
#[tokio::test]
async fn test_bucket_website_routing_http_error() {
    let ctx = S3TestContext::new().await;

    let rule = RoutingRule::builder()
        .condition(Condition::builder().http_error_code_returned_equals("404").build())
        .redirect(Redirect::builder().replace_key_with("404.html").build())
        .build();

    let config = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .routing_rules(rule)
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration with HTTP error routing");
}

/// Test website configuration with multiple routing rules.
/// Ceph: test_set_bucket_website_routing_multiple
#[tokio::test]
async fn test_bucket_website_routing_multiple_rules() {
    let ctx = S3TestContext::new().await;

    let rule1 = RoutingRule::builder()
        .condition(Condition::builder().key_prefix_equals("old/").build())
        .redirect(Redirect::builder().replace_key_prefix_with("new/").build())
        .build();

    let rule2 = RoutingRule::builder()
        .condition(Condition::builder().http_error_code_returned_equals("404").build())
        .redirect(Redirect::builder().replace_key_with("404.html").build())
        .build();

    let config = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .routing_rules(rule1)
        .routing_rules(rule2)
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration with multiple rules");

    let response = ctx
        .client
        .get_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get website configuration");

    assert_eq!(response.routing_rules().len(), 2, "Should have two routing rules");
}

/// Test website configuration without index document (invalid).
/// Ceph: test_set_bucket_website_empty
#[tokio::test]
async fn test_bucket_website_empty_config() {
    let ctx = S3TestContext::new().await;

    // Empty config without index document should fail
    let config = WebsiteConfiguration::builder().build();

    let result = ctx
        .client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await;

    // Should fail - either index or redirect is required
    assert!(result.is_err(), "Should reject empty website configuration");
}

/// Test website configuration on non-existent bucket.
/// Ceph: test_get_bucket_website_nonexistent_bucket
#[tokio::test]
async fn test_bucket_website_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.get_bucket_website().bucket("nonexistent-bucket-12345").send().await;

    assert!(result.is_err(), "Should fail on non-existent bucket");
}

/// Test website with redirect to external host.
/// Ceph: test_set_bucket_website_redirect_external
#[tokio::test]
async fn test_bucket_website_redirect_external() {
    let ctx = S3TestContext::new().await;

    let config = WebsiteConfiguration::builder()
        .redirect_all_requests_to(
            RedirectAllRequestsTo::builder()
                .host_name("www.example.com")
                .protocol(aws_sdk_s3::types::Protocol::Https)
                .build()
                .unwrap(),
        )
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put redirect to external host");

    let response = ctx
        .client
        .get_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get website configuration");

    let redirect = response.redirect_all_requests_to().unwrap();
    assert_eq!(redirect.host_name(), "www.example.com");
    assert_eq!(redirect.protocol(), Some(&aws_sdk_s3::types::Protocol::Https));
}

/// Test website with routing rule using hostname redirect.
/// Ceph: test_set_bucket_website_routing_hostname
#[tokio::test]
async fn test_bucket_website_routing_hostname() {
    let ctx = S3TestContext::new().await;

    let rule = RoutingRule::builder()
        .condition(Condition::builder().key_prefix_equals("legacy/").build())
        .redirect(Redirect::builder().host_name("legacy.example.com").build())
        .build();

    let config = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .routing_rules(rule)
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config)
        .send()
        .await
        .expect("Should put website configuration with hostname redirect");
}

/// Test updating website configuration.
/// Ceph: test_update_bucket_website
#[tokio::test]
async fn test_bucket_website_update() {
    let ctx = S3TestContext::new().await;

    // Initial config
    let config1 = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("index.html").build().unwrap())
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config1)
        .send()
        .await
        .expect("Should put initial config");

    // Update config
    let config2 = WebsiteConfiguration::builder()
        .index_document(IndexDocument::builder().suffix("home.html").build().unwrap())
        .error_document(ErrorDocument::builder().key("error.html").build().unwrap())
        .build();

    ctx.client
        .put_bucket_website()
        .bucket(&ctx.bucket)
        .website_configuration(config2)
        .send()
        .await
        .expect("Should update config");

    let response = ctx
        .client
        .get_bucket_website()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should get updated config");

    let suffix = response.index_document().map(|d| d.suffix());
    assert_eq!(suffix, Some("home.html"), "Index document should be updated to home.html");
    assert!(response.error_document().is_some(), "Error document should be set");
}
