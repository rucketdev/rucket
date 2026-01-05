//! Bucket encryption tests.
//!
//! Tests for S3 bucket default encryption configuration including:
//! - PUT bucket encryption (SSE-S3)
//! - GET bucket encryption
//! - DELETE bucket encryption
//!
//! Note: Default bucket encryption IS implemented in Rucket.
//! Working SSE-S3 object-level tests are in `encryption/sse_s3.rs`.
//!
//! Ported from:
//! - Ceph s3-tests: test_encryption_*

use aws_sdk_s3::types::{
    ServerSideEncryption, ServerSideEncryptionByDefault, ServerSideEncryptionConfiguration,
    ServerSideEncryptionRule,
};

use crate::S3TestContext;

/// Helper to create SSE-S3 (AES256) encryption configuration.
fn sse_s3_config() -> ServerSideEncryptionConfiguration {
    let sse_config = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .expect("Failed to build SSE config");

    let rule = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(sse_config)
        .build();

    ServerSideEncryptionConfiguration::builder()
        .rules(rule)
        .build()
        .expect("Failed to build encryption configuration")
}

// =============================================================================
// Basic Bucket Encryption CRUD Tests
// =============================================================================

/// Test putting bucket encryption configuration (SSE-S3).
#[tokio::test]
async fn test_bucket_encryption_put_sse_s3() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .put_bucket_encryption()
        .bucket(&ctx.bucket)
        .server_side_encryption_configuration(sse_s3_config())
        .send()
        .await;

    assert!(result.is_ok(), "PUT bucket encryption should succeed");
}

/// Test getting bucket encryption configuration.
#[tokio::test]
async fn test_bucket_encryption_get() {
    let ctx = S3TestContext::new().await;

    // First set encryption
    ctx.client
        .put_bucket_encryption()
        .bucket(&ctx.bucket)
        .server_side_encryption_configuration(sse_s3_config())
        .send()
        .await
        .expect("PUT encryption should succeed");

    // Get encryption configuration
    let result = ctx
        .client
        .get_bucket_encryption()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET encryption should succeed");

    // Verify the configuration
    let rules = result.server_side_encryption_configuration().map(|c| c.rules());
    assert!(rules.is_some(), "Should have encryption rules");

    let rules = rules.unwrap();
    assert!(!rules.is_empty(), "Should have at least one rule");

    let default_encryption = rules[0].apply_server_side_encryption_by_default();
    assert!(default_encryption.is_some(), "Rule should have default encryption");
    assert_eq!(default_encryption.unwrap().sse_algorithm(), &ServerSideEncryption::Aes256);
}

/// Test deleting bucket encryption configuration.
#[tokio::test]
async fn test_bucket_encryption_delete() {
    let ctx = S3TestContext::new().await;

    // First set encryption
    ctx.client
        .put_bucket_encryption()
        .bucket(&ctx.bucket)
        .server_side_encryption_configuration(sse_s3_config())
        .send()
        .await
        .expect("PUT encryption should succeed");

    // Verify it exists
    ctx.client
        .get_bucket_encryption()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET encryption should succeed before delete");

    // Delete encryption configuration
    ctx.client
        .delete_bucket_encryption()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("DELETE encryption should succeed");

    // Verify it's deleted - should return error
    let result = ctx.client.get_bucket_encryption().bucket(&ctx.bucket).send().await;

    assert!(
        result.is_err(),
        "GET encryption should fail after delete (ServerSideEncryptionConfigurationNotFoundError)"
    );
}

// =============================================================================
// Error Handling Tests
// =============================================================================

/// Test GET bucket encryption for non-existent bucket returns error.
#[tokio::test]
async fn test_bucket_encryption_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .get_bucket_encryption()
        .bucket("nonexistent-bucket-for-encryption-test")
        .send()
        .await;

    assert!(result.is_err(), "GET encryption for non-existent bucket should fail");
}

/// Test GET bucket encryption for bucket without encryption returns error.
#[tokio::test]
async fn test_bucket_encryption_not_configured() {
    let ctx = S3TestContext::new().await;

    // Try to get encryption on bucket that was just created (no encryption set)
    let result = ctx.client.get_bucket_encryption().bucket(&ctx.bucket).send().await;

    // Should return ServerSideEncryptionConfigurationNotFoundError
    assert!(result.is_err(), "GET encryption should fail when not configured");
}

/// Test DELETE bucket encryption for non-existent bucket returns error.
#[tokio::test]
async fn test_bucket_encryption_delete_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .delete_bucket_encryption()
        .bucket("nonexistent-bucket-for-delete-encryption")
        .send()
        .await;

    assert!(result.is_err(), "DELETE encryption for non-existent bucket should fail");
}

// =============================================================================
// Encryption Overwrite Tests
// =============================================================================

/// Test overwriting bucket encryption configuration.
#[tokio::test]
async fn test_bucket_encryption_overwrite() {
    let ctx = S3TestContext::new().await;

    // Set initial encryption
    ctx.client
        .put_bucket_encryption()
        .bucket(&ctx.bucket)
        .server_side_encryption_configuration(sse_s3_config())
        .send()
        .await
        .expect("First PUT should succeed");

    // Overwrite with new configuration (same algorithm, just testing overwrite works)
    let sse_config2 = ServerSideEncryptionByDefault::builder()
        .sse_algorithm(ServerSideEncryption::Aes256)
        .build()
        .expect("Failed to build SSE config");

    let rule2 = ServerSideEncryptionRule::builder()
        .apply_server_side_encryption_by_default(sse_config2)
        .bucket_key_enabled(true)
        .build();

    let config2 = ServerSideEncryptionConfiguration::builder()
        .rules(rule2)
        .build()
        .expect("Failed to build encryption configuration");

    ctx.client
        .put_bucket_encryption()
        .bucket(&ctx.bucket)
        .server_side_encryption_configuration(config2)
        .send()
        .await
        .expect("Second PUT should succeed");

    // Verify the new configuration
    let result = ctx
        .client
        .get_bucket_encryption()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET should succeed");

    let rules = result.server_side_encryption_configuration().map(|c| c.rules());
    assert!(rules.is_some(), "Should have encryption rules");
}

/// Test bucket encryption persists across operations.
#[tokio::test]
async fn test_bucket_encryption_persist() {
    let ctx = S3TestContext::new().await;

    // Set encryption
    ctx.client
        .put_bucket_encryption()
        .bucket(&ctx.bucket)
        .server_side_encryption_configuration(sse_s3_config())
        .send()
        .await
        .expect("PUT encryption should succeed");

    // Do some other operation (put object)
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test-object.txt")
        .body(Vec::from("test content").into())
        .send()
        .await
        .expect("PUT object should succeed");

    // Encryption should still be configured
    let result = ctx
        .client
        .get_bucket_encryption()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("GET encryption should succeed after other operations");

    let rules = result.server_side_encryption_configuration().map(|c| c.rules());
    assert!(rules.is_some(), "Encryption rules should persist");
}
