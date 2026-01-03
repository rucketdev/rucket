//! Object Lock integration tests.
//!
//! Tests for S3 Object Lock functionality including:
//! - Bucket-level Object Lock configuration
//! - Object-level retention (Governance and Compliance modes)
//! - Legal hold
//! - Delete protection

use aws_sdk_s3::types::{
    DefaultRetention, ObjectLockConfiguration, ObjectLockEnabled, ObjectLockLegalHold,
    ObjectLockLegalHoldStatus, ObjectLockRetention, ObjectLockRetentionMode, ObjectLockRule,
};

use crate::harness::{random_key, S3TestContext};

// =============================================================================
// Bucket-Level Object Lock Configuration Tests
// =============================================================================

/// Test getting Object Lock configuration from a bucket with Object Lock enabled.
#[tokio::test]
async fn test_object_lock_get_configuration() {
    let ctx = S3TestContext::with_object_lock().await;

    let config = ctx.get_object_lock_configuration().await;
    let lock_config = config.object_lock_configuration().expect("Should have config");

    assert_eq!(lock_config.object_lock_enabled(), Some(&ObjectLockEnabled::Enabled));
}

/// Test putting Object Lock configuration with default retention.
#[tokio::test]
async fn test_object_lock_put_configuration() {
    let ctx = S3TestContext::with_object_lock().await;

    // Set default retention of 1 day in Governance mode
    let rule = ObjectLockRule::builder()
        .default_retention(
            DefaultRetention::builder().mode(ObjectLockRetentionMode::Governance).days(1).build(),
        )
        .build();

    let config = ObjectLockConfiguration::builder()
        .object_lock_enabled(ObjectLockEnabled::Enabled)
        .rule(rule)
        .build();

    ctx.put_object_lock_configuration(config).await;

    // Verify the configuration was set
    let retrieved = ctx.get_object_lock_configuration().await;
    let lock_config = retrieved.object_lock_configuration().expect("Should have config");
    let rule = lock_config.rule().expect("Should have rule");
    let retention = rule.default_retention().expect("Should have default retention");

    assert_eq!(retention.mode(), Some(&ObjectLockRetentionMode::Governance));
    assert_eq!(retention.days(), Some(1));
}

/// Test that Object Lock buckets automatically have versioning enabled.
#[tokio::test]
async fn test_object_lock_requires_versioning() {
    let ctx = S3TestContext::with_object_lock().await;

    // Put an object and verify it gets a version ID
    let key = random_key();
    let result = ctx.put(&key, b"test data").await;

    // Object Lock buckets must have versioning, so version_id should be present
    assert!(result.version_id().is_some(), "Object Lock bucket should have versioning enabled");
}

// =============================================================================
// Object Retention Tests
// =============================================================================

/// Test putting and getting object retention in Governance mode.
#[tokio::test]
async fn test_object_lock_governance_mode() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"governance test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set retention for 1 day from now
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(retain_until.timestamp_millis()))
        .build();

    ctx.put_object_retention(&key, retention, Some(version_id), false).await;

    // Verify retention was set
    let retrieved = ctx.get_object_retention(&key, Some(version_id)).await;
    let ret = retrieved.retention().expect("Should have retention");

    assert_eq!(ret.mode(), Some(&ObjectLockRetentionMode::Governance));
}

/// Test putting and getting object retention in Compliance mode.
#[tokio::test]
async fn test_object_lock_compliance_mode() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"compliance test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set retention for 1 day from now
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Compliance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(retain_until.timestamp_millis()))
        .build();

    ctx.put_object_retention(&key, retention, Some(version_id), false).await;

    // Verify retention was set
    let retrieved = ctx.get_object_retention(&key, Some(version_id)).await;
    let ret = retrieved.retention().expect("Should have retention");

    assert_eq!(ret.mode(), Some(&ObjectLockRetentionMode::Compliance));
}

/// Test that Governance mode retention can be bypassed with the bypass header.
#[tokio::test]
async fn test_object_lock_retention_bypass_governance() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"bypass test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set retention for 1 day from now
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(retain_until.timestamp_millis()))
        .build();

    ctx.put_object_retention(&key, retention, Some(version_id), false).await;

    // Try to delete without bypass - should fail
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), false).await;
    assert!(
        delete_result.is_err(),
        "Delete without bypass should fail for governance-locked object"
    );

    // Delete with bypass - should succeed
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), true).await;
    assert!(
        delete_result.is_ok(),
        "Delete with bypass should succeed for governance-locked object"
    );
}

/// Test that Compliance mode retention cannot be bypassed.
#[tokio::test]
async fn test_object_lock_compliance_no_bypass() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"compliance no bypass").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set retention for 1 day from now
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Compliance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(retain_until.timestamp_millis()))
        .build();

    ctx.put_object_retention(&key, retention, Some(version_id), false).await;

    // Try to delete with bypass - should still fail for compliance
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), true).await;
    assert!(delete_result.is_err(), "Delete with bypass should fail for compliance-locked object");
}

/// Test extending retention period is allowed.
#[tokio::test]
async fn test_object_lock_extend_retention() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"extend test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set initial retention for 1 day
    let retain_until_1 = chrono::Utc::now() + chrono::Duration::days(1);
    let retention1 = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(
            retain_until_1.timestamp_millis(),
        ))
        .build();

    ctx.put_object_retention(&key, retention1, Some(version_id), false).await;

    // Extend to 2 days - should succeed
    let retain_until_2 = chrono::Utc::now() + chrono::Duration::days(2);
    let retention2 = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(
            retain_until_2.timestamp_millis(),
        ))
        .build();

    ctx.put_object_retention(&key, retention2, Some(version_id), false).await;

    // Verify extension worked
    let retrieved = ctx.get_object_retention(&key, Some(version_id)).await;
    let ret = retrieved.retention().expect("Should have retention");
    let date = ret.retain_until_date().expect("Should have date");

    // The date should be approximately 2 days from now
    let expected_millis = retain_until_2.timestamp_millis();
    let actual_millis = date.to_millis().unwrap();
    assert!((actual_millis - expected_millis).abs() < 1000, "Retention date should be extended");
}

/// Test that shortening Compliance retention fails.
#[tokio::test]
async fn test_object_lock_shorten_compliance_fails() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"shorten compliance test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set initial retention for 2 days
    let retain_until_2 = chrono::Utc::now() + chrono::Duration::days(2);
    let retention1 = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Compliance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(
            retain_until_2.timestamp_millis(),
        ))
        .build();

    ctx.put_object_retention(&key, retention1, Some(version_id), false).await;

    // Try to shorten to 1 day - should fail
    let retain_until_1 = chrono::Utc::now() + chrono::Duration::days(1);
    let retention2 = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Compliance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(
            retain_until_1.timestamp_millis(),
        ))
        .build();

    let result = ctx
        .client
        .put_object_retention()
        .bucket(&ctx.bucket)
        .key(&key)
        .version_id(version_id)
        .retention(retention2)
        .send()
        .await;

    assert!(result.is_err(), "Shortening compliance retention should fail");
}

// =============================================================================
// Legal Hold Tests
// =============================================================================

/// Test putting legal hold ON.
#[tokio::test]
async fn test_object_lock_legal_hold_on() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"legal hold test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set legal hold ON
    let legal_hold = ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build();

    ctx.put_object_legal_hold(&key, legal_hold, Some(version_id)).await;

    // Verify legal hold is ON
    let retrieved = ctx.get_object_legal_hold(&key, Some(version_id)).await;
    let hold = retrieved.legal_hold().expect("Should have legal hold");

    assert_eq!(hold.status(), Some(&ObjectLockLegalHoldStatus::On));
}

/// Test putting legal hold OFF.
#[tokio::test]
async fn test_object_lock_legal_hold_off() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"legal hold off test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set legal hold ON first
    let legal_hold_on =
        ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build();
    ctx.put_object_legal_hold(&key, legal_hold_on, Some(version_id)).await;

    // Set legal hold OFF
    let legal_hold_off =
        ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::Off).build();
    ctx.put_object_legal_hold(&key, legal_hold_off, Some(version_id)).await;

    // Verify legal hold is OFF
    let retrieved = ctx.get_object_legal_hold(&key, Some(version_id)).await;
    let hold = retrieved.legal_hold().expect("Should have legal hold");

    assert_eq!(hold.status(), Some(&ObjectLockLegalHoldStatus::Off));
}

/// Test that legal hold prevents deletion.
#[tokio::test]
async fn test_object_lock_legal_hold_prevents_delete() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"legal hold delete test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set legal hold ON
    let legal_hold = ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build();
    ctx.put_object_legal_hold(&key, legal_hold, Some(version_id)).await;

    // Try to delete - should fail
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), false).await;
    assert!(delete_result.is_err(), "Delete should fail for object with legal hold");

    // Try with bypass governance - should still fail (legal hold is not bypass-able)
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), true).await;
    assert!(delete_result.is_err(), "Delete with bypass should fail for object with legal hold");
}

/// Test that removing legal hold allows deletion.
#[tokio::test]
async fn test_object_lock_legal_hold_removal_allows_delete() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"legal hold removal test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set legal hold ON
    let legal_hold_on =
        ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build();
    ctx.put_object_legal_hold(&key, legal_hold_on, Some(version_id)).await;

    // Remove legal hold
    let legal_hold_off =
        ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::Off).build();
    ctx.put_object_legal_hold(&key, legal_hold_off, Some(version_id)).await;

    // Now delete should succeed
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), false).await;
    assert!(delete_result.is_ok(), "Delete should succeed after removing legal hold");
}

// =============================================================================
// Combined Retention and Legal Hold Tests
// =============================================================================

/// Test object with both retention and legal hold.
#[tokio::test]
async fn test_object_lock_both_retention_and_legal_hold() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"both locks test").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set retention
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(retain_until.timestamp_millis()))
        .build();
    ctx.put_object_retention(&key, retention, Some(version_id), false).await;

    // Set legal hold
    let legal_hold = ObjectLockLegalHold::builder().status(ObjectLockLegalHoldStatus::On).build();
    ctx.put_object_legal_hold(&key, legal_hold, Some(version_id)).await;

    // Verify both are set
    let ret = ctx.get_object_retention(&key, Some(version_id)).await;
    assert!(ret.retention().is_some());

    let hold = ctx.get_object_legal_hold(&key, Some(version_id)).await;
    assert_eq!(hold.legal_hold().unwrap().status(), Some(&ObjectLockLegalHoldStatus::On));

    // Delete should fail even with bypass (due to legal hold)
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), true).await;
    assert!(
        delete_result.is_err(),
        "Delete should fail when both retention and legal hold are set"
    );
}

// =============================================================================
// Delete Protection Tests
// =============================================================================

/// Test that protected object version cannot be deleted.
#[tokio::test]
async fn test_object_lock_delete_protected() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result = ctx.put(&key, b"protected object").await;
    let version_id = put_result.version_id().expect("Should have version ID");

    // Set retention
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Compliance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(retain_until.timestamp_millis()))
        .build();
    ctx.put_object_retention(&key, retention, Some(version_id), false).await;

    // Try to delete version - should fail
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id), false).await;
    assert!(delete_result.is_err(), "Delete of protected version should fail");

    // Creating a delete marker (without version_id) should still work
    let delete_marker_result = ctx.delete_object_with_options(&key, None, false).await;
    assert!(delete_marker_result.is_ok(), "Creating delete marker should succeed");
}

/// Test overwriting protected object creates new version.
#[tokio::test]
async fn test_object_lock_overwrite_protected() {
    let ctx = S3TestContext::with_object_lock().await;

    let key = random_key();
    let put_result1 = ctx.put(&key, b"version 1").await;
    let version_id1 = put_result1.version_id().expect("Should have version ID");

    // Protect version 1
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retention = ObjectLockRetention::builder()
        .mode(ObjectLockRetentionMode::Governance)
        .retain_until_date(aws_smithy_types::DateTime::from_millis(retain_until.timestamp_millis()))
        .build();
    ctx.put_object_retention(&key, retention, Some(version_id1), false).await;

    // Overwrite with new version - should succeed
    let put_result2 = ctx.put(&key, b"version 2").await;
    let version_id2 = put_result2.version_id().expect("Should have version ID");

    // Version IDs should be different
    assert_ne!(version_id1, version_id2);

    // Version 1 should still be protected
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id1), false).await;
    assert!(delete_result.is_err(), "Protected version 1 should not be deletable");

    // Version 2 (unprotected) should be deletable
    let delete_result = ctx.delete_object_with_options(&key, Some(version_id2), false).await;
    assert!(delete_result.is_ok(), "Unprotected version 2 should be deletable");
}
