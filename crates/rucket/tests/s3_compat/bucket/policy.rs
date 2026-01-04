//! Bucket policy tests.
//!
//! These tests verify bucket policy CRUD operations and validation.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_policy_*

use crate::harness::S3TestContext;

/// Test putting a bucket policy.
#[tokio::test]
async fn test_bucket_policy_put() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy");
}

/// Test getting a bucket policy.
#[tokio::test]
async fn test_bucket_policy_get() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    // Put the policy first
    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy");

    // Get the policy
    let result = ctx
        .client
        .get_bucket_policy()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Failed to get bucket policy");

    let retrieved_policy: serde_json::Value =
        serde_json::from_str(result.policy().unwrap_or("{}")).expect("Failed to parse policy");

    assert_eq!(retrieved_policy["Version"], "2012-10-17");
    assert_eq!(retrieved_policy["Statement"][0]["Effect"], "Allow");
    assert_eq!(retrieved_policy["Statement"][0]["Principal"], "*");
    assert_eq!(retrieved_policy["Statement"][0]["Action"], "s3:GetObject");
}

/// Test getting a bucket policy when none exists returns NoSuchBucketPolicy.
#[tokio::test]
async fn test_bucket_policy_get_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.get_bucket_policy().bucket(&ctx.bucket).send().await;

    assert!(result.is_err(), "Should return error for non-existent policy");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(
        error_str.contains("NoSuchBucketPolicy"),
        "Expected NoSuchBucketPolicy error, got: {}",
        error_str
    );
}

/// Test deleting a bucket policy.
#[tokio::test]
async fn test_bucket_policy_delete() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    // Put the policy
    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy");

    // Delete the policy
    ctx.client
        .delete_bucket_policy()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Failed to delete bucket policy");

    // Verify it's gone
    let result = ctx.client.get_bucket_policy().bucket(&ctx.bucket).send().await;
    assert!(result.is_err());
}

/// Test bucket policy with empty statement returns error.
#[tokio::test]
async fn test_bucket_policy_empty_statement() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": []
    });

    let result =
        ctx.client.put_bucket_policy().bucket(&ctx.bucket).policy(policy.to_string()).send().await;

    assert!(result.is_err(), "Empty statement should be rejected");
}

/// Test bucket policy with invalid JSON returns error.
#[tokio::test]
async fn test_bucket_policy_invalid_json() {
    let ctx = S3TestContext::new().await;

    let result =
        ctx.client.put_bucket_policy().bucket(&ctx.bucket).policy("not valid json").send().await;

    assert!(result.is_err(), "Invalid JSON should be rejected");
}

/// Test bucket policy with multiple statements.
#[tokio::test]
async fn test_bucket_policy_multiple_statements() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "AllowGet",
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
            },
            {
                "Sid": "DenyDelete",
                "Effect": "Deny",
                "Principal": "*",
                "Action": "s3:DeleteObject",
                "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
            }
        ]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy");

    // Verify the policy was stored
    let result = ctx
        .client
        .get_bucket_policy()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Failed to get bucket policy");

    let retrieved: serde_json::Value =
        serde_json::from_str(result.policy().unwrap_or("{}")).expect("Failed to parse");
    assert_eq!(retrieved["Statement"].as_array().unwrap().len(), 2);
}

/// Test bucket policy with multiple actions.
#[tokio::test]
async fn test_bucket_policy_multiple_actions() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with multiple actions");
}

/// Test bucket policy with Principal wildcard.
#[tokio::test]
async fn test_bucket_policy_principal_wildcard() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with wildcard principal");
}

/// Test bucket policy with Principal AWS account.
#[tokio::test]
async fn test_bucket_policy_principal_account() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with AWS principal");
}

/// Test bucket policy with Action wildcard.
#[tokio::test]
async fn test_bucket_policy_action_wildcard() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with action wildcard");
}

/// Test bucket policy with Resource wildcard.
#[tokio::test]
async fn test_bucket_policy_resource_wildcard() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with resource wildcard");
}

/// Test bucket policy with Resource prefix pattern.
#[tokio::test]
async fn test_bucket_policy_resource_prefix() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/photos/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with resource prefix");
}

/// Test bucket policy with IpAddress condition.
#[tokio::test]
async fn test_bucket_policy_condition_ipaddress() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket),
            "Condition": {
                "IpAddress": {
                    "aws:SourceIp": "192.168.1.0/24"
                }
            }
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with IP condition");
}

/// Test bucket policy with NotIpAddress condition.
#[tokio::test]
async fn test_bucket_policy_condition_not_ipaddress() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket),
            "Condition": {
                "NotIpAddress": {
                    "aws:SourceIp": "10.0.0.0/8"
                }
            }
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with NotIpAddress condition");
}

/// Test bucket policy with SecureTransport condition.
#[tokio::test]
async fn test_bucket_policy_condition_secure_transport() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:*",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket),
            "Condition": {
                "Bool": {
                    "aws:SecureTransport": "false"
                }
            }
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with SecureTransport condition");
}

/// Test bucket policy with StringEquals condition.
#[tokio::test]
async fn test_bucket_policy_condition_string_equals() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket),
            "Condition": {
                "StringEquals": {
                    "s3:x-amz-acl": "public-read"
                }
            }
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with StringEquals condition");
}

/// Test bucket policy with StringLike condition.
#[tokio::test]
async fn test_bucket_policy_condition_string_like() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket),
            "Condition": {
                "StringLike": {
                    "s3:prefix": "photos/*"
                }
            }
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy.to_string())
        .send()
        .await
        .expect("Failed to put bucket policy with StringLike condition");
}

/// Test bucket policy on non-existent bucket returns NoSuchBucket.
#[tokio::test]
async fn test_bucket_policy_nonexistent_bucket() {
    let ctx = S3TestContext::new().await;

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::nonexistent-bucket/*"
        }]
    });

    let result = ctx
        .client
        .put_bucket_policy()
        .bucket("nonexistent-bucket-12345")
        .policy(policy.to_string())
        .send()
        .await;

    assert!(result.is_err(), "Should fail for non-existent bucket");
}

/// Test updating an existing bucket policy (overwrite).
#[tokio::test]
async fn test_bucket_policy_update() {
    let ctx = S3TestContext::new().await;

    // Create initial policy
    let policy1 = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "Statement1",
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy1.to_string())
        .send()
        .await
        .expect("Failed to put initial policy");

    // Update to new policy
    let policy2 = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Sid": "Statement2",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:DeleteObject",
            "Resource": format!("arn:aws:s3:::{}/*", ctx.bucket)
        }]
    });

    ctx.client
        .put_bucket_policy()
        .bucket(&ctx.bucket)
        .policy(policy2.to_string())
        .send()
        .await
        .expect("Failed to update policy");

    // Verify the new policy is in place
    let result = ctx
        .client
        .get_bucket_policy()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Failed to get policy");

    let retrieved: serde_json::Value =
        serde_json::from_str(result.policy().unwrap_or("{}")).expect("Failed to parse");
    assert_eq!(retrieved["Statement"][0]["Sid"], "Statement2");
    assert_eq!(retrieved["Statement"][0]["Effect"], "Deny");
}

/// Test deleting non-existent policy (should be idempotent).
#[tokio::test]
async fn test_bucket_policy_delete_nonexistent() {
    let ctx = S3TestContext::new().await;

    // Delete a policy that doesn't exist - this should succeed (idempotent)
    let result = ctx.client.delete_bucket_policy().bucket(&ctx.bucket).send().await;

    // S3 returns success for deleting non-existent policy
    assert!(result.is_ok(), "Deleting non-existent policy should succeed");
}

// =============================================================================
// Tests that require authorization integration (Phase 5)
// These are marked as ignored until policy enforcement is implemented
// =============================================================================

/// Test bucket policy Allow access for anonymous users.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_allow() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy Deny access.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_deny() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy with public access.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_public() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy deny all access.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_deny_all() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy allow GetObject.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_allow_get() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy allow PutObject.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_allow_put() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy allow DeleteObject.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_allow_delete() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy allow ListBucket.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_allow_list() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy explicit deny overrides allow.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_explicit_deny_overrides() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy conflict resolution.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_conflict() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy with IP condition enforcement.
#[tokio::test]
#[ignore = "Requires policy enforcement integration (Phase 5)"]
async fn test_bucket_policy_ip_condition() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy condition with DateGreaterThan.
#[tokio::test]
#[ignore = "Date conditions not yet implemented"]
async fn test_bucket_policy_condition_date() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy condition with Referer.
#[tokio::test]
#[ignore = "Referer condition not yet implemented"]
async fn test_bucket_policy_condition_referer() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy cross-account access.
#[tokio::test]
#[ignore = "Cross-account not supported in single-tenant mode"]
async fn test_bucket_policy_cross_account() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy with NotPrincipal.
#[tokio::test]
#[ignore = "NotPrincipal not yet implemented"]
async fn test_bucket_policy_not_principal() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy with NotAction.
#[tokio::test]
#[ignore = "NotAction not yet implemented"]
async fn test_bucket_policy_not_action() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy with NotResource.
#[tokio::test]
#[ignore = "NotResource not yet implemented"]
async fn test_bucket_policy_not_resource() {
    let _ctx = S3TestContext::new().await;
}

/// Test bucket policy with invalid action.
#[tokio::test]
#[ignore = "Action validation not yet strict"]
async fn test_bucket_policy_invalid_action() {
    let _ctx = S3TestContext::new().await;
}
