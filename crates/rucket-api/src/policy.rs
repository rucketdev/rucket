//! Bucket policy evaluation utilities for API handlers.
//!
//! This module provides helper functions for evaluating bucket policies
//! in request handlers.

use std::net::IpAddr;

use axum::Extension;
use rucket_core::error::S3ErrorCode;
use rucket_core::policy::{BucketPolicy, PolicyDecision, RequestContext, S3Action};
use rucket_storage::backend::StorageBackend;

use crate::auth::AuthContext;
use crate::error::ApiError;

/// Error returned when a request is denied by bucket policy.
pub fn access_denied_error() -> ApiError {
    ApiError::new(S3ErrorCode::AccessDenied, "Access Denied")
}

/// Evaluate a bucket policy for the given request context.
///
/// Returns `Ok(())` if the request is allowed, or an error if denied.
///
/// # Policy Evaluation Logic
///
/// 1. If no policy exists, all requests are allowed (implicit allow)
/// 2. If a policy exists:
///    - Explicit Deny always wins
///    - Explicit Allow permits the request
///    - No matching statement results in implicit deny
///
/// Note: Currently, we implement a permissive model where anonymous
/// requests without policies are allowed. This matches AWS behavior
/// for public buckets.
pub async fn evaluate_bucket_policy<S: StorageBackend>(
    storage: &S,
    auth: &AuthContext,
    bucket: &str,
    key: Option<&str>,
    action: S3Action,
    source_ip: Option<IpAddr>,
    secure_transport: bool,
) -> Result<(), ApiError> {
    // Get bucket policy if it exists
    let policy_json = match storage.get_bucket_policy(bucket).await {
        Ok(Some(json)) => json,
        Ok(None) => {
            // No policy - allow the request (implicit allow)
            return Ok(());
        }
        Err(_) => {
            // Error getting policy - allow the request to avoid blocking
            // In production, you might want to deny on error
            return Ok(());
        }
    };

    // Parse the policy
    let policy: BucketPolicy = match serde_json::from_str(&policy_json) {
        Ok(p) => p,
        Err(_) => {
            // Invalid policy JSON - allow the request
            // The policy was validated on upload, so this shouldn't happen
            return Ok(());
        }
    };

    // Build the request context
    let resource = match key {
        Some(k) => format!("arn:aws:s3:::{}/{}", bucket, k),
        None => format!("arn:aws:s3:::{}", bucket),
    };

    let ctx = RequestContext {
        principal: auth.principal_arn.clone(),
        is_anonymous: auth.is_anonymous,
        action,
        resource,
        bucket: bucket.to_string(),
        key: key.map(String::from),
        source_ip,
        secure_transport,
        context_values: std::collections::HashMap::new(),
    };

    // Evaluate the policy
    match policy.evaluate(&ctx) {
        PolicyDecision::Allow => Ok(()),
        PolicyDecision::Deny => Err(access_denied_error()),
        PolicyDecision::DefaultDeny => {
            // No matching allow statement
            // For authenticated users, this means deny
            // For anonymous users, this also means deny (they need explicit allow)
            Err(access_denied_error())
        }
    }
}

/// Helper to extract AuthContext from request extensions.
///
/// If auth middleware is not configured, returns an anonymous context.
pub fn get_auth_context(auth: Option<Extension<AuthContext>>) -> AuthContext {
    auth.map(|ext| ext.0).unwrap_or_else(AuthContext::anonymous)
}

/// Check if request should skip policy evaluation.
///
/// Some operations should not be subject to bucket policies:
/// - Creating a bucket (policy doesn't exist yet)
/// - Operations by bucket owner (implicit full access)
pub fn should_skip_policy_check(action: S3Action, is_bucket_owner: bool) -> bool {
    // Bucket owners have implicit full access
    if is_bucket_owner {
        return true;
    }

    // These operations don't have policies yet or are administrative
    matches!(action, S3Action::CreateBucket)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_auth_context_none() {
        let ctx = get_auth_context(None);
        assert!(ctx.is_anonymous);
        assert_eq!(ctx.principal_arn, "*");
    }

    #[test]
    fn test_get_auth_context_some() {
        let auth = AuthContext::authenticated("test-key".to_string());
        let ext = Extension(auth);
        let ctx = get_auth_context(Some(ext));
        assert!(!ctx.is_anonymous);
        assert!(ctx.principal_arn.contains("test-key"));
    }

    #[test]
    fn test_should_skip_policy_check() {
        // Bucket owner always skips
        assert!(should_skip_policy_check(S3Action::GetObject, true));
        assert!(should_skip_policy_check(S3Action::PutObject, true));

        // CreateBucket skips even for non-owner
        assert!(should_skip_policy_check(S3Action::CreateBucket, false));

        // Other actions don't skip for non-owner
        assert!(!should_skip_policy_check(S3Action::GetObject, false));
        assert!(!should_skip_policy_check(S3Action::PutObject, false));
    }
}
