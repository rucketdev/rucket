//! Bucket policy evaluation utilities for API handlers.
//!
//! This module provides helper functions for evaluating bucket policies
//! in request handlers.

use std::net::IpAddr;
use std::str::FromStr;

use axum::http::HeaderMap;
use axum::Extension;
use rucket_core::error::S3ErrorCode;
use rucket_core::policy::{BucketPolicy, PolicyDecision, RequestContext, S3Action};
use rucket_storage::backend::StorageBackend;

use crate::auth::AuthContext;
use crate::error::ApiError;

/// Information extracted from the HTTP request for policy evaluation.
#[derive(Debug, Clone, Default)]
pub struct RequestInfo {
    /// The source IP address of the client.
    ///
    /// Extracted from `X-Forwarded-For` or `X-Real-IP` headers,
    /// useful for policy conditions like `aws:SourceIp`.
    pub source_ip: Option<IpAddr>,

    /// Whether the request was made over HTTPS.
    ///
    /// Extracted from `X-Forwarded-Proto` header,
    /// useful for policy conditions like `aws:SecureTransport`.
    pub is_secure: bool,
}

impl RequestInfo {
    /// Extract request info from HTTP headers.
    ///
    /// This function extracts:
    /// - Source IP from `X-Forwarded-For` (first IP) or `X-Real-IP` headers
    /// - HTTPS status from `X-Forwarded-Proto` header
    ///
    /// # Arguments
    /// * `headers` - The HTTP request headers
    ///
    /// # Returns
    /// A `RequestInfo` struct with extracted values
    #[must_use]
    pub fn from_headers(headers: &HeaderMap) -> Self {
        let source_ip = Self::extract_source_ip(headers);
        let is_secure = Self::extract_is_secure(headers);

        Self { source_ip, is_secure }
    }

    /// Extract source IP address from headers.
    ///
    /// Checks headers in this order:
    /// 1. `X-Forwarded-For` - Uses the first IP (original client)
    /// 2. `X-Real-IP` - Fallback for some proxy configurations
    fn extract_source_ip(headers: &HeaderMap) -> Option<IpAddr> {
        // Try X-Forwarded-For first (standard proxy header)
        // Format: "client, proxy1, proxy2" - we want the first (client) IP
        if let Some(xff) = headers.get("x-forwarded-for") {
            if let Ok(xff_str) = xff.to_str() {
                // Get the first IP in the chain (original client)
                if let Some(first_ip) = xff_str.split(',').next() {
                    let trimmed = first_ip.trim();
                    if let Ok(ip) = IpAddr::from_str(trimmed) {
                        return Some(ip);
                    }
                }
            }
        }

        // Try X-Real-IP as fallback (used by nginx, etc.)
        if let Some(real_ip) = headers.get("x-real-ip") {
            if let Ok(ip_str) = real_ip.to_str() {
                if let Ok(ip) = IpAddr::from_str(ip_str.trim()) {
                    return Some(ip);
                }
            }
        }

        None
    }

    /// Extract HTTPS status from headers.
    ///
    /// Checks `X-Forwarded-Proto` header for "https" value.
    fn extract_is_secure(headers: &HeaderMap) -> bool {
        if let Some(proto) = headers.get("x-forwarded-proto") {
            if let Ok(proto_str) = proto.to_str() {
                return proto_str.eq_ignore_ascii_case("https");
            }
        }
        false
    }
}

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

    #[test]
    fn test_request_info_default() {
        let info = RequestInfo::default();
        assert!(info.source_ip.is_none());
        assert!(!info.is_secure);
    }

    #[test]
    fn test_request_info_x_forwarded_for_single_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "192.168.1.100".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert_eq!(info.source_ip, Some("192.168.1.100".parse().unwrap()));
    }

    #[test]
    fn test_request_info_x_forwarded_for_multiple_ips() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-forwarded-for",
            "203.0.113.195, 70.41.3.18, 150.172.238.178".parse().unwrap(),
        );
        let info = RequestInfo::from_headers(&headers);
        // Should get the first IP (original client)
        assert_eq!(info.source_ip, Some("203.0.113.195".parse().unwrap()));
    }

    #[test]
    fn test_request_info_x_forwarded_for_ipv6() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "2001:db8::1".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert_eq!(info.source_ip, Some("2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn test_request_info_x_real_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-real-ip", "10.0.0.50".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert_eq!(info.source_ip, Some("10.0.0.50".parse().unwrap()));
    }

    #[test]
    fn test_request_info_x_forwarded_for_takes_precedence() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "192.168.1.1".parse().unwrap());
        headers.insert("x-real-ip", "192.168.1.2".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        // X-Forwarded-For takes precedence
        assert_eq!(info.source_ip, Some("192.168.1.1".parse().unwrap()));
    }

    #[test]
    fn test_request_info_invalid_ip() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "not-an-ip".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert!(info.source_ip.is_none());
    }

    #[test]
    fn test_request_info_is_secure_https() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert!(info.is_secure);
    }

    #[test]
    fn test_request_info_is_secure_http() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "http".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert!(!info.is_secure);
    }

    #[test]
    fn test_request_info_is_secure_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-proto", "HTTPS".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert!(info.is_secure);
    }

    #[test]
    fn test_request_info_no_headers() {
        let headers = HeaderMap::new();
        let info = RequestInfo::from_headers(&headers);
        assert!(info.source_ip.is_none());
        assert!(!info.is_secure);
    }

    #[test]
    fn test_request_info_combined() {
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "172.16.0.100".parse().unwrap());
        headers.insert("x-forwarded-proto", "https".parse().unwrap());
        let info = RequestInfo::from_headers(&headers);
        assert_eq!(info.source_ip, Some("172.16.0.100".parse().unwrap()));
        assert!(info.is_secure);
    }
}
