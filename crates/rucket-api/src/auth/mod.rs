//! AWS authentication support.

pub mod sigv4;

use std::collections::BTreeMap;
use std::sync::Arc;

use axum::extract::{Request, State};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use rucket_core::config::AuthConfig;
use rucket_core::error::S3ErrorCode;
pub use sigv4::{SigV4Validator, ValidationError};

use crate::error::ApiError;

/// State for the auth middleware.
#[derive(Clone)]
pub struct AuthState {
    /// The SigV4 validator.
    pub validator: Arc<SigV4Validator>,
}

impl AuthState {
    /// Create a new auth state from config.
    #[must_use]
    pub fn new(config: &AuthConfig) -> Self {
        Self { validator: Arc::new(SigV4Validator::new(config)) }
    }
}

/// Authentication middleware.
///
/// Validates requests using either:
/// - Header-based AWS Signature V4 (Authorization header)
/// - Query parameter-based presigned URLs (X-Amz-Signature query param)
pub async fn auth_middleware(
    State(auth_state): State<AuthState>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let uri = request.uri().clone();
    let uri_str = uri.to_string();
    let headers = request.headers().clone();

    // Parse query parameters into BTreeMap for presigned URL check
    let query_params = parse_query_params(uri.query().unwrap_or(""));

    // Check if this is a presigned URL request
    if SigV4Validator::is_presigned_request(&query_params) {
        // Validate presigned URL
        match auth_state.validator.validate_presigned(&method, &uri_str, &headers, &query_params) {
            Ok(()) => next.run(request).await,
            Err(e) => validation_error_to_response(e),
        }
    } else if headers.contains_key("authorization") {
        // Header-based auth - we need to compute payload hash
        // For now, use UNSIGNED-PAYLOAD since we don't have the body yet
        // Real implementation would need to buffer the body or use x-amz-content-sha256
        let payload_hash = headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("UNSIGNED-PAYLOAD");

        match auth_state.validator.validate(&method, &uri_str, &headers, payload_hash) {
            Ok(()) => next.run(request).await,
            Err(e) => validation_error_to_response(e),
        }
    } else {
        // No auth provided - for now, allow anonymous access
        // This matches current behavior; can be made stricter later
        next.run(request).await
    }
}

/// Parse query string into a BTreeMap.
fn parse_query_params(query: &str) -> BTreeMap<String, String> {
    let mut params = BTreeMap::new();
    if query.is_empty() {
        return params;
    }

    for pair in query.split('&') {
        if let Some((key, value)) = pair.split_once('=') {
            // URL-decode the key and value
            let key = percent_encoding::percent_decode_str(key)
                .decode_utf8()
                .unwrap_or_else(|_| key.into())
                .to_string();
            let value = percent_encoding::percent_decode_str(value)
                .decode_utf8()
                .unwrap_or_else(|_| value.into())
                .to_string();
            params.insert(key, value);
        } else {
            params.insert(pair.to_string(), String::new());
        }
    }
    params
}

/// Convert a validation error to an HTTP response.
fn validation_error_to_response(error: ValidationError) -> Response {
    let (code, message) = match &error {
        ValidationError::MissingAuthHeader => {
            (S3ErrorCode::AccessDenied, "Missing Authorization header")
        }
        ValidationError::InvalidAuthHeader => {
            (S3ErrorCode::InvalidArgument, "Invalid Authorization header format")
        }
        ValidationError::MissingHeader(_) => {
            (S3ErrorCode::InvalidArgument, "Missing required header")
        }
        ValidationError::MissingQueryParam(_) => {
            (S3ErrorCode::InvalidArgument, "Missing required query parameter")
        }
        ValidationError::InvalidDate => (S3ErrorCode::InvalidArgument, "Invalid date format"),
        ValidationError::InvalidExpires => (S3ErrorCode::InvalidArgument, "Invalid expires value"),
        ValidationError::SignatureMismatch => (
            S3ErrorCode::SignatureDoesNotMatch,
            "The request signature we calculated does not match the signature you provided",
        ),
        ValidationError::RequestExpired => (S3ErrorCode::AccessDenied, "Request has expired"),
        ValidationError::UnknownAccessKey => (
            S3ErrorCode::InvalidAccessKeyId,
            "The AWS Access Key Id you provided does not exist in our records",
        ),
        ValidationError::InvalidAlgorithm => {
            (S3ErrorCode::InvalidArgument, "Unsupported signature algorithm")
        }
    };

    ApiError::new(code, message).into_response()
}
