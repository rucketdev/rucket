//! SSE-C (Server-Side Encryption with Customer-Provided Keys) support.
//!
//! This module handles parsing and validation of SSE-C request headers and
//! provides encryption/decryption for SSE-C protected objects.

use axum::http::HeaderMap;
use bytes::Bytes;
use rucket_core::error::S3ErrorCode;
use rucket_storage::crypto::{CryptoError, SseCHeaders, SseCProvider};

use crate::error::ApiError;

/// SSE-C algorithm header name.
pub const HEADER_SSE_CUSTOMER_ALGORITHM: &str = "x-amz-server-side-encryption-customer-algorithm";
/// SSE-C key header name.
pub const HEADER_SSE_CUSTOMER_KEY: &str = "x-amz-server-side-encryption-customer-key";
/// SSE-C key MD5 header name.
pub const HEADER_SSE_CUSTOMER_KEY_MD5: &str = "x-amz-server-side-encryption-customer-key-md5";

/// SSE-C copy source algorithm header name.
pub const HEADER_COPY_SOURCE_SSE_CUSTOMER_ALGORITHM: &str =
    "x-amz-copy-source-server-side-encryption-customer-algorithm";
/// SSE-C copy source key header name.
pub const HEADER_COPY_SOURCE_SSE_CUSTOMER_KEY: &str =
    "x-amz-copy-source-server-side-encryption-customer-key";
/// SSE-C copy source key MD5 header name.
pub const HEADER_COPY_SOURCE_SSE_CUSTOMER_KEY_MD5: &str =
    "x-amz-copy-source-server-side-encryption-customer-key-md5";

/// Parse SSE-C headers from a request.
///
/// Returns `None` if no SSE-C headers are present.
/// Returns an error if headers are partially present or invalid.
pub fn parse_sse_c_headers(headers: &HeaderMap) -> Result<Option<SseCHeaders>, ApiError> {
    let algorithm = headers.get(HEADER_SSE_CUSTOMER_ALGORITHM).and_then(|v| v.to_str().ok());
    let key = headers.get(HEADER_SSE_CUSTOMER_KEY).and_then(|v| v.to_str().ok());
    let key_md5 = headers.get(HEADER_SSE_CUSTOMER_KEY_MD5).and_then(|v| v.to_str().ok());

    match (algorithm, key, key_md5) {
        // No SSE-C headers present
        (None, None, None) => Ok(None),

        // All SSE-C headers present - validate and parse
        (Some(algorithm), Some(key), Some(key_md5)) => {
            let sse_c = SseCHeaders::parse(algorithm, key, key_md5).map_err(crypto_to_api_error)?;
            Ok(Some(sse_c))
        }

        // Missing algorithm
        (None, Some(_), _) | (None, _, Some(_)) => Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            format!("Missing {} header", HEADER_SSE_CUSTOMER_ALGORITHM),
        )),

        // Missing key
        (Some(_), None, _) => Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            format!("Missing {} header", HEADER_SSE_CUSTOMER_KEY),
        )),

        // Missing key MD5
        (Some(_), Some(_), None) => Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            format!("Missing {} header", HEADER_SSE_CUSTOMER_KEY_MD5),
        )),
    }
}

/// Parse SSE-C copy source headers from a request.
///
/// Returns `None` if no SSE-C copy source headers are present.
/// Returns an error if headers are partially present or invalid.
pub fn parse_copy_source_sse_c_headers(
    headers: &HeaderMap,
) -> Result<Option<SseCHeaders>, ApiError> {
    let algorithm =
        headers.get(HEADER_COPY_SOURCE_SSE_CUSTOMER_ALGORITHM).and_then(|v| v.to_str().ok());
    let key = headers.get(HEADER_COPY_SOURCE_SSE_CUSTOMER_KEY).and_then(|v| v.to_str().ok());
    let key_md5 =
        headers.get(HEADER_COPY_SOURCE_SSE_CUSTOMER_KEY_MD5).and_then(|v| v.to_str().ok());

    match (algorithm, key, key_md5) {
        // No SSE-C headers present
        (None, None, None) => Ok(None),

        // All SSE-C headers present - validate and parse
        (Some(algorithm), Some(key), Some(key_md5)) => {
            let sse_c = SseCHeaders::parse(algorithm, key, key_md5).map_err(crypto_to_api_error)?;
            Ok(Some(sse_c))
        }

        // Missing algorithm
        (None, Some(_), _) | (None, _, Some(_)) => Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            format!("Missing {} header", HEADER_COPY_SOURCE_SSE_CUSTOMER_ALGORITHM),
        )),

        // Missing key
        (Some(_), None, _) => Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            format!("Missing {} header", HEADER_COPY_SOURCE_SSE_CUSTOMER_KEY),
        )),

        // Missing key MD5
        (Some(_), Some(_), None) => Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            format!("Missing {} header", HEADER_COPY_SOURCE_SSE_CUSTOMER_KEY_MD5),
        )),
    }
}

/// Convert a crypto error to an API error.
fn crypto_to_api_error(err: CryptoError) -> ApiError {
    match err {
        CryptoError::InvalidCustomerKey => ApiError::new(
            S3ErrorCode::InvalidArgument,
            "The encryption key must be exactly 256 bits (32 bytes)",
        ),
        CryptoError::KeyMd5Mismatch => ApiError::new(
            S3ErrorCode::InvalidArgument,
            "The computed MD5 of the key did not match the provided x-amz-server-side-encryption-customer-key-MD5 header",
        ),
        CryptoError::EncryptionFailed(msg) if msg.contains("Invalid SSE-C algorithm") => {
            ApiError::new(S3ErrorCode::InvalidEncryptionAlgorithmError, msg)
        }
        CryptoError::WrongKey => {
            ApiError::new(S3ErrorCode::AccessDenied, "The encryption key provided does not match")
        }
        _ => ApiError::new(S3ErrorCode::InternalError, err.to_string()),
    }
}

/// Encrypt data using SSE-C.
///
/// Returns the encrypted data and the nonce used.
pub fn encrypt_sse_c(headers: &SseCHeaders, data: Bytes) -> Result<(Bytes, Vec<u8>), ApiError> {
    let provider = SseCProvider::new(headers);
    provider.encrypt(&data).map_err(crypto_to_api_error)
}

/// Decrypt data using SSE-C.
///
/// # Arguments
///
/// * `headers` - The SSE-C headers from the request
/// * `data` - The encrypted data
/// * `nonce` - The nonce used during encryption
/// * `stored_key_md5` - The MD5 of the key that was used to encrypt (for validation)
///
/// # Errors
///
/// Returns an error if:
/// - The key MD5 doesn't match the stored value
/// - Decryption fails (wrong key or corrupted data)
pub fn decrypt_sse_c(
    headers: &SseCHeaders,
    data: Bytes,
    nonce: &[u8],
    stored_key_md5: &str,
) -> Result<Bytes, ApiError> {
    // Validate the key MD5 matches what was used during encryption
    if headers.key_md5 != stored_key_md5 {
        return Err(ApiError::new(
            S3ErrorCode::AccessDenied,
            "The encryption key provided does not match the key used to encrypt this object",
        ));
    }

    let provider = SseCProvider::new(headers);
    provider.decrypt(&data, nonce).map_err(crypto_to_api_error)
}

/// Check if SSE-C is required for an object (based on metadata).
pub fn is_sse_c_encrypted(metadata: &rucket_core::types::ObjectMetadata) -> bool {
    metadata.sse_customer_algorithm.is_some()
}

/// Validate that SSE-C headers are provided for an SSE-C encrypted object.
pub fn require_sse_c_for_encrypted_object(
    headers: &HeaderMap,
    metadata: &rucket_core::types::ObjectMetadata,
) -> Result<Option<SseCHeaders>, ApiError> {
    if is_sse_c_encrypted(metadata) {
        // Object is SSE-C encrypted, headers are required
        let sse_c = parse_sse_c_headers(headers)?;
        match sse_c {
            Some(sse_c) => Ok(Some(sse_c)),
            None => Err(ApiError::new(
                S3ErrorCode::InvalidRequest,
                "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
            )),
        }
    } else {
        // Object is not SSE-C encrypted, no headers required
        Ok(None)
    }
}
