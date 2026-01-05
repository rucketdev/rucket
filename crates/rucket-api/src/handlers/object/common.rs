// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Common types and utilities for object handlers.

use std::collections::HashMap;

use axum::http::HeaderMap;
use bytes::Bytes;
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use rucket_core::error::S3ErrorCode;
use rucket_core::types::{Checksum, ChecksumAlgorithm};
use serde::Deserialize;

use crate::error::ApiError;
use crate::handlers::bucket::AppState;

/// Query parameters for overriding response headers in GetObject.
#[derive(Debug, Default)]
pub struct ResponseHeaderOverrides {
    /// Override Content-Type header.
    pub content_type: Option<String>,
    /// Override Content-Disposition header.
    pub content_disposition: Option<String>,
    /// Override Content-Encoding header.
    pub content_encoding: Option<String>,
    /// Override Content-Language header.
    pub content_language: Option<String>,
    /// Override Expires header.
    pub expires: Option<String>,
    /// Override Cache-Control header.
    pub cache_control: Option<String>,
}

/// Query parameters for `ListObjects` (V1 and V2).
#[derive(Debug, Deserialize)]
pub struct ListObjectsQuery {
    /// Prefix filter.
    pub prefix: Option<String>,
    /// Delimiter for grouping.
    pub delimiter: Option<String>,
    /// Marker for ListObjectsV1 pagination.
    pub marker: Option<String>,
    /// Key marker for ListObjectVersions pagination.
    #[serde(rename = "key-marker")]
    pub key_marker: Option<String>,
    /// Version ID marker for ListObjectVersions pagination.
    #[serde(rename = "version-id-marker")]
    pub version_id_marker: Option<String>,
    /// Continuation token for ListObjectsV2.
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
    /// StartAfter - where to start listing from (V2).
    #[serde(rename = "start-after")]
    pub start_after: Option<String>,
    /// Encoding type for keys (url).
    #[serde(rename = "encoding-type")]
    pub encoding_type: Option<String>,
    /// Maximum keys to return (as string to handle invalid values).
    #[serde(rename = "max-keys")]
    pub max_keys: Option<String>,
    /// FetchOwner - whether to include owner info (V2).
    #[serde(rename = "fetch-owner")]
    pub fetch_owner: Option<String>,
    /// Allow unordered listing (Ceph extension).
    #[serde(rename = "allow-unordered")]
    pub allow_unordered: Option<String>,
}

/// Characters that should NOT be encoded in S3 URL encoding.
/// Per S3 spec, unreserved characters are: A-Z, a-z, 0-9, -, _, ., ~
/// Everything else (including +, /, =, etc.) should be percent-encoded.
pub const S3_ENCODE_SET: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// Characters that should NOT be encoded in S3 URL encoding for prefixes.
/// Same as S3_ENCODE_SET but also preserves '/' since common prefixes
/// contain the delimiter which should not be encoded.
pub const S3_PREFIX_ENCODE_SET: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~').remove(b'/');

/// URL-encode a string using S3's encoding rules.
///
/// When `encoding-type=url` is specified, S3 URL-encodes keys, delimiters,
/// prefixes, and markers using RFC 3986.
pub fn s3_url_encode(s: &str) -> String {
    utf8_percent_encode(s, S3_ENCODE_SET).to_string()
}

/// URL-encode a prefix string using S3's encoding rules.
///
/// Same as `s3_url_encode` but preserves the '/' character since
/// common prefixes contain the delimiter which should not be encoded.
pub fn s3_url_encode_prefix(s: &str) -> String {
    utf8_percent_encode(s, S3_PREFIX_ENCODE_SET).to_string()
}

/// URL-decode a string. Returns the original string if decoding fails.
pub fn s3_url_decode(s: &str) -> String {
    percent_decode_str(s).decode_utf8().map(|s| s.to_string()).unwrap_or_else(|_| s.to_string())
}

/// Extract user metadata from request headers.
///
/// User metadata headers start with `x-amz-meta-` (case-insensitive).
/// Returns a HashMap with lowercase keys (without the prefix) and the values.
///
/// While HTTP/1.1 technically specifies ISO-8859-1 (Latin-1) for header values,
/// many modern clients (including boto3) send UTF-8 encoded bytes. This function
/// first tries UTF-8 decoding, then falls back to Latin-1 interpretation.
pub fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if let Some(key) = name_str.strip_prefix("x-amz-meta-") {
            // Try to_str() first (ASCII only), then try UTF-8, finally Latin-1
            let value_str = value.to_str().map(String::from).unwrap_or_else(|_| {
                let bytes = value.as_bytes();
                // Try UTF-8 first (common with boto3 and modern clients)
                std::str::from_utf8(bytes).map(String::from).unwrap_or_else(|_| {
                    // Fall back to Latin-1 interpretation (each byte -> Unicode code point)
                    bytes.iter().map(|&b| b as char).collect()
                })
            });
            metadata.insert(key.to_string(), value_str);
        }
    }
    metadata
}

/// Check if the request uses AWS chunked encoding.
///
/// AWS uses chunked encoding for streaming uploads, indicated by:
/// - `x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD`
/// - Or `Content-Encoding: aws-chunked`
pub fn is_aws_chunked(headers: &HeaderMap) -> bool {
    // Check x-amz-content-sha256 header
    if let Some(sha256) = headers.get("x-amz-content-sha256") {
        if let Ok(val) = sha256.to_str() {
            if val.starts_with("STREAMING-") {
                return true;
            }
        }
    }
    // Check Content-Encoding header
    if let Some(encoding) = headers.get("content-encoding") {
        if let Ok(val) = encoding.to_str() {
            if val.contains("aws-chunked") {
                return true;
            }
        }
    }
    false
}

/// Decode AWS chunked transfer encoding.
///
/// Format: `<chunk-size-hex>;chunk-signature=<sig>\r\n<data>\r\n...0;chunk-signature=<sig>\r\n\r\n`
pub fn decode_aws_chunked(body: Bytes) -> Result<Bytes, ApiError> {
    let mut result = Vec::new();
    let data = body.as_ref();
    let mut pos = 0;

    while pos < data.len() {
        // Find end of chunk header line (CRLF)
        let header_end = find_crlf(data, pos).ok_or_else(|| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Malformed chunked encoding: missing CRLF")
        })?;

        // Parse chunk header: "<size-hex>;chunk-signature=<sig>"
        let header = std::str::from_utf8(&data[pos..header_end]).map_err(|_| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Invalid UTF-8 in chunk header")
        })?;

        // Extract chunk size (everything before first ';')
        let size_str = header.split(';').next().unwrap_or("0");
        let chunk_size = usize::from_str_radix(size_str, 16)
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidRequest, "Invalid chunk size"))?;

        // Move past the header line (including CRLF)
        pos = header_end + 2;

        // If chunk size is 0, we're done (final chunk)
        if chunk_size == 0 {
            break;
        }

        // Read chunk data
        if pos + chunk_size > data.len() {
            return Err(ApiError::new(
                S3ErrorCode::InvalidRequest,
                "Chunk size exceeds available data",
            ));
        }
        result.extend_from_slice(&data[pos..pos + chunk_size]);
        pos += chunk_size;

        // Skip trailing CRLF after chunk data
        if pos + 2 <= data.len() && &data[pos..pos + 2] == b"\r\n" {
            pos += 2;
        }
    }

    Ok(Bytes::from(result))
}

/// Find CRLF (\r\n) in data starting from offset.
fn find_crlf(data: &[u8], start: usize) -> Option<usize> {
    (start..data.len().saturating_sub(1)).find(|&i| data[i] == b'\r' && data[i + 1] == b'\n')
}

/// Parse and validate max-keys parameter.
/// Returns InvalidArgument error for negative or non-numeric values.
pub fn parse_max_keys(max_keys: &Option<String>) -> Result<u32, ApiError> {
    match max_keys {
        None => Ok(1000), // Default
        Some(s) => {
            // Try to parse as i64 first to detect negative numbers
            match s.parse::<i64>() {
                Ok(n) if n < 0 => Err(ApiError::new(
                    S3ErrorCode::InvalidArgument,
                    "Argument max-keys must be a non-negative integer",
                )),
                Ok(n) => Ok(n.min(1000) as u32), // Cap at 1000
                Err(_) => Err(ApiError::new(
                    S3ErrorCode::InvalidArgument,
                    "Argument max-keys must be an integer",
                )),
            }
        }
    }
}

/// Check If-Match and If-None-Match precondition headers.
///
/// Returns an error if preconditions are not met:
/// - `If-Match`: The request succeeds only if the object's ETag matches one of the specified ETags.
/// - `If-None-Match`: The request succeeds only if the object's ETag does NOT match any of the specified ETags.
///   The special value `*` matches any existing object.
pub async fn check_preconditions(
    state: &AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
    use rucket_storage::StorageBackend;

    let if_match = headers.get("if-match").and_then(|v| v.to_str().ok());
    let if_none_match = headers.get("if-none-match").and_then(|v| v.to_str().ok());

    // If no conditional headers, nothing to check
    if if_match.is_none() && if_none_match.is_none() {
        return Ok(());
    }

    // Get current object ETag (if it exists)
    let current_etag =
        state.storage.head_object(bucket, key).await.ok().map(|m| m.etag.as_str().to_string());

    // Check If-Match: request succeeds only if ETag matches
    if let Some(if_match_value) = if_match {
        match &current_etag {
            Some(etag) => {
                // Parse comma-separated ETags and check if any match
                let matches =
                    if_match_value.split(',').map(|s| s.trim().trim_matches('"')).any(|expected| {
                        let actual = etag.trim_matches('"');
                        expected == actual || expected == "*"
                    });

                if !matches {
                    return Err(ApiError::new(
                        S3ErrorCode::PreconditionFailed,
                        "At least one of the pre-conditions you specified did not hold",
                    ));
                }
            }
            None => {
                // Object doesn't exist but If-Match was specified
                return Err(ApiError::new(
                    S3ErrorCode::PreconditionFailed,
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }
        }
    }

    // Check If-None-Match: request succeeds only if ETag does NOT match
    if let Some(if_none_match_value) = if_none_match {
        if let Some(etag) = &current_etag {
            // Special case: "*" matches any existing object
            if if_none_match_value.trim() == "*" {
                return Err(ApiError::new(
                    S3ErrorCode::PreconditionFailed,
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }

            // Parse comma-separated ETags and check if any match
            let matches = if_none_match_value.split(',').map(|s| s.trim().trim_matches('"')).any(
                |expected| {
                    let actual = etag.trim_matches('"');
                    expected == actual
                },
            );

            if matches {
                return Err(ApiError::new(
                    S3ErrorCode::PreconditionFailed,
                    "At least one of the pre-conditions you specified did not hold",
                ));
            }
        }
        // If object doesn't exist and If-None-Match is specified, that's fine
    }

    Ok(())
}

/// Parse client-provided checksum from request headers.
/// Returns Ok(Some(checksum)) if valid, Ok(None) if not provided, Err if invalid.
pub fn parse_client_checksum(
    headers: &HeaderMap,
) -> Result<Option<(ChecksumAlgorithm, Checksum)>, &'static str> {
    // Check each checksum header
    if let Some(value) = headers.get("x-amz-checksum-crc32").and_then(|v| v.to_str().ok()) {
        return Checksum::from_base64(ChecksumAlgorithm::Crc32, value)
            .map(|c| Some((ChecksumAlgorithm::Crc32, c)));
    }
    if let Some(value) = headers.get("x-amz-checksum-crc32c").and_then(|v| v.to_str().ok()) {
        return Checksum::from_base64(ChecksumAlgorithm::Crc32C, value)
            .map(|c| Some((ChecksumAlgorithm::Crc32C, c)));
    }
    if let Some(value) = headers.get("x-amz-checksum-sha1").and_then(|v| v.to_str().ok()) {
        return Checksum::from_base64(ChecksumAlgorithm::Sha1, value)
            .map(|c| Some((ChecksumAlgorithm::Sha1, c)));
    }
    if let Some(value) = headers.get("x-amz-checksum-sha256").and_then(|v| v.to_str().ok()) {
        return Checksum::from_base64(ChecksumAlgorithm::Sha256, value)
            .map(|c| Some((ChecksumAlgorithm::Sha256, c)));
    }
    Ok(None)
}

/// Format a datetime for HTTP headers (RFC 7231 format).
pub fn format_http_date(dt: &chrono::DateTime<chrono::Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Maximum number of tags allowed for an object.
pub const MAX_OBJECT_TAGS: usize = 10;
/// Maximum length of a tag key.
pub const MAX_TAG_KEY_LENGTH: usize = 128;
/// Maximum length of a tag value.
pub const MAX_TAG_VALUE_LENGTH: usize = 256;
