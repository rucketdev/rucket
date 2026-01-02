//! Object operation handlers.

use std::collections::HashMap;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use percent_encoding::{percent_decode_str, utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};
use rucket_core::config::ApiCompatibilityMode;
use rucket_core::error::S3ErrorCode;
use rucket_storage::{ObjectHeaders, StorageBackend};
use serde::Deserialize;

use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::xml::request::DeleteObjects;

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
use rucket_core::types::{Checksum, ChecksumAlgorithm, Tag, TagSet};
use rucket_storage::streaming::compute_checksum;

use crate::xml::request::Tagging as TaggingRequest;
use crate::xml::response::{
    to_xml, CommonPrefix, CopyObjectResponse, DeleteError, DeleteMarker, DeleteObjectsResponse,
    DeletedObject, GetObjectAttributesResponse, ListObjectVersionsResponse, ListObjectsV1Response,
    ListObjectsV2Response, ObjectChecksum, ObjectEntry, ObjectParts, ObjectVersion, TagResponse,
    TagSetResponse, TaggingResponse,
};

/// Format a datetime for HTTP headers (RFC 7231 format).
fn format_http_date(dt: &DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Characters that should NOT be encoded in S3 URL encoding.
/// Per S3 spec, unreserved characters are: A-Z, a-z, 0-9, -, _, ., ~
/// Everything else (including +, /, =, etc.) should be percent-encoded.
const S3_ENCODE_SET: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~');

/// Characters that should NOT be encoded in S3 URL encoding for prefixes.
/// Same as S3_ENCODE_SET but also preserves '/' since common prefixes
/// contain the delimiter which should not be encoded.
const S3_PREFIX_ENCODE_SET: &AsciiSet =
    &NON_ALPHANUMERIC.remove(b'-').remove(b'_').remove(b'.').remove(b'~').remove(b'/');

/// URL-encode a string using S3's encoding rules.
///
/// When `encoding-type=url` is specified, S3 URL-encodes keys, delimiters,
/// prefixes, and markers using RFC 3986.
fn s3_url_encode(s: &str) -> String {
    utf8_percent_encode(s, S3_ENCODE_SET).to_string()
}

/// URL-encode a prefix string using S3's encoding rules.
///
/// Same as `s3_url_encode` but preserves the '/' character since
/// common prefixes contain the delimiter which should not be encoded.
fn s3_url_encode_prefix(s: &str) -> String {
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
fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
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
fn is_aws_chunked(headers: &HeaderMap) -> bool {
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
fn decode_aws_chunked(body: Bytes) -> Result<Bytes, ApiError> {
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

/// Parse and validate max-keys parameter.
/// Returns InvalidArgument error for negative or non-numeric values.
fn parse_max_keys(max_keys: &Option<String>) -> Result<u32, ApiError> {
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
async fn check_preconditions(
    state: &AppState,
    bucket: &str,
    key: &str,
    headers: &HeaderMap,
) -> Result<(), ApiError> {
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

/// `PUT /{bucket}/{key}` - Upload object.
pub async fn put_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Check conditional headers for optimistic locking
    check_preconditions(&state, &bucket, &key, &headers).await?;

    let user_metadata = extract_user_metadata(&headers);

    // Extract HTTP headers
    // Strip "aws-chunked" from Content-Encoding as per S3 spec - it's a transfer encoding,
    // not a content encoding that should be stored with the object metadata.
    let content_encoding =
        headers.get("content-encoding").and_then(|v| v.to_str().ok()).and_then(|encoding| {
            // Split by comma, filter out aws-chunked, rejoin
            let filtered: Vec<&str> =
                encoding.split(',').map(str::trim).filter(|&e| e != "aws-chunked").collect();
            if filtered.is_empty() {
                None
            } else {
                Some(filtered.join(", "))
            }
        });

    // Parse checksum algorithm from header
    // AWS SDK sends "x-amz-sdk-checksum-algorithm", S3 API uses "x-amz-checksum-algorithm"
    let checksum_algorithm = headers
        .get("x-amz-sdk-checksum-algorithm")
        .or_else(|| headers.get("x-amz-checksum-algorithm"))
        .and_then(|v| v.to_str().ok())
        .and_then(ChecksumAlgorithm::parse);

    // Parse client-provided checksum value for validation
    let client_checksum = parse_client_checksum(&headers).map_err(|e| {
        ApiError::new(S3ErrorCode::BadDigest, format!("Invalid checksum format: {e}"))
    })?;

    let object_headers = ObjectHeaders {
        content_type: headers.get("content-type").and_then(|v| v.to_str().ok()).map(String::from),
        cache_control: headers.get("cache-control").and_then(|v| v.to_str().ok()).map(String::from),
        content_disposition: headers
            .get("content-disposition")
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        content_encoding,
        expires: headers.get("expires").and_then(|v| v.to_str().ok()).map(String::from),
        content_language: headers
            .get("content-language")
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        checksum_algorithm,
    };

    // Decode AWS chunked encoding if present
    let body = if is_aws_chunked(&headers) { decode_aws_chunked(body)? } else { body };

    // Validate client-provided checksum before storage
    if let Some((algorithm, ref expected)) = client_checksum {
        let computed = compute_checksum(&body, algorithm);
        if &computed != expected {
            return Err(ApiError::new(
                S3ErrorCode::BadDigest,
                "The Content-MD5 or checksum value that you specified did not match what the server received.",
            ));
        }
    }

    let result =
        state.storage.put_object(&bucket, &key, body, object_headers, user_metadata).await?;

    // Build response with headers
    let mut response =
        Response::builder().status(StatusCode::OK).header("ETag", result.etag.as_str());

    if let Some(ref version_id) = result.version_id {
        response = response.header("x-amz-version-id", version_id.as_str());
    }

    // Add checksum header to response if checksum was computed
    if let Some(ref checksum) = result.checksum {
        response = response.header(checksum.algorithm().header_name(), checksum.to_base64());
    }

    response
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// Parse client-provided checksum from request headers.
/// Returns Ok(Some(checksum)) if valid, Ok(None) if not provided, Err if invalid.
fn parse_client_checksum(
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

/// `GET /{bucket}/{key}` - Download object.
pub async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    overrides: ResponseHeaderOverrides,
    version_id: Option<String>,
) -> Result<Response, ApiError> {
    // Check bucket exists first to return proper error code
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Check for Range header (not supported with versionId for now)
    if let Some(range) = headers.get("range").and_then(|v| v.to_str().ok()) {
        if version_id.is_none() {
            return get_object_range(state, &bucket, &key, range).await;
        }
        // For versioned objects with range, we'd need to extend get_object_range
        // For now, we ignore range for versioned objects
    }

    // Get object, either latest or specific version
    let (meta, data) = if let Some(ref vid) = version_id {
        state.storage.get_object_version(&bucket, &key, vid).await?
    } else {
        state.storage.get_object(&bucket, &key).await?
    };

    // Check If-None-Match: return 304 Not Modified if ETag matches
    if let Some(if_none_match) = headers.get("if-none-match").and_then(|v| v.to_str().ok()) {
        let etag = meta.etag.as_str();
        let etags: Vec<&str> = if_none_match.split(',').map(|s| s.trim()).collect();
        if etags
            .iter()
            .any(|&e| e == "*" || e == etag || e.trim_matches('"') == etag.trim_matches('"'))
        {
            return Response::builder()
                .status(StatusCode::NOT_MODIFIED)
                .header("ETag", etag)
                .body(Body::empty())
                .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()));
        }
    }

    // Check If-Match: return 412 Precondition Failed if ETag doesn't match
    if let Some(if_match) = headers.get("if-match").and_then(|v| v.to_str().ok()) {
        let etag = meta.etag.as_str();
        let etags: Vec<&str> = if_match.split(',').map(|s| s.trim()).collect();
        if !etags
            .iter()
            .any(|&e| e == "*" || e == etag || e.trim_matches('"') == etag.trim_matches('"'))
        {
            return Err(ApiError::new(S3ErrorCode::PreconditionFailed, "Precondition failed"));
        }
    }

    // Check If-Modified-Since: return 304 if object was not modified since the date
    if let Some(if_modified_since) = headers.get("if-modified-since").and_then(|v| v.to_str().ok())
    {
        if let Ok(since) = chrono::DateTime::parse_from_rfc2822(if_modified_since) {
            if meta.last_modified <= since.with_timezone(&chrono::Utc) {
                return Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("ETag", meta.etag.as_str())
                    .body(Body::empty())
                    .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()));
            }
        }
    }

    // Check If-Unmodified-Since: return 412 if object was modified since the date
    if let Some(if_unmodified_since) =
        headers.get("if-unmodified-since").and_then(|v| v.to_str().ok())
    {
        if let Ok(since) = chrono::DateTime::parse_from_rfc2822(if_unmodified_since) {
            if meta.last_modified > since.with_timezone(&chrono::Utc) {
                return Err(ApiError::new(S3ErrorCode::PreconditionFailed, "Precondition failed"));
            }
        }
    }

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", meta.size.to_string())
        .header("Last-Modified", format_http_date(&meta.last_modified));

    // Add version ID header if the object has one
    if let Some(ref version_id) = meta.version_id {
        response = response.header("x-amz-version-id", version_id.as_str());
    }

    // Use override values if provided, otherwise use stored metadata
    if let Some(ct) = overrides.content_type.as_ref().or(meta.content_type.as_ref()) {
        response = response.header("Content-Type", ct.as_str());
    }
    if let Some(cc) = overrides.cache_control.as_ref().or(meta.cache_control.as_ref()) {
        response = response.header("Cache-Control", cc.as_str());
    }
    if let Some(cd) = overrides.content_disposition.as_ref().or(meta.content_disposition.as_ref()) {
        response = response.header("Content-Disposition", cd.as_str());
    }
    if let Some(ce) = overrides.content_encoding.as_ref().or(meta.content_encoding.as_ref()) {
        response = response.header("Content-Encoding", ce.as_str());
    }
    if let Some(exp) = overrides.expires.as_ref().or(meta.expires.as_ref()) {
        response = response.header("Expires", exp.as_str());
    }
    if let Some(cl) = overrides.content_language.as_ref().or(meta.content_language.as_ref()) {
        response = response.header("Content-Language", cl.as_str());
    }

    // Add user metadata headers
    // boto3 sends UTF-8 in headers but expects Latin-1 back (per HTTP/1.1 spec).
    // Convert Unicode chars to Latin-1 bytes (U+0000-U+00FF -> 0x00-0xFF).
    for (key, value) in &meta.user_metadata {
        let latin1_bytes: Vec<u8> = value
            .chars()
            .filter_map(|c| {
                let code = c as u32;
                if code <= 0xFF {
                    Some(code as u8)
                } else {
                    None
                }
            })
            .collect();
        if let Ok(header_value) = HeaderValue::from_bytes(&latin1_bytes) {
            response = response.header(format!("x-amz-meta-{key}"), header_value);
        }
    }

    // Add checksum header if checksum mode is enabled and we have a stored checksum
    let checksum_mode_enabled = headers
        .get("x-amz-checksum-mode")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.eq_ignore_ascii_case("ENABLED"))
        .unwrap_or(false);

    if checksum_mode_enabled {
        if let Some(algorithm) = meta.checksum_algorithm {
            if let Some(checksum) = meta.get_checksum(algorithm) {
                response = response.header(algorithm.header_name(), checksum.to_base64());
            }
        }
    }

    response
        .body(Body::from(data))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

async fn get_object_range(
    state: AppState,
    bucket: &str,
    key: &str,
    range_header: &str,
) -> Result<Response, ApiError> {
    // Parse Range header
    let range_spec = parse_range_header(range_header)?;

    // For suffix ranges, we need to get the object size first
    let (start, end) = match range_spec {
        RangeSpec::FromTo(s, e) => (s, e),
        RangeSpec::SuffixLength(suffix_len) => {
            // Get object metadata to know the size
            let meta = state.storage.head_object(bucket, key).await?;
            let size = meta.size;
            if suffix_len >= size {
                // If suffix length >= size, return entire object
                (0, size.saturating_sub(1))
            } else {
                // Last suffix_len bytes
                (size - suffix_len, size - 1)
            }
        }
    };

    let (meta, data) = state.storage.get_object_range(bucket, key, start, end).await?;

    let actual_end = start + data.len() as u64 - 1;
    let content_range = format!("bytes {start}-{actual_end}/{}", meta.size);

    let mut response = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", data.len().to_string())
        .header("Content-Range", content_range)
        .header("Accept-Ranges", "bytes");

    if let Some(ct) = &meta.content_type {
        response = response.header("Content-Type", ct.as_str());
    }
    if let Some(cc) = &meta.cache_control {
        response = response.header("Cache-Control", cc.as_str());
    }
    if let Some(cd) = &meta.content_disposition {
        response = response.header("Content-Disposition", cd.as_str());
    }
    if let Some(ce) = &meta.content_encoding {
        response = response.header("Content-Encoding", ce.as_str());
    }
    if let Some(exp) = &meta.expires {
        response = response.header("Expires", exp.as_str());
    }
    if let Some(cl) = &meta.content_language {
        response = response.header("Content-Language", cl.as_str());
    }

    response
        .body(Body::from(data))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// Represents a parsed HTTP Range specification.
#[derive(Debug, PartialEq)]
enum RangeSpec {
    /// bytes=X-Y or bytes=X- (from X to Y or to end)
    FromTo(u64, u64),
    /// bytes=-Y (last Y bytes)
    SuffixLength(u64),
}

fn parse_range_header(header: &str) -> Result<RangeSpec, ApiError> {
    let range = header
        .strip_prefix("bytes=")
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRange, "Invalid Range header format"))?;

    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRange, "Invalid Range header format"))?;

    if start.is_empty() {
        // Suffix range: bytes=-N (last N bytes)
        let suffix_length: u64 = end
            .parse()
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidRange, "Invalid range suffix length"))?;
        Ok(RangeSpec::SuffixLength(suffix_length))
    } else {
        let start: u64 = start
            .parse()
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidRange, "Invalid range start"))?;

        let end: u64 = if end.is_empty() {
            u64::MAX
        } else {
            end.parse()
                .map_err(|_| ApiError::new(S3ErrorCode::InvalidRange, "Invalid range end"))?
        };

        Ok(RangeSpec::FromTo(start, end))
    }
}

/// `DELETE /{bucket}/{key}` - Delete object.
pub async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
) -> Result<Response, ApiError> {
    // Check bucket exists first to return proper error code
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    let result = if let Some(ref version_id) = query.version_id {
        // Delete specific version permanently
        state.storage.delete_object_version(&bucket, &key, version_id).await?
    } else {
        // Delete latest version (may create delete marker for versioned buckets)
        state.storage.delete_object(&bucket, &key).await?
    };

    let mut response = Response::builder().status(StatusCode::NO_CONTENT);

    if let Some(version_id) = result.version_id {
        response = response.header("x-amz-version-id", version_id);
    }
    if result.is_delete_marker {
        response = response.header("x-amz-delete-marker", "true");
    }

    Ok(response.body(axum::body::Body::empty()).unwrap())
}

/// `HEAD /{bucket}/{key}` - Get object metadata.
pub async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<crate::router::RequestQuery>,
) -> Result<Response, ApiError> {
    // Check bucket exists first to return proper error code
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Get metadata, either latest or specific version
    let meta = if let Some(ref vid) = query.version_id {
        state.storage.head_object_version(&bucket, &key, vid).await?
    } else {
        state.storage.head_object(&bucket, &key).await?
    };

    // Check If-Match: return 412 Precondition Failed if ETag doesn't match
    if let Some(if_match) = headers.get("if-match").and_then(|v| v.to_str().ok()) {
        let etag = meta.etag.as_str();
        let etags: Vec<&str> = if_match.split(',').map(|s| s.trim()).collect();
        if !etags
            .iter()
            .any(|&e| e == "*" || e == etag || e.trim_matches('"') == etag.trim_matches('"'))
        {
            return Err(ApiError::new(S3ErrorCode::PreconditionFailed, "Precondition failed"));
        }
    }

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", meta.size.to_string())
        .header("Last-Modified", format_http_date(&meta.last_modified))
        .header("Accept-Ranges", "bytes");

    // Add version ID header if the object has one
    if let Some(ref version_id) = meta.version_id {
        response = response.header("x-amz-version-id", version_id.as_str());
    }

    if let Some(ct) = &meta.content_type {
        response = response.header("Content-Type", ct.as_str());
    }
    if let Some(cc) = &meta.cache_control {
        response = response.header("Cache-Control", cc.as_str());
    }
    if let Some(cd) = &meta.content_disposition {
        response = response.header("Content-Disposition", cd.as_str());
    }
    if let Some(ce) = &meta.content_encoding {
        response = response.header("Content-Encoding", ce.as_str());
    }
    if let Some(exp) = &meta.expires {
        response = response.header("Expires", exp.as_str());
    }
    if let Some(cl) = &meta.content_language {
        response = response.header("Content-Language", cl.as_str());
    }

    // Add user metadata headers
    // boto3 sends UTF-8 in headers but expects Latin-1 back (per HTTP/1.1 spec).
    // Convert Unicode chars to Latin-1 bytes (U+0000-U+00FF -> 0x00-0xFF).
    for (key, value) in &meta.user_metadata {
        let latin1_bytes: Vec<u8> = value
            .chars()
            .filter_map(|c| {
                let code = c as u32;
                if code <= 0xFF {
                    Some(code as u8)
                } else {
                    None
                }
            })
            .collect();
        if let Ok(header_value) = HeaderValue::from_bytes(&latin1_bytes) {
            response = response.header(format!("x-amz-meta-{key}"), header_value);
        }
    }

    // Add checksum header if checksum mode is enabled and we have a stored checksum
    let checksum_mode_enabled = headers
        .get("x-amz-checksum-mode")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.eq_ignore_ascii_case("ENABLED"))
        .unwrap_or(false);

    if checksum_mode_enabled {
        if let Some(algorithm) = meta.checksum_algorithm {
            if let Some(checksum) = meta.get_checksum(algorithm) {
                response = response.header(algorithm.header_name(), checksum.to_base64());
            }
        }
    }

    response
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// `PUT /{bucket}/{key}` with `x-amz-copy-source` - Copy object.
pub async fn copy_object(
    State(state): State<AppState>,
    Path((dst_bucket, dst_key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // Check conditional headers for destination object
    check_preconditions(&state, &dst_bucket, &dst_key, &headers).await?;

    let copy_source =
        headers.get("x-amz-copy-source").and_then(|v| v.to_str().ok()).ok_or_else(|| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Missing x-amz-copy-source header")
        })?;

    // Parse source: /bucket/key or bucket/key
    let source = copy_source.trim_start_matches('/');
    let (src_bucket, src_key) = source.split_once('/').ok_or_else(|| {
        ApiError::new(S3ErrorCode::InvalidRequest, "Invalid x-amz-copy-source format")
    })?;

    // URL-decode the source key (it may be percent-encoded)
    let src_key = s3_url_decode(src_key);

    // Check x-amz-copy-source-if-match: fail if source ETag doesn't match
    if let Some(if_match) = headers.get("x-amz-copy-source-if-match").and_then(|v| v.to_str().ok())
    {
        let src_meta = state.storage.head_object(src_bucket, &src_key).await?;
        let src_etag = src_meta.etag.as_str();
        let etags: Vec<&str> = if_match.split(',').map(|s| s.trim()).collect();
        if !etags.iter().any(|&e| {
            e == "*" || e == src_etag || e.trim_matches('"') == src_etag.trim_matches('"')
        }) {
            return Err(ApiError::new(S3ErrorCode::PreconditionFailed, "Precondition failed"));
        }
    }

    // Check MetadataDirective - COPY (default) or REPLACE
    let metadata_directive =
        headers.get("x-amz-metadata-directive").and_then(|v| v.to_str().ok()).unwrap_or("COPY");

    // Check for copy-to-self without metadata replacement
    // Per S3, copying an object to itself is only allowed with MetadataDirective=REPLACE
    if src_bucket == dst_bucket && src_key == dst_key && metadata_directive != "REPLACE" {
        return Err(ApiError::new(
            S3ErrorCode::InvalidRequest,
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
        ));
    }

    // If MetadataDirective=REPLACE, extract new headers and metadata from request
    let (new_headers, new_metadata) = if metadata_directive == "REPLACE" {
        let object_headers = ObjectHeaders {
            content_type: headers
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            cache_control: headers
                .get("cache-control")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            content_disposition: headers
                .get("content-disposition")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            content_encoding: headers
                .get("content-encoding")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            expires: headers.get("expires").and_then(|v| v.to_str().ok()).map(String::from),
            content_language: headers
                .get("content-language")
                .and_then(|v| v.to_str().ok())
                .map(String::from),
            // For copy with REPLACE, we don't support changing checksum algorithm
            checksum_algorithm: None,
        };
        let user_metadata = extract_user_metadata(&headers);
        (Some(object_headers), Some(user_metadata))
    } else {
        (None, None)
    };

    let etag = state
        .storage
        .copy_object(src_bucket, &src_key, &dst_bucket, &dst_key, new_headers, new_metadata)
        .await?;

    let response = CopyObjectResponse::new(etag.as_str().to_string(), Utc::now());

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}` - List objects V1.
pub async fn list_objects_v1(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, ApiError> {
    // Ceph extension: allow-unordered with delimiter is not supported
    if state.compatibility_mode == ApiCompatibilityMode::Ceph
        && query.allow_unordered.as_deref() == Some("true")
        && query.delimiter.is_some()
    {
        return Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            "allow-unordered cannot be used with delimiter",
        ));
    }

    let max_keys = parse_max_keys(&query.max_keys)?;

    let result = state
        .storage
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            query.marker.as_deref(),
            max_keys,
        )
        .await?;

    // For V1, NextMarker is only set when using a delimiter and is_truncated
    // It should be the lexicographically last item returned (object key OR common prefix)
    let next_marker = if result.is_truncated && query.delimiter.is_some() {
        // Find the last key and last prefix, then return the lexicographically greater one
        let last_key = result.objects.last().map(|o| o.key.as_str());
        let last_prefix = result.common_prefixes.last().map(|s| s.as_str());
        match (last_key, last_prefix) {
            (Some(key), Some(prefix)) => Some(std::cmp::max(key, prefix).to_string()),
            (Some(key), None) => Some(key.to_string()),
            (None, Some(prefix)) => Some(prefix.to_string()),
            (None, None) => None,
        }
    } else {
        None
    };

    // Check if URL encoding is requested
    let use_url_encoding = query.encoding_type.as_deref() == Some("url");

    let response = if use_url_encoding {
        ListObjectsV1Response {
            name: bucket,
            prefix: s3_url_encode_prefix(&query.prefix.unwrap_or_default()),
            marker: s3_url_encode_prefix(&query.marker.unwrap_or_default()),
            next_marker: next_marker.map(|m| s3_url_encode_prefix(&m)),
            // Note: Delimiter is NOT encoded per S3 spec
            delimiter: query.delimiter.clone(),
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            contents: result
                .objects
                .iter()
                .map(|m| ObjectEntry::from(m).with_encoded_key(&s3_url_encode(&m.key)))
                .collect(),
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: s3_url_encode_prefix(&p) })
                .collect(),
        }
    } else {
        // In non-encoded mode, URL-decode the prefix and marker
        // (axum may not fully decode special characters like %0A)
        ListObjectsV1Response {
            name: bucket,
            prefix: s3_url_decode(&query.prefix.unwrap_or_default()),
            marker: s3_url_decode(&query.marker.unwrap_or_default()),
            next_marker,
            delimiter: query.delimiter,
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            contents: result.objects.iter().map(ObjectEntry::from).collect(),
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: p })
                .collect(),
        }
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}?list-type=2` - List objects V2.
pub async fn list_objects_v2(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, ApiError> {
    // Ceph extension: allow-unordered with delimiter is not supported
    if state.compatibility_mode == ApiCompatibilityMode::Ceph
        && query.allow_unordered.as_deref() == Some("true")
        && query.delimiter.is_some()
    {
        return Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            "allow-unordered cannot be used with delimiter",
        ));
    }

    let max_keys = parse_max_keys(&query.max_keys)?;

    // Handle start-after: use it as the continuation token if no continuation token is provided
    let effective_token = query.continuation_token.as_deref().or(query.start_after.as_deref());

    let result = state
        .storage
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            effective_token,
            max_keys,
        )
        .await?;

    // Check if fetch-owner is requested
    let fetch_owner = query.fetch_owner.as_deref() == Some("true");

    // Check if URL encoding is requested
    let use_url_encoding = query.encoding_type.as_deref() == Some("url");

    // For V2 with delimiter, use the last returned item as continuation token
    // This ensures the next page starts after all returned items (including prefixes)
    let next_continuation_token = if result.is_truncated {
        if query.delimiter.is_some() {
            // Use the lexicographically last item returned
            let last_key = result.objects.last().map(|o| o.key.as_str());
            let last_prefix = result.common_prefixes.last().map(|s| s.as_str());
            match (last_key, last_prefix) {
                (Some(key), Some(prefix)) => Some(std::cmp::max(key, prefix).to_string()),
                (Some(key), None) => Some(key.to_string()),
                (None, Some(prefix)) => Some(prefix.to_string()),
                (None, None) => result.next_continuation_token.clone(),
            }
        } else {
            // No delimiter - use storage layer's token (the next key to start from)
            result.next_continuation_token.clone()
        }
    } else {
        None
    };

    let response = if use_url_encoding {
        ListObjectsV2Response {
            name: bucket,
            prefix: query.prefix.map(|p| s3_url_encode_prefix(&p)),
            // Note: Delimiter is NOT encoded per S3 spec
            delimiter: query.delimiter.clone(),
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            continuation_token: query.continuation_token,
            next_continuation_token: next_continuation_token.clone(),
            start_after: query.start_after.map(|s| s3_url_encode_prefix(&s)),
            key_count: (result.objects.len() + result.common_prefixes.len()) as u32,
            contents: if fetch_owner {
                result
                    .objects
                    .iter()
                    .map(|m| ObjectEntry::with_owner(m).with_encoded_key(&s3_url_encode(&m.key)))
                    .collect()
            } else {
                result
                    .objects
                    .iter()
                    .map(|m| ObjectEntry::from(m).with_encoded_key(&s3_url_encode(&m.key)))
                    .collect()
            },
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: s3_url_encode_prefix(&p) })
                .collect(),
        }
    } else {
        ListObjectsV2Response {
            name: bucket,
            prefix: query.prefix,
            delimiter: query.delimiter,
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            continuation_token: query.continuation_token,
            next_continuation_token,
            start_after: query.start_after,
            key_count: (result.objects.len() + result.common_prefixes.len()) as u32,
            contents: if fetch_owner {
                result.objects.iter().map(ObjectEntry::with_owner).collect()
            } else {
                result.objects.iter().map(ObjectEntry::from).collect()
            },
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: p })
                .collect(),
        }
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}?versions` - List object versions.
///
/// Returns all versions of objects in a bucket, including delete markers.
/// For non-versioned buckets, returns all objects with a version ID of "null".
pub async fn list_object_versions(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, ApiError> {
    let max_keys = parse_max_keys(&query.max_keys)?;

    // Use the real list_object_versions method that returns all versions
    let result = state
        .storage
        .list_object_versions(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            query.key_marker.as_deref(),
            query.version_id_marker.as_deref(),
            max_keys,
        )
        .await?;

    // Separate versions and delete markers
    let mut versions: Vec<ObjectVersion> = Vec::new();
    let mut delete_markers: Vec<DeleteMarker> = Vec::new();

    for entry in result.versions {
        if entry.is_delete_marker {
            delete_markers.push(DeleteMarker::from_metadata(&entry.metadata));
        } else {
            versions.push(ObjectVersion::from_metadata(&entry.metadata));
        }
    }

    let response = ListObjectVersionsResponse {
        name: bucket,
        prefix: query.prefix,
        key_marker: query.key_marker.clone().unwrap_or_default(),
        version_id_marker: query.version_id_marker.clone().unwrap_or_default(),
        next_key_marker: result.next_key_marker,
        next_version_id_marker: result.next_version_id_marker,
        max_keys,
        is_truncated: result.is_truncated,
        versions,
        delete_markers,
        common_prefixes: result
            .common_prefixes
            .into_iter()
            .map(|p| CommonPrefix { prefix: p })
            .collect(),
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `POST /{bucket}?delete` - Delete multiple objects.
pub async fn delete_objects(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Parse the XML request body
    let request: DeleteObjects = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        ApiError::new(S3ErrorCode::InvalidRequest, format!("Failed to parse DeleteObjects: {e}"))
    })?;

    // S3 limits DeleteObjects to 1000 keys
    if request.objects.len() > 1000 {
        return Err(ApiError::new(
            S3ErrorCode::InvalidRequest,
            "DeleteObjects request must contain at most 1000 keys",
        ));
    }

    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    // Delete each object
    for obj in &request.objects {
        let result = if let Some(ref version_id) = obj.version_id {
            // Delete specific version
            state.storage.delete_object_version(&bucket, &obj.key, version_id).await
        } else {
            // Delete latest version (may create delete marker for versioned buckets)
            state.storage.delete_object(&bucket, &obj.key).await
        };

        match result {
            Ok(delete_result) => {
                // Return the version ID from the result, or from the request, or "null"
                let version_id = delete_result
                    .version_id
                    .or_else(|| obj.version_id.clone())
                    .or_else(|| Some("null".to_string()));
                deleted.push(DeletedObject { key: obj.key.clone(), version_id });
            }
            Err(e) => {
                // Convert storage error to S3 error code and message
                let api_err: ApiError = e.into();
                errors.push(DeleteError {
                    key: obj.key.clone(),
                    code: api_err.code.as_str().to_string(),
                    message: api_err.message,
                });
            }
        }
    }

    // Build response - in quiet mode, only report errors
    let response = if request.quiet {
        DeleteObjectsResponse { deleted: Vec::new(), errors }
    } else {
        DeleteObjectsResponse { deleted, errors }
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}/{key}?tagging` - Get object tagging.
pub async fn get_object_tagging(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
) -> Result<Response, ApiError> {
    // Get tags (this also verifies bucket and object exist)
    let tag_set = if let Some(ref version_id) = query.version_id {
        state.storage.get_object_tagging_version(&bucket, &key, version_id).await?
    } else {
        state.storage.get_object_tagging(&bucket, &key).await?
    };

    let response = TaggingResponse {
        tag_set: TagSetResponse {
            tags: tag_set
                .tags
                .into_iter()
                .map(|t| TagResponse { key: t.key, value: t.value })
                .collect(),
        },
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    let mut response_builder =
        Response::builder().status(StatusCode::OK).header("Content-Type", "application/xml");

    // Add version ID header if provided
    if let Some(ref version_id) = query.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id.as_str());
    }

    response_builder
        .body(Body::from(xml))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// Maximum number of tags allowed for an object.
const MAX_OBJECT_TAGS: usize = 10;
/// Maximum length of a tag key.
const MAX_TAG_KEY_LENGTH: usize = 128;
/// Maximum length of a tag value.
const MAX_TAG_VALUE_LENGTH: usize = 256;

/// `PUT /{bucket}/{key}?tagging` - Set object tagging.
pub async fn put_object_tagging(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Parse the tagging XML
    let tagging: TaggingRequest = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        ApiError::new(S3ErrorCode::MalformedXML, format!("Failed to parse tagging XML: {e}"))
    })?;

    // Validate tag count
    if tagging.tag_set.tags.len() > MAX_OBJECT_TAGS {
        return Err(ApiError::new(
            S3ErrorCode::InvalidTag,
            format!(
                "Object tags cannot be greater than {}. You have {} tags.",
                MAX_OBJECT_TAGS,
                tagging.tag_set.tags.len()
            ),
        ));
    }

    // Validate each tag
    for tag in &tagging.tag_set.tags {
        // Check for empty key
        if tag.key.is_empty() {
            return Err(ApiError::new(S3ErrorCode::InvalidTag, "Tag key cannot be empty"));
        }
        // Check key length
        if tag.key.len() > MAX_TAG_KEY_LENGTH {
            return Err(ApiError::new(
                S3ErrorCode::InvalidTag,
                format!("Tag key exceeds the maximum length of {} characters", MAX_TAG_KEY_LENGTH),
            ));
        }
        // Check value length
        if tag.value.len() > MAX_TAG_VALUE_LENGTH {
            return Err(ApiError::new(
                S3ErrorCode::InvalidTag,
                format!(
                    "Tag value exceeds the maximum length of {} characters",
                    MAX_TAG_VALUE_LENGTH
                ),
            ));
        }
    }

    // Convert to TagSet
    let tag_set = TagSet::with_tags(
        tagging.tag_set.tags.into_iter().map(|t| Tag::new(t.key, t.value)).collect(),
    );

    // Store tags (this also verifies bucket and object exist)
    if let Some(ref version_id) = query.version_id {
        state.storage.put_object_tagging_version(&bucket, &key, version_id, tag_set).await?;
    } else {
        state.storage.put_object_tagging(&bucket, &key, tag_set).await?;
    }

    let mut response_builder = Response::builder().status(StatusCode::OK);

    // Add version ID header if provided
    if let Some(ref version_id) = query.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id.as_str());
    }

    response_builder
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// `DELETE /{bucket}/{key}?tagging` - Delete object tagging.
pub async fn delete_object_tagging(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
) -> Result<Response, ApiError> {
    // Delete tags (this also verifies bucket and object exist)
    if let Some(ref version_id) = query.version_id {
        state.storage.delete_object_tagging_version(&bucket, &key, version_id).await?;
    } else {
        state.storage.delete_object_tagging(&bucket, &key).await?;
    }

    let mut response_builder = Response::builder().status(StatusCode::NO_CONTENT);

    // Add version ID header if provided
    if let Some(ref version_id) = query.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id.as_str());
    }

    response_builder
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// Object attributes that can be requested.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectAttribute {
    /// ETag attribute.
    ETag,
    /// Checksum attribute.
    Checksum,
    /// ObjectParts attribute.
    ObjectParts,
    /// StorageClass attribute.
    StorageClass,
    /// ObjectSize attribute.
    ObjectSize,
}

impl ObjectAttribute {
    /// Parse an attribute from its S3 API string representation.
    fn from_str(s: &str) -> Option<Self> {
        match s.trim() {
            "ETag" => Some(Self::ETag),
            "Checksum" => Some(Self::Checksum),
            "ObjectParts" => Some(Self::ObjectParts),
            "StorageClass" => Some(Self::StorageClass),
            "ObjectSize" => Some(Self::ObjectSize),
            _ => None,
        }
    }
}

/// `GET /{bucket}/{key}?attributes` - Get object attributes.
pub async fn get_object_attributes(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // Parse requested attributes from x-amz-object-attributes header
    // The SDK may send multiple headers or a single comma-separated header
    let mut requested_attrs: Vec<ObjectAttribute> = Vec::new();
    for value in headers.get_all("x-amz-object-attributes") {
        if let Ok(s) = value.to_str() {
            for part in s.split(',') {
                if let Some(attr) = ObjectAttribute::from_str(part) {
                    requested_attrs.push(attr);
                }
            }
        }
    }

    if requested_attrs.is_empty() {
        return Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            "x-amz-object-attributes header is required",
        ));
    }

    // Get object metadata
    let metadata = if let Some(ref version_id) = query.version_id {
        state.storage.head_object_version(&bucket, &key, version_id).await?
    } else {
        state.storage.head_object(&bucket, &key).await?
    };

    // Build response based on requested attributes
    let mut response = GetObjectAttributesResponse {
        etag: None,
        last_modified: None,
        object_size: None,
        storage_class: None,
        checksum: None,
        object_parts: None,
    };

    for attr in &requested_attrs {
        match attr {
            ObjectAttribute::ETag => {
                // Remove quotes from ETag for response
                let etag = metadata.etag.as_str().trim_matches('"').to_string();
                response.etag = Some(etag);
            }
            ObjectAttribute::ObjectSize => {
                response.object_size = Some(metadata.size as i64);
            }
            ObjectAttribute::StorageClass => {
                response.storage_class = Some("STANDARD".to_string());
            }
            ObjectAttribute::Checksum => {
                // We store CRC32C, so include it if available
                if let Some(crc32c) = metadata.crc32c {
                    // Encode as base64
                    use base64::Engine as _;
                    let bytes = crc32c.to_be_bytes();
                    let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                    response.checksum = Some(ObjectChecksum {
                        crc32: None,
                        crc32c: Some(encoded),
                        sha1: None,
                        sha256: None,
                    });
                }
            }
            ObjectAttribute::ObjectParts => {
                // Check if this was a multipart upload by looking at the ETag
                // Multipart ETags have the format "etag-partcount"
                let etag_str = metadata.etag.as_str();
                if let Some(idx) = etag_str.rfind('-') {
                    if let Ok(part_count) = etag_str[idx + 1..].trim_matches('"').parse::<i32>() {
                        response.object_parts = Some(ObjectParts {
                            total_parts_count: Some(part_count),
                            part_number_marker: None,
                            next_part_number_marker: None,
                            max_parts: None,
                            is_truncated: Some(false),
                            parts: Vec::new(),
                        });
                    }
                }
            }
        }
    }

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    let mut response_builder =
        Response::builder().status(StatusCode::OK).header("Content-Type", "application/xml");

    // Add version ID header if provided
    if let Some(ref version_id) = query.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id.as_str());
    }

    // Add last modified header
    response_builder =
        response_builder.header("Last-Modified", format_http_date(&metadata.last_modified));

    response_builder
        .body(Body::from(xml))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use rucket_core::config::ApiCompatibilityMode;
    use rucket_storage::{LocalStorage, ObjectHeaders, StorageBackend};
    use tempfile::TempDir;

    use super::*;
    use crate::handlers::bucket::AppState;
    use crate::router::RequestQuery;

    /// Create a RequestQuery with a specific version_id.
    fn query_with_version_id(version_id: String) -> RequestQuery {
        // Use serde to create a RequestQuery with the version_id set
        serde_json::from_str(&format!(r#"{{"versionId": "{}"}}"#, version_id)).unwrap()
    }

    #[test]
    fn test_parse_range_header() {
        assert_eq!(parse_range_header("bytes=0-499").unwrap(), RangeSpec::FromTo(0, 499));
        assert_eq!(parse_range_header("bytes=500-999").unwrap(), RangeSpec::FromTo(500, 999));
        assert_eq!(parse_range_header("bytes=0-").unwrap(), RangeSpec::FromTo(0, u64::MAX));
        // Suffix range
        assert_eq!(parse_range_header("bytes=-500").unwrap(), RangeSpec::SuffixLength(500));
    }

    #[test]
    fn test_parse_range_header_invalid() {
        assert!(parse_range_header("invalid").is_err());
        assert!(parse_range_header("bytes=abc-def").is_err());
    }

    /// Create a test storage and app state.
    async fn create_test_state() -> (AppState, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let temp_storage_dir = temp_dir.path().join("temp");
        let storage = LocalStorage::new(data_dir, temp_storage_dir).await.unwrap();
        let state = AppState {
            storage: Arc::new(storage),
            compatibility_mode: ApiCompatibilityMode::S3Strict,
        };
        (state, temp_dir)
    }

    /// Create a test bucket and object.
    async fn setup_bucket_and_object(state: &AppState) {
        state.storage.create_bucket("test-bucket").await.unwrap();
        state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("test content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_get_object_tagging_empty() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        let result = get_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_and_get_object_tagging() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Put tagging
        let tagging_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>env</Key>
                        <Value>test</Value>
                    </Tag>
                </TagSet>
            </Tagging>"#;

        let put_result = put_object_tagging(
            State(state.clone()),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
            Bytes::from(tagging_xml),
        )
        .await;

        assert!(put_result.is_ok());
        let response = put_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);

        // Get tagging and verify
        let get_result = get_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(get_result.is_ok());
        let response = get_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_object_tagging() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // First put some tags
        let tagging_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>env</Key>
                        <Value>test</Value>
                    </Tag>
                </TagSet>
            </Tagging>"#;

        put_object_tagging(
            State(state.clone()),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
            Bytes::from(tagging_xml),
        )
        .await
        .unwrap();

        // Delete tagging
        let delete_result = delete_object_tagging(
            State(state.clone()),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(delete_result.is_ok());
        let response = delete_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Verify tags are deleted (should return empty tag set)
        let get_result = get_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(get_result.is_ok());
    }

    #[tokio::test]
    async fn test_get_object_tagging_with_version_id() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Enable versioning
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        // Upload a new version
        let result = state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("version 2 content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();

        let version_id = result.version_id.unwrap();

        // Get tagging with version ID
        let get_result = get_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(query_with_version_id(version_id.clone())),
        )
        .await;

        assert!(get_result.is_ok());
        let response = get_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
        // Verify version ID header is present
        assert!(response.headers().get("x-amz-version-id").is_some());
    }

    #[tokio::test]
    async fn test_put_object_tagging_with_version_id() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Enable versioning
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        // Upload a new version
        let result = state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("version 2 content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();

        let version_id = result.version_id.unwrap();

        // Put tagging with version ID
        let tagging_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>version</Key>
                        <Value>2</Value>
                    </Tag>
                </TagSet>
            </Tagging>"#;

        let put_result = put_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(query_with_version_id(version_id.clone())),
            Bytes::from(tagging_xml),
        )
        .await;

        assert!(put_result.is_ok());
        let response = put_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
        // Verify version ID header is present
        assert!(response.headers().get("x-amz-version-id").is_some());
    }

    #[tokio::test]
    async fn test_delete_object_tagging_with_version_id() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Enable versioning
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        // Upload a new version
        let result = state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("version 2 content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();

        let version_id = result.version_id.unwrap();

        // Delete tagging with version ID
        let delete_result = delete_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(query_with_version_id(version_id.clone())),
        )
        .await;

        assert!(delete_result.is_ok());
        let response = delete_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        // Verify version ID header is present
        assert!(response.headers().get("x-amz-version-id").is_some());
    }

    #[tokio::test]
    async fn test_put_object_tagging_malformed_xml() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Put malformed tagging XML
        let put_result = put_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
            Bytes::from("not valid xml"),
        )
        .await;

        assert!(put_result.is_err());
    }

    #[tokio::test]
    async fn test_get_object_tagging_nonexistent_object() {
        let (state, _temp_dir) = create_test_state().await;
        state.storage.create_bucket("test-bucket").await.unwrap();

        // Try to get tags for non-existent object
        let result = get_object_tagging(
            State(state),
            Path(("test-bucket".to_string(), "nonexistent.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(result.is_err());
    }
}
