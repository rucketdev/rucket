//! Object operation handlers.

use std::collections::HashMap;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use rucket_core::error::S3ErrorCode;
use rucket_storage::StorageBackend;
use serde::Deserialize;

use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::xml::request::DeleteObjects;
use crate::xml::response::{
    to_xml, CommonPrefix, CopyObjectResponse, DeleteError, DeleteObjectsResponse, DeletedObject,
    ListObjectVersionsResponse, ListObjectsV2Response, ObjectEntry, ObjectVersion,
};

/// Format a datetime for HTTP headers (RFC 7231 format).
fn format_http_date(dt: &DateTime<Utc>) -> String {
    dt.format("%a, %d %b %Y %H:%M:%S GMT").to_string()
}

/// Extract user metadata from request headers.
///
/// User metadata headers start with `x-amz-meta-` (case-insensitive).
/// Returns a HashMap with lowercase keys (without the prefix) and the values.
fn extract_user_metadata(headers: &HeaderMap) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    for (name, value) in headers.iter() {
        let name_str = name.as_str().to_lowercase();
        if let Some(key) = name_str.strip_prefix("x-amz-meta-") {
            if let Ok(value_str) = value.to_str() {
                metadata.insert(key.to_string(), value_str.to_string());
            }
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

/// Query parameters for `ListObjectsV2`.
#[derive(Debug, Deserialize)]
pub struct ListObjectsQuery {
    /// Prefix filter.
    pub prefix: Option<String>,
    /// Delimiter for grouping.
    pub delimiter: Option<String>,
    /// Continuation token.
    #[serde(rename = "continuation-token")]
    pub continuation_token: Option<String>,
    /// StartAfter - where to start listing from.
    #[serde(rename = "start-after")]
    pub start_after: Option<String>,
    /// Encoding type for keys (url).
    #[serde(rename = "encoding-type")]
    pub encoding_type: Option<String>,
    /// Maximum keys to return.
    #[serde(rename = "max-keys", default = "default_max_keys")]
    pub max_keys: u32,
    /// FetchOwner - whether to include owner info.
    #[serde(rename = "fetch-owner")]
    pub fetch_owner: Option<String>,
}

fn default_max_keys() -> u32 {
    1000
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
) -> Result<impl IntoResponse, ApiError> {
    // Check conditional headers for optimistic locking
    check_preconditions(&state, &bucket, &key, &headers).await?;

    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let user_metadata = extract_user_metadata(&headers);

    // Decode AWS chunked encoding if present
    let body = if is_aws_chunked(&headers) { decode_aws_chunked(body)? } else { body };

    let etag = state.storage.put_object(&bucket, &key, body, content_type, user_metadata).await?;

    Ok((StatusCode::OK, [("ETag", etag.as_str().to_string())]))
}

/// `GET /{bucket}/{key}` - Download object.
pub async fn get_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // Check for Range header
    if let Some(range) = headers.get("range").and_then(|v| v.to_str().ok()) {
        return get_object_range(state, &bucket, &key, range).await;
    }

    let (meta, data) = state.storage.get_object(&bucket, &key).await?;

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", meta.size.to_string())
        .header("Last-Modified", format_http_date(&meta.last_modified));

    if let Some(ct) = &meta.content_type {
        response = response.header("Content-Type", ct.as_str());
    }

    // Add user metadata headers
    for (key, value) in &meta.user_metadata {
        response = response.header(format!("x-amz-meta-{key}"), value.as_str());
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
    // Parse Range: bytes=start-end
    let (start, end) = parse_range_header(range_header)?;

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

    response
        .body(Body::from(data))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

fn parse_range_header(header: &str) -> Result<(u64, u64), ApiError> {
    let range = header
        .strip_prefix("bytes=")
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRange, "Invalid Range header format"))?;

    let (start, end) = range
        .split_once('-')
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRange, "Invalid Range header format"))?;

    let start: u64 = start
        .parse()
        .map_err(|_| ApiError::new(S3ErrorCode::InvalidRange, "Invalid range start"))?;

    let end: u64 = if end.is_empty() {
        u64::MAX
    } else {
        end.parse().map_err(|_| ApiError::new(S3ErrorCode::InvalidRange, "Invalid range end"))?
    };

    Ok((start, end))
}

/// `DELETE /{bucket}/{key}` - Delete object.
pub async fn delete_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<impl IntoResponse, ApiError> {
    state.storage.delete_object(&bucket, &key).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `HEAD /{bucket}/{key}` - Get object metadata.
pub async fn head_object(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
) -> Result<Response, ApiError> {
    let meta = state.storage.head_object(&bucket, &key).await?;

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", meta.size.to_string())
        .header("Last-Modified", format_http_date(&meta.last_modified))
        .header("Accept-Ranges", "bytes");

    if let Some(ct) = &meta.content_type {
        response = response.header("Content-Type", ct.as_str());
    }

    // Add user metadata headers
    for (key, value) in &meta.user_metadata {
        response = response.header(format!("x-amz-meta-{key}"), value.as_str());
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

    let etag = state.storage.copy_object(src_bucket, src_key, &dst_bucket, &dst_key).await?;

    let response = CopyObjectResponse::new(etag.as_str().to_string(), Utc::now());

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
    // Handle start-after: use it as the continuation token if no continuation token is provided
    let effective_token = query.continuation_token.as_deref().or(query.start_after.as_deref());

    let result = state
        .storage
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            effective_token,
            query.max_keys,
        )
        .await?;

    let response = ListObjectsV2Response {
        name: bucket,
        prefix: query.prefix,
        delimiter: query.delimiter,
        max_keys: query.max_keys,
        encoding_type: query.encoding_type,
        is_truncated: result.is_truncated,
        continuation_token: query.continuation_token,
        next_continuation_token: result.next_continuation_token,
        start_after: query.start_after,
        key_count: result.objects.len() as u32,
        contents: result.objects.iter().map(ObjectEntry::from).collect(),
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

/// `GET /{bucket}?versions` - List object versions.
///
/// Returns all versions of objects in a bucket. For non-versioned buckets,
/// returns all objects with a version ID of "null".
pub async fn list_object_versions(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, ApiError> {
    // For now, use list_objects and convert to versions format
    // (full versioning support would require storage changes)
    let result = state
        .storage
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            query.continuation_token.as_deref(),
            query.max_keys,
        )
        .await?;

    // Convert objects to versions (with "null" version ID for non-versioned)
    let versions: Vec<ObjectVersion> =
        result.objects.iter().map(ObjectVersion::from_metadata).collect();

    let response = ListObjectVersionsResponse {
        name: bucket,
        prefix: query.prefix,
        key_marker: None,
        version_id_marker: None,
        next_key_marker: result.next_continuation_token.clone(),
        next_version_id_marker: None,
        max_keys: query.max_keys,
        is_truncated: result.is_truncated,
        versions,
        delete_markers: Vec::new(), // No delete markers for non-versioned buckets
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

    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    // Delete each object
    for obj in &request.objects {
        match state.storage.delete_object(&bucket, &obj.key).await {
            Ok(()) => {
                // Return the version ID from the request, or "null" for non-versioned objects
                let version_id = obj.version_id.clone().or_else(|| Some("null".to_string()));
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_range_header() {
        assert_eq!(parse_range_header("bytes=0-499").unwrap(), (0, 499));
        assert_eq!(parse_range_header("bytes=500-999").unwrap(), (500, 999));
        assert_eq!(parse_range_header("bytes=0-").unwrap(), (0, u64::MAX));
    }

    #[test]
    fn test_parse_range_header_invalid() {
        assert!(parse_range_header("invalid").is_err());
        assert!(parse_range_header("bytes=abc-def").is_err());
    }
}
