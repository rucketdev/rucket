// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Core CRUD operations for objects: put, get, delete, head, copy.

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use bytes::Bytes;
use chrono::Utc;
use percent_encoding::percent_decode_str;
use rucket_core::error::S3ErrorCode;
use rucket_core::policy::S3Action;
use rucket_core::types::{ChecksumAlgorithm, StorageClass, Tag, TagSet};
use rucket_storage::streaming::compute_checksum;
use rucket_storage::{ObjectHeaders, StorageBackend};

use super::common::{
    check_preconditions, decode_aws_chunked, extract_user_metadata, format_http_date,
    is_aws_chunked, parse_client_checksum, s3_url_decode, ResponseHeaderOverrides, MAX_OBJECT_TAGS,
    MAX_TAG_KEY_LENGTH, MAX_TAG_VALUE_LENGTH,
};
use super::sse_c::{
    decrypt_sse_c, encrypt_sse_c, is_sse_c_encrypted, parse_sse_c_headers,
    require_sse_c_for_encrypted_object, HEADER_SSE_CUSTOMER_ALGORITHM, HEADER_SSE_CUSTOMER_KEY_MD5,
};
use crate::auth::AuthContext;
use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::policy::{evaluate_bucket_policy, get_auth_context, RequestInfo};
use crate::xml::response::{to_xml, CopyObjectResponse};

/// `PUT /{bucket}/{key}` - Upload object.
pub async fn put_object(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::from_headers(&headers);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::PutObject,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Check conditional headers for optimistic locking
    check_preconditions(&state, &bucket, &key, &headers).await?;

    // Parse x-amz-tagging header early to fail fast on invalid tags
    let tagging =
        if let Some(tagging_header) = headers.get("x-amz-tagging").and_then(|v| v.to_str().ok()) {
            Some(parse_tagging_header(tagging_header)?)
        } else {
            None
        };

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

    // Parse storage class from header
    let storage_class = headers
        .get("x-amz-storage-class")
        .and_then(|v| v.to_str().ok())
        .and_then(StorageClass::parse);

    // Parse client-provided checksum value for validation
    let client_checksum = parse_client_checksum(&headers).map_err(|e| {
        ApiError::new(S3ErrorCode::BadDigest, format!("Invalid checksum format: {e}"))
    })?;

    // Parse SSE-C headers
    let sse_c_headers = parse_sse_c_headers(&headers)?;

    // Decode AWS chunked encoding if present
    let body = if is_aws_chunked(&headers) { decode_aws_chunked(body)? } else { body };

    // Validate client-provided checksum before storage (before encryption)
    if let Some((algorithm, ref expected)) = client_checksum {
        let computed = compute_checksum(&body, algorithm);
        if &computed != expected {
            return Err(ApiError::new(
                S3ErrorCode::BadDigest,
                "The Content-MD5 or checksum value that you specified did not match what the server received.",
            ));
        }
    }

    // Encrypt body with SSE-C if headers are present
    let (body, sse_c_algorithm, sse_c_key_md5, encryption_nonce) = if let Some(ref sse_c) =
        sse_c_headers
    {
        let (encrypted_data, nonce) = encrypt_sse_c(sse_c, body)?;
        (encrypted_data, Some(sse_c.algorithm.clone()), Some(sse_c.key_md5.clone()), Some(nonce))
    } else {
        (body, None, None, None)
    };

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
        storage_class,
        sse_customer_algorithm: sse_c_algorithm,
        sse_customer_key_md5: sse_c_key_md5,
        encryption_nonce,
    };

    let result =
        state.storage.put_object(&bucket, &key, body, object_headers, user_metadata).await?;

    // Apply tags if x-amz-tagging header was provided
    if let Some(tag_set) = tagging {
        if let Some(ref version_id) = result.version_id {
            state.storage.put_object_tagging_version(&bucket, &key, version_id, tag_set).await?;
        } else {
            state.storage.put_object_tagging(&bucket, &key, tag_set).await?;
        }
    }

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

    // Add server-side encryption header if encryption was applied
    if let Some(ref sse) = result.server_side_encryption {
        response = response.header("x-amz-server-side-encryption", sse.as_str());
    }

    // Add SSE-C response headers if customer encryption was used
    if let Some(ref algorithm) = result.sse_customer_algorithm {
        response = response.header(HEADER_SSE_CUSTOMER_ALGORITHM, algorithm.as_str());
    }
    if let Some(ref key_md5) = result.sse_customer_key_md5 {
        response = response.header(HEADER_SSE_CUSTOMER_KEY_MD5, key_md5.as_str());
    }

    response
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// Parse x-amz-tagging header into a TagSet.
///
/// The header format is URL-encoded `key=value` pairs separated by `&`.
/// Example: `key1=value1&key2=value2`
fn parse_tagging_header(header_value: &str) -> Result<TagSet, ApiError> {
    let mut tags = Vec::new();

    for pair in header_value.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = match pair.split_once('=') {
            Some((k, v)) => (k, v),
            None => (pair, ""),
        };

        // URL-decode key and value
        let key = percent_decode_str(key)
            .decode_utf8()
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidTag, "Invalid UTF-8 in tag key"))?
            .to_string();
        let value = percent_decode_str(value)
            .decode_utf8()
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidTag, "Invalid UTF-8 in tag value"))?
            .to_string();

        // Validate tag key
        if key.is_empty() {
            return Err(ApiError::new(S3ErrorCode::InvalidTag, "Tag key cannot be empty"));
        }
        if key.len() > MAX_TAG_KEY_LENGTH {
            return Err(ApiError::new(
                S3ErrorCode::InvalidTag,
                format!("Tag key exceeds the maximum length of {} characters", MAX_TAG_KEY_LENGTH),
            ));
        }
        // Validate tag value
        if value.len() > MAX_TAG_VALUE_LENGTH {
            return Err(ApiError::new(
                S3ErrorCode::InvalidTag,
                format!(
                    "Tag value exceeds the maximum length of {} characters",
                    MAX_TAG_VALUE_LENGTH
                ),
            ));
        }

        tags.push(Tag::new(key, value));
    }

    // Validate tag count
    if tags.len() > MAX_OBJECT_TAGS {
        return Err(ApiError::new(
            S3ErrorCode::InvalidTag,
            format!(
                "Object tags cannot be greater than {}. You have {} tags.",
                MAX_OBJECT_TAGS,
                tags.len()
            ),
        ));
    }

    Ok(TagSet::with_tags(tags))
}

/// `GET /{bucket}/{key}` - Download object.
pub async fn get_object(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
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

    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::from_headers(&headers);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::GetObject,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Parse Range header if present
    let range_header = headers.get("range").and_then(|v| v.to_str().ok());

    // Handle range request for non-versioned objects using efficient storage method
    if let Some(range) = range_header {
        if version_id.is_none() {
            return get_object_range(state, &bucket, &key, range).await;
        }
    }

    // Get object, either latest or specific version
    let (meta, data) = if let Some(ref vid) = version_id {
        state.storage.get_object_version(&bucket, &key, vid).await?
    } else {
        state.storage.get_object(&bucket, &key).await?
    };

    // Check if object is SSE-C encrypted and decrypt if so
    let sse_c_headers = require_sse_c_for_encrypted_object(&headers, &meta)?;
    let data = if let Some(ref sse_c) = sse_c_headers {
        // Object is SSE-C encrypted, decrypt it
        let nonce = meta.encryption_nonce.as_ref().ok_or_else(|| {
            ApiError::new(
                S3ErrorCode::InternalError,
                "SSE-C encrypted object is missing encryption nonce",
            )
        })?;
        let stored_key_md5 = meta.sse_customer_key_md5.as_ref().ok_or_else(|| {
            ApiError::new(S3ErrorCode::InternalError, "SSE-C encrypted object is missing key MD5")
        })?;
        decrypt_sse_c(sse_c, data, nonce, stored_key_md5)?
    } else {
        data
    };

    // Handle range request for versioned objects (extract range from full data)
    if let Some(range) = range_header {
        return get_object_version_range(meta, data, range);
    }

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
    // HTTP dates only have second precision, so we compare using Unix timestamps
    // to avoid false negatives from sub-second differences.
    if let Some(if_modified_since) = headers.get("if-modified-since").and_then(|v| v.to_str().ok())
    {
        if let Ok(since) = chrono::DateTime::parse_from_rfc2822(if_modified_since) {
            // Compare at second precision: object not modified if last_modified <= since
            if meta.last_modified.timestamp() <= since.timestamp() {
                return Response::builder()
                    .status(StatusCode::NOT_MODIFIED)
                    .header("ETag", meta.etag.as_str())
                    .body(Body::empty())
                    .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()));
            }
        }
    }

    // Check If-Unmodified-Since: return 412 if object was modified since the date
    // HTTP dates only have second precision, so we compare using Unix timestamps.
    if let Some(if_unmodified_since) =
        headers.get("if-unmodified-since").and_then(|v| v.to_str().ok())
    {
        if let Ok(since) = chrono::DateTime::parse_from_rfc2822(if_unmodified_since) {
            // Compare at second precision: object modified if last_modified > since
            if meta.last_modified.timestamp() > since.timestamp() {
                return Err(ApiError::new(S3ErrorCode::PreconditionFailed, "Precondition failed"));
            }
        }
    }

    // For SSE-C encrypted objects, use the actual decrypted data length
    let content_length = if is_sse_c_encrypted(&meta) { data.len() as u64 } else { meta.size };

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", content_length.to_string())
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

    // Add server-side encryption header if object is encrypted
    if let Some(ref sse) = meta.server_side_encryption {
        response = response.header("x-amz-server-side-encryption", sse.as_str());
    }

    // Add storage class header if not STANDARD
    if meta.storage_class != StorageClass::Standard {
        response = response.header("x-amz-storage-class", meta.storage_class.as_str());
    }

    // Add SSE-C response headers if customer encryption was used
    if let Some(ref algorithm) = meta.sse_customer_algorithm {
        response = response.header(HEADER_SSE_CUSTOMER_ALGORITHM, algorithm.as_str());
    }
    if let Some(ref key_md5) = meta.sse_customer_key_md5 {
        response = response.header(HEADER_SSE_CUSTOMER_KEY_MD5, key_md5.as_str());
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

    // Add server-side encryption header if object is encrypted
    if let Some(ref sse) = meta.server_side_encryption {
        response = response.header("x-amz-server-side-encryption", sse.as_str());
    }

    // Add storage class header if not STANDARD
    if meta.storage_class != StorageClass::Standard {
        response = response.header("x-amz-storage-class", meta.storage_class.as_str());
    }

    response
        .body(Body::from(data))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// Represents a parsed HTTP Range specification.
#[derive(Debug, PartialEq)]
pub enum RangeSpec {
    /// bytes=X-Y or bytes=X- (from X to Y or to end)
    FromTo(u64, u64),
    /// bytes=-Y (last Y bytes)
    SuffixLength(u64),
}

/// Parse an HTTP Range header into a RangeSpec.
pub fn parse_range_header(header: &str) -> Result<RangeSpec, ApiError> {
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

/// Handle range request for a versioned object by extracting the range from loaded data.
fn get_object_version_range(
    meta: rucket_core::types::ObjectMetadata,
    data: Bytes,
    range_header: &str,
) -> Result<Response, ApiError> {
    let range_spec = parse_range_header(range_header)?;
    let size = meta.size;

    // Calculate actual start and end
    let (start, end) = match range_spec {
        RangeSpec::FromTo(s, e) => {
            if s > size {
                return Err(ApiError::new(
                    S3ErrorCode::InvalidRange,
                    "The requested range is not satisfiable",
                ));
            }
            let actual_end = e.min(size.saturating_sub(1));
            (s, actual_end)
        }
        RangeSpec::SuffixLength(suffix_len) => {
            if suffix_len >= size {
                (0, size.saturating_sub(1))
            } else {
                (size - suffix_len, size - 1)
            }
        }
    };

    // Extract the range from data
    let start_usize = start as usize;
    let end_usize = (end + 1).min(size) as usize;
    let range_data = data.slice(start_usize..end_usize);

    let content_range = format!("bytes {start}-{end}/{size}");

    let mut response = Response::builder()
        .status(StatusCode::PARTIAL_CONTENT)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", range_data.len().to_string())
        .header("Content-Range", content_range)
        .header("Accept-Ranges", "bytes");

    // Add version ID header if present
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

    // Add server-side encryption header if object is encrypted
    if let Some(ref sse) = meta.server_side_encryption {
        response = response.header("x-amz-server-side-encryption", sse.as_str());
    }

    // Add storage class header if not STANDARD
    if meta.storage_class != StorageClass::Standard {
        response = response.header("x-amz-storage-class", meta.storage_class.as_str());
    }

    response
        .body(Body::from(range_data))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// `DELETE /{bucket}/{key}` - Delete object.
pub async fn delete_object(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
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

    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::from_headers(&headers);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::DeleteObject,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Check for bypass governance header
    let bypass_governance = headers
        .get("x-amz-bypass-governance-retention")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Check Object Lock retention before deleting
    // SECURITY: Fail-secure - if we can't verify retention, deny deletion
    // NOTE: Only check retention when a specific version_id is provided.
    // When no version_id is specified on a versioned bucket, we create a delete marker,
    // which doesn't actually remove the protected version - it just hides it.
    if let Some(ref version_id) = query.version_id {
        // Check specific version - this is a permanent deletion
        match state.storage.head_object_version(&bucket, &key, version_id).await {
            Ok(meta) => {
                if !meta.can_delete(bypass_governance) {
                    return Err(ApiError::new(
                        S3ErrorCode::AccessDenied,
                        "Object is protected by Object Lock",
                    )
                    .with_resource(&key));
                }
            }
            Err(rucket_core::Error::S3 {
                code: S3ErrorCode::NoSuchKey | S3ErrorCode::NoSuchVersion,
                ..
            }) => {
                // Object/version doesn't exist - allow delete (will be no-op)
            }
            Err(e) => {
                // Any other error - fail secure, deny deletion
                tracing::error!(error = %e, "Failed to check object lock retention, denying delete");
                return Err(ApiError::new(
                    S3ErrorCode::InternalError,
                    "Could not verify Object Lock status",
                ));
            }
        }
    }
    // When no version_id is specified, we allow creating a delete marker even for locked objects.
    // The locked version remains protected; the delete marker just hides the object.

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
    auth: Option<Extension<AuthContext>>,
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

    // Evaluate bucket policy (HEAD uses same permission as GET)
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::from_headers(&headers);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::GetObject,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Get metadata, either latest or specific version
    let meta = if let Some(ref vid) = query.version_id {
        state.storage.head_object_version(&bucket, &key, vid).await?
    } else {
        state.storage.head_object(&bucket, &key).await?
    };

    // For SSE-C encrypted objects, require SSE-C headers
    // (HEAD returns metadata only, doesn't need to decrypt, but AWS requires headers)
    require_sse_c_for_encrypted_object(&headers, &meta)?;

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

    // For SSE-C encrypted objects, the stored size includes the 16-byte auth tag
    // Return the plaintext size (encrypted_size - 16)
    const AES_GCM_TAG_SIZE: u64 = 16;
    let content_length = if is_sse_c_encrypted(&meta) {
        meta.size.saturating_sub(AES_GCM_TAG_SIZE)
    } else {
        meta.size
    };

    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header("ETag", meta.etag.as_str())
        .header("Content-Length", content_length.to_string())
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

    // Add server-side encryption header if object is encrypted
    if let Some(ref sse) = meta.server_side_encryption {
        response = response.header("x-amz-server-side-encryption", sse.as_str());
    }

    // Add storage class header if not STANDARD (STANDARD is the default and often omitted)
    if meta.storage_class != StorageClass::Standard {
        response = response.header("x-amz-storage-class", meta.storage_class.as_str());
    }

    // Add SSE-C response headers if customer encryption was used
    if let Some(ref algorithm) = meta.sse_customer_algorithm {
        response = response.header(HEADER_SSE_CUSTOMER_ALGORITHM, algorithm.as_str());
    }
    if let Some(ref key_md5) = meta.sse_customer_key_md5 {
        response = response.header(HEADER_SSE_CUSTOMER_KEY_MD5, key_md5.as_str());
    }

    response
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// `PUT /{bucket}/{key}` with `x-amz-copy-source` - Copy object.
pub async fn copy_object(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((dst_bucket, dst_key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let copy_source =
        headers.get("x-amz-copy-source").and_then(|v| v.to_str().ok()).ok_or_else(|| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Missing x-amz-copy-source header")
        })?;

    // Parse source: /bucket/key or bucket/key
    let source = copy_source.trim_start_matches('/');
    let (src_bucket, src_key_raw) = source.split_once('/').ok_or_else(|| {
        ApiError::new(S3ErrorCode::InvalidRequest, "Invalid x-amz-copy-source format")
    })?;

    // URL-decode the source key (it may be percent-encoded)
    let src_key = s3_url_decode(src_key_raw);

    // Evaluate bucket policies for both source and destination
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::from_headers(&headers);

    // Check GetObject permission on source
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        src_bucket,
        Some(&src_key),
        S3Action::GetObject,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Check PutObject permission on destination
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &dst_bucket,
        Some(&dst_key),
        S3Action::PutObject,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Check conditional headers for destination object
    check_preconditions(&state, &dst_bucket, &dst_key, &headers).await?;

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

    // Check x-amz-copy-source-if-none-match: fail if source ETag DOES match
    if let Some(if_none_match) =
        headers.get("x-amz-copy-source-if-none-match").and_then(|v| v.to_str().ok())
    {
        let src_meta = state.storage.head_object(src_bucket, &src_key).await?;
        let src_etag = src_meta.etag.as_str();
        let etags: Vec<&str> = if_none_match.split(',').map(|s| s.trim()).collect();
        if etags.iter().any(|&e| {
            e == "*" || e == src_etag || e.trim_matches('"') == src_etag.trim_matches('"')
        }) {
            return Err(ApiError::new(S3ErrorCode::PreconditionFailed, "Precondition failed"));
        }
    }

    // Check MetadataDirective - COPY (default) or REPLACE
    let metadata_directive =
        headers.get("x-amz-metadata-directive").and_then(|v| v.to_str().ok()).unwrap_or("COPY");

    // Parse storage class - can be changed independent of MetadataDirective
    let requested_storage_class = headers
        .get("x-amz-storage-class")
        .and_then(|v| v.to_str().ok())
        .and_then(StorageClass::parse);

    // Check for copy-to-self without metadata replacement or storage class change
    // Per S3, copying an object to itself is only allowed if changing metadata, storage class, etc.
    if src_bucket == dst_bucket
        && src_key == dst_key
        && metadata_directive != "REPLACE"
        && requested_storage_class.is_none()
    {
        return Err(ApiError::new(
            S3ErrorCode::InvalidRequest,
            "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
        ));
    }

    // If MetadataDirective=REPLACE, extract new headers and metadata from request
    // If MetadataDirective=COPY but storage_class is specified, only override storage class
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
            storage_class: requested_storage_class,
            // SSE-C is not currently supported for copy operations
            sse_customer_algorithm: None,
            sse_customer_key_md5: None,
            encryption_nonce: None,
        };
        let user_metadata = extract_user_metadata(&headers);
        (Some(object_headers), Some(user_metadata))
    } else if requested_storage_class.is_some() {
        // MetadataDirective=COPY but storage class is specified - only override storage class
        let object_headers = ObjectHeaders {
            content_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            expires: None,
            content_language: None,
            checksum_algorithm: None,
            storage_class: requested_storage_class,
            // SSE-C is not currently supported for copy operations
            sse_customer_algorithm: None,
            sse_customer_key_md5: None,
            encryption_nonce: None,
        };
        (Some(object_headers), None)
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
