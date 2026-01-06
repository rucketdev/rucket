//! Multipart upload handlers.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use chrono::Utc;
use rucket_core::error::S3ErrorCode;
use rucket_core::types::StorageClass;
use rucket_storage::{ObjectHeaders, StorageBackend};
use serde::Deserialize;

use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::handlers::object::s3_url_decode;
use crate::xml::request::CompleteMultipartUpload;
use crate::xml::response::{
    to_xml, CompleteMultipartUploadResponse, CopyPartResult, InitiateMultipartUploadResponse,
    ListMultipartUploadsResponse, ListPartsResponse, MultipartUploadEntry, PartEntry,
};

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
fn is_aws_chunked(headers: &HeaderMap) -> bool {
    if let Some(sha256) = headers.get("x-amz-content-sha256") {
        if let Ok(val) = sha256.to_str() {
            if val.starts_with("STREAMING-") {
                return true;
            }
        }
    }
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
fn decode_aws_chunked(body: Bytes) -> Result<Bytes, ApiError> {
    let mut result = Vec::new();
    let data = body.as_ref();
    let mut pos = 0;

    while pos < data.len() {
        // Find end of chunk header line (CRLF)
        let header_end = find_crlf(data, pos).ok_or_else(|| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Malformed chunked encoding")
        })?;

        // Parse chunk header: "<size-hex>;chunk-signature=<sig>"
        let header = std::str::from_utf8(&data[pos..header_end]).map_err(|_| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Invalid UTF-8 in chunk header")
        })?;

        let size_str = header.split(';').next().unwrap_or("0");
        let chunk_size = usize::from_str_radix(size_str, 16)
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidRequest, "Invalid chunk size"))?;

        pos = header_end + 2;

        if chunk_size == 0 {
            break;
        }

        if pos + chunk_size > data.len() {
            return Err(ApiError::new(S3ErrorCode::InvalidRequest, "Chunk size exceeds data"));
        }
        result.extend_from_slice(&data[pos..pos + chunk_size]);
        pos += chunk_size;

        if pos + 2 <= data.len() && &data[pos..pos + 2] == b"\r\n" {
            pos += 2;
        }
    }

    Ok(Bytes::from(result))
}

fn find_crlf(data: &[u8], start: usize) -> Option<usize> {
    (start..data.len().saturating_sub(1)).find(|&i| data[i] == b'\r' && data[i + 1] == b'\n')
}

/// Query parameters for multipart operations.
#[derive(Debug, Deserialize)]
pub struct MultipartQuery {
    /// Upload ID.
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    /// Part number.
    #[serde(rename = "partNumber")]
    pub part_number: Option<u32>,
}

/// `POST /{bucket}/{key}?uploads` - Initiate multipart upload.
pub async fn create_multipart_upload(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let user_metadata = extract_user_metadata(&headers);

    // Parse storage class from header
    let storage_class = headers
        .get("x-amz-storage-class")
        .and_then(|v| v.to_str().ok())
        .and_then(StorageClass::parse);

    let object_headers = ObjectHeaders {
        content_type: headers.get("content-type").and_then(|v| v.to_str().ok()).map(String::from),
        cache_control: headers.get("cache-control").and_then(|v| v.to_str().ok()).map(String::from),
        content_disposition: headers
            .get("content-disposition")
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        content_encoding: headers
            .get("content-encoding")
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        content_language: headers
            .get("content-language")
            .and_then(|v| v.to_str().ok())
            .map(String::from),
        expires: headers.get("expires").and_then(|v| v.to_str().ok()).map(String::from),
        checksum_algorithm: None,
        storage_class,
    };

    let upload =
        state.storage.create_multipart_upload(&bucket, &key, object_headers, user_metadata).await?;

    let response = InitiateMultipartUploadResponse {
        bucket: upload.bucket,
        key: upload.key,
        upload_id: upload.upload_id,
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `PUT /{bucket}/{key}?partNumber=N&uploadId=ID` - Upload part.
pub async fn upload_part(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    let upload_id = query
        .upload_id
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing uploadId parameter"))?;

    let part_number = query.part_number.ok_or_else(|| {
        ApiError::new(S3ErrorCode::InvalidRequest, "Missing partNumber parameter")
    })?;

    // Decode AWS chunked encoding if present
    let body = if is_aws_chunked(&headers) { decode_aws_chunked(body)? } else { body };

    let etag = state.storage.upload_part(&bucket, &key, &upload_id, part_number, body).await?;

    Ok((StatusCode::OK, [("ETag", etag.to_string())]))
}

/// `PUT /{bucket}/{key}?partNumber=N&uploadId=ID` with `x-amz-copy-source` - Upload part copy.
pub async fn upload_part_copy(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let upload_id = query
        .upload_id
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing uploadId parameter"))?;

    let part_number = query.part_number.ok_or_else(|| {
        ApiError::new(S3ErrorCode::InvalidRequest, "Missing partNumber parameter")
    })?;

    // Parse x-amz-copy-source header
    let copy_source =
        headers.get("x-amz-copy-source").and_then(|v| v.to_str().ok()).ok_or_else(|| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Missing x-amz-copy-source header")
        })?;

    // Parse source: /bucket/key or bucket/key
    let source = copy_source.trim_start_matches('/');
    let (src_bucket, src_key) = source.split_once('/').ok_or_else(|| {
        ApiError::new(S3ErrorCode::InvalidRequest, "Invalid x-amz-copy-source format")
    })?;

    // URL-decode the source key
    let src_key = s3_url_decode(src_key);

    // Check for x-amz-copy-source-range header
    let copy_range = headers.get("x-amz-copy-source-range").and_then(|v| v.to_str().ok());

    // Get the source data (full object or range)
    let data = if let Some(range_header) = copy_range {
        // Parse range: bytes=start-end
        let range_spec = range_header.strip_prefix("bytes=").ok_or_else(|| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Invalid x-amz-copy-source-range format")
        })?;
        let (start_str, end_str) = range_spec.split_once('-').ok_or_else(|| {
            ApiError::new(S3ErrorCode::InvalidRequest, "Invalid x-amz-copy-source-range format")
        })?;
        let start: u64 = start_str
            .parse()
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidRequest, "Invalid range start"))?;
        let end: u64 = end_str
            .parse()
            .map_err(|_| ApiError::new(S3ErrorCode::InvalidRequest, "Invalid range end"))?;
        let (_, data) = state.storage.get_object_range(src_bucket, &src_key, start, end).await?;
        data
    } else {
        // Get full object
        let (_, data) = state.storage.get_object(src_bucket, &src_key).await?;
        data
    };

    // Upload the part
    let etag = state.storage.upload_part(&bucket, &key, &upload_id, part_number, data).await?;

    // Return CopyPartResult XML
    let response = CopyPartResult::new(etag.to_string(), Utc::now());

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `POST /{bucket}/{key}?uploadId=ID` - Complete multipart upload.
pub async fn complete_multipart_upload(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let upload_id = query
        .upload_id
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing uploadId parameter"))?;

    // Parse the XML request body
    let request: CompleteMultipartUpload =
        quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
            ApiError::new(
                S3ErrorCode::InvalidRequest,
                format!("Failed to parse CompleteMultipartUpload: {e}"),
            )
        })?;

    // Convert to (part_number, etag) pairs
    let parts: Vec<(u32, String)> =
        request.parts.into_iter().map(|p| (p.part_number, p.etag)).collect();

    // Validate that parts are in ascending order by part number
    for i in 1..parts.len() {
        if parts[i].0 <= parts[i - 1].0 {
            return Err(ApiError::new(
                S3ErrorCode::InvalidPartOrder,
                "The list of parts was not in ascending order. Parts must be ordered by part number.",
            ));
        }
    }

    let etag = state.storage.complete_multipart_upload(&bucket, &key, &upload_id, &parts).await?;

    let response = CompleteMultipartUploadResponse {
        location: format!("/{bucket}/{key}"),
        bucket: bucket.clone(),
        key: key.clone(),
        etag: etag.to_string(),
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `DELETE /{bucket}/{key}?uploadId=ID` - Abort multipart upload.
pub async fn abort_multipart_upload(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<MultipartQuery>,
) -> Result<impl IntoResponse, ApiError> {
    let upload_id = query
        .upload_id
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing uploadId parameter"))?;

    state.storage.abort_multipart_upload(&bucket, &key, &upload_id).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// `GET /{bucket}/{key}?uploadId=ID` - List parts.
pub async fn list_parts(
    State(state): State<AppState>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<ListPartsQuery>,
) -> Result<Response, ApiError> {
    let upload_id = query
        .upload_id
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing uploadId parameter"))?;

    let max_parts = query.max_parts.unwrap_or(1000).min(1000);
    let part_number_marker = query.part_number_marker.unwrap_or(0);

    // Get the upload info to retrieve storage class
    let upload = state.storage.get_multipart_upload(&upload_id).await?;

    let parts = state.storage.list_parts(&bucket, &key, &upload_id).await?;

    // Filter by part number marker
    let mut parts: Vec<_> =
        parts.into_iter().filter(|p| p.part_number > part_number_marker).collect();

    // Sort by part number
    parts.sort_by_key(|p| p.part_number);

    // Apply max_parts limit
    let is_truncated = parts.len() > max_parts as usize;
    let result_parts: Vec<_> = parts.into_iter().take(max_parts as usize).collect();

    // Determine next part number marker
    let next_part_number_marker =
        if is_truncated { result_parts.last().map(|p| p.part_number) } else { None };

    let response = ListPartsResponse {
        bucket,
        key,
        upload_id,
        part_number_marker,
        next_part_number_marker,
        max_parts,
        is_truncated,
        initiator: crate::xml::response::Owner::default(),
        owner: crate::xml::response::Owner::default(),
        storage_class: upload.storage_class.as_str().to_string(),
        parts: result_parts.iter().map(PartEntry::from).collect(),
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// Query parameters for listing multipart uploads.
#[derive(Debug, Deserialize, Default)]
pub struct ListUploadsQuery {
    /// Prefix filter.
    pub prefix: Option<String>,
    /// Delimiter for grouping keys.
    pub delimiter: Option<String>,
    /// Maximum number of uploads to return.
    #[serde(rename = "max-uploads")]
    pub max_uploads: Option<u32>,
    /// Key marker for pagination.
    #[serde(rename = "key-marker")]
    pub key_marker: Option<String>,
    /// Upload ID marker for pagination.
    #[serde(rename = "upload-id-marker")]
    pub upload_id_marker: Option<String>,
}

/// Query parameters for listing parts.
#[derive(Debug, Deserialize, Default)]
pub struct ListPartsQuery {
    /// Upload ID.
    #[serde(rename = "uploadId")]
    pub upload_id: Option<String>,
    /// Maximum number of parts to return.
    #[serde(rename = "max-parts")]
    pub max_parts: Option<u32>,
    /// Part number marker for pagination.
    #[serde(rename = "part-number-marker")]
    pub part_number_marker: Option<u32>,
}

/// `GET /{bucket}?uploads` - List multipart uploads.
pub async fn list_multipart_uploads(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    Query(query): Query<ListUploadsQuery>,
) -> Result<Response, ApiError> {
    use std::collections::BTreeSet;

    use crate::xml::response::CommonPrefix;

    let max_uploads = query.max_uploads.unwrap_or(1000).min(1000);
    let uploads = state.storage.list_multipart_uploads(&bucket).await?;

    // Filter by prefix if provided
    let mut uploads: Vec<_> =
        uploads
            .into_iter()
            .filter(|u| {
                if let Some(ref prefix) = query.prefix {
                    u.key.starts_with(prefix)
                } else {
                    true
                }
            })
            .collect();

    // Sort by key for consistent ordering
    uploads.sort_by(|a, b| a.key.cmp(&b.key));

    // Apply key-marker pagination
    if let Some(ref key_marker) = query.key_marker {
        uploads.retain(|u| u.key.as_str() > key_marker.as_str());
    }

    // Handle delimiter and common prefixes
    let mut common_prefixes: BTreeSet<String> = BTreeSet::new();
    let mut filtered_uploads = Vec::new();

    if let Some(ref delimiter) = query.delimiter {
        let prefix_len = query.prefix.as_ref().map_or(0, |p| p.len());

        for upload in uploads {
            let key_after_prefix = &upload.key[prefix_len..];
            if let Some(pos) = key_after_prefix.find(delimiter.as_str()) {
                // Found delimiter - add to common prefixes
                let prefix = format!(
                    "{}{}",
                    query.prefix.as_deref().unwrap_or(""),
                    &key_after_prefix[..=pos]
                );
                common_prefixes.insert(prefix);
            } else {
                // No delimiter - include in results
                filtered_uploads.push(upload);
            }
        }
    } else {
        filtered_uploads = uploads;
    }

    // Apply max_uploads limit
    let is_truncated = filtered_uploads.len() > max_uploads as usize;
    let result_uploads: Vec<_> = filtered_uploads.into_iter().take(max_uploads as usize).collect();

    // Determine next markers
    let (next_key_marker, next_upload_id_marker) = if is_truncated {
        result_uploads
            .last()
            .map(|u| (Some(u.key.clone()), Some(u.upload_id.clone())))
            .unwrap_or((None, None))
    } else {
        (None, None)
    };

    let response = ListMultipartUploadsResponse {
        bucket,
        key_marker: query.key_marker.clone(),
        upload_id_marker: query.upload_id_marker.clone(),
        next_key_marker,
        next_upload_id_marker,
        max_uploads,
        is_truncated,
        prefix: query.prefix.clone(),
        delimiter: query.delimiter.clone(),
        common_prefixes: common_prefixes.into_iter().map(|p| CommonPrefix { prefix: p }).collect(),
        uploads: result_uploads.iter().map(MultipartUploadEntry::from).collect(),
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}
