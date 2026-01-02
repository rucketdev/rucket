//! Multipart upload handlers.

use std::collections::HashMap;

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use rucket_core::error::S3ErrorCode;
use rucket_storage::StorageBackend;
use serde::Deserialize;

use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::xml::request::CompleteMultipartUpload;
use crate::xml::response::{
    to_xml, CompleteMultipartUploadResponse, InitiateMultipartUploadResponse,
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
    let content_type = headers.get("content-type").and_then(|v| v.to_str().ok());
    let user_metadata = extract_user_metadata(&headers);

    let upload =
        state.storage.create_multipart_upload(&bucket, &key, content_type, user_metadata).await?;

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
    Query(query): Query<MultipartQuery>,
) -> Result<Response, ApiError> {
    let upload_id = query
        .upload_id
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing uploadId parameter"))?;

    let parts = state.storage.list_parts(&bucket, &key, &upload_id).await?;

    let response = ListPartsResponse {
        bucket,
        key,
        upload_id,
        parts: parts.into_iter().map(|p| PartEntry::from(&p)).collect(),
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}?uploads` - List multipart uploads.
pub async fn list_multipart_uploads(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    let uploads = state.storage.list_multipart_uploads(&bucket).await?;

    let response = ListMultipartUploadsResponse {
        bucket,
        uploads: uploads.into_iter().map(|u| MultipartUploadEntry::from(&u)).collect(),
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}
