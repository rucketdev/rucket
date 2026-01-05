// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Object ACL and attributes operations.

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Extension;
use bytes::Bytes;
use rucket_core::error::S3ErrorCode;
use rucket_core::policy::S3Action;
use rucket_storage::StorageBackend;

use super::common::format_http_date;
use crate::auth::AuthContext;
use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::policy::{evaluate_bucket_policy, get_auth_context, RequestInfo};
use crate::xml::response::{to_xml, GetObjectAttributesResponse, ObjectChecksum, ObjectParts};

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
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy (GetObjectAttributes requires GetObject permission)
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

/// `GET /{bucket}/{key}?acl` - Get object ACL.
///
/// Returns a minimal ACL with owner having FULL_CONTROL.
/// This is a simplified implementation for single-tenant mode.
pub async fn get_object_acl(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::default();
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::GetObjectAcl,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Get object metadata to verify it exists
    let _metadata = if let Some(ref version_id) = query.version_id {
        state.storage.head_object_version(&bucket, &key, version_id).await?
    } else {
        state.storage.head_object(&bucket, &key).await?
    };

    // Return minimal ACL with owner having FULL_CONTROL
    let response = crate::xml::response::AccessControlPolicy::owner_full_control();
    let xml = crate::xml::response::to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    let mut response_builder =
        Response::builder().status(StatusCode::OK).header("Content-Type", "application/xml");

    if let Some(ref version_id) = query.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id.as_str());
    }

    response_builder
        .body(Body::from(xml))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// `PUT /{bucket}/{key}?acl` - Set object ACL.
///
/// Accepts but ignores ACL changes in single-tenant mode.
pub async fn put_object_acl(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
    _body: Bytes,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::default();
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::PutObjectAcl,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Get object metadata to verify it exists
    let _metadata = if let Some(ref version_id) = query.version_id {
        state.storage.head_object_version(&bucket, &key, version_id).await?
    } else {
        state.storage.head_object(&bucket, &key).await?
    };

    // Accept but ignore ACL changes (single-tenant mode)
    let mut response_builder = Response::builder().status(StatusCode::OK);

    if let Some(ref version_id) = query.version_id {
        response_builder = response_builder.header("x-amz-version-id", version_id.as_str());
    }

    response_builder
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}
