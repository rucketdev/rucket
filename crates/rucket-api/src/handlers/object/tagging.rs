// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Object tagging operations: get, put, delete object tagging.

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;
use axum::Extension;
use bytes::Bytes;
use rucket_core::error::S3ErrorCode;
use rucket_core::policy::S3Action;
use rucket_core::types::{Tag, TagSet};
use rucket_storage::StorageBackend;

use super::common::{MAX_OBJECT_TAGS, MAX_TAG_KEY_LENGTH, MAX_TAG_VALUE_LENGTH};
use crate::auth::AuthContext;
use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::policy::{evaluate_bucket_policy, get_auth_context, RequestInfo};
use crate::xml::request::Tagging as TaggingRequest;
use crate::xml::response::{to_xml, TagResponse, TagSetResponse, TaggingResponse};

/// `GET /{bucket}/{key}?tagging` - Get object tagging.
pub async fn get_object_tagging(
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
        S3Action::GetObjectTagging,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

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

/// `PUT /{bucket}/{key}?tagging` - Set object tagging.
pub async fn put_object_tagging(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::default();
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::PutObjectTagging,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

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
        S3Action::DeleteObjectTagging,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

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
