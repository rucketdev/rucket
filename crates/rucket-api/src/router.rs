// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! S3 API router configuration.

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use bytes::Bytes;
use serde::Deserialize;
use std::sync::Arc;

use rucket_storage::LocalStorage;

use crate::handlers::bucket::{self, AppState};
use crate::handlers::{multipart, object};

/// Query parameters to determine request type.
#[derive(Debug, Deserialize, Default)]
struct RequestQuery {
    /// List type (for ListObjectsV2).
    #[serde(rename = "list-type")]
    #[allow(dead_code)]
    list_type: Option<u32>,
    /// Uploads marker (for multipart).
    uploads: Option<String>,
    /// Upload ID (for multipart).
    #[serde(rename = "uploadId")]
    upload_id: Option<String>,
    /// Part number (for multipart).
    #[serde(rename = "partNumber")]
    part_number: Option<u32>,
    /// Prefix filter (for ListObjectsV2).
    prefix: Option<String>,
    /// Delimiter for grouping (for ListObjectsV2).
    delimiter: Option<String>,
    /// Continuation token (for ListObjectsV2).
    #[serde(rename = "continuation-token")]
    continuation_token: Option<String>,
    /// Maximum keys to return (for ListObjectsV2).
    #[serde(rename = "max-keys")]
    max_keys: Option<u32>,
}

/// Create the S3 API router.
pub fn create_router(storage: Arc<LocalStorage>) -> Router {
    let state = AppState { storage };

    Router::new()
        // Service-level operations
        .route("/", get(list_buckets))
        // Bucket operations with complex routing (with and without trailing slash)
        .route(
            "/{bucket}",
            get(handle_bucket_get).put(create_bucket).delete(delete_bucket).head(head_bucket),
        )
        .route(
            "/{bucket}/",
            get(handle_bucket_get).put(create_bucket).delete(delete_bucket).head(head_bucket),
        )
        // Object operations with complex routing
        .route(
            "/{bucket}/{*key}",
            get(handle_object_get)
                .put(handle_object_put)
                .delete(handle_object_delete)
                .head(head_object)
                .post(handle_object_post),
        )
        .with_state(state)
}

// Re-export handlers with concrete types

async fn list_buckets(state: State<AppState>) -> Response {
    bucket::list_buckets(state).await.into_response()
}

async fn create_bucket(state: State<AppState>, path: Path<String>) -> Response {
    bucket::create_bucket(state, path).await.into_response()
}

async fn delete_bucket(state: State<AppState>, path: Path<String>) -> Response {
    bucket::delete_bucket(state, path).await.into_response()
}

async fn head_bucket(state: State<AppState>, path: Path<String>) -> Response {
    bucket::head_bucket(state, path).await.into_response()
}

async fn head_object(state: State<AppState>, path: Path<(String, String)>) -> Response {
    object::head_object(state, path).await.into_response()
}

/// Handle GET requests to bucket (list objects or list uploads).
async fn handle_bucket_get(
    state: State<AppState>,
    path: Path<String>,
    Query(query): Query<RequestQuery>,
) -> Response {
    // Check for ?uploads (list multipart uploads)
    if query.uploads.is_some() {
        return multipart::list_multipart_uploads(state, path).await.into_response();
    }

    // Default: ListObjectsV2
    let list_query = object::ListObjectsQuery {
        prefix: query.prefix,
        delimiter: query.delimiter,
        continuation_token: query.continuation_token,
        max_keys: query.max_keys.unwrap_or(1000),
    };

    object::list_objects_v2(state, path, Query(list_query)).await.into_response()
}

/// Handle GET requests to object (get object or list parts).
async fn handle_object_get(
    state: State<AppState>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
    headers: HeaderMap,
) -> Response {
    // Check for ?uploadId (list parts)
    if query.upload_id.is_some() {
        let mp_query = multipart::MultipartQuery {
            upload_id: query.upload_id,
            part_number: query.part_number,
        };
        return multipart::list_parts(state, path, Query(mp_query)).await.into_response();
    }

    // Default: GetObject
    object::get_object(state, path, headers).await.into_response()
}

/// Handle PUT requests to object (put object, upload part, or copy).
async fn handle_object_put(
    state: State<AppState>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Check for x-amz-copy-source (copy object)
    if headers.contains_key("x-amz-copy-source") {
        return object::copy_object(state, path, headers).await.into_response();
    }

    // Check for ?partNumber&uploadId (upload part)
    if query.part_number.is_some() && query.upload_id.is_some() {
        let mp_query = multipart::MultipartQuery {
            upload_id: query.upload_id,
            part_number: query.part_number,
        };
        return multipart::upload_part(state, path, Query(mp_query), body).await.into_response();
    }

    // Default: PutObject
    object::put_object(state, path, headers, body).await.into_response()
}

/// Handle DELETE requests to object (delete object or abort upload).
async fn handle_object_delete(
    state: State<AppState>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
) -> Response {
    // Check for ?uploadId (abort multipart upload)
    if query.upload_id.is_some() {
        let mp_query = multipart::MultipartQuery {
            upload_id: query.upload_id,
            part_number: query.part_number,
        };
        return multipart::abort_multipart_upload(state, path, Query(mp_query))
            .await
            .into_response();
    }

    // Default: DeleteObject
    object::delete_object(state, path).await.into_response()
}

/// Handle POST requests to object (initiate or complete multipart).
async fn handle_object_post(
    state: State<AppState>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
    body: Bytes,
) -> Response {
    // Check for ?uploads (initiate multipart upload)
    if query.uploads.is_some() {
        return multipart::create_multipart_upload(state, path).await.into_response();
    }

    // Check for ?uploadId (complete multipart upload)
    if query.upload_id.is_some() {
        let mp_query = multipart::MultipartQuery {
            upload_id: query.upload_id,
            part_number: query.part_number,
        };
        return multipart::complete_multipart_upload(state, path, Query(mp_query), body)
            .await
            .into_response();
    }

    // Unsupported POST
    crate::error::ApiError::new(
        rucket_core::error::S3ErrorCode::InvalidRequest,
        "Unsupported POST request",
    )
    .into_response()
}
