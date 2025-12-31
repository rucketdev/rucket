//! S3 API router configuration.

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware as axum_middleware, Router};
use bytes::Bytes;
use rucket_core::config::ApiCompatibilityMode;
use rucket_storage::LocalStorage;
use serde::Deserialize;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::handlers::bucket::{self, AppState};
use crate::handlers::{minio, multipart, object};
use crate::middleware::metrics_layer;

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
    /// Parsed as i32 to handle invalid negative values from some SDKs.
    #[serde(rename = "max-keys")]
    max_keys: Option<i32>,
    /// Delete marker (for DeleteObjects).
    delete: Option<String>,
}

/// Create the S3 API router.
///
/// # Arguments
/// * `storage` - The storage backend
/// * `max_body_size` - Maximum request body size in bytes (0 for unlimited)
/// * `compatibility_mode` - API compatibility mode (s3-strict or minio)
/// * `log_requests` - Whether to log HTTP requests
pub fn create_router(
    storage: Arc<LocalStorage>,
    max_body_size: u64,
    compatibility_mode: ApiCompatibilityMode,
    log_requests: bool,
) -> Router {
    let state = AppState { storage };

    let mut router = Router::new()
        // Service-level operations
        .route("/", get(list_buckets))
        // Bucket operations with complex routing (with and without trailing slash)
        .route(
            "/{bucket}",
            get(handle_bucket_get)
                .put(create_bucket)
                .delete(delete_bucket)
                .head(head_bucket)
                .post(handle_bucket_post),
        )
        .route(
            "/{bucket}/",
            get(handle_bucket_get)
                .put(create_bucket)
                .delete(delete_bucket)
                .head(head_bucket)
                .post(handle_bucket_post),
        )
        // Object operations with complex routing
        .route(
            "/{bucket}/{*key}",
            get(handle_object_get)
                .put(handle_object_put)
                .delete(handle_object_delete)
                .head(head_object)
                .post(handle_object_post),
        );

    // Add MinIO-specific routes if compatibility mode is Minio
    if compatibility_mode == ApiCompatibilityMode::Minio {
        router = router
            .route("/minio/health/live", get(minio::health_live))
            .route("/minio/health/ready", get(minio::health_ready));
    }

    let router = router.with_state(state);

    // Add metrics middleware
    let router = router.layer(axum_middleware::from_fn(metrics_layer));

    // Add HTTP request/response tracing if enabled
    let router = if log_requests {
        let trace_layer = TraceLayer::new_for_http()
            .make_span_with(DefaultMakeSpan::new().level(Level::INFO))
            .on_response(DefaultOnResponse::new().level(Level::INFO));
        router.layer(trace_layer)
    } else {
        router
    };

    // Apply body limit (0 means unlimited)
    if max_body_size > 0 {
        router.layer(DefaultBodyLimit::max(max_body_size as usize))
    } else {
        router.layer(DefaultBodyLimit::disable())
    }
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

/// Handle POST requests to bucket (delete multiple objects).
async fn handle_bucket_post(
    state: State<AppState>,
    path: Path<String>,
    Query(query): Query<RequestQuery>,
    body: Bytes,
) -> Response {
    // Check for ?delete (DeleteObjects)
    if query.delete.is_some() {
        return object::delete_objects(state, path, body).await.into_response();
    }

    // Unsupported POST to bucket
    crate::error::ApiError::new(
        rucket_core::error::S3ErrorCode::InvalidRequest,
        "Unsupported POST request to bucket",
    )
    .into_response()
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
    // Clamp max_keys to valid range [0, 1000], treating negative values as default
    let max_keys =
        query.max_keys.map(|v| if v < 0 { 1000 } else { v.min(1000) as u32 }).unwrap_or(1000);

    let list_query = object::ListObjectsQuery {
        prefix: query.prefix,
        delimiter: query.delimiter,
        continuation_token: query.continuation_token,
        max_keys,
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
