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
pub struct RequestQuery {
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
    /// Marker for ListObjectsV1 pagination.
    marker: Option<String>,
    /// Key marker for ListObjectVersions pagination.
    #[serde(rename = "key-marker")]
    key_marker: Option<String>,
    /// Version ID marker for ListObjectVersions pagination.
    #[serde(rename = "version-id-marker")]
    version_id_marker: Option<String>,
    /// Continuation token (for ListObjectsV2).
    #[serde(rename = "continuation-token")]
    continuation_token: Option<String>,
    /// StartAfter for ListObjectsV2.
    #[serde(rename = "start-after")]
    start_after: Option<String>,
    /// Encoding type (url) for ListObjectsV2.
    #[serde(rename = "encoding-type")]
    encoding_type: Option<String>,
    /// FetchOwner for ListObjectsV2.
    #[serde(rename = "fetch-owner")]
    fetch_owner: Option<String>,
    /// Maximum keys to return (for ListObjectsV2).
    /// Kept as String to allow handler to validate and return proper S3 errors.
    #[serde(rename = "max-keys")]
    max_keys: Option<String>,
    /// Delete marker (for DeleteObjects).
    delete: Option<String>,
    /// Versioning configuration.
    versioning: Option<String>,
    /// Bucket policy.
    policy: Option<String>,
    /// Bucket CORS configuration.
    cors: Option<String>,
    /// Bucket tagging.
    tagging: Option<String>,
    /// Bucket lifecycle.
    lifecycle: Option<String>,
    /// Object lock configuration.
    #[serde(rename = "object-lock")]
    object_lock: Option<String>,
    /// Bucket encryption.
    encryption: Option<String>,
    /// Bucket notification.
    notification: Option<String>,
    /// List object versions.
    versions: Option<String>,
    /// Bucket location.
    location: Option<String>,
    /// Override response Content-Type.
    #[serde(rename = "response-content-type")]
    response_content_type: Option<String>,
    /// Override response Content-Disposition.
    #[serde(rename = "response-content-disposition")]
    response_content_disposition: Option<String>,
    /// Override response Content-Encoding.
    #[serde(rename = "response-content-encoding")]
    response_content_encoding: Option<String>,
    /// Override response Content-Language.
    #[serde(rename = "response-content-language")]
    response_content_language: Option<String>,
    /// Override response Expires.
    #[serde(rename = "response-expires")]
    response_expires: Option<String>,
    /// Override response Cache-Control.
    #[serde(rename = "response-cache-control")]
    response_cache_control: Option<String>,
    /// Allow unordered listing (Ceph extension).
    #[serde(rename = "allow-unordered")]
    allow_unordered: Option<String>,
    /// Version ID for versioned object operations.
    #[serde(rename = "versionId")]
    pub version_id: Option<String>,
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
    let state = AppState { storage, compatibility_mode };

    let mut router = Router::new()
        // Service-level operations
        .route("/", get(list_buckets))
        // Bucket operations with complex routing (with and without trailing slash)
        .route(
            "/{bucket}",
            get(handle_bucket_get)
                .put(handle_bucket_put)
                .delete(handle_bucket_delete)
                .head(head_bucket)
                .post(handle_bucket_post),
        )
        .route(
            "/{bucket}/",
            get(handle_bucket_get)
                .put(handle_bucket_put)
                .delete(handle_bucket_delete)
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

/// Handle PUT requests to bucket (create bucket, versioning, policy, etc.).
async fn handle_bucket_put(
    state: State<AppState>,
    path: Path<String>,
    Query(query): Query<RequestQuery>,
    body: Bytes,
) -> Response {
    // Check for ?versioning (SetBucketVersioning)
    if query.versioning.is_some() {
        return bucket::set_bucket_versioning(state, path, body).await.into_response();
    }
    // Check for ?policy (SetBucketPolicy)
    if query.policy.is_some() {
        return bucket::set_bucket_policy(state, path, body).await.into_response();
    }
    // Check for ?cors (SetBucketCORS)
    if query.cors.is_some() {
        return bucket::set_bucket_cors(state, path, body).await.into_response();
    }
    // Check for ?tagging (SetBucketTagging)
    if query.tagging.is_some() {
        return bucket::set_bucket_tagging(state, path, body).await.into_response();
    }
    // Check for ?lifecycle (SetBucketLifecycle)
    if query.lifecycle.is_some() {
        return bucket::set_bucket_lifecycle(state, path, body).await.into_response();
    }
    // Check for ?object-lock (SetObjectLockConfiguration)
    if query.object_lock.is_some() {
        return bucket::set_object_lock_configuration(state, path, body).await.into_response();
    }
    // Check for ?encryption (SetBucketEncryption)
    if query.encryption.is_some() {
        return bucket::set_bucket_encryption(state, path, body).await.into_response();
    }
    // Check for ?notification (SetBucketNotification)
    if query.notification.is_some() {
        return bucket::set_bucket_notification(state, path, body).await.into_response();
    }
    // Default: CreateBucket
    bucket::create_bucket(state, path).await.into_response()
}

/// Handle DELETE requests to bucket (delete bucket, policy, tagging, etc.).
async fn handle_bucket_delete(
    state: State<AppState>,
    path: Path<String>,
    Query(query): Query<RequestQuery>,
) -> Response {
    // Check for ?policy (DeleteBucketPolicy)
    if query.policy.is_some() {
        return bucket::delete_bucket_policy(state, path).await.into_response();
    }
    // Check for ?cors (DeleteBucketCORS)
    if query.cors.is_some() {
        return bucket::delete_bucket_cors(state, path).await.into_response();
    }
    // Check for ?tagging (DeleteBucketTagging)
    if query.tagging.is_some() {
        return bucket::delete_bucket_tagging(state, path).await.into_response();
    }
    // Check for ?lifecycle (DeleteBucketLifecycle)
    if query.lifecycle.is_some() {
        return bucket::delete_bucket_lifecycle(state, path).await.into_response();
    }
    // Check for ?encryption (DeleteBucketEncryption)
    if query.encryption.is_some() {
        return bucket::delete_bucket_encryption(state, path).await.into_response();
    }
    // Default: DeleteBucket
    bucket::delete_bucket(state, path).await.into_response()
}

async fn head_bucket(state: State<AppState>, path: Path<String>) -> Response {
    bucket::head_bucket(state, path).await.into_response()
}

async fn head_object(
    state: State<AppState>,
    path: Path<(String, String)>,
    headers: HeaderMap,
    query: Query<RequestQuery>,
) -> Response {
    object::head_object(state, path, headers, query).await.into_response()
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

/// Handle GET requests to bucket (list objects, uploads, or config).
async fn handle_bucket_get(
    state: State<AppState>,
    path: Path<String>,
    Query(query): Query<RequestQuery>,
) -> Response {
    // Check for ?uploads (list multipart uploads)
    if query.uploads.is_some() {
        let list_query = multipart::ListUploadsQuery { prefix: query.prefix.clone() };
        return multipart::list_multipart_uploads(state, path, Query(list_query))
            .await
            .into_response();
    }
    // Check for ?versioning (GetBucketVersioning)
    if query.versioning.is_some() {
        return bucket::get_bucket_versioning(state, path).await.into_response();
    }
    // Check for ?policy (GetBucketPolicy)
    if query.policy.is_some() {
        return bucket::get_bucket_policy(state, path).await.into_response();
    }
    // Check for ?cors (GetBucketCORS)
    if query.cors.is_some() {
        return bucket::get_bucket_cors(state, path).await.into_response();
    }
    // Check for ?tagging (GetBucketTagging)
    if query.tagging.is_some() {
        return bucket::get_bucket_tagging(state, path).await.into_response();
    }
    // Check for ?lifecycle (GetBucketLifecycle)
    if query.lifecycle.is_some() {
        return bucket::get_bucket_lifecycle(state, path).await.into_response();
    }
    // Check for ?object-lock (GetObjectLockConfiguration)
    if query.object_lock.is_some() {
        return bucket::get_object_lock_configuration(state, path).await.into_response();
    }
    // Check for ?encryption (GetBucketEncryption)
    if query.encryption.is_some() {
        return bucket::get_bucket_encryption(state, path).await.into_response();
    }
    // Check for ?notification (GetBucketNotification)
    if query.notification.is_some() {
        return bucket::get_bucket_notification(state, path).await.into_response();
    }
    // Check for ?location (GetBucketLocation)
    if query.location.is_some() {
        return bucket::get_bucket_location(state, path).await.into_response();
    }

    // Check for ?versions (ListObjectVersions)
    if query.versions.is_some() {
        let list_query = object::ListObjectsQuery {
            prefix: query.prefix,
            delimiter: query.delimiter,
            marker: query.marker,
            key_marker: query.key_marker,
            version_id_marker: query.version_id_marker,
            continuation_token: query.continuation_token,
            start_after: query.start_after,
            encoding_type: query.encoding_type,
            max_keys: query.max_keys,
            fetch_owner: query.fetch_owner,
            allow_unordered: query.allow_unordered,
        };
        return object::list_object_versions(state, path, Query(list_query)).await.into_response();
    }

    let list_query = object::ListObjectsQuery {
        prefix: query.prefix,
        delimiter: query.delimiter,
        marker: query.marker,
        key_marker: query.key_marker,
        version_id_marker: query.version_id_marker,
        continuation_token: query.continuation_token,
        start_after: query.start_after,
        encoding_type: query.encoding_type,
        max_keys: query.max_keys,
        fetch_owner: query.fetch_owner,
        allow_unordered: query.allow_unordered,
    };

    // Use V1 or V2 based on list-type parameter
    // list-type=2 means V2, otherwise V1
    if query.list_type == Some(2) {
        object::list_objects_v2(state, path, Query(list_query)).await.into_response()
    } else {
        object::list_objects_v1(state, path, Query(list_query)).await.into_response()
    }
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

    // Check for ?tagging (GetObjectTagging)
    if query.tagging.is_some() {
        let tagging_query =
            RequestQuery { version_id: query.version_id.clone(), ..Default::default() };
        return object::get_object_tagging(state, path, Query(tagging_query)).await.into_response();
    }

    // Build response header overrides
    let overrides = object::ResponseHeaderOverrides {
        content_type: query.response_content_type,
        content_disposition: query.response_content_disposition,
        content_encoding: query.response_content_encoding,
        content_language: query.response_content_language,
        expires: query.response_expires,
        cache_control: query.response_cache_control,
    };

    // Default: GetObject
    object::get_object(state, path, headers, overrides, query.version_id).await.into_response()
}

/// Handle PUT requests to object (put object, upload part, or copy).
async fn handle_object_put(
    state: State<AppState>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Check for ?partNumber&uploadId with x-amz-copy-source (upload part copy)
    if query.part_number.is_some()
        && query.upload_id.is_some()
        && headers.contains_key("x-amz-copy-source")
    {
        let mp_query = multipart::MultipartQuery {
            upload_id: query.upload_id,
            part_number: query.part_number,
        };
        return multipart::upload_part_copy(state, path, Query(mp_query), headers)
            .await
            .into_response();
    }

    // Check for x-amz-copy-source (copy object)
    if headers.contains_key("x-amz-copy-source") {
        return object::copy_object(state, path, headers).await.into_response();
    }

    // Check for ?tagging (PutObjectTagging)
    if query.tagging.is_some() {
        let tagging_query =
            RequestQuery { version_id: query.version_id.clone(), ..Default::default() };
        return object::put_object_tagging(state, path, Query(tagging_query), body)
            .await
            .into_response();
    }

    // Check for ?partNumber&uploadId (upload part)
    if query.part_number.is_some() && query.upload_id.is_some() {
        let mp_query = multipart::MultipartQuery {
            upload_id: query.upload_id,
            part_number: query.part_number,
        };
        return multipart::upload_part(state, path, Query(mp_query), headers, body)
            .await
            .into_response();
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

    // Check for ?tagging (DeleteObjectTagging)
    if query.tagging.is_some() {
        let tagging_query =
            RequestQuery { version_id: query.version_id.clone(), ..Default::default() };
        return object::delete_object_tagging(state, path, Query(tagging_query))
            .await
            .into_response();
    }

    // Default: DeleteObject
    object::delete_object(state, path, Query(query)).await.into_response()
}

/// Handle POST requests to object (initiate or complete multipart).
async fn handle_object_post(
    state: State<AppState>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    // Check for ?uploads (initiate multipart upload)
    if query.uploads.is_some() {
        return multipart::create_multipart_upload(state, path, headers).await.into_response();
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
