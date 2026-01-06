//! S3 API router configuration.

use std::sync::Arc;

use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{middleware as axum_middleware, Extension, Router};
use bytes::Bytes;
use rucket_core::config::{ApiCompatibilityMode, AuthConfig};
use rucket_storage::LocalStorage;
use serde::Deserialize;
use tower_http::trace::{DefaultMakeSpan, DefaultOnResponse, TraceLayer};
use tracing::Level;

use crate::auth::{auth_middleware, AuthContext, AuthState};
use crate::handlers::bucket::{self, AppState};
use crate::handlers::{logging, minio, multipart, object, replication, website};
use crate::middleware::metrics_layer;

/// Query parameters to determine request type.
#[derive(Debug, Deserialize, Default)]
pub struct RequestQuery {
    /// List type (for ListObjectsV2).
    #[serde(rename = "list-type")]
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
    /// Object retention configuration.
    retention: Option<String>,
    /// Object legal hold status.
    #[serde(rename = "legal-hold")]
    legal_hold: Option<String>,
    /// Bucket encryption.
    encryption: Option<String>,
    /// Bucket notification.
    notification: Option<String>,
    /// Public access block configuration.
    #[serde(rename = "publicAccessBlock")]
    public_access_block: Option<String>,
    /// Access control list.
    acl: Option<String>,
    /// Accelerate configuration (not implemented).
    accelerate: Option<String>,
    /// Request payment configuration (not implemented).
    #[serde(rename = "requestPayment")]
    request_payment: Option<String>,
    /// Bucket replication configuration.
    replication: Option<String>,
    /// Bucket website configuration.
    website: Option<String>,
    /// Bucket logging configuration.
    logging: Option<String>,
    /// List object versions.
    versions: Option<String>,
    /// Bucket location.
    location: Option<String>,
    /// GetObjectAttributes request.
    attributes: Option<String>,
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
    /// Max uploads for ListMultipartUploads.
    #[serde(rename = "max-uploads")]
    max_uploads: Option<u32>,
    /// Upload ID marker for ListMultipartUploads.
    #[serde(rename = "upload-id-marker")]
    upload_id_marker: Option<String>,
    /// Max parts for ListParts.
    #[serde(rename = "max-parts")]
    max_parts: Option<u32>,
    /// Part number marker for ListParts.
    #[serde(rename = "part-number-marker")]
    part_number_marker: Option<u32>,
}

/// Create the S3 API router.
///
/// # Arguments
/// * `storage` - The storage backend
/// * `max_body_size` - Maximum request body size in bytes (0 for unlimited)
/// * `compatibility_mode` - API compatibility mode (s3-strict or minio)
/// * `log_requests` - Whether to log HTTP requests
/// * `auth_config` - Optional authentication config (None for anonymous access)
pub fn create_router(
    storage: Arc<LocalStorage>,
    max_body_size: u64,
    compatibility_mode: ApiCompatibilityMode,
    log_requests: bool,
    auth_config: Option<&AuthConfig>,
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
                .post(handle_bucket_post)
                .options(cors_preflight_bucket),
        )
        .route(
            "/{bucket}/",
            get(handle_bucket_get)
                .put(handle_bucket_put)
                .delete(handle_bucket_delete)
                .head(head_bucket)
                .post(handle_bucket_post)
                .options(cors_preflight_bucket),
        )
        // Object operations with complex routing
        .route(
            "/{bucket}/{*key}",
            get(handle_object_get)
                .put(handle_object_put)
                .delete(handle_object_delete)
                .head(head_object)
                .post(handle_object_post)
                .options(cors_preflight_object),
        );

    // Add MinIO-specific routes if compatibility mode is Minio
    if compatibility_mode == ApiCompatibilityMode::Minio {
        router = router
            .route("/minio/health/live", get(minio::health_live))
            .route("/minio/health/ready", get(minio::health_ready));
    }

    let router = router.with_state(state);

    // Add auth middleware if config provided
    let router = if let Some(auth) = auth_config {
        let auth_state = AuthState::new(auth);
        router.layer(axum_middleware::from_fn_with_state(auth_state, auth_middleware))
    } else {
        router
    };

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
    headers: axum::http::HeaderMap,
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
    // Check for ?publicAccessBlock (PutPublicAccessBlock)
    if query.public_access_block.is_some() {
        return bucket::put_public_access_block(state, path, body).await.into_response();
    }
    // Check for ?acl (PutBucketAcl)
    if query.acl.is_some() {
        return bucket::put_bucket_acl(state, path, body).await.into_response();
    }
    // Check for ?accelerate (PutBucketAccelerateConfiguration - not implemented)
    if query.accelerate.is_some() {
        return bucket::put_bucket_accelerate(state, path).await.into_response();
    }
    // Check for ?requestPayment (PutBucketRequestPayment - not implemented)
    if query.request_payment.is_some() {
        return bucket::put_bucket_request_payment(state, path).await.into_response();
    }
    // Check for ?replication (PutBucketReplication)
    if query.replication.is_some() {
        return replication::put_bucket_replication(state, path, body).await.into_response();
    }
    // Check for ?website (PutBucketWebsite)
    if query.website.is_some() {
        return website::put_bucket_website(state, path, body).await.into_response();
    }
    // Check for ?logging (PutBucketLogging)
    if query.logging.is_some() {
        return logging::put_bucket_logging(state, path, body).await.into_response();
    }
    // Default: CreateBucket
    bucket::create_bucket(state, path, headers).await.into_response()
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
    // Check for ?publicAccessBlock (DeletePublicAccessBlock)
    if query.public_access_block.is_some() {
        return bucket::delete_public_access_block(state, path).await.into_response();
    }
    // Check for ?replication (DeleteBucketReplication)
    if query.replication.is_some() {
        return replication::delete_bucket_replication(state, path).await.into_response();
    }
    // Check for ?website (DeleteBucketWebsite)
    if query.website.is_some() {
        return website::delete_bucket_website(state, path).await.into_response();
    }
    // Default: DeleteBucket
    bucket::delete_bucket(state, path).await.into_response()
}

async fn head_bucket(state: State<AppState>, path: Path<String>) -> Response {
    bucket::head_bucket(state, path).await.into_response()
}

async fn head_object(
    state: State<AppState>,
    auth: Option<Extension<AuthContext>>,
    path: Path<(String, String)>,
    headers: HeaderMap,
    query: Query<RequestQuery>,
) -> Response {
    object::head_object(state, auth, path, headers, query).await.into_response()
}

/// Handle POST requests to bucket (delete multiple objects).
async fn handle_bucket_post(
    state: State<AppState>,
    auth: Option<Extension<AuthContext>>,
    path: Path<String>,
    Query(query): Query<RequestQuery>,
    body: Bytes,
) -> Response {
    // Check for ?delete (DeleteObjects)
    if query.delete.is_some() {
        return object::delete_objects(state, auth, path, body).await.into_response();
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
    auth: Option<Extension<AuthContext>>,
    path: Path<String>,
    Query(query): Query<RequestQuery>,
) -> Response {
    // Check for ?uploads (list multipart uploads)
    if query.uploads.is_some() {
        let list_query = multipart::ListUploadsQuery {
            prefix: query.prefix.clone(),
            delimiter: query.delimiter.clone(),
            max_uploads: query.max_uploads,
            key_marker: query.key_marker.clone(),
            upload_id_marker: query.upload_id_marker.clone(),
        };
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
    // Check for ?publicAccessBlock (GetPublicAccessBlock)
    if query.public_access_block.is_some() {
        return bucket::get_public_access_block(state, path).await.into_response();
    }
    // Check for ?acl (GetBucketAcl)
    if query.acl.is_some() {
        return bucket::get_bucket_acl(state, path).await.into_response();
    }
    // Check for ?accelerate (GetBucketAccelerateConfiguration - not implemented)
    if query.accelerate.is_some() {
        return bucket::get_bucket_accelerate(state, path).await.into_response();
    }
    // Check for ?requestPayment (GetBucketRequestPayment - not implemented)
    if query.request_payment.is_some() {
        return bucket::get_bucket_request_payment(state, path).await.into_response();
    }
    // Check for ?replication (GetBucketReplication)
    if query.replication.is_some() {
        return replication::get_bucket_replication(state, path).await.into_response();
    }
    // Check for ?website (GetBucketWebsite)
    if query.website.is_some() {
        return website::get_bucket_website(state, path).await.into_response();
    }
    // Check for ?logging (GetBucketLogging)
    if query.logging.is_some() {
        return logging::get_bucket_logging(state, path).await.into_response();
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
        return object::list_object_versions(state, auth, path, Query(list_query))
            .await
            .into_response();
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
        object::list_objects_v2(state, auth.clone(), path, Query(list_query)).await.into_response()
    } else {
        object::list_objects_v1(state, auth, path, Query(list_query)).await.into_response()
    }
}

/// Handle GET requests to object (get object or list parts).
async fn handle_object_get(
    state: State<AppState>,
    auth: Option<Extension<AuthContext>>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
    headers: HeaderMap,
) -> Response {
    // Check for ?uploadId (list parts)
    if query.upload_id.is_some() {
        let parts_query = multipart::ListPartsQuery {
            upload_id: query.upload_id,
            max_parts: query.max_parts,
            part_number_marker: query.part_number_marker,
        };
        return multipart::list_parts(state, path, Query(parts_query)).await.into_response();
    }

    // Check for ?tagging (GetObjectTagging)
    if query.tagging.is_some() {
        let tagging_query =
            RequestQuery { version_id: query.version_id.clone(), ..Default::default() };
        return object::get_object_tagging(state, auth, path, Query(tagging_query))
            .await
            .into_response();
    }

    // Check for ?attributes (GetObjectAttributes)
    if query.attributes.is_some() {
        return object::get_object_attributes(state, auth, path, Query(query), headers)
            .await
            .into_response();
    }

    // Check for ?retention (GetObjectRetention)
    if query.retention.is_some() {
        return object::get_object_retention(state, auth, path, Query(query)).await.into_response();
    }

    // Check for ?legal-hold (GetObjectLegalHold)
    if query.legal_hold.is_some() {
        return object::get_object_legal_hold(state, auth, path, Query(query))
            .await
            .into_response();
    }

    // Check for ?acl (GetObjectAcl)
    if query.acl.is_some() {
        return object::get_object_acl(state, auth, path, Query(query)).await.into_response();
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
    object::get_object(state, auth, path, headers, overrides, query.version_id)
        .await
        .into_response()
}

/// Handle PUT requests to object (put object, upload part, or copy).
async fn handle_object_put(
    state: State<AppState>,
    auth: Option<Extension<AuthContext>>,
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
        return object::copy_object(state, auth, path, headers).await.into_response();
    }

    // Check for ?tagging (PutObjectTagging)
    if query.tagging.is_some() {
        let tagging_query =
            RequestQuery { version_id: query.version_id.clone(), ..Default::default() };
        return object::put_object_tagging(state, auth, path, Query(tagging_query), body)
            .await
            .into_response();
    }

    // Check for ?retention (PutObjectRetention)
    if query.retention.is_some() {
        return object::put_object_retention(state, auth, path, Query(query), headers, body)
            .await
            .into_response();
    }

    // Check for ?legal-hold (PutObjectLegalHold)
    if query.legal_hold.is_some() {
        return object::put_object_legal_hold(state, auth, path, Query(query), body)
            .await
            .into_response();
    }

    // Check for ?acl (PutObjectAcl)
    if query.acl.is_some() {
        return object::put_object_acl(state, auth, path, Query(query), body).await.into_response();
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
    object::put_object(state, auth, path, headers, body).await.into_response()
}

/// Handle DELETE requests to object (delete object or abort upload).
async fn handle_object_delete(
    state: State<AppState>,
    auth: Option<Extension<AuthContext>>,
    path: Path<(String, String)>,
    Query(query): Query<RequestQuery>,
    headers: HeaderMap,
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
        return object::delete_object_tagging(state, auth, path, Query(tagging_query))
            .await
            .into_response();
    }

    // Default: DeleteObject
    object::delete_object(state, auth, path, headers, Query(query)).await.into_response()
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

/// Handle OPTIONS requests to bucket (CORS preflight).
async fn cors_preflight_bucket(
    state: State<AppState>,
    path: Path<String>,
    headers: HeaderMap,
) -> Response {
    bucket::cors_preflight(state, path, headers).await.into_response()
}

/// Handle OPTIONS requests to object (CORS preflight).
async fn cors_preflight_object(
    state: State<AppState>,
    path: Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    bucket::cors_preflight_object(state, path, headers).await.into_response()
}

/// Create the cluster admin API router.
///
/// This router provides endpoints for cluster management operations:
/// - `GET /_cluster/status` - Get cluster status
/// - `GET /_cluster/nodes` - List all nodes
/// - `POST /_cluster/nodes` - Add a node
/// - `DELETE /_cluster/nodes/{node_id}` - Remove a node
/// - `GET /_cluster/rebalance` - Get rebalance status
/// - `POST /_cluster/rebalance` - Trigger rebalance
///
/// # Arguments
/// * `admin_state` - Admin state containing the Raft instance and rebalance manager
pub fn create_admin_router(admin_state: crate::handlers::admin::AdminState) -> Router {
    use crate::handlers::admin;

    Router::new()
        .route("/_cluster/status", get(admin::get_cluster_status))
        .route("/_cluster/nodes", get(admin::list_nodes).post(admin::add_node))
        .route("/_cluster/nodes/{node_id}", axum::routing::delete(admin::remove_node))
        .route(
            "/_cluster/rebalance",
            get(admin::get_rebalance_status).post(admin::trigger_rebalance),
        )
        .with_state(admin_state)
}
