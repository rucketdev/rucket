//! Bucket operation handlers.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use rucket_core::config::ApiCompatibilityMode;
use rucket_core::types::{CorsConfiguration, CorsRule, Tag, TagSet, VersioningStatus};
use rucket_storage::{LocalStorage, StorageBackend};

use crate::error::ApiError;
use crate::xml::request::{CorsConfigurationRequest, Tagging, VersioningConfiguration};
use crate::xml::response::{
    to_xml, BucketEntry, Buckets, CorsConfigurationResponse, ListBucketsResponse, Owner,
    TagResponse, TagSetResponse, TaggingResponse,
};

/// Validates an S3 bucket name according to S3 naming rules.
/// Returns an error message if invalid, None if valid.
fn validate_bucket_name(name: &str) -> Option<&'static str> {
    // Length check (3-63 characters)
    if name.len() < 3 {
        return Some("Bucket name must be at least 3 characters long");
    }
    if name.len() > 63 {
        return Some("Bucket name must be no more than 63 characters long");
    }

    // Must start with lowercase letter or number
    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_lowercase() && !first_char.is_ascii_digit() {
        return Some("Bucket name must start with a lowercase letter or number");
    }

    // Must end with lowercase letter or number
    let last_char = name.chars().next_back().unwrap();
    if !last_char.is_ascii_lowercase() && !last_char.is_ascii_digit() {
        return Some("Bucket name must end with a lowercase letter or number");
    }

    // Check for valid characters and patterns
    let mut prev_char = '\0';
    for c in name.chars() {
        if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' && c != '.' {
            return Some(
                "Bucket name can only contain lowercase letters, numbers, hyphens, and periods",
            );
        }

        // No consecutive periods
        if c == '.' && prev_char == '.' {
            return Some("Bucket name must not contain consecutive periods");
        }

        // No period followed by hyphen or hyphen followed by period
        if (c == '.' && prev_char == '-') || (c == '-' && prev_char == '.') {
            return Some("Bucket name must not contain period adjacent to hyphen");
        }

        prev_char = c;
    }

    // Must not be formatted as an IP address
    let parts: Vec<&str> = name.split('.').collect();
    if parts.len() == 4 && parts.iter().all(|p| p.parse::<u8>().is_ok()) {
        return Some("Bucket name must not be formatted as an IP address");
    }

    None
}

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    /// Storage backend.
    pub storage: Arc<LocalStorage>,
    /// API compatibility mode.
    pub compatibility_mode: ApiCompatibilityMode,
}

/// `PUT /{bucket}` - Create bucket.
pub async fn create_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    // Validate bucket name before creating
    if let Some(error_msg) = validate_bucket_name(&bucket) {
        return Err(ApiError::new(rucket_core::error::S3ErrorCode::InvalidBucketName, error_msg)
            .with_resource(&bucket));
    }

    // Check if bucket already exists - S3 returns 200 OK for idempotent creates by same owner
    if state.storage.head_bucket(&bucket).await? {
        // Bucket already exists and is "owned" by us (single-tenant)
        return Ok((StatusCode::OK, [("Location", format!("/{bucket}"))]));
    }

    state.storage.create_bucket(&bucket).await?;

    Ok((StatusCode::OK, [("Location", format!("/{bucket}"))]))
}

/// `DELETE /{bucket}` - Delete bucket.
pub async fn delete_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.storage.delete_bucket(&bucket).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `HEAD /{bucket}` - Check bucket exists.
pub async fn head_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    let exists = state.storage.head_bucket(&bucket).await?;

    if exists {
        Ok(StatusCode::OK)
    } else {
        Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket))
    }
}

/// `GET /` - List buckets.
pub async fn list_buckets(State(state): State<AppState>) -> Result<Response, ApiError> {
    let buckets = state.storage.list_buckets().await?;

    let response = ListBucketsResponse {
        owner: Owner::default(),
        buckets: Buckets { bucket: buckets.iter().map(BucketEntry::from).collect() },
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::InternalError,
            format!("Failed to serialize response: {e}"),
        )
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

// ============================================================================
// Bucket Configuration Handlers (Stub implementations)
// ============================================================================

/// `PUT /{bucket}?versioning` - Set bucket versioning.
pub async fn set_bucket_versioning(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    // Check bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Parse the versioning configuration XML
    let config: VersioningConfiguration =
        quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
            ApiError::new(
                rucket_core::error::S3ErrorCode::MalformedXML,
                format!("Invalid versioning configuration XML: {e}"),
            )
        })?;

    // Set versioning status if provided
    if let Some(status_str) = config.status {
        let status = VersioningStatus::parse(&status_str).ok_or_else(|| {
            ApiError::new(
                rucket_core::error::S3ErrorCode::MalformedXML,
                format!(
                    "Invalid versioning status: {status_str}. Must be 'Enabled' or 'Suspended'"
                ),
            )
        })?;

        state.storage.set_bucket_versioning(&bucket, status).await?;
    }

    Ok(StatusCode::OK)
}

/// `GET /{bucket}?versioning` - Get bucket versioning.
pub async fn get_bucket_versioning(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    // Get bucket info (includes versioning status)
    let bucket_info = state.storage.get_bucket(&bucket).await?;

    // Build the XML response
    let xml = match bucket_info.versioning_status {
        Some(status) => {
            format!(
                r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Status>{}</Status>
</VersioningConfiguration>"#,
                status.as_str()
            )
        }
        None => {
            // Versioning has never been enabled
            r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#
                .to_string()
        }
    };

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `PUT /{bucket}?policy` - Set bucket policy.
pub async fn set_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    _body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /{bucket}?policy` - Get bucket policy.
pub async fn get_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Err(ApiError::new(
        rucket_core::error::S3ErrorCode::NoSuchBucketPolicy,
        "The bucket policy does not exist",
    )
    .with_resource(&bucket))
}

/// `DELETE /{bucket}?policy` - Delete bucket policy.
pub async fn delete_bucket_policy(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /{bucket}?cors` - Set bucket CORS configuration.
pub async fn set_bucket_cors(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Parse the CORS configuration XML
    let config_req: CorsConfigurationRequest =
        quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
            ApiError::new(
                rucket_core::error::S3ErrorCode::MalformedXML,
                format!("Invalid CORS configuration XML: {e}"),
            )
        })?;

    // Convert request type to domain type
    let config = CorsConfiguration {
        rules: config_req
            .rules
            .into_iter()
            .map(|r| CorsRule {
                id: r.id,
                allowed_origins: r.allowed_origins,
                allowed_methods: r.allowed_methods,
                allowed_headers: r.allowed_headers,
                expose_headers: r.expose_headers,
                max_age_seconds: r.max_age_seconds,
            })
            .collect(),
    };

    state.storage.put_bucket_cors(&bucket, config).await?;

    Ok(StatusCode::OK)
}

/// `GET /{bucket}?cors` - Get bucket CORS configuration.
pub async fn get_bucket_cors(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    let config = state.storage.get_bucket_cors(&bucket).await?;

    match config {
        Some(cors) => {
            let response = CorsConfigurationResponse::from(&cors);
            let xml = to_xml(&response).map_err(|e| {
                ApiError::new(
                    rucket_core::error::S3ErrorCode::InternalError,
                    format!("Failed to serialize response: {e}"),
                )
            })?;
            Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
        }
        None => Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchCORSConfiguration,
            "The CORS configuration does not exist",
        )
        .with_resource(&bucket)),
    }
}

/// `DELETE /{bucket}?cors` - Delete bucket CORS configuration.
pub async fn delete_bucket_cors(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.storage.delete_bucket_cors(&bucket).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /{bucket}?tagging` - Set bucket tagging.
pub async fn set_bucket_tagging(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    // Parse the tagging configuration XML
    let tagging: Tagging = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            format!("Invalid tagging configuration XML: {e}"),
        )
    })?;

    // Convert request type to domain type
    let tags = TagSet {
        tags: tagging
            .tag_set
            .tags
            .into_iter()
            .map(|t| Tag { key: t.key, value: t.value })
            .collect(),
    };

    state.storage.put_bucket_tagging(&bucket, tags).await?;

    Ok(StatusCode::NO_CONTENT)
}

/// `GET /{bucket}?tagging` - Get bucket tagging.
pub async fn get_bucket_tagging(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    let tags = state.storage.get_bucket_tagging(&bucket).await?;

    let response = TaggingResponse {
        tag_set: TagSetResponse {
            tags: tags
                .tags
                .into_iter()
                .map(|t| TagResponse { key: t.key, value: t.value })
                .collect(),
        },
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::InternalError,
            format!("Failed to serialize response: {e}"),
        )
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `DELETE /{bucket}?tagging` - Delete bucket tagging.
pub async fn delete_bucket_tagging(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    state.storage.delete_bucket_tagging(&bucket).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /{bucket}?lifecycle` - Set bucket lifecycle.
pub async fn set_bucket_lifecycle(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    _body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::OK)
}

/// `GET /{bucket}?lifecycle` - Get bucket lifecycle.
pub async fn get_bucket_lifecycle(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Err(ApiError::new(
        rucket_core::error::S3ErrorCode::NoSuchLifecycleConfiguration,
        "The lifecycle configuration does not exist",
    )
    .with_resource(&bucket))
}

/// `DELETE /{bucket}?lifecycle` - Delete bucket lifecycle.
pub async fn delete_bucket_lifecycle(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /{bucket}?object-lock` - Set object lock configuration.
pub async fn set_object_lock_configuration(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    _body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::OK)
}

/// `GET /{bucket}?object-lock` - Get object lock configuration.
pub async fn get_object_lock_configuration(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Err(ApiError::new(
        rucket_core::error::S3ErrorCode::ObjectLockConfigurationNotFoundError,
        "Object Lock configuration does not exist for this bucket",
    )
    .with_resource(&bucket))
}

/// `PUT /{bucket}?encryption` - Set bucket encryption.
pub async fn set_bucket_encryption(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    _body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::OK)
}

/// `GET /{bucket}?encryption` - Get bucket encryption.
pub async fn get_bucket_encryption(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Err(ApiError::new(
        rucket_core::error::S3ErrorCode::ServerSideEncryptionConfigurationNotFoundError,
        "The server side encryption configuration was not found",
    )
    .with_resource(&bucket))
}

/// `DELETE /{bucket}?encryption` - Delete bucket encryption.
pub async fn delete_bucket_encryption(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `PUT /{bucket}?notification` - Set bucket notification.
pub async fn set_bucket_notification(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    _body: Bytes,
) -> Result<impl IntoResponse, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    Ok(StatusCode::OK)
}

/// `GET /{bucket}?notification` - Get bucket notification.
pub async fn get_bucket_notification(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#;
    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}?location` - Get bucket location.
pub async fn get_bucket_location(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }
    // Return 'default' as the location constraint for Ceph compatibility.
    // For AWS S3, us-east-1 buckets would return an empty LocationConstraint.
    let xml = r#"<?xml version="1.0" encoding="UTF-8"?>
<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">default</LocationConstraint>"#;
    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `OPTIONS /{bucket}` or `OPTIONS /{bucket}/{key}` - CORS preflight request.
///
/// Handles preflight requests for cross-origin resource sharing.
pub async fn cors_preflight(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    // Get Origin header (required for CORS preflight)
    let origin = match headers.get("origin") {
        Some(o) => o.to_str().unwrap_or(""),
        None => {
            // No Origin header - just return empty 200 (not a CORS request)
            return Ok(StatusCode::OK.into_response());
        }
    };

    // Get the requested method and headers
    let request_method =
        headers.get("access-control-request-method").and_then(|v| v.to_str().ok()).unwrap_or("");

    let request_headers =
        headers.get("access-control-request-headers").and_then(|v| v.to_str().ok()).unwrap_or("");

    // Get CORS config for this bucket
    let cors_config = match state.storage.get_bucket_cors(&bucket).await {
        Ok(Some(config)) => config,
        Ok(None) => {
            // No CORS config - return 403
            return Err(ApiError::new(
                rucket_core::error::S3ErrorCode::AccessDenied,
                "CORSResponse: This CORS request is not allowed",
            ));
        }
        Err(e) => {
            // Bucket doesn't exist or error
            return Err(ApiError::from(e));
        }
    };

    // Find a matching CORS rule
    for rule in &cors_config.rules {
        // Check if origin matches
        let origin_matches = rule.allowed_origins.iter().any(|allowed| {
            allowed == "*" || allowed == origin || matches_wildcard(allowed, origin)
        });

        if !origin_matches {
            continue;
        }

        // Check if method matches
        let method_matches =
            rule.allowed_methods.iter().any(|m| m.eq_ignore_ascii_case(request_method) || m == "*");

        if !method_matches {
            continue;
        }

        // Check if all requested headers are allowed
        let headers_match = if request_headers.is_empty() {
            true
        } else {
            let requested: Vec<&str> = request_headers.split(',').map(|s| s.trim()).collect();
            requested.iter().all(|h| {
                rule.allowed_headers
                    .iter()
                    .any(|allowed| allowed == "*" || allowed.eq_ignore_ascii_case(h))
            })
        };

        if !headers_match {
            continue;
        }

        // Found a matching rule - build response
        let mut response_headers = vec![
            ("Access-Control-Allow-Origin", origin.to_string()),
            ("Access-Control-Allow-Methods", rule.allowed_methods.join(", ")),
            ("Vary", "Origin".to_string()),
        ];

        if !rule.allowed_headers.is_empty() {
            response_headers
                .push(("Access-Control-Allow-Headers", rule.allowed_headers.join(", ")));
        }

        if !rule.expose_headers.is_empty() {
            response_headers
                .push(("Access-Control-Expose-Headers", rule.expose_headers.join(", ")));
        }

        if let Some(max_age) = rule.max_age_seconds {
            response_headers.push(("Access-Control-Max-Age", max_age.to_string()));
        }

        // Build the response with headers
        let mut builder = axum::http::Response::builder().status(StatusCode::OK);
        for (key, value) in response_headers {
            builder = builder.header(key, value);
        }
        return Ok(builder.body(axum::body::Body::empty()).unwrap().into_response());
    }

    // No matching rule found
    Err(ApiError::new(
        rucket_core::error::S3ErrorCode::AccessDenied,
        "CORSResponse: This CORS request is not allowed",
    ))
}

/// `OPTIONS /{bucket}/{key}` - CORS preflight request for object operations.
///
/// Handles preflight requests for cross-origin resource sharing on objects.
pub async fn cors_preflight_object(
    State(state): State<AppState>,
    Path((bucket, _key)): Path<(String, String)>,
    headers: axum::http::HeaderMap,
) -> Result<Response, ApiError> {
    // Delegate to bucket-level handler (CORS config is per-bucket)
    cors_preflight(State(state), Path(bucket), headers).await
}

/// Check if a wildcard pattern matches a value.
/// Supports single `*` anywhere in the pattern.
fn matches_wildcard(pattern: &str, value: &str) -> bool {
    if let Some(pos) = pattern.find('*') {
        let prefix = &pattern[..pos];
        let suffix = &pattern[pos + 1..];
        value.starts_with(prefix) && value.ends_with(suffix) && value.len() >= pattern.len() - 1
    } else {
        pattern == value
    }
}
