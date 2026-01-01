//! Bucket operation handlers.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use rucket_core::config::ApiCompatibilityMode;
use rucket_core::types::VersioningStatus;
use rucket_storage::{LocalStorage, StorageBackend};

use crate::error::ApiError;
use crate::xml::request::VersioningConfiguration;
use crate::xml::response::{to_xml, BucketEntry, Buckets, ListBucketsResponse, Owner};

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
            return Some("Bucket name can only contain lowercase letters, numbers, hyphens, and periods");
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
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::InvalidBucketName,
            error_msg,
        )
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
    let config: VersioningConfiguration = quick_xml::de::from_reader(body.as_ref())
        .map_err(|e| {
            ApiError::new(
                rucket_core::error::S3ErrorCode::MalformedXML,
                format!("Invalid versioning configuration XML: {e}"),
            )
        })?;

    // Set versioning status if provided
    if let Some(status_str) = config.status {
        let status = VersioningStatus::from_str(&status_str).ok_or_else(|| {
            ApiError::new(
                rucket_core::error::S3ErrorCode::MalformedXML,
                format!("Invalid versioning status: {status_str}. Must be 'Enabled' or 'Suspended'"),
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

/// `PUT /{bucket}?tagging` - Set bucket tagging.
pub async fn set_bucket_tagging(
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

/// `GET /{bucket}?tagging` - Get bucket tagging.
pub async fn get_bucket_tagging(
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
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet/></Tagging>"#;
    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `DELETE /{bucket}?tagging` - Delete bucket tagging.
pub async fn delete_bucket_tagging(
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
