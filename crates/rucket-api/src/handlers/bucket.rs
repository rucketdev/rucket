//! Bucket operation handlers.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use rucket_storage::{LocalStorage, StorageBackend};

use crate::error::ApiError;
use crate::xml::response::{to_xml, BucketEntry, Buckets, ListBucketsResponse, Owner};

/// Application state shared across handlers.
#[derive(Clone)]
pub struct AppState {
    /// Storage backend.
    pub storage: Arc<LocalStorage>,
}

/// `PUT /{bucket}` - Create bucket.
pub async fn create_bucket(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<impl IntoResponse, ApiError> {
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

/// `GET /{bucket}?versioning` - Get bucket versioning.
pub async fn get_bucket_versioning(
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
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"/>"#;
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
