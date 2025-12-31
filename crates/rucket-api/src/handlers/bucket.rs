//! Bucket operation handlers.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
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
