// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! S3 bucket replication configuration handlers.
//!
//! Implements the following S3 API operations:
//! - `GET /{bucket}?replication` - GetBucketReplicationConfiguration
//! - `PUT /{bucket}?replication` - PutBucketReplicationConfiguration
//! - `DELETE /{bucket}?replication` - DeleteBucketReplicationConfiguration

use axum::extract::{Path, State};
use axum::http::StatusCode;
use bytes::Bytes;
use rucket_core::types::VersioningStatus;
use rucket_storage::StorageBackend;

use super::bucket::AppState;
use crate::error::ApiError;

/// `GET /{bucket}?replication` - Get bucket replication configuration.
///
/// Returns the replication configuration for the specified bucket.
/// Returns 404 ReplicationConfigurationNotFoundError if not configured.
pub async fn get_bucket_replication(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<StatusCode, ApiError> {
    // Verify bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // TODO: Implement replication configuration retrieval
    // For now, return that replication is not configured
    Err(ApiError::new(
        rucket_core::error::S3ErrorCode::ReplicationConfigurationNotFoundError,
        "The replication configuration was not found",
    )
    .with_resource(&bucket))
}

/// `PUT /{bucket}?replication` - Set bucket replication configuration.
///
/// Sets the replication configuration for the specified bucket.
/// Requires versioning to be enabled on the bucket.
pub async fn put_bucket_replication(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    // Get bucket info (also verifies bucket exists)
    let bucket_info = state.storage.get_bucket(&bucket).await.map_err(|_| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket)
    })?;

    // Verify versioning is enabled (required for replication)
    let versioning = bucket_info.versioning_status.unwrap_or(VersioningStatus::Suspended);
    if versioning != VersioningStatus::Enabled {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::InvalidBucketState,
            "Versioning must be enabled on the bucket to configure replication",
        )
        .with_resource(&bucket));
    }

    // TODO: Parse the XML body and store the replication configuration
    // For now, just validate the body is not empty
    if body.is_empty() {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            "Request body is empty",
        ));
    }

    // Return success (placeholder)
    Ok(StatusCode::OK)
}

/// `DELETE /{bucket}?replication` - Delete bucket replication configuration.
///
/// Removes the replication configuration from the specified bucket.
pub async fn delete_bucket_replication(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<StatusCode, ApiError> {
    // Verify bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // TODO: Delete replication configuration from storage
    // For now, return success (idempotent delete)
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::extract::{Path, State};
    use bytes::Bytes;
    use rucket_core::config::ApiCompatibilityMode;
    use rucket_storage::{LocalStorage, StorageBackend};
    use tempfile::TempDir;

    use super::*;

    async fn create_test_state() -> (AppState, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let temp_storage_dir = temp_dir.path().join("temp");
        let storage = LocalStorage::new(data_dir, temp_storage_dir).await.unwrap();
        let state = AppState {
            storage: Arc::new(storage),
            compatibility_mode: ApiCompatibilityMode::S3Strict,
        };
        (state, temp_dir)
    }

    #[tokio::test]
    async fn test_get_replication_no_bucket() {
        let (state, _temp_dir) = create_test_state().await;

        let result = get_bucket_replication(State(state), Path("nonexistent".to_string())).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::NoSuchBucket);
    }

    #[tokio::test]
    async fn test_get_replication_not_configured() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = get_bucket_replication(State(state), Path("test-bucket".to_string())).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(
            err.code,
            rucket_core::error::S3ErrorCode::ReplicationConfigurationNotFoundError
        );
    }

    #[tokio::test]
    async fn test_put_replication_no_versioning() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket without versioning
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = put_bucket_replication(
            State(state),
            Path("test-bucket".to_string()),
            Bytes::from("<ReplicationConfiguration></ReplicationConfiguration>"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::InvalidBucketState);
    }

    #[tokio::test]
    async fn test_put_replication_with_versioning() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket with versioning enabled
        state.storage.create_bucket("test-bucket").await.unwrap();
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        let result = put_bucket_replication(
            State(state),
            Path("test-bucket".to_string()),
            Bytes::from("<ReplicationConfiguration></ReplicationConfiguration>"),
        )
        .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_replication() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = delete_bucket_replication(State(state), Path("test-bucket".to_string())).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
    }
}
