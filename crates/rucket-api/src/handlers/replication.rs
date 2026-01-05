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
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use quick_xml::de::from_str;
use rucket_core::replication::{
    AccessControlTranslation, DeleteMarkerReplication, EncryptionConfiguration,
    ExistingObjectReplication, ReplicaModifications, ReplicationConfiguration,
    ReplicationDestination, ReplicationFilter, ReplicationFilterAnd, ReplicationMetrics,
    ReplicationRule, ReplicationTag, ReplicationTime, ReplicationTimeValue, RuleStatus,
    SourceSelectionCriteria, SseKmsEncryptedObjects,
};
use rucket_core::types::VersioningStatus;
use rucket_storage::StorageBackend;

use super::bucket::AppState;
use crate::error::ApiError;
use crate::xml::request::ReplicationConfigurationRequest;
use crate::xml::response::{
    to_xml, AccessControlTranslationResponse, DeleteMarkerReplicationResponse,
    ExistingObjectReplicationResponse, ReplicaModificationsResponse,
    ReplicationConfigurationResponse, ReplicationDestinationResponse,
    ReplicationEncryptionConfigurationResponse, ReplicationFilterAndResponse,
    ReplicationFilterResponse, ReplicationMetricsResponse, ReplicationRuleResponse,
    ReplicationTagResponse, ReplicationTimeResponse, ReplicationTimeValueResponse,
    SourceSelectionCriteriaResponse, SseKmsEncryptedObjectsResponse,
};

/// Convert a request to the core type.
fn convert_request_to_config(
    request: ReplicationConfigurationRequest,
) -> Result<ReplicationConfiguration, ApiError> {
    let rules = request
        .rules
        .into_iter()
        .map(|r| {
            let status = RuleStatus::parse(&r.status).ok_or_else(|| {
                ApiError::new(
                    rucket_core::error::S3ErrorCode::MalformedXML,
                    format!("Invalid rule status: {}", r.status),
                )
            })?;

            let filter = r.filter.map(|f| ReplicationFilter {
                prefix: f.prefix,
                tag: f.tag.map(|t| ReplicationTag { key: t.key, value: t.value }),
                and: f.and.map(|a| ReplicationFilterAnd {
                    prefix: a.prefix,
                    tags: a
                        .tags
                        .into_iter()
                        .map(|t| ReplicationTag { key: t.key, value: t.value })
                        .collect(),
                }),
            });

            let destination = ReplicationDestination {
                bucket: r.destination.bucket,
                account: r.destination.account,
                storage_class: r.destination.storage_class,
                access_control_translation: r
                    .destination
                    .access_control_translation
                    .map(|a| AccessControlTranslation { owner: a.owner }),
                encryption_configuration: r
                    .destination
                    .encryption_configuration
                    .map(|e| EncryptionConfiguration { replica_kms_key_id: e.replica_kms_key_id }),
                replication_time: r.destination.replication_time.map(|rt| ReplicationTime {
                    status: RuleStatus::parse(&rt.status).unwrap_or(RuleStatus::Disabled),
                    time: rt.time.map(|t| ReplicationTimeValue { minutes: t.minutes }),
                }),
                metrics: r.destination.metrics.map(|m| ReplicationMetrics {
                    status: RuleStatus::parse(&m.status).unwrap_or(RuleStatus::Disabled),
                    event_threshold: m
                        .event_threshold
                        .map(|t| ReplicationTimeValue { minutes: t.minutes }),
                }),
            };

            let delete_marker_replication =
                r.delete_marker_replication.map(|d| DeleteMarkerReplication {
                    status: RuleStatus::parse(&d.status).unwrap_or(RuleStatus::Disabled),
                });

            let source_selection_criteria =
                r.source_selection_criteria.map(|s| SourceSelectionCriteria {
                    sse_kms_encrypted_objects: s.sse_kms_encrypted_objects.map(|sse| {
                        SseKmsEncryptedObjects {
                            status: RuleStatus::parse(&sse.status).unwrap_or(RuleStatus::Disabled),
                        }
                    }),
                    replica_modifications: s.replica_modifications.map(|rm| ReplicaModifications {
                        status: RuleStatus::parse(&rm.status).unwrap_or(RuleStatus::Disabled),
                    }),
                });

            let existing_object_replication =
                r.existing_object_replication.map(|e| ExistingObjectReplication {
                    status: RuleStatus::parse(&e.status).unwrap_or(RuleStatus::Disabled),
                });

            Ok(ReplicationRule {
                id: r.id,
                status,
                priority: r.priority,
                filter,
                destination,
                delete_marker_replication,
                source_selection_criteria,
                existing_object_replication,
            })
        })
        .collect::<Result<Vec<_>, ApiError>>()?;

    Ok(ReplicationConfiguration { role: request.role, rules })
}

/// Convert the core type to a response.
fn convert_config_to_response(
    config: ReplicationConfiguration,
) -> ReplicationConfigurationResponse {
    ReplicationConfigurationResponse {
        role: config.role,
        rules: config
            .rules
            .into_iter()
            .map(|r| ReplicationRuleResponse {
                id: r.id,
                priority: r.priority,
                status: r.status.as_str().to_string(),
                filter: r.filter.map(|f| ReplicationFilterResponse {
                    prefix: f.prefix,
                    tag: f.tag.map(|t| ReplicationTagResponse { key: t.key, value: t.value }),
                    and: f.and.map(|a| ReplicationFilterAndResponse {
                        prefix: a.prefix,
                        tags: a
                            .tags
                            .into_iter()
                            .map(|t| ReplicationTagResponse { key: t.key, value: t.value })
                            .collect(),
                    }),
                }),
                destination: ReplicationDestinationResponse {
                    bucket: r.destination.bucket,
                    account: r.destination.account,
                    storage_class: r.destination.storage_class,
                    access_control_translation: r
                        .destination
                        .access_control_translation
                        .map(|a| AccessControlTranslationResponse { owner: a.owner }),
                    encryption_configuration: r.destination.encryption_configuration.map(|e| {
                        ReplicationEncryptionConfigurationResponse {
                            replica_kms_key_id: e.replica_kms_key_id,
                        }
                    }),
                    replication_time: r.destination.replication_time.map(|rt| {
                        ReplicationTimeResponse {
                            status: rt.status.as_str().to_string(),
                            time: rt
                                .time
                                .map(|t| ReplicationTimeValueResponse { minutes: t.minutes }),
                        }
                    }),
                    metrics: r.destination.metrics.map(|m| ReplicationMetricsResponse {
                        status: m.status.as_str().to_string(),
                        event_threshold: m
                            .event_threshold
                            .map(|t| ReplicationTimeValueResponse { minutes: t.minutes }),
                    }),
                },
                delete_marker_replication: r.delete_marker_replication.map(|d| {
                    DeleteMarkerReplicationResponse { status: d.status.as_str().to_string() }
                }),
                source_selection_criteria: r.source_selection_criteria.map(|s| {
                    SourceSelectionCriteriaResponse {
                        sse_kms_encrypted_objects: s.sse_kms_encrypted_objects.map(|sse| {
                            SseKmsEncryptedObjectsResponse {
                                status: sse.status.as_str().to_string(),
                            }
                        }),
                        replica_modifications: s.replica_modifications.map(|rm| {
                            ReplicaModificationsResponse { status: rm.status.as_str().to_string() }
                        }),
                    }
                }),
                existing_object_replication: r.existing_object_replication.map(|e| {
                    ExistingObjectReplicationResponse { status: e.status.as_str().to_string() }
                }),
            })
            .collect(),
    }
}

/// `GET /{bucket}?replication` - Get bucket replication configuration.
///
/// Returns the replication configuration for the specified bucket.
/// Returns 404 ReplicationConfigurationNotFoundError if not configured.
pub async fn get_bucket_replication(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
) -> Result<Response, ApiError> {
    // Verify bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    match state.storage.get_replication_configuration(&bucket).await? {
        Some(config) => {
            let response = convert_config_to_response(config);
            let xml = to_xml(&response).map_err(|e| {
                ApiError::new(
                    rucket_core::error::S3ErrorCode::InternalError,
                    format!("Failed to serialize response: {e}"),
                )
            })?;
            Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
        }
        None => Err(ApiError::new(
            rucket_core::error::S3ErrorCode::ReplicationConfigurationNotFoundError,
            "The replication configuration was not found",
        )
        .with_resource(&bucket)),
    }
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

    // Parse the XML body
    if body.is_empty() {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            "Request body is empty",
        ));
    }

    let body_str = std::str::from_utf8(&body).map_err(|_| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            "Invalid UTF-8 in request body",
        )
    })?;

    let request: ReplicationConfigurationRequest = from_str(body_str).map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            format!("Failed to parse replication configuration: {e}"),
        )
    })?;

    // Convert to core type
    let config = convert_request_to_config(request)?;

    // Validate configuration
    config.validate().map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            format!("Invalid replication configuration: {e}"),
        )
    })?;

    // Store the configuration
    state.storage.put_replication_configuration(&bucket, config).await?;

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

    // Delete the configuration (idempotent)
    state.storage.delete_replication_configuration(&bucket).await?;

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

        let xml = r#"
            <ReplicationConfiguration>
                <Role>arn:aws:iam::123456789:role/replication</Role>
                <Rule>
                    <ID>rule1</ID>
                    <Status>Enabled</Status>
                    <Destination>
                        <Bucket>dest-bucket</Bucket>
                    </Destination>
                </Rule>
            </ReplicationConfiguration>
        "#;

        let result =
            put_bucket_replication(State(state), Path("test-bucket".to_string()), Bytes::from(xml))
                .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::InvalidBucketState);
    }

    #[tokio::test]
    async fn test_put_and_get_replication_with_versioning() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket with versioning enabled
        state.storage.create_bucket("test-bucket").await.unwrap();
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        let xml = r#"
            <ReplicationConfiguration>
                <Role>arn:aws:iam::123456789:role/replication</Role>
                <Rule>
                    <ID>rule1</ID>
                    <Status>Enabled</Status>
                    <Priority>1</Priority>
                    <Filter>
                        <Prefix>logs/</Prefix>
                    </Filter>
                    <Destination>
                        <Bucket>dest-bucket</Bucket>
                        <StorageClass>STANDARD_IA</StorageClass>
                    </Destination>
                    <DeleteMarkerReplication>
                        <Status>Enabled</Status>
                    </DeleteMarkerReplication>
                </Rule>
            </ReplicationConfiguration>
        "#;

        let put_result = put_bucket_replication(
            State(state.clone()),
            Path("test-bucket".to_string()),
            Bytes::from(xml),
        )
        .await;
        assert!(put_result.is_ok());

        // Get the configuration back
        let get_result =
            get_bucket_replication(State(state), Path("test-bucket".to_string())).await;
        assert!(get_result.is_ok());
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

    #[tokio::test]
    async fn test_put_replication_malformed_xml() {
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
            Bytes::from("<invalid>"),
        )
        .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::MalformedXML);
    }
}
