// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! S3 bucket logging configuration handlers.
//!
//! Implements the following S3 API operations:
//! - `GET /{bucket}?logging` - GetBucketLogging
//! - `PUT /{bucket}?logging` - PutBucketLogging

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use quick_xml::de::from_str;
use rucket_core::types::{
    BucketLoggingStatus, LoggingEnabled, LoggingGrant, LoggingGrantee, LoggingPermission,
};
use rucket_storage::StorageBackend;

use super::bucket::AppState;
use crate::error::ApiError;
use crate::xml::request::BucketLoggingStatusRequest;
use crate::xml::response::{
    to_xml, BucketLoggingStatusResponse, LoggingEnabledResponse, LoggingGrantResponse,
    LoggingGranteeResponse, TargetGrantsResponse,
};

/// Convert a request to the core type.
fn convert_request_to_config(
    request: BucketLoggingStatusRequest,
) -> Result<BucketLoggingStatus, ApiError> {
    let logging_enabled = match request.logging_enabled {
        Some(enabled) => {
            let target_grants = enabled
                .target_grants
                .map(|tg| {
                    tg.grants
                        .into_iter()
                        .filter_map(|g| {
                            let permission = LoggingPermission::parse(&g.permission)?;
                            let grantee = match g.grantee.grantee_type.as_deref() {
                                Some("CanonicalUser") => LoggingGrantee::CanonicalUser {
                                    id: g.grantee.id.unwrap_or_default(),
                                    display_name: g.grantee.display_name,
                                },
                                Some("AmazonCustomerByEmail") => {
                                    LoggingGrantee::AmazonCustomerByEmail {
                                        email_address: g.grantee.email_address.unwrap_or_default(),
                                    }
                                }
                                Some("Group") => {
                                    LoggingGrantee::Group { uri: g.grantee.uri.unwrap_or_default() }
                                }
                                _ => {
                                    // Default to CanonicalUser if no type specified but ID present
                                    if let Some(id) = g.grantee.id {
                                        LoggingGrantee::CanonicalUser { id, display_name: None }
                                    } else if let Some(uri) = g.grantee.uri {
                                        LoggingGrantee::Group { uri }
                                    } else {
                                        return None;
                                    }
                                }
                            };
                            Some(LoggingGrant { grantee, permission })
                        })
                        .collect()
                })
                .unwrap_or_default();

            Some(LoggingEnabled {
                target_bucket: enabled.target_bucket,
                target_prefix: enabled.target_prefix,
                target_grants,
            })
        }
        None => None,
    };

    Ok(BucketLoggingStatus { logging_enabled })
}

/// Convert the core type to a response.
fn convert_config_to_response(config: BucketLoggingStatus) -> BucketLoggingStatusResponse {
    BucketLoggingStatusResponse {
        xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
        logging_enabled: config.logging_enabled.map(|enabled| LoggingEnabledResponse {
            target_bucket: enabled.target_bucket,
            target_prefix: enabled.target_prefix,
            target_grants: if enabled.target_grants.is_empty() {
                None
            } else {
                Some(TargetGrantsResponse {
                    grants: enabled
                        .target_grants
                        .into_iter()
                        .map(|g| {
                            let (grantee_type, id, display_name, email_address, uri) =
                                match g.grantee {
                                    LoggingGrantee::CanonicalUser { id, display_name } => (
                                        Some("CanonicalUser".to_string()),
                                        Some(id),
                                        display_name,
                                        None,
                                        None,
                                    ),
                                    LoggingGrantee::AmazonCustomerByEmail { email_address } => (
                                        Some("AmazonCustomerByEmail".to_string()),
                                        None,
                                        None,
                                        Some(email_address),
                                        None,
                                    ),
                                    LoggingGrantee::Group { uri } => {
                                        (Some("Group".to_string()), None, None, None, Some(uri))
                                    }
                                };
                            LoggingGrantResponse {
                                grantee: LoggingGranteeResponse {
                                    grantee_type,
                                    xmlns_xsi: Some("http://www.w3.org/2001/XMLSchema-instance"),
                                    id,
                                    display_name,
                                    email_address,
                                    uri,
                                },
                                permission: g.permission.as_str().to_string(),
                            }
                        })
                        .collect(),
                })
            },
        }),
    }
}

/// `GET /{bucket}?logging` - Get bucket logging configuration.
///
/// Returns the logging configuration for the specified bucket.
/// If logging is not configured, returns an empty BucketLoggingStatus.
pub async fn get_bucket_logging(
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

    let config = state.storage.get_bucket_logging(&bucket).await?.unwrap_or_default();

    let response = convert_config_to_response(config);
    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::InternalError,
            format!("Failed to serialize response: {e}"),
        )
    })?;
    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `PUT /{bucket}?logging` - Set bucket logging configuration.
///
/// Sets the logging configuration for the specified bucket.
pub async fn put_bucket_logging(
    State(state): State<AppState>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<StatusCode, ApiError> {
    // Verify bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
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

    let request: BucketLoggingStatusRequest = from_str(body_str).map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            format!("Failed to parse logging configuration: {e}"),
        )
    })?;

    // If logging_enabled has a target_bucket, verify it exists
    if let Some(ref enabled) = request.logging_enabled {
        if !state.storage.head_bucket(&enabled.target_bucket).await? {
            return Err(ApiError::new(
                rucket_core::error::S3ErrorCode::InvalidTargetBucketForLogging,
                "The target bucket for logging does not exist",
            )
            .with_resource(&enabled.target_bucket));
        }
    }

    // Convert to core type
    let config = convert_request_to_config(request)?;

    // Store the configuration
    state.storage.put_bucket_logging(&bucket, config).await?;

    Ok(StatusCode::OK)
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
    async fn test_get_logging_no_bucket() {
        let (state, _temp_dir) = create_test_state().await;

        let result = get_bucket_logging(State(state), Path("nonexistent".to_string())).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::NoSuchBucket);
    }

    #[tokio::test]
    async fn test_get_logging_not_configured() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = get_bucket_logging(State(state), Path("test-bucket".to_string())).await;

        // Should return empty config, not an error
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_and_get_logging() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket and target bucket
        state.storage.create_bucket("test-bucket").await.unwrap();
        state.storage.create_bucket("log-bucket").await.unwrap();

        let xml = r#"
            <BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <LoggingEnabled>
                    <TargetBucket>log-bucket</TargetBucket>
                    <TargetPrefix>logs/</TargetPrefix>
                </LoggingEnabled>
            </BucketLoggingStatus>
        "#;

        let put_result = put_bucket_logging(
            State(state.clone()),
            Path("test-bucket".to_string()),
            Bytes::from(xml),
        )
        .await;
        assert!(put_result.is_ok(), "Put failed: {:?}", put_result.err());

        // Get the configuration back
        let get_result = get_bucket_logging(State(state), Path("test-bucket".to_string())).await;
        assert!(get_result.is_ok(), "Get failed: {:?}", get_result.err());
    }

    #[tokio::test]
    async fn test_put_logging_invalid_target() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket but NOT target bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let xml = r#"
            <BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
                <LoggingEnabled>
                    <TargetBucket>nonexistent-bucket</TargetBucket>
                    <TargetPrefix>logs/</TargetPrefix>
                </LoggingEnabled>
            </BucketLoggingStatus>
        "#;

        let result =
            put_bucket_logging(State(state), Path("test-bucket".to_string()), Bytes::from(xml))
                .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::InvalidTargetBucketForLogging);
    }

    #[tokio::test]
    async fn test_put_logging_empty_config() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        // Empty config disables logging
        let xml = r#"
            <BucketLoggingStatus xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            </BucketLoggingStatus>
        "#;

        let result =
            put_bucket_logging(State(state), Path("test-bucket".to_string()), Bytes::from(xml))
                .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_put_logging_malformed_xml() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = put_bucket_logging(
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
