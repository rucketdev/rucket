// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! S3 bucket website configuration handlers.
//!
//! Implements the following S3 API operations:
//! - `GET /{bucket}?website` - GetBucketWebsite
//! - `PUT /{bucket}?website` - PutBucketWebsite
//! - `DELETE /{bucket}?website` - DeleteBucketWebsite

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use quick_xml::de::from_str;
use rucket_core::types::{
    ErrorDocument, IndexDocument, RedirectAllRequestsTo, RoutingRule, RoutingRuleCondition,
    RoutingRuleRedirect, WebsiteConfiguration, WebsiteRedirectProtocol,
};
use rucket_storage::StorageBackend;

use super::bucket::AppState;
use crate::error::ApiError;
use crate::xml::request::WebsiteConfigurationRequest;
use crate::xml::response::{
    to_xml, ErrorDocumentResponse, IndexDocumentResponse, RedirectAllRequestsToResponse,
    RoutingRuleConditionResponse, RoutingRuleRedirectResponse, RoutingRuleResponse,
    RoutingRulesResponse, WebsiteConfigurationResponse,
};

/// Convert a request to the core type.
fn convert_request_to_config(
    request: WebsiteConfigurationRequest,
) -> Result<WebsiteConfiguration, ApiError> {
    // Validate: either index_document or redirect_all_requests_to must be set
    if request.index_document.is_none() && request.redirect_all_requests_to.is_none() {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::InvalidArgument,
            "A website configuration must have an IndexDocument or a RedirectAllRequestsTo element",
        ));
    }

    // If redirect_all_requests_to is set, index_document and error_document should not be
    if request.redirect_all_requests_to.is_some()
        && (request.index_document.is_some() || request.error_document.is_some())
    {
        return Err(ApiError::new(
            rucket_core::error::S3ErrorCode::InvalidArgument,
            "RedirectAllRequestsTo cannot be used with IndexDocument or ErrorDocument",
        ));
    }

    let index_document = request.index_document.map(|i| IndexDocument { suffix: i.suffix });

    let error_document = request.error_document.map(|e| ErrorDocument { key: e.key });

    let redirect_all_requests_to =
        request.redirect_all_requests_to.map(|r| RedirectAllRequestsTo {
            host_name: r.host_name,
            protocol: r.protocol.and_then(|p| WebsiteRedirectProtocol::parse(&p)),
        });

    let routing_rules = request
        .routing_rules
        .map(|rules| {
            rules
                .rules
                .into_iter()
                .map(|r| RoutingRule {
                    condition: r.condition.map(|c| RoutingRuleCondition {
                        key_prefix_equals: c.key_prefix_equals,
                        http_error_code_returned_equals: c.http_error_code_returned_equals,
                    }),
                    redirect: RoutingRuleRedirect {
                        host_name: r.redirect.host_name,
                        http_redirect_code: r.redirect.http_redirect_code,
                        protocol: r
                            .redirect
                            .protocol
                            .and_then(|p| WebsiteRedirectProtocol::parse(&p)),
                        replace_key_prefix_with: r.redirect.replace_key_prefix_with,
                        replace_key_with: r.redirect.replace_key_with,
                    },
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(WebsiteConfiguration {
        index_document,
        error_document,
        redirect_all_requests_to,
        routing_rules,
    })
}

/// Convert the core type to a response.
fn convert_config_to_response(config: WebsiteConfiguration) -> WebsiteConfigurationResponse {
    WebsiteConfigurationResponse {
        index_document: config.index_document.map(|i| IndexDocumentResponse { suffix: i.suffix }),
        error_document: config.error_document.map(|e| ErrorDocumentResponse { key: e.key }),
        redirect_all_requests_to: config.redirect_all_requests_to.map(|r| {
            RedirectAllRequestsToResponse {
                host_name: r.host_name,
                protocol: r.protocol.map(|p| p.as_str().to_string()),
            }
        }),
        routing_rules: if config.routing_rules.is_empty() {
            None
        } else {
            Some(RoutingRulesResponse {
                rules: config
                    .routing_rules
                    .into_iter()
                    .map(|r| RoutingRuleResponse {
                        condition: r.condition.map(|c| RoutingRuleConditionResponse {
                            key_prefix_equals: c.key_prefix_equals,
                            http_error_code_returned_equals: c.http_error_code_returned_equals,
                        }),
                        redirect: RoutingRuleRedirectResponse {
                            host_name: r.redirect.host_name,
                            http_redirect_code: r.redirect.http_redirect_code,
                            protocol: r.redirect.protocol.map(|p| p.as_str().to_string()),
                            replace_key_prefix_with: r.redirect.replace_key_prefix_with,
                            replace_key_with: r.redirect.replace_key_with,
                        },
                    })
                    .collect(),
            })
        },
    }
}

/// `GET /{bucket}?website` - Get bucket website configuration.
///
/// Returns the website configuration for the specified bucket.
/// Returns 404 NoSuchWebsiteConfiguration if not configured.
pub async fn get_bucket_website(
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

    match state.storage.get_bucket_website(&bucket).await? {
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
            rucket_core::error::S3ErrorCode::NoSuchWebsiteConfiguration,
            "The specified bucket does not have a website configuration",
        )
        .with_resource(&bucket)),
    }
}

/// `PUT /{bucket}?website` - Set bucket website configuration.
///
/// Sets the website configuration for the specified bucket.
pub async fn put_bucket_website(
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

    let request: WebsiteConfigurationRequest = from_str(body_str).map_err(|e| {
        ApiError::new(
            rucket_core::error::S3ErrorCode::MalformedXML,
            format!("Failed to parse website configuration: {e}"),
        )
    })?;

    // Convert to core type
    let config = convert_request_to_config(request)?;

    // Store the configuration
    state.storage.put_bucket_website(&bucket, config).await?;

    Ok(StatusCode::OK)
}

/// `DELETE /{bucket}?website` - Delete bucket website configuration.
///
/// Removes the website configuration from the specified bucket.
pub async fn delete_bucket_website(
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
    state.storage.delete_bucket_website(&bucket).await?;

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
    async fn test_get_website_no_bucket() {
        let (state, _temp_dir) = create_test_state().await;

        let result = get_bucket_website(State(state), Path("nonexistent".to_string())).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::NoSuchBucket);
    }

    #[tokio::test]
    async fn test_get_website_not_configured() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = get_bucket_website(State(state), Path("test-bucket".to_string())).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::NoSuchWebsiteConfiguration);
    }

    #[tokio::test]
    async fn test_put_and_get_website() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        // First test direct storage layer
        let config = rucket_core::types::WebsiteConfiguration {
            index_document: Some(rucket_core::types::IndexDocument {
                suffix: "index.html".to_string(),
            }),
            error_document: Some(rucket_core::types::ErrorDocument {
                key: "error.html".to_string(),
            }),
            redirect_all_requests_to: None,
            routing_rules: vec![],
        };

        // Direct storage put
        state
            .storage
            .put_bucket_website("test-bucket", config.clone())
            .await
            .expect("Direct put should succeed");

        // Direct storage get
        let direct_get = state.storage.get_bucket_website("test-bucket").await;
        assert!(direct_get.is_ok(), "Direct get failed: {:?}", direct_get.err());
        let stored = direct_get.unwrap();
        assert!(stored.is_some(), "Stored config should exist");
        assert_eq!(stored.unwrap(), config);

        // Now test via handlers (clear the config first)
        state.storage.delete_bucket_website("test-bucket").await.unwrap();

        let xml = r#"
            <WebsiteConfiguration>
                <IndexDocument>
                    <Suffix>index.html</Suffix>
                </IndexDocument>
                <ErrorDocument>
                    <Key>error.html</Key>
                </ErrorDocument>
            </WebsiteConfiguration>
        "#;

        let put_result = put_bucket_website(
            State(state.clone()),
            Path("test-bucket".to_string()),
            Bytes::from(xml),
        )
        .await;
        assert!(put_result.is_ok(), "Put failed: {:?}", put_result.err());

        // Get the configuration back
        let get_result = get_bucket_website(State(state), Path("test-bucket".to_string())).await;
        if let Err(ref e) = get_result {
            eprintln!("Get error: {:?}", e);
        }
        assert!(get_result.is_ok(), "Get failed: {:?}", get_result.err());
    }

    #[tokio::test]
    async fn test_put_website_redirect_all() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let xml = r#"
            <WebsiteConfiguration>
                <RedirectAllRequestsTo>
                    <HostName>example.com</HostName>
                    <Protocol>https</Protocol>
                </RedirectAllRequestsTo>
            </WebsiteConfiguration>
        "#;

        let put_result = put_bucket_website(
            State(state.clone()),
            Path("test-bucket".to_string()),
            Bytes::from(xml),
        )
        .await;
        assert!(put_result.is_ok());
    }

    #[tokio::test]
    async fn test_delete_website() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = delete_bucket_website(State(state), Path("test-bucket".to_string())).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), StatusCode::NO_CONTENT);
    }

    #[tokio::test]
    async fn test_put_website_empty_config() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let xml = r#"
            <WebsiteConfiguration>
            </WebsiteConfiguration>
        "#;

        let result =
            put_bucket_website(State(state), Path("test-bucket".to_string()), Bytes::from(xml))
                .await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, rucket_core::error::S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn test_put_website_malformed_xml() {
        let (state, _temp_dir) = create_test_state().await;

        // Create bucket
        state.storage.create_bucket("test-bucket").await.unwrap();

        let result = put_bucket_website(
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
