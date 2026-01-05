// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Object operation handlers.
//!
//! This module contains all S3 object-level operations:
//! - CRUD: put, get, delete, head, copy
//! - Listing: list_objects_v1, list_objects_v2, list_object_versions, delete_objects
//! - Tagging: get, put, delete object tagging
//! - Retention: object lock retention and legal hold
//! - ACL: object ACL and attributes

pub mod acl;
mod common;
pub mod crud;
pub mod list;
pub mod retention;
pub mod tagging;

// Re-export common types
// Re-export ACL handlers
pub use acl::{get_object_acl, get_object_attributes, put_object_acl, ObjectAttribute};
pub use common::{
    format_http_date, s3_url_decode, ListObjectsQuery, ResponseHeaderOverrides, MAX_OBJECT_TAGS,
    MAX_TAG_KEY_LENGTH, MAX_TAG_VALUE_LENGTH,
};
// Re-export CRUD handlers
pub use crud::{copy_object, delete_object, get_object, head_object, put_object, RangeSpec};
// Re-export list handlers
pub use list::{delete_objects, list_object_versions, list_objects_v1, list_objects_v2};
// Re-export retention handlers
pub use retention::{
    get_object_legal_hold, get_object_retention, put_object_legal_hold, put_object_retention,
};
// Re-export tagging handlers
pub use tagging::{delete_object_tagging, get_object_tagging, put_object_tagging};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::response::IntoResponse;
    use bytes::Bytes;
    use rucket_core::config::ApiCompatibilityMode;
    use rucket_storage::{LocalStorage, ObjectHeaders, StorageBackend};
    use tempfile::TempDir;

    use super::*;
    use crate::handlers::bucket::AppState;
    use crate::router::RequestQuery;

    /// Create a RequestQuery with a specific version_id.
    fn query_with_version_id(version_id: String) -> RequestQuery {
        // Use serde to create a RequestQuery with the version_id set
        serde_json::from_str(&format!(r#"{{"versionId": "{}"}}"#, version_id)).unwrap()
    }

    /// Create a test storage and app state.
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

    /// Create a test bucket and object.
    async fn setup_bucket_and_object(state: &AppState) {
        state.storage.create_bucket("test-bucket").await.unwrap();
        state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("test content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_get_object_tagging_empty() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        let result = tagging::get_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(result.is_ok());
        let response = result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_put_and_get_object_tagging() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Put tagging
        let tagging_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>env</Key>
                        <Value>test</Value>
                    </Tag>
                </TagSet>
            </Tagging>"#;

        let put_result = tagging::put_object_tagging(
            State(state.clone()),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
            Bytes::from(tagging_xml),
        )
        .await;

        assert!(put_result.is_ok());
        let response = put_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);

        // Get tagging and verify
        let get_result = tagging::get_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(get_result.is_ok());
        let response = get_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_delete_object_tagging() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // First put some tags
        let tagging_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>env</Key>
                        <Value>test</Value>
                    </Tag>
                </TagSet>
            </Tagging>"#;

        tagging::put_object_tagging(
            State(state.clone()),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
            Bytes::from(tagging_xml),
        )
        .await
        .unwrap();

        // Delete tagging
        let delete_result = tagging::delete_object_tagging(
            State(state.clone()),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(delete_result.is_ok());
        let response = delete_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        // Verify tags are deleted (should return empty tag set)
        let get_result = tagging::get_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(get_result.is_ok());
    }

    #[tokio::test]
    async fn test_get_object_tagging_with_version_id() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Enable versioning
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        // Upload a new version
        let result = state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("version 2 content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();

        let version_id = result.version_id.unwrap();

        // Get tagging with version ID
        let get_result = tagging::get_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(query_with_version_id(version_id.clone())),
        )
        .await;

        assert!(get_result.is_ok());
        let response = get_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
        // Verify version ID header is present
        assert!(response.headers().get("x-amz-version-id").is_some());
    }

    #[tokio::test]
    async fn test_put_object_tagging_with_version_id() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Enable versioning
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        // Upload a new version
        let result = state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("version 2 content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();

        let version_id = result.version_id.unwrap();

        // Put tagging with version ID
        let tagging_xml = r#"<?xml version="1.0" encoding="UTF-8"?>
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>version</Key>
                        <Value>2</Value>
                    </Tag>
                </TagSet>
            </Tagging>"#;

        let put_result = tagging::put_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(query_with_version_id(version_id.clone())),
            Bytes::from(tagging_xml),
        )
        .await;

        assert!(put_result.is_ok());
        let response = put_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::OK);
        // Verify version ID header is present
        assert!(response.headers().get("x-amz-version-id").is_some());
    }

    #[tokio::test]
    async fn test_delete_object_tagging_with_version_id() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Enable versioning
        state
            .storage
            .set_bucket_versioning("test-bucket", rucket_core::types::VersioningStatus::Enabled)
            .await
            .unwrap();

        // Upload a new version
        let result = state
            .storage
            .put_object(
                "test-bucket",
                "test-key.txt",
                Bytes::from("version 2 content"),
                ObjectHeaders::default(),
                std::collections::HashMap::new(),
            )
            .await
            .unwrap();

        let version_id = result.version_id.unwrap();

        // Delete tagging with version ID
        let delete_result = tagging::delete_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(query_with_version_id(version_id.clone())),
        )
        .await;

        assert!(delete_result.is_ok());
        let response = delete_result.unwrap().into_response();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);
        // Verify version ID header is present
        assert!(response.headers().get("x-amz-version-id").is_some());
    }

    #[tokio::test]
    async fn test_put_object_tagging_malformed_xml() {
        let (state, _temp_dir) = create_test_state().await;
        setup_bucket_and_object(&state).await;

        // Put malformed tagging XML
        let put_result = tagging::put_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "test-key.txt".to_string())),
            Query(RequestQuery::default()),
            Bytes::from("not valid xml"),
        )
        .await;

        assert!(put_result.is_err());
    }

    #[tokio::test]
    async fn test_get_object_tagging_nonexistent_object() {
        let (state, _temp_dir) = create_test_state().await;
        state.storage.create_bucket("test-bucket").await.unwrap();

        // Try to get tags for non-existent object
        let result = tagging::get_object_tagging(
            State(state),
            None,
            Path(("test-bucket".to_string(), "nonexistent.txt".to_string())),
            Query(RequestQuery::default()),
        )
        .await;

        assert!(result.is_err());
    }
}
