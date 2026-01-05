// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Object listing operations: list_objects_v1, list_objects_v2, list_object_versions, delete_objects.

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Extension;
use bytes::Bytes;
use rucket_core::config::ApiCompatibilityMode;
use rucket_core::error::S3ErrorCode;
use rucket_core::policy::S3Action;
use rucket_storage::StorageBackend;

use super::common::{
    parse_max_keys, s3_url_decode, s3_url_encode, s3_url_encode_prefix, ListObjectsQuery,
};
use crate::auth::AuthContext;
use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::policy::{evaluate_bucket_policy, get_auth_context};
use crate::xml::request::DeleteObjects;
use crate::xml::response::{
    to_xml, CommonPrefix, DeleteError, DeleteMarker, DeleteObjectsResponse, DeletedObject,
    ListObjectVersionsResponse, ListObjectsV1Response, ListObjectsV2Response, ObjectEntry,
    ObjectVersion,
};

/// `GET /{bucket}` - List objects V1.
pub async fn list_objects_v1(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        None,
        S3Action::ListBucket,
        None,
        false,
    )
    .await?;

    // Ceph extension: allow-unordered with delimiter is not supported
    if state.compatibility_mode == ApiCompatibilityMode::Ceph
        && query.allow_unordered.as_deref() == Some("true")
        && query.delimiter.is_some()
    {
        return Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            "allow-unordered cannot be used with delimiter",
        ));
    }

    let max_keys = parse_max_keys(&query.max_keys)?;

    let result = state
        .storage
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            query.marker.as_deref(),
            max_keys,
        )
        .await?;

    // For V1, NextMarker is only set when using a delimiter and is_truncated
    // It should be the lexicographically last item returned (object key OR common prefix)
    let next_marker = if result.is_truncated && query.delimiter.is_some() {
        // Find the last key and last prefix, then return the lexicographically greater one
        let last_key = result.objects.last().map(|o| o.key.as_str());
        let last_prefix = result.common_prefixes.last().map(|s| s.as_str());
        match (last_key, last_prefix) {
            (Some(key), Some(prefix)) => Some(std::cmp::max(key, prefix).to_string()),
            (Some(key), None) => Some(key.to_string()),
            (None, Some(prefix)) => Some(prefix.to_string()),
            (None, None) => None,
        }
    } else {
        None
    };

    // Check if URL encoding is requested
    let use_url_encoding = query.encoding_type.as_deref() == Some("url");

    let response = if use_url_encoding {
        ListObjectsV1Response {
            name: bucket,
            prefix: s3_url_encode_prefix(&query.prefix.unwrap_or_default()),
            marker: s3_url_encode_prefix(&query.marker.unwrap_or_default()),
            next_marker: next_marker.map(|m| s3_url_encode_prefix(&m)),
            // Note: Delimiter is NOT encoded per S3 spec
            delimiter: query.delimiter.clone(),
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            contents: result
                .objects
                .iter()
                .map(|m| ObjectEntry::from(m).with_encoded_key(&s3_url_encode(&m.key)))
                .collect(),
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: s3_url_encode_prefix(&p) })
                .collect(),
        }
    } else {
        // In non-encoded mode, URL-decode the prefix and marker
        // (axum may not fully decode special characters like %0A)
        ListObjectsV1Response {
            name: bucket,
            prefix: s3_url_decode(&query.prefix.unwrap_or_default()),
            marker: s3_url_decode(&query.marker.unwrap_or_default()),
            next_marker,
            delimiter: query.delimiter,
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            contents: result.objects.iter().map(ObjectEntry::from).collect(),
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: p })
                .collect(),
        }
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}?list-type=2` - List objects V2.
pub async fn list_objects_v2(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        None,
        S3Action::ListBucket,
        None,
        false,
    )
    .await?;

    // Ceph extension: allow-unordered with delimiter is not supported
    if state.compatibility_mode == ApiCompatibilityMode::Ceph
        && query.allow_unordered.as_deref() == Some("true")
        && query.delimiter.is_some()
    {
        return Err(ApiError::new(
            S3ErrorCode::InvalidArgument,
            "allow-unordered cannot be used with delimiter",
        ));
    }

    let max_keys = parse_max_keys(&query.max_keys)?;

    // Handle start-after: use it as the continuation token if no continuation token is provided
    let effective_token = query.continuation_token.as_deref().or(query.start_after.as_deref());

    let result = state
        .storage
        .list_objects(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            effective_token,
            max_keys,
        )
        .await?;

    // Check if fetch-owner is requested
    let fetch_owner = query.fetch_owner.as_deref() == Some("true");

    // Check if URL encoding is requested
    let use_url_encoding = query.encoding_type.as_deref() == Some("url");

    // For V2 with delimiter, use the last returned item as continuation token
    // This ensures the next page starts after all returned items (including prefixes)
    let next_continuation_token = if result.is_truncated {
        if query.delimiter.is_some() {
            // Use the lexicographically last item returned
            let last_key = result.objects.last().map(|o| o.key.as_str());
            let last_prefix = result.common_prefixes.last().map(|s| s.as_str());
            match (last_key, last_prefix) {
                (Some(key), Some(prefix)) => Some(std::cmp::max(key, prefix).to_string()),
                (Some(key), None) => Some(key.to_string()),
                (None, Some(prefix)) => Some(prefix.to_string()),
                (None, None) => result.next_continuation_token.clone(),
            }
        } else {
            // No delimiter - use storage layer's token (the next key to start from)
            result.next_continuation_token.clone()
        }
    } else {
        None
    };

    let response = if use_url_encoding {
        ListObjectsV2Response {
            name: bucket,
            prefix: query.prefix.map(|p| s3_url_encode_prefix(&p)),
            // Note: Delimiter is NOT encoded per S3 spec
            delimiter: query.delimiter.clone(),
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            continuation_token: query.continuation_token,
            next_continuation_token: next_continuation_token.clone(),
            start_after: query.start_after.map(|s| s3_url_encode_prefix(&s)),
            key_count: (result.objects.len() + result.common_prefixes.len()) as u32,
            contents: if fetch_owner {
                result
                    .objects
                    .iter()
                    .map(|m| ObjectEntry::with_owner(m).with_encoded_key(&s3_url_encode(&m.key)))
                    .collect()
            } else {
                result
                    .objects
                    .iter()
                    .map(|m| ObjectEntry::from(m).with_encoded_key(&s3_url_encode(&m.key)))
                    .collect()
            },
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: s3_url_encode_prefix(&p) })
                .collect(),
        }
    } else {
        ListObjectsV2Response {
            name: bucket,
            prefix: query.prefix,
            delimiter: query.delimiter,
            max_keys,
            encoding_type: query.encoding_type,
            is_truncated: result.is_truncated,
            continuation_token: query.continuation_token,
            next_continuation_token,
            start_after: query.start_after,
            key_count: (result.objects.len() + result.common_prefixes.len()) as u32,
            contents: if fetch_owner {
                result.objects.iter().map(ObjectEntry::with_owner).collect()
            } else {
                result.objects.iter().map(ObjectEntry::from).collect()
            },
            common_prefixes: result
                .common_prefixes
                .into_iter()
                .map(|p| CommonPrefix { prefix: p })
                .collect(),
        }
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `GET /{bucket}?versions` - List object versions.
///
/// Returns all versions of objects in a bucket, including delete markers.
/// For non-versioned buckets, returns all objects with a version ID of "null".
pub async fn list_object_versions(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(bucket): Path<String>,
    Query(query): Query<ListObjectsQuery>,
) -> Result<Response, ApiError> {
    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        None,
        S3Action::ListBucketVersions,
        None,
        false,
    )
    .await?;

    let max_keys = parse_max_keys(&query.max_keys)?;

    // Use the real list_object_versions method that returns all versions
    let result = state
        .storage
        .list_object_versions(
            &bucket,
            query.prefix.as_deref(),
            query.delimiter.as_deref(),
            query.key_marker.as_deref(),
            query.version_id_marker.as_deref(),
            max_keys,
        )
        .await?;

    // Separate versions and delete markers
    let mut versions: Vec<ObjectVersion> = Vec::new();
    let mut delete_markers: Vec<DeleteMarker> = Vec::new();

    for entry in result.versions {
        if entry.is_delete_marker {
            delete_markers.push(DeleteMarker::from_metadata(&entry.metadata));
        } else {
            versions.push(ObjectVersion::from_metadata(&entry.metadata));
        }
    }

    let response = ListObjectVersionsResponse {
        name: bucket,
        prefix: query.prefix,
        key_marker: query.key_marker.clone().unwrap_or_default(),
        version_id_marker: query.version_id_marker.clone().unwrap_or_default(),
        next_key_marker: result.next_key_marker,
        next_version_id_marker: result.next_version_id_marker,
        max_keys,
        is_truncated: result.is_truncated,
        versions,
        delete_markers,
        common_prefixes: result
            .common_prefixes
            .into_iter()
            .map(|p| CommonPrefix { prefix: p })
            .collect(),
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}

/// `POST /{bucket}?delete` - Delete multiple objects.
pub async fn delete_objects(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path(bucket): Path<String>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Parse the XML request body
    let request: DeleteObjects = quick_xml::de::from_reader(body.as_ref()).map_err(|e| {
        ApiError::new(S3ErrorCode::InvalidRequest, format!("Failed to parse DeleteObjects: {e}"))
    })?;

    // Evaluate bucket policy for DeleteObject action
    // Note: In full implementation, this would check per-object, but for simplicity
    // we check at the bucket level
    let auth_ctx = get_auth_context(auth);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        None,
        S3Action::DeleteObject,
        None,
        false,
    )
    .await?;

    // S3 limits DeleteObjects to 1000 keys
    if request.objects.len() > 1000 {
        return Err(ApiError::new(
            S3ErrorCode::InvalidRequest,
            "DeleteObjects request must contain at most 1000 keys",
        ));
    }

    let mut deleted = Vec::new();
    let mut errors = Vec::new();

    // Delete each object
    for obj in &request.objects {
        let result = if let Some(ref version_id) = obj.version_id {
            // Delete specific version
            state.storage.delete_object_version(&bucket, &obj.key, version_id).await
        } else {
            // Delete latest version (may create delete marker for versioned buckets)
            state.storage.delete_object(&bucket, &obj.key).await
        };

        match result {
            Ok(delete_result) => {
                // Return the version ID from the result, or from the request, or "null"
                let version_id = delete_result
                    .version_id
                    .or_else(|| obj.version_id.clone())
                    .or_else(|| Some("null".to_string()));
                deleted.push(DeletedObject { key: obj.key.clone(), version_id });
            }
            Err(e) => {
                // Convert storage error to S3 error code and message
                let api_err: ApiError = e.into();
                errors.push(DeleteError {
                    key: obj.key.clone(),
                    code: api_err.code.as_str().to_string(),
                    message: api_err.message,
                });
            }
        }
    }

    // Build response - in quiet mode, only report errors
    let response = if request.quiet {
        DeleteObjectsResponse { deleted: Vec::new(), errors }
    } else {
        DeleteObjectsResponse { deleted, errors }
    };

    let xml = to_xml(&response).map_err(|e| {
        ApiError::new(S3ErrorCode::InternalError, format!("Failed to serialize response: {e}"))
    })?;

    Ok((StatusCode::OK, [("Content-Type", "application/xml")], xml).into_response())
}
