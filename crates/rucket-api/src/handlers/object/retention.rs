// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Object Lock operations: retention and legal hold.

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use axum::Extension;
use bytes::Bytes;
use chrono::Utc;
use rucket_core::error::S3ErrorCode;
use rucket_core::policy::S3Action;
use rucket_core::types::{ObjectRetention, RetentionMode};
use rucket_storage::StorageBackend;

use crate::auth::AuthContext;
use crate::error::ApiError;
use crate::handlers::bucket::AppState;
use crate::policy::{evaluate_bucket_policy, get_auth_context, RequestInfo};

/// `GET /{bucket}/{key}?retention` - Get object retention configuration.
pub async fn get_object_retention(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
) -> Result<Response, ApiError> {
    // Check bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::default();
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::GetObjectRetention,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Get retention from storage
    let retention = if let Some(ref version_id) = query.version_id {
        state.storage.get_object_retention_version(&bucket, &key, version_id).await?
    } else {
        state.storage.get_object_retention(&bucket, &key).await?
    };

    match retention {
        Some(ret) => {
            // Format date as ISO 8601
            let date = ret.retain_until_date.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string();

            let xml = format!(
                r#"<?xml version="1.0" encoding="UTF-8"?><Retention xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Mode>{}</Mode><RetainUntilDate>{}</RetainUntilDate></Retention>"#,
                ret.mode.as_str(),
                date
            );

            let mut response = Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/xml");

            if let Some(ref version_id) = query.version_id {
                response = response.header("x-amz-version-id", version_id.as_str());
            }

            response
                .body(Body::from(xml))
                .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
        }
        None => Err(ApiError::new(
            S3ErrorCode::ObjectLockConfigurationNotFoundError,
            "The specified object does not have a retention configuration",
        )
        .with_resource(&key)),
    }
}

/// `PUT /{bucket}/{key}?retention` - Set object retention configuration.
pub async fn put_object_retention(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Check bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::from_headers(&headers);
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::PutObjectRetention,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Check for bypass governance header
    let bypass_governance = headers
        .get("x-amz-bypass-governance-retention")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    // Parse the XML body
    let body_str = std::str::from_utf8(&body).map_err(|_| {
        ApiError::new(S3ErrorCode::MalformedXML, "The XML provided is not well-formed")
    })?;

    // Parse Retention XML
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(body_str);
    reader.config_mut().trim_text(true);

    let mut mode: Option<RetentionMode> = None;
    let mut retain_until_date: Option<chrono::DateTime<Utc>> = None;
    let mut current_element = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();
            }
            Ok(Event::Text(e)) => {
                let text = String::from_utf8_lossy(&e).to_string();
                match current_element.as_str() {
                    "Mode" => {
                        mode = RetentionMode::parse(&text);
                    }
                    "RetainUntilDate" => {
                        retain_until_date = chrono::DateTime::parse_from_rfc3339(&text)
                            .ok()
                            .map(|dt| dt.with_timezone(&Utc));
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => {
                return Err(ApiError::new(
                    S3ErrorCode::MalformedXML,
                    "The XML provided is not well-formed",
                ));
            }
            _ => {}
        }
    }

    // Validate we have both mode and date
    let mode = mode.ok_or_else(|| {
        ApiError::new(S3ErrorCode::MalformedXML, "Missing Mode in Retention configuration")
    })?;
    let retain_until_date = retain_until_date.ok_or_else(|| {
        ApiError::new(
            S3ErrorCode::MalformedXML,
            "Missing RetainUntilDate in Retention configuration",
        )
    })?;

    // Check if we're allowed to modify the retention
    // If current retention is in Compliance mode and hasn't expired, we can't shorten it
    let current_retention = if let Some(ref version_id) = query.version_id {
        state.storage.get_object_retention_version(&bucket, &key, version_id).await.ok().flatten()
    } else {
        state.storage.get_object_retention(&bucket, &key).await.ok().flatten()
    };

    if let Some(ref current) = current_retention {
        // Check if we're trying to shorten retention
        if retain_until_date < current.retain_until_date {
            // In Compliance mode, never allowed to shorten
            if current.mode == RetentionMode::Compliance {
                return Err(ApiError::new(
                    S3ErrorCode::AccessDenied,
                    "Cannot shorten retention period in Compliance mode",
                )
                .with_resource(&key));
            }
            // In Governance mode, need bypass header
            if current.mode == RetentionMode::Governance && !bypass_governance {
                return Err(ApiError::new(
                    S3ErrorCode::AccessDenied,
                    "Cannot shorten retention period without bypass-governance-retention header",
                )
                .with_resource(&key));
            }
        }
    }

    let retention = ObjectRetention { mode, retain_until_date };

    // Set retention in storage
    if let Some(ref version_id) = query.version_id {
        state.storage.put_object_retention_version(&bucket, &key, version_id, retention).await?;
    } else {
        state.storage.put_object_retention(&bucket, &key, retention).await?;
    }

    let mut response = Response::builder().status(StatusCode::OK);

    if let Some(ref version_id) = query.version_id {
        response = response.header("x-amz-version-id", version_id.as_str());
    }

    response
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// `GET /{bucket}/{key}?legal-hold` - Get object legal hold status.
pub async fn get_object_legal_hold(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
) -> Result<Response, ApiError> {
    // Check bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::default();
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::GetObjectLegalHold,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Get legal hold status from storage
    let enabled = if let Some(ref version_id) = query.version_id {
        state.storage.get_object_legal_hold_version(&bucket, &key, version_id).await?
    } else {
        state.storage.get_object_legal_hold(&bucket, &key).await?
    };

    let status = if enabled { "ON" } else { "OFF" };
    let xml = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?><LegalHold xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>{status}</Status></LegalHold>"#
    );

    let mut response =
        Response::builder().status(StatusCode::OK).header("Content-Type", "application/xml");

    if let Some(ref version_id) = query.version_id {
        response = response.header("x-amz-version-id", version_id.as_str());
    }

    response
        .body(Body::from(xml))
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}

/// `PUT /{bucket}/{key}?legal-hold` - Set object legal hold status.
pub async fn put_object_legal_hold(
    State(state): State<AppState>,
    auth: Option<Extension<AuthContext>>,
    Path((bucket, key)): Path<(String, String)>,
    Query(query): Query<crate::router::RequestQuery>,
    body: Bytes,
) -> Result<Response, ApiError> {
    // Check bucket exists
    if !state.storage.head_bucket(&bucket).await? {
        return Err(ApiError::new(
            S3ErrorCode::NoSuchBucket,
            "The specified bucket does not exist",
        )
        .with_resource(&bucket));
    }

    // Evaluate bucket policy
    let auth_ctx = get_auth_context(auth);
    let req_info = RequestInfo::default();
    evaluate_bucket_policy(
        &*state.storage,
        &auth_ctx,
        &bucket,
        Some(&key),
        S3Action::PutObjectLegalHold,
        req_info.source_ip,
        req_info.is_secure,
    )
    .await?;

    // Parse the XML body
    let body_str = std::str::from_utf8(&body).map_err(|_| {
        ApiError::new(S3ErrorCode::MalformedXML, "The XML provided is not well-formed")
    })?;

    // Parse LegalHold XML
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(body_str);
    reader.config_mut().trim_text(true);

    let mut status: Option<bool> = None;
    let mut current_element = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                current_element = String::from_utf8_lossy(e.name().as_ref()).to_string();
            }
            Ok(Event::Text(e)) => {
                let text = String::from_utf8_lossy(&e).to_string();
                if current_element == "Status" {
                    status = Some(text.eq_ignore_ascii_case("ON"));
                }
            }
            Ok(Event::Eof) => break,
            Err(_) => {
                return Err(ApiError::new(
                    S3ErrorCode::MalformedXML,
                    "The XML provided is not well-formed",
                ));
            }
            _ => {}
        }
    }

    let enabled = status.ok_or_else(|| {
        ApiError::new(S3ErrorCode::MalformedXML, "Missing Status in LegalHold configuration")
    })?;

    // Set legal hold in storage
    if let Some(ref version_id) = query.version_id {
        state.storage.put_object_legal_hold_version(&bucket, &key, version_id, enabled).await?;
    } else {
        state.storage.put_object_legal_hold(&bucket, &key, enabled).await?;
    }

    let mut response = Response::builder().status(StatusCode::OK);

    if let Some(ref version_id) = query.version_id {
        response = response.header("x-amz-version-id", version_id.as_str());
    }

    response
        .body(Body::empty())
        .map_err(|e| ApiError::new(S3ErrorCode::InternalError, e.to_string()))
}
