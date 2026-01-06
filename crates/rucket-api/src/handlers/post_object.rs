//! POST Object (form-based upload) handler.
//!
//! This module implements the browser-based form upload for S3.
//! POST Object allows uploading objects using HTML forms with multipart/form-data encoding.

use std::collections::HashMap;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;
use base64::Engine;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use multer::{parse_boundary, Multipart};
use rucket_core::error::S3ErrorCode;
use rucket_core::types::StorageClass;
use rucket_storage::{ObjectHeaders, StorageBackend};

use crate::error::ApiError;
use crate::handlers::bucket::AppState;

/// Form fields for POST Object.
#[derive(Debug, Default)]
pub struct PostObjectForm {
    /// The object key.
    pub key: Option<String>,
    /// The access key ID.
    pub aws_access_key_id: Option<String>,
    /// The signature (v2) or x-amz-signature (v4).
    pub signature: Option<String>,
    /// The encoded policy.
    pub policy: Option<String>,
    /// The algorithm (x-amz-algorithm).
    pub algorithm: Option<String>,
    /// The credential (x-amz-credential).
    pub credential: Option<String>,
    /// The date (x-amz-date).
    pub date: Option<String>,
    /// The ACL for the object.
    pub acl: Option<String>,
    /// Success action redirect URL.
    pub success_action_redirect: Option<String>,
    /// Success action status (200, 201, 204).
    pub success_action_status: Option<u16>,
    /// Content-Type for the uploaded object.
    pub content_type: Option<String>,
    /// Cache-Control header.
    pub cache_control: Option<String>,
    /// Content-Disposition header.
    pub content_disposition: Option<String>,
    /// Content-Encoding header.
    pub content_encoding: Option<String>,
    /// Expires header.
    pub expires: Option<String>,
    /// Storage class.
    pub storage_class: Option<StorageClass>,
    /// Object tagging.
    pub tagging: Option<String>,
    /// Server-side encryption.
    pub server_side_encryption: Option<String>,
    /// User metadata (x-amz-meta-*).
    pub user_metadata: HashMap<String, String>,
    /// The file content.
    pub file_content: Option<Bytes>,
    /// The file name (from Content-Disposition in file field).
    pub file_name: Option<String>,
}

impl PostObjectForm {
    /// Process the key, replacing ${filename} with the actual filename.
    pub fn process_key(&self) -> Option<String> {
        self.key.as_ref().map(|k| {
            if let Some(filename) = &self.file_name {
                k.replace("${filename}", filename)
            } else {
                k.clone()
            }
        })
    }
}

/// Parse multipart form data for POST Object.
async fn parse_post_form(content_type: &str, body: Bytes) -> Result<PostObjectForm, ApiError> {
    let boundary = parse_boundary(content_type).map_err(|e| {
        ApiError::new(S3ErrorCode::InvalidRequest, format!("Invalid Content-Type boundary: {}", e))
    })?;

    let mut form = PostObjectForm::default();
    let stream = futures_util::stream::once(async { Ok::<_, std::io::Error>(body) });
    let mut multipart = Multipart::new(stream, boundary);

    while let Some(field) = multipart.next_field().await.map_err(|e| {
        ApiError::new(S3ErrorCode::InvalidRequest, format!("Multipart error: {}", e))
    })? {
        let name = field.name().map(|s| s.to_lowercase());
        let field_name = name.as_deref().unwrap_or("");

        // Check if this is the file field
        if field_name == "file" {
            // Get filename from Content-Disposition if present
            if let Some(filename) = field.file_name() {
                form.file_name = Some(filename.to_string());
            }
            // Content-Type of the file
            if form.content_type.is_none() {
                if let Some(ct) = field.content_type() {
                    form.content_type = Some(ct.to_string());
                }
            }
            // Read file content
            let data = field.bytes().await.map_err(|e| {
                ApiError::new(S3ErrorCode::InvalidRequest, format!("Error reading file: {}", e))
            })?;
            form.file_content = Some(data);
            continue;
        }

        // Read field value
        let value = field.text().await.map_err(|e| {
            ApiError::new(S3ErrorCode::InvalidRequest, format!("Error reading field: {}", e))
        })?;

        match field_name {
            "key" => form.key = Some(value),
            "awsaccesskeyid" | "x-amz-access-key-id" => form.aws_access_key_id = Some(value),
            "signature" => form.signature = Some(value),
            "x-amz-signature" => form.signature = Some(value),
            "policy" => form.policy = Some(value),
            "x-amz-algorithm" => form.algorithm = Some(value),
            "x-amz-credential" => form.credential = Some(value),
            "x-amz-date" => form.date = Some(value),
            "acl" | "x-amz-acl" => form.acl = Some(value),
            "success_action_redirect" | "redirect" => form.success_action_redirect = Some(value),
            "success_action_status" => {
                form.success_action_status = value.parse().ok();
            }
            "content-type" => form.content_type = Some(value),
            "cache-control" => form.cache_control = Some(value),
            "content-disposition" => form.content_disposition = Some(value),
            "content-encoding" => form.content_encoding = Some(value),
            "expires" => form.expires = Some(value),
            "x-amz-storage-class" => {
                form.storage_class = StorageClass::parse(&value);
            }
            "x-amz-tagging" | "tagging" => form.tagging = Some(value),
            "x-amz-server-side-encryption" => form.server_side_encryption = Some(value),
            _ => {
                // Check for user metadata
                if field_name.starts_with("x-amz-meta-") {
                    let meta_key = field_name.trim_start_matches("x-amz-meta-").to_string();
                    form.user_metadata.insert(meta_key, value);
                }
            }
        }
    }

    Ok(form)
}

/// Validate the POST policy.
///
/// The policy is a base64-encoded JSON document that specifies conditions
/// that must be met for the upload to succeed.
fn validate_policy(form: &PostObjectForm, bucket: &str) -> Result<(), ApiError> {
    let policy_str = match &form.policy {
        Some(p) => p,
        None => {
            // No policy means anonymous upload is allowed (if configured)
            return Ok(());
        }
    };

    // Decode policy
    let policy_bytes =
        base64::engine::general_purpose::STANDARD.decode(policy_str).map_err(|_| {
            ApiError::new(S3ErrorCode::InvalidPolicyDocument, "Invalid policy encoding")
        })?;

    let policy: serde_json::Value = serde_json::from_slice(&policy_bytes)
        .map_err(|_| ApiError::new(S3ErrorCode::InvalidPolicyDocument, "Invalid policy JSON"))?;

    // Check expiration
    if let Some(expiration) = policy.get("expiration").and_then(|v| v.as_str()) {
        let exp_time = DateTime::parse_from_rfc3339(expiration)
            .or_else(|_| {
                // Try ISO 8601 format without timezone
                chrono::NaiveDateTime::parse_from_str(expiration, "%Y-%m-%dT%H:%M:%S%.fZ")
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).fixed_offset())
            })
            .map_err(|_| {
                ApiError::new(S3ErrorCode::InvalidPolicyDocument, "Invalid expiration format")
            })?;

        if Utc::now() > exp_time {
            return Err(ApiError::new(S3ErrorCode::ExpiredToken, "POST policy has expired"));
        }
    }

    // Validate conditions
    if let Some(conditions) = policy.get("conditions").and_then(|v| v.as_array()) {
        for condition in conditions {
            validate_condition(condition, form, bucket)?;
        }
    }

    Ok(())
}

/// Validate a single policy condition.
fn validate_condition(
    condition: &serde_json::Value,
    form: &PostObjectForm,
    bucket: &str,
) -> Result<(), ApiError> {
    if let Some(arr) = condition.as_array() {
        // Array condition: ["operator", "$field", "value"]
        if arr.len() == 3 {
            let operator = arr[0].as_str().unwrap_or("");
            let field = arr[1].as_str().unwrap_or("").trim_start_matches('$');
            let value = arr[2].as_str().unwrap_or("");

            let form_value = get_form_field_value(form, field, bucket);

            match operator.to_lowercase().as_str() {
                "eq" => {
                    if form_value.as_deref() != Some(value) {
                        return Err(ApiError::new(
                            S3ErrorCode::AccessDenied,
                            format!("Policy condition failed: {} must equal {}", field, value),
                        ));
                    }
                }
                "starts-with" => {
                    if !form_value.as_deref().unwrap_or("").starts_with(value) {
                        return Err(ApiError::new(
                            S3ErrorCode::AccessDenied,
                            format!("Policy condition failed: {} must start with {}", field, value),
                        ));
                    }
                }
                _ => {}
            }
        }
        // Check for content-length-range: ["content-length-range", min, max]
        if arr.len() == 3 && arr[0].as_str() == Some("content-length-range") {
            if let (Some(min), Some(max)) = (arr[1].as_i64(), arr[2].as_i64()) {
                let size = form.file_content.as_ref().map(|c| c.len() as i64).unwrap_or(0);
                if size < min || size > max {
                    return Err(ApiError::new(
                        S3ErrorCode::EntityTooSmall,
                        format!("File size {} not in range [{}, {}]", size, min, max),
                    ));
                }
            }
        }
    } else if let Some(obj) = condition.as_object() {
        // Object condition: {"field": "value"}
        for (field, expected) in obj {
            let field = field.trim_start_matches('$');
            let expected_val = expected.as_str().unwrap_or("");
            let form_value = get_form_field_value(form, field, bucket);

            if form_value.as_deref() != Some(expected_val) {
                return Err(ApiError::new(
                    S3ErrorCode::AccessDenied,
                    format!("Policy condition failed: {} must equal {}", field, expected_val),
                ));
            }
        }
    }

    Ok(())
}

/// Get a form field value by name.
fn get_form_field_value(form: &PostObjectForm, field: &str, bucket: &str) -> Option<String> {
    match field.to_lowercase().as_str() {
        "bucket" => Some(bucket.to_string()),
        "key" => form.process_key(),
        "acl" | "x-amz-acl" => form.acl.clone(),
        "content-type" => form.content_type.clone(),
        "cache-control" => form.cache_control.clone(),
        "content-disposition" => form.content_disposition.clone(),
        "content-encoding" => form.content_encoding.clone(),
        "expires" => form.expires.clone(),
        "success_action_redirect" | "redirect" => form.success_action_redirect.clone(),
        "success_action_status" => form.success_action_status.map(|s| s.to_string()),
        "x-amz-algorithm" => form.algorithm.clone(),
        "x-amz-credential" => form.credential.clone(),
        "x-amz-date" => form.date.clone(),
        "x-amz-storage-class" => form.storage_class.map(|s| s.as_str().to_string()),
        _ => {
            if field.starts_with("x-amz-meta-") {
                let key = field.trim_start_matches("x-amz-meta-");
                form.user_metadata.get(key).cloned()
            } else {
                None
            }
        }
    }
}

/// `POST /{bucket}` - Upload object using HTML form (POST Object).
pub async fn post_object(
    State(state): State<AppState>,
    bucket: String,
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

    // Get Content-Type header
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing Content-Type header"))?;

    // Check if this is multipart/form-data
    if !content_type.starts_with("multipart/form-data") {
        return Err(ApiError::new(
            S3ErrorCode::InvalidRequest,
            "POST Object requires Content-Type: multipart/form-data",
        ));
    }

    // Parse multipart form
    let form = parse_post_form(content_type, body).await?;

    // Validate required fields
    let key = form
        .process_key()
        .ok_or_else(|| ApiError::new(S3ErrorCode::InvalidRequest, "Missing required field: key"))?;

    let file_content = form.file_content.clone().ok_or_else(|| {
        ApiError::new(S3ErrorCode::InvalidRequest, "Missing required field: file")
    })?;

    // Validate policy if provided
    validate_policy(&form, &bucket)?;

    // Verify signature if credentials provided
    if form.policy.is_some() && form.signature.is_some() {
        // Signature verification is handled by auth middleware or here
        // For now, we accept any valid-looking signature
        // TODO: Implement proper signature verification
    }

    // Build object headers
    let object_headers = ObjectHeaders {
        content_type: form.content_type.clone(),
        cache_control: form.cache_control.clone(),
        content_disposition: form.content_disposition.clone(),
        content_encoding: form.content_encoding.clone(),
        expires: form.expires.clone(),
        content_language: None,
        checksum_algorithm: None,
        storage_class: form.storage_class,
        sse_customer_algorithm: None,
        sse_customer_key_md5: None,
        encryption_nonce: None,
    };

    // Upload the object
    let result = state
        .storage
        .put_object(&bucket, &key, file_content, object_headers, form.user_metadata)
        .await?;

    // Handle success_action_redirect
    if let Some(redirect_url) = &form.success_action_redirect {
        // Append bucket, key, and etag to redirect URL
        let mut url = redirect_url.clone();
        if url.contains('?') {
            url.push('&');
        } else {
            url.push('?');
        }
        url.push_str(&format!(
            "bucket={}&key={}&etag={}",
            percent_encoding::utf8_percent_encode(&bucket, percent_encoding::NON_ALPHANUMERIC),
            percent_encoding::utf8_percent_encode(&key, percent_encoding::NON_ALPHANUMERIC),
            percent_encoding::utf8_percent_encode(
                result.etag.as_str(),
                percent_encoding::NON_ALPHANUMERIC
            ),
        ));

        return Ok(Response::builder()
            .status(StatusCode::SEE_OTHER)
            .header("Location", &url)
            .header("ETag", result.etag.as_str())
            .body(axum::body::Body::empty())
            .unwrap());
    }

    // Handle success_action_status
    let status = match form.success_action_status {
        Some(200) => StatusCode::OK,
        Some(201) => StatusCode::CREATED,
        Some(204) | None => StatusCode::NO_CONTENT,
        _ => StatusCode::NO_CONTENT,
    };

    if status == StatusCode::CREATED {
        // Return XML response with location, bucket, key, etag
        let location = format!("/{}/{}", bucket, key);
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<PostResponse>
    <Location>{}</Location>
    <Bucket>{}</Bucket>
    <Key>{}</Key>
    <ETag>{}</ETag>
</PostResponse>"#,
            location,
            bucket,
            key,
            result.etag.as_str()
        );

        return Ok(Response::builder()
            .status(StatusCode::CREATED)
            .header("Content-Type", "application/xml")
            .header("ETag", result.etag.as_str())
            .header("Location", &location)
            .body(axum::body::Body::from(xml))
            .unwrap());
    }

    // Default: 204 No Content
    Ok(Response::builder()
        .status(status)
        .header("ETag", result.etag.as_str())
        .body(axum::body::Body::empty())
        .unwrap())
}
