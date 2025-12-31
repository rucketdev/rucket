// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! S3 API metrics definitions.
//!
//! This module provides Prometheus-compatible metrics for S3 operations.

use std::time::Duration;

use metrics::{counter, describe_counter, describe_histogram, histogram};

/// S3 operation type for metrics labeling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3Operation {
    // Bucket operations
    /// Create a new bucket.
    CreateBucket,
    /// Delete a bucket.
    DeleteBucket,
    /// Check if bucket exists.
    HeadBucket,
    /// List all buckets.
    ListBuckets,

    // Object operations
    /// Upload an object.
    PutObject,
    /// Download an object.
    GetObject,
    /// Get object metadata.
    HeadObject,
    /// Delete an object.
    DeleteObject,
    /// Copy an object.
    CopyObject,
    /// Delete multiple objects.
    DeleteObjects,
    /// List objects in a bucket.
    ListObjectsV2,

    // Multipart operations
    /// Initiate a multipart upload.
    CreateMultipartUpload,
    /// Upload a part.
    UploadPart,
    /// Complete a multipart upload.
    CompleteMultipartUpload,
    /// Abort a multipart upload.
    AbortMultipartUpload,
    /// List parts of a multipart upload.
    ListParts,
    /// List multipart uploads in a bucket.
    ListMultipartUploads,
}

impl S3Operation {
    /// Returns the operation name as a string for metric labels.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateBucket => "CreateBucket",
            Self::DeleteBucket => "DeleteBucket",
            Self::HeadBucket => "HeadBucket",
            Self::ListBuckets => "ListBuckets",
            Self::PutObject => "PutObject",
            Self::GetObject => "GetObject",
            Self::HeadObject => "HeadObject",
            Self::DeleteObject => "DeleteObject",
            Self::CopyObject => "CopyObject",
            Self::DeleteObjects => "DeleteObjects",
            Self::ListObjectsV2 => "ListObjectsV2",
            Self::CreateMultipartUpload => "CreateMultipartUpload",
            Self::UploadPart => "UploadPart",
            Self::CompleteMultipartUpload => "CompleteMultipartUpload",
            Self::AbortMultipartUpload => "AbortMultipartUpload",
            Self::ListParts => "ListParts",
            Self::ListMultipartUploads => "ListMultipartUploads",
        }
    }
}

/// Initialize metric descriptions (call once at startup).
pub fn init_metrics() {
    // Request metrics
    describe_counter!("rucket_requests_total", "Total number of S3 API requests");
    describe_histogram!("rucket_request_duration_seconds", "Request duration in seconds");
    describe_counter!("rucket_request_bytes_total", "Total bytes received in requests");
    describe_counter!("rucket_response_bytes_total", "Total bytes sent in responses");
    describe_counter!("rucket_errors_total", "Total number of errors by type");
}

/// Record a completed request.
pub fn record_request(
    operation: S3Operation,
    status_code: u16,
    duration: Duration,
    request_bytes: u64,
    response_bytes: u64,
) {
    let op = operation.as_str();
    let status = status_class(status_code);
    let success = if status_code < 400 { "true" } else { "false" };

    counter!("rucket_requests_total",
        "operation" => op,
        "status" => status,
        "success" => success
    )
    .increment(1);

    histogram!("rucket_request_duration_seconds",
        "operation" => op,
        "status" => status
    )
    .record(duration.as_secs_f64());

    if request_bytes > 0 {
        counter!("rucket_request_bytes_total", "operation" => op).increment(request_bytes);
    }

    if response_bytes > 0 {
        counter!("rucket_response_bytes_total", "operation" => op).increment(response_bytes);
    }
}

/// Record an error by S3 error code.
pub fn record_error(operation: S3Operation, error_code: &str) {
    counter!("rucket_errors_total",
        "operation" => operation.as_str(),
        "error_code" => error_code.to_string()
    )
    .increment(1);
}

/// Convert status code to status class for metric labels.
fn status_class(status_code: u16) -> &'static str {
    match status_code {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

/// Determine the S3 operation from HTTP method, path, and query parameters.
#[must_use]
pub fn determine_operation(
    method: &http::Method,
    path: &str,
    query: Option<&str>,
) -> Option<S3Operation> {
    let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
    let has_bucket = !parts.is_empty() && !parts[0].is_empty();
    let has_key = parts.len() > 1;

    match (method.clone(), has_bucket, has_key) {
        // Service level
        (http::Method::GET, false, false) => Some(S3Operation::ListBuckets),

        // Bucket level
        (http::Method::PUT, true, false) => Some(S3Operation::CreateBucket),
        (http::Method::DELETE, true, false) => Some(S3Operation::DeleteBucket),
        (http::Method::HEAD, true, false) => Some(S3Operation::HeadBucket),
        (http::Method::GET, true, false) => {
            if query.is_some_and(|q| q.contains("uploads")) {
                Some(S3Operation::ListMultipartUploads)
            } else {
                Some(S3Operation::ListObjectsV2)
            }
        }
        (http::Method::POST, true, false) => {
            if query.is_some_and(|q| q.contains("delete")) {
                Some(S3Operation::DeleteObjects)
            } else {
                None
            }
        }

        // Object level
        (http::Method::PUT, true, true) => {
            if query.is_some_and(|q| q.contains("partNumber")) {
                Some(S3Operation::UploadPart)
            } else {
                Some(S3Operation::PutObject)
            }
        }
        (http::Method::GET, true, true) => {
            if query.is_some_and(|q| q.contains("uploadId")) {
                Some(S3Operation::ListParts)
            } else {
                Some(S3Operation::GetObject)
            }
        }
        (http::Method::HEAD, true, true) => Some(S3Operation::HeadObject),
        (http::Method::DELETE, true, true) => {
            if query.is_some_and(|q| q.contains("uploadId")) {
                Some(S3Operation::AbortMultipartUpload)
            } else {
                Some(S3Operation::DeleteObject)
            }
        }
        (http::Method::POST, true, true) => {
            if query.is_some_and(|q| q.contains("uploads")) {
                Some(S3Operation::CreateMultipartUpload)
            } else if query.is_some_and(|q| q.contains("uploadId")) {
                Some(S3Operation::CompleteMultipartUpload)
            } else {
                None
            }
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_determine_operation_list_buckets() {
        let op = determine_operation(&http::Method::GET, "/", None);
        assert_eq!(op, Some(S3Operation::ListBuckets));
    }

    #[test]
    fn test_determine_operation_create_bucket() {
        let op = determine_operation(&http::Method::PUT, "/my-bucket", None);
        assert_eq!(op, Some(S3Operation::CreateBucket));
    }

    #[test]
    fn test_determine_operation_put_object() {
        let op = determine_operation(&http::Method::PUT, "/my-bucket/my-key", None);
        assert_eq!(op, Some(S3Operation::PutObject));
    }

    #[test]
    fn test_determine_operation_get_object() {
        let op = determine_operation(&http::Method::GET, "/my-bucket/my-key", None);
        assert_eq!(op, Some(S3Operation::GetObject));
    }

    #[test]
    fn test_determine_operation_upload_part() {
        let op = determine_operation(&http::Method::PUT, "/my-bucket/my-key", Some("partNumber=1"));
        assert_eq!(op, Some(S3Operation::UploadPart));
    }

    #[test]
    fn test_determine_operation_list_objects() {
        let op = determine_operation(&http::Method::GET, "/my-bucket", Some("list-type=2"));
        assert_eq!(op, Some(S3Operation::ListObjectsV2));
    }

    #[test]
    fn test_status_class() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(204), "2xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(500), "5xx");
    }
}
