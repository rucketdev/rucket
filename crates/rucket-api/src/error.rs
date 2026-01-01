//! API error types and S3 error response formatting.

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use rucket_core::error::{Error as CoreError, S3ErrorCode};

/// API-level error that can be converted to an HTTP response.
#[derive(Debug)]
pub struct ApiError {
    /// S3 error code.
    pub code: S3ErrorCode,
    /// Human-readable message.
    pub message: String,
    /// Resource that caused the error (bucket, key, etc.).
    pub resource: Option<String>,
    /// Request ID for tracking.
    pub request_id: String,
}

impl ApiError {
    /// Create a new API error.
    #[must_use]
    pub fn new(code: S3ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            resource: None,
            request_id: uuid::Uuid::new_v4().to_string(),
        }
    }

    /// Add resource information to the error.
    #[must_use]
    pub fn with_resource(mut self, resource: impl Into<String>) -> Self {
        self.resource = Some(resource.into());
        self
    }

    /// Get the HTTP status code for this error.
    #[must_use]
    pub fn status_code(&self) -> StatusCode {
        self.code.status_code()
    }

    /// Convert to XML error response body.
    #[must_use]
    pub fn to_xml(&self) -> String {
        let resource =
            self.resource.as_deref().map_or(String::new(), |r| format!("<Resource>{r}</Resource>"));

        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
<Code>{}</Code>
<Message>{}</Message>
{resource}
<RequestId>{}</RequestId>
</Error>"#,
            self.code.as_str(),
            self.message,
            self.request_id
        )
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status = self.status_code();
        let request_id = self.request_id.clone();
        let body = self.to_xml();

        (
            status,
            [("Content-Type", "application/xml"), ("x-amz-request-id", request_id.as_str())],
            body,
        )
            .into_response()
    }
}

impl From<CoreError> for ApiError {
    fn from(err: CoreError) -> Self {
        match err {
            CoreError::S3 { code, message, resource } => {
                let mut api_err = ApiError::new(code, message);
                if let Some(r) = resource {
                    api_err = api_err.with_resource(r);
                }
                api_err
            }
            CoreError::Io(e) => {
                ApiError::new(S3ErrorCode::InternalError, format!("I/O error: {e}"))
            }
            CoreError::Database(msg) => {
                ApiError::new(S3ErrorCode::InternalError, format!("Database error: {msg}"))
            }
            CoreError::Config(msg) => {
                ApiError::new(S3ErrorCode::InternalError, format!("Configuration error: {msg}"))
            }
            CoreError::InvalidRequest(msg) => ApiError::new(S3ErrorCode::InvalidRequest, msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_error_to_xml() {
        let err =
            ApiError::new(S3ErrorCode::NoSuchBucket, "Bucket not found").with_resource("my-bucket");

        let xml = err.to_xml();
        assert!(xml.contains("<Code>NoSuchBucket</Code>"));
        assert!(xml.contains("<Message>Bucket not found</Message>"));
        assert!(xml.contains("<Resource>my-bucket</Resource>"));
    }

    #[test]
    fn test_status_code_mapping() {
        assert_eq!(
            ApiError::new(S3ErrorCode::NoSuchBucket, "").status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            ApiError::new(S3ErrorCode::AccessDenied, "").status_code(),
            StatusCode::FORBIDDEN
        );
    }
}
