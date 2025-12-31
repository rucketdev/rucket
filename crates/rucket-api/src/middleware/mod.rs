//! Axum middleware for metrics and request logging.

use std::time::Instant;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use http::header::CONTENT_LENGTH;
use tracing::{info_span, Instrument};
use uuid::Uuid;

use crate::metrics::{determine_operation, record_request, S3Operation};

/// Extension to store request metadata for metrics.
#[derive(Clone)]
pub struct RequestMetadata {
    /// The detected S3 operation.
    pub operation: Option<S3Operation>,
    /// The unique request ID.
    pub request_id: String,
    /// Request start time.
    pub start_time: Instant,
    /// Request body size (if known).
    pub request_bytes: u64,
}

/// Metrics and logging middleware.
///
/// This middleware:
/// - Generates a unique request ID
/// - Detects the S3 operation from the request
/// - Times the request duration
/// - Records metrics after the request completes
/// - Creates a tracing span for the request
pub async fn metrics_layer(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let request_id = Uuid::new_v4().to_string();
    let method = request.method().clone();
    let uri = request.uri().clone();
    let path = uri.path().to_string();

    // Get request body size from Content-Length header
    let request_bytes = request
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    // Determine S3 operation from method + path + query
    let operation = determine_operation(&method, &path, uri.query());

    // Create span with trace context
    let span = info_span!(
        "http_request",
        request_id = %request_id,
        method = %method,
        path = %path,
        operation = operation.map(|o| o.as_str()).unwrap_or("unknown"),
    );

    // Execute request within span
    let response = next.run(request).instrument(span).await;

    let duration = start.elapsed();
    let status = response.status().as_u16();

    // Get response body size from Content-Length header if available
    let response_bytes = response
        .headers()
        .get(CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(0);

    // Record metrics if we identified the operation
    if let Some(op) = operation {
        record_request(op, status, duration, request_bytes, response_bytes);
    }

    response
}

/// Middleware that adds a request ID header to responses.
pub async fn request_id_layer(request: Request, next: Next) -> impl IntoResponse {
    let request_id = Uuid::new_v4().to_string();

    let response = next.run(request).await;

    // Add request ID to response headers
    let mut response = response.into_response();
    response.headers_mut().insert(
        "x-amz-request-id",
        request_id.parse().unwrap_or_else(|_| "unknown".parse().unwrap()),
    );

    response
}
