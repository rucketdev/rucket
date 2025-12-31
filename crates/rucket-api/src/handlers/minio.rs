// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! MinIO-specific endpoint handlers.
//!
//! These handlers provide MinIO-compatible endpoints that are not part of
//! the standard S3 API. They are only enabled when `api.compatibility_mode`
//! is set to `minio`.

use axum::http::StatusCode;
use axum::response::IntoResponse;

/// `GET /minio/health/live` - Liveness probe.
///
/// Returns 200 OK if the server is running.
/// Used by Kubernetes and other orchestrators for liveness checks.
pub async fn health_live() -> impl IntoResponse {
    StatusCode::OK
}

/// `GET /minio/health/ready` - Readiness probe.
///
/// Returns 200 OK if the server is ready to accept requests.
/// Used by Kubernetes and other orchestrators for readiness checks.
pub async fn health_ready() -> impl IntoResponse {
    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn test_health_live() {
        let response = health_live().await;
        assert_eq!(response.into_response().status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_health_ready() {
        let response = health_ready().await;
        assert_eq!(response.into_response().status(), StatusCode::OK);
    }
}
