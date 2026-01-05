// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Error types for cross-region replication.

use thiserror::Error;

/// Result type for geo operations.
pub type GeoResult<T> = Result<T, GeoError>;

/// Errors that can occur during cross-region replication.
#[derive(Debug, Error)]
pub enum GeoError {
    /// Invalid replication configuration.
    #[error("Invalid replication configuration: {0}")]
    InvalidConfig(String),

    /// Region not found.
    #[error("Region not found: {0}")]
    RegionNotFound(String),

    /// Replication rule not found.
    #[error("Replication rule not found: {0}")]
    RuleNotFound(String),

    /// Connection error to remote region.
    #[error("Connection error to region {region}: {message}")]
    ConnectionError {
        /// The target region.
        region: String,
        /// Error message.
        message: String,
    },

    /// Replication timeout.
    #[error("Replication timeout to region {region} after {timeout_ms}ms")]
    Timeout {
        /// The target region.
        region: String,
        /// Timeout in milliseconds.
        timeout_ms: u64,
    },

    /// Conflict detected during replication.
    #[error(
        "Conflict detected for {bucket}/{key}: local HLC={local_hlc}, remote HLC={remote_hlc}"
    )]
    Conflict {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Local HLC timestamp.
        local_hlc: u64,
        /// Remote HLC timestamp.
        remote_hlc: u64,
    },

    /// Object not found.
    #[error("Object not found: {bucket}/{key}")]
    ObjectNotFound {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },

    /// Bucket not found.
    #[error("Bucket not found: {0}")]
    BucketNotFound(String),

    /// Replication is disabled for this bucket.
    #[error("Replication is disabled for bucket: {0}")]
    ReplicationDisabled(String),

    /// Invalid filter specification.
    #[error("Invalid filter: {0}")]
    InvalidFilter(String),

    /// Storage error.
    #[error("Storage error: {0}")]
    Storage(#[from] rucket_core::Error),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Internal error.
    #[error("Internal error: {0}")]
    Internal(String),
}

impl GeoError {
    /// Create a connection error.
    pub fn connection(region: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ConnectionError { region: region.into(), message: message.into() }
    }

    /// Create a timeout error.
    pub fn timeout(region: impl Into<String>, timeout_ms: u64) -> Self {
        Self::Timeout { region: region.into(), timeout_ms }
    }

    /// Create a conflict error.
    pub fn conflict(
        bucket: impl Into<String>,
        key: impl Into<String>,
        local_hlc: u64,
        remote_hlc: u64,
    ) -> Self {
        Self::Conflict { bucket: bucket.into(), key: key.into(), local_hlc, remote_hlc }
    }

    /// Create an object not found error.
    pub fn object_not_found(bucket: impl Into<String>, key: impl Into<String>) -> Self {
        Self::ObjectNotFound { bucket: bucket.into(), key: key.into() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = GeoError::InvalidConfig("missing destination".to_string());
        assert!(err.to_string().contains("missing destination"));
    }

    #[test]
    fn test_conflict_error() {
        let err = GeoError::conflict("bucket", "key", 100, 200);
        assert!(err.to_string().contains("bucket/key"));
        assert!(err.to_string().contains("100"));
        assert!(err.to_string().contains("200"));
    }

    #[test]
    fn test_connection_error() {
        let err = GeoError::connection("us-west-2", "timeout");
        assert!(err.to_string().contains("us-west-2"));
        assert!(err.to_string().contains("timeout"));
    }
}
