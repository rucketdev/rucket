// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Error types for Rucket with S3-compatible error codes.

use thiserror::Error;

/// A specialized `Result` type for Rucket operations.
pub type Result<T> = std::result::Result<T, Error>;

/// S3-compatible error codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum S3ErrorCode {
    /// Access denied.
    AccessDenied,
    /// The specified bucket already exists.
    BucketAlreadyExists,
    /// The bucket you tried to delete is not empty.
    BucketNotEmpty,
    /// The Content-MD5 you specified did not match what we received.
    BadDigest,
    /// The request signature we calculated does not match the signature you provided.
    SignatureDoesNotMatch,
    /// The AWS access key ID you provided does not exist in our records.
    InvalidAccessKeyId,
    /// The specified bucket does not exist.
    NoSuchBucket,
    /// The specified key does not exist.
    NoSuchKey,
    /// The specified upload does not exist.
    NoSuchUpload,
    /// Your proposed upload is smaller than the minimum allowed object size.
    EntityTooSmall,
    /// Your proposed upload exceeds the maximum allowed object size.
    EntityTooLarge,
    /// One or more of the specified parts could not be found.
    InvalidPart,
    /// The list of parts was not in ascending order.
    InvalidPartOrder,
    /// Internal server error.
    InternalError,
    /// The request method is not allowed.
    MethodNotAllowed,
    /// The specified key is not valid.
    InvalidKey,
    /// The specified argument is not valid.
    InvalidArgument,
    /// The requested range is not satisfiable.
    InvalidRange,
    /// Invalid request.
    InvalidRequest,
    /// The functionality is not implemented.
    NotImplemented,
    /// At least one of the preconditions you specified did not hold.
    PreconditionFailed,
}

impl S3ErrorCode {
    /// Returns the HTTP status code for this error.
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::AccessDenied | Self::SignatureDoesNotMatch | Self::InvalidAccessKeyId => 403,
            Self::NoSuchBucket | Self::NoSuchKey | Self::NoSuchUpload => 404,
            Self::BucketAlreadyExists | Self::BucketNotEmpty => 409,
            Self::MethodNotAllowed => 405,
            Self::EntityTooSmall
            | Self::EntityTooLarge
            | Self::InvalidPart
            | Self::InvalidPartOrder
            | Self::InvalidKey
            | Self::InvalidArgument
            | Self::BadDigest
            | Self::InvalidRequest => 400,
            Self::InvalidRange => 416,
            Self::InternalError => 500,
            Self::NotImplemented => 501,
            Self::PreconditionFailed => 412,
        }
    }

    /// Returns the HTTP status code as an `http::StatusCode`.
    #[must_use]
    pub fn status_code(&self) -> http::StatusCode {
        http::StatusCode::from_u16(self.http_status())
            .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR)
    }

    /// Returns the S3 error code string.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AccessDenied => "AccessDenied",
            Self::BucketAlreadyExists => "BucketAlreadyExists",
            Self::BucketNotEmpty => "BucketNotEmpty",
            Self::BadDigest => "BadDigest",
            Self::SignatureDoesNotMatch => "SignatureDoesNotMatch",
            Self::InvalidAccessKeyId => "InvalidAccessKeyId",
            Self::NoSuchBucket => "NoSuchBucket",
            Self::NoSuchKey => "NoSuchKey",
            Self::NoSuchUpload => "NoSuchUpload",
            Self::EntityTooSmall => "EntityTooSmall",
            Self::EntityTooLarge => "EntityTooLarge",
            Self::InvalidPart => "InvalidPart",
            Self::InvalidPartOrder => "InvalidPartOrder",
            Self::InternalError => "InternalError",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::InvalidKey => "InvalidKey",
            Self::InvalidArgument => "InvalidArgument",
            Self::InvalidRange => "InvalidRange",
            Self::InvalidRequest => "InvalidRequest",
            Self::NotImplemented => "NotImplemented",
            Self::PreconditionFailed => "PreconditionFailed",
        }
    }
}

impl std::fmt::Display for S3ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Errors that can occur during Rucket operations.
#[derive(Debug, Error)]
pub enum Error {
    /// An S3 API error with a specific error code.
    #[error("{code}: {message}")]
    S3 {
        /// The S3 error code.
        code: S3ErrorCode,
        /// A human-readable error message.
        message: String,
        /// The resource that caused the error (bucket name, key, etc.).
        resource: Option<String>,
    },

    /// Configuration error.
    #[error("configuration error: {0}")]
    Config(String),

    /// I/O error.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Database error.
    #[error("database error: {0}")]
    Database(String),

    /// Invalid request.
    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

impl Error {
    /// Creates a new S3 error.
    #[must_use]
    pub fn s3(code: S3ErrorCode, message: impl Into<String>) -> Self {
        Self::S3 { code, message: message.into(), resource: None }
    }

    /// Creates a new S3 error with a resource.
    #[must_use]
    pub fn s3_with_resource(
        code: S3ErrorCode,
        message: impl Into<String>,
        resource: impl Into<String>,
    ) -> Self {
        Self::S3 { code, message: message.into(), resource: Some(resource.into()) }
    }

    /// Returns the S3 error code, if this is an S3 error.
    #[must_use]
    pub const fn s3_error_code(&self) -> Option<S3ErrorCode> {
        match self {
            Self::S3 { code, .. } => Some(*code),
            _ => None,
        }
    }

    /// Returns the HTTP status code for this error.
    #[must_use]
    pub const fn http_status(&self) -> u16 {
        match self {
            Self::S3 { code, .. } => code.http_status(),
            Self::Config(_) | Self::InvalidRequest(_) => 400,
            Self::Io(_) | Self::Database(_) => 500,
        }
    }
}
