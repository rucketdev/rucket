//! Error types for erasure coding operations.

use thiserror::Error;

/// Result type for erasure operations.
pub type Result<T> = std::result::Result<T, ErasureError>;

/// Errors that can occur during erasure coding operations.
#[derive(Debug, Error)]
pub enum ErasureError {
    /// Invalid configuration parameters.
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Encoding operation failed.
    #[error("Encoding failed: {0}")]
    EncodingFailed(String),

    /// Decoding operation failed.
    #[error("Decoding failed: {0}")]
    DecodingFailed(String),

    /// Too many shards are missing to reconstruct data.
    #[error("Too many shards missing: have {available}, need at least {required}")]
    TooManyMissing {
        /// Number of shards available.
        available: usize,
        /// Minimum number of shards required.
        required: usize,
    },

    /// Shard size mismatch during reconstruction.
    #[error("Shard size mismatch: expected {expected}, got {actual}")]
    ShardSizeMismatch {
        /// Expected shard size.
        expected: usize,
        /// Actual shard size.
        actual: usize,
    },

    /// Invalid shard count.
    #[error("Invalid shard count: expected {expected}, got {actual}")]
    InvalidShardCount {
        /// Expected number of shards.
        expected: usize,
        /// Actual number of shards.
        actual: usize,
    },

    /// Reed-Solomon library error.
    #[error("Reed-Solomon error: {0}")]
    ReedSolomon(String),
}

impl From<reed_solomon_erasure::Error> for ErasureError {
    fn from(err: reed_solomon_erasure::Error) -> Self {
        ErasureError::ReedSolomon(err.to_string())
    }
}
