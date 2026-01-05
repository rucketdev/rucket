//! Responses from applying metadata commands.
//!
//! This module defines the response types returned after a `MetadataCommand`
//! is applied to the Raft state machine.

use rucket_core::types::{BucketInfo, Part};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Response from applying a metadata command.
///
/// Each command returns a specific response indicating success or failure.
/// The response is returned to the client after the command is committed
/// to a quorum of nodes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum MetadataResponse {
    /// Operation succeeded with no specific return value.
    #[default]
    Ok,

    /// Bucket created successfully.
    BucketCreated(BucketInfo),

    /// Delete marker created (versioned bucket deletion).
    ///
    /// Returns the version ID of the new delete marker.
    DeleteMarkerCreated {
        /// Version ID of the created delete marker.
        version_id: String,
    },

    /// Object or version deleted.
    ///
    /// Returns the UUID of the deleted object data for file cleanup.
    ObjectDeleted {
        /// UUID of the deleted object's data file, if any.
        ///
        /// This is `None` if:
        /// - The object didn't exist
        /// - It was a delete marker (no data file)
        deleted_uuid: Option<Uuid>,
    },

    /// Multipart upload created.
    MultipartCreated {
        /// The upload ID.
        upload_id: String,
    },

    /// Part uploaded successfully.
    PartUploaded {
        /// Part number.
        part_number: u32,
        /// Part ETag.
        etag: String,
    },

    /// Version ID (for delete marker creation, etc).
    VersionId(String),

    /// UUID of a deleted object data file.
    DeletedUuid(Uuid),

    /// UUIDs of multiple deleted object data files.
    DeletedUuids(Vec<Uuid>),

    /// Part metadata for a multipart upload.
    Part(Part),

    /// PG ownership updated successfully.
    PgOwnershipUpdated {
        /// Number of PG entries updated.
        count: u32,
        /// Ownership epoch.
        epoch: u64,
    },

    /// Operation failed with an error.
    Error {
        /// S3-compatible error code (e.g., "NoSuchBucket", "BucketNotEmpty").
        code: String,
        /// Human-readable error message.
        message: String,
    },
}

impl MetadataResponse {
    /// Creates an `Ok` response.
    #[must_use]
    pub fn ok() -> Self {
        Self::Ok
    }

    /// Creates an `Error` response from an error code and message.
    #[must_use]
    pub fn error(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self::Error { code: code.into(), message: message.into() }
    }

    /// Creates an error response from a `rucket_core::Error`.
    #[must_use]
    pub fn from_error(err: &rucket_core::Error) -> Self {
        Self::Error {
            code: err
                .s3_error_code()
                .map_or_else(|| "InternalError".to_string(), |c| c.to_string()),
            message: err.to_string(),
        }
    }

    /// Returns `true` if this is a successful response.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        !matches!(self, Self::Error { .. })
    }

    /// Returns `true` if this is an error response.
    #[must_use]
    pub fn is_err(&self) -> bool {
        matches!(self, Self::Error { .. })
    }

    /// Converts this response to a `Result`.
    ///
    /// # Errors
    ///
    /// Returns an error if this is an `Error` response.
    pub fn into_result(self) -> Result<Self, (String, String)> {
        match self {
            Self::Error { code, message } => Err((code, message)),
            other => Ok(other),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_serialization() {
        let response = MetadataResponse::DeleteMarkerCreated { version_id: "v1".to_string() };

        let serialized = bincode::serialize(&response).unwrap();
        let deserialized: MetadataResponse = bincode::deserialize(&serialized).unwrap();

        assert!(matches!(
            deserialized,
            MetadataResponse::DeleteMarkerCreated { version_id }
            if version_id == "v1"
        ));
    }

    #[test]
    fn test_error_response() {
        let response = MetadataResponse::error("NoSuchBucket", "The bucket does not exist");
        assert!(response.is_err());
        assert!(!response.is_ok());
    }

    #[test]
    fn test_ok_response() {
        let response = MetadataResponse::Ok;
        assert!(response.is_ok());
        assert!(!response.is_err());
    }

    #[test]
    fn test_into_result() {
        let ok = MetadataResponse::Ok;
        assert!(ok.into_result().is_ok());

        let err = MetadataResponse::error("TestError", "Test message");
        let result = err.into_result();
        assert!(result.is_err());

        let (code, message) = result.unwrap_err();
        assert_eq!(code, "TestError");
        assert_eq!(message, "Test message");
    }
}
