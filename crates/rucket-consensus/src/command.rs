//! Metadata commands that go through Raft consensus.
//!
//! This module defines all metadata mutations that require linearizable
//! consistency across the cluster. Data writes (object bytes) are NOT
//! included here - they go through Primary-Backup replication.

use std::collections::HashMap;

use rucket_core::encryption::ServerSideEncryptionConfiguration;
use rucket_core::lifecycle::LifecycleConfiguration;
use rucket_core::public_access_block::PublicAccessBlockConfiguration;
use rucket_core::types::{
    CorsConfiguration, ObjectLockConfig, ObjectMetadata, ObjectRetention, TagSet, VersioningStatus,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Placement group ownership entry.
///
/// Tracks which nodes are responsible for a placement group.
/// This is the result of running CRUSH algorithm on the cluster map.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PgOwnershipEntry {
    /// Placement group ID.
    pub pg_id: u32,
    /// Primary node ID (CRUSH device ID).
    pub primary_node: u64,
    /// Replica node IDs (CRUSH device IDs).
    pub replica_nodes: Vec<u64>,
}

/// Commands that go through Raft consensus.
///
/// These are operations that must be linearizable across the cluster:
/// - **Bucket operations**: Create, delete, versioning changes
/// - **Object metadata**: Put, delete, version management
/// - **Configuration changes**: Policies, encryption, lifecycle
/// - **Object Lock**: Retention and legal hold
///
/// # Consistency Guarantee
///
/// All commands in this enum are applied atomically through Raft consensus.
/// A command is considered committed only after a quorum of nodes have
/// persisted it to their logs.
///
/// # What's NOT Here
///
/// Object data (bytes) is NOT replicated through Raft. Instead:
/// 1. Metadata command goes through Raft (this enum)
/// 2. Data is written to primary node
/// 3. Data is replicated to backups via Primary-Backup protocol
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetadataCommand {
    // ========================================================================
    // Bucket Operations (Strong Consistency Required)
    // ========================================================================
    /// Create a new bucket.
    ///
    /// Bucket names are globally unique across the cluster.
    CreateBucket {
        /// Bucket name.
        name: String,
        /// Whether to enable Object Lock on creation.
        lock_enabled: bool,
    },

    /// Delete a bucket.
    ///
    /// Fails if the bucket is not empty or doesn't exist.
    DeleteBucket {
        /// Bucket name.
        name: String,
    },

    /// Set bucket versioning status.
    ///
    /// Once versioning is enabled, it cannot be disabled (only suspended).
    SetBucketVersioning {
        /// Bucket name.
        bucket: String,
        /// New versioning status.
        status: VersioningStatus,
    },

    // ========================================================================
    // Bucket Configuration
    // ========================================================================
    /// Set bucket CORS configuration.
    PutBucketCors {
        /// Bucket name.
        bucket: String,
        /// CORS configuration.
        config: CorsConfiguration,
    },

    /// Delete bucket CORS configuration.
    DeleteBucketCors {
        /// Bucket name.
        bucket: String,
    },

    /// Set bucket policy (IAM-style JSON policy).
    PutBucketPolicy {
        /// Bucket name.
        bucket: String,
        /// Policy JSON document.
        policy_json: String,
    },

    /// Delete bucket policy.
    DeleteBucketPolicy {
        /// Bucket name.
        bucket: String,
    },

    /// Set bucket tags.
    PutBucketTagging {
        /// Bucket name.
        bucket: String,
        /// Tag set.
        tags: TagSet,
    },

    /// Delete bucket tags.
    DeleteBucketTagging {
        /// Bucket name.
        bucket: String,
    },

    /// Set public access block configuration.
    PutPublicAccessBlock {
        /// Bucket name.
        bucket: String,
        /// Public access block configuration.
        config: PublicAccessBlockConfiguration,
    },

    /// Delete public access block configuration.
    DeletePublicAccessBlock {
        /// Bucket name.
        bucket: String,
    },

    /// Set lifecycle configuration.
    PutLifecycleConfiguration {
        /// Bucket name.
        bucket: String,
        /// Lifecycle configuration.
        config: LifecycleConfiguration,
    },

    /// Delete lifecycle configuration.
    DeleteLifecycleConfiguration {
        /// Bucket name.
        bucket: String,
    },

    /// Set server-side encryption configuration.
    PutEncryptionConfiguration {
        /// Bucket name.
        bucket: String,
        /// Encryption configuration.
        config: ServerSideEncryptionConfiguration,
    },

    /// Delete server-side encryption configuration.
    DeleteEncryptionConfiguration {
        /// Bucket name.
        bucket: String,
    },

    /// Set bucket-level Object Lock configuration.
    PutBucketLockConfig {
        /// Bucket name.
        bucket: String,
        /// Object Lock configuration.
        config: ObjectLockConfig,
    },

    // ========================================================================
    // Object Metadata Operations
    // ========================================================================
    /// Store object metadata.
    ///
    /// This is called after the object data has been written to storage.
    /// The HLC timestamp ensures causality ordering.
    PutObjectMetadata {
        /// Bucket name.
        bucket: String,
        /// Full object metadata.
        metadata: ObjectMetadata,
        /// HLC timestamp for this write.
        hlc: u64,
    },

    /// Delete an object (non-versioned bucket or specific version).
    ///
    /// Returns the UUID of the deleted object for file cleanup.
    DeleteObject {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// HLC timestamp for this delete.
        hlc: u64,
    },

    /// Delete a specific object version.
    DeleteObjectVersion {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID to delete.
        version_id: String,
        /// HLC timestamp for this delete.
        hlc: u64,
    },

    /// Create a delete marker (versioned bucket deletion).
    ///
    /// In versioned buckets, DELETE creates a delete marker instead
    /// of actually removing the object.
    CreateDeleteMarker {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// HLC timestamp for this delete marker.
        hlc: u64,
    },

    // ========================================================================
    // Object Tagging
    // ========================================================================
    /// Set object tags.
    PutObjectTagging {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (None = current version).
        version_id: Option<String>,
        /// Tag set.
        tags: TagSet,
    },

    /// Delete object tags.
    DeleteObjectTagging {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (None = current version).
        version_id: Option<String>,
    },

    // ========================================================================
    // Object Lock Operations
    // ========================================================================
    /// Set object retention (WORM).
    PutObjectRetention {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (None = current version).
        version_id: Option<String>,
        /// Retention configuration.
        retention: ObjectRetention,
        /// Whether to bypass governance mode (requires special permission).
        bypass_governance: bool,
    },

    /// Set object legal hold.
    PutObjectLegalHold {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (None = current version).
        version_id: Option<String>,
        /// Whether legal hold is enabled.
        enabled: bool,
    },

    // ========================================================================
    // Multipart Upload Operations
    // ========================================================================
    /// Create a multipart upload.
    CreateMultipartUpload {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Upload ID (generated by caller).
        upload_id: String,
        /// Content type.
        content_type: Option<String>,
        /// User metadata.
        user_metadata: HashMap<String, String>,
        /// Cache-Control header.
        cache_control: Option<String>,
        /// Content-Disposition header.
        content_disposition: Option<String>,
        /// Content-Encoding header.
        content_encoding: Option<String>,
        /// Content-Language header.
        content_language: Option<String>,
        /// Expires header.
        expires: Option<String>,
    },

    /// Complete a multipart upload.
    ///
    /// Called after all parts have been uploaded and the final
    /// object metadata is assembled.
    CompleteMultipartUpload {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Upload ID.
        upload_id: String,
        /// Final assembled object metadata.
        metadata: ObjectMetadata,
        /// HLC timestamp for this completion.
        hlc: u64,
    },

    /// Abort a multipart upload.
    AbortMultipartUpload {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Upload ID.
        upload_id: String,
    },

    /// Record a part upload.
    PutPart {
        /// Upload ID.
        upload_id: String,
        /// Part number (1-10000).
        part_number: u32,
        /// UUID of the part data file.
        uuid: Uuid,
        /// Part size in bytes.
        size: u64,
        /// Part ETag.
        etag: String,
    },

    /// Delete parts for an upload (cleanup after abort/complete).
    DeleteParts {
        /// Upload ID.
        upload_id: String,
    },

    // ========================================================================
    // Placement Group Operations
    // ========================================================================
    /// Update placement group ownership.
    ///
    /// This is called when the cluster map changes and PG ownership
    /// needs to be recalculated. All nodes must agree on PG ownership
    /// to ensure consistent data routing.
    UpdatePgOwnership {
        /// Placement group ID.
        pg_id: u32,
        /// Primary node ID (CRUSH device ID).
        primary_node: u64,
        /// Replica node IDs (CRUSH device IDs).
        replica_nodes: Vec<u64>,
        /// Ownership epoch (incremented on each cluster map change).
        epoch: u64,
    },

    /// Batch update of all placement group ownership.
    ///
    /// Used during initial cluster setup or major reconfiguration.
    UpdateAllPgOwnership {
        /// All PG ownership entries.
        entries: Vec<PgOwnershipEntry>,
        /// Ownership epoch.
        epoch: u64,
    },
}

impl MetadataCommand {
    /// Returns the bucket name this command operates on, if any.
    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        match self {
            Self::CreateBucket { name, .. } | Self::DeleteBucket { name } => Some(name),

            Self::SetBucketVersioning { bucket, .. }
            | Self::PutBucketCors { bucket, .. }
            | Self::DeleteBucketCors { bucket }
            | Self::PutBucketPolicy { bucket, .. }
            | Self::DeleteBucketPolicy { bucket }
            | Self::PutBucketTagging { bucket, .. }
            | Self::DeleteBucketTagging { bucket }
            | Self::PutPublicAccessBlock { bucket, .. }
            | Self::DeletePublicAccessBlock { bucket }
            | Self::PutLifecycleConfiguration { bucket, .. }
            | Self::DeleteLifecycleConfiguration { bucket }
            | Self::PutEncryptionConfiguration { bucket, .. }
            | Self::DeleteEncryptionConfiguration { bucket }
            | Self::PutBucketLockConfig { bucket, .. }
            | Self::PutObjectMetadata { bucket, .. }
            | Self::DeleteObject { bucket, .. }
            | Self::DeleteObjectVersion { bucket, .. }
            | Self::CreateDeleteMarker { bucket, .. }
            | Self::PutObjectTagging { bucket, .. }
            | Self::DeleteObjectTagging { bucket, .. }
            | Self::PutObjectRetention { bucket, .. }
            | Self::PutObjectLegalHold { bucket, .. }
            | Self::CreateMultipartUpload { bucket, .. }
            | Self::CompleteMultipartUpload { bucket, .. }
            | Self::AbortMultipartUpload { bucket, .. } => Some(bucket),

            Self::PutPart { .. }
            | Self::DeleteParts { .. }
            | Self::UpdatePgOwnership { .. }
            | Self::UpdateAllPgOwnership { .. } => None,
        }
    }

    /// Returns the object key this command operates on, if any.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        match self {
            Self::PutObjectMetadata { metadata, .. } => Some(&metadata.key),
            Self::DeleteObject { key, .. }
            | Self::DeleteObjectVersion { key, .. }
            | Self::CreateDeleteMarker { key, .. }
            | Self::PutObjectTagging { key, .. }
            | Self::DeleteObjectTagging { key, .. }
            | Self::PutObjectRetention { key, .. }
            | Self::PutObjectLegalHold { key, .. }
            | Self::CreateMultipartUpload { key, .. }
            | Self::CompleteMultipartUpload { key, .. }
            | Self::AbortMultipartUpload { key, .. } => Some(key),

            _ => None,
        }
    }

    /// Returns the HLC timestamp for this command, if applicable.
    #[must_use]
    pub fn hlc(&self) -> Option<u64> {
        match self {
            Self::PutObjectMetadata { hlc, .. }
            | Self::DeleteObject { hlc, .. }
            | Self::DeleteObjectVersion { hlc, .. }
            | Self::CreateDeleteMarker { hlc, .. }
            | Self::CompleteMultipartUpload { hlc, .. } => Some(*hlc),

            _ => None,
        }
    }

    /// Returns a human-readable command type string.
    #[must_use]
    pub fn command_type(&self) -> &'static str {
        match self {
            Self::CreateBucket { .. } => "CreateBucket",
            Self::DeleteBucket { .. } => "DeleteBucket",
            Self::SetBucketVersioning { .. } => "SetBucketVersioning",
            Self::PutBucketCors { .. } => "PutBucketCors",
            Self::DeleteBucketCors { .. } => "DeleteBucketCors",
            Self::PutBucketPolicy { .. } => "PutBucketPolicy",
            Self::DeleteBucketPolicy { .. } => "DeleteBucketPolicy",
            Self::PutBucketTagging { .. } => "PutBucketTagging",
            Self::DeleteBucketTagging { .. } => "DeleteBucketTagging",
            Self::PutPublicAccessBlock { .. } => "PutPublicAccessBlock",
            Self::DeletePublicAccessBlock { .. } => "DeletePublicAccessBlock",
            Self::PutLifecycleConfiguration { .. } => "PutLifecycleConfiguration",
            Self::DeleteLifecycleConfiguration { .. } => "DeleteLifecycleConfiguration",
            Self::PutEncryptionConfiguration { .. } => "PutEncryptionConfiguration",
            Self::DeleteEncryptionConfiguration { .. } => "DeleteEncryptionConfiguration",
            Self::PutBucketLockConfig { .. } => "PutBucketLockConfig",
            Self::PutObjectMetadata { .. } => "PutObjectMetadata",
            Self::DeleteObject { .. } => "DeleteObject",
            Self::DeleteObjectVersion { .. } => "DeleteObjectVersion",
            Self::CreateDeleteMarker { .. } => "CreateDeleteMarker",
            Self::PutObjectTagging { .. } => "PutObjectTagging",
            Self::DeleteObjectTagging { .. } => "DeleteObjectTagging",
            Self::PutObjectRetention { .. } => "PutObjectRetention",
            Self::PutObjectLegalHold { .. } => "PutObjectLegalHold",
            Self::CreateMultipartUpload { .. } => "CreateMultipartUpload",
            Self::CompleteMultipartUpload { .. } => "CompleteMultipartUpload",
            Self::AbortMultipartUpload { .. } => "AbortMultipartUpload",
            Self::PutPart { .. } => "PutPart",
            Self::DeleteParts { .. } => "DeleteParts",
            Self::UpdatePgOwnership { .. } => "UpdatePgOwnership",
            Self::UpdateAllPgOwnership { .. } => "UpdateAllPgOwnership",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_command_serialization() {
        let cmd =
            MetadataCommand::CreateBucket { name: "test-bucket".to_string(), lock_enabled: false };

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: MetadataCommand = bincode::deserialize(&serialized).unwrap();

        assert!(matches!(
            deserialized,
            MetadataCommand::CreateBucket { name, lock_enabled }
            if name == "test-bucket" && !lock_enabled
        ));
    }

    #[test]
    fn test_bucket_accessor() {
        let cmd = MetadataCommand::DeleteBucket { name: "my-bucket".to_string() };
        assert_eq!(cmd.bucket(), Some("my-bucket"));

        let cmd = MetadataCommand::PutPart {
            upload_id: "upload-123".to_string(),
            part_number: 1,
            uuid: Uuid::nil(),
            size: 1024,
            etag: "\"abc\"".to_string(),
        };
        assert_eq!(cmd.bucket(), None);
    }

    #[test]
    fn test_command_type() {
        let cmd = MetadataCommand::CreateBucket { name: "test".to_string(), lock_enabled: false };
        assert_eq!(cmd.command_type(), "CreateBucket");
    }
}
