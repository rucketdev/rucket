//! Metadata backend trait definition.

use std::collections::HashMap;

use async_trait::async_trait;
use rucket_core::encryption::ServerSideEncryptionConfiguration;
use rucket_core::lifecycle::LifecycleConfiguration;
use rucket_core::public_access_block::PublicAccessBlockConfiguration;
use rucket_core::types::{
    BucketInfo, CorsConfiguration, MultipartUpload, ObjectLockConfig, ObjectMetadata,
    ObjectRetention, Part, TagSet, VersioningStatus,
};
use rucket_core::Result;
use uuid::Uuid;

/// Result of a delete operation in a versioned bucket.
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// The version ID of the delete marker (if one was created).
    pub delete_marker_version_id: Option<String>,
    /// Whether this was a delete marker creation (vs. permanent delete).
    pub is_delete_marker: bool,
    /// The UUID of the deleted object's data file (if permanently deleted).
    pub deleted_uuid: Option<Uuid>,
}

/// A version entry for listing object versions.
#[derive(Debug, Clone)]
pub struct VersionEntry {
    /// The object metadata for this version.
    pub metadata: ObjectMetadata,
    /// Whether this is a delete marker.
    pub is_delete_marker: bool,
    /// Whether this is the latest version.
    pub is_latest: bool,
}

/// Result of listing object versions.
#[derive(Debug, Clone)]
pub struct ListVersionsResult {
    /// Object versions (including delete markers).
    pub versions: Vec<VersionEntry>,
    /// Common prefixes (when using delimiter).
    pub common_prefixes: Vec<String>,
    /// Whether there are more results.
    pub is_truncated: bool,
    /// Token for the next key (for pagination).
    pub next_key_marker: Option<String>,
    /// Token for the next version (for pagination).
    pub next_version_id_marker: Option<String>,
}

/// Trait for metadata storage backends.
///
/// This trait abstracts the metadata storage layer, allowing different
/// implementations (redb, file-based, etc.) to be used interchangeably.
///
/// All operations are async to support both blocking (via spawn_blocking)
/// and truly async backends.
#[async_trait]
pub trait MetadataBackend: Send + Sync + 'static {
    // === Bucket Operations ===

    /// Create a new bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket already exists or cannot be created.
    async fn create_bucket(&self, name: &str) -> Result<BucketInfo>;

    /// Delete a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist, is not empty,
    /// or cannot be deleted.
    async fn delete_bucket(&self, name: &str) -> Result<()>;

    /// Check if a bucket exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the check cannot be performed.
    async fn bucket_exists(&self, name: &str) -> Result<bool>;

    /// List all buckets ordered by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the list cannot be retrieved.
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;

    /// Get bucket info by name.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_bucket(&self, name: &str) -> Result<BucketInfo>;

    /// Set the versioning status for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist or the status cannot be set.
    async fn set_bucket_versioning(&self, name: &str, status: VersioningStatus) -> Result<()>;

    // === Bucket CORS Operations ===

    /// Get CORS configuration for a bucket.
    ///
    /// Returns `None` if no CORS configuration is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_bucket_cors(&self, name: &str) -> Result<Option<CorsConfiguration>>;

    /// Set CORS configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn put_bucket_cors(&self, name: &str, config: CorsConfiguration) -> Result<()>;

    /// Delete CORS configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn delete_bucket_cors(&self, name: &str) -> Result<()>;

    // === Object Operations ===

    /// Insert or update object metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist or the metadata
    /// cannot be stored.
    async fn put_object(&self, bucket: &str, meta: ObjectMetadata) -> Result<()>;

    /// Get object metadata by bucket and key.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist.
    async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata>;

    /// Delete object metadata.
    ///
    /// Returns the UUID of the deleted object if it existed, for file cleanup.
    /// This performs a permanent deletion (removes the metadata entry).
    ///
    /// # Errors
    ///
    /// Returns an error if the deletion cannot be performed.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Uuid>>;

    /// Get a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata>;

    /// Delete a specific version of an object permanently.
    ///
    /// Returns the UUID of the deleted version's data file for cleanup.
    ///
    /// # Errors
    ///
    /// Returns an error if the version does not exist or deletion fails.
    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<Uuid>>;

    /// Create a delete marker for a versioned object.
    ///
    /// This is used when deleting an object in a versioned bucket without specifying
    /// a version ID. Returns the version ID of the created delete marker.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist or the operation fails.
    async fn create_delete_marker(&self, bucket: &str, key: &str) -> Result<String>;

    /// List all versions of objects in a bucket.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `prefix` - Optional prefix to filter keys
    /// * `delimiter` - Optional delimiter for common prefix grouping
    /// * `key_marker` - Start listing after this key (for pagination)
    /// * `version_id_marker` - Start listing after this version (for pagination)
    /// * `max_keys` - Maximum number of keys to return
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist or the list cannot be retrieved.
    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: u32,
    ) -> Result<ListVersionsResult>;

    /// List objects in a bucket with optional prefix and pagination.
    ///
    /// # Arguments
    ///
    /// * `bucket` - The bucket name
    /// * `prefix` - Optional prefix to filter keys
    /// * `continuation_token` - Optional token for pagination (last key from previous page)
    /// * `max_keys` - Maximum number of keys to return
    ///
    /// # Returns
    ///
    /// A tuple of (objects, next_continuation_token). The token is `Some` if
    /// there are more results, `None` if this is the last page.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist or the list cannot be retrieved.
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<(Vec<ObjectMetadata>, Option<String>)>;

    // === Multipart Upload Operations (stubs for now) ===

    /// Create a new multipart upload.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    #[allow(clippy::too_many_arguments)]
    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        content_type: Option<&str>,
        user_metadata: HashMap<String, String>,
        cache_control: Option<&str>,
        content_disposition: Option<&str>,
        content_encoding: Option<&str>,
        content_language: Option<&str>,
        expires: Option<&str>,
    ) -> Result<MultipartUpload>;

    /// Get multipart upload info.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    async fn get_multipart_upload(&self, upload_id: &str) -> Result<MultipartUpload>;

    /// Delete a multipart upload and all its parts.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    async fn delete_multipart_upload(&self, upload_id: &str) -> Result<()>;

    /// List all multipart uploads for a bucket.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>>;

    /// Store a part for a multipart upload.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    async fn put_part(
        &self,
        upload_id: &str,
        part_number: u32,
        uuid: Uuid,
        size: u64,
        etag: &str,
    ) -> Result<Part>;

    /// List all parts for a multipart upload.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    async fn list_parts(&self, upload_id: &str) -> Result<Vec<Part>>;

    /// List all parts for a multipart upload with their UUIDs for file operations.
    ///
    /// # Errors
    ///
    /// Returns error if the upload does not exist.
    async fn list_parts_with_uuids(&self, upload_id: &str) -> Result<Vec<(Part, Uuid)>>;

    /// Delete parts for an upload, returning their UUIDs for file cleanup.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    async fn delete_parts(&self, upload_id: &str) -> Result<Vec<Uuid>>;

    /// Check if a UUID exists in the metadata (synchronous version for callbacks).
    ///
    /// This is used during recovery to check if a data file has corresponding metadata.
    /// Returns `false` if the check fails or the UUID doesn't exist.
    fn uuid_exists_sync(&self, bucket: &str, uuid: Uuid) -> bool;

    // === Object Tagging Operations ===

    /// Get tags for an object.
    ///
    /// Returns an empty TagSet if the object has no tags.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist.
    async fn get_object_tagging(&self, bucket: &str, key: &str) -> Result<TagSet>;

    /// Set tags for an object.
    ///
    /// Replaces any existing tags with the provided tags.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist.
    async fn put_object_tagging(&self, bucket: &str, key: &str, tags: TagSet) -> Result<()>;

    /// Delete tags for an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist.
    async fn delete_object_tagging(&self, bucket: &str, key: &str) -> Result<()>;

    /// Get tags for a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn get_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<TagSet>;

    /// Set tags for a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn put_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        tags: TagSet,
    ) -> Result<()>;

    /// Delete tags for a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn delete_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()>;

    // === Bucket Tagging Operations ===

    /// Get tags for a bucket.
    ///
    /// Returns an empty TagSet if the bucket has no tags.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_bucket_tagging(&self, bucket: &str) -> Result<TagSet>;

    /// Set tags for a bucket.
    ///
    /// Replaces any existing tags with the provided tags.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn put_bucket_tagging(&self, bucket: &str, tags: TagSet) -> Result<()>;

    /// Delete tags for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn delete_bucket_tagging(&self, bucket: &str) -> Result<()>;

    // === Object Lock Operations ===

    /// Get Object Lock configuration for a bucket.
    ///
    /// Returns `None` if Object Lock is not configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_bucket_lock_config(&self, bucket: &str) -> Result<Option<ObjectLockConfig>>;

    /// Set Object Lock configuration for a bucket.
    ///
    /// Once enabled, Object Lock cannot be disabled.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn put_bucket_lock_config(&self, bucket: &str, config: ObjectLockConfig) -> Result<()>;

    /// Get retention for an object.
    ///
    /// Returns `None` if no retention is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist.
    async fn get_object_retention(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectRetention>>;

    /// Set retention for an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist,
    /// or if retention cannot be shortened in Compliance mode.
    async fn put_object_retention(
        &self,
        bucket: &str,
        key: &str,
        retention: ObjectRetention,
    ) -> Result<()>;

    /// Get retention for a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn get_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectRetention>>;

    /// Set retention for a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn put_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        retention: ObjectRetention,
    ) -> Result<()>;

    /// Get legal hold status for an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist.
    async fn get_object_legal_hold(&self, bucket: &str, key: &str) -> Result<bool>;

    /// Set legal hold status for an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket or object does not exist.
    async fn put_object_legal_hold(&self, bucket: &str, key: &str, enabled: bool) -> Result<()>;

    /// Get legal hold status for a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn get_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool>;

    /// Set legal hold status for a specific version of an object.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket, object, or version does not exist.
    async fn put_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        enabled: bool,
    ) -> Result<()>;

    // === Bucket Policy Operations ===

    /// Get bucket policy for a bucket.
    ///
    /// Returns the raw JSON policy string.
    /// Returns `None` if no policy is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_bucket_policy(&self, bucket: &str) -> Result<Option<String>>;

    /// Set bucket policy for a bucket.
    ///
    /// Stores the raw JSON policy string.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn put_bucket_policy(&self, bucket: &str, policy_json: &str) -> Result<()>;

    /// Delete bucket policy for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn delete_bucket_policy(&self, bucket: &str) -> Result<()>;

    // === Public Access Block Operations ===

    /// Get Public Access Block configuration for a bucket.
    ///
    /// Returns `None` if no configuration is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_public_access_block(
        &self,
        bucket: &str,
    ) -> Result<Option<PublicAccessBlockConfiguration>>;

    /// Set Public Access Block configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn put_public_access_block(
        &self,
        bucket: &str,
        config: PublicAccessBlockConfiguration,
    ) -> Result<()>;

    /// Delete Public Access Block configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn delete_public_access_block(&self, bucket: &str) -> Result<()>;

    // === Lifecycle Configuration Operations ===

    /// Get Lifecycle Configuration for a bucket.
    ///
    /// Returns `None` if no configuration is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_lifecycle_configuration(
        &self,
        bucket: &str,
    ) -> Result<Option<LifecycleConfiguration>>;

    /// Set Lifecycle Configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn put_lifecycle_configuration(
        &self,
        bucket: &str,
        config: LifecycleConfiguration,
    ) -> Result<()>;

    /// Delete Lifecycle Configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn delete_lifecycle_configuration(&self, bucket: &str) -> Result<()>;

    // === Server-Side Encryption Configuration Operations ===

    /// Get Server-Side Encryption Configuration for a bucket.
    ///
    /// Returns `None` if no configuration is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn get_encryption_configuration(
        &self,
        bucket: &str,
    ) -> Result<Option<ServerSideEncryptionConfiguration>>;

    /// Set Server-Side Encryption Configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn put_encryption_configuration(
        &self,
        bucket: &str,
        config: ServerSideEncryptionConfiguration,
    ) -> Result<()>;

    /// Delete Server-Side Encryption Configuration for a bucket.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket does not exist.
    async fn delete_encryption_configuration(&self, bucket: &str) -> Result<()>;

    // === Placement Group Ownership Operations ===

    /// Update ownership for a single placement group.
    ///
    /// # Errors
    ///
    /// Returns an error if the update cannot be performed.
    async fn update_pg_ownership(
        &self,
        pg_id: u32,
        primary_node: u64,
        replica_nodes: Vec<u64>,
        epoch: u64,
    ) -> Result<()>;

    /// Update ownership for all placement groups.
    ///
    /// This replaces all existing PG ownership entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the update cannot be performed.
    async fn update_all_pg_ownership(
        &self,
        entries: Vec<PgOwnershipEntry>,
        epoch: u64,
    ) -> Result<u32>;

    /// Get ownership for a placement group.
    ///
    /// Returns `None` if no ownership is set for this PG.
    ///
    /// # Errors
    ///
    /// Returns an error if the query cannot be performed.
    async fn get_pg_ownership(&self, pg_id: u32) -> Result<Option<PgOwnershipEntry>>;

    /// Get the current PG ownership epoch.
    ///
    /// Returns 0 if no epoch is set.
    ///
    /// # Errors
    ///
    /// Returns an error if the query cannot be performed.
    async fn get_pg_epoch(&self) -> Result<u64>;

    /// List all PG ownership entries.
    ///
    /// # Errors
    ///
    /// Returns an error if the query cannot be performed.
    async fn list_pg_ownership(&self) -> Result<Vec<PgOwnershipEntry>>;
}

/// Placement group ownership entry.
///
/// Tracks which nodes are responsible for a placement group.
#[derive(Debug, Clone)]
pub struct PgOwnershipEntry {
    /// Placement group ID.
    pub pg_id: u32,
    /// Primary node ID (CRUSH device ID).
    pub primary_node: u64,
    /// Replica node IDs (CRUSH device IDs).
    pub replica_nodes: Vec<u64>,
}
