//! Storage backend trait definition.

use std::collections::HashMap;

use bytes::Bytes;
use rucket_core::encryption::ServerSideEncryptionConfiguration;
use rucket_core::lifecycle::LifecycleConfiguration;
use rucket_core::public_access_block::PublicAccessBlockConfiguration;
use rucket_core::replication::ReplicationConfiguration;
use rucket_core::types::{
    BucketInfo, BucketLoggingStatus, Checksum, ChecksumAlgorithm, CorsConfiguration, ETag,
    MultipartUpload, ObjectLockConfig, ObjectMetadata, ObjectRetention, Part, StorageClass, TagSet,
    VersioningStatus, WebsiteConfiguration,
};
use rucket_core::Result;

use crate::metadata::ListVersionsResult;

/// Result of a put_object operation.
#[derive(Debug, Clone)]
pub struct PutObjectResult {
    /// The ETag of the stored object.
    pub etag: ETag,
    /// The version ID if bucket versioning is enabled.
    pub version_id: Option<String>,
    /// The checksum if a checksum algorithm was requested.
    pub checksum: Option<Checksum>,
    /// Server-side encryption algorithm if encryption was applied.
    pub server_side_encryption: Option<String>,
    /// SSE-C algorithm if customer-provided encryption was used.
    pub sse_customer_algorithm: Option<String>,
    /// MD5 hash of the customer-provided encryption key.
    pub sse_customer_key_md5: Option<String>,
}

/// Result of a delete_object operation.
#[derive(Debug, Clone, Default)]
pub struct DeleteObjectResult {
    /// The version ID of the delete marker (if one was created).
    pub version_id: Option<String>,
    /// Whether a delete marker was created (vs. permanent delete).
    pub is_delete_marker: bool,
}

/// HTTP headers for object storage.
#[derive(Debug, Clone, Default)]
pub struct ObjectHeaders {
    /// Content-Type header.
    pub content_type: Option<String>,
    /// Cache-Control header.
    pub cache_control: Option<String>,
    /// Content-Disposition header.
    pub content_disposition: Option<String>,
    /// Content-Encoding header.
    pub content_encoding: Option<String>,
    /// Expires header.
    pub expires: Option<String>,
    /// Content-Language header.
    pub content_language: Option<String>,
    /// Checksum algorithm requested for this upload.
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
    /// Storage class for the object.
    pub storage_class: Option<StorageClass>,
    /// SSE-C algorithm if customer-provided encryption was used.
    pub sse_customer_algorithm: Option<String>,
    /// MD5 hash of the customer-provided encryption key (base64-encoded).
    pub sse_customer_key_md5: Option<String>,
    /// Encryption nonce for SSE-C (12 bytes for AES-256-GCM).
    pub encryption_nonce: Option<Vec<u8>>,
}

/// Trait for object storage backends.
#[allow(async_fn_in_trait)]
pub trait StorageBackend: Send + Sync {
    // Bucket operations

    /// Create a new bucket.
    async fn create_bucket(&self, name: &str) -> Result<BucketInfo>;

    /// Delete a bucket (must be empty).
    async fn delete_bucket(&self, name: &str) -> Result<()>;

    /// Check if a bucket exists.
    async fn head_bucket(&self, name: &str) -> Result<bool>;

    /// List all buckets.
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>>;

    /// Get bucket info by name.
    async fn get_bucket(&self, name: &str) -> Result<BucketInfo>;

    /// Set the versioning status for a bucket.
    async fn set_bucket_versioning(&self, name: &str, status: VersioningStatus) -> Result<()>;

    // Bucket CORS operations

    /// Get CORS configuration for a bucket.
    async fn get_bucket_cors(&self, name: &str) -> Result<Option<CorsConfiguration>>;

    /// Set CORS configuration for a bucket.
    async fn put_bucket_cors(&self, name: &str, config: CorsConfiguration) -> Result<()>;

    /// Delete CORS configuration for a bucket.
    async fn delete_bucket_cors(&self, name: &str) -> Result<()>;

    // Bucket policy operations

    /// Get bucket policy for a bucket.
    /// Returns the raw JSON policy string, or None if no policy is set.
    async fn get_bucket_policy(&self, name: &str) -> Result<Option<String>>;

    /// Set bucket policy for a bucket.
    async fn put_bucket_policy(&self, name: &str, policy_json: &str) -> Result<()>;

    /// Delete bucket policy for a bucket.
    async fn delete_bucket_policy(&self, name: &str) -> Result<()>;

    // Public Access Block operations

    /// Get Public Access Block configuration for a bucket.
    async fn get_public_access_block(
        &self,
        name: &str,
    ) -> Result<Option<PublicAccessBlockConfiguration>>;

    /// Set Public Access Block configuration for a bucket.
    async fn put_public_access_block(
        &self,
        name: &str,
        config: PublicAccessBlockConfiguration,
    ) -> Result<()>;

    /// Delete Public Access Block configuration for a bucket.
    async fn delete_public_access_block(&self, name: &str) -> Result<()>;

    // Lifecycle Configuration operations

    /// Get Lifecycle Configuration for a bucket.
    async fn get_lifecycle_configuration(
        &self,
        name: &str,
    ) -> Result<Option<LifecycleConfiguration>>;

    /// Set Lifecycle Configuration for a bucket.
    async fn put_lifecycle_configuration(
        &self,
        name: &str,
        config: LifecycleConfiguration,
    ) -> Result<()>;

    /// Delete Lifecycle Configuration for a bucket.
    async fn delete_lifecycle_configuration(&self, name: &str) -> Result<()>;

    // Server-Side Encryption Configuration operations

    /// Get Server-Side Encryption Configuration for a bucket.
    async fn get_encryption_configuration(
        &self,
        name: &str,
    ) -> Result<Option<ServerSideEncryptionConfiguration>>;

    /// Set Server-Side Encryption Configuration for a bucket.
    async fn put_encryption_configuration(
        &self,
        name: &str,
        config: ServerSideEncryptionConfiguration,
    ) -> Result<()>;

    /// Delete Server-Side Encryption Configuration for a bucket.
    async fn delete_encryption_configuration(&self, name: &str) -> Result<()>;

    // Replication Configuration operations

    /// Get Replication Configuration for a bucket.
    async fn get_replication_configuration(
        &self,
        name: &str,
    ) -> Result<Option<ReplicationConfiguration>>;

    /// Set Replication Configuration for a bucket.
    async fn put_replication_configuration(
        &self,
        name: &str,
        config: ReplicationConfiguration,
    ) -> Result<()>;

    /// Delete Replication Configuration for a bucket.
    async fn delete_replication_configuration(&self, name: &str) -> Result<()>;

    // Website Configuration operations

    /// Get Website Configuration for a bucket.
    async fn get_bucket_website(&self, name: &str) -> Result<Option<WebsiteConfiguration>>;

    /// Set Website Configuration for a bucket.
    async fn put_bucket_website(&self, name: &str, config: WebsiteConfiguration) -> Result<()>;

    /// Delete Website Configuration for a bucket.
    async fn delete_bucket_website(&self, name: &str) -> Result<()>;

    // Logging Configuration operations

    /// Get Logging Configuration for a bucket.
    async fn get_bucket_logging(&self, name: &str) -> Result<Option<BucketLoggingStatus>>;

    /// Set Logging Configuration for a bucket.
    async fn put_bucket_logging(&self, name: &str, config: BucketLoggingStatus) -> Result<()>;

    /// Delete Logging Configuration for a bucket.
    async fn delete_bucket_logging(&self, name: &str) -> Result<()>;

    // Object operations

    /// Store an object.
    ///
    /// Returns the ETag and version ID (if bucket versioning is enabled).
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        headers: ObjectHeaders,
        user_metadata: HashMap<String, String>,
    ) -> Result<PutObjectResult>;

    /// Retrieve an object.
    async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMetadata, Bytes)>;

    /// Retrieve a range of bytes from an object.
    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<(ObjectMetadata, Bytes)>;

    /// Delete an object.
    ///
    /// For versioned buckets, this creates a delete marker and returns its version ID.
    /// For non-versioned buckets, this permanently deletes the object.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<DeleteObjectResult>;

    /// Get object metadata without the data.
    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata>;

    // === Versioning Operations ===

    /// Retrieve a specific version of an object.
    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectMetadata, Bytes)>;

    /// Get metadata for a specific version of an object.
    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata>;

    /// Delete a specific version of an object permanently.
    ///
    /// This always performs a permanent deletion, regardless of versioning status.
    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<DeleteObjectResult>;

    /// List all versions of objects in a bucket.
    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: u32,
    ) -> Result<ListVersionsResult>;

    /// Copy an object.
    ///
    /// If `new_headers` and `new_metadata` are provided, they replace the source object's
    /// headers and metadata (used for MetadataDirective=REPLACE). If None, the source
    /// object's headers and metadata are preserved (MetadataDirective=COPY).
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        new_headers: Option<ObjectHeaders>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<ETag>;

    /// List objects in a bucket.
    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<ListObjectsResult>;

    // Multipart upload operations

    /// Initiate a multipart upload.
    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        headers: ObjectHeaders,
        user_metadata: HashMap<String, String>,
    ) -> Result<MultipartUpload>;

    /// Upload a part for a multipart upload.
    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<ETag>;

    /// Complete a multipart upload by combining all parts.
    ///
    /// If `sse_c_key` is provided, the assembled object will be encrypted with SSE-C.
    /// The key must be exactly 32 bytes (256 bits) for AES-256.
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
        sse_c_key: Option<&[u8; 32]>,
    ) -> Result<ETag>;

    /// Abort a multipart upload, cleaning up all parts.
    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()>;

    /// List parts for a multipart upload.
    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> Result<Vec<Part>>;

    /// List all in-progress multipart uploads for a bucket.
    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>>;

    /// Get a specific multipart upload by upload ID.
    async fn get_multipart_upload(&self, upload_id: &str) -> Result<MultipartUpload>;

    // Object tagging operations

    /// Get tags for an object.
    async fn get_object_tagging(&self, bucket: &str, key: &str) -> Result<TagSet>;

    /// Set tags for an object.
    async fn put_object_tagging(&self, bucket: &str, key: &str, tags: TagSet) -> Result<()>;

    /// Delete tags for an object.
    async fn delete_object_tagging(&self, bucket: &str, key: &str) -> Result<()>;

    /// Get tags for a specific version of an object.
    async fn get_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<TagSet>;

    /// Set tags for a specific version of an object.
    async fn put_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        tags: TagSet,
    ) -> Result<()>;

    /// Delete tags for a specific version of an object.
    async fn delete_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()>;

    // Bucket tagging operations

    /// Get tags for a bucket.
    async fn get_bucket_tagging(&self, bucket: &str) -> Result<TagSet>;

    /// Set tags for a bucket.
    async fn put_bucket_tagging(&self, bucket: &str, tags: TagSet) -> Result<()>;

    /// Delete tags for a bucket.
    async fn delete_bucket_tagging(&self, bucket: &str) -> Result<()>;

    // Object Lock operations

    /// Get Object Lock configuration for a bucket.
    async fn get_bucket_lock_config(&self, bucket: &str) -> Result<Option<ObjectLockConfig>>;

    /// Set Object Lock configuration for a bucket.
    async fn put_bucket_lock_config(&self, bucket: &str, config: ObjectLockConfig) -> Result<()>;

    /// Get retention for an object.
    async fn get_object_retention(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectRetention>>;

    /// Set retention for an object.
    async fn put_object_retention(
        &self,
        bucket: &str,
        key: &str,
        retention: ObjectRetention,
    ) -> Result<()>;

    /// Get retention for a specific version of an object.
    async fn get_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectRetention>>;

    /// Set retention for a specific version of an object.
    async fn put_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        retention: ObjectRetention,
    ) -> Result<()>;

    /// Get legal hold status for an object.
    async fn get_object_legal_hold(&self, bucket: &str, key: &str) -> Result<bool>;

    /// Set legal hold status for an object.
    async fn put_object_legal_hold(&self, bucket: &str, key: &str, enabled: bool) -> Result<()>;

    /// Get legal hold status for a specific version of an object.
    async fn get_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool>;

    /// Set legal hold status for a specific version of an object.
    async fn put_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        enabled: bool,
    ) -> Result<()>;
}

/// Result of listing objects.
#[derive(Debug, Clone)]
pub struct ListObjectsResult {
    /// Objects matching the criteria.
    pub objects: Vec<ObjectMetadata>,
    /// Common prefixes (when using delimiter).
    pub common_prefixes: Vec<String>,
    /// Whether there are more results.
    pub is_truncated: bool,
    /// Token for the next page (if truncated).
    pub next_continuation_token: Option<String>,
}
