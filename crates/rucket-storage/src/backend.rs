//! Storage backend trait definition.

use std::collections::HashMap;

use bytes::Bytes;
use rucket_core::types::{BucketInfo, ETag, MultipartUpload, ObjectMetadata, Part, VersioningStatus};
use rucket_core::Result;

use crate::metadata::ListVersionsResult;

/// Result of a put_object operation.
#[derive(Debug, Clone)]
pub struct PutObjectResult {
    /// The ETag of the stored object.
    pub etag: ETag,
    /// The version ID if bucket versioning is enabled.
    pub version_id: Option<String>,
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
        content_type: Option<&str>,
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
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Result<ETag>;

    /// Abort a multipart upload, cleaning up all parts.
    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()>;

    /// List parts for a multipart upload.
    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> Result<Vec<Part>>;

    /// List all in-progress multipart uploads for a bucket.
    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>>;
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
