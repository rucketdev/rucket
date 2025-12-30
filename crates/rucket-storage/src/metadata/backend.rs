// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Metadata backend trait definition.

use async_trait::async_trait;
use rucket_core::types::{BucketInfo, MultipartUpload, ObjectMetadata, Part};
use rucket_core::Result;
use uuid::Uuid;

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
    ///
    /// # Errors
    ///
    /// Returns an error if the deletion cannot be performed.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Uuid>>;

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
    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
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

    /// Delete parts for an upload, returning their UUIDs for file cleanup.
    ///
    /// # Errors
    ///
    /// Returns `NotImplemented` error (stub).
    async fn delete_parts(&self, upload_id: &str) -> Result<Vec<Uuid>>;
}
