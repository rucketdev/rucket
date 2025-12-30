// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Storage backend trait definition.

use bytes::Bytes;
use rucket_core::types::{BucketInfo, ETag, ObjectMetadata};
use rucket_core::Result;

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

    // Object operations

    /// Store an object.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
    ) -> Result<ETag>;

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
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()>;

    /// Get object metadata without the data.
    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata>;

    /// Copy an object.
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
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
