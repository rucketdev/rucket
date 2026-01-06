//! Replicated storage backend wrapper.
//!
//! This module provides a storage backend that replicates write operations
//! to backup nodes based on the configured replication level.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use rucket_core::encryption::ServerSideEncryptionConfiguration;
use rucket_core::hlc::HlcClock;
use rucket_core::lifecycle::LifecycleConfiguration;
use rucket_core::public_access_block::PublicAccessBlockConfiguration;
use rucket_core::types::{
    BucketInfo, CorsConfiguration, ETag, MultipartUpload, ObjectLockConfig, ObjectMetadata,
    ObjectRetention, Part, TagSet, VersioningStatus,
};
use rucket_core::Result;
use rucket_replication::{
    NoOpReplicator, ReplicationConfig, ReplicationEntry, ReplicationLevel, ReplicationOperation,
    Replicator,
};

use crate::backend::{
    DeleteObjectResult, ListObjectsResult, ObjectHeaders, PutObjectResult, StorageBackend,
};
use crate::metadata::ListVersionsResult;

/// A storage backend that replicates write operations to backup nodes.
///
/// This wrapper intercepts all write operations (put_object, delete_object,
/// create_bucket, delete_bucket) and replicates them according to the
/// configured replication level.
///
/// # Replication Levels
///
/// - **Local**: No replication, writes are acknowledged after local persistence
/// - **Replicated**: Async replication, writes are acknowledged immediately
/// - **Durable**: Sync replication with quorum acknowledgment
pub struct ReplicatedStorage<S: StorageBackend> {
    /// The underlying storage backend.
    inner: S,
    /// The replicator for sending writes to backup nodes.
    replicator: Arc<dyn Replicator>,
    /// HLC clock for generating timestamps.
    clock: Arc<HlcClock>,
    /// Default replication level for write operations.
    default_level: ReplicationLevel,
}

impl<S: StorageBackend> ReplicatedStorage<S> {
    /// Creates a new replicated storage with no replication (single-node mode).
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            replicator: Arc::new(NoOpReplicator),
            clock: Arc::new(HlcClock::new()),
            default_level: ReplicationLevel::Local,
        }
    }

    /// Creates a new replicated storage with the specified replicator.
    pub fn with_replicator(
        inner: S,
        replicator: Arc<dyn Replicator>,
        config: &ReplicationConfig,
    ) -> Self {
        Self {
            inner,
            replicator,
            clock: Arc::new(HlcClock::new()),
            default_level: config.default_level,
        }
    }

    /// Sets the default replication level for write operations.
    pub fn set_default_level(&mut self, level: ReplicationLevel) {
        self.default_level = level;
    }

    /// Returns the default replication level.
    pub fn default_level(&self) -> ReplicationLevel {
        self.default_level
    }

    /// Returns a reference to the underlying storage backend.
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Returns a reference to the replicator.
    pub fn replicator(&self) -> &dyn Replicator {
        self.replicator.as_ref()
    }

    /// Replicates a put object operation.
    async fn replicate_put(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        data: &Bytes,
        content_type: Option<&str>,
        etag: &ETag,
    ) -> Result<()> {
        if !self.default_level.replicates() {
            return Ok(());
        }

        let hlc = self.clock.now().as_raw();
        let entry = ReplicationEntry::new(
            ReplicationOperation::PutObject {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version_id.map(String::from),
                data: data.clone(),
                content_type: content_type.map(String::from),
                etag: etag.to_string(),
            },
            hlc,
            self.default_level,
        );

        let result = self.replicator.replicate(entry).await.map_err(|e| {
            rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!("Replication failed: {e}"),
            )
        })?;

        if self.default_level.is_synchronous() && !result.quorum_achieved {
            return Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!(
                    "Quorum not achieved: {} of {} required",
                    result.acks_received, result.acks_required
                ),
            ));
        }

        Ok(())
    }

    /// Replicates a delete object operation.
    async fn replicate_delete(
        &self,
        bucket: &str,
        key: &str,
        version_id: Option<&str>,
        is_delete_marker: bool,
    ) -> Result<()> {
        if !self.default_level.replicates() {
            return Ok(());
        }

        let hlc = self.clock.now().as_raw();
        let entry = ReplicationEntry::new(
            ReplicationOperation::DeleteObject {
                bucket: bucket.to_string(),
                key: key.to_string(),
                version_id: version_id.map(String::from),
                is_delete_marker,
            },
            hlc,
            self.default_level,
        );

        let result = self.replicator.replicate(entry).await.map_err(|e| {
            rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!("Replication failed: {e}"),
            )
        })?;

        if self.default_level.is_synchronous() && !result.quorum_achieved {
            return Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!(
                    "Quorum not achieved: {} of {} required",
                    result.acks_received, result.acks_required
                ),
            ));
        }

        Ok(())
    }

    /// Replicates a create bucket operation.
    async fn replicate_create_bucket(&self, bucket: &str) -> Result<()> {
        if !self.default_level.replicates() {
            return Ok(());
        }

        let hlc = self.clock.now().as_raw();
        let entry = ReplicationEntry::new(
            ReplicationOperation::CreateBucket { bucket: bucket.to_string() },
            hlc,
            self.default_level,
        );

        let result = self.replicator.replicate(entry).await.map_err(|e| {
            rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!("Replication failed: {e}"),
            )
        })?;

        if self.default_level.is_synchronous() && !result.quorum_achieved {
            return Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!(
                    "Quorum not achieved: {} of {} required",
                    result.acks_received, result.acks_required
                ),
            ));
        }

        Ok(())
    }

    /// Replicates a delete bucket operation.
    async fn replicate_delete_bucket(&self, bucket: &str) -> Result<()> {
        if !self.default_level.replicates() {
            return Ok(());
        }

        let hlc = self.clock.now().as_raw();
        let entry = ReplicationEntry::new(
            ReplicationOperation::DeleteBucket { bucket: bucket.to_string() },
            hlc,
            self.default_level,
        );

        let result = self.replicator.replicate(entry).await.map_err(|e| {
            rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!("Replication failed: {e}"),
            )
        })?;

        if self.default_level.is_synchronous() && !result.quorum_achieved {
            return Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::InternalError,
                format!(
                    "Quorum not achieved: {} of {} required",
                    result.acks_received, result.acks_required
                ),
            ));
        }

        Ok(())
    }
}

impl<S: StorageBackend> StorageBackend for ReplicatedStorage<S> {
    // Bucket operations with replication

    async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        let result = self.inner.create_bucket(name).await?;
        self.replicate_create_bucket(name).await?;
        Ok(result)
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.inner.delete_bucket(name).await?;
        self.replicate_delete_bucket(name).await?;
        Ok(())
    }

    // Object operations with replication

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        headers: ObjectHeaders,
        user_metadata: HashMap<String, String>,
    ) -> Result<PutObjectResult> {
        let result = self
            .inner
            .put_object(bucket, key, data.clone(), headers.clone(), user_metadata)
            .await?;
        self.replicate_put(
            bucket,
            key,
            result.version_id.as_deref(),
            &data,
            headers.content_type.as_deref(),
            &result.etag,
        )
        .await?;
        Ok(result)
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<DeleteObjectResult> {
        let result = self.inner.delete_object(bucket, key).await?;
        self.replicate_delete(bucket, key, result.version_id.as_deref(), result.is_delete_marker)
            .await?;
        Ok(result)
    }

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<DeleteObjectResult> {
        let result = self.inner.delete_object_version(bucket, key, version_id).await?;
        self.replicate_delete(bucket, key, Some(version_id), result.is_delete_marker).await?;
        Ok(result)
    }

    // Read operations pass through without replication

    async fn head_bucket(&self, name: &str) -> Result<bool> {
        self.inner.head_bucket(name).await
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.inner.list_buckets().await
    }

    async fn get_bucket(&self, name: &str) -> Result<BucketInfo> {
        self.inner.get_bucket(name).await
    }

    async fn set_bucket_versioning(&self, name: &str, status: VersioningStatus) -> Result<()> {
        self.inner.set_bucket_versioning(name, status).await
    }

    async fn get_bucket_cors(&self, name: &str) -> Result<Option<CorsConfiguration>> {
        self.inner.get_bucket_cors(name).await
    }

    async fn put_bucket_cors(&self, name: &str, config: CorsConfiguration) -> Result<()> {
        self.inner.put_bucket_cors(name, config).await
    }

    async fn delete_bucket_cors(&self, name: &str) -> Result<()> {
        self.inner.delete_bucket_cors(name).await
    }

    async fn get_bucket_policy(&self, name: &str) -> Result<Option<String>> {
        self.inner.get_bucket_policy(name).await
    }

    async fn put_bucket_policy(&self, name: &str, policy_json: &str) -> Result<()> {
        self.inner.put_bucket_policy(name, policy_json).await
    }

    async fn delete_bucket_policy(&self, name: &str) -> Result<()> {
        self.inner.delete_bucket_policy(name).await
    }

    async fn get_public_access_block(
        &self,
        name: &str,
    ) -> Result<Option<PublicAccessBlockConfiguration>> {
        self.inner.get_public_access_block(name).await
    }

    async fn put_public_access_block(
        &self,
        name: &str,
        config: PublicAccessBlockConfiguration,
    ) -> Result<()> {
        self.inner.put_public_access_block(name, config).await
    }

    async fn delete_public_access_block(&self, name: &str) -> Result<()> {
        self.inner.delete_public_access_block(name).await
    }

    async fn get_lifecycle_configuration(
        &self,
        name: &str,
    ) -> Result<Option<LifecycleConfiguration>> {
        self.inner.get_lifecycle_configuration(name).await
    }

    async fn put_lifecycle_configuration(
        &self,
        name: &str,
        config: LifecycleConfiguration,
    ) -> Result<()> {
        self.inner.put_lifecycle_configuration(name, config).await
    }

    async fn delete_lifecycle_configuration(&self, name: &str) -> Result<()> {
        self.inner.delete_lifecycle_configuration(name).await
    }

    async fn get_encryption_configuration(
        &self,
        name: &str,
    ) -> Result<Option<ServerSideEncryptionConfiguration>> {
        self.inner.get_encryption_configuration(name).await
    }

    async fn put_encryption_configuration(
        &self,
        name: &str,
        config: ServerSideEncryptionConfiguration,
    ) -> Result<()> {
        self.inner.put_encryption_configuration(name, config).await
    }

    async fn delete_encryption_configuration(&self, name: &str) -> Result<()> {
        self.inner.delete_encryption_configuration(name).await
    }

    async fn get_replication_configuration(
        &self,
        name: &str,
    ) -> Result<Option<rucket_core::replication::ReplicationConfiguration>> {
        self.inner.get_replication_configuration(name).await
    }

    async fn put_replication_configuration(
        &self,
        name: &str,
        config: rucket_core::replication::ReplicationConfiguration,
    ) -> Result<()> {
        self.inner.put_replication_configuration(name, config).await
    }

    async fn delete_replication_configuration(&self, name: &str) -> Result<()> {
        self.inner.delete_replication_configuration(name).await
    }

    async fn get_bucket_website(
        &self,
        name: &str,
    ) -> Result<Option<rucket_core::types::WebsiteConfiguration>> {
        self.inner.get_bucket_website(name).await
    }

    async fn put_bucket_website(
        &self,
        name: &str,
        config: rucket_core::types::WebsiteConfiguration,
    ) -> Result<()> {
        self.inner.put_bucket_website(name, config).await
    }

    async fn delete_bucket_website(&self, name: &str) -> Result<()> {
        self.inner.delete_bucket_website(name).await
    }

    async fn get_bucket_logging(
        &self,
        name: &str,
    ) -> Result<Option<rucket_core::types::BucketLoggingStatus>> {
        self.inner.get_bucket_logging(name).await
    }

    async fn put_bucket_logging(
        &self,
        name: &str,
        config: rucket_core::types::BucketLoggingStatus,
    ) -> Result<()> {
        self.inner.put_bucket_logging(name, config).await
    }

    async fn delete_bucket_logging(&self, name: &str) -> Result<()> {
        self.inner.delete_bucket_logging(name).await
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMetadata, Bytes)> {
        self.inner.get_object(bucket, key).await
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<(ObjectMetadata, Bytes)> {
        self.inner.get_object_range(bucket, key, start, end).await
    }

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectMetadata, Bytes)> {
        self.inner.get_object_version(bucket, key, version_id).await
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        self.inner.head_object(bucket, key).await
    }

    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata> {
        self.inner.head_object_version(bucket, key, version_id).await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<ListObjectsResult> {
        self.inner.list_objects(bucket, prefix, delimiter, continuation_token, max_keys).await
    }

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: u32,
    ) -> Result<ListVersionsResult> {
        self.inner
            .list_object_versions(
                bucket,
                prefix,
                delimiter,
                key_marker,
                version_id_marker,
                max_keys,
            )
            .await
    }

    async fn get_object_tagging(&self, bucket: &str, key: &str) -> Result<TagSet> {
        self.inner.get_object_tagging(bucket, key).await
    }

    async fn get_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<TagSet> {
        self.inner.get_object_tagging_version(bucket, key, version_id).await
    }

    async fn put_object_tagging(&self, bucket: &str, key: &str, tags: TagSet) -> Result<()> {
        self.inner.put_object_tagging(bucket, key, tags).await
    }

    async fn put_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        tags: TagSet,
    ) -> Result<()> {
        self.inner.put_object_tagging_version(bucket, key, version_id, tags).await
    }

    async fn delete_object_tagging(&self, bucket: &str, key: &str) -> Result<()> {
        self.inner.delete_object_tagging(bucket, key).await
    }

    async fn delete_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()> {
        self.inner.delete_object_tagging_version(bucket, key, version_id).await
    }

    async fn get_bucket_tagging(&self, bucket: &str) -> Result<TagSet> {
        self.inner.get_bucket_tagging(bucket).await
    }

    async fn put_bucket_tagging(&self, bucket: &str, tags: TagSet) -> Result<()> {
        self.inner.put_bucket_tagging(bucket, tags).await
    }

    async fn delete_bucket_tagging(&self, bucket: &str) -> Result<()> {
        self.inner.delete_bucket_tagging(bucket).await
    }

    async fn get_bucket_lock_config(&self, bucket: &str) -> Result<Option<ObjectLockConfig>> {
        self.inner.get_bucket_lock_config(bucket).await
    }

    async fn put_bucket_lock_config(&self, bucket: &str, config: ObjectLockConfig) -> Result<()> {
        self.inner.put_bucket_lock_config(bucket, config).await
    }

    async fn get_object_retention(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectRetention>> {
        self.inner.get_object_retention(bucket, key).await
    }

    async fn get_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectRetention>> {
        self.inner.get_object_retention_version(bucket, key, version_id).await
    }

    async fn put_object_retention(
        &self,
        bucket: &str,
        key: &str,
        retention: ObjectRetention,
    ) -> Result<()> {
        self.inner.put_object_retention(bucket, key, retention).await
    }

    async fn put_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        retention: ObjectRetention,
    ) -> Result<()> {
        self.inner.put_object_retention_version(bucket, key, version_id, retention).await
    }

    async fn get_object_legal_hold(&self, bucket: &str, key: &str) -> Result<bool> {
        self.inner.get_object_legal_hold(bucket, key).await
    }

    async fn get_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool> {
        self.inner.get_object_legal_hold_version(bucket, key, version_id).await
    }

    async fn put_object_legal_hold(&self, bucket: &str, key: &str, enabled: bool) -> Result<()> {
        self.inner.put_object_legal_hold(bucket, key, enabled).await
    }

    async fn put_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        enabled: bool,
    ) -> Result<()> {
        self.inner.put_object_legal_hold_version(bucket, key, version_id, enabled).await
    }

    // Multipart upload operations pass through

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        headers: ObjectHeaders,
        user_metadata: HashMap<String, String>,
    ) -> Result<MultipartUpload> {
        self.inner.create_multipart_upload(bucket, key, headers, user_metadata).await
    }

    async fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<ETag> {
        self.inner.upload_part(bucket, key, upload_id, part_number, data).await
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
        sse_c_key: Option<&[u8; 32]>,
    ) -> Result<ETag> {
        // Note: The completed multipart should be replicated
        // For now, just pass through - full multipart replication is complex
        self.inner.complete_multipart_upload(bucket, key, upload_id, parts, sse_c_key).await
    }

    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        self.inner.abort_multipart_upload(bucket, key, upload_id).await
    }

    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> Result<Vec<Part>> {
        self.inner.list_parts(bucket, key, upload_id).await
    }

    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>> {
        self.inner.list_multipart_uploads(bucket).await
    }

    async fn get_multipart_upload(&self, upload_id: &str) -> Result<MultipartUpload> {
        self.inner.get_multipart_upload(upload_id).await
    }

    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
        new_headers: Option<ObjectHeaders>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<ETag> {
        // Note: Copy should be replicated as a put on the destination
        // For now, just pass through
        self.inner
            .copy_object(src_bucket, src_key, dst_bucket, dst_key, new_headers, new_metadata)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex as StdMutex;

    use async_trait::async_trait;
    use rucket_replication::{ReplicaInfo, ReplicationResult};

    use super::*;

    /// Mock storage backend for testing.
    struct MockStorage {
        puts: AtomicUsize,
        deletes: AtomicUsize,
        creates: AtomicUsize,
    }

    impl MockStorage {
        fn new() -> Self {
            Self {
                puts: AtomicUsize::new(0),
                deletes: AtomicUsize::new(0),
                creates: AtomicUsize::new(0),
            }
        }
    }

    #[allow(unused_variables)]
    impl StorageBackend for MockStorage {
        async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
            self.creates.fetch_add(1, Ordering::SeqCst);
            Ok(BucketInfo::new(name))
        }

        async fn delete_bucket(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn head_bucket(&self, name: &str) -> Result<bool> {
            Ok(true)
        }

        async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
            Ok(vec![])
        }

        async fn get_bucket(&self, name: &str) -> Result<BucketInfo> {
            Ok(BucketInfo::new(name))
        }

        async fn set_bucket_versioning(&self, name: &str, status: VersioningStatus) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_cors(&self, name: &str) -> Result<Option<CorsConfiguration>> {
            Ok(None)
        }

        async fn put_bucket_cors(&self, name: &str, config: CorsConfiguration) -> Result<()> {
            Ok(())
        }

        async fn delete_bucket_cors(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_policy(&self, name: &str) -> Result<Option<String>> {
            Ok(None)
        }

        async fn put_bucket_policy(&self, name: &str, policy_json: &str) -> Result<()> {
            Ok(())
        }

        async fn delete_bucket_policy(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_public_access_block(
            &self,
            name: &str,
        ) -> Result<Option<PublicAccessBlockConfiguration>> {
            Ok(None)
        }

        async fn put_public_access_block(
            &self,
            name: &str,
            config: PublicAccessBlockConfiguration,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_public_access_block(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_lifecycle_configuration(
            &self,
            name: &str,
        ) -> Result<Option<LifecycleConfiguration>> {
            Ok(None)
        }

        async fn put_lifecycle_configuration(
            &self,
            name: &str,
            config: LifecycleConfiguration,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_lifecycle_configuration(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_encryption_configuration(
            &self,
            name: &str,
        ) -> Result<Option<ServerSideEncryptionConfiguration>> {
            Ok(None)
        }

        async fn put_encryption_configuration(
            &self,
            name: &str,
            config: ServerSideEncryptionConfiguration,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_encryption_configuration(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_replication_configuration(
            &self,
            name: &str,
        ) -> Result<Option<rucket_core::replication::ReplicationConfiguration>> {
            Ok(None)
        }

        async fn put_replication_configuration(
            &self,
            name: &str,
            config: rucket_core::replication::ReplicationConfiguration,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_replication_configuration(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_website(
            &self,
            name: &str,
        ) -> Result<Option<rucket_core::types::WebsiteConfiguration>> {
            Ok(None)
        }

        async fn put_bucket_website(
            &self,
            name: &str,
            config: rucket_core::types::WebsiteConfiguration,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_bucket_website(&self, name: &str) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_logging(
            &self,
            _name: &str,
        ) -> Result<Option<rucket_core::types::BucketLoggingStatus>> {
            Ok(None)
        }

        async fn put_bucket_logging(
            &self,
            _name: &str,
            _config: rucket_core::types::BucketLoggingStatus,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_bucket_logging(&self, _name: &str) -> Result<()> {
            Ok(())
        }

        async fn put_object(
            &self,
            bucket: &str,
            key: &str,
            data: Bytes,
            headers: ObjectHeaders,
            user_metadata: HashMap<String, String>,
        ) -> Result<PutObjectResult> {
            self.puts.fetch_add(1, Ordering::SeqCst);
            Ok(PutObjectResult {
                etag: ETag::from("\"test-etag\"".to_string()),
                version_id: None,
                checksum: None,
                server_side_encryption: None,
                sse_customer_algorithm: None,
                sse_customer_key_md5: None,
            })
        }

        async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMetadata, Bytes)> {
            Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::NoSuchKey,
                "Not found",
            ))
        }

        async fn get_object_range(
            &self,
            bucket: &str,
            key: &str,
            start: u64,
            end: u64,
        ) -> Result<(ObjectMetadata, Bytes)> {
            Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::NoSuchKey,
                "Not found",
            ))
        }

        async fn get_object_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
        ) -> Result<(ObjectMetadata, Bytes)> {
            Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::NoSuchKey,
                "Not found",
            ))
        }

        async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
            Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::NoSuchKey,
                "Not found",
            ))
        }

        async fn head_object_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
        ) -> Result<ObjectMetadata> {
            Err(rucket_core::error::Error::s3(
                rucket_core::error::S3ErrorCode::NoSuchKey,
                "Not found",
            ))
        }

        async fn delete_object(&self, bucket: &str, key: &str) -> Result<DeleteObjectResult> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            Ok(DeleteObjectResult::default())
        }

        async fn delete_object_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
        ) -> Result<DeleteObjectResult> {
            self.deletes.fetch_add(1, Ordering::SeqCst);
            Ok(DeleteObjectResult::default())
        }

        async fn list_objects(
            &self,
            bucket: &str,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            continuation_token: Option<&str>,
            max_keys: u32,
        ) -> Result<ListObjectsResult> {
            Ok(ListObjectsResult {
                is_truncated: false,
                objects: vec![],
                common_prefixes: vec![],
                next_continuation_token: None,
            })
        }

        async fn list_object_versions(
            &self,
            bucket: &str,
            prefix: Option<&str>,
            delimiter: Option<&str>,
            key_marker: Option<&str>,
            version_id_marker: Option<&str>,
            max_keys: u32,
        ) -> Result<ListVersionsResult> {
            Ok(ListVersionsResult {
                is_truncated: false,
                versions: vec![],
                common_prefixes: vec![],
                next_key_marker: None,
                next_version_id_marker: None,
            })
        }

        async fn get_object_tagging(&self, bucket: &str, key: &str) -> Result<TagSet> {
            Ok(TagSet::default())
        }

        async fn get_object_tagging_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
        ) -> Result<TagSet> {
            Ok(TagSet::default())
        }

        async fn put_object_tagging(&self, bucket: &str, key: &str, tags: TagSet) -> Result<()> {
            Ok(())
        }

        async fn put_object_tagging_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
            tags: TagSet,
        ) -> Result<()> {
            Ok(())
        }

        async fn delete_object_tagging(&self, bucket: &str, key: &str) -> Result<()> {
            Ok(())
        }

        async fn delete_object_tagging_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
        ) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_tagging(&self, bucket: &str) -> Result<TagSet> {
            Ok(TagSet::default())
        }

        async fn put_bucket_tagging(&self, bucket: &str, tags: TagSet) -> Result<()> {
            Ok(())
        }

        async fn delete_bucket_tagging(&self, bucket: &str) -> Result<()> {
            Ok(())
        }

        async fn get_bucket_lock_config(&self, bucket: &str) -> Result<Option<ObjectLockConfig>> {
            Ok(None)
        }

        async fn put_bucket_lock_config(
            &self,
            bucket: &str,
            config: ObjectLockConfig,
        ) -> Result<()> {
            Ok(())
        }

        async fn get_object_retention(
            &self,
            bucket: &str,
            key: &str,
        ) -> Result<Option<ObjectRetention>> {
            Ok(None)
        }

        async fn get_object_retention_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
        ) -> Result<Option<ObjectRetention>> {
            Ok(None)
        }

        async fn put_object_retention(
            &self,
            bucket: &str,
            key: &str,
            retention: ObjectRetention,
        ) -> Result<()> {
            Ok(())
        }

        async fn put_object_retention_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
            retention: ObjectRetention,
        ) -> Result<()> {
            Ok(())
        }

        async fn get_object_legal_hold(&self, bucket: &str, key: &str) -> Result<bool> {
            Ok(false)
        }

        async fn get_object_legal_hold_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
        ) -> Result<bool> {
            Ok(false)
        }

        async fn put_object_legal_hold(
            &self,
            bucket: &str,
            key: &str,
            enabled: bool,
        ) -> Result<()> {
            Ok(())
        }

        async fn put_object_legal_hold_version(
            &self,
            bucket: &str,
            key: &str,
            version_id: &str,
            enabled: bool,
        ) -> Result<()> {
            Ok(())
        }

        async fn create_multipart_upload(
            &self,
            bucket: &str,
            key: &str,
            headers: ObjectHeaders,
            user_metadata: HashMap<String, String>,
        ) -> Result<MultipartUpload> {
            Ok(MultipartUpload {
                upload_id: "test-upload-id".to_string(),
                bucket: bucket.to_string(),
                key: key.to_string(),
                initiated: chrono::Utc::now(),
                content_type: headers.content_type.clone(),
                user_metadata,
                cache_control: headers.cache_control.clone(),
                content_disposition: headers.content_disposition.clone(),
                content_encoding: headers.content_encoding.clone(),
                content_language: headers.content_language.clone(),
                expires: headers.expires.clone(),
                storage_class: headers.storage_class.unwrap_or_default(),
                sse_customer_algorithm: headers.sse_customer_algorithm.clone(),
                sse_customer_key_md5: headers.sse_customer_key_md5.clone(),
            })
        }

        async fn upload_part(
            &self,
            bucket: &str,
            key: &str,
            upload_id: &str,
            part_number: u32,
            data: Bytes,
        ) -> Result<ETag> {
            Ok(ETag::from("\"part-etag\"".to_string()))
        }

        async fn complete_multipart_upload(
            &self,
            bucket: &str,
            key: &str,
            upload_id: &str,
            parts: &[(u32, String)],
            _sse_c_key: Option<&[u8; 32]>,
        ) -> Result<ETag> {
            Ok(ETag::from("\"complete-etag\"".to_string()))
        }

        async fn abort_multipart_upload(
            &self,
            bucket: &str,
            key: &str,
            upload_id: &str,
        ) -> Result<()> {
            Ok(())
        }

        async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> Result<Vec<Part>> {
            Ok(vec![])
        }

        async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>> {
            Ok(vec![])
        }

        async fn get_multipart_upload(&self, _upload_id: &str) -> Result<MultipartUpload> {
            unimplemented!()
        }

        async fn copy_object(
            &self,
            src_bucket: &str,
            src_key: &str,
            dst_bucket: &str,
            dst_key: &str,
            new_headers: Option<ObjectHeaders>,
            new_metadata: Option<HashMap<String, String>>,
        ) -> Result<ETag> {
            Ok(ETag::from("\"copy-etag\"".to_string()))
        }
    }

    /// Mock replicator that tracks replication calls.
    struct MockReplicator {
        replications: AtomicUsize,
        #[allow(dead_code)]
        entries: StdMutex<Vec<ReplicationEntry>>,
    }

    impl MockReplicator {
        fn new() -> Self {
            Self { replications: AtomicUsize::new(0), entries: StdMutex::new(Vec::new()) }
        }

        fn replication_count(&self) -> usize {
            self.replications.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Replicator for MockReplicator {
        async fn replicate(
            &self,
            entry: ReplicationEntry,
        ) -> rucket_replication::Result<ReplicationResult> {
            self.replications.fetch_add(1, Ordering::SeqCst);
            self.entries.lock().unwrap().push(entry.clone());
            Ok(ReplicationResult::success(entry.id, 2, 2))
        }

        async fn get_replicas(&self) -> Vec<ReplicaInfo> {
            vec![]
        }
    }

    #[tokio::test]
    async fn test_replicated_storage_creation() {
        let storage = MockStorage::new();
        let replicated = ReplicatedStorage::new(storage);

        assert_eq!(replicated.default_level(), ReplicationLevel::Local);
    }

    #[tokio::test]
    async fn test_local_level_no_replication() {
        let storage = MockStorage::new();
        let replicator = Arc::new(MockReplicator::new());
        let config = ReplicationConfig::new().default_level(ReplicationLevel::Local);
        let replicated = ReplicatedStorage::with_replicator(storage, replicator.clone(), &config);

        // Put object
        let result = replicated
            .put_object(
                "bucket",
                "key",
                Bytes::from("data"),
                ObjectHeaders::default(),
                HashMap::new(),
            )
            .await;
        assert!(result.is_ok());

        // No replication should happen for Local level
        assert_eq!(replicator.replication_count(), 0);
    }

    #[tokio::test]
    async fn test_replicated_level_triggers_replication() {
        let storage = MockStorage::new();
        let replicator = Arc::new(MockReplicator::new());
        let config = ReplicationConfig::new().default_level(ReplicationLevel::Replicated);
        let replicated = ReplicatedStorage::with_replicator(storage, replicator.clone(), &config);

        // Put object
        let result = replicated
            .put_object(
                "bucket",
                "key",
                Bytes::from("data"),
                ObjectHeaders::default(),
                HashMap::new(),
            )
            .await;
        assert!(result.is_ok());

        // Replication should happen for Replicated level
        assert_eq!(replicator.replication_count(), 1);

        // Delete object
        let result = replicated.delete_object("bucket", "key").await;
        assert!(result.is_ok());

        // Another replication
        assert_eq!(replicator.replication_count(), 2);
    }

    #[tokio::test]
    async fn test_create_bucket_replication() {
        let storage = MockStorage::new();
        let replicator = Arc::new(MockReplicator::new());
        let config = ReplicationConfig::new().default_level(ReplicationLevel::Replicated);
        let replicated = ReplicatedStorage::with_replicator(storage, replicator.clone(), &config);

        // Create bucket
        let result = replicated.create_bucket("mybucket").await;
        assert!(result.is_ok());

        // Replication should happen
        assert_eq!(replicator.replication_count(), 1);
    }

    #[tokio::test]
    async fn test_durable_level_triggers_sync_replication() {
        let storage = MockStorage::new();
        let replicator = Arc::new(MockReplicator::new());
        let config = ReplicationConfig::new().default_level(ReplicationLevel::Durable);
        let replicated = ReplicatedStorage::with_replicator(storage, replicator.clone(), &config);

        // Put object with Durable level
        let result = replicated
            .put_object(
                "bucket",
                "key",
                Bytes::from("data"),
                ObjectHeaders::default(),
                HashMap::new(),
            )
            .await;
        assert!(result.is_ok());

        // Replication should happen
        assert_eq!(replicator.replication_count(), 1);
    }

    #[tokio::test]
    async fn test_read_operations_not_replicated() {
        let storage = MockStorage::new();
        let replicator = Arc::new(MockReplicator::new());
        let config = ReplicationConfig::new().default_level(ReplicationLevel::Replicated);
        let replicated = ReplicatedStorage::with_replicator(storage, replicator.clone(), &config);

        // Read operations should not trigger replication
        let _ = replicated.head_bucket("bucket").await;
        let _ = replicated.list_buckets().await;
        let _ = replicated.get_bucket("bucket").await;

        assert_eq!(replicator.replication_count(), 0);
    }
}
