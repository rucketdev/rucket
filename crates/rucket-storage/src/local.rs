// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Local filesystem storage implementation.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use rucket_core::error::{Error, S3ErrorCode};
use rucket_core::types::{BucketInfo, ETag, ObjectMetadata};
use rucket_core::{RedbConfig, Result, SyncConfig, SyncStrategy};
use tokio::fs;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::backend::{ListObjectsResult, StorageBackend};
use crate::metadata::{MetadataBackend, RedbMetadataStore};
use crate::sync::{write_and_hash_with_strategy, SyncManager};

/// Per-key lock type for serializing concurrent writes.
type KeyLock = Arc<tokio::sync::Mutex<()>>;

/// Local filesystem storage backend.
pub struct LocalStorage {
    data_dir: PathBuf,
    temp_dir: PathBuf,
    metadata: Arc<dyn MetadataBackend>,
    sync_manager: Arc<SyncManager>,
    /// Per-key write locks to serialize concurrent writes to the same object.
    /// This prevents race conditions during overwrites.
    key_locks: Arc<DashMap<(String, String), KeyLock>>,
}

impl LocalStorage {
    /// Create a new local storage backend with default configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created or the metadata store cannot be opened.
    pub async fn new(data_dir: PathBuf, temp_dir: PathBuf) -> Result<Self> {
        Self::with_config(data_dir, temp_dir, SyncConfig::default(), RedbConfig::default()).await
    }

    /// Create a new local storage backend with custom sync configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created or the metadata store cannot be opened.
    pub async fn with_sync_config(
        data_dir: PathBuf,
        temp_dir: PathBuf,
        sync_config: SyncConfig,
    ) -> Result<Self> {
        Self::with_config(data_dir, temp_dir, sync_config, RedbConfig::default()).await
    }

    /// Create a new local storage backend with custom sync and redb configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created or the metadata store cannot be opened.
    pub async fn with_config(
        data_dir: PathBuf,
        temp_dir: PathBuf,
        sync_config: SyncConfig,
        redb_config: RedbConfig,
    ) -> Result<Self> {
        // Create directories if they don't exist
        fs::create_dir_all(&data_dir).await?;
        fs::create_dir_all(&temp_dir).await?;

        let metadata_path = data_dir.join("metadata.redb");
        let metadata = RedbMetadataStore::open_with_cache(
            &metadata_path,
            sync_config.metadata,
            redb_config.cache_size_bytes,
        )?;

        let storage = Self {
            data_dir,
            temp_dir,
            metadata: Arc::new(metadata),
            sync_manager: SyncManager::new(sync_config),
            key_locks: Arc::new(DashMap::new()),
        };

        // Clean up any orphaned temp files from previous runs
        storage.recover_temp_files().await?;

        Ok(storage)
    }

    /// Create a local storage backend with an in-memory metadata store (for testing).
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created.
    pub async fn new_in_memory(data_dir: PathBuf, temp_dir: PathBuf) -> Result<Self> {
        Self::new_in_memory_with_sync(data_dir, temp_dir, SyncConfig::default()).await
    }

    /// Create a local storage backend with in-memory metadata and custom sync config.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created.
    pub async fn new_in_memory_with_sync(
        data_dir: PathBuf,
        temp_dir: PathBuf,
        sync_config: SyncConfig,
    ) -> Result<Self> {
        fs::create_dir_all(&data_dir).await?;
        fs::create_dir_all(&temp_dir).await?;

        let metadata = RedbMetadataStore::open_in_memory()?;

        let storage = Self {
            data_dir,
            temp_dir,
            metadata: Arc::new(metadata),
            sync_manager: SyncManager::new(sync_config),
            key_locks: Arc::new(DashMap::new()),
        };

        // Clean up any orphaned temp files from previous runs
        storage.recover_temp_files().await?;

        Ok(storage)
    }

    /// Get the current sync configuration.
    #[must_use]
    pub fn sync_config(&self) -> &SyncConfig {
        self.sync_manager.config()
    }

    /// Get or create a lock for a specific (bucket, key) pair.
    ///
    /// This ensures that concurrent writes to the same key are serialized.
    fn get_key_lock(&self, bucket: &str, key: &str) -> KeyLock {
        let lock_key = (bucket.to_string(), key.to_string());
        self.key_locks
            .entry(lock_key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// Clean up orphaned temp files from previous runs.
    ///
    /// This is called during startup to ensure a clean state.
    async fn recover_temp_files(&self) -> Result<()> {
        let mut entries = fs::read_dir(&self.temp_dir).await?;
        let mut cleaned = 0u64;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "tmp") {
                if let Err(e) = fs::remove_file(&path).await {
                    tracing::warn!(?path, error = %e, "Failed to remove orphaned temp file");
                } else {
                    cleaned += 1;
                }
            }
        }

        if cleaned > 0 {
            tracing::info!(count = cleaned, "Cleaned up orphaned temp files");
        }

        Ok(())
    }

    fn bucket_path(&self, bucket: &str) -> PathBuf {
        self.data_dir.join(bucket)
    }

    fn object_path(&self, bucket: &str, uuid: &Uuid) -> PathBuf {
        self.bucket_path(bucket).join(format!("{uuid}.dat"))
    }

    fn temp_path(&self, uuid: &Uuid) -> PathBuf {
        self.temp_dir.join(format!("{uuid}.tmp"))
    }

    async fn ensure_bucket_dir(&self, bucket: &str) -> Result<()> {
        let path = self.bucket_path(bucket);
        if !path.exists() {
            fs::create_dir_all(&path).await?;
        }
        Ok(())
    }

    /// Read object data directly, bypassing OS page cache.
    ///
    /// Used for benchmarking to get accurate disk I/O measurements.
    #[cfg(feature = "bench")]
    pub async fn get_object_direct(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<(ObjectMetadata, Bytes)> {
        let meta = self.metadata.get_object(bucket, key).await?;
        let path = self.object_path(bucket, &meta.uuid);

        // Use blocking task for sync direct I/O
        let data = tokio::task::spawn_blocking(move || crate::direct_io::read_direct(&path))
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?
            .map_err(Error::Io)?;

        Ok((meta, Bytes::from(data)))
    }
}

impl StorageBackend for LocalStorage {
    async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        let info = self.metadata.create_bucket(name).await?;
        self.ensure_bucket_dir(name).await?;
        Ok(info)
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        self.metadata.delete_bucket(name).await?;

        // Remove the bucket directory
        let path = self.bucket_path(name);
        if path.exists() {
            fs::remove_dir_all(&path).await?;
        }

        Ok(())
    }

    async fn head_bucket(&self, name: &str) -> Result<bool> {
        self.metadata.bucket_exists(name).await
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.metadata.list_buckets().await
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        content_type: Option<&str>,
    ) -> Result<ETag> {
        // Check bucket exists first (before acquiring lock)
        if !self.metadata.bucket_exists(bucket).await? {
            return Err(Error::s3_with_resource(
                S3ErrorCode::NoSuchBucket,
                "The specified bucket does not exist",
                bucket,
            ));
        }

        // Acquire per-key lock to serialize concurrent writes to the same key.
        // This prevents race conditions during overwrites.
        let lock = self.get_key_lock(bucket, key);
        let _guard = lock.lock().await;

        self.ensure_bucket_dir(bucket).await?;

        let uuid = Uuid::new_v4();
        let temp_path = self.temp_path(&uuid);
        let data_len = data.len() as u64;
        let sync_strategy = self.sync_manager.config().data;

        // Write to temp file and compute ETag (no sync yet - we handle it below)
        let etag = write_and_hash_with_strategy(&temp_path, &data, SyncStrategy::None).await?;

        // Check if we should sync NOW (Always mode, or threshold reached for Periodic/Threshold)
        let should_sync = self.sync_manager.should_sync_now(data_len);

        if should_sync {
            // Sync the temp file BEFORE rename for durability
            let file = fs::File::open(&temp_path).await?;
            file.sync_all().await?;
            // Reset counters since we just synced
            self.sync_manager.reset_counters();
        }

        // Move to final location (atomic on same filesystem)
        let final_path = self.object_path(bucket, &uuid);
        fs::rename(&temp_path, &final_path).await?;

        // Record the write (updates counters)
        self.sync_manager.record_write(data_len);

        // For periodic/threshold without immediate sync, track for background sync
        if !should_sync && matches!(sync_strategy, SyncStrategy::Periodic | SyncStrategy::Threshold)
        {
            self.sync_manager.add_pending_file(final_path).await;
        }

        // Delete old object if it exists
        if let Ok(old_meta) = self.metadata.get_object(bucket, key).await {
            let old_path = self.object_path(bucket, &old_meta.uuid);
            let _ = fs::remove_file(&old_path).await;
        }

        // Update metadata
        let mut meta = ObjectMetadata::new(key, uuid, data_len, etag.clone());
        if let Some(ct) = content_type {
            meta = meta.with_content_type(ct);
        }
        self.metadata.put_object(bucket, meta).await?;

        Ok(etag)
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMetadata, Bytes)> {
        let meta = self.metadata.get_object(bucket, key).await?;
        let path = self.object_path(bucket, &meta.uuid);

        let data = fs::read(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::s3_with_resource(
                    S3ErrorCode::NoSuchKey,
                    "The specified key does not exist",
                    key,
                )
            } else {
                Error::Io(e)
            }
        })?;

        Ok((meta, Bytes::from(data)))
    }

    async fn get_object_range(
        &self,
        bucket: &str,
        key: &str,
        start: u64,
        end: u64,
    ) -> Result<(ObjectMetadata, Bytes)> {
        let meta = self.metadata.get_object(bucket, key).await?;
        let path = self.object_path(bucket, &meta.uuid);

        // Validate range
        if start > meta.size || start > end {
            return Err(Error::s3(
                S3ErrorCode::InvalidRange,
                "The requested range is not satisfiable",
            ));
        }

        let end = end.min(meta.size - 1);
        let length = (end - start + 1) as usize;

        let mut file = fs::File::open(&path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                Error::s3_with_resource(
                    S3ErrorCode::NoSuchKey,
                    "The specified key does not exist",
                    key,
                )
            } else {
                Error::Io(e)
            }
        })?;

        // Seek to start position
        use tokio::io::AsyncSeekExt;
        file.seek(std::io::SeekFrom::Start(start)).await?;

        // Read the range
        let mut buffer = vec![0u8; length];
        file.read_exact(&mut buffer).await?;

        Ok((meta, Bytes::from(buffer)))
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        // Acquire per-key lock to serialize with concurrent writes
        let lock = self.get_key_lock(bucket, key);
        let _guard = lock.lock().await;

        if let Some(uuid) = self.metadata.delete_object(bucket, key).await? {
            let path = self.object_path(bucket, &uuid);
            let _ = fs::remove_file(&path).await;
        }
        Ok(())
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        self.metadata.get_object(bucket, key).await
    }

    async fn copy_object(
        &self,
        src_bucket: &str,
        src_key: &str,
        dst_bucket: &str,
        dst_key: &str,
    ) -> Result<ETag> {
        // Get source object
        let (src_meta, data) = self.get_object(src_bucket, src_key).await?;

        // Put to destination
        self.put_object(dst_bucket, dst_key, data, src_meta.content_type.as_deref()).await
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<ListObjectsResult> {
        // Check bucket exists
        if !self.metadata.bucket_exists(bucket).await? {
            return Err(Error::s3_with_resource(
                S3ErrorCode::NoSuchBucket,
                "The specified bucket does not exist",
                bucket,
            ));
        }

        let (objects, next_token): (Vec<ObjectMetadata>, Option<String>) =
            self.metadata.list_objects(bucket, prefix, continuation_token, max_keys).await?;

        // Handle delimiter (compute common prefixes)
        let mut common_prefixes = Vec::new();
        let filtered_objects = if let Some(delim) = delimiter {
            let prefix_len = prefix.map_or(0, str::len);
            let mut seen_prefixes = std::collections::HashSet::new();
            let mut result = Vec::new();

            for obj in objects {
                let key_suffix = &obj.key[prefix_len..];
                if let Some(pos) = key_suffix.find(delim) {
                    let common_prefix = format!("{}{}", prefix.unwrap_or(""), &key_suffix[..=pos]);
                    if seen_prefixes.insert(common_prefix.clone()) {
                        common_prefixes.push(common_prefix);
                    }
                } else {
                    result.push(obj);
                }
            }
            result
        } else {
            objects
        };

        Ok(ListObjectsResult {
            objects: filtered_objects,
            common_prefixes,
            is_truncated: next_token.is_some(),
            next_continuation_token: next_token,
        })
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    async fn create_test_storage() -> (LocalStorage, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        let storage = LocalStorage::new_in_memory(data_dir, tmp_dir).await.unwrap();

        (storage, temp_dir)
    }

    #[tokio::test]
    async fn test_bucket_operations() {
        let (storage, _temp) = create_test_storage().await;

        // Create bucket
        let bucket = storage.create_bucket("test-bucket").await.unwrap();
        assert_eq!(bucket.name, "test-bucket");

        // Check bucket exists
        assert!(storage.head_bucket("test-bucket").await.unwrap());
        assert!(!storage.head_bucket("nonexistent").await.unwrap());

        // List buckets
        let buckets = storage.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 1);

        // Delete bucket
        storage.delete_bucket("test-bucket").await.unwrap();
        assert!(!storage.head_bucket("test-bucket").await.unwrap());
    }

    #[tokio::test]
    async fn test_object_operations() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Put object
        let data = Bytes::from("Hello, World!");
        let etag = storage
            .put_object("test-bucket", "hello.txt", data.clone(), Some("text/plain"))
            .await
            .unwrap();

        assert!(!etag.is_multipart());

        // Get object
        let (meta, retrieved) = storage.get_object("test-bucket", "hello.txt").await.unwrap();
        assert_eq!(retrieved, data);
        assert_eq!(meta.content_type, Some("text/plain".to_string()));

        // Head object
        let meta = storage.head_object("test-bucket", "hello.txt").await.unwrap();
        assert_eq!(meta.size, 13);

        // Delete object
        storage.delete_object("test-bucket", "hello.txt").await.unwrap();
        assert!(storage.head_object("test-bucket", "hello.txt").await.is_err());
    }

    #[tokio::test]
    async fn test_range_request() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        let data = Bytes::from("Hello, World!");
        storage.put_object("test-bucket", "hello.txt", data, None).await.unwrap();

        // Get range
        let (_, range_data) =
            storage.get_object_range("test-bucket", "hello.txt", 0, 4).await.unwrap();
        assert_eq!(range_data, Bytes::from("Hello"));

        let (_, range_data) =
            storage.get_object_range("test-bucket", "hello.txt", 7, 11).await.unwrap();
        assert_eq!(range_data, Bytes::from("World"));
    }

    #[tokio::test]
    async fn test_copy_object() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("bucket1").await.unwrap();
        storage.create_bucket("bucket2").await.unwrap();

        let data = Bytes::from("Hello, World!");
        storage
            .put_object("bucket1", "source.txt", data.clone(), Some("text/plain"))
            .await
            .unwrap();

        // Copy to same bucket
        storage.copy_object("bucket1", "source.txt", "bucket1", "copy.txt").await.unwrap();

        let (_, copied) = storage.get_object("bucket1", "copy.txt").await.unwrap();
        assert_eq!(copied, data);

        // Copy to different bucket
        storage.copy_object("bucket1", "source.txt", "bucket2", "dest.txt").await.unwrap();

        let (meta, copied) = storage.get_object("bucket2", "dest.txt").await.unwrap();
        assert_eq!(copied, data);
        assert_eq!(meta.content_type, Some("text/plain".to_string()));
    }

    #[tokio::test]
    async fn test_concurrent_writes_same_key() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let (storage, _temp) = create_test_storage().await;
        let storage = Arc::new(storage);

        storage.create_bucket("test-bucket").await.unwrap();

        let num_writers = 10;
        let completed = Arc::new(AtomicUsize::new(0));

        // Spawn multiple concurrent writers to the same key
        let mut handles = Vec::new();
        for i in 0..num_writers {
            let storage = Arc::clone(&storage);
            let completed = Arc::clone(&completed);
            handles.push(tokio::spawn(async move {
                let data = Bytes::from(format!("data-from-writer-{i}"));
                storage
                    .put_object("test-bucket", "same-key", data, None)
                    .await
                    .expect("put_object failed");
                completed.fetch_add(1, Ordering::SeqCst);
            }));
        }

        // Wait for all writers
        for handle in handles {
            handle.await.unwrap();
        }

        // All writers should have completed
        assert_eq!(completed.load(Ordering::SeqCst), num_writers);

        // The key should exist with one of the values
        let (meta, data) = storage.get_object("test-bucket", "same-key").await.unwrap();
        assert!(data.starts_with(b"data-from-writer-"));
        assert!(meta.size > 0);

        // Count data files in the bucket directory - should be exactly 1
        // (no orphaned files from race conditions)
        let bucket_path = storage.bucket_path("test-bucket");
        let mut file_count = 0;
        let mut entries = tokio::fs::read_dir(&bucket_path).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry.path().extension().is_some_and(|ext| ext == "dat") {
                file_count += 1;
            }
        }
        assert_eq!(file_count, 1, "Should have exactly one data file, no orphans");
    }

    #[tokio::test]
    async fn test_concurrent_writes_different_keys() {
        let (storage, _temp) = create_test_storage().await;
        let storage = Arc::new(storage);

        storage.create_bucket("test-bucket").await.unwrap();

        let num_writers = 10;

        // Spawn multiple concurrent writers to different keys
        let mut handles = Vec::new();
        for i in 0..num_writers {
            let storage = Arc::clone(&storage);
            handles.push(tokio::spawn(async move {
                let key = format!("key-{i}");
                let data = Bytes::from(format!("data-{i}"));
                storage
                    .put_object("test-bucket", &key, data, None)
                    .await
                    .expect("put_object failed");
            }));
        }

        // Wait for all writers
        for handle in handles {
            handle.await.unwrap();
        }

        // All keys should exist with correct values
        for i in 0..num_writers {
            let key = format!("key-{i}");
            let expected_data = format!("data-{i}");
            let (_, data) = storage.get_object("test-bucket", &key).await.unwrap();
            assert_eq!(data, Bytes::from(expected_data));
        }
    }

    #[tokio::test]
    async fn test_overwrite_preserves_no_orphans() {
        let (storage, _temp) = create_test_storage().await;

        storage.create_bucket("test-bucket").await.unwrap();

        // Write initial object
        let data1 = Bytes::from("initial data");
        storage.put_object("test-bucket", "test-key", data1, None).await.unwrap();

        // Overwrite multiple times
        for i in 0..5 {
            let data = Bytes::from(format!("overwrite-{i}"));
            storage.put_object("test-bucket", "test-key", data, None).await.unwrap();
        }

        // Count data files - should be exactly 1
        let bucket_path = storage.bucket_path("test-bucket");
        let mut file_count = 0;
        let mut entries = tokio::fs::read_dir(&bucket_path).await.unwrap();
        while let Some(entry) = entries.next_entry().await.unwrap() {
            if entry.path().extension().is_some_and(|ext| ext == "dat") {
                file_count += 1;
            }
        }
        assert_eq!(file_count, 1, "Should have exactly one data file after overwrites");

        // Verify final content
        let (_, data) = storage.get_object("test-bucket", "test-key").await.unwrap();
        assert_eq!(data, Bytes::from("overwrite-4"));
    }

    #[tokio::test]
    async fn test_temp_file_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        // Create tmp directory and add orphaned temp files
        tokio::fs::create_dir_all(&tmp_dir).await.unwrap();
        tokio::fs::write(tmp_dir.join("orphan1.tmp"), b"orphan1").await.unwrap();
        tokio::fs::write(tmp_dir.join("orphan2.tmp"), b"orphan2").await.unwrap();
        tokio::fs::write(tmp_dir.join("not-a-temp.txt"), b"keep").await.unwrap();

        // Create storage (should clean up .tmp files)
        let _storage = LocalStorage::new_in_memory(data_dir, tmp_dir.clone()).await.unwrap();

        // Orphaned .tmp files should be removed
        assert!(!tmp_dir.join("orphan1.tmp").exists());
        assert!(!tmp_dir.join("orphan2.tmp").exists());
        // Non-tmp files should remain
        assert!(tmp_dir.join("not-a-temp.txt").exists());
    }
}
