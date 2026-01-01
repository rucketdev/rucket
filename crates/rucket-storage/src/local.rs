//! Local filesystem storage implementation.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use rucket_core::error::{Error, S3ErrorCode};
use rucket_core::types::{BucketInfo, ETag, MultipartUpload, ObjectMetadata, Part};
use rucket_core::{RedbConfig, Result, SyncConfig, SyncStrategy, WalConfig};
use tokio::fs;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::backend::{ListObjectsResult, StorageBackend};
use crate::metadata::{MetadataBackend, RedbMetadataStore};
use crate::streaming::compute_crc32c;
use crate::sync::{write_and_hash_with_strategy, SyncManager};
use crate::wal::{RecoveryManager, WalEntry, WalSyncMode, WalWriter, WalWriterConfig};

/// Sync a directory to ensure its entries (file names) are persisted.
/// This is critical for durability after rename operations.
async fn sync_directory(path: &std::path::Path) -> std::io::Result<()> {
    let dir = fs::File::open(path).await?;
    dir.sync_all().await
}

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
    /// Write-ahead log for crash recovery.
    wal: Option<Arc<WalWriter>>,
}

impl LocalStorage {
    /// Create a new local storage backend with default configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created or the metadata store cannot be opened.
    pub async fn new(data_dir: PathBuf, temp_dir: PathBuf) -> Result<Self> {
        Self::with_full_config(
            data_dir,
            temp_dir,
            SyncConfig::default(),
            RedbConfig::default(),
            WalConfig::default(),
        )
        .await
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
        Self::with_full_config(
            data_dir,
            temp_dir,
            sync_config,
            RedbConfig::default(),
            WalConfig::default(),
        )
        .await
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
        Self::with_full_config(data_dir, temp_dir, sync_config, redb_config, WalConfig::default())
            .await
    }

    /// Create a new local storage backend with full configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created or the metadata store cannot be opened.
    pub async fn with_full_config(
        data_dir: PathBuf,
        temp_dir: PathBuf,
        sync_config: SyncConfig,
        redb_config: RedbConfig,
        wal_config: WalConfig,
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

        // Open WAL and run recovery if enabled
        let wal = if wal_config.enabled {
            let wal_dir = data_dir.join("wal");

            // Run recovery before opening WAL for writes
            let recovery = RecoveryManager::new(wal_dir.clone(), data_dir.clone());
            match recovery.recover().await {
                Ok(stats) => {
                    if stats.puts_rolled_back > 0 || stats.deletes_rolled_back > 0 {
                        tracing::info!(
                            puts_rolled_back = stats.puts_rolled_back,
                            deletes_rolled_back = stats.deletes_rolled_back,
                            "WAL recovery complete"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "WAL recovery failed, continuing anyway");
                }
            }

            // Clean up old WAL files
            if let Err(e) = recovery.cleanup_old_wals().await {
                tracing::warn!(error = %e, "Failed to cleanup old WAL files");
            }

            // Open WAL for writes
            let wal_sync_mode = match wal_config.sync_mode {
                rucket_core::WalSyncMode::None => WalSyncMode::None,
                rucket_core::WalSyncMode::Fdatasync => WalSyncMode::Fdatasync,
                rucket_core::WalSyncMode::Fsync => WalSyncMode::Fsync,
            };

            let writer_config =
                WalWriterConfig { wal_dir, sync_mode: wal_sync_mode, ..Default::default() };

            match WalWriter::open(&writer_config) {
                Ok(writer) => Some(Arc::new(writer)),
                Err(e) => {
                    tracing::warn!(error = %e, "Failed to open WAL, continuing without");
                    None
                }
            }
        } else {
            None
        };

        let storage = Self {
            data_dir,
            temp_dir,
            metadata: Arc::new(metadata),
            sync_manager: SyncManager::new(sync_config),
            key_locks: Arc::new(DashMap::new()),
            wal,
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
            wal: None, // No WAL for in-memory tests
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

    fn part_path(&self, uuid: &Uuid) -> PathBuf {
        self.temp_dir.join(format!("{uuid}.part"))
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
        user_metadata: HashMap<String, String>,
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

        // Compute CRC32C early for WAL intent
        let crc32c = compute_crc32c(&data);
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Step 1: Write intent to WAL (if enabled)
        // This is the durability point - if we crash after this, recovery will clean up
        if let Some(wal) = &self.wal {
            let intent = WalEntry::PutIntent {
                bucket: bucket.to_string(),
                key: key.to_string(),
                uuid,
                size: data_len,
                crc32c,
                timestamp,
            };
            if let Err(e) = wal.append_sync(intent).await {
                tracing::warn!(error = %e, "Failed to write WAL intent, continuing without");
            }
        }

        // Step 2: Write to temp file and compute ETag (no sync yet - WAL protects us)
        let write_result =
            write_and_hash_with_strategy(&temp_path, &data, SyncStrategy::None).await?;

        // Check if we should sync NOW (Always mode, or threshold reached for Periodic/Threshold)
        let should_sync = self.sync_manager.should_sync_now(data_len);

        if should_sync {
            // Sync the temp file BEFORE rename for durability
            let file = fs::File::open(&temp_path).await?;
            file.sync_all().await?;
            // Reset counters since we just synced
            self.sync_manager.reset_counters();
        }

        // Step 3: Move to final location (atomic on same filesystem)
        let final_path = self.object_path(bucket, &uuid);
        fs::rename(&temp_path, &final_path).await?;

        // For maximum durability (Always mode), also sync the parent directory
        // to ensure the directory entry (file name) is persisted to disk.
        if sync_strategy == SyncStrategy::Always {
            let bucket_dir = self.bucket_path(bucket);
            if let Err(e) = sync_directory(&bucket_dir).await {
                tracing::warn!(?bucket_dir, error = %e, "Failed to sync directory");
            }
        }

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

        // Step 4: Update metadata with checksum and user metadata
        let mut meta = ObjectMetadata::new(key, uuid, data_len, write_result.etag.clone())
            .with_checksum(write_result.crc32c)
            .with_user_metadata(user_metadata);
        if let Some(ct) = content_type {
            meta = meta.with_content_type(ct);
        }
        self.metadata.put_object(bucket, meta).await?;

        // Step 5: Write commit to WAL (if enabled)
        // After this point, the operation is complete and won't be rolled back
        if let Some(wal) = &self.wal {
            let commit =
                WalEntry::PutCommit { bucket: bucket.to_string(), key: key.to_string(), uuid };
            if let Err(e) = wal.append(commit).await {
                tracing::warn!(error = %e, "Failed to write WAL commit");
            }
        }

        Ok(write_result.etag)
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

        // Get the old UUID before deletion for WAL
        let old_uuid = self.metadata.get_object(bucket, key).await.ok().map(|m| m.uuid);

        // Step 1: Write delete intent to WAL (if enabled and object exists)
        if let (Some(wal), Some(uuid)) = (&self.wal, old_uuid) {
            let timestamp = chrono::Utc::now().timestamp_millis();
            let intent = WalEntry::DeleteIntent {
                bucket: bucket.to_string(),
                key: key.to_string(),
                old_uuid: uuid,
                timestamp,
            };
            if let Err(e) = wal.append_sync(intent).await {
                tracing::warn!(error = %e, "Failed to write WAL delete intent");
            }
        }

        // Step 2: Delete metadata and file
        if let Some(uuid) = self.metadata.delete_object(bucket, key).await? {
            let path = self.object_path(bucket, &uuid);
            let _ = fs::remove_file(&path).await;

            // Step 3: Write delete commit to WAL
            if let Some(wal) = &self.wal {
                let commit =
                    WalEntry::DeleteCommit { bucket: bucket.to_string(), key: key.to_string() };
                if let Err(e) = wal.append(commit).await {
                    tracing::warn!(error = %e, "Failed to write WAL delete commit");
                }
            }
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

        // Put to destination, preserving content type and user metadata
        self.put_object(
            dst_bucket,
            dst_key,
            data,
            src_meta.content_type.as_deref(),
            src_meta.user_metadata,
        )
        .await
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

        // When using delimiter, multiple objects may collapse into a single common prefix.
        // To get max_keys unique items, we may need to fetch more objects from the DB.
        // We fetch in batches until we have enough unique items or run out of objects.
        let use_delimiter = delimiter.map_or(false, |d| !d.is_empty());

        if !use_delimiter {
            // No delimiter - simple case, just fetch max_keys objects
            let (objects, next_token) =
                self.metadata.list_objects(bucket, prefix, continuation_token, max_keys).await?;

            return Ok(ListObjectsResult {
                objects,
                common_prefixes: Vec::new(),
                is_truncated: next_token.is_some(),
                next_continuation_token: next_token,
            });
        }

        // With delimiter - need to handle collapsing into common prefixes
        let delim = delimiter.unwrap();
        let prefix_str = prefix.unwrap_or("");
        let marker = continuation_token.unwrap_or("");
        let mut seen_prefixes = std::collections::HashSet::new();
        let mut common_prefixes = Vec::new();
        let mut filtered_objects = Vec::new();
        let mut current_token = continuation_token.map(|s| s.to_string());
        let mut final_next_token: Option<String> = None;

        // Fetch objects in batches until we have max_keys unique items
        let batch_size = max_keys.max(100); // Fetch at least 100 at a time for efficiency

        loop {
            let (objects, next_token) = self
                .metadata
                .list_objects(bucket, prefix, current_token.as_deref(), batch_size)
                .await?;

            for obj in objects {
                // Process the object
                let key_suffix = if obj.key.starts_with(prefix_str) {
                    &obj.key[prefix_str.len()..]
                } else {
                    &obj.key
                };

                if let Some(pos) = key_suffix.find(delim) {
                    let delim_end = pos + delim.len();
                    let common_prefix = format!("{}{}", prefix_str, &key_suffix[..delim_end]);

                    // Skip common prefixes that are <= marker (already returned in previous page)
                    if common_prefix.as_str() <= marker {
                        continue;
                    }

                    // Check if this would be a new unique item
                    if !seen_prefixes.contains(&common_prefix) {
                        // Check if we've already reached max_keys
                        let total_items = filtered_objects.len() + common_prefixes.len();
                        if total_items >= max_keys as usize {
                            // This would be a new item beyond max_keys - we're truncated
                            final_next_token = Some(obj.key.clone());
                            break;
                        }
                        seen_prefixes.insert(common_prefix.clone());
                        common_prefixes.push(common_prefix);
                    }
                    // If prefix already seen, just continue (don't count as new item)
                } else {
                    // This is a direct key (not under a delimiter)
                    let total_items = filtered_objects.len() + common_prefixes.len();
                    if total_items >= max_keys as usize {
                        final_next_token = Some(obj.key.clone());
                        break;
                    }
                    filtered_objects.push(obj);
                }
            }

            // If we found a next item beyond max_keys, we're done
            if final_next_token.is_some() {
                break;
            }

            // If no more objects in the DB, we're done
            if next_token.is_none() {
                break;
            }

            // Need to fetch more to see if there's a next item
            current_token = next_token;
        }

        // We're truncated only if we found an actual next item beyond max_keys
        let is_truncated = final_next_token.is_some();

        Ok(ListObjectsResult {
            objects: filtered_objects,
            common_prefixes,
            is_truncated,
            next_continuation_token: final_next_token,
        })
    }

    // Multipart upload operations

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        content_type: Option<&str>,
        user_metadata: HashMap<String, String>,
    ) -> Result<MultipartUpload> {
        // Check bucket exists
        if !self.metadata.bucket_exists(bucket).await? {
            return Err(Error::s3_with_resource(
                S3ErrorCode::NoSuchBucket,
                "The specified bucket does not exist",
                bucket,
            ));
        }

        let upload_id = Uuid::new_v4().to_string();
        self.metadata
            .create_multipart_upload(bucket, key, &upload_id, content_type, user_metadata)
            .await
    }

    async fn upload_part(
        &self,
        bucket: &str,
        _key: &str,
        upload_id: &str,
        part_number: u32,
        data: Bytes,
    ) -> Result<ETag> {
        // Check bucket exists
        if !self.metadata.bucket_exists(bucket).await? {
            return Err(Error::s3_with_resource(
                S3ErrorCode::NoSuchBucket,
                "The specified bucket does not exist",
                bucket,
            ));
        }

        // Check upload exists
        let _upload = self.metadata.get_multipart_upload(upload_id).await?;

        // Validate part number (1-10000)
        if part_number == 0 || part_number > 10000 {
            return Err(Error::s3(
                S3ErrorCode::InvalidPart,
                "Part number must be between 1 and 10000",
            ));
        }

        let uuid = Uuid::new_v4();
        let part_path = self.part_path(&uuid);
        let data_len = data.len() as u64;

        // Write part to file and compute hash
        let write_result =
            write_and_hash_with_strategy(&part_path, &data, SyncStrategy::None).await?;

        // Store part metadata
        self.metadata
            .put_part(upload_id, part_number, uuid, data_len, write_result.etag.as_str())
            .await?;

        Ok(write_result.etag)
    }

    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        parts: &[(u32, String)],
    ) -> Result<ETag> {
        use tokio::io::AsyncWriteExt;

        // Check bucket exists
        if !self.metadata.bucket_exists(bucket).await? {
            return Err(Error::s3_with_resource(
                S3ErrorCode::NoSuchBucket,
                "The specified bucket does not exist",
                bucket,
            ));
        }

        // Get upload info
        let upload = self.metadata.get_multipart_upload(upload_id).await?;
        if upload.bucket != bucket || upload.key != key {
            return Err(Error::s3(S3ErrorCode::InvalidRequest, "Upload does not match bucket/key"));
        }

        // Get all parts with their UUIDs
        let stored_parts = self.metadata.list_parts_with_uuids(upload_id).await?;

        // Build map of part_number -> (Part, UUID)
        let part_map: std::collections::HashMap<u32, (Part, Uuid)> =
            stored_parts.into_iter().map(|(p, u)| (p.part_number, (p, u))).collect();

        // Validate all requested parts exist and ETags match
        let mut ordered_parts = Vec::with_capacity(parts.len());
        for (part_num, expected_etag) in parts {
            let (part, uuid) = part_map.get(part_num).ok_or_else(|| {
                Error::s3(S3ErrorCode::InvalidPart, format!("Part {part_num} not found"))
            })?;

            // Normalize ETags for comparison (remove quotes if present)
            let stored_etag = part.etag.as_str().trim_matches('"');
            let expected = expected_etag.trim_matches('"');
            if stored_etag != expected {
                return Err(Error::s3(
                    S3ErrorCode::InvalidPart,
                    format!("Part {part_num} ETag mismatch"),
                ));
            }

            ordered_parts.push((*part_num, part.clone(), *uuid));
        }

        // Ensure parts are in order
        ordered_parts.sort_by_key(|(n, _, _)| *n);

        // Create final object by concatenating parts
        self.ensure_bucket_dir(bucket).await?;
        let final_uuid = Uuid::new_v4();
        let temp_path = self.temp_path(&final_uuid);

        let mut final_file = fs::File::create(&temp_path).await?;
        let mut total_size = 0u64;
        let mut md5_concat = Vec::new();
        let mut part_count = 0u32;

        for (_part_num, part, uuid) in &ordered_parts {
            let part_path = self.part_path(uuid);
            let part_data = fs::read(&part_path).await?;

            // Accumulate part MD5 hashes for multipart ETag computation
            // The part ETag is the hex-encoded MD5, so we decode it back to bytes
            if let Ok(part_md5) = hex::decode(part.etag.as_str().trim_matches('"')) {
                md5_concat.extend_from_slice(&part_md5);
            }

            final_file.write_all(&part_data).await?;
            total_size += part.size;
            part_count += 1;
        }

        final_file.flush().await?;
        drop(final_file);

        // Move to final location
        let final_path = self.object_path(bucket, &final_uuid);
        fs::rename(&temp_path, &final_path).await?;

        // Compute multipart ETag: MD5 of concatenated part MD5s + "-" + part count
        use md5::{Digest, Md5};
        let mut hasher = Md5::new();
        hasher.update(&md5_concat);
        let hash: [u8; 16] = hasher.finalize().into();
        let etag = ETag::from_multipart(&hash, part_count as usize);

        // Delete old object if overwriting
        if let Ok(old_meta) = self.metadata.get_object(bucket, key).await {
            let old_path = self.object_path(bucket, &old_meta.uuid);
            let _ = fs::remove_file(&old_path).await;
        }

        // Store final object metadata with content_type and user_metadata from the upload
        let mut meta = ObjectMetadata::new(key, final_uuid, total_size, etag.clone())
            .with_user_metadata(upload.user_metadata);
        if let Some(ct) = upload.content_type {
            meta = meta.with_content_type(&ct);
        }
        self.metadata.put_object(bucket, meta).await?;

        // Clean up parts
        let part_uuids = self.metadata.delete_parts(upload_id).await?;
        for uuid in part_uuids {
            let part_path = self.part_path(&uuid);
            let _ = fs::remove_file(&part_path).await;
        }

        // Delete upload record
        self.metadata.delete_multipart_upload(upload_id).await?;

        Ok(etag)
    }

    async fn abort_multipart_upload(&self, bucket: &str, key: &str, upload_id: &str) -> Result<()> {
        // Get upload info to validate
        let upload = self.metadata.get_multipart_upload(upload_id).await?;
        if upload.bucket != bucket || upload.key != key {
            return Err(Error::s3(S3ErrorCode::InvalidRequest, "Upload does not match bucket/key"));
        }

        // Delete parts and their files
        let part_uuids = self.metadata.delete_parts(upload_id).await?;
        for uuid in part_uuids {
            let part_path = self.part_path(&uuid);
            let _ = fs::remove_file(&part_path).await;
        }

        // Delete upload record
        self.metadata.delete_multipart_upload(upload_id).await?;

        Ok(())
    }

    async fn list_parts(&self, bucket: &str, key: &str, upload_id: &str) -> Result<Vec<Part>> {
        // Validate upload matches bucket/key
        let upload = self.metadata.get_multipart_upload(upload_id).await?;
        if upload.bucket != bucket || upload.key != key {
            return Err(Error::s3(S3ErrorCode::InvalidRequest, "Upload does not match bucket/key"));
        }

        self.metadata.list_parts(upload_id).await
    }

    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>> {
        // Check bucket exists
        if !self.metadata.bucket_exists(bucket).await? {
            return Err(Error::s3_with_resource(
                S3ErrorCode::NoSuchBucket,
                "The specified bucket does not exist",
                bucket,
            ));
        }

        self.metadata.list_multipart_uploads(bucket).await
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
            .put_object(
                "test-bucket",
                "hello.txt",
                data.clone(),
                Some("text/plain"),
                HashMap::new(),
            )
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
        storage.put_object("test-bucket", "hello.txt", data, None, HashMap::new()).await.unwrap();

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
            .put_object("bucket1", "source.txt", data.clone(), Some("text/plain"), HashMap::new())
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
                    .put_object("test-bucket", "same-key", data, None, HashMap::new())
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
                    .put_object("test-bucket", &key, data, None, HashMap::new())
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
        storage.put_object("test-bucket", "test-key", data1, None, HashMap::new()).await.unwrap();

        // Overwrite multiple times
        for i in 0..5 {
            let data = Bytes::from(format!("overwrite-{i}"));
            storage
                .put_object("test-bucket", "test-key", data, None, HashMap::new())
                .await
                .unwrap();
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
