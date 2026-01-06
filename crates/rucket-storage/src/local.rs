//! Local filesystem storage implementation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use bytes::Bytes;
use dashmap::DashMap;
use rucket_core::encryption::ServerSideEncryptionConfiguration;
use rucket_core::error::{Error, S3ErrorCode};
use rucket_core::lifecycle::LifecycleConfiguration;
use rucket_core::public_access_block::PublicAccessBlockConfiguration;
use rucket_core::types::{
    BucketInfo, CorsConfiguration, ETag, MultipartUpload, ObjectMetadata, Part, TagSet,
    VersioningStatus,
};
use rucket_core::{RecoveryMode, RedbConfig, Result, SyncConfig, SyncStrategy, WalConfig};
use tokio::fs;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

use crate::backend::{
    DeleteObjectResult, ListObjectsResult, ObjectHeaders, PutObjectResult, StorageBackend,
};
use crate::metadata::{ListVersionsResult, MetadataBackend, RedbMetadataStore};
use crate::streaming::{compute_checksum, compute_crc32c};
use crate::sync::{write_and_hash_with_strategy, SyncManager};
use crate::wal::{
    RecoveryManager, RecoveryStats, WalEntry, WalSyncMode, WalWriter, WalWriterConfig,
};

/// Sync a directory to ensure its entries (file names) are persisted.
/// This is critical for durability after rename operations.
async fn sync_directory(path: &std::path::Path) -> std::io::Result<()> {
    let dir = fs::File::open(path).await?;
    let result = dir.sync_all().await;
    #[cfg(test)]
    crate::sync::test_stats::record_dir_sync();
    result
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
    /// Whether to verify CRC32C checksums on read operations.
    verify_checksums_on_read: bool,
    /// Recovery stats from the last startup (if WAL was enabled).
    last_recovery_stats: Option<RecoveryStats>,
    /// Optional SSE-S3 encryption provider.
    encryption_provider: Option<Arc<crate::crypto::SseS3Provider>>,
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

        let metadata: Arc<dyn MetadataBackend> = Arc::new(metadata);

        // Open WAL and run recovery if enabled
        let mut last_recovery_stats = None;
        let wal = if wal_config.enabled {
            let wal_dir = data_dir.join("wal");

            // Run recovery before opening WAL for writes
            let recovery = RecoveryManager::new(wal_dir.clone(), data_dir.clone());
            match recovery.recover().await {
                Ok(mut stats) => {
                    if stats.recovery_needed {
                        tracing::info!(
                            puts_rolled_back = stats.puts_rolled_back,
                            deletes_rolled_back = stats.deletes_rolled_back,
                            "WAL recovery complete (crash detected)"
                        );
                    }

                    // Full recovery: scan orphans and verify checksums
                    if wal_config.recovery_mode == RecoveryMode::Full {
                        tracing::info!(
                            "Running full recovery with orphan scan and checksum verification"
                        );

                        // Scan for orphaned files
                        let metadata_ref = Arc::clone(&metadata);
                        match recovery
                            .scan_orphans(|bucket, uuid| {
                                // Check if this UUID exists in metadata
                                metadata_ref.uuid_exists_sync(bucket, uuid)
                            })
                            .await
                        {
                            Ok(orphans) => {
                                stats.orphans_found = orphans.len();
                                if !orphans.is_empty() {
                                    tracing::warn!(
                                        count = orphans.len(),
                                        "Found orphaned files (files without metadata)"
                                    );
                                    for path in &orphans {
                                        tracing::warn!(path = %path.display(), "Orphaned file found");
                                    }
                                    // Note: We don't automatically delete orphans in full recovery
                                    // to allow manual inspection. Use clean_orphans() if desired.
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to scan for orphans");
                                stats.errors += 1;
                            }
                        }

                        // Verify checksums of all objects
                        match Self::verify_all_checksums(&data_dir, &metadata_ref).await {
                            Ok((verified, mismatches)) => {
                                stats.objects_verified = verified;
                                stats.checksum_mismatches = mismatches;
                                if mismatches > 0 {
                                    tracing::error!(
                                        objects_verified = verified,
                                        checksum_mismatches = mismatches,
                                        "DATA CORRUPTION DETECTED: {} objects have invalid checksums",
                                        mismatches
                                    );
                                } else if verified > 0 {
                                    tracing::info!(
                                        objects_verified = verified,
                                        "All object checksums verified successfully"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to verify checksums");
                                stats.errors += 1;
                            }
                        }

                        tracing::info!(?stats, "Full recovery complete");
                    }

                    // Store the recovery stats for later inspection
                    last_recovery_stats = Some(stats);
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

        let verify_checksums_on_read = sync_config.verify_checksums_on_read;
        let storage = Self {
            data_dir,
            temp_dir,
            metadata,
            sync_manager: SyncManager::new(sync_config),
            key_locks: Arc::new(DashMap::new()),
            wal,
            verify_checksums_on_read,
            last_recovery_stats,
            encryption_provider: None,
        };

        // Clean up any orphaned temp files from previous runs
        storage.recover_temp_files().await?;

        Ok(storage)
    }

    /// Create a new local storage backend with an externally provided metadata backend.
    ///
    /// This constructor is used in cluster mode to inject a `RaftMetadataBackend`
    /// that routes metadata operations through Raft consensus.
    ///
    /// # Errors
    ///
    /// Returns an error if the directories cannot be created or WAL recovery fails.
    pub async fn with_metadata_backend(
        metadata: Arc<dyn MetadataBackend>,
        data_dir: PathBuf,
        temp_dir: PathBuf,
        sync_config: SyncConfig,
        wal_config: WalConfig,
    ) -> Result<Self> {
        // Create directories if they don't exist
        fs::create_dir_all(&data_dir).await?;
        fs::create_dir_all(&temp_dir).await?;

        // Open WAL and run recovery if enabled
        let mut last_recovery_stats = None;
        let wal = if wal_config.enabled {
            let wal_dir = data_dir.join("wal");

            // Run recovery before opening WAL for writes
            let recovery = RecoveryManager::new(wal_dir.clone(), data_dir.clone());
            match recovery.recover().await {
                Ok(mut stats) => {
                    if stats.recovery_needed {
                        tracing::info!(
                            puts_rolled_back = stats.puts_rolled_back,
                            deletes_rolled_back = stats.deletes_rolled_back,
                            "WAL recovery complete (crash detected)"
                        );
                    }

                    // Full recovery: scan orphans and verify checksums
                    if wal_config.recovery_mode == RecoveryMode::Full {
                        tracing::info!(
                            "Running full recovery with orphan scan and checksum verification"
                        );

                        // Scan for orphaned files
                        let metadata_ref = Arc::clone(&metadata);
                        match recovery
                            .scan_orphans(|bucket, uuid| {
                                metadata_ref.uuid_exists_sync(bucket, uuid)
                            })
                            .await
                        {
                            Ok(orphans) => {
                                stats.orphans_found = orphans.len();
                                if !orphans.is_empty() {
                                    tracing::warn!(
                                        count = orphans.len(),
                                        "Found orphaned files (files without metadata)"
                                    );
                                    for path in &orphans {
                                        tracing::warn!(path = %path.display(), "Orphaned file found");
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to scan for orphans");
                                stats.errors += 1;
                            }
                        }

                        // Verify checksums of all objects
                        match Self::verify_all_checksums(&data_dir, &metadata).await {
                            Ok((verified, mismatches)) => {
                                stats.objects_verified = verified;
                                stats.checksum_mismatches = mismatches;
                                if mismatches > 0 {
                                    tracing::error!(
                                        objects_verified = verified,
                                        checksum_mismatches = mismatches,
                                        "DATA CORRUPTION DETECTED: {} objects have invalid checksums",
                                        mismatches
                                    );
                                } else if verified > 0 {
                                    tracing::info!(
                                        objects_verified = verified,
                                        "All object checksums verified successfully"
                                    );
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "Failed to verify checksums");
                                stats.errors += 1;
                            }
                        }

                        tracing::info!(?stats, "Full recovery complete");
                    }

                    // Store the recovery stats for later inspection
                    last_recovery_stats = Some(stats);
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

        let verify_checksums_on_read = sync_config.verify_checksums_on_read;
        let storage = Self {
            data_dir,
            temp_dir,
            metadata,
            sync_manager: SyncManager::new(sync_config),
            key_locks: Arc::new(DashMap::new()),
            wal,
            verify_checksums_on_read,
            last_recovery_stats,
            encryption_provider: None,
        };

        // Clean up any orphaned temp files from previous runs
        storage.recover_temp_files().await?;

        Ok(storage)
    }

    /// Enable SSE-S3 encryption with the given master key.
    ///
    /// Once enabled, all new objects will be encrypted at rest.
    /// Existing objects remain unencrypted until overwritten.
    ///
    /// # Errors
    ///
    /// Returns an error if the master key is invalid.
    pub fn with_encryption(mut self, master_key: &[u8]) -> Result<Self> {
        let provider = crate::crypto::SseS3Provider::new(master_key).map_err(|e| {
            Error::s3(S3ErrorCode::InternalError, format!("Invalid encryption master key: {e}"))
        })?;
        self.encryption_provider = Some(Arc::new(provider));
        Ok(self)
    }

    /// Enable SSE-S3 encryption with a hex-encoded master key.
    ///
    /// # Errors
    ///
    /// Returns an error if the master key is not a valid 64-character hex string.
    pub fn with_encryption_hex(mut self, hex_key: &str) -> Result<Self> {
        let provider = crate::crypto::SseS3Provider::from_hex(hex_key).map_err(|e| {
            Error::s3(S3ErrorCode::InternalError, format!("Invalid encryption master key: {e}"))
        })?;
        self.encryption_provider = Some(Arc::new(provider));
        Ok(self)
    }

    /// Returns true if encryption is enabled.
    #[must_use]
    pub fn is_encryption_enabled(&self) -> bool {
        self.encryption_provider.is_some()
    }

    /// Returns a reference to the metadata backend.
    ///
    /// This is useful for integrating with distributed consensus,
    /// where the metadata backend needs to be wrapped by a Raft layer.
    #[must_use]
    pub fn metadata_backend(&self) -> Arc<dyn MetadataBackend> {
        Arc::clone(&self.metadata)
    }

    /// Verify CRC32C checksums of all objects in storage.
    ///
    /// Returns (objects_verified, checksum_mismatches).
    ///
    /// This is used during full recovery to detect data corruption.
    #[cfg_attr(test, allow(dead_code))]
    pub(crate) async fn verify_all_checksums(
        data_dir: &Path,
        metadata: &Arc<dyn MetadataBackend>,
    ) -> std::io::Result<(usize, usize)> {
        let mut verified = 0;
        let mut mismatches = 0;

        // List all buckets and iterate through all objects
        let buckets = match metadata.list_buckets().await {
            Ok(b) => b,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to list buckets for checksum verification");
                return Err(std::io::Error::other(e.to_string()));
            }
        };

        for bucket_info in buckets {
            let bucket = &bucket_info.name;

            // List all objects in bucket (with pagination)
            let mut continuation_token = None;
            loop {
                let (objects, next_token) = match metadata
                    .list_objects(bucket, None, continuation_token.as_deref(), 1000)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        tracing::warn!(bucket = bucket, error = %e, "Failed to list objects");
                        break;
                    }
                };

                for obj in &objects {
                    if let Some(expected_crc) = obj.crc32c {
                        let path = data_dir.join(bucket).join(format!("{}.dat", obj.uuid));

                        match fs::read(&path).await {
                            Ok(data) => {
                                let actual_crc = compute_crc32c(&data);
                                verified += 1;

                                if actual_crc != expected_crc {
                                    mismatches += 1;
                                    tracing::error!(
                                        bucket = bucket,
                                        key = obj.key,
                                        uuid = %obj.uuid,
                                        expected = expected_crc,
                                        actual = actual_crc,
                                        "CHECKSUM MISMATCH - data corruption detected"
                                    );
                                }
                            }
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                                // File doesn't exist - this is an orphaned metadata entry
                                tracing::warn!(
                                    bucket = bucket,
                                    key = obj.key,
                                    uuid = %obj.uuid,
                                    "Object metadata exists but file is missing"
                                );
                            }
                            Err(e) => {
                                tracing::warn!(
                                    bucket = bucket,
                                    key = obj.key,
                                    error = %e,
                                    "Failed to read object for checksum verification"
                                );
                            }
                        }
                    }
                }

                if next_token.is_some() {
                    continuation_token = next_token;
                } else {
                    break;
                }
            }
        }

        Ok((verified, mismatches))
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

        let verify_checksums_on_read = sync_config.verify_checksums_on_read;
        let storage = Self {
            data_dir,
            temp_dir,
            metadata: Arc::new(metadata),
            sync_manager: SyncManager::new(sync_config),
            key_locks: Arc::new(DashMap::new()),
            wal: None, // No WAL for in-memory tests
            verify_checksums_on_read,
            last_recovery_stats: None, // No recovery for in-memory
            encryption_provider: None,
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

    /// Returns recovery stats from the last startup, if WAL recovery was run.
    ///
    /// This is `Some` if WAL was enabled and recovery ran (even if no issues were found).
    /// Check `recovery_needed` field to see if actual crash recovery was performed.
    #[must_use]
    pub fn last_recovery_stats(&self) -> Option<&RecoveryStats> {
        self.last_recovery_stats.as_ref()
    }

    /// Returns true if crash recovery was needed on the last startup.
    ///
    /// This indicates that incomplete WAL operations were found, meaning a
    /// crash or unclean shutdown occurred before the previous session completed.
    #[must_use]
    pub fn recovery_was_needed(&self) -> bool {
        self.last_recovery_stats.as_ref().is_some_and(|s| s.recovery_needed)
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
        // Check if bucket is empty before deleting (S3 requires buckets to be empty)
        let list_result = self.list_objects(name, None, None, None, 1).await?;
        if !list_result.objects.is_empty() || !list_result.common_prefixes.is_empty() {
            return Err(Error::S3 {
                code: S3ErrorCode::BucketNotEmpty,
                message: "The bucket you tried to delete is not empty".to_string(),
                resource: Some(name.to_string()),
            });
        }

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

    async fn get_bucket(&self, name: &str) -> Result<BucketInfo> {
        self.metadata.get_bucket(name).await
    }

    async fn set_bucket_versioning(&self, name: &str, status: VersioningStatus) -> Result<()> {
        self.metadata.set_bucket_versioning(name, status).await
    }

    async fn get_bucket_cors(&self, name: &str) -> Result<Option<CorsConfiguration>> {
        self.metadata.get_bucket_cors(name).await
    }

    async fn put_bucket_cors(&self, name: &str, config: CorsConfiguration) -> Result<()> {
        self.metadata.put_bucket_cors(name, config).await
    }

    async fn delete_bucket_cors(&self, name: &str) -> Result<()> {
        self.metadata.delete_bucket_cors(name).await
    }

    async fn get_bucket_policy(&self, name: &str) -> Result<Option<String>> {
        self.metadata.get_bucket_policy(name).await
    }

    async fn put_bucket_policy(&self, name: &str, policy_json: &str) -> Result<()> {
        self.metadata.put_bucket_policy(name, policy_json).await
    }

    async fn delete_bucket_policy(&self, name: &str) -> Result<()> {
        self.metadata.delete_bucket_policy(name).await
    }

    async fn get_public_access_block(
        &self,
        name: &str,
    ) -> Result<Option<PublicAccessBlockConfiguration>> {
        self.metadata.get_public_access_block(name).await
    }

    async fn put_public_access_block(
        &self,
        name: &str,
        config: PublicAccessBlockConfiguration,
    ) -> Result<()> {
        self.metadata.put_public_access_block(name, config).await
    }

    async fn delete_public_access_block(&self, name: &str) -> Result<()> {
        self.metadata.delete_public_access_block(name).await
    }

    async fn get_lifecycle_configuration(
        &self,
        name: &str,
    ) -> Result<Option<LifecycleConfiguration>> {
        self.metadata.get_lifecycle_configuration(name).await
    }

    async fn put_lifecycle_configuration(
        &self,
        name: &str,
        config: LifecycleConfiguration,
    ) -> Result<()> {
        self.metadata.put_lifecycle_configuration(name, config).await
    }

    async fn delete_lifecycle_configuration(&self, name: &str) -> Result<()> {
        self.metadata.delete_lifecycle_configuration(name).await
    }

    async fn get_encryption_configuration(
        &self,
        name: &str,
    ) -> Result<Option<ServerSideEncryptionConfiguration>> {
        self.metadata.get_encryption_configuration(name).await
    }

    async fn put_encryption_configuration(
        &self,
        name: &str,
        config: ServerSideEncryptionConfiguration,
    ) -> Result<()> {
        self.metadata.put_encryption_configuration(name, config).await
    }

    async fn delete_encryption_configuration(&self, name: &str) -> Result<()> {
        self.metadata.delete_encryption_configuration(name).await
    }

    async fn get_replication_configuration(
        &self,
        name: &str,
    ) -> Result<Option<rucket_core::replication::ReplicationConfiguration>> {
        self.metadata.get_replication_configuration(name).await
    }

    async fn put_replication_configuration(
        &self,
        name: &str,
        config: rucket_core::replication::ReplicationConfiguration,
    ) -> Result<()> {
        self.metadata.put_replication_configuration(name, config).await
    }

    async fn delete_replication_configuration(&self, name: &str) -> Result<()> {
        self.metadata.delete_replication_configuration(name).await
    }

    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        data: Bytes,
        headers: ObjectHeaders,
        user_metadata: HashMap<String, String>,
    ) -> Result<PutObjectResult> {
        // Get bucket info (also verifies bucket exists)
        let bucket_info = self.metadata.get_bucket(bucket).await?;

        // Check if versioning is enabled and generate a version_id if so
        // For suspended versioning, we use None (stored as _current internally),
        // which allows the object to be overwritten and maps correctly when
        // deleting with versionId="null".
        let version_id = match bucket_info.versioning_status {
            Some(VersioningStatus::Enabled) => Some(Uuid::new_v4().to_string()),
            Some(VersioningStatus::Suspended) | None => None,
        };

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

        // Compute user-requested checksum if algorithm is specified
        let requested_checksum =
            headers.checksum_algorithm.map(|algorithm| compute_checksum(&data, algorithm));

        // Encrypt data if encryption is enabled
        let (data_to_write, encryption_metadata) =
            if let Some(ref provider) = self.encryption_provider {
                let (encrypted, enc_meta) = provider.encrypt(uuid, &data).map_err(|e| {
                    Error::s3(S3ErrorCode::InternalError, format!("Encryption failed: {e}"))
                })?;
                (encrypted, Some(enc_meta))
            } else {
                (data, None)
            };

        // Step 1: Write intent to WAL (if enabled)
        // This is the durability point - if we crash after this, recovery will clean up
        if let Some(wal) = &self.wal {
            let intent = WalEntry::PutIntent {
                bucket: bucket.to_string(),
                key: key.to_string(),
                uuid,
                size: data_to_write.len() as u64, // Use encrypted size for WAL
                crc32c,
                timestamp,
            };
            if let Err(e) = wal.append_sync(intent).await {
                tracing::warn!(error = %e, "Failed to write WAL intent, continuing without");
            }
        }

        // Step 2: Write to temp file and compute ETag (no sync yet - WAL protects us)
        let write_result =
            write_and_hash_with_strategy(&temp_path, &data_to_write, SyncStrategy::None).await?;

        // Check if we should sync NOW (Always mode, or threshold reached for Periodic/Threshold)
        let should_sync = self.sync_manager.should_sync_now(data_len);

        if should_sync {
            // Sync the temp file BEFORE rename for durability
            let file = fs::File::open(&temp_path).await?;
            file.sync_all().await?;
            #[cfg(test)]
            crate::sync::test_stats::record_data_sync();
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

        // Delete old object file if versioning is NOT enabled
        // For versioned buckets, we keep old files for historical versions
        if bucket_info.versioning_status.is_none() {
            if let Ok(old_meta) = self.metadata.get_object(bucket, key).await {
                let old_path = self.object_path(bucket, &old_meta.uuid);
                if let Err(e) = fs::remove_file(&old_path).await {
                    tracing::warn!(?old_path, error = %e, "Failed to cleanup old object file");
                }
            }
        }

        // Step 4: Update metadata with checksum, headers, user metadata, and version_id
        let mut meta = ObjectMetadata::new(key, uuid, data_len, write_result.etag.clone())
            .with_checksum(write_result.crc32c)
            .with_user_metadata(user_metadata);
        if let Some(ct) = headers.content_type {
            meta = meta.with_content_type(ct);
        }
        if let Some(ref vid) = version_id {
            meta = meta.with_version_id(vid);
        }
        meta.cache_control = headers.cache_control;
        meta.content_disposition = headers.content_disposition;
        meta.content_encoding = headers.content_encoding;
        meta.expires = headers.expires;
        meta.content_language = headers.content_language;

        // Set storage class if specified, otherwise use default (Standard)
        if let Some(storage_class) = headers.storage_class {
            meta.storage_class = storage_class;
        }

        // Store the user-requested checksum in metadata
        if let Some(ref checksum) = requested_checksum {
            let algorithm = checksum.algorithm();
            meta = meta.with_algorithm_checksum(algorithm, checksum.clone());
        }

        // Store encryption metadata if encryption was used
        if let Some(ref enc_meta) = encryption_metadata {
            meta = meta.with_encryption(enc_meta.algorithm.as_s3_header(), enc_meta.nonce.clone());
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

        let server_side_encryption =
            encryption_metadata.as_ref().map(|m| m.algorithm.as_s3_header().to_string());

        Ok(PutObjectResult {
            etag: write_result.etag,
            version_id,
            checksum: requested_checksum,
            server_side_encryption,
        })
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectMetadata, Bytes)> {
        let meta = self.metadata.get_object(bucket, key).await?;
        let path = self.object_path(bucket, &meta.uuid);

        let raw_data = fs::read(&path).await.map_err(|e| {
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

        // Decrypt data if the object is encrypted
        let data = if meta.is_encrypted() {
            if let Some(ref provider) = self.encryption_provider {
                // Build encryption metadata from stored values
                let enc_meta = crate::crypto::EncryptionMetadata {
                    algorithm: crate::crypto::EncryptionAlgorithm::Aes256Gcm,
                    nonce: meta.encryption_nonce.clone().unwrap_or_default(),
                };
                provider.decrypt(meta.uuid, &raw_data, &enc_meta).map_err(|e| {
                    Error::s3_with_resource(
                        S3ErrorCode::InternalError,
                        format!("Decryption failed: {e}"),
                        key,
                    )
                })?
            } else {
                // Object is encrypted but no encryption provider is configured
                return Err(Error::s3_with_resource(
                    S3ErrorCode::InternalError,
                    "Object is encrypted but server encryption is not configured",
                    key,
                ));
            }
        } else {
            Bytes::from(raw_data)
        };

        // Verify checksum if enabled and checksum is available
        // Note: CRC32C is computed on original (unencrypted) data
        if self.verify_checksums_on_read {
            if let Some(expected_crc) = meta.crc32c {
                let actual_crc = compute_crc32c(&data);
                if actual_crc != expected_crc {
                    tracing::error!(
                        bucket = bucket,
                        key = key,
                        expected = expected_crc,
                        actual = actual_crc,
                        "CRC32C checksum mismatch - data corruption detected"
                    );
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::ChecksumMismatch,
                        format!(
                            "Data integrity check failed: expected CRC32C {expected_crc:#010x}, got {actual_crc:#010x}"
                        ),
                        key,
                    ));
                }
            }
        }

        Ok((meta, data))
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

        // For encrypted objects, we must decrypt the entire file first
        // (AES-GCM doesn't support partial decryption)
        if meta.is_encrypted() {
            let raw_data = fs::read(&path).await.map_err(|e| {
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

            let decrypted = if let Some(ref provider) = self.encryption_provider {
                let enc_meta = crate::crypto::EncryptionMetadata {
                    algorithm: crate::crypto::EncryptionAlgorithm::Aes256Gcm,
                    nonce: meta.encryption_nonce.clone().unwrap_or_default(),
                };
                provider.decrypt(meta.uuid, &raw_data, &enc_meta).map_err(|e| {
                    Error::s3_with_resource(
                        S3ErrorCode::InternalError,
                        format!("Decryption failed: {e}"),
                        key,
                    )
                })?
            } else {
                return Err(Error::s3_with_resource(
                    S3ErrorCode::InternalError,
                    "Object is encrypted but server encryption is not configured",
                    key,
                ));
            };

            // Return the requested range from decrypted data
            let range_data = decrypted.slice(start as usize..(end as usize + 1));
            return Ok((meta, range_data));
        }

        // For non-encrypted objects, use efficient seek-based reading
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

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<DeleteObjectResult> {
        // Acquire per-key lock to serialize with concurrent writes
        let lock = self.get_key_lock(bucket, key);
        let _guard = lock.lock().await;

        // Check bucket versioning status
        let bucket_info = self.metadata.get_bucket(bucket).await?;

        // For versioned buckets, create a delete marker instead of permanent deletion
        if bucket_info.versioning_status == Some(VersioningStatus::Enabled) {
            let version_id = self.metadata.create_delete_marker(bucket, key).await?;
            return Ok(DeleteObjectResult { version_id: Some(version_id), is_delete_marker: true });
        }

        // For non-versioned or suspended buckets, perform permanent deletion
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
            if let Err(e) = fs::remove_file(&path).await {
                tracing::warn!(?path, error = %e, "Failed to delete object file (orphan may remain)");
            }

            // Step 3: Write delete commit to WAL
            if let Some(wal) = &self.wal {
                let commit =
                    WalEntry::DeleteCommit { bucket: bucket.to_string(), key: key.to_string() };
                if let Err(e) = wal.append(commit).await {
                    tracing::warn!(error = %e, "Failed to write WAL delete commit");
                }
            }
        }
        Ok(DeleteObjectResult::default())
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
        new_headers: Option<ObjectHeaders>,
        new_metadata: Option<HashMap<String, String>>,
    ) -> Result<ETag> {
        // Get source object
        let (src_meta, data) = self.get_object(src_bucket, src_key).await?;

        // Get source tags (these should be preserved by default)
        let src_tags = self.get_object_tagging(src_bucket, src_key).await.unwrap_or_default();

        // Merge headers: use new values if provided, otherwise preserve source
        // This handles: REPLACE (all new), COPY (all source), or partial (e.g., only storage_class)
        let headers = if let Some(new_h) = new_headers {
            ObjectHeaders {
                content_type: new_h.content_type.or_else(|| src_meta.content_type.clone()),
                cache_control: new_h.cache_control.or_else(|| src_meta.cache_control.clone()),
                content_disposition: new_h
                    .content_disposition
                    .or_else(|| src_meta.content_disposition.clone()),
                content_encoding: new_h
                    .content_encoding
                    .or_else(|| src_meta.content_encoding.clone()),
                expires: new_h.expires.or_else(|| src_meta.expires.clone()),
                content_language: new_h
                    .content_language
                    .or_else(|| src_meta.content_language.clone()),
                checksum_algorithm: new_h.checksum_algorithm.or(src_meta.checksum_algorithm),
                storage_class: new_h.storage_class.or(Some(src_meta.storage_class)),
            }
        } else {
            ObjectHeaders {
                content_type: src_meta.content_type.clone(),
                cache_control: src_meta.cache_control.clone(),
                content_disposition: src_meta.content_disposition.clone(),
                content_encoding: src_meta.content_encoding.clone(),
                expires: src_meta.expires.clone(),
                content_language: src_meta.content_language.clone(),
                checksum_algorithm: src_meta.checksum_algorithm,
                storage_class: Some(src_meta.storage_class),
            }
        };
        let metadata = new_metadata.unwrap_or(src_meta.user_metadata);

        let result = self.put_object(dst_bucket, dst_key, data, headers, metadata).await?;

        // Copy tags to destination object (tags are preserved by default per S3 spec)
        if !src_tags.tags.is_empty() {
            if let Some(ref version_id) = result.version_id {
                self.put_object_tagging_version(dst_bucket, dst_key, version_id, src_tags).await?;
            } else {
                self.put_object_tagging(dst_bucket, dst_key, src_tags).await?;
            }
        }

        Ok(result.etag)
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
        let use_delimiter = delimiter.is_some_and(|d| !d.is_empty());

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
        headers: ObjectHeaders,
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
            .create_multipart_upload(
                bucket,
                key,
                &upload_id,
                headers.content_type.as_deref(),
                user_metadata,
                headers.cache_control.as_deref(),
                headers.content_disposition.as_deref(),
                headers.content_encoding.as_deref(),
                headers.content_language.as_deref(),
                headers.expires.as_deref(),
                headers.storage_class.unwrap_or_default(),
            )
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

        // Encrypt part data if encryption is enabled
        let data_to_write = if let Some(ref provider) = self.encryption_provider {
            provider.encrypt_part(uuid, &data).map_err(|e| {
                Error::s3(S3ErrorCode::InternalError, format!("Part encryption failed: {e}"))
            })?
        } else {
            data
        };

        // Write part to file and compute hash (hash is of encrypted data if encrypted)
        let write_result =
            write_and_hash_with_strategy(&part_path, &data_to_write, SyncStrategy::None).await?;

        // Store part metadata
        // Note: If encryption is enabled, all parts are encrypted (server-side encryption)
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

        // Collect all part data (decrypting if necessary)
        let mut concatenated_data = Vec::new();
        let mut md5_concat = Vec::new();
        let mut part_count = 0u32;
        let mut plaintext_size = 0u64;

        for (_part_num, part, uuid) in &ordered_parts {
            let part_path = self.part_path(uuid);
            let raw_part_data = fs::read(&part_path).await?;

            // Accumulate part MD5 hashes for multipart ETag computation
            // The part ETag is the hex-encoded MD5, so we decode it back to bytes
            if let Ok(part_md5) = hex::decode(part.etag.as_str().trim_matches('"')) {
                md5_concat.extend_from_slice(&part_md5);
            }

            // Decrypt part if encryption is enabled
            let part_data = if let Some(ref provider) = self.encryption_provider {
                provider.decrypt_part(*uuid, &raw_part_data).map_err(|e| {
                    Error::s3(S3ErrorCode::InternalError, format!("Part decryption failed: {e}"))
                })?
            } else {
                Bytes::from(raw_part_data)
            };

            plaintext_size += part_data.len() as u64;
            concatenated_data.extend_from_slice(&part_data);
            part_count += 1;
        }

        // Encrypt the final concatenated data if encryption is enabled
        let (final_data, encryption_metadata) = if let Some(ref provider) = self.encryption_provider
        {
            let (encrypted, enc_meta) =
                provider.encrypt(final_uuid, &concatenated_data).map_err(|e| {
                    Error::s3(
                        S3ErrorCode::InternalError,
                        format!("Final object encryption failed: {e}"),
                    )
                })?;
            (encrypted, Some(enc_meta))
        } else {
            (Bytes::from(concatenated_data), None)
        };

        // Write final file
        fs::write(&temp_path, &final_data).await?;

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
            if let Err(e) = fs::remove_file(&old_path).await {
                tracing::warn!(?old_path, error = %e, "Failed to cleanup old object during multipart complete");
            }
        }

        // Store final object metadata with headers, user_metadata, and encryption info
        let mut meta = ObjectMetadata::new(key, final_uuid, plaintext_size, etag.clone())
            .with_user_metadata(upload.user_metadata);
        if let Some(ct) = upload.content_type {
            meta = meta.with_content_type(&ct);
        }
        meta.cache_control = upload.cache_control;
        meta.content_disposition = upload.content_disposition;
        meta.content_encoding = upload.content_encoding;
        meta.content_language = upload.content_language;
        meta.expires = upload.expires;
        meta.storage_class = upload.storage_class;

        // Store encryption metadata if encryption was used
        if let Some(ref enc_meta) = encryption_metadata {
            meta = meta.with_encryption(enc_meta.algorithm.as_s3_header(), enc_meta.nonce.clone());
        }

        self.metadata.put_object(bucket, meta).await?;

        // Clean up parts
        let part_uuids = self.metadata.delete_parts(upload_id).await?;
        for uuid in part_uuids {
            let part_path = self.part_path(&uuid);
            if let Err(e) = fs::remove_file(&part_path).await {
                tracing::warn!(?part_path, error = %e, "Failed to cleanup part file");
            }
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
            if let Err(e) = fs::remove_file(&part_path).await {
                tracing::warn!(?part_path, error = %e, "Failed to cleanup part file during abort");
            }
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

    async fn get_multipart_upload(&self, upload_id: &str) -> Result<MultipartUpload> {
        self.metadata.get_multipart_upload(upload_id).await
    }

    // === Versioning Operations ===

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<(ObjectMetadata, Bytes)> {
        let meta = self.metadata.get_object_version(bucket, key, version_id).await?;

        // Check if this is a delete marker
        if meta.is_delete_marker {
            return Err(Error::s3_with_resource(
                S3ErrorCode::MethodNotAllowed,
                "The specified method is not allowed against this resource",
                key,
            ));
        }

        let path = self.object_path(bucket, &meta.uuid);
        let raw_data = fs::read(&path).await.map_err(|e| {
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

        // Decrypt data if the object is encrypted
        let data = if meta.is_encrypted() {
            if let Some(ref provider) = self.encryption_provider {
                let enc_meta = crate::crypto::EncryptionMetadata {
                    algorithm: crate::crypto::EncryptionAlgorithm::Aes256Gcm,
                    nonce: meta.encryption_nonce.clone().unwrap_or_default(),
                };
                provider.decrypt(meta.uuid, &raw_data, &enc_meta).map_err(|e| {
                    Error::s3_with_resource(
                        S3ErrorCode::InternalError,
                        format!("Decryption failed: {e}"),
                        key,
                    )
                })?
            } else {
                return Err(Error::s3_with_resource(
                    S3ErrorCode::InternalError,
                    "Object is encrypted but server encryption is not configured",
                    key,
                ));
            }
        } else {
            Bytes::from(raw_data)
        };

        // Verify checksum if enabled and checksum is available
        // Note: CRC32C is computed on original (unencrypted) data
        if self.verify_checksums_on_read {
            if let Some(expected_crc) = meta.crc32c {
                let actual_crc = compute_crc32c(&data);
                if actual_crc != expected_crc {
                    tracing::error!(
                        bucket = bucket,
                        key = key,
                        version_id = version_id,
                        expected = expected_crc,
                        actual = actual_crc,
                        "CRC32C checksum mismatch - data corruption detected"
                    );
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::ChecksumMismatch,
                        format!(
                            "Data integrity check failed: expected CRC32C {expected_crc:#010x}, got {actual_crc:#010x}"
                        ),
                        key,
                    ));
                }
            }
        }

        Ok((meta, data))
    }

    async fn head_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata> {
        self.metadata.get_object_version(bucket, key, version_id).await
    }

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<DeleteObjectResult> {
        // Acquire per-key lock
        let lock = self.get_key_lock(bucket, key);
        let _guard = lock.lock().await;

        // Delete the specific version and its file
        if let Some(uuid) = self.metadata.delete_object_version(bucket, key, version_id).await? {
            let path = self.object_path(bucket, &uuid);
            if let Err(e) = fs::remove_file(&path).await {
                tracing::warn!(?path, error = %e, "Failed to delete object version file (orphan may remain)");
            }
        }

        Ok(DeleteObjectResult { version_id: Some(version_id.to_string()), is_delete_marker: false })
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
        self.metadata
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

    // === Object Tagging Operations ===

    async fn get_object_tagging(&self, bucket: &str, key: &str) -> Result<TagSet> {
        self.metadata.get_object_tagging(bucket, key).await
    }

    async fn put_object_tagging(&self, bucket: &str, key: &str, tags: TagSet) -> Result<()> {
        self.metadata.put_object_tagging(bucket, key, tags).await
    }

    async fn delete_object_tagging(&self, bucket: &str, key: &str) -> Result<()> {
        self.metadata.delete_object_tagging(bucket, key).await
    }

    async fn get_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<TagSet> {
        self.metadata.get_object_tagging_version(bucket, key, version_id).await
    }

    async fn put_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        tags: TagSet,
    ) -> Result<()> {
        self.metadata.put_object_tagging_version(bucket, key, version_id, tags).await
    }

    async fn delete_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()> {
        self.metadata.delete_object_tagging_version(bucket, key, version_id).await
    }

    async fn get_bucket_tagging(&self, bucket: &str) -> Result<TagSet> {
        self.metadata.get_bucket_tagging(bucket).await
    }

    async fn put_bucket_tagging(&self, bucket: &str, tags: TagSet) -> Result<()> {
        self.metadata.put_bucket_tagging(bucket, tags).await
    }

    async fn delete_bucket_tagging(&self, bucket: &str) -> Result<()> {
        self.metadata.delete_bucket_tagging(bucket).await
    }

    // Object Lock operations

    async fn get_bucket_lock_config(
        &self,
        bucket: &str,
    ) -> Result<Option<rucket_core::types::ObjectLockConfig>> {
        self.metadata.get_bucket_lock_config(bucket).await
    }

    async fn put_bucket_lock_config(
        &self,
        bucket: &str,
        config: rucket_core::types::ObjectLockConfig,
    ) -> Result<()> {
        self.metadata.put_bucket_lock_config(bucket, config).await
    }

    async fn get_object_retention(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<rucket_core::types::ObjectRetention>> {
        self.metadata.get_object_retention(bucket, key).await
    }

    async fn put_object_retention(
        &self,
        bucket: &str,
        key: &str,
        retention: rucket_core::types::ObjectRetention,
    ) -> Result<()> {
        self.metadata.put_object_retention(bucket, key, retention).await
    }

    async fn get_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<rucket_core::types::ObjectRetention>> {
        self.metadata.get_object_retention_version(bucket, key, version_id).await
    }

    async fn put_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        retention: rucket_core::types::ObjectRetention,
    ) -> Result<()> {
        self.metadata.put_object_retention_version(bucket, key, version_id, retention).await
    }

    async fn get_object_legal_hold(&self, bucket: &str, key: &str) -> Result<bool> {
        self.metadata.get_object_legal_hold(bucket, key).await
    }

    async fn put_object_legal_hold(&self, bucket: &str, key: &str, enabled: bool) -> Result<()> {
        self.metadata.put_object_legal_hold(bucket, key, enabled).await
    }

    async fn get_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool> {
        self.metadata.get_object_legal_hold_version(bucket, key, version_id).await
    }

    async fn put_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        enabled: bool,
    ) -> Result<()> {
        self.metadata.put_object_legal_hold_version(bucket, key, version_id, enabled).await
    }
}

#[cfg(test)]
mod tests {
    use serial_test::serial;
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
        let headers =
            ObjectHeaders { content_type: Some("text/plain".to_string()), ..Default::default() };
        let result = storage
            .put_object("test-bucket", "hello.txt", data.clone(), headers, HashMap::new())
            .await
            .unwrap();

        assert!(!result.etag.is_multipart());

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
        storage
            .put_object("test-bucket", "hello.txt", data, ObjectHeaders::default(), HashMap::new())
            .await
            .unwrap();

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
        let headers =
            ObjectHeaders { content_type: Some("text/plain".to_string()), ..Default::default() };
        storage
            .put_object("bucket1", "source.txt", data.clone(), headers, HashMap::new())
            .await
            .unwrap();

        // Copy to same bucket
        storage
            .copy_object("bucket1", "source.txt", "bucket1", "copy.txt", None, None)
            .await
            .unwrap();

        let (_, copied) = storage.get_object("bucket1", "copy.txt").await.unwrap();
        assert_eq!(copied, data);

        // Copy to different bucket
        storage
            .copy_object("bucket1", "source.txt", "bucket2", "dest.txt", None, None)
            .await
            .unwrap();

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
                    .put_object(
                        "test-bucket",
                        "same-key",
                        data,
                        ObjectHeaders::default(),
                        HashMap::new(),
                    )
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
                    .put_object("test-bucket", &key, data, ObjectHeaders::default(), HashMap::new())
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
        storage
            .put_object("test-bucket", "test-key", data1, ObjectHeaders::default(), HashMap::new())
            .await
            .unwrap();

        // Overwrite multiple times
        for i in 0..5 {
            let data = Bytes::from(format!("overwrite-{i}"));
            storage
                .put_object(
                    "test-bucket",
                    "test-key",
                    data,
                    ObjectHeaders::default(),
                    HashMap::new(),
                )
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

    #[tokio::test]
    async fn test_checksum_verification_disabled_by_default() {
        // By default, checksum verification should be off (Balanced mode)
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        let sync_config = SyncConfig::default();
        assert!(!sync_config.verify_checksums_on_read);

        let storage =
            LocalStorage::new_in_memory_with_sync(data_dir, tmp_dir, sync_config).await.unwrap();

        // Create bucket and object
        storage.create_bucket("test-bucket").await.unwrap();
        let data = Bytes::from("test data");
        let headers = ObjectHeaders::default();
        storage
            .put_object("test-bucket", "test-key", data.clone(), headers, HashMap::new())
            .await
            .unwrap();

        // Corrupt the file by modifying it directly
        let (meta, _) = storage.get_object("test-bucket", "test-key").await.unwrap();
        let file_path = storage.object_path("test-bucket", &meta.uuid);
        tokio::fs::write(&file_path, b"corrupted!").await.unwrap();

        // With verification disabled, corrupted data should be returned without error
        let result = storage.get_object("test-bucket", "test-key").await;
        assert!(result.is_ok(), "Should return OK when verification is disabled");
        let (_, returned_data) = result.unwrap();
        assert_eq!(returned_data.as_ref(), b"corrupted!");
    }

    #[tokio::test]
    async fn test_checksum_verification_enabled() {
        // With Durable mode, checksum verification should be on
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        let sync_config = SyncConfig { verify_checksums_on_read: true, ..Default::default() };

        let storage =
            LocalStorage::new_in_memory_with_sync(data_dir, tmp_dir, sync_config).await.unwrap();

        // Create bucket and object
        storage.create_bucket("test-bucket").await.unwrap();
        let data = Bytes::from("test data");
        let headers = ObjectHeaders::default();
        storage
            .put_object("test-bucket", "test-key", data.clone(), headers, HashMap::new())
            .await
            .unwrap();

        // First verify that uncorrupted read works
        let result = storage.get_object("test-bucket", "test-key").await;
        assert!(result.is_ok(), "Should return OK for uncorrupted data");

        // Now corrupt the file by modifying it directly
        let (meta, _) = storage.get_object("test-bucket", "test-key").await.unwrap();
        let file_path = storage.object_path("test-bucket", &meta.uuid);
        tokio::fs::write(&file_path, b"corrupted!").await.unwrap();

        // With verification enabled, corrupted data should return an error
        let result = storage.get_object("test-bucket", "test-key").await;
        assert!(result.is_err(), "Should return error when data is corrupted");

        let err = result.unwrap_err();
        match &err {
            rucket_core::Error::S3 { code, .. } => {
                assert_eq!(*code, S3ErrorCode::ChecksumMismatch);
            }
            _ => panic!("Expected S3 ChecksumMismatch error, got: {err:?}"),
        }
    }

    #[tokio::test]
    async fn test_durable_preset_enables_verification() {
        // DurabilityPreset::Durable should enable checksum verification
        use rucket_core::DurabilityPreset;

        let durable_config = DurabilityPreset::Durable.to_sync_config();
        assert!(
            durable_config.verify_checksums_on_read,
            "Durable preset should enable checksum verification"
        );

        let balanced_config = DurabilityPreset::Balanced.to_sync_config();
        assert!(
            !balanced_config.verify_checksums_on_read,
            "Balanced preset should not enable checksum verification"
        );

        let performance_config = DurabilityPreset::Performance.to_sync_config();
        assert!(
            !performance_config.verify_checksums_on_read,
            "Performance preset should not enable checksum verification"
        );
    }

    #[tokio::test]
    async fn test_full_recovery_mode() {
        use rucket_core::{RecoveryMode, WalConfig, WalSyncMode};

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        // Create storage with WAL enabled
        let wal_config = WalConfig {
            enabled: true,
            sync_mode: WalSyncMode::None,
            recovery_mode: RecoveryMode::Light,
            ..Default::default()
        };

        let storage = LocalStorage::with_full_config(
            data_dir.clone(),
            tmp_dir.clone(),
            SyncConfig::default(),
            RedbConfig::default(),
            wal_config,
        )
        .await
        .unwrap();

        // Create bucket and some objects
        storage.create_bucket("test-bucket").await.unwrap();
        let data = Bytes::from("test data for recovery");
        let headers = ObjectHeaders::default();
        storage
            .put_object("test-bucket", "key1", data.clone(), headers.clone(), HashMap::new())
            .await
            .unwrap();
        storage
            .put_object("test-bucket", "key2", data.clone(), headers, HashMap::new())
            .await
            .unwrap();

        // Create an orphaned file (file without metadata)
        let bucket_dir = data_dir.join("test-bucket");
        let orphan_uuid = uuid::Uuid::new_v4();
        tokio::fs::write(bucket_dir.join(format!("{orphan_uuid}.dat")), b"orphan data")
            .await
            .unwrap();

        // Drop storage to release locks
        drop(storage);

        // Reopen with Full recovery mode - should scan orphans and verify checksums
        let wal_config_full = WalConfig {
            enabled: true,
            sync_mode: WalSyncMode::None,
            recovery_mode: RecoveryMode::Full,
            ..Default::default()
        };

        let storage = LocalStorage::with_full_config(
            data_dir,
            tmp_dir,
            SyncConfig::default(),
            RedbConfig::default(),
            wal_config_full,
        )
        .await
        .unwrap();

        // Verify objects are still accessible
        let result = storage.get_object("test-bucket", "key1").await;
        assert!(result.is_ok(), "Should be able to read object after full recovery");

        let result = storage.get_object("test-bucket", "key2").await;
        assert!(result.is_ok(), "Should be able to read second object after full recovery");
    }

    #[tokio::test]
    async fn test_recovery_mode_config() {
        use rucket_core::{RecoveryMode, WalConfig};

        // Light mode is default
        let default_config = WalConfig::default();
        assert_eq!(
            default_config.recovery_mode,
            RecoveryMode::Light,
            "Default recovery mode should be Light"
        );

        // Full mode can be set
        let full_config = WalConfig { recovery_mode: RecoveryMode::Full, ..Default::default() };
        assert_eq!(full_config.recovery_mode, RecoveryMode::Full);
    }

    #[tokio::test]
    async fn test_verify_all_checksums_detects_corruption() {
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        // Create storage and add objects
        let storage = LocalStorage::new(data_dir.clone(), tmp_dir).await.unwrap();

        storage.create_bucket("test-bucket").await.unwrap();

        // Add multiple objects
        for i in 0..3 {
            let data = Bytes::from(format!("test data {i}"));
            let headers = ObjectHeaders::default();
            storage
                .put_object("test-bucket", &format!("key{i}"), data, headers, HashMap::new())
                .await
                .unwrap();
        }

        // Verify all checksums pass initially (use storage's metadata)
        let (verified, mismatches) =
            LocalStorage::verify_all_checksums(&data_dir, &storage.metadata).await.unwrap();
        assert_eq!(verified, 3, "Should verify 3 objects");
        assert_eq!(mismatches, 0, "Should have no mismatches initially");

        // Corrupt one of the files
        let (meta, _) = storage.get_object("test-bucket", "key1").await.unwrap();
        let file_path = storage.object_path("test-bucket", &meta.uuid);
        tokio::fs::write(&file_path, b"corrupted data!").await.unwrap();

        // Verify checksums - should detect the corruption
        let (verified, mismatches) =
            LocalStorage::verify_all_checksums(&data_dir, &storage.metadata).await.unwrap();
        assert_eq!(verified, 3, "Should still verify 3 objects");
        assert_eq!(mismatches, 1, "Should detect 1 checksum mismatch");
    }

    #[tokio::test]
    async fn test_orphan_detection_during_recovery() {
        use crate::wal::RecoveryManager;

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");
        let wal_dir = data_dir.join("wal");

        // Create storage and add an object
        let storage = LocalStorage::new(data_dir.clone(), tmp_dir).await.unwrap();

        storage.create_bucket("test-bucket").await.unwrap();
        let data = Bytes::from("legitimate data");
        let headers = ObjectHeaders::default();
        storage
            .put_object("test-bucket", "legit-key", data, headers, HashMap::new())
            .await
            .unwrap();

        // Create orphaned files (files without metadata)
        let bucket_dir = data_dir.join("test-bucket");
        let orphan_uuid1 = uuid::Uuid::new_v4();
        let orphan_uuid2 = uuid::Uuid::new_v4();
        tokio::fs::write(bucket_dir.join(format!("{orphan_uuid1}.dat")), b"orphan 1")
            .await
            .unwrap();
        tokio::fs::write(bucket_dir.join(format!("{orphan_uuid2}.dat")), b"orphan 2")
            .await
            .unwrap();

        // Get the metadata backend for UUID checking
        let metadata = storage.metadata.clone();

        // Create recovery manager and scan for orphans
        let recovery = RecoveryManager::new(wal_dir, data_dir);
        let orphans = recovery
            .scan_orphans(|bucket, uuid| metadata.uuid_exists_sync(bucket, uuid))
            .await
            .unwrap();

        // Should find exactly 2 orphaned files
        assert_eq!(orphans.len(), 2, "Should detect 2 orphaned files");

        // Verify the orphan paths contain our orphan UUIDs
        let orphan_paths: Vec<String> =
            orphans.iter().map(|p| p.to_string_lossy().to_string()).collect();
        assert!(
            orphan_paths.iter().any(|p| p.contains(&orphan_uuid1.to_string())),
            "Should find orphan 1"
        );
        assert!(
            orphan_paths.iter().any(|p| p.contains(&orphan_uuid2.to_string())),
            "Should find orphan 2"
        );
    }

    #[tokio::test]
    async fn test_full_recovery_with_corruption_and_orphans() {
        use rucket_core::{RecoveryMode, WalConfig, WalSyncMode};

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        // Phase 1: Create storage with data
        let wal_config = WalConfig {
            enabled: true,
            sync_mode: WalSyncMode::None,
            recovery_mode: RecoveryMode::Light,
            ..Default::default()
        };

        let storage = LocalStorage::with_full_config(
            data_dir.clone(),
            tmp_dir.clone(),
            SyncConfig::default(),
            RedbConfig::default(),
            wal_config,
        )
        .await
        .unwrap();

        storage.create_bucket("test-bucket").await.unwrap();

        // Add objects
        for i in 0..5 {
            let data = Bytes::from(format!("object data {i}"));
            let headers = ObjectHeaders::default();
            storage
                .put_object(
                    "test-bucket",
                    &format!("obj{i}"),
                    data,
                    headers.clone(),
                    HashMap::new(),
                )
                .await
                .unwrap();
        }

        // Get metadata for one object before dropping storage
        let (meta_to_corrupt, _) = storage.get_object("test-bucket", "obj2").await.unwrap();
        let file_to_corrupt = storage.object_path("test-bucket", &meta_to_corrupt.uuid);

        drop(storage);

        // Phase 2: Create corruption and orphans while storage is closed
        // Corrupt one file
        tokio::fs::write(&file_to_corrupt, b"CORRUPTED!!!").await.unwrap();

        // Create orphan files
        let bucket_dir = data_dir.join("test-bucket");
        let orphan_uuid = uuid::Uuid::new_v4();
        tokio::fs::write(bucket_dir.join(format!("{orphan_uuid}.dat")), b"orphan").await.unwrap();

        // Phase 3: Reopen with Full recovery mode
        // This should scan for orphans and verify checksums
        let wal_config_full = WalConfig {
            enabled: true,
            sync_mode: WalSyncMode::None,
            recovery_mode: RecoveryMode::Full,
            ..Default::default()
        };

        // Full recovery should complete without panic
        // (It logs errors but doesn't fail startup)
        let storage = LocalStorage::with_full_config(
            data_dir,
            tmp_dir,
            SyncConfig::default(),
            RedbConfig::default(),
            wal_config_full,
        )
        .await
        .unwrap();

        // Verify uncorrupted objects are still accessible
        let result = storage.get_object("test-bucket", "obj0").await;
        assert!(result.is_ok(), "Uncorrupted object should be readable");

        let result = storage.get_object("test-bucket", "obj4").await;
        assert!(result.is_ok(), "Another uncorrupted object should be readable");

        // The corrupted object is still readable (corruption detected at recovery time, not blocked)
        // But if verify_checksums_on_read were enabled, it would fail on read
        let result = storage.get_object("test-bucket", "obj2").await;
        assert!(result.is_ok(), "Corrupted object still readable when verify disabled");

        // Verify recovery stats are available and show what was detected
        let stats = storage.last_recovery_stats().expect("should have recovery stats");
        assert_eq!(stats.orphans_found, 1, "Should detect 1 orphan");
        assert_eq!(stats.checksum_mismatches, 1, "Should detect 1 checksum mismatch");
        assert!(stats.objects_verified >= 5, "Should have verified at least 5 objects");
    }

    #[tokio::test]
    async fn test_crash_detection_with_incomplete_wal() {
        use rucket_core::{RecoveryMode, WalConfig, WalSyncMode};

        use crate::wal::{WalEntry, WalWriter, WalWriterConfig};

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");
        let wal_dir = data_dir.join("wal");

        // Create WAL directory and write an incomplete operation (simulating crash)
        std::fs::create_dir_all(&wal_dir).unwrap();
        std::fs::create_dir_all(&data_dir).unwrap();

        let uuid = uuid::Uuid::new_v4();
        {
            let writer_config = WalWriterConfig {
                wal_dir: wal_dir.clone(),
                sync_mode: crate::wal::WalSyncMode::None,
                ..Default::default()
            };
            let writer = WalWriter::open(&writer_config).unwrap();

            // Write a PutIntent without a matching PutCommit (simulating crash)
            writer
                .append(WalEntry::PutIntent {
                    bucket: "test-bucket".to_string(),
                    key: "orphaned-key".to_string(),
                    uuid,
                    size: 100,
                    crc32c: 12345,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
            // Intentionally NOT writing PutCommit - simulating crash
        }

        // Create the orphaned file that the incomplete PUT was writing
        let bucket_dir = data_dir.join("test-bucket");
        std::fs::create_dir_all(&bucket_dir).unwrap();
        std::fs::write(bucket_dir.join(format!("{uuid}.dat")), b"incomplete write").unwrap();

        // Now open storage - it should detect the crash and recover
        let wal_config = WalConfig {
            enabled: true,
            sync_mode: WalSyncMode::None,
            recovery_mode: RecoveryMode::Light,
            ..Default::default()
        };

        let storage = LocalStorage::with_full_config(
            data_dir.clone(),
            tmp_dir,
            SyncConfig::default(),
            RedbConfig::default(),
            wal_config,
        )
        .await
        .unwrap();

        // Verify crash was detected
        assert!(storage.recovery_was_needed(), "Should detect that crash recovery was needed");

        let stats = storage.last_recovery_stats().expect("should have recovery stats");
        assert!(stats.recovery_needed, "recovery_needed should be true");
        assert_eq!(stats.puts_rolled_back, 1, "Should have rolled back 1 incomplete PUT");

        // The orphaned file should have been cleaned up
        assert!(
            !bucket_dir.join(format!("{uuid}.dat")).exists(),
            "Orphaned file should be deleted during recovery"
        );
    }

    #[tokio::test]
    async fn test_clean_shutdown_no_recovery_needed() {
        use rucket_core::{RecoveryMode, WalConfig, WalSyncMode};

        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        // Create storage with WAL enabled
        let wal_config = WalConfig {
            enabled: true,
            sync_mode: WalSyncMode::None,
            recovery_mode: RecoveryMode::Light,
            ..Default::default()
        };

        let storage = LocalStorage::with_full_config(
            data_dir.clone(),
            tmp_dir.clone(),
            SyncConfig::default(),
            RedbConfig::default(),
            wal_config.clone(),
        )
        .await
        .unwrap();

        // Do some normal operations (these will complete properly)
        storage.create_bucket("test-bucket").await.unwrap();
        let data = Bytes::from("test data");
        let headers = ObjectHeaders::default();
        storage.put_object("test-bucket", "key1", data, headers, HashMap::new()).await.unwrap();

        // Drop storage (clean shutdown)
        drop(storage);

        // Reopen storage
        let storage = LocalStorage::with_full_config(
            data_dir,
            tmp_dir,
            SyncConfig::default(),
            RedbConfig::default(),
            wal_config,
        )
        .await
        .unwrap();

        // Should NOT need recovery (clean shutdown)
        assert!(!storage.recovery_was_needed(), "Clean shutdown should not require recovery");

        let stats = storage.last_recovery_stats().expect("should have recovery stats");
        assert!(!stats.recovery_needed, "recovery_needed should be false");
        assert_eq!(stats.puts_rolled_back, 0, "No operations should be rolled back");
        assert_eq!(stats.deletes_rolled_back, 0, "No operations should be rolled back");
    }

    #[tokio::test]
    #[serial]
    async fn test_sync_counters_in_always_mode() {
        use crate::sync::test_stats;

        test_stats::reset();
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        // Use Always sync mode to trigger both file and directory sync
        let sync_config = SyncConfig { data: SyncStrategy::Always, ..Default::default() };

        let storage = LocalStorage::with_full_config(
            data_dir,
            tmp_dir,
            sync_config,
            RedbConfig::default(),
            WalConfig::disabled(),
        )
        .await
        .unwrap();

        storage.create_bucket("test").await.unwrap();

        // Put an object - should trigger file sync and dir sync
        storage
            .put_object(
                "test",
                "key1",
                Bytes::from("test data"),
                Default::default(),
                HashMap::new(),
            )
            .await
            .unwrap();

        // Verify syncs were recorded
        // File sync happens in streaming.rs (Always mode) and local.rs (threshold)
        let data_syncs = test_stats::data_sync_count();
        assert!(data_syncs >= 1, "Always mode should trigger file sync: got {data_syncs}");

        // Directory sync happens in Always mode
        let dir_syncs = test_stats::dir_sync_count();
        assert!(dir_syncs >= 1, "Always mode should trigger directory sync: got {dir_syncs}");
    }

    #[tokio::test]
    #[serial]
    async fn test_sync_counters_threshold_triggered() {
        use crate::sync::test_stats;

        test_stats::reset();
        let temp_dir = TempDir::new().unwrap();
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        // Use Threshold mode with low threshold to trigger sync
        let sync_config = SyncConfig {
            data: SyncStrategy::Threshold,
            bytes_threshold: 10, // Very low threshold
            ops_threshold: 2,    // Trigger on 2nd op
            ..Default::default()
        };

        let storage = LocalStorage::with_full_config(
            data_dir,
            tmp_dir,
            sync_config,
            RedbConfig::default(),
            WalConfig::disabled(),
        )
        .await
        .unwrap();

        storage.create_bucket("test").await.unwrap();

        let initial_syncs = test_stats::data_sync_count();

        // First put - may or may not trigger depending on byte count
        storage
            .put_object("test", "key1", Bytes::from("data1"), Default::default(), HashMap::new())
            .await
            .unwrap();

        // Second put - should definitely trigger threshold
        storage
            .put_object(
                "test",
                "key2",
                Bytes::from("data2 with more bytes to exceed threshold"),
                Default::default(),
                HashMap::new(),
            )
            .await
            .unwrap();

        // Verify threshold-triggered sync happened
        let final_syncs = test_stats::data_sync_count();
        assert!(
            final_syncs > initial_syncs,
            "Threshold mode should trigger sync when threshold exceeded: got {final_syncs} (was {initial_syncs})"
        );
    }

    #[tokio::test]
    async fn test_object_tagging_put_get_delete() {
        use rucket_core::types::{Tag, TagSet};

        let (storage, _temp) = create_test_storage().await;
        storage.create_bucket("test-bucket").await.unwrap();

        // Create an object first
        let data = Bytes::from("test data");
        storage
            .put_object("test-bucket", "test-key", data, ObjectHeaders::default(), HashMap::new())
            .await
            .unwrap();

        // Initially no tags
        let tags = storage.get_object_tagging("test-bucket", "test-key").await.unwrap();
        assert!(tags.is_empty());

        // Put tags
        let tag_set =
            TagSet::with_tags(vec![Tag::new("env", "test"), Tag::new("project", "rucket")]);
        storage.put_object_tagging("test-bucket", "test-key", tag_set).await.unwrap();

        // Get tags
        let retrieved = storage.get_object_tagging("test-bucket", "test-key").await.unwrap();
        assert_eq!(retrieved.len(), 2);

        // Delete tags
        storage.delete_object_tagging("test-bucket", "test-key").await.unwrap();
        let tags = storage.get_object_tagging("test-bucket", "test-key").await.unwrap();
        assert!(tags.is_empty());
    }

    #[tokio::test]
    async fn test_object_tagging_versioned() {
        use rucket_core::types::{Tag, TagSet, VersioningStatus};

        let (storage, _temp) = create_test_storage().await;
        storage.create_bucket("test-bucket").await.unwrap();
        storage.set_bucket_versioning("test-bucket", VersioningStatus::Enabled).await.unwrap();

        // Create v1
        let data = Bytes::from("version 1");
        let meta1 = storage
            .put_object("test-bucket", "test-key", data, ObjectHeaders::default(), HashMap::new())
            .await
            .unwrap();
        let v1 = meta1.version_id.clone().unwrap();

        // Create v2
        let data = Bytes::from("version 2");
        let meta2 = storage
            .put_object("test-bucket", "test-key", data, ObjectHeaders::default(), HashMap::new())
            .await
            .unwrap();
        let v2 = meta2.version_id.clone().unwrap();

        // Tag v1
        let tags_v1 = TagSet::with_tags(vec![Tag::new("version", "1")]);
        storage.put_object_tagging_version("test-bucket", "test-key", &v1, tags_v1).await.unwrap();

        // Tag v2
        let tags_v2 = TagSet::with_tags(vec![Tag::new("version", "2")]);
        storage.put_object_tagging_version("test-bucket", "test-key", &v2, tags_v2).await.unwrap();

        // Get v1 tags
        let retrieved_v1 =
            storage.get_object_tagging_version("test-bucket", "test-key", &v1).await.unwrap();
        assert_eq!(retrieved_v1.len(), 1);
        assert_eq!(retrieved_v1.tags[0].value, "1");

        // Get v2 tags
        let retrieved_v2 =
            storage.get_object_tagging_version("test-bucket", "test-key", &v2).await.unwrap();
        assert_eq!(retrieved_v2.len(), 1);
        assert_eq!(retrieved_v2.tags[0].value, "2");

        // Delete v1 tags
        storage.delete_object_tagging_version("test-bucket", "test-key", &v1).await.unwrap();
        let tags =
            storage.get_object_tagging_version("test-bucket", "test-key", &v1).await.unwrap();
        assert!(tags.is_empty());

        // v2 tags should still exist
        let tags =
            storage.get_object_tagging_version("test-bucket", "test-key", &v2).await.unwrap();
        assert_eq!(tags.len(), 1);
    }
}
