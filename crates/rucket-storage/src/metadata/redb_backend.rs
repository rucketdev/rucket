//! redb-based metadata storage backend.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use redb::{Database, Durability, ReadableDatabase, ReadableTable, TableDefinition};
use rucket_core::error::{Error, S3ErrorCode};
use rucket_core::types::{
    BucketInfo, ETag, MultipartUpload, ObjectMetadata, Part, Tag, TagSet, VersioningStatus,
};
use rucket_core::{Result, SyncStrategy};
use serde::{Deserialize, Serialize};
use tracing::debug;
use uuid::Uuid;

use super::MetadataBackend;

// === Table Definitions ===

/// Buckets table: bucket_name -> StoredBucketInfo (bincode)
const BUCKETS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("buckets");

/// Objects table: composite key "bucket\0key" -> StoredObjectMetadata (bincode)
const OBJECTS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("objects");

/// Multipart uploads table: upload_id -> StoredMultipartUpload (bincode)
const MULTIPART_UPLOADS: TableDefinition<'_, &str, &[u8]> =
    TableDefinition::new("multipart_uploads");

/// Parts table: composite key "upload_id\0part_number" -> StoredPart (bincode)
const PARTS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("parts");

/// Object tagging table: composite key "bucket\0key" or "bucket\0key\0version" -> StoredTagSet (bincode)
const OBJECT_TAGGING: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("object_tagging");

// === Stored Types (for bincode serialization) ===

#[derive(Serialize, Deserialize)]
struct StoredBucketInfo {
    name: String,
    created_at_millis: i64,
    /// Versioning status: None = never enabled, Some("Enabled") or Some("Suspended").
    #[serde(default)]
    versioning_status: Option<String>,
}

impl StoredBucketInfo {
    fn from_bucket_info(info: &BucketInfo) -> Self {
        Self {
            name: info.name.clone(),
            created_at_millis: info.created_at.timestamp_millis(),
            versioning_status: info.versioning_status.map(|s| s.as_str().to_string()),
        }
    }

    fn to_bucket_info(&self) -> BucketInfo {
        BucketInfo {
            name: self.name.clone(),
            created_at: Utc
                .timestamp_millis_opt(self.created_at_millis)
                .single()
                .unwrap_or_else(Utc::now),
            versioning_status: self
                .versioning_status
                .as_ref()
                .and_then(|s| VersioningStatus::parse(s)),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StoredObjectMetadata {
    key: String,
    uuid: [u8; 16],
    size: u64,
    etag: String,
    /// CRC32C checksum for data integrity verification.
    /// Added in v0.2.0 - defaults to None for backward compatibility with older data.
    #[serde(default)]
    crc32c: Option<u32>,
    content_type: Option<String>,
    #[serde(default)]
    cache_control: Option<String>,
    #[serde(default)]
    content_disposition: Option<String>,
    #[serde(default)]
    content_encoding: Option<String>,
    #[serde(default)]
    expires: Option<String>,
    #[serde(default)]
    content_language: Option<String>,
    last_modified_millis: i64,
    user_metadata: Vec<(String, String)>,
    /// Version ID for this object (None = "null" for non-versioned buckets).
    #[serde(default)]
    version_id: Option<String>,
    /// Whether this is a delete marker (for versioned buckets).
    #[serde(default)]
    is_delete_marker: bool,
    /// Whether this is the latest version of the object.
    /// Defaults to true for backward compatibility with pre-versioning data.
    #[serde(default = "default_is_latest")]
    is_latest: bool,
}

fn default_is_latest() -> bool {
    true
}

impl StoredObjectMetadata {
    fn from_object_metadata(meta: &ObjectMetadata) -> Self {
        Self {
            key: meta.key.clone(),
            uuid: *meta.uuid.as_bytes(),
            size: meta.size,
            etag: meta.etag.as_str().to_string(),
            crc32c: meta.crc32c,
            content_type: meta.content_type.clone(),
            cache_control: meta.cache_control.clone(),
            content_disposition: meta.content_disposition.clone(),
            content_encoding: meta.content_encoding.clone(),
            expires: meta.expires.clone(),
            content_language: meta.content_language.clone(),
            last_modified_millis: meta.last_modified.timestamp_millis(),
            user_metadata: meta.user_metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            version_id: meta.version_id.clone(),
            is_delete_marker: meta.is_delete_marker,
            is_latest: meta.is_latest,
        }
    }

    fn to_object_metadata(&self) -> ObjectMetadata {
        ObjectMetadata {
            key: self.key.clone(),
            uuid: Uuid::from_bytes(self.uuid),
            size: self.size,
            etag: ETag::new(&self.etag),
            crc32c: self.crc32c,
            content_type: self.content_type.clone(),
            cache_control: self.cache_control.clone(),
            content_disposition: self.content_disposition.clone(),
            content_encoding: self.content_encoding.clone(),
            expires: self.expires.clone(),
            content_language: self.content_language.clone(),
            last_modified: Utc
                .timestamp_millis_opt(self.last_modified_millis)
                .single()
                .unwrap_or_else(Utc::now),
            user_metadata: self.user_metadata.iter().cloned().collect(),
            version_id: self.version_id.clone(),
            is_delete_marker: self.is_delete_marker,
            is_latest: self.is_latest,
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StoredMultipartUpload {
    upload_id: String,
    bucket: String,
    key: String,
    initiated_millis: i64,
    #[serde(default)]
    content_type: Option<String>,
    #[serde(default)]
    user_metadata: HashMap<String, String>,
}

impl StoredMultipartUpload {
    fn from_multipart_upload(upload: &MultipartUpload) -> Self {
        Self {
            upload_id: upload.upload_id.clone(),
            bucket: upload.bucket.clone(),
            key: upload.key.clone(),
            initiated_millis: upload.initiated.timestamp_millis(),
            content_type: upload.content_type.clone(),
            user_metadata: upload.user_metadata.clone(),
        }
    }

    fn to_multipart_upload(&self) -> MultipartUpload {
        MultipartUpload {
            upload_id: self.upload_id.clone(),
            bucket: self.bucket.clone(),
            key: self.key.clone(),
            initiated: Utc
                .timestamp_millis_opt(self.initiated_millis)
                .single()
                .unwrap_or_else(Utc::now),
            content_type: self.content_type.clone(),
            user_metadata: self.user_metadata.clone(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StoredPart {
    part_number: u32,
    uuid: [u8; 16],
    etag: String,
    size: u64,
    last_modified_millis: i64,
}

impl StoredPart {
    fn from_part(part: &Part, uuid: Uuid) -> Self {
        Self {
            part_number: part.part_number,
            uuid: *uuid.as_bytes(),
            etag: part.etag.as_str().to_string(),
            size: part.size,
            last_modified_millis: part.last_modified.timestamp_millis(),
        }
    }

    fn to_part(&self) -> Part {
        Part {
            part_number: self.part_number,
            etag: ETag::new(&self.etag),
            size: self.size,
            last_modified: Utc
                .timestamp_millis_opt(self.last_modified_millis)
                .single()
                .unwrap_or_else(Utc::now),
        }
    }

    fn uuid(&self) -> Uuid {
        Uuid::from_bytes(self.uuid)
    }
}

/// Stored representation of a tag set.
#[derive(Serialize, Deserialize)]
struct StoredTagSet {
    tags: Vec<(String, String)>,
}

impl StoredTagSet {
    fn from_tagset(tagset: &TagSet) -> Self {
        Self { tags: tagset.tags.iter().map(|t| (t.key.clone(), t.value.clone())).collect() }
    }

    fn to_tagset(&self) -> TagSet {
        TagSet::with_tags(self.tags.iter().map(|(k, v)| Tag::new(k, v)).collect())
    }
}

/// Convert any error with Display to our Error type.
fn db_err(e: impl std::fmt::Display) -> Error {
    Error::Database(e.to_string())
}

/// redb-based metadata storage.
pub struct RedbMetadataStore {
    db: Arc<Database>,
    durability: Durability,
}

impl RedbMetadataStore {
    /// Open or create a redb database at the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn open(path: &Path, sync_strategy: SyncStrategy) -> Result<Self> {
        debug!(?path, ?sync_strategy, "Opening redb metadata store");

        let db = Database::create(path).map_err(db_err)?;

        // Initialize tables by opening them in a write transaction
        // This ensures tables exist before any read operations
        {
            let txn = db.begin_write().map_err(db_err)?;
            let _ = txn.open_table(BUCKETS).map_err(db_err)?;
            let _ = txn.open_table(OBJECTS).map_err(db_err)?;
            let _ = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;
            let _ = txn.open_table(PARTS).map_err(db_err)?;
            let _ = txn.open_table(OBJECT_TAGGING).map_err(db_err)?;
            txn.commit().map_err(db_err)?;
        }

        let durability = Self::sync_to_durability(sync_strategy);

        Ok(Self { db: Arc::new(db), durability })
    }

    /// Open or create a redb database with cache size configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened.
    pub fn open_with_cache(
        path: &Path,
        sync_strategy: SyncStrategy,
        _cache_size_bytes: u64,
    ) -> Result<Self> {
        // Note: redb 2.x doesn't have a cache_size builder method like earlier versions
        // The cache is managed internally by redb
        Self::open(path, sync_strategy)
    }

    /// Open an in-memory database for testing.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be created.
    pub fn open_in_memory() -> Result<Self> {
        let db = Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(db_err)?;

        // Initialize tables
        {
            let txn = db.begin_write().map_err(db_err)?;
            let _ = txn.open_table(BUCKETS).map_err(db_err)?;
            let _ = txn.open_table(OBJECTS).map_err(db_err)?;
            let _ = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;
            let _ = txn.open_table(PARTS).map_err(db_err)?;
            let _ = txn.open_table(OBJECT_TAGGING).map_err(db_err)?;
            txn.commit().map_err(db_err)?;
        }

        Ok(Self { db: Arc::new(db), durability: Durability::None })
    }

    /// Map SyncStrategy to redb Durability.
    fn sync_to_durability(strategy: SyncStrategy) -> Durability {
        match strategy {
            SyncStrategy::Always => Durability::Immediate,
            // redb 3.x only has None and Immediate; None commits are batched
            // until an Immediate commit flushes them to disk
            SyncStrategy::None | SyncStrategy::Periodic | SyncStrategy::Threshold => {
                Durability::None
            }
        }
    }

    /// Create a composite key for objects with version: "bucket\0key\0version_id"
    ///
    /// For versioned buckets, the version_id is the actual version ID (UUID or "null").
    /// For non-versioned buckets, we use a placeholder version ID ("_current") to maintain
    /// compatibility with the three-part key structure.
    fn object_version_key(bucket: &str, key: &str, version_id: &str) -> String {
        format!("{}\0{}\0{}", bucket, key, version_id)
    }

    /// Create a prefix for scanning all versions of an object: "bucket\0key\0"
    fn object_key_prefix(bucket: &str, key: &str) -> String {
        format!("{}\0{}\0", bucket, key)
    }

    /// Parse a composite object key back to (bucket, key, version_id)
    fn parse_object_version_key(composite: &str) -> Option<(&str, &str, &str)> {
        let mut parts = composite.splitn(3, '\0');
        let bucket = parts.next()?;
        let key = parts.next()?;
        let version_id = parts.next()?;
        Some((bucket, key, version_id))
    }

    /// Legacy key format for backward compatibility: "bucket\0key"
    /// Used for reading old data that doesn't have version_id in the key.
    fn legacy_object_key(bucket: &str, key: &str) -> String {
        format!("{}\0{}", bucket, key)
    }

    /// Parse a legacy composite object key back to (bucket, key)
    fn parse_legacy_object_key(composite: &str) -> Option<(&str, &str)> {
        composite.split_once('\0')
    }

    /// Create a composite key for parts: "upload_id\0part_number"
    fn part_key(upload_id: &str, part_number: u32) -> String {
        format!("{}\0{:010}", upload_id, part_number)
    }

    /// The placeholder version ID for non-versioned buckets.
    const CURRENT_VERSION: &'static str = "_current";
}

#[async_trait]
impl MetadataBackend for RedbMetadataStore {
    async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        let name = name.to_string();
        let now = Utc::now();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            {
                let mut table = txn.open_table(BUCKETS).map_err(db_err)?;

                // Check if bucket already exists
                if table.get(name.as_str()).map_err(db_err)?.is_some() {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::BucketAlreadyExists,
                        "The bucket already exists",
                        name,
                    ));
                }

                let info =
                    BucketInfo { name: name.clone(), created_at: now, versioning_status: None };
                let stored = StoredBucketInfo::from_bucket_info(&info);
                let serialized = bincode::serialize(&stored).map_err(db_err)?;

                table.insert(name.as_str(), serialized.as_slice()).map_err(db_err)?;
            }

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(BucketInfo { name, created_at: now, versioning_status: None })
        })
        .await
        .map_err(db_err)?
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            {
                let mut buckets_table = txn.open_table(BUCKETS).map_err(db_err)?;

                // Check if bucket exists
                let bucket_exists = buckets_table.get(name.as_str()).map_err(db_err)?.is_some();
                if !bucket_exists {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        name,
                    ));
                }

                // Auto-cleanup all bucket contents
                let mut objects_table = txn.open_table(OBJECTS).map_err(db_err)?;

                let prefix = format!("{}\0", name);
                let end = format!("{}\x01", name);

                // Auto-cleanup all objects when deleting a bucket
                // This enables tools like mc, minio-js etc. that expect force-delete behavior
                // Collect keys to delete first (to avoid borrow issues)
                let range = objects_table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;
                let keys_to_delete: Vec<String> = range
                    .filter_map(|entry| entry.ok().map(|(k, _)| k.value().to_string()))
                    .collect();

                // Delete all object versions and delete markers
                for key in keys_to_delete {
                    objects_table.remove(key.as_str()).map_err(db_err)?;
                }

                // Also clean up any multipart uploads for this bucket
                let mut uploads_table = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;
                let upload_range =
                    uploads_table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;
                let uploads_to_delete: Vec<String> = upload_range
                    .filter_map(|entry| entry.ok().map(|(k, _)| k.value().to_string()))
                    .collect();
                for key in uploads_to_delete {
                    uploads_table.remove(key.as_str()).map_err(db_err)?;
                }

                // Clean up any parts for this bucket
                let mut parts_table = txn.open_table(PARTS).map_err(db_err)?;
                let parts_range =
                    parts_table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;
                let parts_to_delete: Vec<String> = parts_range
                    .filter_map(|entry| entry.ok().map(|(k, _)| k.value().to_string()))
                    .collect();
                for key in parts_to_delete {
                    parts_table.remove(key.as_str()).map_err(db_err)?;
                }

                buckets_table.remove(name.as_str()).map_err(db_err)?;
            }

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(())
        })
        .await
        .map_err(db_err)?
    }

    async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;
            let table = txn.open_table(BUCKETS).map_err(db_err)?;

            let exists = table.get(name.as_str()).map_err(db_err)?.is_some();

            Ok(exists)
        })
        .await
        .map_err(db_err)?
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;
            let table = txn.open_table(BUCKETS).map_err(db_err)?;

            let mut buckets = Vec::new();
            for entry in table.iter().map_err(db_err)? {
                let (_, value) = entry.map_err(db_err)?;
                let stored: StoredBucketInfo =
                    bincode::deserialize(value.value()).map_err(db_err)?;
                buckets.push(stored.to_bucket_info());
            }

            // Sort by name
            buckets.sort_by(|a, b| a.name.cmp(&b.name));

            Ok(buckets)
        })
        .await
        .map_err(db_err)?
    }

    async fn get_bucket(&self, name: &str) -> Result<BucketInfo> {
        let name = name.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;
            let table = txn.open_table(BUCKETS).map_err(db_err)?;

            match table.get(name.as_str()).map_err(db_err)? {
                Some(value) => {
                    let stored: StoredBucketInfo =
                        bincode::deserialize(value.value()).map_err(db_err)?;
                    Ok(stored.to_bucket_info())
                }
                None => Err(Error::s3_with_resource(
                    S3ErrorCode::NoSuchBucket,
                    "The specified bucket does not exist",
                    name,
                )),
            }
        })
        .await
        .map_err(db_err)?
    }

    async fn set_bucket_versioning(&self, name: &str, status: VersioningStatus) -> Result<()> {
        let name = name.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            {
                let mut table = txn.open_table(BUCKETS).map_err(db_err)?;

                // Get existing bucket info - extract data and drop the guard
                let mut bucket_info = {
                    let existing = table.get(name.as_str()).map_err(db_err)?;
                    match existing {
                        Some(value) => {
                            let stored: StoredBucketInfo =
                                bincode::deserialize(value.value()).map_err(db_err)?;
                            stored.to_bucket_info()
                        }
                        None => {
                            return Err(Error::s3_with_resource(
                                S3ErrorCode::NoSuchBucket,
                                "The specified bucket does not exist",
                                name,
                            ));
                        }
                    }
                };

                // Update versioning status
                bucket_info.versioning_status = Some(status);

                // Store updated info
                let stored = StoredBucketInfo::from_bucket_info(&bucket_info);
                let serialized = bincode::serialize(&stored).map_err(db_err)?;
                table.insert(name.as_str(), serialized.as_slice()).map_err(db_err)?;
            }

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(())
        })
        .await
        .map_err(db_err)?
    }

    async fn put_object(&self, bucket: &str, meta: ObjectMetadata) -> Result<()> {
        let bucket = bucket.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            {
                // Check bucket exists and get versioning status
                let buckets_table = txn.open_table(BUCKETS).map_err(db_err)?;

                let bucket_info = match buckets_table.get(bucket.as_str()).map_err(db_err)? {
                    Some(value) => {
                        let stored: StoredBucketInfo =
                            bincode::deserialize(value.value()).map_err(db_err)?;
                        stored.to_bucket_info()
                    }
                    None => {
                        return Err(Error::s3_with_resource(
                            S3ErrorCode::NoSuchBucket,
                            "The specified bucket does not exist",
                            bucket,
                        ));
                    }
                };

                let mut objects_table = txn.open_table(OBJECTS).map_err(db_err)?;

                // Determine the version ID to use for the key
                let version_id =
                    meta.version_id.clone().unwrap_or_else(|| Self::CURRENT_VERSION.to_string());

                // If bucket has versioning, mark old versions as not latest
                if bucket_info.versioning_status.is_some() {
                    let prefix = Self::object_key_prefix(&bucket, &meta.key);
                    let end = format!("{}\0{}\x01", bucket, meta.key);

                    // Collect keys to update (can't modify while iterating)
                    let mut keys_to_update = Vec::new();
                    {
                        let range =
                            objects_table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;
                        for entry in range {
                            let (key, value) = entry.map_err(db_err)?;
                            let mut stored: StoredObjectMetadata =
                                bincode::deserialize(value.value()).map_err(db_err)?;
                            if stored.is_latest {
                                stored.is_latest = false;
                                keys_to_update.push((key.value().to_string(), stored));
                            }
                        }
                    }

                    // Update old versions
                    for (key, stored) in keys_to_update {
                        let serialized = bincode::serialize(&stored).map_err(db_err)?;
                        objects_table
                            .insert(key.as_str(), serialized.as_slice())
                            .map_err(db_err)?;
                    }
                } else {
                    // Non-versioned bucket: delete old entry if exists
                    let old_key =
                        Self::object_version_key(&bucket, &meta.key, Self::CURRENT_VERSION);
                    // Ignore the return value (old entry), but check for errors
                    objects_table.remove(old_key.as_str()).map_err(db_err)?;
                }

                // Insert new version with is_latest = true
                let key = Self::object_version_key(&bucket, &meta.key, &version_id);
                let mut stored = StoredObjectMetadata::from_object_metadata(&meta);
                stored.is_latest = true;
                let serialized = bincode::serialize(&stored).map_err(db_err)?;

                objects_table.insert(key.as_str(), serialized.as_slice()).map_err(db_err)?;
            }

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(())
        })
        .await
        .map_err(db_err)?
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        let bucket = bucket.to_string();
        let key_str = key.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;
            let table = txn.open_table(OBJECTS).map_err(db_err)?;

            // First try the new key format with version ID
            // Search for the latest version (is_latest = true)
            let prefix = Self::object_key_prefix(&bucket, &key_str);
            let end = format!("{}\0{}\x01", bucket, key_str);

            let range = table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;

            for entry in range {
                let (_, value) = entry.map_err(db_err)?;
                let stored: StoredObjectMetadata =
                    bincode::deserialize(value.value()).map_err(db_err)?;

                if stored.is_latest {
                    let meta = stored.to_object_metadata();
                    // If the latest version is a delete marker, return NoSuchKey
                    if meta.is_delete_marker {
                        return Err(Error::s3_with_resource(
                            S3ErrorCode::NoSuchKey,
                            "The specified key does not exist",
                            key_str,
                        ));
                    }
                    return Ok(meta);
                }
            }

            // Fallback: try the legacy key format (bucket\0key) for backward compatibility
            let legacy_key = Self::legacy_object_key(&bucket, &key_str);
            match table.get(legacy_key.as_str()).map_err(db_err)? {
                Some(value) => {
                    let stored: StoredObjectMetadata =
                        bincode::deserialize(value.value()).map_err(db_err)?;
                    Ok(stored.to_object_metadata())
                }
                None => Err(Error::s3_with_resource(
                    S3ErrorCode::NoSuchKey,
                    "The specified key does not exist",
                    key_str,
                )),
            }
        })
        .await
        .map_err(db_err)?
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Uuid>> {
        let bucket = bucket.to_string();
        let key_str = key.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            let uuid = {
                let mut table = txn.open_table(OBJECTS).map_err(db_err)?;

                // Find the latest version and delete it
                let prefix = Self::object_key_prefix(&bucket, &key_str);
                let end = format!("{}\0{}\x01", bucket, key_str);

                // Collect keys and UUIDs to delete
                let mut uuid_to_delete = None;
                let mut keys_to_delete = Vec::new();

                {
                    let range = table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;
                    for entry in range {
                        let (key, value) = entry.map_err(db_err)?;
                        let stored: StoredObjectMetadata =
                            bincode::deserialize(value.value()).map_err(db_err)?;
                        keys_to_delete.push(key.value().to_string());
                        if stored.is_latest && !stored.is_delete_marker {
                            uuid_to_delete = Some(Uuid::from_bytes(stored.uuid));
                        }
                    }
                }

                // Delete all versions
                for key in keys_to_delete {
                    table.remove(key.as_str()).map_err(db_err)?;
                }

                // Also try to delete legacy key format for backward compatibility
                let legacy_key = Self::legacy_object_key(&bucket, &key_str);
                let legacy_uuid = {
                    table.get(legacy_key.as_str()).map_err(db_err)?.map(|value| {
                        bincode::deserialize::<StoredObjectMetadata>(value.value())
                            .ok()
                            .map(|stored| Uuid::from_bytes(stored.uuid))
                    })
                };
                if let Some(Some(uuid)) = legacy_uuid {
                    if uuid_to_delete.is_none() {
                        uuid_to_delete = Some(uuid);
                    }
                    table.remove(legacy_key.as_str()).map_err(db_err)?;
                } else if legacy_uuid.is_some() {
                    // Entry exists but couldn't deserialize, still remove it
                    table.remove(legacy_key.as_str()).map_err(db_err)?;
                }

                uuid_to_delete
            };

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(uuid)
        })
        .await
        .map_err(db_err)?
    }

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<(Vec<ObjectMetadata>, Option<String>)> {
        let bucket = bucket.to_string();
        let prefix = prefix.map(String::from);
        let continuation_token = continuation_token.map(String::from);
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;

            // Check bucket exists
            {
                let buckets_table = txn.open_table(BUCKETS).map_err(db_err)?;
                if buckets_table.get(bucket.as_str()).map_err(db_err)?.is_none() {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        bucket,
                    ));
                }
            }

            let table = txn.open_table(OBJECTS).map_err(db_err)?;

            // Build range bounds
            let start_key = match (&prefix, &continuation_token) {
                (_, Some(token)) => {
                    // Continue after the token (start right after all versions of that key)
                    format!("{}\0{}\x01", bucket, token)
                }
                (Some(p), None) => format!("{}\0{}", bucket, p),
                (None, None) => format!("{}\0", bucket),
            };

            let end_key = format!("{}\x01", bucket);

            let mut objects = Vec::new();
            let limit = max_keys as usize + 1; // +1 to detect truncation
            let mut last_key_seen: Option<String> = None;

            let range = table.range(start_key.as_str()..end_key.as_str()).map_err(db_err)?;

            for entry in range {
                let (composite_key, value) = entry.map_err(db_err)?;

                // Try to parse as new three-part key format
                let obj_key = if let Some((_, key, _)) =
                    Self::parse_object_version_key(composite_key.value())
                {
                    key.to_string()
                } else if let Some((_, key)) = Self::parse_legacy_object_key(composite_key.value())
                {
                    // Legacy two-part key
                    key.to_string()
                } else {
                    continue; // Skip invalid keys
                };

                // Skip duplicate keys (we only want the latest version of each key)
                if last_key_seen.as_ref() == Some(&obj_key) {
                    continue;
                }

                // Check prefix match
                if let Some(ref p) = prefix {
                    if !obj_key.starts_with(p) {
                        continue;
                    }
                }

                let stored: StoredObjectMetadata =
                    bincode::deserialize(value.value()).map_err(db_err)?;

                // Only include latest versions that are not delete markers
                if !stored.is_latest || stored.is_delete_marker {
                    continue;
                }

                last_key_seen = Some(obj_key);

                if objects.len() >= limit {
                    break;
                }

                objects.push(stored.to_object_metadata());
            }

            // Check truncation
            let (objects, next_token) = if objects.len() > max_keys as usize {
                objects.pop();
                // Token is the last key in the returned list
                let token = objects.last().map(|o| o.key.clone());
                (objects, token)
            } else {
                (objects, None)
            };

            Ok((objects, next_token))
        })
        .await
        .map_err(db_err)?
    }

    // === Multipart Upload Operations ===

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        content_type: Option<&str>,
        user_metadata: HashMap<String, String>,
    ) -> Result<MultipartUpload> {
        let bucket = bucket.to_string();
        let key = key.to_string();
        let upload_id = upload_id.to_string();
        let content_type = content_type.map(String::from);
        let now = Utc::now();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            {
                // Check bucket exists
                let buckets_table = txn.open_table(BUCKETS).map_err(db_err)?;
                if buckets_table.get(bucket.as_str()).map_err(db_err)?.is_none() {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        bucket,
                    ));
                }

                // Create multipart upload record
                let mut uploads_table = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;

                let upload = MultipartUpload {
                    upload_id: upload_id.clone(),
                    bucket: bucket.clone(),
                    key: key.clone(),
                    initiated: now,
                    content_type: content_type.clone(),
                    user_metadata: user_metadata.clone(),
                };
                let stored = StoredMultipartUpload::from_multipart_upload(&upload);
                let serialized = bincode::serialize(&stored).map_err(db_err)?;

                uploads_table.insert(upload_id.as_str(), serialized.as_slice()).map_err(db_err)?;
            }

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(MultipartUpload {
                upload_id,
                bucket,
                key,
                initiated: now,
                content_type,
                user_metadata,
            })
        })
        .await
        .map_err(db_err)?
    }

    async fn get_multipart_upload(&self, upload_id: &str) -> Result<MultipartUpload> {
        let upload_id = upload_id.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;
            let table = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;

            match table.get(upload_id.as_str()).map_err(db_err)? {
                Some(value) => {
                    let stored: StoredMultipartUpload =
                        bincode::deserialize(value.value()).map_err(db_err)?;
                    Ok(stored.to_multipart_upload())
                }
                None => Err(Error::s3_with_resource(
                    S3ErrorCode::NoSuchUpload,
                    "The specified upload does not exist",
                    upload_id,
                )),
            }
        })
        .await
        .map_err(db_err)?
    }

    async fn delete_multipart_upload(&self, upload_id: &str) -> Result<()> {
        let upload_id = upload_id.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            {
                let mut uploads_table = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;
                uploads_table.remove(upload_id.as_str()).map_err(db_err)?;
            }

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(())
        })
        .await
        .map_err(db_err)?
    }

    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>> {
        let bucket = bucket.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;
            let table = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;

            let mut uploads = Vec::new();
            for entry in table.iter().map_err(db_err)? {
                let (_, value) = entry.map_err(db_err)?;
                let stored: StoredMultipartUpload =
                    bincode::deserialize(value.value()).map_err(db_err)?;
                let upload = stored.to_multipart_upload();
                if upload.bucket == bucket {
                    uploads.push(upload);
                }
            }

            // Sort by initiated time
            uploads.sort_by(|a, b| a.initiated.cmp(&b.initiated));

            Ok(uploads)
        })
        .await
        .map_err(db_err)?
    }

    async fn put_part(
        &self,
        upload_id: &str,
        part_number: u32,
        uuid: Uuid,
        size: u64,
        etag: &str,
    ) -> Result<Part> {
        let upload_id = upload_id.to_string();
        let etag = etag.to_string();
        let now = Utc::now();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            {
                // Check upload exists
                let uploads_table = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;
                if uploads_table.get(upload_id.as_str()).map_err(db_err)?.is_none() {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchUpload,
                        "The specified upload does not exist",
                        upload_id,
                    ));
                }

                // Store part
                let mut parts_table = txn.open_table(PARTS).map_err(db_err)?;
                let part_key = Self::part_key(&upload_id, part_number);

                let part = Part { part_number, etag: ETag::new(&etag), size, last_modified: now };
                let stored = StoredPart::from_part(&part, uuid);
                let serialized = bincode::serialize(&stored).map_err(db_err)?;

                parts_table.insert(part_key.as_str(), serialized.as_slice()).map_err(db_err)?;
            }

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(Part { part_number, etag: ETag::new(&etag), size, last_modified: now })
        })
        .await
        .map_err(db_err)?
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<Part>> {
        let parts_with_uuids = self.list_parts_with_uuids(upload_id).await?;
        Ok(parts_with_uuids.into_iter().map(|(p, _)| p).collect())
    }

    async fn list_parts_with_uuids(&self, upload_id: &str) -> Result<Vec<(Part, Uuid)>> {
        let upload_id = upload_id.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;

            // Check upload exists
            {
                let uploads_table = txn.open_table(MULTIPART_UPLOADS).map_err(db_err)?;
                if uploads_table.get(upload_id.as_str()).map_err(db_err)?.is_none() {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchUpload,
                        "The specified upload does not exist",
                        upload_id.clone(),
                    ));
                }
            }

            let table = txn.open_table(PARTS).map_err(db_err)?;

            // Parts are keyed as "upload_id\0part_number"
            let start_key = format!("{}\0", upload_id);
            let end_key = format!("{}\x01", upload_id);

            let mut parts = Vec::new();
            let range = table.range(start_key.as_str()..end_key.as_str()).map_err(db_err)?;

            for entry in range {
                let (_, value) = entry.map_err(db_err)?;
                let stored: StoredPart = bincode::deserialize(value.value()).map_err(db_err)?;
                parts.push((stored.to_part(), stored.uuid()));
            }

            // Sort by part number
            parts.sort_by_key(|(p, _)| p.part_number);

            Ok(parts)
        })
        .await
        .map_err(db_err)?
    }

    async fn delete_parts(&self, upload_id: &str) -> Result<Vec<Uuid>> {
        let upload_id = upload_id.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            let uuids = {
                let mut table = txn.open_table(PARTS).map_err(db_err)?;

                // Find all parts for this upload
                let start_key = format!("{}\0", upload_id);
                let end_key = format!("{}\x01", upload_id);

                let mut uuids = Vec::new();
                let mut keys_to_delete = Vec::new();

                {
                    let range =
                        table.range(start_key.as_str()..end_key.as_str()).map_err(db_err)?;
                    for entry in range {
                        let (key, value) = entry.map_err(db_err)?;
                        let stored: StoredPart =
                            bincode::deserialize(value.value()).map_err(db_err)?;
                        uuids.push(stored.uuid());
                        keys_to_delete.push(key.value().to_string());
                    }
                }

                // Delete parts
                for key in keys_to_delete {
                    table.remove(key.as_str()).map_err(db_err)?;
                }

                uuids
            };

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(uuids)
        })
        .await
        .map_err(db_err)?
    }

    // === Versioning Operations ===

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata> {
        let bucket = bucket.to_string();
        let key_str = key.to_string();
        let version_id = version_id.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;
            let table = txn.open_table(OBJECTS).map_err(db_err)?;

            // Look up the specific version
            let composite_key = Self::object_version_key(&bucket, &key_str, &version_id);

            match table.get(composite_key.as_str()).map_err(db_err)? {
                Some(value) => {
                    let stored: StoredObjectMetadata =
                        bincode::deserialize(value.value()).map_err(db_err)?;
                    Ok(stored.to_object_metadata())
                }
                None => Err(Error::s3_with_resource(
                    S3ErrorCode::NoSuchVersion,
                    "The specified version does not exist",
                    format!("{} (version: {})", key_str, version_id),
                )),
            }
        })
        .await
        .map_err(db_err)?
    }

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<Uuid>> {
        let bucket = bucket.to_string();
        let key_str = key.to_string();
        let version_id = version_id.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            let uuid = {
                let mut table = txn.open_table(OBJECTS).map_err(db_err)?;

                // Get the specific version
                let composite_key = Self::object_version_key(&bucket, &key_str, &version_id);

                let (uuid, was_latest) = match table.get(composite_key.as_str()).map_err(db_err)? {
                    Some(value) => {
                        let stored: StoredObjectMetadata =
                            bincode::deserialize(value.value()).map_err(db_err)?;
                        let uuid = if stored.is_delete_marker {
                            None
                        } else {
                            Some(Uuid::from_bytes(stored.uuid))
                        };
                        (uuid, stored.is_latest)
                    }
                    None => {
                        return Err(Error::s3_with_resource(
                            S3ErrorCode::NoSuchVersion,
                            "The specified version does not exist",
                            format!("{} (version: {})", key_str, version_id),
                        ));
                    }
                };

                // Remove this version
                table.remove(composite_key.as_str()).map_err(db_err)?;

                // If this was the latest version, find the next newest and mark it as latest
                if was_latest {
                    let prefix = Self::object_key_prefix(&bucket, &key_str);
                    let end = format!("{}\0{}\x01", bucket, key_str);

                    // Find the newest remaining version (last in sort order)
                    let mut newest_key: Option<String> = None;
                    {
                        let range = table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;
                        for entry in range {
                            let (key, _) = entry.map_err(db_err)?;
                            newest_key = Some(key.value().to_string());
                        }
                    }

                    // Mark the newest as latest
                    if let Some(newest_key) = newest_key {
                        // Extract data first, then mutate
                        let stored_data =
                            table.get(newest_key.as_str()).map_err(db_err)?.map(|value| {
                                bincode::deserialize::<StoredObjectMetadata>(value.value()).ok()
                            });

                        if let Some(Some(mut stored)) = stored_data {
                            stored.is_latest = true;
                            let serialized = bincode::serialize(&stored).map_err(db_err)?;
                            table
                                .insert(newest_key.as_str(), serialized.as_slice())
                                .map_err(db_err)?;
                        }
                    }
                }

                uuid
            };

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(uuid)
        })
        .await
        .map_err(db_err)?
    }

    async fn create_delete_marker(&self, bucket: &str, key: &str) -> Result<String> {
        let bucket = bucket.to_string();
        let key_str = key.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;

            let version_id = {
                // Check bucket exists
                let buckets_table = txn.open_table(BUCKETS).map_err(db_err)?;

                if buckets_table.get(bucket.as_str()).map_err(db_err)?.is_none() {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        bucket,
                    ));
                }

                let mut objects_table = txn.open_table(OBJECTS).map_err(db_err)?;

                // Generate new version ID for the delete marker
                let version_id = Uuid::new_v4().to_string();

                // Mark old versions as not latest
                let prefix = Self::object_key_prefix(&bucket, &key_str);
                let end = format!("{}\0{}\x01", bucket, key_str);

                let mut keys_to_update = Vec::new();
                {
                    let range =
                        objects_table.range(prefix.as_str()..end.as_str()).map_err(db_err)?;
                    for entry in range {
                        let (key, value) = entry.map_err(db_err)?;
                        let mut stored: StoredObjectMetadata =
                            bincode::deserialize(value.value()).map_err(db_err)?;
                        if stored.is_latest {
                            stored.is_latest = false;
                            keys_to_update.push((key.value().to_string(), stored));
                        }
                    }
                }

                for (key, stored) in keys_to_update {
                    let serialized = bincode::serialize(&stored).map_err(db_err)?;
                    objects_table.insert(key.as_str(), serialized.as_slice()).map_err(db_err)?;
                }

                // Create the delete marker
                let marker = ObjectMetadata::new_delete_marker(&key_str, &version_id);
                let composite_key = Self::object_version_key(&bucket, &key_str, &version_id);
                let stored = StoredObjectMetadata::from_object_metadata(&marker);
                let serialized = bincode::serialize(&stored).map_err(db_err)?;
                objects_table
                    .insert(composite_key.as_str(), serialized.as_slice())
                    .map_err(db_err)?;

                version_id
            };

            txn.set_durability(durability).map_err(db_err)?;
            txn.commit().map_err(db_err)?;

            Ok(version_id)
        })
        .await
        .map_err(db_err)?
    }

    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        delimiter: Option<&str>,
        key_marker: Option<&str>,
        version_id_marker: Option<&str>,
        max_keys: u32,
    ) -> Result<super::ListVersionsResult> {
        let bucket = bucket.to_string();
        let prefix = prefix.map(String::from);
        let delimiter = delimiter.map(String::from);
        let key_marker = key_marker.map(String::from);
        let version_id_marker = version_id_marker.map(String::from);
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(db_err)?;

            // Check bucket exists
            {
                let buckets_table = txn.open_table(BUCKETS).map_err(db_err)?;
                if buckets_table.get(bucket.as_str()).map_err(db_err)?.is_none() {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        bucket,
                    ));
                }
            }

            let table = txn.open_table(OBJECTS).map_err(db_err)?;

            // Build range bounds
            let start_key = match (&key_marker, &version_id_marker) {
                (Some(km), Some(vm)) => {
                    // Start after this specific version
                    format!("{}\0{}\0{}\0", bucket, km, vm)
                }
                (Some(km), None) => format!("{}\0{}\0", bucket, km),
                _ => match &prefix {
                    Some(p) => format!("{}\0{}", bucket, p),
                    None => format!("{}\0", bucket),
                },
            };

            let end_key = format!("{}\x01", bucket);

            let mut versions = Vec::new();
            let mut common_prefixes = std::collections::HashSet::new();
            let limit = max_keys as usize + 1;

            let range = table.range(start_key.as_str()..end_key.as_str()).map_err(db_err)?;

            for entry in range {
                let (composite_key, value) = entry.map_err(db_err)?;

                // Parse the key to get object key
                let obj_key = if let Some((_, key, _)) =
                    Self::parse_object_version_key(composite_key.value())
                {
                    key.to_string()
                } else if let Some((_, key)) = Self::parse_legacy_object_key(composite_key.value())
                {
                    key.to_string()
                } else {
                    continue;
                };

                // Skip entries at or before the key_marker
                if let Some(ref km) = key_marker {
                    if obj_key <= *km {
                        continue;
                    }
                }

                // Check prefix match
                if let Some(ref p) = prefix {
                    if !obj_key.starts_with(p) {
                        continue;
                    }
                }

                // Handle delimiter
                if let Some(ref d) = delimiter {
                    let prefix_str = prefix.as_deref().unwrap_or("");
                    if let Some(suffix) = obj_key.strip_prefix(prefix_str) {
                        if let Some(pos) = suffix.find(d.as_str()) {
                            let common_prefix =
                                format!("{}{}", prefix_str, &suffix[..pos + d.len()]);
                            common_prefixes.insert(common_prefix);
                            continue; // Skip adding individual versions
                        }
                    }
                }

                if versions.len() >= limit {
                    break;
                }

                let stored: StoredObjectMetadata =
                    bincode::deserialize(value.value()).map_err(db_err)?;
                let meta = stored.to_object_metadata();

                versions.push(super::VersionEntry {
                    is_delete_marker: meta.is_delete_marker,
                    is_latest: meta.is_latest,
                    metadata: meta,
                });
            }

            // Check truncation
            let is_truncated = versions.len() > max_keys as usize;
            if is_truncated {
                versions.pop();
            }

            let (next_key_marker, next_version_id_marker) = if is_truncated {
                versions
                    .last()
                    .map(|v| (Some(v.metadata.key.clone()), v.metadata.version_id.clone()))
                    .unwrap_or((None, None))
            } else {
                (None, None)
            };

            Ok(super::ListVersionsResult {
                versions,
                common_prefixes: common_prefixes.into_iter().collect(),
                is_truncated,
                next_key_marker,
                next_version_id_marker,
            })
        })
        .await
        .map_err(db_err)?
    }

    fn uuid_exists_sync(&self, bucket: &str, uuid: Uuid) -> bool {
        // This is a blocking operation used during recovery
        let read_txn = match self.db.begin_read() {
            Ok(txn) => txn,
            Err(_) => return false,
        };

        let table = match read_txn.open_table(OBJECTS) {
            Ok(t) => t,
            Err(_) => return false,
        };

        // Scan the bucket's objects to find if any has this UUID
        // Using composite key: "bucket\0key" - we need to scan all keys in the bucket
        let prefix = format!("{bucket}\0");
        let range = table.range::<&str>(prefix.as_str()..);
        let iter = match range {
            Ok(i) => i,
            Err(_) => return false,
        };

        for result in iter {
            match result {
                Ok((key, value)) => {
                    // Check if still in the same bucket
                    let key_str = key.value();
                    if !key_str.starts_with(&prefix) {
                        break;
                    }

                    // Deserialize and check UUID
                    let stored: StoredObjectMetadata = match bincode::deserialize(value.value()) {
                        Ok(m) => m,
                        Err(_) => continue,
                    };

                    if stored.uuid == *uuid.as_bytes() {
                        return true;
                    }
                }
                Err(_) => break,
            }
        }

        false
    }

    // === Object Tagging Operations ===

    async fn get_object_tagging(&self, bucket: &str, key: &str) -> Result<TagSet> {
        // First verify the object exists
        let _ = self.get_object(bucket, key).await?;

        let db = Arc::clone(&self.db);
        let tag_key = format!("{bucket}\0{key}");

        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(db_err)?;
            let table = read_txn.open_table(OBJECT_TAGGING).map_err(db_err)?;

            match table.get(tag_key.as_str()).map_err(db_err)? {
                Some(value) => {
                    let stored: StoredTagSet = bincode::deserialize(value.value())
                        .map_err(|e| Error::Database(format!("Failed to deserialize tags: {e}")))?;
                    Ok(stored.to_tagset())
                }
                None => Ok(TagSet::new()),
            }
        })
        .await
        .map_err(db_err)?
    }

    async fn put_object_tagging(&self, bucket: &str, key: &str, tags: TagSet) -> Result<()> {
        // First verify the object exists
        let _ = self.get_object(bucket, key).await?;

        let db = Arc::clone(&self.db);
        let durability = self.durability;
        let tag_key = format!("{bucket}\0{key}");

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;
            txn.set_durability(durability).map_err(db_err)?;

            {
                let mut table = txn.open_table(OBJECT_TAGGING).map_err(db_err)?;
                let stored = StoredTagSet::from_tagset(&tags);
                let bytes = bincode::serialize(&stored)
                    .map_err(|e| Error::Database(format!("Failed to serialize tags: {e}")))?;
                table.insert(tag_key.as_str(), bytes.as_slice()).map_err(db_err)?;
            }

            txn.commit().map_err(db_err)?;
            Ok(())
        })
        .await
        .map_err(db_err)?
    }

    async fn delete_object_tagging(&self, bucket: &str, key: &str) -> Result<()> {
        // First verify the object exists
        let _ = self.get_object(bucket, key).await?;

        let db = Arc::clone(&self.db);
        let durability = self.durability;
        let tag_key = format!("{bucket}\0{key}");

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;
            txn.set_durability(durability).map_err(db_err)?;

            {
                let mut table = txn.open_table(OBJECT_TAGGING).map_err(db_err)?;
                // Remove if exists, ignore if not
                let _ = table.remove(tag_key.as_str()).map_err(db_err)?;
            }

            txn.commit().map_err(db_err)?;
            Ok(())
        })
        .await
        .map_err(db_err)?
    }

    async fn get_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<TagSet> {
        // First verify the object version exists
        let _ = self.get_object_version(bucket, key, version_id).await?;

        let db = Arc::clone(&self.db);
        let tag_key = format!("{bucket}\0{key}\0{version_id}");

        tokio::task::spawn_blocking(move || {
            let read_txn = db.begin_read().map_err(db_err)?;
            let table = read_txn.open_table(OBJECT_TAGGING).map_err(db_err)?;

            match table.get(tag_key.as_str()).map_err(db_err)? {
                Some(value) => {
                    let stored: StoredTagSet = bincode::deserialize(value.value())
                        .map_err(|e| Error::Database(format!("Failed to deserialize tags: {e}")))?;
                    Ok(stored.to_tagset())
                }
                None => Ok(TagSet::new()),
            }
        })
        .await
        .map_err(db_err)?
    }

    async fn put_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        tags: TagSet,
    ) -> Result<()> {
        // First verify the object version exists
        let _ = self.get_object_version(bucket, key, version_id).await?;

        let db = Arc::clone(&self.db);
        let durability = self.durability;
        let tag_key = format!("{bucket}\0{key}\0{version_id}");

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;
            txn.set_durability(durability).map_err(db_err)?;

            {
                let mut table = txn.open_table(OBJECT_TAGGING).map_err(db_err)?;
                let stored = StoredTagSet::from_tagset(&tags);
                let bytes = bincode::serialize(&stored)
                    .map_err(|e| Error::Database(format!("Failed to serialize tags: {e}")))?;
                table.insert(tag_key.as_str(), bytes.as_slice()).map_err(db_err)?;
            }

            txn.commit().map_err(db_err)?;
            Ok(())
        })
        .await
        .map_err(db_err)?
    }

    async fn delete_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()> {
        // First verify the object version exists
        let _ = self.get_object_version(bucket, key, version_id).await?;

        let db = Arc::clone(&self.db);
        let durability = self.durability;
        let tag_key = format!("{bucket}\0{key}\0{version_id}");

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(db_err)?;
            txn.set_durability(durability).map_err(db_err)?;

            {
                let mut table = txn.open_table(OBJECT_TAGGING).map_err(db_err)?;
                // Remove if exists, ignore if not
                let _ = table.remove(tag_key.as_str()).map_err(db_err)?;
            }

            txn.commit().map_err(db_err)?;
            Ok(())
        })
        .await
        .map_err(db_err)?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_create_and_list_buckets() {
        let store = RedbMetadataStore::open_in_memory().unwrap();

        store.create_bucket("bucket1").await.unwrap();
        store.create_bucket("bucket2").await.unwrap();

        let buckets = store.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 2);
        assert_eq!(buckets[0].name, "bucket1");
        assert_eq!(buckets[1].name, "bucket2");
    }

    #[tokio::test]
    async fn test_bucket_already_exists() {
        let store = RedbMetadataStore::open_in_memory().unwrap();

        store.create_bucket("bucket1").await.unwrap();
        let result = store.create_bucket("bucket1").await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_put_and_get_object() {
        let store = RedbMetadataStore::open_in_memory().unwrap();

        store.create_bucket("bucket1").await.unwrap();

        let uuid = Uuid::new_v4();
        let meta = ObjectMetadata::new("test/key.txt", uuid, 1024, ETag::new("\"abc123\""))
            .with_content_type("text/plain");

        store.put_object("bucket1", meta.clone()).await.unwrap();

        let retrieved = store.get_object("bucket1", "test/key.txt").await.unwrap();
        assert_eq!(retrieved.key, "test/key.txt");
        assert_eq!(retrieved.size, 1024);
        assert_eq!(retrieved.content_type, Some("text/plain".to_string()));
    }

    #[tokio::test]
    async fn test_delete_object() {
        let store = RedbMetadataStore::open_in_memory().unwrap();

        store.create_bucket("bucket1").await.unwrap();

        let uuid = Uuid::new_v4();
        let meta = ObjectMetadata::new("test/key.txt", uuid, 1024, ETag::new("\"abc123\""));

        store.put_object("bucket1", meta).await.unwrap();
        let deleted_uuid = store.delete_object("bucket1", "test/key.txt").await.unwrap();

        assert_eq!(deleted_uuid, Some(uuid));

        let result = store.get_object("bucket1", "test/key.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_list_objects_with_prefix() {
        let store = RedbMetadataStore::open_in_memory().unwrap();

        store.create_bucket("bucket1").await.unwrap();

        // Create objects with different prefixes
        for key in ["photos/a.jpg", "photos/b.jpg", "docs/readme.txt"] {
            let meta = ObjectMetadata::new(key, Uuid::new_v4(), 100, ETag::new("\"test\""));
            store.put_object("bucket1", meta).await.unwrap();
        }

        // List with prefix
        let (objects, _) = store.list_objects("bucket1", Some("photos/"), None, 100).await.unwrap();

        assert_eq!(objects.len(), 2);
        assert!(objects.iter().all(|o| o.key.starts_with("photos/")));
    }

    #[tokio::test]
    async fn test_list_objects_pagination() {
        let store = RedbMetadataStore::open_in_memory().unwrap();

        store.create_bucket("bucket1").await.unwrap();

        // Create 5 objects
        for i in 0..5 {
            let meta = ObjectMetadata::new(
                format!("key{i:02}"),
                Uuid::new_v4(),
                100,
                ETag::new("\"test\""),
            );
            store.put_object("bucket1", meta).await.unwrap();
        }

        // Get first page (max 2)
        let (page1, token1) = store.list_objects("bucket1", None, None, 2).await.unwrap();

        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());

        // Get second page
        let (page2, token2) =
            store.list_objects("bucket1", None, token1.as_deref(), 2).await.unwrap();

        assert_eq!(page2.len(), 2);
        assert!(token2.is_some());

        // Get third page (should have 1 item, no more token)
        let (page3, token3) =
            store.list_objects("bucket1", None, token2.as_deref(), 2).await.unwrap();

        assert_eq!(page3.len(), 1);
        assert!(token3.is_none());
    }

    #[tokio::test]
    async fn test_uuid_exists_sync() {
        let store = RedbMetadataStore::open_in_memory().unwrap();

        store.create_bucket("test-bucket").await.unwrap();

        let uuid = Uuid::new_v4();
        let meta = ObjectMetadata::new("test-key", uuid, 1024, ETag::new("\"abc123\""));

        // UUID should not exist before putting
        assert!(!store.uuid_exists_sync("test-bucket", uuid));

        // Put the object
        store.put_object("test-bucket", meta).await.unwrap();

        // UUID should exist now
        assert!(store.uuid_exists_sync("test-bucket", uuid));

        // Random UUID should not exist
        assert!(!store.uuid_exists_sync("test-bucket", Uuid::new_v4()));

        // UUID should not exist in non-existent bucket
        assert!(!store.uuid_exists_sync("nonexistent-bucket", uuid));
    }

    #[tokio::test]
    async fn test_object_tagging_put_get() {
        use rucket_core::types::{Tag, TagSet};

        let store = RedbMetadataStore::open_in_memory().unwrap();
        store.create_bucket("test-bucket").await.unwrap();

        let meta = ObjectMetadata::new("test-key", Uuid::new_v4(), 100, ETag::new("\"test\""));
        store.put_object("test-bucket", meta).await.unwrap();

        // Initially empty
        let tags = store.get_object_tagging("test-bucket", "test-key").await.unwrap();
        assert!(tags.is_empty());

        // Put tags
        let tag_set =
            TagSet::with_tags(vec![Tag::new("env", "test"), Tag::new("project", "rucket")]);
        store.put_object_tagging("test-bucket", "test-key", tag_set.clone()).await.unwrap();

        // Get tags
        let retrieved = store.get_object_tagging("test-bucket", "test-key").await.unwrap();
        assert_eq!(retrieved.len(), 2);
    }

    #[tokio::test]
    async fn test_object_tagging_delete() {
        use rucket_core::types::{Tag, TagSet};

        let store = RedbMetadataStore::open_in_memory().unwrap();
        store.create_bucket("test-bucket").await.unwrap();

        let meta = ObjectMetadata::new("test-key", Uuid::new_v4(), 100, ETag::new("\"test\""));
        store.put_object("test-bucket", meta).await.unwrap();

        // Put tags
        let tag_set = TagSet::with_tags(vec![Tag::new("env", "test")]);
        store.put_object_tagging("test-bucket", "test-key", tag_set).await.unwrap();

        // Delete tags
        store.delete_object_tagging("test-bucket", "test-key").await.unwrap();

        // Should be empty now
        let tags = store.get_object_tagging("test-bucket", "test-key").await.unwrap();
        assert!(tags.is_empty());
    }

    #[tokio::test]
    async fn test_object_tagging_versioned() {
        use rucket_core::types::{Tag, TagSet, VersioningStatus};

        let store = RedbMetadataStore::open_in_memory().unwrap();
        store.create_bucket("test-bucket").await.unwrap();
        store.set_bucket_versioning("test-bucket", VersioningStatus::Enabled).await.unwrap();

        // Create v1
        let meta_v1 = ObjectMetadata::new("test-key", Uuid::new_v4(), 100, ETag::new("\"v1\""))
            .with_version_id("v1");
        store.put_object("test-bucket", meta_v1).await.unwrap();

        // Create v2
        let meta_v2 = ObjectMetadata::new("test-key", Uuid::new_v4(), 200, ETag::new("\"v2\""))
            .with_version_id("v2");
        store.put_object("test-bucket", meta_v2).await.unwrap();

        // Tag v1
        let tags_v1 = TagSet::with_tags(vec![Tag::new("version", "1")]);
        store.put_object_tagging_version("test-bucket", "test-key", "v1", tags_v1).await.unwrap();

        // Tag v2
        let tags_v2 = TagSet::with_tags(vec![Tag::new("version", "2")]);
        store.put_object_tagging_version("test-bucket", "test-key", "v2", tags_v2).await.unwrap();

        // Get v1 tags
        let retrieved_v1 =
            store.get_object_tagging_version("test-bucket", "test-key", "v1").await.unwrap();
        assert_eq!(retrieved_v1.len(), 1);
        assert_eq!(retrieved_v1.tags[0].value, "1");

        // Get v2 tags
        let retrieved_v2 =
            store.get_object_tagging_version("test-bucket", "test-key", "v2").await.unwrap();
        assert_eq!(retrieved_v2.len(), 1);
        assert_eq!(retrieved_v2.tags[0].value, "2");
    }

    #[tokio::test]
    async fn test_object_tagging_nonexistent_object() {
        let store = RedbMetadataStore::open_in_memory().unwrap();
        store.create_bucket("test-bucket").await.unwrap();

        let result = store.get_object_tagging("test-bucket", "nonexistent").await;
        assert!(result.is_err());
    }
}
