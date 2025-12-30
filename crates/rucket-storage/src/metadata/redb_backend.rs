// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! redb-based metadata storage backend.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use rucket_core::error::{Error, S3ErrorCode};
use rucket_core::types::{BucketInfo, ETag, MultipartUpload, ObjectMetadata, Part};
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

// === Stored Types (for bincode serialization) ===

#[derive(Serialize, Deserialize)]
struct StoredBucketInfo {
    name: String,
    created_at_millis: i64,
}

impl StoredBucketInfo {
    fn from_bucket_info(info: &BucketInfo) -> Self {
        Self {
            name: info.name.clone(),
            created_at_millis: info.created_at.timestamp_millis(),
        }
    }

    fn to_bucket_info(&self) -> BucketInfo {
        BucketInfo {
            name: self.name.clone(),
            created_at: Utc
                .timestamp_millis_opt(self.created_at_millis)
                .single()
                .unwrap_or_else(Utc::now),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct StoredObjectMetadata {
    key: String,
    uuid: [u8; 16],
    size: u64,
    etag: String,
    content_type: Option<String>,
    last_modified_millis: i64,
    user_metadata: Vec<(String, String)>,
}

impl StoredObjectMetadata {
    fn from_object_metadata(meta: &ObjectMetadata) -> Self {
        Self {
            key: meta.key.clone(),
            uuid: *meta.uuid.as_bytes(),
            size: meta.size,
            etag: meta.etag.as_str().to_string(),
            content_type: meta.content_type.clone(),
            last_modified_millis: meta.last_modified.timestamp_millis(),
            user_metadata: meta.user_metadata.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
        }
    }

    fn to_object_metadata(&self) -> ObjectMetadata {
        ObjectMetadata {
            key: self.key.clone(),
            uuid: Uuid::from_bytes(self.uuid),
            size: self.size,
            etag: ETag::new(&self.etag),
            content_type: self.content_type.clone(),
            last_modified: Utc
                .timestamp_millis_opt(self.last_modified_millis)
                .single()
                .unwrap_or_else(Utc::now),
            user_metadata: self.user_metadata.iter().cloned().collect(),
        }
    }
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

        let db = Database::create(path).map_err(|e| Error::Database(e.to_string()))?;

        let durability = Self::sync_to_durability(sync_strategy);

        Ok(Self {
            db: Arc::new(db),
            durability,
        })
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
            .map_err(|e| Error::Database(e.to_string()))?;

        Ok(Self {
            db: Arc::new(db),
            durability: Durability::None,
        })
    }

    /// Map SyncStrategy to redb Durability.
    fn sync_to_durability(strategy: SyncStrategy) -> Durability {
        match strategy {
            SyncStrategy::Always => Durability::Immediate,
            SyncStrategy::None => Durability::None,
            SyncStrategy::Periodic | SyncStrategy::Threshold => Durability::Eventual,
        }
    }

    /// Create a composite key for objects: "bucket\0key"
    fn object_key(bucket: &str, key: &str) -> String {
        format!("{}\0{}", bucket, key)
    }

    /// Parse a composite object key back to (bucket, key)
    fn parse_object_key(composite: &str) -> Option<(&str, &str)> {
        composite.split_once('\0')
    }
}

#[async_trait]
impl MetadataBackend for RedbMetadataStore {
    async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        let name = name.to_string();
        let now = Utc::now();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(|e| Error::Database(e.to_string()))?;

            {
                let mut table = txn.open_table(BUCKETS).map_err(|e| Error::Database(e.to_string()))?;

                // Check if bucket already exists
                if table
                    .get(name.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?
                    .is_some()
                {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::BucketAlreadyExists,
                        "The bucket already exists",
                        name,
                    ));
                }

                let info = BucketInfo {
                    name: name.clone(),
                    created_at: now,
                };
                let stored = StoredBucketInfo::from_bucket_info(&info);
                let serialized =
                    bincode::serialize(&stored).map_err(|e| Error::Database(e.to_string()))?;

                table
                    .insert(name.as_str(), serialized.as_slice())
                    .map_err(|e| Error::Database(e.to_string()))?;
            }

            txn.set_durability(durability);
            txn.commit().map_err(|e| Error::Database(e.to_string()))?;

            Ok(BucketInfo {
                name,
                created_at: now,
            })
        })
        .await
        .map_err(|e| Error::Database(e.to_string()))?
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(|e| Error::Database(e.to_string()))?;

            {
                let mut buckets_table =
                    txn.open_table(BUCKETS).map_err(|e| Error::Database(e.to_string()))?;

                // Check if bucket exists
                if buckets_table
                    .get(name.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?
                    .is_none()
                {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        name,
                    ));
                }

                // Check if bucket is empty
                let objects_table =
                    txn.open_table(OBJECTS).map_err(|e| Error::Database(e.to_string()))?;

                let prefix = format!("{}\0", name);
                let end = format!("{}\x01", name);

                let range = objects_table
                    .range(prefix.as_str()..end.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?;

                if range.count() > 0 {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::BucketNotEmpty,
                        "The bucket is not empty",
                        name,
                    ));
                }

                buckets_table
                    .remove(name.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?;
            }

            txn.set_durability(durability);
            txn.commit().map_err(|e| Error::Database(e.to_string()))?;

            Ok(())
        })
        .await
        .map_err(|e| Error::Database(e.to_string()))?
    }

    async fn bucket_exists(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Database(e.to_string()))?;
            let table = txn.open_table(BUCKETS).map_err(|e| Error::Database(e.to_string()))?;

            let exists = table
                .get(name.as_str())
                .map_err(|e| Error::Database(e.to_string()))?
                .is_some();

            Ok(exists)
        })
        .await
        .map_err(|e| Error::Database(e.to_string()))?
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Database(e.to_string()))?;
            let table = txn.open_table(BUCKETS).map_err(|e| Error::Database(e.to_string()))?;

            let mut buckets = Vec::new();
            for entry in table.iter().map_err(|e| Error::Database(e.to_string()))? {
                let (_, value) = entry.map_err(|e| Error::Database(e.to_string()))?;
                let stored: StoredBucketInfo =
                    bincode::deserialize(value.value()).map_err(|e| Error::Database(e.to_string()))?;
                buckets.push(stored.to_bucket_info());
            }

            // Sort by name
            buckets.sort_by(|a, b| a.name.cmp(&b.name));

            Ok(buckets)
        })
        .await
        .map_err(|e| Error::Database(e.to_string()))?
    }

    async fn put_object(&self, bucket: &str, meta: ObjectMetadata) -> Result<()> {
        let bucket = bucket.to_string();
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(|e| Error::Database(e.to_string()))?;

            {
                // Check bucket exists
                let buckets_table =
                    txn.open_table(BUCKETS).map_err(|e| Error::Database(e.to_string()))?;

                if buckets_table
                    .get(bucket.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?
                    .is_none()
                {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        bucket,
                    ));
                }

                // Insert/update object
                let mut objects_table =
                    txn.open_table(OBJECTS).map_err(|e| Error::Database(e.to_string()))?;

                let key = Self::object_key(&bucket, &meta.key);
                let stored = StoredObjectMetadata::from_object_metadata(&meta);
                let serialized =
                    bincode::serialize(&stored).map_err(|e| Error::Database(e.to_string()))?;

                objects_table
                    .insert(key.as_str(), serialized.as_slice())
                    .map_err(|e| Error::Database(e.to_string()))?;
            }

            txn.set_durability(durability);
            txn.commit().map_err(|e| Error::Database(e.to_string()))?;

            Ok(())
        })
        .await
        .map_err(|e| Error::Database(e.to_string()))?
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        let composite_key = Self::object_key(bucket, key);
        let key_for_error = key.to_string();
        let db = Arc::clone(&self.db);

        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Database(e.to_string()))?;
            let table = txn.open_table(OBJECTS).map_err(|e| Error::Database(e.to_string()))?;

            match table
                .get(composite_key.as_str())
                .map_err(|e| Error::Database(e.to_string()))?
            {
                Some(value) => {
                    let stored: StoredObjectMetadata = bincode::deserialize(value.value())
                        .map_err(|e| Error::Database(e.to_string()))?;
                    Ok(stored.to_object_metadata())
                }
                None => Err(Error::s3_with_resource(
                    S3ErrorCode::NoSuchKey,
                    "The specified key does not exist",
                    key_for_error,
                )),
            }
        })
        .await
        .map_err(|e| Error::Database(e.to_string()))?
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Uuid>> {
        let composite_key = Self::object_key(bucket, key);
        let db = Arc::clone(&self.db);
        let durability = self.durability;

        tokio::task::spawn_blocking(move || {
            let mut txn = db.begin_write().map_err(|e| Error::Database(e.to_string()))?;

            let uuid = {
                let mut table =
                    txn.open_table(OBJECTS).map_err(|e| Error::Database(e.to_string()))?;

                // Get the UUID before deleting
                let uuid = match table
                    .get(composite_key.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?
                {
                    Some(value) => {
                        let stored: StoredObjectMetadata = bincode::deserialize(value.value())
                            .map_err(|e| Error::Database(e.to_string()))?;
                        Some(Uuid::from_bytes(stored.uuid))
                    }
                    None => None,
                };

                table
                    .remove(composite_key.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?;

                uuid
            };

            txn.set_durability(durability);
            txn.commit().map_err(|e| Error::Database(e.to_string()))?;

            Ok(uuid)
        })
        .await
        .map_err(|e| Error::Database(e.to_string()))?
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
            let txn = db.begin_read().map_err(|e| Error::Database(e.to_string()))?;

            // Check bucket exists
            {
                let buckets_table =
                    txn.open_table(BUCKETS).map_err(|e| Error::Database(e.to_string()))?;
                if buckets_table
                    .get(bucket.as_str())
                    .map_err(|e| Error::Database(e.to_string()))?
                    .is_none()
                {
                    return Err(Error::s3_with_resource(
                        S3ErrorCode::NoSuchBucket,
                        "The specified bucket does not exist",
                        bucket,
                    ));
                }
            }

            let table = txn.open_table(OBJECTS).map_err(|e| Error::Database(e.to_string()))?;

            // Build range bounds
            let start_key = match (&prefix, &continuation_token) {
                (_, Some(token)) => {
                    // Continue after the token
                    format!("{}\0{}\0", bucket, token)
                }
                (Some(p), None) => format!("{}\0{}", bucket, p),
                (None, None) => format!("{}\0", bucket),
            };

            let end_key = format!("{}\x01", bucket);

            let mut objects = Vec::new();
            let limit = max_keys as usize + 1; // +1 to detect truncation

            let range = table
                .range(start_key.as_str()..end_key.as_str())
                .map_err(|e| Error::Database(e.to_string()))?;

            for entry in range {
                let (key, value) = entry.map_err(|e| Error::Database(e.to_string()))?;
                let (_, obj_key) = Self::parse_object_key(key.value()).ok_or_else(|| {
                    Error::Database("Invalid object key format".to_string())
                })?;

                // Check prefix match
                if let Some(ref p) = prefix {
                    if !obj_key.starts_with(p) {
                        continue;
                    }
                }

                // Skip if this is the continuation token itself
                if continuation_token
                    .as_ref()
                    .is_some_and(|t| t == obj_key)
                {
                    continue;
                }

                if objects.len() >= limit {
                    break;
                }

                let stored: StoredObjectMetadata = bincode::deserialize(value.value())
                    .map_err(|e| Error::Database(e.to_string()))?;
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
        .map_err(|e| Error::Database(e.to_string()))?
    }

    // === Multipart Upload Stubs ===

    async fn create_multipart_upload(
        &self,
        _bucket: &str,
        _key: &str,
        _upload_id: &str,
    ) -> Result<MultipartUpload> {
        Err(Error::s3(
            S3ErrorCode::NotImplemented,
            "Multipart uploads are not yet implemented",
        ))
    }

    async fn get_multipart_upload(&self, _upload_id: &str) -> Result<MultipartUpload> {
        Err(Error::s3(
            S3ErrorCode::NotImplemented,
            "Multipart uploads are not yet implemented",
        ))
    }

    async fn delete_multipart_upload(&self, _upload_id: &str) -> Result<()> {
        Err(Error::s3(
            S3ErrorCode::NotImplemented,
            "Multipart uploads are not yet implemented",
        ))
    }

    async fn list_multipart_uploads(&self, _bucket: &str) -> Result<Vec<MultipartUpload>> {
        Err(Error::s3(
            S3ErrorCode::NotImplemented,
            "Multipart uploads are not yet implemented",
        ))
    }

    async fn put_part(
        &self,
        _upload_id: &str,
        _part_number: u32,
        _uuid: Uuid,
        _size: u64,
        _etag: &str,
    ) -> Result<Part> {
        Err(Error::s3(
            S3ErrorCode::NotImplemented,
            "Multipart uploads are not yet implemented",
        ))
    }

    async fn list_parts(&self, _upload_id: &str) -> Result<Vec<Part>> {
        Err(Error::s3(
            S3ErrorCode::NotImplemented,
            "Multipart uploads are not yet implemented",
        ))
    }

    async fn delete_parts(&self, _upload_id: &str) -> Result<Vec<Uuid>> {
        Err(Error::s3(
            S3ErrorCode::NotImplemented,
            "Multipart uploads are not yet implemented",
        ))
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
        let deleted_uuid = store
            .delete_object("bucket1", "test/key.txt")
            .await
            .unwrap();

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
        let (objects, _) = store
            .list_objects("bucket1", Some("photos/"), None, 100)
            .await
            .unwrap();

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
        let (page1, token1) = store
            .list_objects("bucket1", None, None, 2)
            .await
            .unwrap();

        assert_eq!(page1.len(), 2);
        assert!(token1.is_some());

        // Get second page
        let (page2, token2) = store
            .list_objects("bucket1", None, token1.as_deref(), 2)
            .await
            .unwrap();

        assert_eq!(page2.len(), 2);
        assert!(token2.is_some());

        // Get third page (should have 1 item, no more token)
        let (page3, token3) = store
            .list_objects("bucket1", None, token2.as_deref(), 2)
            .await
            .unwrap();

        assert_eq!(page3.len(), 1);
        assert!(token3.is_none());
    }
}
