//! Raft state machine for metadata operations.
//!
//! This module implements the `RaftStateMachine` trait, applying `MetadataCommand`s
//! to the underlying metadata backend.

use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine, Snapshot};
use openraft::{EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError, StoredMembership};
use rucket_storage::metadata::MetadataBackend;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::command::MetadataCommand;
use crate::response::MetadataResponse;
use crate::types::{RaftMembership, RaftNodeId, RaftSnapshotMeta, RaftTypeConfig};

/// Snapshot data serialization format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// Last applied log ID.
    pub last_applied: Option<LogId<RaftNodeId>>,
    /// Last membership configuration.
    pub last_membership: RaftMembership,
    // Note: For Phase 3.1, we store minimal snapshot data.
    // The actual metadata is stored in redb and is not included in snapshots.
    // This works because:
    // 1. Single metadata store is shared by all nodes
    // 2. Each node has its own redb instance that's synced via Raft
    // In a true distributed setup with separate data stores, we'd need to
    // serialize the entire metadata state.
}

/// Raft state machine for metadata operations.
///
/// Applies `MetadataCommand`s to the underlying `MetadataBackend`,
/// tracking the last applied log ID and membership configuration.
pub struct MetadataStateMachine {
    /// The underlying metadata backend.
    backend: Arc<dyn MetadataBackend>,

    /// Last applied log ID.
    last_applied: RwLock<Option<LogId<RaftNodeId>>>,

    /// Last applied membership configuration.
    last_membership: RwLock<RaftMembership>,
}

impl MetadataStateMachine {
    /// Creates a new metadata state machine.
    pub fn new(backend: Arc<dyn MetadataBackend>) -> Self {
        Self {
            backend,
            last_applied: RwLock::new(None),
            last_membership: RwLock::new(StoredMembership::default()),
        }
    }

    /// Applies a single metadata command to the backend.
    ///
    /// Returns the response for the command.
    async fn apply_command(&self, cmd: MetadataCommand) -> MetadataResponse {
        match cmd {
            MetadataCommand::CreateBucket { name, .. } => {
                match self.backend.create_bucket(&name).await {
                    Ok(info) => MetadataResponse::BucketCreated(info),
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteBucket { name } => match self.backend.delete_bucket(&name).await
            {
                Ok(()) => MetadataResponse::Ok,
                Err(e) => MetadataResponse::from_error(&e),
            },

            MetadataCommand::SetBucketVersioning { bucket, status } => {
                match self.backend.set_bucket_versioning(&bucket, status).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutBucketCors { bucket, config } => {
                match self.backend.put_bucket_cors(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteBucketCors { bucket } => {
                match self.backend.delete_bucket_cors(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutBucketPolicy { bucket, policy_json } => {
                match self.backend.put_bucket_policy(&bucket, &policy_json).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteBucketPolicy { bucket } => {
                match self.backend.delete_bucket_policy(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutBucketTagging { bucket, tags } => {
                match self.backend.put_bucket_tagging(&bucket, tags).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteBucketTagging { bucket } => {
                match self.backend.delete_bucket_tagging(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutBucketLockConfig { bucket, config } => {
                match self.backend.put_bucket_lock_config(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutLifecycleConfiguration { bucket, config } => {
                match self.backend.put_lifecycle_configuration(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteLifecycleConfiguration { bucket } => {
                match self.backend.delete_lifecycle_configuration(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutPublicAccessBlock { bucket, config } => {
                match self.backend.put_public_access_block(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeletePublicAccessBlock { bucket } => {
                match self.backend.delete_public_access_block(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutEncryptionConfiguration { bucket, config } => {
                match self.backend.put_encryption_configuration(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteEncryptionConfiguration { bucket } => {
                match self.backend.delete_encryption_configuration(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutReplicationConfiguration { bucket, config } => {
                match self.backend.put_replication_configuration(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteReplicationConfiguration { bucket } => {
                match self.backend.delete_replication_configuration(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutBucketWebsite { bucket, config } => {
                match self.backend.put_bucket_website(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteBucketWebsite { bucket } => {
                match self.backend.delete_bucket_website(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutBucketLogging { bucket, config } => {
                match self.backend.put_bucket_logging(&bucket, config).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteBucketLogging { bucket } => {
                match self.backend.delete_bucket_logging(&bucket).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutObjectMetadata { bucket, metadata, .. } => {
                match self.backend.put_object(&bucket, metadata).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteObject { bucket, key, .. } => {
                match self.backend.delete_object(&bucket, &key).await {
                    Ok(uuid) => MetadataResponse::ObjectDeleted { deleted_uuid: uuid },
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteObjectVersion { bucket, key, version_id, .. } => {
                match self.backend.delete_object_version(&bucket, &key, &version_id).await {
                    Ok(uuid) => MetadataResponse::ObjectDeleted { deleted_uuid: uuid },
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::CreateDeleteMarker { bucket, key, .. } => {
                match self.backend.create_delete_marker(&bucket, &key).await {
                    Ok(version_id) => MetadataResponse::DeleteMarkerCreated { version_id },
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutObjectTagging { bucket, key, version_id, tags } => {
                let result = if let Some(vid) = version_id {
                    self.backend.put_object_tagging_version(&bucket, &key, &vid, tags).await
                } else {
                    self.backend.put_object_tagging(&bucket, &key, tags).await
                };
                match result {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteObjectTagging { bucket, key, version_id } => {
                let result = if let Some(vid) = version_id {
                    self.backend.delete_object_tagging_version(&bucket, &key, &vid).await
                } else {
                    self.backend.delete_object_tagging(&bucket, &key).await
                };
                match result {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutObjectRetention { bucket, key, version_id, retention, .. } => {
                let result = if let Some(vid) = version_id {
                    self.backend.put_object_retention_version(&bucket, &key, &vid, retention).await
                } else {
                    self.backend.put_object_retention(&bucket, &key, retention).await
                };
                match result {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutObjectLegalHold { bucket, key, version_id, enabled } => {
                let result = if let Some(vid) = version_id {
                    self.backend.put_object_legal_hold_version(&bucket, &key, &vid, enabled).await
                } else {
                    self.backend.put_object_legal_hold(&bucket, &key, enabled).await
                };
                match result {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::CreateMultipartUpload {
                bucket,
                key,
                upload_id,
                content_type,
                user_metadata,
                cache_control,
                content_disposition,
                content_encoding,
                content_language,
                expires,
                storage_class,
            } => {
                match self
                    .backend
                    .create_multipart_upload(
                        &bucket,
                        &key,
                        &upload_id,
                        content_type.as_deref(),
                        user_metadata,
                        cache_control.as_deref(),
                        content_disposition.as_deref(),
                        content_encoding.as_deref(),
                        content_language.as_deref(),
                        expires.as_deref(),
                        storage_class,
                    )
                    .await
                {
                    Ok(upload) => {
                        MetadataResponse::MultipartCreated { upload_id: upload.upload_id }
                    }
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::AbortMultipartUpload { upload_id, .. } => {
                match self.backend.delete_multipart_upload(&upload_id).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::PutPart { upload_id, part_number, uuid, size, etag } => {
                match self.backend.put_part(&upload_id, part_number, uuid, size, &etag).await {
                    Ok(part) => MetadataResponse::PartUploaded {
                        part_number: part.part_number,
                        etag: part.etag.to_string(),
                    },
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::DeleteParts { upload_id } => {
                match self.backend.delete_parts(&upload_id).await {
                    Ok(_) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::CompleteMultipartUpload { bucket, upload_id, metadata, .. } => {
                // First delete the multipart upload record
                if let Err(e) = self.backend.delete_multipart_upload(&upload_id).await {
                    return MetadataResponse::from_error(&e);
                }
                // Then save the final object metadata
                match self.backend.put_object(&bucket, metadata).await {
                    Ok(()) => MetadataResponse::Ok,
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::UpdatePgOwnership { pg_id, primary_node, replica_nodes, epoch } => {
                match self
                    .backend
                    .update_pg_ownership(pg_id, primary_node, replica_nodes, epoch)
                    .await
                {
                    Ok(()) => MetadataResponse::PgOwnershipUpdated { count: 1, epoch },
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }

            MetadataCommand::UpdateAllPgOwnership { entries, epoch } => {
                // Convert from command PgOwnershipEntry to storage PgOwnershipEntry
                let storage_entries: Vec<rucket_storage::metadata::PgOwnershipEntry> = entries
                    .into_iter()
                    .map(|e| rucket_storage::metadata::PgOwnershipEntry {
                        pg_id: e.pg_id,
                        primary_node: e.primary_node,
                        replica_nodes: e.replica_nodes,
                    })
                    .collect();

                match self.backend.update_all_pg_ownership(storage_entries, epoch).await {
                    Ok(count) => MetadataResponse::PgOwnershipUpdated { count, epoch },
                    Err(e) => MetadataResponse::from_error(&e),
                }
            }
        }
    }
}

/// Snapshot builder for the metadata state machine.
pub struct MetadataSnapshotBuilder {
    /// Last applied log ID at time of snapshot.
    last_applied: Option<LogId<RaftNodeId>>,
    /// Last membership at time of snapshot.
    last_membership: RaftMembership,
}

impl RaftSnapshotBuilder<RaftTypeConfig> for MetadataSnapshotBuilder {
    async fn build_snapshot(
        &mut self,
    ) -> Result<Snapshot<RaftTypeConfig>, StorageError<RaftNodeId>> {
        let data = SnapshotData {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
        };

        let serialized = bincode::serialize(&data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                std::io::Error::other(e.to_string()),
            )
        })?;

        let snapshot_id =
            format!("{}-{}", self.last_applied.map_or(0, |l| l.index), uuid::Uuid::new_v4());

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(serialized)) })
    }
}

impl RaftStateMachine<RaftTypeConfig> for MetadataStateMachine {
    type SnapshotBuilder = MetadataSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (Option<LogId<RaftNodeId>>, StoredMembership<RaftNodeId, openraft::BasicNode>),
        StorageError<RaftNodeId>,
    > {
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();
        Ok((last_applied, last_membership))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<MetadataResponse>, StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = openraft::Entry<RaftTypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            // Update last_applied
            *self.last_applied.write().await = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => {
                    responses.push(MetadataResponse::Ok);
                }
                EntryPayload::Normal(cmd) => {
                    let response = self.apply_command(cmd).await;
                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    *self.last_membership.write().await =
                        StoredMembership::new(Some(entry.log_id), membership);
                    responses.push(MetadataResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        MetadataSnapshotBuilder {
            last_applied: *self.last_applied.read().await,
            last_membership: self.last_membership.read().await.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<RaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &RaftSnapshotMeta,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let data: SnapshotData = bincode::deserialize(snapshot.get_ref()).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Read,
                std::io::Error::other(e.to_string()),
            )
        })?;

        *self.last_applied.write().await = data.last_applied;
        *self.last_membership.write().await = meta.last_membership.clone();

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftTypeConfig>>, StorageError<RaftNodeId>> {
        let last_applied = *self.last_applied.read().await;
        let last_membership = self.last_membership.read().await.clone();

        // If we have no applied state, there's no snapshot
        if last_applied.is_none() {
            return Ok(None);
        }

        let data = SnapshotData { last_applied, last_membership: last_membership.clone() };

        let serialized = bincode::serialize(&data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Store,
                openraft::ErrorVerb::Write,
                std::io::Error::other(e.to_string()),
            )
        })?;

        let snapshot_id = format!("{}-current", last_applied.map_or(0, |l| l.index));

        let meta = SnapshotMeta { last_log_id: last_applied, last_membership, snapshot_id };

        Ok(Some(Snapshot { meta, snapshot: Box::new(Cursor::new(serialized)) }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests would require mocking the MetadataBackend trait
    // which is complex. For now, integration tests will cover this.

    #[test]
    fn test_snapshot_data_serialization() {
        let data = SnapshotData {
            last_applied: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 10)),
            last_membership: StoredMembership::default(),
        };

        let serialized = bincode::serialize(&data).unwrap();
        let deserialized: SnapshotData = bincode::deserialize(&serialized).unwrap();

        assert_eq!(data.last_applied, deserialized.last_applied);
    }
}
