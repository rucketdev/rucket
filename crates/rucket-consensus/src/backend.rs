//! Raft-backed metadata backend.
//!
//! This module provides `RaftMetadataBackend`, which implements
//! the `MetadataBackend` trait by routing write operations through
//! Raft consensus and read operations to the local store.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rucket_core::encryption::ServerSideEncryptionConfiguration;
use rucket_core::error::S3ErrorCode;
use rucket_core::lifecycle::LifecycleConfiguration;
use rucket_core::public_access_block::PublicAccessBlockConfiguration;
use rucket_core::types::{
    BucketInfo, CorsConfiguration, MultipartUpload, ObjectLockConfig, ObjectMetadata,
    ObjectRetention, Part, TagSet, VersioningStatus,
};
use rucket_core::{Error, Result};
use rucket_storage::metadata::{ListVersionsResult, MetadataBackend};
use tracing::{debug, instrument, warn};
use uuid::Uuid;

use crate::command::MetadataCommand;
use crate::response::MetadataResponse;
use crate::types::RucketRaft;

/// Converts a string error code to S3ErrorCode.
fn s3_code_from_string(code: &str) -> S3ErrorCode {
    match code {
        "AccessDenied" => S3ErrorCode::AccessDenied,
        "BucketAlreadyExists" => S3ErrorCode::BucketAlreadyExists,
        "BucketNotEmpty" => S3ErrorCode::BucketNotEmpty,
        "NoSuchBucket" => S3ErrorCode::NoSuchBucket,
        "NoSuchKey" => S3ErrorCode::NoSuchKey,
        "NoSuchUpload" => S3ErrorCode::NoSuchUpload,
        "NoSuchVersion" => S3ErrorCode::NoSuchVersion,
        "InvalidArgument" => S3ErrorCode::InvalidArgument,
        "InvalidRequest" => S3ErrorCode::InvalidRequest,
        "NotImplemented" => S3ErrorCode::NotImplemented,
        _ => S3ErrorCode::InternalError,
    }
}

/// Helper trait to convert MetadataResponse to Result.
trait ResponseExt {
    /// Converts the response to a unit result, returning Err on Error response.
    fn to_unit_result(self) -> Result<()>;
}

impl ResponseExt for MetadataResponse {
    fn to_unit_result(self) -> Result<()> {
        match self {
            MetadataResponse::Error { code, message } => {
                Err(Error::s3(s3_code_from_string(&code), message))
            }
            _ => Ok(()),
        }
    }
}

/// Metadata backend that routes operations through Raft consensus.
///
/// Write operations are proposed to Raft and applied only after
/// being committed to a quorum. Read operations go directly to
/// the local store (eventually consistent).
///
/// # Architecture
///
/// ```text
/// ┌─────────────────────────────────────────────────┐
/// │              RaftMetadataBackend                │
/// ├─────────────────┬───────────────────────────────┤
/// │  Write Ops      │  Read Ops                     │
/// │  (via Raft)     │  (direct to local store)      │
/// │                 │                               │
/// │  create_bucket  │  get_bucket                   │
/// │  delete_bucket  │  bucket_exists                │
/// │  put_object     │  list_buckets                 │
/// │  delete_object  │  get_object                   │
/// │  ...            │  list_objects                 │
/// └────────┬────────┴───────────────┬───────────────┘
///          │                        │
///          ▼                        ▼
/// ┌─────────────────┐    ┌─────────────────────────┐
/// │    Raft         │    │   Local Backend         │
/// │  (proposal)     │    │   (RedbMetadata)        │
/// └────────┬────────┘    └─────────────────────────┘
///          │
///          ▼
/// ┌─────────────────┐
/// │  State Machine  │
/// │  (applies to    │
/// │   local store)  │
/// └─────────────────┘
/// ```
pub struct RaftMetadataBackend<B: MetadataBackend> {
    /// The Raft instance for consensus.
    raft: Arc<RucketRaft>,
    /// The underlying local backend for read operations.
    local: Arc<B>,
}

impl<B: MetadataBackend> RaftMetadataBackend<B> {
    /// Creates a new Raft-backed metadata backend.
    #[must_use]
    pub fn new(raft: Arc<RucketRaft>, local: Arc<B>) -> Self {
        Self { raft, local }
    }

    /// Proposes a command to Raft and waits for it to be applied.
    ///
    /// Returns the response from the state machine after the command is committed.
    #[instrument(skip(self, command), fields(command_type = %command.command_type()))]
    async fn propose(&self, command: MetadataCommand) -> Result<MetadataResponse> {
        debug!("Proposing command to Raft");

        let result = self.raft.client_write(command).await;

        match result {
            Ok(client_write_response) => {
                let response = client_write_response.data;
                debug!("Command committed successfully");
                Ok(response)
            }
            Err(e) => {
                warn!(error = %e, "Failed to commit command to Raft");
                Err(Error::s3(S3ErrorCode::InternalError, format!("Raft consensus failed: {}", e)))
            }
        }
    }
}

#[async_trait]
impl<B: MetadataBackend> MetadataBackend for RaftMetadataBackend<B> {
    // ========================================================================
    // Write Operations (routed through Raft)
    // ========================================================================

    async fn create_bucket(&self, name: &str) -> Result<BucketInfo> {
        let command = MetadataCommand::CreateBucket { name: name.to_string(), lock_enabled: false };
        let response = self.propose(command).await?;
        response.to_unit_result()?;
        // After Raft commits, fetch the bucket info from local store
        self.local.get_bucket(name).await
    }

    async fn delete_bucket(&self, name: &str) -> Result<()> {
        let command = MetadataCommand::DeleteBucket { name: name.to_string() };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn set_bucket_versioning(&self, name: &str, status: VersioningStatus) -> Result<()> {
        let command = MetadataCommand::SetBucketVersioning { bucket: name.to_string(), status };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_bucket_cors(&self, name: &str, config: CorsConfiguration) -> Result<()> {
        let command = MetadataCommand::PutBucketCors { bucket: name.to_string(), config };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_bucket_cors(&self, name: &str) -> Result<()> {
        let command = MetadataCommand::DeleteBucketCors { bucket: name.to_string() };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_object(&self, bucket: &str, meta: ObjectMetadata) -> Result<()> {
        let command = MetadataCommand::PutObjectMetadata {
            bucket: bucket.to_string(),
            metadata: meta,
            hlc: 0, // TODO: Use HLC from cluster clock
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_object(&self, bucket: &str, key: &str) -> Result<Option<Uuid>> {
        let command = MetadataCommand::DeleteObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            hlc: 0, // TODO: Use HLC from cluster clock
        };
        let response = self.propose(command).await?;
        match response {
            MetadataResponse::DeletedUuid(uuid) => Ok(Some(uuid)),
            MetadataResponse::Ok => Ok(None),
            MetadataResponse::Error { code, message } => {
                Err(Error::s3(s3_code_from_string(&code), message))
            }
            _ => {
                Err(Error::s3(S3ErrorCode::InternalError, "Unexpected response from delete_object"))
            }
        }
    }

    async fn delete_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<Uuid>> {
        let command = MetadataCommand::DeleteObjectVersion {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: version_id.to_string(),
            hlc: 0, // TODO: Use HLC from cluster clock
        };
        let response = self.propose(command).await?;
        match response {
            MetadataResponse::DeletedUuid(uuid) => Ok(Some(uuid)),
            MetadataResponse::Ok => Ok(None),
            MetadataResponse::Error { code, message } => {
                Err(Error::s3(s3_code_from_string(&code), message))
            }
            _ => Err(Error::s3(
                S3ErrorCode::InternalError,
                "Unexpected response from delete_object_version",
            )),
        }
    }

    async fn create_delete_marker(&self, bucket: &str, key: &str) -> Result<String> {
        let command = MetadataCommand::CreateDeleteMarker {
            bucket: bucket.to_string(),
            key: key.to_string(),
            hlc: 0, // TODO: Use HLC from cluster clock
        };
        let response = self.propose(command).await?;
        match response {
            MetadataResponse::VersionId(version_id) => Ok(version_id),
            MetadataResponse::Error { code, message } => {
                Err(Error::s3(s3_code_from_string(&code), message))
            }
            _ => Err(Error::s3(
                S3ErrorCode::InternalError,
                "Unexpected response from create_delete_marker",
            )),
        }
    }

    async fn create_multipart_upload(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        content_type: Option<&str>,
        user_metadata: HashMap<String, String>,
        cache_control: Option<&str>,
        content_disposition: Option<&str>,
        content_encoding: Option<&str>,
        content_language: Option<&str>,
        expires: Option<&str>,
    ) -> Result<MultipartUpload> {
        let command = MetadataCommand::CreateMultipartUpload {
            bucket: bucket.to_string(),
            key: key.to_string(),
            upload_id: upload_id.to_string(),
            content_type: content_type.map(String::from),
            user_metadata,
            cache_control: cache_control.map(String::from),
            content_disposition: content_disposition.map(String::from),
            content_encoding: content_encoding.map(String::from),
            content_language: content_language.map(String::from),
            expires: expires.map(String::from),
        };
        let response = self.propose(command).await?;
        response.to_unit_result()?;
        self.local.get_multipart_upload(upload_id).await
    }

    async fn delete_multipart_upload(&self, upload_id: &str) -> Result<()> {
        let command = MetadataCommand::AbortMultipartUpload {
            bucket: String::new(), // Will be looked up by upload_id
            key: String::new(),
            upload_id: upload_id.to_string(),
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_part(
        &self,
        upload_id: &str,
        part_number: u32,
        uuid: Uuid,
        size: u64,
        etag: &str,
    ) -> Result<Part> {
        let command = MetadataCommand::PutPart {
            upload_id: upload_id.to_string(),
            part_number,
            uuid,
            size,
            etag: etag.to_string(),
        };
        let response = self.propose(command).await?;
        match response {
            MetadataResponse::Part(part) => Ok(part),
            MetadataResponse::Error { code, message } => {
                Err(Error::s3(s3_code_from_string(&code), message))
            }
            _ => Err(Error::s3(S3ErrorCode::InternalError, "Unexpected response from put_part")),
        }
    }

    async fn delete_parts(&self, upload_id: &str) -> Result<Vec<Uuid>> {
        let command = MetadataCommand::DeleteParts { upload_id: upload_id.to_string() };
        let response = self.propose(command).await?;
        match response {
            MetadataResponse::DeletedUuids(uuids) => Ok(uuids),
            MetadataResponse::Error { code, message } => {
                Err(Error::s3(s3_code_from_string(&code), message))
            }
            _ => {
                Err(Error::s3(S3ErrorCode::InternalError, "Unexpected response from delete_parts"))
            }
        }
    }

    async fn put_object_tagging(&self, bucket: &str, key: &str, tags: TagSet) -> Result<()> {
        let command = MetadataCommand::PutObjectTagging {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
            tags,
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_object_tagging(&self, bucket: &str, key: &str) -> Result<()> {
        let command = MetadataCommand::DeleteObjectTagging {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        tags: TagSet,
    ) -> Result<()> {
        let command = MetadataCommand::PutObjectTagging {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version_id.to_string()),
            tags,
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<()> {
        let command = MetadataCommand::DeleteObjectTagging {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version_id.to_string()),
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_bucket_tagging(&self, bucket: &str, tags: TagSet) -> Result<()> {
        let command = MetadataCommand::PutBucketTagging { bucket: bucket.to_string(), tags };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_bucket_tagging(&self, bucket: &str) -> Result<()> {
        let command = MetadataCommand::DeleteBucketTagging { bucket: bucket.to_string() };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_bucket_lock_config(&self, bucket: &str, config: ObjectLockConfig) -> Result<()> {
        let command = MetadataCommand::PutBucketLockConfig { bucket: bucket.to_string(), config };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_object_retention(
        &self,
        bucket: &str,
        key: &str,
        retention: ObjectRetention,
    ) -> Result<()> {
        let command = MetadataCommand::PutObjectRetention {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
            retention,
            bypass_governance: false,
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        retention: ObjectRetention,
    ) -> Result<()> {
        let command = MetadataCommand::PutObjectRetention {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version_id.to_string()),
            retention,
            bypass_governance: false,
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_object_legal_hold(&self, bucket: &str, key: &str, enabled: bool) -> Result<()> {
        let command = MetadataCommand::PutObjectLegalHold {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: None,
            enabled,
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
        enabled: bool,
    ) -> Result<()> {
        let command = MetadataCommand::PutObjectLegalHold {
            bucket: bucket.to_string(),
            key: key.to_string(),
            version_id: Some(version_id.to_string()),
            enabled,
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_bucket_policy(&self, bucket: &str, policy_json: &str) -> Result<()> {
        let command = MetadataCommand::PutBucketPolicy {
            bucket: bucket.to_string(),
            policy_json: policy_json.to_string(),
        };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_bucket_policy(&self, bucket: &str) -> Result<()> {
        let command = MetadataCommand::DeleteBucketPolicy { bucket: bucket.to_string() };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_public_access_block(
        &self,
        bucket: &str,
        config: PublicAccessBlockConfiguration,
    ) -> Result<()> {
        let command = MetadataCommand::PutPublicAccessBlock { bucket: bucket.to_string(), config };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_public_access_block(&self, bucket: &str) -> Result<()> {
        let command = MetadataCommand::DeletePublicAccessBlock { bucket: bucket.to_string() };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_lifecycle_configuration(
        &self,
        bucket: &str,
        config: LifecycleConfiguration,
    ) -> Result<()> {
        let command =
            MetadataCommand::PutLifecycleConfiguration { bucket: bucket.to_string(), config };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_lifecycle_configuration(&self, bucket: &str) -> Result<()> {
        let command = MetadataCommand::DeleteLifecycleConfiguration { bucket: bucket.to_string() };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn put_encryption_configuration(
        &self,
        bucket: &str,
        config: ServerSideEncryptionConfiguration,
    ) -> Result<()> {
        let command =
            MetadataCommand::PutEncryptionConfiguration { bucket: bucket.to_string(), config };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    async fn delete_encryption_configuration(&self, bucket: &str) -> Result<()> {
        let command = MetadataCommand::DeleteEncryptionConfiguration { bucket: bucket.to_string() };
        let response = self.propose(command).await?;
        response.to_unit_result()
    }

    // ========================================================================
    // Read Operations (direct to local store)
    // ========================================================================

    async fn bucket_exists(&self, name: &str) -> Result<bool> {
        self.local.bucket_exists(name).await
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>> {
        self.local.list_buckets().await
    }

    async fn get_bucket(&self, name: &str) -> Result<BucketInfo> {
        self.local.get_bucket(name).await
    }

    async fn get_bucket_cors(&self, name: &str) -> Result<Option<CorsConfiguration>> {
        self.local.get_bucket_cors(name).await
    }

    async fn get_object(&self, bucket: &str, key: &str) -> Result<ObjectMetadata> {
        self.local.get_object(bucket, key).await
    }

    async fn get_object_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<ObjectMetadata> {
        self.local.get_object_version(bucket, key, version_id).await
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
        self.local
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

    async fn list_objects(
        &self,
        bucket: &str,
        prefix: Option<&str>,
        continuation_token: Option<&str>,
        max_keys: u32,
    ) -> Result<(Vec<ObjectMetadata>, Option<String>)> {
        self.local.list_objects(bucket, prefix, continuation_token, max_keys).await
    }

    async fn get_multipart_upload(&self, upload_id: &str) -> Result<MultipartUpload> {
        self.local.get_multipart_upload(upload_id).await
    }

    async fn list_multipart_uploads(&self, bucket: &str) -> Result<Vec<MultipartUpload>> {
        self.local.list_multipart_uploads(bucket).await
    }

    async fn list_parts(&self, upload_id: &str) -> Result<Vec<Part>> {
        self.local.list_parts(upload_id).await
    }

    async fn list_parts_with_uuids(&self, upload_id: &str) -> Result<Vec<(Part, Uuid)>> {
        self.local.list_parts_with_uuids(upload_id).await
    }

    fn uuid_exists_sync(&self, bucket: &str, uuid: Uuid) -> bool {
        self.local.uuid_exists_sync(bucket, uuid)
    }

    async fn get_object_tagging(&self, bucket: &str, key: &str) -> Result<TagSet> {
        self.local.get_object_tagging(bucket, key).await
    }

    async fn get_object_tagging_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<TagSet> {
        self.local.get_object_tagging_version(bucket, key, version_id).await
    }

    async fn get_bucket_tagging(&self, bucket: &str) -> Result<TagSet> {
        self.local.get_bucket_tagging(bucket).await
    }

    async fn get_bucket_lock_config(&self, bucket: &str) -> Result<Option<ObjectLockConfig>> {
        self.local.get_bucket_lock_config(bucket).await
    }

    async fn get_object_retention(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectRetention>> {
        self.local.get_object_retention(bucket, key).await
    }

    async fn get_object_retention_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<Option<ObjectRetention>> {
        self.local.get_object_retention_version(bucket, key, version_id).await
    }

    async fn get_object_legal_hold(&self, bucket: &str, key: &str) -> Result<bool> {
        self.local.get_object_legal_hold(bucket, key).await
    }

    async fn get_object_legal_hold_version(
        &self,
        bucket: &str,
        key: &str,
        version_id: &str,
    ) -> Result<bool> {
        self.local.get_object_legal_hold_version(bucket, key, version_id).await
    }

    async fn get_bucket_policy(&self, bucket: &str) -> Result<Option<String>> {
        self.local.get_bucket_policy(bucket).await
    }

    async fn get_public_access_block(
        &self,
        bucket: &str,
    ) -> Result<Option<PublicAccessBlockConfiguration>> {
        self.local.get_public_access_block(bucket).await
    }

    async fn get_lifecycle_configuration(
        &self,
        bucket: &str,
    ) -> Result<Option<LifecycleConfiguration>> {
        self.local.get_lifecycle_configuration(bucket).await
    }

    async fn get_encryption_configuration(
        &self,
        bucket: &str,
    ) -> Result<Option<ServerSideEncryptionConfiguration>> {
        self.local.get_encryption_configuration(bucket).await
    }
}

#[cfg(test)]
mod tests {
    // Tests would require mocking Raft, which is complex.
    // Integration tests in the main crate will cover this.
}
