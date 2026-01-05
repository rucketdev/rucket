// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Replication stream types for event-sourced cross-region replication.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::{GeoError, GeoResult};
use crate::region::RegionId;
use crate::types::{ObjectReplicationStatus, ReplicationStatusInfo};

/// Unique identifier for a replication entry.
pub type ReplicationEntryId = Uuid;

/// An entry in the replication stream.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationEntry {
    /// Unique ID for this entry.
    pub id: ReplicationEntryId,
    /// Type of operation.
    pub operation: ReplicationOperation,
    /// Source region.
    pub source_region: RegionId,
    /// HLC timestamp for causality ordering.
    pub hlc_timestamp: u64,
    /// Wall-clock time when event occurred.
    pub timestamp: DateTime<Utc>,
    /// Priority (lower = higher priority).
    pub priority: u32,
}

impl ReplicationEntry {
    /// Create a new replication entry.
    pub fn new(
        operation: ReplicationOperation,
        source_region: RegionId,
        hlc_timestamp: u64,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            operation,
            source_region,
            hlc_timestamp,
            timestamp: Utc::now(),
            priority: 100,
        }
    }

    /// Set the priority.
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }
}

/// Operations that can be replicated across regions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationOperation {
    /// Put/create an object.
    PutObject {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (if versioned).
        version_id: Option<String>,
        /// Object data.
        #[serde(with = "bytes_serde")]
        data: Bytes,
        /// Content type.
        content_type: Option<String>,
        /// ETag.
        etag: String,
        /// Object size in bytes.
        size: u64,
    },

    /// Delete an object.
    DeleteObject {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (if versioned).
        version_id: Option<String>,
        /// Whether this is a delete marker.
        is_delete_marker: bool,
    },

    /// Update object metadata.
    UpdateMetadata {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (if versioned).
        version_id: Option<String>,
        /// Updated metadata.
        metadata: std::collections::HashMap<String, String>,
    },

    /// Update object tagging.
    UpdateTagging {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (if versioned).
        version_id: Option<String>,
        /// Updated tags.
        tags: std::collections::HashMap<String, String>,
    },
}

impl ReplicationOperation {
    /// Get the bucket name.
    pub fn bucket(&self) -> &str {
        match self {
            Self::PutObject { bucket, .. } => bucket,
            Self::DeleteObject { bucket, .. } => bucket,
            Self::UpdateMetadata { bucket, .. } => bucket,
            Self::UpdateTagging { bucket, .. } => bucket,
        }
    }

    /// Get the object key.
    pub fn key(&self) -> &str {
        match self {
            Self::PutObject { key, .. } => key,
            Self::DeleteObject { key, .. } => key,
            Self::UpdateMetadata { key, .. } => key,
            Self::UpdateTagging { key, .. } => key,
        }
    }

    /// Get the version ID if present.
    pub fn version_id(&self) -> Option<&str> {
        match self {
            Self::PutObject { version_id, .. } => version_id.as_deref(),
            Self::DeleteObject { version_id, .. } => version_id.as_deref(),
            Self::UpdateMetadata { version_id, .. } => version_id.as_deref(),
            Self::UpdateTagging { version_id, .. } => version_id.as_deref(),
        }
    }

    /// Check if this is a delete operation.
    pub fn is_delete(&self) -> bool {
        matches!(self, Self::DeleteObject { .. })
    }
}

/// Serde helper for Bytes.
mod bytes_serde {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(bytes: &Bytes, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        bytes.as_ref().serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Bytes, D::Error>
    where
        D: Deserializer<'de>,
    {
        let vec: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Ok(Bytes::from(vec))
    }
}

/// Configuration for a replication stream.
#[derive(Debug, Clone)]
pub struct ReplicationStreamConfig {
    /// Queue size for pending entries.
    pub queue_size: usize,
    /// Batch size for sending entries.
    pub batch_size: usize,
    /// Timeout for sending entries.
    pub send_timeout: Duration,
    /// Maximum retry attempts.
    pub max_retries: u32,
    /// Retry backoff base in milliseconds.
    pub retry_backoff_ms: u64,
}

impl Default for ReplicationStreamConfig {
    fn default() -> Self {
        Self {
            queue_size: 10_000,
            batch_size: 100,
            send_timeout: Duration::from_secs(30),
            max_retries: 3,
            retry_backoff_ms: 1000,
        }
    }
}

/// Trait for receiving replicated entries.
#[async_trait]
pub trait ReplicationSink: Send + Sync {
    /// Receive a replication entry and apply it.
    async fn receive(&self, entry: ReplicationEntry) -> GeoResult<()>;

    /// Check if the sink is healthy.
    async fn health_check(&self) -> bool;

    /// Get the region ID for this sink.
    fn region_id(&self) -> &RegionId;
}

/// Trait for sourcing replication entries.
#[async_trait]
pub trait ReplicationSource: Send + Sync {
    /// Get the next entry to replicate.
    async fn next(&self) -> Option<ReplicationEntry>;

    /// Acknowledge that an entry was successfully replicated.
    async fn acknowledge(&self, entry_id: ReplicationEntryId) -> GeoResult<()>;

    /// Get entries that need to be retried.
    async fn pending_retries(&self) -> Vec<ReplicationEntry>;
}

/// A no-op replication sink for testing and single-region deployments.
pub struct NoOpReplicationSink {
    region_id: RegionId,
}

impl NoOpReplicationSink {
    /// Create a new no-op sink.
    pub fn new(region_id: RegionId) -> Self {
        Self { region_id }
    }
}

#[async_trait]
impl ReplicationSink for NoOpReplicationSink {
    async fn receive(&self, entry: ReplicationEntry) -> GeoResult<()> {
        debug!(
            entry_id = %entry.id,
            operation = ?entry.operation.bucket(),
            "NoOp sink received entry (discarding)"
        );
        Ok(())
    }

    async fn health_check(&self) -> bool {
        true
    }

    fn region_id(&self) -> &RegionId {
        &self.region_id
    }
}

/// Queued entry for replication.
struct QueuedEntry {
    entry: ReplicationEntry,
    #[allow(dead_code)]
    retries: u32,
    destinations: Vec<RegionId>,
}

/// Replication stream for sending entries to remote regions.
pub struct ReplicationStream {
    #[allow(dead_code)]
    config: ReplicationStreamConfig,
    sinks: DashMap<RegionId, Arc<dyn ReplicationSink>>,
    queue_tx: mpsc::Sender<QueuedEntry>,
    status_tracker: Arc<DashMap<ReplicationEntryId, ReplicationStatusInfo>>,
}

impl ReplicationStream {
    /// Create a new replication stream.
    pub fn new(config: ReplicationStreamConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.queue_size);
        let sinks = DashMap::new();
        let status_tracker = Arc::new(DashMap::new());

        let stream = Self {
            config: config.clone(),
            sinks: sinks.clone(),
            queue_tx: tx,
            status_tracker: status_tracker.clone(),
        };

        // Spawn background worker
        tokio::spawn(Self::worker(rx, sinks.clone(), config, status_tracker));

        stream
    }

    /// Add a replication sink for a region.
    pub fn add_sink(&self, sink: Arc<dyn ReplicationSink>) {
        let region_id = sink.region_id().clone();
        info!(region = %region_id, "Adding replication sink");
        self.sinks.insert(region_id, sink);
    }

    /// Remove a replication sink.
    pub fn remove_sink(&self, region_id: &RegionId) {
        info!(region = %region_id, "Removing replication sink");
        self.sinks.remove(region_id);
    }

    /// Submit an entry for replication to all sinks.
    pub async fn submit(&self, entry: ReplicationEntry) -> GeoResult<()> {
        let destinations: Vec<_> = self.sinks.iter().map(|r| r.key().clone()).collect();

        if destinations.is_empty() {
            debug!(entry_id = %entry.id, "No replication sinks configured");
            return Ok(());
        }

        // Track status
        let mut status = ReplicationStatusInfo::new(
            entry.operation.bucket(),
            entry.operation.key(),
            entry.source_region.clone(),
            entry.hlc_timestamp,
        );
        if let Some(version_id) = entry.operation.version_id() {
            status = status.with_version_id(version_id);
        }
        for dest in &destinations {
            status.add_destination(dest.clone());
        }
        self.status_tracker.insert(entry.id, status);

        let queued = QueuedEntry { entry, retries: 0, destinations };

        self.queue_tx.try_send(queued).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                GeoError::Internal("Replication queue full".to_string())
            }
            mpsc::error::TrySendError::Closed(_) => {
                GeoError::Internal("Replication stream closed".to_string())
            }
        })
    }

    /// Submit an entry for replication to specific regions.
    pub async fn submit_to(
        &self,
        entry: ReplicationEntry,
        destinations: Vec<RegionId>,
    ) -> GeoResult<()> {
        if destinations.is_empty() {
            return Ok(());
        }

        // Track status
        let mut status = ReplicationStatusInfo::new(
            entry.operation.bucket(),
            entry.operation.key(),
            entry.source_region.clone(),
            entry.hlc_timestamp,
        );
        if let Some(version_id) = entry.operation.version_id() {
            status = status.with_version_id(version_id);
        }
        for dest in &destinations {
            status.add_destination(dest.clone());
        }
        self.status_tracker.insert(entry.id, status);

        let queued = QueuedEntry { entry, retries: 0, destinations };

        self.queue_tx.try_send(queued).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => {
                GeoError::Internal("Replication queue full".to_string())
            }
            mpsc::error::TrySendError::Closed(_) => {
                GeoError::Internal("Replication stream closed".to_string())
            }
        })
    }

    /// Get the replication status for an entry.
    pub fn get_status(&self, entry_id: &ReplicationEntryId) -> Option<ReplicationStatusInfo> {
        self.status_tracker.get(entry_id).map(|r| r.clone())
    }

    /// Get the number of pending entries.
    pub fn pending_count(&self) -> usize {
        self.status_tracker.iter().filter(|r| r.value().status.is_pending()).count()
    }

    /// Background worker for processing the queue.
    async fn worker(
        mut rx: mpsc::Receiver<QueuedEntry>,
        sinks: DashMap<RegionId, Arc<dyn ReplicationSink>>,
        config: ReplicationStreamConfig,
        status_tracker: Arc<DashMap<ReplicationEntryId, ReplicationStatusInfo>>,
    ) {
        info!("Starting replication stream worker");

        let mut batch = Vec::with_capacity(config.batch_size);

        loop {
            batch.clear();

            // Wait for at least one entry
            match rx.recv().await {
                Some(entry) => batch.push(entry),
                None => {
                    info!("Replication stream closed");
                    break;
                }
            }

            // Collect more entries non-blocking
            while batch.len() < config.batch_size {
                match rx.try_recv() {
                    Ok(entry) => batch.push(entry),
                    Err(_) => break,
                }
            }

            // Process batch
            for queued in batch.drain(..) {
                for dest_id in &queued.destinations {
                    if let Some(sink) = sinks.get(dest_id) {
                        match tokio::time::timeout(
                            config.send_timeout,
                            sink.receive(queued.entry.clone()),
                        )
                        .await
                        {
                            Ok(Ok(())) => {
                                debug!(
                                    entry_id = %queued.entry.id,
                                    destination = %dest_id,
                                    "Successfully replicated entry"
                                );
                                if let Some(mut status) = status_tracker.get_mut(&queued.entry.id) {
                                    status.update_destination(
                                        dest_id,
                                        ObjectReplicationStatus::Complete,
                                    );
                                }
                            }
                            Ok(Err(e)) => {
                                warn!(
                                    entry_id = %queued.entry.id,
                                    destination = %dest_id,
                                    error = %e,
                                    "Failed to replicate entry"
                                );
                                if let Some(mut status) = status_tracker.get_mut(&queued.entry.id) {
                                    status.record_error(dest_id, e.to_string());
                                }
                            }
                            Err(_) => {
                                warn!(
                                    entry_id = %queued.entry.id,
                                    destination = %dest_id,
                                    "Replication timed out"
                                );
                                if let Some(mut status) = status_tracker.get_mut(&queued.entry.id) {
                                    status.record_error(dest_id, "Timeout");
                                }
                            }
                        }
                    } else {
                        error!(
                            destination = %dest_id,
                            "Sink not found for destination"
                        );
                    }
                }
            }
        }
    }

    /// Get sink count.
    pub fn sink_count(&self) -> usize {
        self.sinks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_entry_creation() {
        let op = ReplicationOperation::PutObject {
            bucket: "test-bucket".to_string(),
            key: "test-key".to_string(),
            version_id: None,
            data: Bytes::from("test data"),
            content_type: Some("text/plain".to_string()),
            etag: "abc123".to_string(),
            size: 9,
        };

        let entry = ReplicationEntry::new(op, RegionId::new("us-east-1"), 12345);

        assert_eq!(entry.hlc_timestamp, 12345);
        assert_eq!(entry.source_region.as_str(), "us-east-1");
        assert_eq!(entry.priority, 100);
    }

    #[test]
    fn test_replication_operation_accessors() {
        let op = ReplicationOperation::PutObject {
            bucket: "my-bucket".to_string(),
            key: "my-key".to_string(),
            version_id: Some("v1".to_string()),
            data: Bytes::new(),
            content_type: None,
            etag: "etag".to_string(),
            size: 0,
        };

        assert_eq!(op.bucket(), "my-bucket");
        assert_eq!(op.key(), "my-key");
        assert_eq!(op.version_id(), Some("v1"));
        assert!(!op.is_delete());

        let delete_op = ReplicationOperation::DeleteObject {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            version_id: None,
            is_delete_marker: true,
        };

        assert!(delete_op.is_delete());
    }

    #[test]
    fn test_stream_config_default() {
        let config = ReplicationStreamConfig::default();
        assert_eq!(config.queue_size, 10_000);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.max_retries, 3);
    }

    #[tokio::test]
    async fn test_noop_sink() {
        let sink = NoOpReplicationSink::new(RegionId::new("us-west-2"));

        assert_eq!(sink.region_id().as_str(), "us-west-2");
        assert!(sink.health_check().await);

        let entry = ReplicationEntry::new(
            ReplicationOperation::DeleteObject {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: None,
                is_delete_marker: false,
            },
            RegionId::new("us-east-1"),
            100,
        );

        let result = sink.receive(entry).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replication_stream_no_sinks() {
        let config = ReplicationStreamConfig::default();
        let stream = ReplicationStream::new(config);

        assert_eq!(stream.sink_count(), 0);

        let entry = ReplicationEntry::new(
            ReplicationOperation::PutObject {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: None,
                data: Bytes::new(),
                content_type: None,
                etag: "etag".to_string(),
                size: 0,
            },
            RegionId::new("us-east-1"),
            100,
        );

        // Should succeed with no sinks
        let result = stream.submit(entry).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_replication_stream_with_sink() {
        let config = ReplicationStreamConfig::default();
        let stream = ReplicationStream::new(config);

        let sink = Arc::new(NoOpReplicationSink::new(RegionId::new("us-west-2")));
        stream.add_sink(sink);

        assert_eq!(stream.sink_count(), 1);

        let entry = ReplicationEntry::new(
            ReplicationOperation::PutObject {
                bucket: "bucket".to_string(),
                key: "key".to_string(),
                version_id: None,
                data: Bytes::from("data"),
                content_type: None,
                etag: "etag".to_string(),
                size: 4,
            },
            RegionId::new("us-east-1"),
            100,
        );

        let entry_id = entry.id;
        stream.submit(entry).await.unwrap();

        // Give worker time to process
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Check status
        let status = stream.get_status(&entry_id);
        assert!(status.is_some());
    }

    #[tokio::test]
    async fn test_replication_stream_remove_sink() {
        let config = ReplicationStreamConfig::default();
        let stream = ReplicationStream::new(config);

        let sink = Arc::new(NoOpReplicationSink::new(RegionId::new("us-west-2")));
        stream.add_sink(sink);
        assert_eq!(stream.sink_count(), 1);

        stream.remove_sink(&RegionId::new("us-west-2"));
        assert_eq!(stream.sink_count(), 0);
    }
}
