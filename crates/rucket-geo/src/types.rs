// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Replication status types for tracking cross-region replication.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::region::RegionId;

/// Replication status for an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "UPPERCASE")]
pub enum ObjectReplicationStatus {
    /// Replication is pending (queued but not started).
    #[default]
    Pending,
    /// Replication is in progress.
    InProgress,
    /// Replication completed successfully.
    Complete,
    /// Replication failed.
    Failed,
    /// Object is a replica (received from another region).
    Replica,
    /// Replication is disabled for this object.
    Disabled,
}

impl ObjectReplicationStatus {
    /// Returns true if replication is complete.
    pub fn is_complete(&self) -> bool {
        matches!(self, Self::Complete | Self::Replica)
    }

    /// Returns true if replication is still in progress.
    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending | Self::InProgress)
    }

    /// Returns true if replication failed.
    pub fn is_failed(&self) -> bool {
        matches!(self, Self::Failed)
    }
}

impl std::fmt::Display for ObjectReplicationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::InProgress => write!(f, "IN_PROGRESS"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed => write!(f, "FAILED"),
            Self::Replica => write!(f, "REPLICA"),
            Self::Disabled => write!(f, "DISABLED"),
        }
    }
}

/// Progress tracking for ongoing replication.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationProgress {
    /// Total bytes to replicate.
    pub total_bytes: u64,
    /// Bytes replicated so far.
    pub replicated_bytes: u64,
    /// When replication started.
    pub started_at: DateTime<Utc>,
    /// Last progress update time.
    pub updated_at: DateTime<Utc>,
    /// Estimated time remaining in seconds.
    pub eta_seconds: Option<u64>,
    /// Current transfer rate in bytes per second.
    pub transfer_rate_bps: Option<u64>,
}

impl ReplicationProgress {
    /// Create a new progress tracker.
    pub fn new(total_bytes: u64) -> Self {
        let now = Utc::now();
        Self {
            total_bytes,
            replicated_bytes: 0,
            started_at: now,
            updated_at: now,
            eta_seconds: None,
            transfer_rate_bps: None,
        }
    }

    /// Update progress with new bytes replicated.
    pub fn update(&mut self, bytes_replicated: u64) {
        let now = Utc::now();
        self.replicated_bytes = bytes_replicated;
        self.updated_at = now;

        // Calculate transfer rate
        let elapsed_secs = (now - self.started_at).num_seconds() as f64;
        if elapsed_secs > 0.0 {
            self.transfer_rate_bps = Some((bytes_replicated as f64 / elapsed_secs) as u64);

            // Calculate ETA
            let remaining = self.total_bytes.saturating_sub(bytes_replicated);
            if let Some(rate) = self.transfer_rate_bps {
                if rate > 0 {
                    self.eta_seconds = Some(remaining / rate);
                }
            }
        }
    }

    /// Get the completion percentage (0-100).
    pub fn percentage(&self) -> f64 {
        if self.total_bytes == 0 {
            return 100.0;
        }
        (self.replicated_bytes as f64 / self.total_bytes as f64) * 100.0
    }

    /// Check if replication is complete.
    pub fn is_complete(&self) -> bool {
        self.replicated_bytes >= self.total_bytes
    }
}

/// Detailed replication status information for an object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationStatusInfo {
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// Version ID (if versioned).
    pub version_id: Option<String>,
    /// Overall replication status.
    pub status: ObjectReplicationStatus,
    /// Source region ID.
    pub source_region: RegionId,
    /// Status for each destination region.
    pub destinations: HashMap<RegionId, DestinationStatus>,
    /// HLC timestamp from the source.
    pub hlc_timestamp: u64,
    /// When replication was initiated.
    pub initiated_at: DateTime<Utc>,
    /// When replication completed (if complete).
    pub completed_at: Option<DateTime<Utc>>,
    /// Retry count if failed.
    pub retry_count: u32,
    /// Last error message (if any).
    pub last_error: Option<String>,
}

impl ReplicationStatusInfo {
    /// Create a new replication status.
    pub fn new(
        bucket: impl Into<String>,
        key: impl Into<String>,
        source_region: RegionId,
        hlc_timestamp: u64,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            key: key.into(),
            version_id: None,
            status: ObjectReplicationStatus::Pending,
            source_region,
            destinations: HashMap::new(),
            hlc_timestamp,
            initiated_at: Utc::now(),
            completed_at: None,
            retry_count: 0,
            last_error: None,
        }
    }

    /// Set the version ID.
    pub fn with_version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    /// Add a destination region.
    pub fn add_destination(&mut self, region_id: RegionId) {
        self.destinations.insert(region_id, DestinationStatus::default());
    }

    /// Update status for a destination.
    pub fn update_destination(&mut self, region_id: &RegionId, status: ObjectReplicationStatus) {
        if let Some(dest) = self.destinations.get_mut(region_id) {
            dest.status = status;
            dest.updated_at = Utc::now();
            if status.is_complete() {
                dest.completed_at = Some(Utc::now());
            }
        }
        self.update_overall_status();
    }

    /// Record an error for a destination.
    pub fn record_error(&mut self, region_id: &RegionId, error: impl Into<String>) {
        if let Some(dest) = self.destinations.get_mut(region_id) {
            dest.status = ObjectReplicationStatus::Failed;
            dest.last_error = Some(error.into());
            dest.retry_count += 1;
            dest.updated_at = Utc::now();
        }
        self.last_error = self.destinations.values().filter_map(|d| d.last_error.clone()).next();
        self.update_overall_status();
    }

    /// Update overall status based on destination statuses.
    fn update_overall_status(&mut self) {
        if self.destinations.is_empty() {
            return;
        }

        let all_complete = self.destinations.values().all(|d| d.status.is_complete());
        let any_failed = self.destinations.values().any(|d| d.status.is_failed());
        let any_pending = self.destinations.values().any(|d| d.status.is_pending());

        self.status = if all_complete {
            self.completed_at = Some(Utc::now());
            ObjectReplicationStatus::Complete
        } else if any_failed && !any_pending {
            ObjectReplicationStatus::Failed
        } else if any_pending {
            ObjectReplicationStatus::InProgress
        } else {
            ObjectReplicationStatus::Pending
        };

        self.retry_count = self.destinations.values().map(|d| d.retry_count).max().unwrap_or(0);
    }

    /// Check if replication is complete to all destinations.
    pub fn is_complete(&self) -> bool {
        self.status.is_complete()
    }

    /// Check if replication failed to any destination.
    pub fn has_failures(&self) -> bool {
        self.destinations.values().any(|d| d.status.is_failed())
    }

    /// Get the number of destinations that have completed.
    pub fn completed_count(&self) -> usize {
        self.destinations.values().filter(|d| d.status.is_complete()).count()
    }
}

/// Status for replication to a specific destination region.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationStatus {
    /// Current status.
    pub status: ObjectReplicationStatus,
    /// When this destination was last updated.
    pub updated_at: DateTime<Utc>,
    /// When replication completed (if complete).
    pub completed_at: Option<DateTime<Utc>>,
    /// Progress for large objects.
    pub progress: Option<ReplicationProgress>,
    /// Retry count.
    pub retry_count: u32,
    /// Last error message.
    pub last_error: Option<String>,
}

impl Default for DestinationStatus {
    fn default() -> Self {
        Self {
            status: ObjectReplicationStatus::Pending,
            updated_at: Utc::now(),
            completed_at: None,
            progress: None,
            retry_count: 0,
            last_error: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_status_display() {
        assert_eq!(ObjectReplicationStatus::Pending.to_string(), "PENDING");
        assert_eq!(ObjectReplicationStatus::Complete.to_string(), "COMPLETE");
        assert_eq!(ObjectReplicationStatus::Failed.to_string(), "FAILED");
    }

    #[test]
    fn test_replication_status_checks() {
        assert!(ObjectReplicationStatus::Complete.is_complete());
        assert!(ObjectReplicationStatus::Replica.is_complete());
        assert!(!ObjectReplicationStatus::Pending.is_complete());

        assert!(ObjectReplicationStatus::Pending.is_pending());
        assert!(ObjectReplicationStatus::InProgress.is_pending());
        assert!(!ObjectReplicationStatus::Complete.is_pending());

        assert!(ObjectReplicationStatus::Failed.is_failed());
        assert!(!ObjectReplicationStatus::Complete.is_failed());
    }

    #[test]
    fn test_replication_progress() {
        let mut progress = ReplicationProgress::new(1000);
        assert_eq!(progress.percentage(), 0.0);
        assert!(!progress.is_complete());

        progress.update(500);
        assert_eq!(progress.percentage(), 50.0);
        assert!(!progress.is_complete());

        progress.update(1000);
        assert_eq!(progress.percentage(), 100.0);
        assert!(progress.is_complete());
    }

    #[test]
    fn test_replication_progress_empty() {
        let progress = ReplicationProgress::new(0);
        assert_eq!(progress.percentage(), 100.0);
        assert!(progress.is_complete());
    }

    #[test]
    fn test_replication_status_info() {
        let mut info =
            ReplicationStatusInfo::new("my-bucket", "my-key", RegionId::new("us-east-1"), 12345);

        info.add_destination(RegionId::new("us-west-2"));
        info.add_destination(RegionId::new("eu-west-1"));

        assert_eq!(info.status, ObjectReplicationStatus::Pending);
        assert_eq!(info.destinations.len(), 2);

        // Update one destination
        info.update_destination(&RegionId::new("us-west-2"), ObjectReplicationStatus::Complete);
        assert_eq!(info.status, ObjectReplicationStatus::InProgress);
        assert_eq!(info.completed_count(), 1);

        // Update second destination
        info.update_destination(&RegionId::new("eu-west-1"), ObjectReplicationStatus::Complete);
        assert_eq!(info.status, ObjectReplicationStatus::Complete);
        assert!(info.is_complete());
        assert!(info.completed_at.is_some());
    }

    #[test]
    fn test_replication_status_with_failure() {
        let mut info =
            ReplicationStatusInfo::new("my-bucket", "my-key", RegionId::new("us-east-1"), 12345);

        info.add_destination(RegionId::new("us-west-2"));

        info.record_error(&RegionId::new("us-west-2"), "Connection timeout");

        assert!(info.has_failures());
        assert_eq!(info.last_error, Some("Connection timeout".to_string()));
        assert_eq!(info.retry_count, 1);
    }

    #[test]
    fn test_destination_status_default() {
        let status = DestinationStatus::default();
        assert_eq!(status.status, ObjectReplicationStatus::Pending);
        assert!(status.completed_at.is_none());
        assert_eq!(status.retry_count, 0);
    }
}
