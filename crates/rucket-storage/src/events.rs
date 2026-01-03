//! Storage events for replication and change streaming.
//!
//! This module provides high-level events that are emitted after storage
//! operations complete. These events enable:
//!
//! - Cross-region replication (Phase 4)
//! - Change data capture (CDC)
//! - Event-driven workflows
//! - Audit logging
//!
//! # Phase 1 Usage
//!
//! In Phase 1, events are defined but not actively used. The infrastructure
//! is in place for when replication is enabled in later phases.
//!
//! # Example
//!
//! ```ignore
//! use rucket_storage::events::{StorageEvent, EventHandler};
//!
//! let handler: EventHandler = Arc::new(|event| {
//!     match event {
//!         StorageEvent::ObjectCreated { bucket, key, .. } => {
//!             println!("Object created: {}/{}", bucket, key);
//!         }
//!         _ => {}
//!     }
//! });
//! ```

use std::sync::Arc;

/// A storage event representing a completed operation.
///
/// Events are emitted after the operation has been durably committed
/// (i.e., after WAL fsync). They contain all information needed for
/// replication or CDC consumers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorageEvent {
    /// An object was created or updated.
    ObjectCreated {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID (or "null" for non-versioned).
        version_id: String,
        /// HLC timestamp for causality ordering.
        hlc: u64,
        /// Object size in bytes.
        size: u64,
        /// Object ETag.
        etag: String,
        /// Placement group for this object.
        placement_group: u32,
    },

    /// An object was deleted.
    ObjectDeleted {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Version ID if deleting a specific version.
        version_id: Option<String>,
        /// HLC timestamp for causality ordering.
        hlc: u64,
        /// Whether this created a delete marker (versioned bucket).
        is_delete_marker: bool,
        /// Placement group for this object.
        placement_group: u32,
    },

    /// A bucket was created.
    BucketCreated {
        /// Bucket name.
        bucket: String,
        /// HLC timestamp for causality ordering.
        hlc: u64,
    },

    /// A bucket was deleted.
    BucketDeleted {
        /// Bucket name.
        bucket: String,
        /// HLC timestamp for causality ordering.
        hlc: u64,
    },

    /// A multipart upload was completed.
    MultipartCompleted {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Upload ID.
        upload_id: String,
        /// Final version ID.
        version_id: String,
        /// HLC timestamp.
        hlc: u64,
        /// Total object size.
        size: u64,
        /// Object ETag.
        etag: String,
        /// Placement group.
        placement_group: u32,
    },

    /// A multipart upload was aborted.
    MultipartAborted {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// Upload ID.
        upload_id: String,
        /// HLC timestamp.
        hlc: u64,
    },
}

impl StorageEvent {
    /// Returns the bucket name for this event.
    #[must_use]
    pub fn bucket(&self) -> &str {
        match self {
            Self::ObjectCreated { bucket, .. }
            | Self::ObjectDeleted { bucket, .. }
            | Self::BucketCreated { bucket, .. }
            | Self::BucketDeleted { bucket, .. }
            | Self::MultipartCompleted { bucket, .. }
            | Self::MultipartAborted { bucket, .. } => bucket,
        }
    }

    /// Returns the object key for this event, if applicable.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        match self {
            Self::ObjectCreated { key, .. }
            | Self::ObjectDeleted { key, .. }
            | Self::MultipartCompleted { key, .. }
            | Self::MultipartAborted { key, .. } => Some(key),
            Self::BucketCreated { .. } | Self::BucketDeleted { .. } => None,
        }
    }

    /// Returns the HLC timestamp for this event.
    #[must_use]
    pub fn hlc(&self) -> u64 {
        match self {
            Self::ObjectCreated { hlc, .. }
            | Self::ObjectDeleted { hlc, .. }
            | Self::BucketCreated { hlc, .. }
            | Self::BucketDeleted { hlc, .. }
            | Self::MultipartCompleted { hlc, .. }
            | Self::MultipartAborted { hlc, .. } => *hlc,
        }
    }

    /// Returns the placement group for this event, if applicable.
    #[must_use]
    pub fn placement_group(&self) -> Option<u32> {
        match self {
            Self::ObjectCreated { placement_group, .. }
            | Self::ObjectDeleted { placement_group, .. }
            | Self::MultipartCompleted { placement_group, .. } => Some(*placement_group),
            Self::BucketCreated { .. }
            | Self::BucketDeleted { .. }
            | Self::MultipartAborted { .. } => None,
        }
    }

    /// Returns true if this is an object-level event.
    #[must_use]
    pub fn is_object_event(&self) -> bool {
        matches!(
            self,
            Self::ObjectCreated { .. }
                | Self::ObjectDeleted { .. }
                | Self::MultipartCompleted { .. }
                | Self::MultipartAborted { .. }
        )
    }

    /// Returns true if this is a bucket-level event.
    #[must_use]
    pub fn is_bucket_event(&self) -> bool {
        matches!(self, Self::BucketCreated { .. } | Self::BucketDeleted { .. })
    }

    /// Returns a short description of the event type.
    #[must_use]
    pub fn event_type(&self) -> &'static str {
        match self {
            Self::ObjectCreated { .. } => "ObjectCreated",
            Self::ObjectDeleted { .. } => "ObjectDeleted",
            Self::BucketCreated { .. } => "BucketCreated",
            Self::BucketDeleted { .. } => "BucketDeleted",
            Self::MultipartCompleted { .. } => "MultipartCompleted",
            Self::MultipartAborted { .. } => "MultipartAborted",
        }
    }
}

/// Type alias for an event handler callback.
///
/// Event handlers receive events asynchronously and should not block.
/// For Phase 1, this is typically unused (None).
pub type EventHandler = Arc<dyn Fn(StorageEvent) + Send + Sync>;

/// A no-op event sink that discards all events.
///
/// Used when event handling is disabled (Phase 1 default).
#[derive(Debug, Clone, Default)]
pub struct NoOpEventSink;

impl NoOpEventSink {
    /// Creates a new no-op event sink.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Discards the event (no-op).
    pub fn emit(&self, _event: StorageEvent) {
        // Intentionally empty - events are discarded
    }
}

/// Event sink that collects events for testing or buffering.
#[derive(Debug, Default)]
pub struct CollectingEventSink {
    events: std::sync::Mutex<Vec<StorageEvent>>,
}

impl CollectingEventSink {
    /// Creates a new collecting event sink.
    #[must_use]
    pub fn new() -> Self {
        Self { events: std::sync::Mutex::new(Vec::new()) }
    }

    /// Emits an event by adding it to the collection.
    pub fn emit(&self, event: StorageEvent) {
        self.events.lock().expect("lock poisoned").push(event);
    }

    /// Returns all collected events.
    #[must_use]
    pub fn events(&self) -> Vec<StorageEvent> {
        self.events.lock().expect("lock poisoned").clone()
    }

    /// Clears all collected events.
    pub fn clear(&self) {
        self.events.lock().expect("lock poisoned").clear();
    }

    /// Returns the number of collected events.
    #[must_use]
    pub fn len(&self) -> usize {
        self.events.lock().expect("lock poisoned").len()
    }

    /// Returns true if no events have been collected.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.events.lock().expect("lock poisoned").is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_created_event() {
        let event = StorageEvent::ObjectCreated {
            bucket: "test-bucket".to_string(),
            key: "path/to/object.txt".to_string(),
            version_id: "v1".to_string(),
            hlc: 12345,
            size: 1024,
            etag: "\"abc123\"".to_string(),
            placement_group: 0,
        };

        assert_eq!(event.bucket(), "test-bucket");
        assert_eq!(event.key(), Some("path/to/object.txt"));
        assert_eq!(event.hlc(), 12345);
        assert_eq!(event.placement_group(), Some(0));
        assert!(event.is_object_event());
        assert!(!event.is_bucket_event());
        assert_eq!(event.event_type(), "ObjectCreated");
    }

    #[test]
    fn test_object_deleted_event() {
        let event = StorageEvent::ObjectDeleted {
            bucket: "test-bucket".to_string(),
            key: "deleted-key".to_string(),
            version_id: Some("v2".to_string()),
            hlc: 12346,
            is_delete_marker: false,
            placement_group: 0,
        };

        assert_eq!(event.bucket(), "test-bucket");
        assert_eq!(event.key(), Some("deleted-key"));
        assert_eq!(event.hlc(), 12346);
        assert!(event.is_object_event());
        assert_eq!(event.event_type(), "ObjectDeleted");
    }

    #[test]
    fn test_bucket_created_event() {
        let event = StorageEvent::BucketCreated { bucket: "new-bucket".to_string(), hlc: 100 };

        assert_eq!(event.bucket(), "new-bucket");
        assert_eq!(event.key(), None);
        assert_eq!(event.hlc(), 100);
        assert_eq!(event.placement_group(), None);
        assert!(!event.is_object_event());
        assert!(event.is_bucket_event());
        assert_eq!(event.event_type(), "BucketCreated");
    }

    #[test]
    fn test_bucket_deleted_event() {
        let event = StorageEvent::BucketDeleted { bucket: "old-bucket".to_string(), hlc: 200 };

        assert_eq!(event.bucket(), "old-bucket");
        assert!(event.is_bucket_event());
        assert_eq!(event.event_type(), "BucketDeleted");
    }

    #[test]
    fn test_noop_event_sink() {
        let sink = NoOpEventSink::new();
        let event = StorageEvent::BucketCreated { bucket: "test".to_string(), hlc: 0 };

        // Should not panic
        sink.emit(event);
    }

    #[test]
    fn test_collecting_event_sink() {
        let sink = CollectingEventSink::new();
        assert!(sink.is_empty());

        sink.emit(StorageEvent::BucketCreated { bucket: "bucket1".to_string(), hlc: 1 });
        sink.emit(StorageEvent::BucketCreated { bucket: "bucket2".to_string(), hlc: 2 });

        assert_eq!(sink.len(), 2);
        assert!(!sink.is_empty());

        let events = sink.events();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].bucket(), "bucket1");
        assert_eq!(events[1].bucket(), "bucket2");

        sink.clear();
        assert!(sink.is_empty());
    }

    #[test]
    fn test_multipart_completed_event() {
        let event = StorageEvent::MultipartCompleted {
            bucket: "bucket".to_string(),
            key: "large-file.bin".to_string(),
            upload_id: "upload-123".to_string(),
            version_id: "v1".to_string(),
            hlc: 500,
            size: 1024 * 1024 * 100,
            etag: "\"abc-5\"".to_string(),
            placement_group: 0,
        };

        assert!(event.is_object_event());
        assert_eq!(event.event_type(), "MultipartCompleted");
        assert_eq!(event.placement_group(), Some(0));
    }

    #[test]
    fn test_multipart_aborted_event() {
        let event = StorageEvent::MultipartAborted {
            bucket: "bucket".to_string(),
            key: "cancelled-upload.bin".to_string(),
            upload_id: "upload-456".to_string(),
            hlc: 600,
        };

        assert!(event.is_object_event());
        assert_eq!(event.event_type(), "MultipartAborted");
        assert_eq!(event.placement_group(), None);
    }
}
