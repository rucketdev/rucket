//! WAL entry types for crash recovery.
//!
//! Each entry represents an intent or completion of an operation,
//! allowing recovery to identify incomplete operations after a crash.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single entry in the write-ahead log.
///
/// Entries follow an intent/commit pattern:
/// - Intent entries are written BEFORE the operation starts
/// - Commit entries are written AFTER the operation completes
///
/// During recovery, an Intent without a matching Commit indicates
/// an incomplete operation that should be rolled back.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalEntry {
    /// Intent to write (PUT) an object.
    ///
    /// Written before the data file is created. If recovery finds this
    /// without a matching `PutCommit`, the data file (if any) is deleted.
    PutIntent {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// UUID for the data file.
        uuid: Uuid,
        /// Object size in bytes.
        size: u64,
        /// CRC32C checksum of the data.
        crc32c: u32,
        /// Timestamp when the intent was created (milliseconds since epoch).
        timestamp: i64,
    },

    /// Object write completed successfully.
    ///
    /// Written after the data file is renamed and metadata is updated.
    PutCommit {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// UUID of the committed data file.
        uuid: Uuid,
    },

    /// Intent to delete an object.
    ///
    /// Written before the data file is deleted. If recovery finds this
    /// without a matching `DeleteCommit`, the file is kept (rollback).
    DeleteIntent {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// UUID of the data file being deleted.
        old_uuid: Uuid,
        /// Timestamp when the intent was created.
        timestamp: i64,
    },

    /// Delete completed successfully.
    ///
    /// Written after the data file is deleted and metadata is updated.
    DeleteCommit {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
    },

    /// Checkpoint marker indicating all prior entries have been applied.
    ///
    /// After a checkpoint, the WAL can be truncated/rotated.
    Checkpoint {
        /// Sequence number at checkpoint.
        sequence: u64,
        /// Timestamp of the checkpoint.
        timestamp: i64,
    },
}

impl WalEntry {
    /// Returns the bucket name for this entry, if applicable.
    #[must_use]
    pub fn bucket(&self) -> Option<&str> {
        match self {
            Self::PutIntent { bucket, .. }
            | Self::PutCommit { bucket, .. }
            | Self::DeleteIntent { bucket, .. }
            | Self::DeleteCommit { bucket, .. } => Some(bucket),
            Self::Checkpoint { .. } => None,
        }
    }

    /// Returns the object key for this entry, if applicable.
    #[must_use]
    pub fn key(&self) -> Option<&str> {
        match self {
            Self::PutIntent { key, .. }
            | Self::PutCommit { key, .. }
            | Self::DeleteIntent { key, .. }
            | Self::DeleteCommit { key, .. } => Some(key),
            Self::Checkpoint { .. } => None,
        }
    }

    /// Returns true if this is an intent entry (not yet committed).
    #[must_use]
    pub fn is_intent(&self) -> bool {
        matches!(self, Self::PutIntent { .. } | Self::DeleteIntent { .. })
    }

    /// Returns true if this is a commit entry.
    #[must_use]
    pub fn is_commit(&self) -> bool {
        matches!(self, Self::PutCommit { .. } | Self::DeleteCommit { .. })
    }

    /// Returns true if this is a checkpoint entry.
    #[must_use]
    pub fn is_checkpoint(&self) -> bool {
        matches!(self, Self::Checkpoint { .. })
    }
}

/// A WAL entry with its sequence number.
#[derive(Debug, Clone)]
pub struct SequencedEntry {
    /// Sequence number (monotonically increasing).
    pub sequence: u64,
    /// The entry data.
    pub entry: WalEntry,
}

/// Represents an incomplete operation found during recovery.
#[derive(Debug, Clone)]
pub enum IncompleteOperation {
    /// A PUT that started but didn't complete.
    IncompletePut {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// UUID of the data file to clean up.
        uuid: Uuid,
        /// Sequence number of the intent.
        sequence: u64,
    },
    /// A DELETE that started but didn't complete.
    IncompleteDelete {
        /// Bucket name.
        bucket: String,
        /// Object key.
        key: String,
        /// UUID of the file that should be kept.
        uuid: Uuid,
        /// Sequence number of the intent.
        sequence: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entry_serialization() {
        let entry = WalEntry::PutIntent {
            bucket: "test-bucket".to_string(),
            key: "test-key".to_string(),
            uuid: Uuid::new_v4(),
            size: 1024,
            crc32c: 0x12345678,
            timestamp: 1234567890,
        };

        let encoded = bincode::serialize(&entry).unwrap();
        let decoded: WalEntry = bincode::deserialize(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_entry_accessors() {
        let entry = WalEntry::PutIntent {
            bucket: "bucket".to_string(),
            key: "key".to_string(),
            uuid: Uuid::nil(),
            size: 0,
            crc32c: 0,
            timestamp: 0,
        };

        assert_eq!(entry.bucket(), Some("bucket"));
        assert_eq!(entry.key(), Some("key"));
        assert!(entry.is_intent());
        assert!(!entry.is_commit());

        let checkpoint = WalEntry::Checkpoint { sequence: 1, timestamp: 0 };
        assert!(checkpoint.bucket().is_none());
        assert!(checkpoint.is_checkpoint());
    }
}
