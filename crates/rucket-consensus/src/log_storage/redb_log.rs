//! Redb-backed Raft log storage.
//!
//! This module implements `RaftLogStorage` and `RaftLogReader` traits using redb
//! as the underlying storage engine.

use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{ErrorSubject, ErrorVerb, LogId, OptionalSend, StorageError, Vote};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::RwLock;

use crate::types::{RaftEntry, RaftLogId, RaftNodeId, RaftTypeConfig, RaftVote};

/// Table for Raft log entries.
/// Key: log index (u64), Value: bincode-serialized Entry
const LOG_TABLE: TableDefinition<'static, u64, &'static [u8]> = TableDefinition::new("raft_log");

/// Table for Raft metadata (vote, committed log id, last purged log id).
/// Key: metadata key, Value: bincode-serialized value
const META_TABLE: TableDefinition<'static, &'static str, &'static [u8]> =
    TableDefinition::new("raft_meta");

// Metadata keys
const KEY_VOTE: &str = "vote";
const KEY_COMMITTED: &str = "committed";
const KEY_LAST_PURGED: &str = "last_purged";

/// Error type for log storage operations.
#[derive(Debug, thiserror::Error)]
pub enum LogStorageError {
    /// Database error.
    #[error("database error: {0}")]
    Database(String),

    /// Serialization error.
    #[error("serialization error: {0}")]
    Serialization(String),
}

impl LogStorageError {
    fn from_redb<E: std::error::Error>(e: E) -> Self {
        LogStorageError::Database(e.to_string())
    }

    fn from_bincode(e: bincode::Error) -> Self {
        LogStorageError::Serialization(e.to_string())
    }
}

fn storage_error<E: std::error::Error>(
    subject: ErrorSubject<RaftNodeId>,
    verb: ErrorVerb,
    e: E,
) -> StorageError<RaftNodeId> {
    StorageError::from_io_error(subject, verb, io::Error::other(e.to_string()))
}

/// Redb-backed Raft log storage.
///
/// Stores:
/// - Raft log entries (indexed by log index)
/// - Vote (current term and voted_for)
/// - Committed log index
/// - Last purged log id
pub struct RedbLogStorage {
    /// The redb database.
    db: Arc<Database>,

    /// Cached last purged log id.
    last_purged: RwLock<Option<RaftLogId>>,

    /// Cached last log id.
    last_log_id: RwLock<Option<RaftLogId>>,
}

impl RedbLogStorage {
    /// Opens or creates a new Raft log storage at the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or created.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogStorageError> {
        let db = Database::create(path).map_err(LogStorageError::from_redb)?;

        // Initialize tables if they don't exist
        let write_txn = db.begin_write().map_err(LogStorageError::from_redb)?;
        {
            let _ = write_txn.open_table(LOG_TABLE).map_err(LogStorageError::from_redb)?;
            let _ = write_txn.open_table(META_TABLE).map_err(LogStorageError::from_redb)?;
        }
        write_txn.commit().map_err(LogStorageError::from_redb)?;

        let storage = Self {
            db: Arc::new(db),
            last_purged: RwLock::new(None),
            last_log_id: RwLock::new(None),
        };

        Ok(storage)
    }

    /// Loads cached state from the database.
    ///
    /// Call this after opening to initialize the cache.
    pub async fn load_state(&self) -> Result<(), LogStorageError> {
        let read_txn = self.db.begin_read().map_err(LogStorageError::from_redb)?;

        // Load last_purged
        let meta_table = read_txn.open_table(META_TABLE).map_err(LogStorageError::from_redb)?;
        if let Some(data) = meta_table.get(KEY_LAST_PURGED).map_err(LogStorageError::from_redb)? {
            let bytes: &[u8] = data.value();
            let log_id: RaftLogId =
                bincode::deserialize(bytes).map_err(LogStorageError::from_bincode)?;
            *self.last_purged.write().await = Some(log_id);
        }

        // Compute last_log_id from log table
        let log_table = read_txn.open_table(LOG_TABLE).map_err(LogStorageError::from_redb)?;
        if let Some(result) = log_table.last().map_err(LogStorageError::from_redb)? {
            let bytes: &[u8] = result.1.value();
            let entry: RaftEntry =
                bincode::deserialize(bytes).map_err(LogStorageError::from_bincode)?;
            *self.last_log_id.write().await = Some(entry.log_id);
        } else {
            // No entries, last_log_id = last_purged
            *self.last_log_id.write().await = *self.last_purged.read().await;
        }

        Ok(())
    }

    /// Reads the vote from storage.
    fn read_vote_sync(&self) -> Result<Option<RaftVote>, LogStorageError> {
        let read_txn = self.db.begin_read().map_err(LogStorageError::from_redb)?;
        let meta_table = read_txn.open_table(META_TABLE).map_err(LogStorageError::from_redb)?;

        if let Some(data) = meta_table.get(KEY_VOTE).map_err(LogStorageError::from_redb)? {
            let bytes: &[u8] = data.value();
            let vote: RaftVote =
                bincode::deserialize(bytes).map_err(LogStorageError::from_bincode)?;
            Ok(Some(vote))
        } else {
            Ok(None)
        }
    }

    /// Saves the vote to storage.
    fn save_vote_sync(&self, vote: &RaftVote) -> Result<(), LogStorageError> {
        let write_txn = self.db.begin_write().map_err(LogStorageError::from_redb)?;
        {
            let mut meta_table =
                write_txn.open_table(META_TABLE).map_err(LogStorageError::from_redb)?;
            let data = bincode::serialize(vote).map_err(LogStorageError::from_bincode)?;
            meta_table.insert(KEY_VOTE, data.as_slice()).map_err(LogStorageError::from_redb)?;
        }
        write_txn.commit().map_err(LogStorageError::from_redb)?;
        Ok(())
    }

    /// Gets log entries in the given range.
    fn get_entries_sync(
        &self,
        range: impl RangeBounds<u64>,
    ) -> Result<Vec<RaftEntry>, LogStorageError> {
        let read_txn = self.db.begin_read().map_err(LogStorageError::from_redb)?;
        let log_table = read_txn.open_table(LOG_TABLE).map_err(LogStorageError::from_redb)?;

        let mut entries = Vec::new();
        for result in log_table.range(range).map_err(LogStorageError::from_redb)? {
            let (_, value) = result.map_err(LogStorageError::from_redb)?;
            let bytes: &[u8] = value.value();
            let entry: RaftEntry =
                bincode::deserialize(bytes).map_err(LogStorageError::from_bincode)?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Appends entries to the log.
    fn append_entries_sync(&self, entries: Vec<RaftEntry>) -> Result<(), LogStorageError> {
        if entries.is_empty() {
            return Ok(());
        }

        let write_txn = self.db.begin_write().map_err(LogStorageError::from_redb)?;
        {
            let mut log_table =
                write_txn.open_table(LOG_TABLE).map_err(LogStorageError::from_redb)?;
            for entry in &entries {
                let data = bincode::serialize(entry).map_err(LogStorageError::from_bincode)?;
                log_table
                    .insert(entry.log_id.index, data.as_slice())
                    .map_err(LogStorageError::from_redb)?;
            }
        }
        write_txn.commit().map_err(LogStorageError::from_redb)?;

        Ok(())
    }

    /// Truncates the log from the given index (inclusive).
    fn truncate_sync(&self, from: u64) -> Result<(), LogStorageError> {
        let write_txn = self.db.begin_write().map_err(LogStorageError::from_redb)?;
        {
            let mut log_table =
                write_txn.open_table(LOG_TABLE).map_err(LogStorageError::from_redb)?;
            // Delete all entries >= from
            let mut to_delete = Vec::new();
            for result in log_table.range(from..).map_err(LogStorageError::from_redb)? {
                let (key, _) = result.map_err(LogStorageError::from_redb)?;
                to_delete.push(key.value());
            }
            for key in to_delete {
                log_table.remove(key).map_err(LogStorageError::from_redb)?;
            }
        }
        write_txn.commit().map_err(LogStorageError::from_redb)?;

        Ok(())
    }

    /// Purges entries up to and including the given index.
    fn purge_sync(&self, up_to: &RaftLogId) -> Result<(), LogStorageError> {
        let write_txn = self.db.begin_write().map_err(LogStorageError::from_redb)?;
        {
            let mut log_table =
                write_txn.open_table(LOG_TABLE).map_err(LogStorageError::from_redb)?;
            let mut meta_table =
                write_txn.open_table(META_TABLE).map_err(LogStorageError::from_redb)?;

            // Delete all entries <= up_to.index
            let mut to_delete = Vec::new();
            for result in log_table.range(..=up_to.index).map_err(LogStorageError::from_redb)? {
                let (key, _) = result.map_err(LogStorageError::from_redb)?;
                to_delete.push(key.value());
            }
            for key in to_delete {
                log_table.remove(key).map_err(LogStorageError::from_redb)?;
            }

            // Save last_purged
            let data = bincode::serialize(up_to).map_err(LogStorageError::from_bincode)?;
            meta_table
                .insert(KEY_LAST_PURGED, data.as_slice())
                .map_err(LogStorageError::from_redb)?;
        }
        write_txn.commit().map_err(LogStorageError::from_redb)?;

        Ok(())
    }

    /// Gets the log state.
    fn get_log_state_sync(
        &self,
    ) -> Result<(Option<RaftLogId>, Option<RaftLogId>), LogStorageError> {
        let read_txn = self.db.begin_read().map_err(LogStorageError::from_redb)?;
        let log_table = read_txn.open_table(LOG_TABLE).map_err(LogStorageError::from_redb)?;
        let meta_table = read_txn.open_table(META_TABLE).map_err(LogStorageError::from_redb)?;

        // Get last_purged
        let last_purged = if let Some(data) =
            meta_table.get(KEY_LAST_PURGED).map_err(LogStorageError::from_redb)?
        {
            let bytes: &[u8] = data.value();
            Some(bincode::deserialize(bytes).map_err(LogStorageError::from_bincode)?)
        } else {
            None
        };

        // Get last_log_id
        let last_log_id =
            if let Some(result) = log_table.last().map_err(LogStorageError::from_redb)? {
                let bytes: &[u8] = result.1.value();
                let entry: RaftEntry =
                    bincode::deserialize(bytes).map_err(LogStorageError::from_bincode)?;
                Some(entry.log_id)
            } else {
                last_purged
            };

        Ok((last_purged, last_log_id))
    }
}

/// Log reader implementation for replication tasks.
#[derive(Clone)]
pub struct RedbLogReader {
    db: Arc<Database>,
}

impl RedbLogReader {
    /// Creates a new log reader from the shared database.
    pub fn new(db: Arc<Database>) -> Self {
        Self { db }
    }
}

impl RaftLogReader<RaftTypeConfig> for RedbLogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, StorageError<RaftNodeId>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        let log_table = read_txn
            .open_table(LOG_TABLE)
            .map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;

        let mut entries = Vec::new();
        for result in log_table
            .range(range)
            .map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))?
        {
            let (_, value) =
                result.map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
            let bytes: &[u8] = value.value();
            let entry: RaftEntry = bincode::deserialize(bytes).map_err(|e| {
                storage_error(ErrorSubject::Logs, ErrorVerb::Read, LogStorageError::from_bincode(e))
            })?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

impl RaftLogReader<RaftTypeConfig> for RedbLogStorage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<RaftEntry>, StorageError<RaftNodeId>> {
        self.get_entries_sync(range)
            .map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))
    }
}

impl RaftLogStorage<RaftTypeConfig> for RedbLogStorage {
    type LogReader = RedbLogReader;

    async fn get_log_state(
        &mut self,
    ) -> Result<LogState<RaftTypeConfig>, StorageError<RaftNodeId>> {
        let (last_purged, last_log_id) = self
            .get_log_state_sync()
            .map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;

        // Update cache
        *self.last_purged.write().await = last_purged;
        *self.last_log_id.write().await = last_log_id;

        Ok(LogState { last_purged_log_id: last_purged, last_log_id })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        RedbLogReader::new(self.db.clone())
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.save_vote_sync(vote)
            .map_err(|e| storage_error(ErrorSubject::Vote, ErrorVerb::Write, e))
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        self.read_vote_sync().map_err(|e| storage_error(ErrorSubject::Vote, ErrorVerb::Read, e))
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<RaftNodeId>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let write_txn = self
            .db
            .begin_write()
            .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
        {
            let mut meta_table = write_txn
                .open_table(META_TABLE)
                .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
            if let Some(ref log_id) = committed {
                let data = bincode::serialize(log_id).map_err(|e| {
                    storage_error(
                        ErrorSubject::Store,
                        ErrorVerb::Write,
                        LogStorageError::from_bincode(e),
                    )
                })?;
                meta_table
                    .insert(KEY_COMMITTED, data.as_slice())
                    .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
            } else {
                let _ = meta_table
                    .remove(KEY_COMMITTED)
                    .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
            }
        }
        write_txn.commit().map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> Result<Option<LogId<RaftNodeId>>, StorageError<RaftNodeId>> {
        let read_txn = self
            .db
            .begin_read()
            .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Read, e))?;
        let meta_table = read_txn
            .open_table(META_TABLE)
            .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Read, e))?;

        if let Some(data) = meta_table
            .get(KEY_COMMITTED)
            .map_err(|e| storage_error(ErrorSubject::Store, ErrorVerb::Read, e))?
        {
            let bytes: &[u8] = data.value();
            let log_id: LogId<RaftNodeId> = bincode::deserialize(bytes).map_err(|e| {
                storage_error(
                    ErrorSubject::Store,
                    ErrorVerb::Read,
                    LogStorageError::from_bincode(e),
                )
            })?;
            Ok(Some(log_id))
        } else {
            Ok(None)
        }
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RaftTypeConfig>,
    ) -> Result<(), StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = RaftEntry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let entries: Vec<RaftEntry> = entries.into_iter().collect();

        // Update last_log_id cache
        if let Some(last) = entries.last() {
            *self.last_log_id.write().await = Some(last.log_id);
        }

        // Append to storage
        self.append_entries_sync(entries)
            .map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Write, e))?;

        // Signal that log is flushed (we sync on commit)
        callback.log_io_completed(Ok(()));

        Ok(())
    }

    async fn truncate(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        self.truncate_sync(log_id.index)
            .map_err(|e| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;

        // Update cache - get the new last entry
        let (last_purged, last_log_id) = self
            .get_log_state_sync()
            .map_err(|e| storage_error(ErrorSubject::Logs, ErrorVerb::Read, e))?;
        *self.last_purged.write().await = last_purged;
        *self.last_log_id.write().await = last_log_id;

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        self.purge_sync(&log_id)
            .map_err(|e| storage_error(ErrorSubject::Log(log_id), ErrorVerb::Delete, e))?;

        // Update cache
        *self.last_purged.write().await = Some(log_id);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use openraft::EntryPayload;
    use tempfile::TempDir;

    use super::*;

    fn create_entry(index: u64, term: u64) -> RaftEntry {
        openraft::Entry {
            log_id: LogId::new(openraft::CommittedLeaderId::new(term, 1), index),
            payload: EntryPayload::Blank,
        }
    }

    #[tokio::test]
    async fn test_open_and_load() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("raft.db");

        let storage = RedbLogStorage::open(&path).unwrap();
        storage.load_state().await.unwrap();

        let state = storage.get_log_state_sync().unwrap();
        assert_eq!(state.0, None); // no purged
        assert_eq!(state.1, None); // no logs
    }

    #[tokio::test]
    async fn test_vote_persistence() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("raft.db");

        let storage = RedbLogStorage::open(&path).unwrap();

        // Initially no vote
        let vote = storage.read_vote_sync().unwrap();
        assert!(vote.is_none());

        // Save a vote
        let new_vote = Vote::new(1, 1);
        storage.save_vote_sync(&new_vote).unwrap();

        // Read it back
        let vote = storage.read_vote_sync().unwrap();
        assert_eq!(vote, Some(new_vote));
    }

    #[tokio::test]
    async fn test_append_and_get_entries() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("raft.db");

        let storage = RedbLogStorage::open(&path).unwrap();

        // Append entries
        let entries = vec![create_entry(1, 1), create_entry(2, 1), create_entry(3, 2)];
        storage.append_entries_sync(entries.clone()).unwrap();

        // Get all entries
        let result = storage.get_entries_sync(1..4).unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].log_id.index, 1);
        assert_eq!(result[2].log_id.index, 3);

        // Get subset
        let result = storage.get_entries_sync(2..3).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].log_id.index, 2);
    }

    #[tokio::test]
    async fn test_truncate() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("raft.db");

        let storage = RedbLogStorage::open(&path).unwrap();

        // Append entries
        let entries = vec![create_entry(1, 1), create_entry(2, 1), create_entry(3, 2)];
        storage.append_entries_sync(entries).unwrap();

        // Truncate from index 2
        storage.truncate_sync(2).unwrap();

        // Only entry 1 should remain
        let result = storage.get_entries_sync(..).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].log_id.index, 1);
    }

    #[tokio::test]
    async fn test_purge() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("raft.db");

        let storage = RedbLogStorage::open(&path).unwrap();

        // Append entries
        let entries = vec![create_entry(1, 1), create_entry(2, 1), create_entry(3, 2)];
        storage.append_entries_sync(entries).unwrap();

        // Purge up to index 2
        let log_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 2);
        storage.purge_sync(&log_id).unwrap();

        // Only entry 3 should remain
        let result = storage.get_entries_sync(..).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].log_id.index, 3);

        // Verify last_purged is saved
        let (last_purged, _) = storage.get_log_state_sync().unwrap();
        assert_eq!(last_purged, Some(log_id));
    }

    #[tokio::test]
    async fn test_log_state() {
        let tmp_dir = TempDir::new().unwrap();
        let path = tmp_dir.path().join("raft.db");

        let storage = RedbLogStorage::open(&path).unwrap();

        // Empty state
        let (last_purged, last_log) = storage.get_log_state_sync().unwrap();
        assert!(last_purged.is_none());
        assert!(last_log.is_none());

        // Append entries
        let entries = vec![create_entry(1, 1), create_entry(2, 1)];
        storage.append_entries_sync(entries).unwrap();

        let (last_purged, last_log) = storage.get_log_state_sync().unwrap();
        assert!(last_purged.is_none());
        assert_eq!(last_log.map(|l| l.index), Some(2));

        // Purge
        let purge_log_id = LogId::new(openraft::CommittedLeaderId::new(1, 1), 1);
        storage.purge_sync(&purge_log_id).unwrap();

        let (last_purged, last_log) = storage.get_log_state_sync().unwrap();
        assert_eq!(last_purged.map(|l| l.index), Some(1));
        assert_eq!(last_log.map(|l| l.index), Some(2));
    }
}
