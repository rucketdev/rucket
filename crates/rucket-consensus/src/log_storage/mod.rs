//! Raft log storage implementation.
//!
//! This module provides persistent storage for the Raft log using redb.

mod redb_log;

pub use redb_log::{LogStorageError, RedbLogReader, RedbLogStorage};
