// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Write-Ahead Log (WAL) for crash recovery.
//!
//! The WAL provides durability guarantees by logging operation intents before
//! performing them. On startup, incomplete operations are identified and
//! rolled back.
//!
//! # How It Works
//!
//! Each write operation follows this pattern:
//!
//! 1. Write `PutIntent` to WAL (with fsync)
//! 2. Write data file
//! 3. Rename temp file to final location
//! 4. Update metadata
//! 5. Write `PutCommit` to WAL
//!
//! If the system crashes at any point:
//! - Crash before step 1: Nothing happened, no recovery needed
//! - Crash after step 1 but before step 5: `PutIntent` without `PutCommit`
//!   Recovery deletes the orphaned data file
//! - Crash after step 5: Operation completed, no recovery needed
//!
//! # Checkpointing
//!
//! Checkpoints mark points where all prior operations are known to be complete.
//! After a checkpoint, the WAL can be truncated to reduce size and recovery time.
//!
//! # Example
//!
//! ```ignore
//! use rucket_storage::wal::{WalWriter, WalWriterConfig, WalEntry, RecoveryManager};
//!
//! // On startup: recover from any crash
//! let recovery = RecoveryManager::new(wal_dir, data_dir);
//! let stats = recovery.recover().await?;
//! println!("Recovered {} incomplete operations", stats.puts_rolled_back);
//!
//! // Normal operation: use WAL for writes
//! let writer = WalWriter::open(&config)?;
//!
//! // Log intent
//! writer.append_sync(WalEntry::PutIntent { ... }).await?;
//!
//! // Perform operation...
//!
//! // Log completion
//! writer.append(WalEntry::PutCommit { ... }).await?;
//! ```

mod entry;
mod reader;
mod recovery;
mod writer;

pub use entry::{IncompleteOperation, SequencedEntry, WalEntry};
pub use reader::WalReader;
pub use recovery::{RecoveryManager, RecoveryStats};
pub use writer::{WalSyncMode, WalWriter, WalWriterConfig};
