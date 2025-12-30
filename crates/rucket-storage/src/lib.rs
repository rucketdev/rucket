// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Storage backend for Rucket object storage.
//!
//! This crate provides:
//! - redb-based metadata storage
//! - Local filesystem storage for object data
//! - Streaming I/O utilities

#![deny(unsafe_code)]
#![warn(missing_docs)]

#[cfg(feature = "bench")]
pub mod direct_io;

pub mod backend;
pub mod batch;
pub mod local;
pub mod metadata;
pub mod streaming;
pub mod sync;
pub mod wal;

pub use backend::StorageBackend;
pub use batch::{BatchConfig, BatchWriter};
pub use local::LocalStorage;
pub use metadata::{MetadataBackend, RedbMetadataStore};
pub use sync::SyncManager;
pub use wal::{RecoveryManager, RecoveryStats, WalEntry, WalReader, WalWriter, WalWriterConfig};
