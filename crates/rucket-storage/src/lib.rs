// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Storage backend for Rucket object storage.
//!
//! This crate provides:
//! - redb-based metadata storage
//! - Local filesystem storage for object data
//! - Streaming I/O utilities

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod backend;
pub mod local;
pub mod metadata;
pub mod streaming;
pub mod sync;

pub use backend::StorageBackend;
pub use local::LocalStorage;
pub use metadata::{MetadataBackend, RedbMetadataStore};
pub use sync::SyncManager;
