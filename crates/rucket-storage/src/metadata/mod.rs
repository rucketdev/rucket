// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Metadata storage backends.
//!
//! This module provides the [`MetadataBackend`] trait and implementations
//! for storing object and bucket metadata.
//!
//! Currently supported backends:
//! - [`RedbMetadataStore`]: Pure Rust embedded database (default)
//!
//! Future backends (Phase 3):
//! - File-based: `.meta` sidecar files for cluster mode

mod backend;
mod redb_backend;

pub use backend::MetadataBackend;
pub use redb_backend::RedbMetadataStore;
