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

pub use backend::{DeleteResult, ListVersionsResult, MetadataBackend, VersionEntry};
pub use redb_backend::RedbMetadataStore;
