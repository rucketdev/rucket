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
pub mod crypto;
pub mod erasure;
pub mod events;
pub mod local;
pub mod metadata;
pub mod metrics;
pub mod placement;
pub mod replicated;
pub mod streaming;
pub mod sync;
pub mod wal;

#[cfg(test)]
mod sync_durability_tests;

pub use backend::{DeleteObjectResult, ObjectHeaders, PutObjectResult, StorageBackend};
pub use batch::{BatchConfig, BatchWriter};
pub use crypto::{CryptoError, EncryptionAlgorithm, EncryptionMetadata, SseS3Provider};
// Erasure coding integration
pub use erasure::{
    ErasureObjectMetadata, ErasureStorageConfig, ErasureStorageError, ShardDecoder, ShardEncoder,
    ShardLocation, ShardPlacement, DEFAULT_EC_THRESHOLD,
};
pub use events::{CollectingEventSink, EventHandler, NoOpEventSink, StorageEvent};
pub use local::LocalStorage;
pub use metadata::{MetadataBackend, RedbMetadataStore};
pub use placement::{
    NodeId, PlacementPolicy, PlacementResult, SharedPlacementPolicy, SingleNodePlacement,
};
pub use replicated::ReplicatedStorage;
pub use sync::SyncManager;
pub use wal::{RecoveryManager, RecoveryStats, WalEntry, WalReader, WalWriter, WalWriterConfig};
