//! Core types and utilities for Rucket object storage.
//!
//! This crate provides the fundamental building blocks used across all Rucket components:
//! - Configuration management
//! - Error types with S3-compatible error codes
//! - Common data types (ETag, metadata, etc.)

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod error;
pub mod types;

pub use config::{
    BatchConfig, CheckpointConfig, Config, DurabilityPreset, RedbConfig, RecoveryMode, SyncConfig,
    SyncStrategy, WalConfig, WalSyncMode,
};
pub use error::{Error, Result};
