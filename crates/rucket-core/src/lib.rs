//! Core types and utilities for Rucket object storage.
//!
//! This crate provides the fundamental building blocks used across all Rucket components:
//! - Configuration management
//! - Error types with S3-compatible error codes
//! - Common data types (ETag, metadata, etc.)

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod encryption;
pub mod error;
pub mod hlc;
pub mod lifecycle;
pub mod policy;
pub mod public_access_block;
pub mod types;

pub use config::{
    BatchConfig, CheckpointConfig, Config, DurabilityPreset, RecoveryMode, RedbConfig, SyncConfig,
    SyncStrategy, WalConfig, WalSyncMode,
};
pub use error::{Error, Result};
pub use hlc::{
    ClockError, ClockHealthMetrics, ClockResult, HlcClock, HlcTimestamp,
    DRIFT_WARNING_THRESHOLD_MS, MAX_CLOCK_SKEW_MS,
};
