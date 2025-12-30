// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

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

pub use config::{BatchConfig, Config, DurabilityPreset, RedbConfig, SyncConfig, SyncStrategy};
pub use error::{Error, Result};
