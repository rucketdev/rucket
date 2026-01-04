//! Raft state machine implementation.
//!
//! This module provides the metadata state machine that applies
//! commands to the underlying storage.

mod metadata_sm;

pub use metadata_sm::{MetadataSnapshotBuilder, MetadataStateMachine, SnapshotData};
