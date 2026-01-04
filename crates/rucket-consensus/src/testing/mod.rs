//! Testing utilities for chaos testing, fault injection, and linearizability checking.
//!
//! This module provides tools for testing distributed systems under failure conditions:
//!
//! - [`ChaosController`]: Fault injection controller for network delays, drops, and partitions
//! - [`ChaosNetwork`]: Network wrapper that applies controlled failures
//! - [`HistoryRecorder`]: Records operation histories for linearizability checking
//! - [`BucketHistoryRecorder`]: Records bucket operation histories
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Test Harness                                │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!         ┌────────────────────┼────────────────────┐
//!         ▼                    ▼                    ▼
//! ┌───────────────┐    ┌───────────────┐    ┌───────────────┐
//! │ChaosController│    │HistoryRecorder│    │  Turmoil Sim  │
//! │  (Faults)     │    │(Linearizability│    │ (Determinism) │
//! └───────────────┘    └───────────────┘    └───────────────┘
//!         │                    │                    │
//!         ▼                    ▼                    ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   Raft Consensus Cluster                         │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Chaos Testing Example
//!
//! ```ignore
//! use rucket_consensus::testing::{ChaosController, ChaosNetwork};
//!
//! let controller = ChaosController::new();
//!
//! // Inject 100ms delay between node 1 and node 2
//! controller.inject_delay(1, 2, Duration::from_millis(100));
//!
//! // Create a network partition isolating node 3
//! controller.partition(hashset![3]);
//!
//! // Run tests...
//!
//! // Heal the partition
//! controller.heal_all();
//! ```
//!
//! # Linearizability Testing Example
//!
//! ```ignore
//! use rucket_consensus::testing::{HistoryRecorder, KvOp, KvRet};
//!
//! let recorder = HistoryRecorder::new();
//!
//! // Thread 1 writes
//! recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v".into() })?;
//! recorder.return_value(1, KvRet::PutOk)?;
//!
//! // Thread 2 reads
//! recorder.invoke(2, KvOp::Get { key: "k".into() })?;
//! recorder.return_value(2, KvRet::GetOk(Some("v".into())))?;
//!
//! // Verify linearizability
//! assert!(recorder.check_linearizable().is_ok());
//! ```

#[cfg(feature = "chaos-testing")]
mod chaos;
#[cfg(feature = "chaos-testing")]
mod linearizability;

#[cfg(feature = "chaos-testing")]
pub use chaos::{ChaosController, ChaosError, ChaosNetwork, PartitionId};
#[cfg(feature = "chaos-testing")]
pub use linearizability::{
    BucketHistoryRecorder, BucketOp, BucketRet, BucketSpec, HistoryRecorder, KvOp, KvRet, KvSpec,
    LinearizabilityError, ThreadId,
};
