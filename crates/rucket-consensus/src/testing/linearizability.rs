//! Linearizability checker for validating distributed system correctness.
//!
//! This module provides tools for verifying that concurrent operations on the
//! distributed storage system are linearizable - meaning they appear to execute
//! atomically and in some sequential order consistent with real-time ordering.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Test Execution                             │
//! │   Thread 1: put(k, v1)  ────────────────►                       │
//! │   Thread 2:      get(k) ──────────────────────►                 │
//! │   Thread 3:           put(k, v2) ───────────────────►           │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                   HistoryRecorder                               │
//! │   Records invoke/return events with timestamps                  │
//! └─────────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                LinearizabilityChecker                           │
//! │   Uses Stateright's WGL algorithm to verify linearizability     │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Example
//!
//! ```ignore
//! use rucket_consensus::testing::{HistoryRecorder, KvOp, KvRet};
//!
//! let recorder = HistoryRecorder::new();
//!
//! // Thread 1 writes value
//! recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v1".into() });
//! // ... operation executes ...
//! recorder.return_value(1, KvRet::PutOk);
//!
//! // Thread 2 reads value
//! recorder.invoke(2, KvOp::Get { key: "k".into() });
//! // ... operation executes ...
//! recorder.return_value(2, KvRet::GetOk(Some("v1".into())));
//!
//! // Check if history is linearizable
//! assert!(recorder.check_linearizable().is_ok());
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use stateright::semantics::{ConsistencyTester, LinearizabilityTester, SequentialSpec};

/// A key-value store operation for linearizability checking.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum KvOp {
    /// Get a value by key.
    Get {
        /// The key to retrieve.
        key: String,
    },
    /// Put a key-value pair.
    Put {
        /// The key to store.
        key: String,
        /// The value to store.
        value: String,
    },
    /// Delete a key.
    Delete {
        /// The key to delete.
        key: String,
    },
    /// Check if a key exists.
    Exists {
        /// The key to check.
        key: String,
    },
    /// Compare-and-swap operation.
    Cas {
        /// The key to update.
        key: String,
        /// The expected current value (None means key should not exist).
        expected: Option<String>,
        /// The new value to set if expected matches.
        new_value: String,
    },
}

/// Return value from a key-value store operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum KvRet {
    /// Get succeeded
    GetOk(Option<String>),
    /// Put succeeded
    PutOk,
    /// Delete succeeded
    DeleteOk,
    /// Exists check result
    ExistsOk(bool),
    /// CAS succeeded (true) or failed due to mismatch (false)
    CasOk(bool),
    /// Operation failed with error
    Error(String),
}

/// Sequential specification for a key-value store.
///
/// This defines the expected behavior of a linearizable key-value store,
/// which Stateright uses to verify operation histories.
#[derive(Clone, Debug, Default)]
pub struct KvSpec {
    store: HashMap<String, String>,
}

impl SequentialSpec for KvSpec {
    type Op = KvOp;
    type Ret = KvRet;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            KvOp::Get { key } => KvRet::GetOk(self.store.get(key).cloned()),
            KvOp::Put { key, value } => {
                self.store.insert(key.clone(), value.clone());
                KvRet::PutOk
            }
            KvOp::Delete { key } => {
                self.store.remove(key);
                KvRet::DeleteOk
            }
            KvOp::Exists { key } => KvRet::ExistsOk(self.store.contains_key(key)),
            KvOp::Cas { key, expected, new_value } => {
                let current = self.store.get(key);
                if current == expected.as_ref() {
                    self.store.insert(key.clone(), new_value.clone());
                    KvRet::CasOk(true)
                } else {
                    KvRet::CasOk(false)
                }
            }
        }
    }

    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
        // Check if the return value matches what we'd expect
        let expected = self.invoke(op);

        // For error returns, we allow any error message
        match (ret, &expected) {
            (KvRet::Error(_), _) => true, // Errors are always valid (operation may have failed)
            _ => ret == &expected,
        }
    }
}

/// Thread ID type for the history recorder.
pub type ThreadId = u64;

/// Records a history of concurrent operations for linearizability checking.
///
/// This struct is thread-safe and can be shared across multiple threads.
#[derive(Clone)]
pub struct HistoryRecorder {
    inner: Arc<Mutex<LinearizabilityTester<ThreadId, KvSpec>>>,
}

impl Default for HistoryRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoryRecorder {
    /// Create a new history recorder with an empty key-value store.
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(LinearizabilityTester::new(KvSpec::default()))) }
    }

    /// Create a new history recorder with initial key-value pairs.
    pub fn with_initial_state(initial: HashMap<String, String>) -> Self {
        Self { inner: Arc::new(Mutex::new(LinearizabilityTester::new(KvSpec { store: initial }))) }
    }

    /// Record that a thread has invoked an operation.
    ///
    /// This should be called immediately before the operation begins.
    pub fn invoke(&self, thread_id: ThreadId, op: KvOp) -> Result<(), String> {
        self.inner.lock().on_invoke(thread_id, op)?;
        Ok(())
    }

    /// Record that a thread's operation has returned.
    ///
    /// This should be called immediately after the operation completes.
    pub fn return_value(&self, thread_id: ThreadId, ret: KvRet) -> Result<(), String> {
        self.inner.lock().on_return(thread_id, ret)?;
        Ok(())
    }

    /// Check if the recorded history is linearizable.
    ///
    /// Returns `Ok(linearization)` if linearizable, with the serialized history.
    /// Returns `Err` if the history is not linearizable or invalid.
    pub fn check_linearizable(&self) -> Result<Vec<(KvOp, KvRet)>, LinearizabilityError> {
        let tester = self.inner.lock();
        tester.serialized_history().ok_or(LinearizabilityError::NotLinearizable)
    }

    /// Get the number of operations recorded (completed + in-flight).
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Check if any operations have been recorded.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Error type for linearizability checking.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinearizabilityError {
    /// The recorded history is not linearizable.
    NotLinearizable,
    /// The history recording was invalid (e.g., return without invoke).
    InvalidHistory(String),
}

impl std::fmt::Display for LinearizabilityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotLinearizable => {
                write!(f, "History is not linearizable")
            }
            Self::InvalidHistory(msg) => write!(f, "Invalid history: {}", msg),
        }
    }
}

impl std::error::Error for LinearizabilityError {}

/// Bucket operation for S3-like metadata linearizability checking.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BucketOp {
    /// Create a bucket.
    CreateBucket {
        /// The bucket name.
        name: String,
    },
    /// Delete a bucket.
    DeleteBucket {
        /// The bucket name.
        name: String,
    },
    /// Check if bucket exists.
    BucketExists {
        /// The bucket name.
        name: String,
    },
    /// List all buckets.
    ListBuckets,
}

/// Return value from bucket operations.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum BucketRet {
    /// Create succeeded
    CreateOk,
    /// Create failed (already exists)
    CreateExists,
    /// Delete succeeded
    DeleteOk,
    /// Delete failed (not found)
    DeleteNotFound,
    /// Exists check result
    ExistsOk(bool),
    /// List result
    ListOk(Vec<String>),
    /// Error occurred
    Error(String),
}

/// Sequential specification for bucket operations.
#[derive(Clone, Debug, Default)]
pub struct BucketSpec {
    buckets: std::collections::HashSet<String>,
}

impl SequentialSpec for BucketSpec {
    type Op = BucketOp;
    type Ret = BucketRet;

    fn invoke(&mut self, op: &Self::Op) -> Self::Ret {
        match op {
            BucketOp::CreateBucket { name } => {
                if self.buckets.contains(name) {
                    BucketRet::CreateExists
                } else {
                    self.buckets.insert(name.clone());
                    BucketRet::CreateOk
                }
            }
            BucketOp::DeleteBucket { name } => {
                if self.buckets.remove(name) {
                    BucketRet::DeleteOk
                } else {
                    BucketRet::DeleteNotFound
                }
            }
            BucketOp::BucketExists { name } => BucketRet::ExistsOk(self.buckets.contains(name)),
            BucketOp::ListBuckets => {
                let mut list: Vec<_> = self.buckets.iter().cloned().collect();
                list.sort();
                BucketRet::ListOk(list)
            }
        }
    }

    fn is_valid_step(&mut self, op: &Self::Op, ret: &Self::Ret) -> bool {
        match ret {
            BucketRet::Error(_) => true, // Errors are always valid
            _ => {
                let expected = self.invoke(op);
                ret == &expected
            }
        }
    }
}

/// Records bucket operation history for linearizability checking.
#[derive(Clone)]
pub struct BucketHistoryRecorder {
    inner: Arc<Mutex<LinearizabilityTester<ThreadId, BucketSpec>>>,
}

impl Default for BucketHistoryRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl BucketHistoryRecorder {
    /// Create a new bucket history recorder.
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(LinearizabilityTester::new(BucketSpec::default()))) }
    }

    /// Record that a thread has invoked an operation.
    pub fn invoke(&self, thread_id: ThreadId, op: BucketOp) -> Result<(), String> {
        self.inner.lock().on_invoke(thread_id, op)?;
        Ok(())
    }

    /// Record that a thread's operation has returned.
    pub fn return_value(&self, thread_id: ThreadId, ret: BucketRet) -> Result<(), String> {
        self.inner.lock().on_return(thread_id, ret)?;
        Ok(())
    }

    /// Check if the recorded history is linearizable.
    pub fn check_linearizable(&self) -> Result<Vec<(BucketOp, BucketRet)>, LinearizabilityError> {
        let tester = self.inner.lock();
        tester.serialized_history().ok_or(LinearizabilityError::NotLinearizable)
    }

    /// Get the number of operations recorded.
    pub fn len(&self) -> usize {
        self.inner.lock().len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linearizable_sequential_history() {
        let recorder = HistoryRecorder::new();

        // Sequential operations should always be linearizable
        recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v1".into() }).unwrap();
        recorder.return_value(1, KvRet::PutOk).unwrap();

        recorder.invoke(1, KvOp::Get { key: "k".into() }).unwrap();
        recorder.return_value(1, KvRet::GetOk(Some("v1".into()))).unwrap();

        recorder.invoke(1, KvOp::Delete { key: "k".into() }).unwrap();
        recorder.return_value(1, KvRet::DeleteOk).unwrap();

        recorder.invoke(1, KvOp::Get { key: "k".into() }).unwrap();
        recorder.return_value(1, KvRet::GetOk(None)).unwrap();

        assert!(recorder.check_linearizable().is_ok());
    }

    #[test]
    fn test_linearizable_concurrent_history() {
        let recorder = HistoryRecorder::new();

        // Thread 1 starts a put
        recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v1".into() }).unwrap();

        // Thread 2 does a concurrent get that sees the value
        recorder.invoke(2, KvOp::Get { key: "k".into() }).unwrap();
        recorder.return_value(2, KvRet::GetOk(Some("v1".into()))).unwrap();

        // Thread 1's put completes
        recorder.return_value(1, KvRet::PutOk).unwrap();

        // This is linearizable: put -> get
        assert!(recorder.check_linearizable().is_ok());
    }

    #[test]
    fn test_linearizable_concurrent_get_before_put() {
        let recorder = HistoryRecorder::new();

        // Thread 1 starts a put
        recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v1".into() }).unwrap();

        // Thread 2 does a concurrent get that doesn't see the value yet
        recorder.invoke(2, KvOp::Get { key: "k".into() }).unwrap();
        recorder.return_value(2, KvRet::GetOk(None)).unwrap();

        // Thread 1's put completes
        recorder.return_value(1, KvRet::PutOk).unwrap();

        // This is linearizable: get -> put
        assert!(recorder.check_linearizable().is_ok());
    }

    #[test]
    fn test_unlinearizable_history() {
        let recorder = HistoryRecorder::new();

        // Put completes before get starts
        recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v1".into() }).unwrap();
        recorder.return_value(1, KvRet::PutOk).unwrap();

        // But get returns None - this is NOT linearizable
        recorder.invoke(2, KvOp::Get { key: "k".into() }).unwrap();
        recorder.return_value(2, KvRet::GetOk(None)).unwrap();

        assert_eq!(recorder.check_linearizable(), Err(LinearizabilityError::NotLinearizable));
    }

    #[test]
    fn test_cas_linearizable() {
        let recorder = HistoryRecorder::new();

        // Put initial value
        recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v1".into() }).unwrap();
        recorder.return_value(1, KvRet::PutOk).unwrap();

        // CAS succeeds
        recorder
            .invoke(
                2,
                KvOp::Cas { key: "k".into(), expected: Some("v1".into()), new_value: "v2".into() },
            )
            .unwrap();
        recorder.return_value(2, KvRet::CasOk(true)).unwrap();

        // Read shows new value
        recorder.invoke(3, KvOp::Get { key: "k".into() }).unwrap();
        recorder.return_value(3, KvRet::GetOk(Some("v2".into()))).unwrap();

        assert!(recorder.check_linearizable().is_ok());
    }

    #[test]
    fn test_concurrent_cas_one_wins() {
        let recorder = HistoryRecorder::new();

        // Put initial value
        recorder.invoke(1, KvOp::Put { key: "k".into(), value: "v1".into() }).unwrap();
        recorder.return_value(1, KvRet::PutOk).unwrap();

        // Two concurrent CAS operations
        recorder
            .invoke(
                2,
                KvOp::Cas { key: "k".into(), expected: Some("v1".into()), new_value: "v2".into() },
            )
            .unwrap();
        recorder
            .invoke(
                3,
                KvOp::Cas { key: "k".into(), expected: Some("v1".into()), new_value: "v3".into() },
            )
            .unwrap();

        // One wins, one loses
        recorder.return_value(2, KvRet::CasOk(true)).unwrap();
        recorder.return_value(3, KvRet::CasOk(false)).unwrap();

        assert!(recorder.check_linearizable().is_ok());
    }

    #[test]
    fn test_bucket_linearizable() {
        let recorder = BucketHistoryRecorder::new();

        // Create bucket
        recorder.invoke(1, BucketOp::CreateBucket { name: "test".into() }).unwrap();
        recorder.return_value(1, BucketRet::CreateOk).unwrap();

        // Check exists
        recorder.invoke(2, BucketOp::BucketExists { name: "test".into() }).unwrap();
        recorder.return_value(2, BucketRet::ExistsOk(true)).unwrap();

        // List buckets
        recorder.invoke(3, BucketOp::ListBuckets).unwrap();
        recorder.return_value(3, BucketRet::ListOk(vec!["test".into()])).unwrap();

        // Delete bucket
        recorder.invoke(4, BucketOp::DeleteBucket { name: "test".into() }).unwrap();
        recorder.return_value(4, BucketRet::DeleteOk).unwrap();

        assert!(recorder.check_linearizable().is_ok());
    }

    #[test]
    fn test_bucket_create_conflict() {
        let recorder = BucketHistoryRecorder::new();

        // Two concurrent creates
        recorder.invoke(1, BucketOp::CreateBucket { name: "test".into() }).unwrap();
        recorder.invoke(2, BucketOp::CreateBucket { name: "test".into() }).unwrap();

        // One succeeds, one fails
        recorder.return_value(1, BucketRet::CreateOk).unwrap();
        recorder.return_value(2, BucketRet::CreateExists).unwrap();

        assert!(recorder.check_linearizable().is_ok());
    }

    #[test]
    fn test_bucket_unlinearizable() {
        let recorder = BucketHistoryRecorder::new();

        // Create completes
        recorder.invoke(1, BucketOp::CreateBucket { name: "test".into() }).unwrap();
        recorder.return_value(1, BucketRet::CreateOk).unwrap();

        // But exists returns false after create completed - NOT linearizable
        recorder.invoke(2, BucketOp::BucketExists { name: "test".into() }).unwrap();
        recorder.return_value(2, BucketRet::ExistsOk(false)).unwrap();

        assert_eq!(recorder.check_linearizable(), Err(LinearizabilityError::NotLinearizable));
    }
}
