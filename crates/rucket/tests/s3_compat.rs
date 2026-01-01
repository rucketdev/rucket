//! S3 Compatibility Test Suite
//!
//! This test suite provides comprehensive coverage of S3 API compatibility,
//! ported from MinIO Mint (~350 tests) and Ceph S3-Tests (~829 tests).
//!
//! Tests are organized by category:
//! - bucket: Bucket operations (create, delete, list, head, versioning, etc.)
//! - object: Object operations (put, get, head, delete, copy, metadata)
//! - list: List operations (v1, v2, versions, delimiter handling)
//! - multipart: Multipart upload operations
//! - versioning: Object versioning operations
//! - conditional: Conditional requests (If-Match, If-None-Match, etc.)
//! - range: Range request operations
//! - presigned: Presigned URL operations
//! - delete_multiple: Batch delete operations
//! - atomic: Concurrent/atomic operations
//! - encryption: Server-side encryption (mostly #[ignore])
//! - access_control: ACL and policy operations (mostly #[ignore])
//! - post_object: POST object operations
//!
//! Run tests:
//! ```bash
//! # All S3 compat tests
//! cargo test --test s3_compat
//!
//! # Specific category
//! cargo test --test s3_compat bucket::
//!
//! # Including ignored (unimplemented features)
//! cargo test --test s3_compat -- --ignored
//! ```

// Test harness module
#[path = "s3_compat/harness.rs"]
mod harness;
pub use harness::*;

// Test modules
#[path = "s3_compat/bucket/mod.rs"]
mod bucket;
#[path = "s3_compat/object/mod.rs"]
mod object;
#[path = "s3_compat/list/mod.rs"]
mod list;
#[path = "s3_compat/multipart/mod.rs"]
mod multipart;
#[path = "s3_compat/versioning/mod.rs"]
mod versioning;
#[path = "s3_compat/conditional/mod.rs"]
mod conditional;
#[path = "s3_compat/range/mod.rs"]
mod range;
#[path = "s3_compat/presigned/mod.rs"]
mod presigned;
#[path = "s3_compat/delete_multiple/mod.rs"]
mod delete_multiple;
#[path = "s3_compat/atomic/mod.rs"]
mod atomic;
#[path = "s3_compat/encryption/mod.rs"]
mod encryption;
#[path = "s3_compat/access_control/mod.rs"]
mod access_control;
#[path = "s3_compat/post_object/mod.rs"]
mod post_object;
