//! Server-side encryption tests.
//!
//! SSE-C (customer-provided keys) is fully implemented.
//! SSE-S3 (server-managed keys) is implemented.
//! SSE-KMS (AWS KMS keys) tests are marked #[ignore] as KMS is not implemented.

pub mod sse_c;
pub mod sse_kms;
pub mod sse_s3;
