//! S3 API implementation for Rucket object storage.
//!
//! This crate provides the HTTP API layer including:
//! - S3-compatible REST API handlers
//! - AWS Signature V4 authentication
//! - XML request/response parsing

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod auth;
pub mod error;
pub mod handlers;
pub mod metrics;
pub mod middleware;
pub mod policy;
pub mod router;
pub mod xml;

pub use error::ApiError;
pub use handlers::bucket::AppState;
pub use metrics::init_metrics;
pub use router::create_router;
