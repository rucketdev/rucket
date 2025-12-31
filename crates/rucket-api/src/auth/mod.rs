//! AWS authentication support.

pub mod sigv4;

pub use sigv4::{SigV4Validator, ValidationError};
