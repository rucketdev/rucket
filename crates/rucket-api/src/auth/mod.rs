// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! AWS authentication support.

pub mod sigv4;

pub use sigv4::{SigV4Validator, ValidationError};
