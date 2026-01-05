//! Reed-Solomon erasure coding for Rucket distributed storage.
//!
//! This crate provides erasure coding functionality using Reed-Solomon codes,
//! enabling storage-efficient durability by splitting data into data shards
//! and parity shards.
//!
//! # Default Configuration: 8+4
//!
//! The default configuration uses 8 data shards and 4 parity shards:
//! - **8 data shards**: The original data is split into 8 equal-sized pieces
//! - **4 parity shards**: 4 redundancy shards are computed from the data
//! - **Fault tolerance**: Can recover from up to 4 shard losses
//! - **Storage overhead**: 1.5x (50% overhead: 12 shards for 8 shards worth of data)
//!
//! # Example
//!
//! ```
//! use rucket_erasure::{ErasureCodec, ErasureConfig};
//!
//! // Create a codec with default 8+4 configuration
//! let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
//!
//! // Encode data into shards
//! let data = b"Hello, World! This is some data to encode.";
//! let shards = codec.encode(data).unwrap();
//!
//! // We now have 12 shards (8 data + 4 parity)
//! assert_eq!(shards.len(), 12);
//!
//! // Simulate losing some shards (up to 4)
//! let mut recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
//! recoverable[0] = None;  // Lost shard 0
//! recoverable[5] = None;  // Lost shard 5
//!
//! // Reconstruct the original data (need to pass original size to trim padding)
//! let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
//! assert_eq!(&recovered, data);
//! ```

mod codec;
mod config;
mod error;
mod shard;

pub use codec::ErasureCodec;
pub use config::ErasureConfig;
pub use error::{ErasureError, Result};
pub use shard::{Shard, ShardId, ShardSet, ShardType};
