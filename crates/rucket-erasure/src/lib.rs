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
//! - **Storage overhead**: 1.5x (12 shards for 8 shards worth of data)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_encode_decode() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"Hello, World! This is test data for erasure coding.";

        let shards = codec.encode(data).unwrap();
        assert_eq!(shards.len(), 12); // 8 data + 4 parity

        // All shards present - should decode fine
        let recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_recover_with_missing_shards() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"Test data that will survive shard loss!";

        let shards = codec.encode(data).unwrap();

        // Lose 4 shards (maximum recoverable)
        let mut recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        recoverable[0] = None;
        recoverable[3] = None;
        recoverable[7] = None;
        recoverable[10] = None;

        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_too_many_missing_shards_fails() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"This data will be unrecoverable with too many losses.";

        let shards = codec.encode(data).unwrap();

        // Lose 5 shards (one more than we can handle)
        let mut recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        recoverable[0] = None;
        recoverable[1] = None;
        recoverable[2] = None;
        recoverable[3] = None;
        recoverable[4] = None;

        let result = codec.decode(recoverable);
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_data() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"";

        let shards = codec.encode(data).unwrap();
        assert_eq!(shards.len(), 12);

        let recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_small_data() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"X"; // Single byte

        let shards = codec.encode(data).unwrap();
        assert_eq!(shards.len(), 12);

        let recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_large_data() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        // 1MB of data
        let data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

        let shards = codec.encode(&data).unwrap();
        assert_eq!(shards.len(), 12);

        // Lose some shards
        let mut recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        recoverable[2] = None;
        recoverable[9] = None;

        let recovered = codec.decode(recoverable).unwrap();
        assert_eq!(recovered, data);
    }

    #[test]
    fn test_custom_config() {
        // 4+2 configuration (smaller overhead, less fault tolerance)
        let config = ErasureConfig::new(4, 2).unwrap();
        let codec = ErasureCodec::new(config).unwrap();
        let data = b"Custom configuration test data.";

        let shards = codec.encode(data).unwrap();
        assert_eq!(shards.len(), 6); // 4 data + 2 parity

        // Can lose up to 2 shards
        let mut recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        recoverable[0] = None;
        recoverable[4] = None;

        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_shard_info() {
        let config = ErasureConfig::default();
        assert_eq!(config.data_shards(), 8);
        assert_eq!(config.parity_shards(), 4);
        assert_eq!(config.total_shards(), 12);
        assert_eq!(config.fault_tolerance(), 4);
    }

    #[test]
    fn test_invalid_config() {
        // Zero data shards
        assert!(ErasureConfig::new(0, 4).is_err());
        // Zero parity shards
        assert!(ErasureConfig::new(8, 0).is_err());
        // Too many shards (> 256)
        assert!(ErasureConfig::new(200, 100).is_err());
    }
}
