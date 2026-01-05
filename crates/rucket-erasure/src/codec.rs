//! Reed-Solomon erasure codec implementation.

use reed_solomon_erasure::galois_8::ReedSolomon;
use tracing::{debug, trace};

use crate::config::ErasureConfig;
use crate::error::{ErasureError, Result};

/// Reed-Solomon erasure codec for encoding and decoding data.
///
/// The codec splits data into data shards and computes parity shards.
/// It can reconstruct the original data from any subset of shards
/// as long as at least `data_shards` shards are available.
#[derive(Debug)]
pub struct ErasureCodec {
    /// Configuration for this codec.
    config: ErasureConfig,
    /// The Reed-Solomon encoder/decoder.
    rs: ReedSolomon,
}

impl ErasureCodec {
    /// Creates a new erasure codec with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the Reed-Solomon encoder cannot be created.
    pub fn new(config: ErasureConfig) -> Result<Self> {
        let rs = ReedSolomon::new(config.data_shards(), config.parity_shards())
            .map_err(|e| ErasureError::InvalidConfig(format!("Failed to create RS codec: {e}")))?;

        Ok(Self { config, rs })
    }

    /// Returns the configuration for this codec.
    #[must_use]
    pub const fn config(&self) -> ErasureConfig {
        self.config
    }

    /// Encodes data into shards.
    ///
    /// Returns a vector of shards where:
    /// - Indices 0 to data_shards-1 contain data shards
    /// - Indices data_shards to total_shards-1 contain parity shards
    ///
    /// # Example
    ///
    /// ```
    /// use rucket_erasure::{ErasureCodec, ErasureConfig};
    ///
    /// let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
    /// let data = b"Hello, World!";
    /// let shards = codec.encode(data).unwrap();
    /// assert_eq!(shards.len(), 12); // 8 data + 4 parity
    /// ```
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        let data_shards = self.config.data_shards();
        let parity_shards = self.config.parity_shards();
        let total_shards = self.config.total_shards();

        // Calculate shard size (round up to ensure all data fits)
        let shard_size = if data.is_empty() {
            1 // Minimum shard size of 1 byte
        } else {
            data.len().div_ceil(data_shards)
        };

        debug!(
            data_len = data.len(),
            shard_size, data_shards, parity_shards, "Encoding data into shards"
        );

        // Create data shards with padding
        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(total_shards);

        for i in 0..data_shards {
            let start = i * shard_size;
            let end = std::cmp::min(start + shard_size, data.len());

            let mut shard = if start < data.len() { data[start..end].to_vec() } else { Vec::new() };

            // Pad shard to shard_size
            shard.resize(shard_size, 0);
            shards.push(shard);
        }

        // Create empty parity shards
        for _ in 0..parity_shards {
            shards.push(vec![0u8; shard_size]);
        }

        // Encode parity shards
        self.rs.encode(&mut shards).map_err(|e| {
            ErasureError::EncodingFailed(format!("Reed-Solomon encoding failed: {e}"))
        })?;

        trace!(shard_count = shards.len(), shard_size, "Encoding complete");

        Ok(shards)
    }

    /// Decodes shards back to the original data.
    ///
    /// Takes a vector of optional shards. `None` indicates a missing shard.
    /// Returns the original data if enough shards are present.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Not enough shards are present (need at least `data_shards`)
    /// - Shard sizes don't match
    /// - Reconstruction fails
    ///
    /// # Example
    ///
    /// ```
    /// use rucket_erasure::{ErasureCodec, ErasureConfig};
    ///
    /// let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
    /// let data = b"Hello, World!";
    /// let shards = codec.encode(data).unwrap();
    ///
    /// // Simulate losing some shards
    /// let mut partial: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
    /// partial[0] = None;
    /// partial[5] = None;
    ///
    /// // Use decode_with_size to get exact original data
    /// let recovered = codec.decode_with_size(partial, data.len()).unwrap();
    /// assert_eq!(&recovered, data);
    /// ```
    pub fn decode(&self, shards: Vec<Option<Vec<u8>>>) -> Result<Vec<u8>> {
        let data_shards = self.config.data_shards();
        let total_shards = self.config.total_shards();

        // Validate shard count
        if shards.len() != total_shards {
            return Err(ErasureError::InvalidShardCount {
                expected: total_shards,
                actual: shards.len(),
            });
        }

        // Count available shards and find shard size
        let available = shards.iter().filter(|s| s.is_some()).count();
        if available < data_shards {
            return Err(ErasureError::TooManyMissing { available, required: data_shards });
        }

        // Determine shard size from first available shard
        let shard_size = shards
            .iter()
            .find_map(|s| s.as_ref().map(|v| v.len()))
            .ok_or_else(|| ErasureError::DecodingFailed("No shards available".to_string()))?;

        debug!(available, shard_size, data_shards, "Decoding shards");

        // Validate all shard sizes match
        for (i, shard) in shards.iter().enumerate() {
            if let Some(s) = shard {
                if s.len() != shard_size {
                    return Err(ErasureError::ShardSizeMismatch {
                        expected: shard_size,
                        actual: s.len(),
                    });
                }
            }
            trace!(shard_index = i, present = shard.is_some(), "Shard status");
        }

        // Convert to format expected by reed-solomon-erasure
        let mut shard_refs: Vec<Option<Vec<u8>>> = shards;

        // Reconstruct missing shards
        self.rs.reconstruct(&mut shard_refs).map_err(|e| {
            ErasureError::DecodingFailed(format!("Reed-Solomon reconstruction failed: {e}"))
        })?;

        // Combine data shards to get original data
        let mut result = Vec::with_capacity(data_shards * shard_size);
        for shard in shard_refs.into_iter().take(data_shards) {
            if let Some(data) = shard {
                result.extend_from_slice(&data);
            } else {
                return Err(ErasureError::DecodingFailed(
                    "Reconstruction failed: data shard still missing".to_string(),
                ));
            }
        }

        // Note: The result may contain padding zeros. The caller should use
        // decode_with_size() to get the exact original data, or know the expected size.

        trace!(result_len = result.len(), "Decoding complete");

        Ok(result)
    }

    /// Decodes shards and trims to the original data size.
    ///
    /// This variant takes the original data size as a parameter to correctly
    /// remove any padding that was added during encoding.
    pub fn decode_with_size(
        &self,
        shards: Vec<Option<Vec<u8>>>,
        original_size: usize,
    ) -> Result<Vec<u8>> {
        let mut result = self.decode(shards)?;
        result.truncate(original_size);
        Ok(result)
    }

    /// Checks if a set of shards can be successfully reconstructed.
    ///
    /// Returns `true` if at least `data_shards` shards are available.
    #[must_use]
    pub fn can_reconstruct(&self, shards: &[Option<Vec<u8>>]) -> bool {
        let available = shards.iter().filter(|s| s.is_some()).count();
        available >= self.config.data_shards()
    }

    /// Returns the indices of missing shards.
    #[must_use]
    pub fn missing_indices(&self, shards: &[Option<Vec<u8>>]) -> Vec<usize> {
        shards.iter().enumerate().filter(|(_, s)| s.is_none()).map(|(i, _)| i).collect()
    }
}

impl Clone for ErasureCodec {
    fn clone(&self) -> Self {
        // ReedSolomon doesn't implement Clone, so we create a new one
        Self::new(self.config).expect("Config was valid during construction")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_roundtrip() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"The quick brown fox jumps over the lazy dog.";

        let shards = codec.encode(data).unwrap();
        let recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();

        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_recovery_with_exact_data_shards() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"Recovery test with minimum shards.";

        let shards = codec.encode(data).unwrap();
        let mut recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();

        // Remove all parity shards (indices 8-11)
        for shard in recoverable.iter_mut().skip(8) {
            *shard = None;
        }

        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_recovery_with_mixed_missing() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"Mixed shard loss recovery test.";

        let shards = codec.encode(data).unwrap();
        let mut recoverable: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();

        // Remove 2 data shards and 2 parity shards
        recoverable[1] = None;
        recoverable[3] = None;
        recoverable[8] = None;
        recoverable[10] = None;

        let recovered = codec.decode_with_size(recoverable, data.len()).unwrap();
        assert_eq!(&recovered, data);
    }

    #[test]
    fn test_can_reconstruct() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let data = b"Test data";
        let shards = codec.encode(data).unwrap();

        // All present
        let all: Vec<Option<Vec<u8>>> = shards.clone().into_iter().map(Some).collect();
        assert!(codec.can_reconstruct(&all));

        // Exactly 8 present
        let mut eight: Vec<Option<Vec<u8>>> = shards.clone().into_iter().map(Some).collect();
        for shard in eight.iter_mut().take(4) {
            *shard = None;
        }
        assert!(codec.can_reconstruct(&eight));

        // Only 7 present
        let mut seven: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        for shard in seven.iter_mut().take(5) {
            *shard = None;
        }
        assert!(!codec.can_reconstruct(&seven));
    }

    #[test]
    fn test_missing_indices() {
        let codec = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let shards: Vec<Option<Vec<u8>>> = vec![
            Some(vec![1]),
            None,
            Some(vec![3]),
            None,
            Some(vec![5]),
            None,
            Some(vec![7]),
            Some(vec![8]),
            Some(vec![9]),
            None,
            Some(vec![11]),
            Some(vec![12]),
        ];

        let missing = codec.missing_indices(&shards);
        assert_eq!(missing, vec![1, 3, 5, 9]);
    }

    #[test]
    fn test_clone() {
        let codec1 = ErasureCodec::new(ErasureConfig::default()).unwrap();
        let codec2 = codec1.clone();

        assert_eq!(codec1.config(), codec2.config());

        // Both should encode identically
        let data = b"Clone test";
        let shards1 = codec1.encode(data).unwrap();
        let shards2 = codec2.encode(data).unwrap();
        assert_eq!(shards1, shards2);
    }
}
