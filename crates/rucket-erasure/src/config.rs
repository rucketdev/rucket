//! Configuration for erasure coding.

use crate::error::{ErasureError, Result};

/// Configuration for Reed-Solomon erasure coding.
///
/// The configuration specifies the number of data shards and parity shards.
/// Data is split into `data_shards` pieces, and `parity_shards` redundancy
/// shards are computed. The total number of shards is `data_shards + parity_shards`.
///
/// # Fault Tolerance
///
/// The codec can recover from losing up to `parity_shards` shards.
/// As long as at least `data_shards` shards are available (any combination
/// of data and parity shards), the original data can be reconstructed.
///
/// # Storage Overhead
///
/// Storage overhead = total_shards / data_shards
/// For 8+4: overhead = 12/8 = 1.5x
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ErasureConfig {
    /// Number of data shards.
    data_shards: usize,
    /// Number of parity (redundancy) shards.
    parity_shards: usize,
}

impl ErasureConfig {
    /// Creates a new erasure configuration.
    ///
    /// # Arguments
    ///
    /// * `data_shards` - Number of data shards (1-255)
    /// * `parity_shards` - Number of parity shards (1-255)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - Either count is zero
    /// - Total shards exceeds 256 (Reed-Solomon limitation)
    ///
    /// # Example
    ///
    /// ```
    /// use rucket_erasure::ErasureConfig;
    ///
    /// // 8 data shards + 4 parity shards
    /// let config = ErasureConfig::new(8, 4).unwrap();
    /// assert_eq!(config.data_shards(), 8);
    /// assert_eq!(config.parity_shards(), 4);
    /// ```
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self> {
        if data_shards == 0 {
            return Err(ErasureError::InvalidConfig("data_shards must be at least 1".to_string()));
        }
        if parity_shards == 0 {
            return Err(ErasureError::InvalidConfig(
                "parity_shards must be at least 1".to_string(),
            ));
        }
        if data_shards + parity_shards > 256 {
            return Err(ErasureError::InvalidConfig("total shards cannot exceed 256".to_string()));
        }

        Ok(Self { data_shards, parity_shards })
    }

    /// Returns the number of data shards.
    #[must_use]
    pub const fn data_shards(&self) -> usize {
        self.data_shards
    }

    /// Returns the number of parity shards.
    #[must_use]
    pub const fn parity_shards(&self) -> usize {
        self.parity_shards
    }

    /// Returns the total number of shards (data + parity).
    #[must_use]
    pub const fn total_shards(&self) -> usize {
        self.data_shards + self.parity_shards
    }

    /// Returns the fault tolerance (number of shards that can be lost).
    #[must_use]
    pub const fn fault_tolerance(&self) -> usize {
        self.parity_shards
    }

    /// Returns the storage overhead ratio.
    ///
    /// This is total_shards / data_shards. For 8+4 config, this is 1.5.
    #[must_use]
    pub fn storage_overhead(&self) -> f64 {
        self.total_shards() as f64 / self.data_shards as f64
    }

    /// Creates a configuration for small objects (4+2).
    ///
    /// Suitable for objects < 64KB where lower shard count reduces overhead.
    /// Fault tolerance: 2 shards, overhead: 1.5x
    #[must_use]
    pub fn small() -> Self {
        Self { data_shards: 4, parity_shards: 2 }
    }

    /// Creates a configuration for large objects (16+4).
    ///
    /// Suitable for large objects where more shards provide better parallelism.
    /// Fault tolerance: 4 shards, overhead: 1.25x
    #[must_use]
    pub fn large() -> Self {
        Self { data_shards: 16, parity_shards: 4 }
    }
}

impl Default for ErasureConfig {
    /// Returns the default 8+4 configuration.
    ///
    /// - 8 data shards
    /// - 4 parity shards
    /// - Fault tolerance: 4 shards
    /// - Storage overhead: 1.5x
    fn default() -> Self {
        Self { data_shards: 8, parity_shards: 4 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ErasureConfig::default();
        assert_eq!(config.data_shards(), 8);
        assert_eq!(config.parity_shards(), 4);
        assert_eq!(config.total_shards(), 12);
        assert_eq!(config.fault_tolerance(), 4);
        assert!((config.storage_overhead() - 1.5).abs() < 0.001);
    }

    #[test]
    fn test_custom_config() {
        let config = ErasureConfig::new(10, 5).unwrap();
        assert_eq!(config.data_shards(), 10);
        assert_eq!(config.parity_shards(), 5);
        assert_eq!(config.total_shards(), 15);
        assert_eq!(config.fault_tolerance(), 5);
    }

    #[test]
    fn test_small_config() {
        let config = ErasureConfig::small();
        assert_eq!(config.data_shards(), 4);
        assert_eq!(config.parity_shards(), 2);
    }

    #[test]
    fn test_large_config() {
        let config = ErasureConfig::large();
        assert_eq!(config.data_shards(), 16);
        assert_eq!(config.parity_shards(), 4);
    }

    #[test]
    fn test_invalid_zero_data() {
        assert!(ErasureConfig::new(0, 4).is_err());
    }

    #[test]
    fn test_invalid_zero_parity() {
        assert!(ErasureConfig::new(8, 0).is_err());
    }

    #[test]
    fn test_invalid_too_many_shards() {
        assert!(ErasureConfig::new(200, 100).is_err());
    }

    #[test]
    fn test_max_shards() {
        // 256 total is the maximum
        let config = ErasureConfig::new(200, 56).unwrap();
        assert_eq!(config.total_shards(), 256);
    }
}
