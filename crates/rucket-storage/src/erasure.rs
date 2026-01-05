//! Erasure coding integration for distributed storage.
//!
//! This module provides erasure-coded storage that splits large objects
//! into data and parity shards distributed across nodes for fault tolerance.
//!
//! # Configuration
//!
//! Objects larger than `ec_threshold` (default 1MB) are automatically
//! encoded using Reed-Solomon erasure coding. Smaller objects use
//! standard replication.
//!
//! # Default Configuration: 8+4
//!
//! - 8 data shards + 4 parity shards
//! - Can tolerate up to 4 shard losses
//! - Storage overhead: 50% (1.5x)

use std::collections::HashMap;

use rucket_erasure::{ErasureCodec, ErasureConfig as RsConfig, ErasureError};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, trace};
use uuid::Uuid;

use crate::placement::{NodeId, PlacementPolicy};

/// Default threshold for using erasure coding (1MB).
pub const DEFAULT_EC_THRESHOLD: u64 = 1024 * 1024;

/// Configuration for erasure-coded storage.
#[derive(Debug, Clone)]
pub struct ErasureStorageConfig {
    /// Minimum object size to use erasure coding (bytes).
    /// Objects smaller than this use replication instead.
    pub threshold: u64,

    /// Whether erasure coding is enabled.
    pub enabled: bool,

    /// Reed-Solomon configuration (data shards, parity shards).
    pub rs_config: RsConfig,
}

impl Default for ErasureStorageConfig {
    fn default() -> Self {
        Self {
            threshold: DEFAULT_EC_THRESHOLD,
            enabled: true,
            rs_config: RsConfig::default(), // 8+4
        }
    }
}

impl ErasureStorageConfig {
    /// Create a new configuration with the specified threshold.
    pub fn new(threshold: u64) -> Self {
        Self { threshold, ..Default::default() }
    }

    /// Create a disabled erasure coding configuration.
    pub fn disabled() -> Self {
        Self { enabled: false, ..Default::default() }
    }

    /// Set the Reed-Solomon configuration.
    pub fn with_rs_config(mut self, config: RsConfig) -> Self {
        self.rs_config = config;
        self
    }

    /// Check if an object of the given size should use erasure coding.
    pub fn should_use_ec(&self, size: u64) -> bool {
        self.enabled && size >= self.threshold
    }

    /// Get the number of data shards.
    pub fn data_shards(&self) -> usize {
        self.rs_config.data_shards()
    }

    /// Get the number of parity shards.
    pub fn parity_shards(&self) -> usize {
        self.rs_config.parity_shards()
    }

    /// Get the total number of shards.
    pub fn total_shards(&self) -> usize {
        self.rs_config.total_shards()
    }
}

/// Location information for a stored shard.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardLocation {
    /// Shard index (0 to total_shards-1).
    pub shard_index: u8,

    /// UUID of the stored shard data file.
    pub shard_uuid: Uuid,

    /// Node where this shard is stored.
    pub node_id: String,

    /// Network address of the node.
    pub node_address: String,

    /// Size of this shard in bytes.
    pub size: u64,

    /// CRC32C checksum of the shard data.
    pub checksum: u32,
}

impl ShardLocation {
    /// Create a new shard location.
    pub fn new(
        shard_index: u8,
        shard_uuid: Uuid,
        node_id: String,
        node_address: String,
        size: u64,
        checksum: u32,
    ) -> Self {
        Self { shard_index, shard_uuid, node_id, node_address, size, checksum }
    }
}

/// Metadata for an erasure-coded object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErasureObjectMetadata {
    /// Parent object UUID (links all shards together).
    pub object_uuid: Uuid,

    /// Original object size before encoding.
    pub original_size: u64,

    /// Number of data shards.
    pub data_shards: u8,

    /// Number of parity shards.
    pub parity_shards: u8,

    /// Size of each shard (after padding).
    pub shard_size: u64,

    /// Locations of all shards (indexed by shard index).
    pub shard_locations: Vec<ShardLocation>,

    /// ETag of the original object (MD5 of original data).
    pub etag: String,
}

impl ErasureObjectMetadata {
    /// Create new erasure object metadata.
    pub fn new(
        object_uuid: Uuid,
        original_size: u64,
        data_shards: u8,
        parity_shards: u8,
        shard_size: u64,
        etag: String,
    ) -> Self {
        Self {
            object_uuid,
            original_size,
            data_shards,
            parity_shards,
            shard_size,
            shard_locations: Vec::with_capacity((data_shards + parity_shards) as usize),
            etag,
        }
    }

    /// Add a shard location.
    pub fn add_shard(&mut self, location: ShardLocation) {
        self.shard_locations.push(location);
    }

    /// Get the total number of shards.
    pub fn total_shards(&self) -> usize {
        (self.data_shards + self.parity_shards) as usize
    }

    /// Check if we have enough shards to reconstruct the data.
    pub fn can_reconstruct(&self) -> bool {
        self.shard_locations.len() >= self.data_shards as usize
    }

    /// Get shards grouped by node for efficient retrieval.
    pub fn shards_by_node(&self) -> HashMap<String, Vec<&ShardLocation>> {
        let mut by_node: HashMap<String, Vec<&ShardLocation>> = HashMap::new();
        for shard in &self.shard_locations {
            by_node.entry(shard.node_id.clone()).or_default().push(shard);
        }
        by_node
    }
}

/// Errors that can occur during erasure storage operations.
#[derive(Debug, Error)]
pub enum ErasureStorageError {
    /// Erasure coding error.
    #[error("Erasure coding error: {0}")]
    Erasure(#[from] ErasureError),

    /// Not enough shards available for reconstruction.
    #[error("Insufficient shards: have {available}, need {required}")]
    InsufficientShards {
        /// Number of shards available.
        available: usize,
        /// Number of shards required for reconstruction.
        required: usize,
    },

    /// Shard checksum mismatch.
    #[error(
        "Checksum mismatch for shard {shard_index}: expected {expected:08x}, got {actual:08x}"
    )]
    ChecksumMismatch {
        /// Index of the shard with mismatched checksum.
        shard_index: u8,
        /// Expected checksum value.
        expected: u32,
        /// Actual checksum value.
        actual: u32,
    },

    /// Node communication error.
    #[error("Failed to communicate with node {node_id}: {message}")]
    NodeError {
        /// ID of the node that failed.
        node_id: String,
        /// Error message.
        message: String,
    },

    /// Storage I/O error.
    #[error("Storage I/O error: {0}")]
    Io(#[from] std::io::Error),
}

/// Result type for erasure storage operations.
pub type Result<T> = std::result::Result<T, ErasureStorageError>;

/// Encoder for splitting objects into erasure-coded shards.
pub struct ShardEncoder {
    codec: ErasureCodec,
    config: ErasureStorageConfig,
}

impl ShardEncoder {
    /// Create a new shard encoder with the given configuration.
    pub fn new(config: ErasureStorageConfig) -> Result<Self> {
        let codec = ErasureCodec::new(config.rs_config)?;
        Ok(Self { codec, config })
    }

    /// Encode data into shards.
    ///
    /// Returns a vector of shard data, indexed 0 to total_shards-1.
    /// Shards 0..data_shards are data shards, data_shards..total are parity.
    pub fn encode(&self, data: &[u8]) -> Result<Vec<Vec<u8>>> {
        debug!(
            data_len = data.len(),
            data_shards = self.config.data_shards(),
            parity_shards = self.config.parity_shards(),
            "Encoding data into shards"
        );

        let shards = self.codec.encode(data)?;

        trace!(
            shard_count = shards.len(),
            shard_size = shards.first().map(|s| s.len()).unwrap_or(0),
            "Encoding complete"
        );

        Ok(shards)
    }

    /// Get the shard size for data of the given length.
    pub fn shard_size(&self, data_len: usize) -> usize {
        if data_len == 0 {
            1
        } else {
            data_len.div_ceil(self.config.data_shards())
        }
    }

    /// Check if the given size should use erasure coding.
    pub fn should_encode(&self, size: u64) -> bool {
        self.config.should_use_ec(size)
    }
}

/// Decoder for reconstructing objects from erasure-coded shards.
pub struct ShardDecoder {
    codec: ErasureCodec,
    config: ErasureStorageConfig,
}

impl ShardDecoder {
    /// Create a new shard decoder with the given configuration.
    pub fn new(config: ErasureStorageConfig) -> Result<Self> {
        let codec = ErasureCodec::new(config.rs_config)?;
        Ok(Self { codec, config })
    }

    /// Decode shards back into the original data.
    ///
    /// # Arguments
    ///
    /// * `shards` - Vector of optional shard data (None for missing shards)
    /// * `original_size` - Original size of the data before encoding
    ///
    /// # Returns
    ///
    /// The reconstructed original data.
    pub fn decode(&self, shards: Vec<Option<Vec<u8>>>, original_size: usize) -> Result<Vec<u8>> {
        let available = shards.iter().filter(|s| s.is_some()).count();
        let required = self.config.data_shards();

        debug!(available, required, original_size, "Decoding shards back to data");

        if available < required {
            return Err(ErasureStorageError::InsufficientShards { available, required });
        }

        let data = self.codec.decode_with_size(shards, original_size)?;

        trace!(decoded_len = data.len(), "Decoding complete");

        Ok(data)
    }

    /// Check if we can reconstruct from the given shards.
    pub fn can_reconstruct(&self, shards: &[Option<Vec<u8>>]) -> bool {
        self.codec.can_reconstruct(shards)
    }
}

/// Shard placement calculator for distributed storage.
pub struct ShardPlacement<'a> {
    policy: &'a dyn PlacementPolicy,
    config: &'a ErasureStorageConfig,
}

impl<'a> ShardPlacement<'a> {
    /// Create a new shard placement calculator.
    pub fn new(policy: &'a dyn PlacementPolicy, config: &'a ErasureStorageConfig) -> Self {
        Self { policy, config }
    }

    /// Calculate placement for all shards of an object.
    ///
    /// Returns a vector of (shard_index, node) pairs indicating where
    /// each shard should be stored.
    pub fn calculate_placements(&self, bucket: &str, key: &str) -> Vec<(u8, NodeId)> {
        let total_shards = self.config.total_shards();
        let mut placements = Vec::with_capacity(total_shards);

        // For each shard, compute a consistent placement
        // We use the shard index to vary the key, ensuring deterministic
        // but distributed placement across failure domains
        for shard_index in 0..total_shards {
            let shard_key = format!("{key}#shard{shard_index}");
            let placement = self.policy.compute_placement(bucket, &shard_key);

            // Use primary for this shard's placement
            placements.push((shard_index as u8, placement.primary_node));
        }

        // Log the distribution
        if tracing::enabled!(tracing::Level::DEBUG) {
            let mut node_counts: HashMap<String, usize> = HashMap::new();
            for (_, node) in &placements {
                *node_counts.entry(node.id.clone()).or_default() += 1;
            }
            debug!(bucket, key, total_shards, ?node_counts, "Calculated shard placements");
        }

        placements
    }

    /// Get the expected nodes for a placement group.
    pub fn get_nodes_for_pg(&self, pg: u32) -> Vec<NodeId> {
        self.policy.get_nodes_for_pg(pg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_erasure_storage_config_default() {
        let config = ErasureStorageConfig::default();
        assert_eq!(config.threshold, DEFAULT_EC_THRESHOLD);
        assert!(config.enabled);
        assert_eq!(config.data_shards(), 8);
        assert_eq!(config.parity_shards(), 4);
    }

    #[test]
    fn test_should_use_ec() {
        let config = ErasureStorageConfig::default();

        // Below threshold - no EC
        assert!(!config.should_use_ec(1000));
        assert!(!config.should_use_ec(DEFAULT_EC_THRESHOLD - 1));

        // At or above threshold - use EC
        assert!(config.should_use_ec(DEFAULT_EC_THRESHOLD));
        assert!(config.should_use_ec(DEFAULT_EC_THRESHOLD + 1));
        assert!(config.should_use_ec(10 * DEFAULT_EC_THRESHOLD));
    }

    #[test]
    fn test_disabled_config() {
        let config = ErasureStorageConfig::disabled();
        assert!(!config.enabled);
        assert!(!config.should_use_ec(10 * DEFAULT_EC_THRESHOLD));
    }

    #[test]
    fn test_shard_encoder() {
        let config = ErasureStorageConfig::default();
        let encoder = ShardEncoder::new(config).unwrap();

        let data = vec![0u8; 1024 * 1024]; // 1MB
        let shards = encoder.encode(&data).unwrap();

        assert_eq!(shards.len(), 12); // 8+4
        assert!(shards.iter().all(|s| s.len() == shards[0].len()));
    }

    #[test]
    fn test_shard_decoder() {
        let config = ErasureStorageConfig::default();
        let encoder = ShardEncoder::new(config.clone()).unwrap();
        let decoder = ShardDecoder::new(config).unwrap();

        let original = b"Hello, erasure coding world! This is a test.".to_vec();
        let shards = encoder.encode(&original).unwrap();

        // All shards present
        let decoded =
            decoder.decode(shards.into_iter().map(Some).collect(), original.len()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_with_missing_shards() {
        let config = ErasureStorageConfig::default();
        let encoder = ShardEncoder::new(config.clone()).unwrap();
        let decoder = ShardDecoder::new(config).unwrap();

        let original = b"Test data for partial reconstruction".to_vec();
        let shards = encoder.encode(&original).unwrap();

        // Remove 4 shards (all parity)
        let mut partial: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        partial[8] = None;
        partial[9] = None;
        partial[10] = None;
        partial[11] = None;

        let decoded = decoder.decode(partial, original.len()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn test_decode_insufficient_shards() {
        let config = ErasureStorageConfig::default();
        let encoder = ShardEncoder::new(config.clone()).unwrap();
        let decoder = ShardDecoder::new(config).unwrap();

        let original = b"Test data".to_vec();
        let shards = encoder.encode(&original).unwrap();

        // Remove 5 shards (more than parity count)
        let mut partial: Vec<Option<Vec<u8>>> = shards.into_iter().map(Some).collect();
        for shard in partial.iter_mut().take(5) {
            *shard = None;
        }

        let result = decoder.decode(partial, original.len());
        assert!(matches!(result, Err(ErasureStorageError::InsufficientShards { .. })));
    }

    #[test]
    fn test_erasure_object_metadata() {
        let mut meta = ErasureObjectMetadata::new(
            Uuid::new_v4(),
            1024 * 1024,
            8,
            4,
            128 * 1024,
            "abc123".to_string(),
        );

        assert_eq!(meta.total_shards(), 12);
        assert!(!meta.can_reconstruct()); // No shards yet

        // Add 8 shards
        for i in 0..8 {
            meta.add_shard(ShardLocation::new(
                i,
                Uuid::new_v4(),
                format!("node-{}", i % 3),
                format!("192.168.1.{}:8080", i % 3),
                128 * 1024,
                0x12345678,
            ));
        }

        assert!(meta.can_reconstruct());

        let by_node = meta.shards_by_node();
        assert!(by_node.len() <= 3); // Distributed across up to 3 nodes
    }

    #[test]
    fn test_shard_location() {
        let loc = ShardLocation::new(
            0,
            Uuid::new_v4(),
            "node-1".to_string(),
            "192.168.1.1:8080".to_string(),
            65536,
            0xDEADBEEF,
        );

        assert_eq!(loc.shard_index, 0);
        assert_eq!(loc.node_id, "node-1");
        assert_eq!(loc.size, 65536);
        assert_eq!(loc.checksum, 0xDEADBEEF);
    }
}
