//! Shard types and management for erasure coding.

use std::fmt;

/// Unique identifier for a shard within an encoded object.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ShardId {
    /// Index of the shard (0 to total_shards - 1).
    index: u8,
    /// Type of shard (data or parity).
    shard_type: ShardType,
}

impl ShardId {
    /// Creates a new shard ID.
    #[must_use]
    pub const fn new(index: u8, shard_type: ShardType) -> Self {
        Self { index, shard_type }
    }

    /// Creates a data shard ID.
    #[must_use]
    pub const fn data(index: u8) -> Self {
        Self { index, shard_type: ShardType::Data }
    }

    /// Creates a parity shard ID.
    #[must_use]
    pub const fn parity(index: u8) -> Self {
        Self { index, shard_type: ShardType::Parity }
    }

    /// Returns the shard index.
    #[must_use]
    pub const fn index(&self) -> u8 {
        self.index
    }

    /// Returns the shard type.
    #[must_use]
    pub const fn shard_type(&self) -> ShardType {
        self.shard_type
    }

    /// Returns true if this is a data shard.
    #[must_use]
    pub const fn is_data(&self) -> bool {
        matches!(self.shard_type, ShardType::Data)
    }

    /// Returns true if this is a parity shard.
    #[must_use]
    pub const fn is_parity(&self) -> bool {
        matches!(self.shard_type, ShardType::Parity)
    }
}

impl fmt::Display for ShardId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.shard_type, self.index)
    }
}

/// Type of shard in erasure coding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ShardType {
    /// Data shard containing original data.
    Data,
    /// Parity shard for redundancy.
    Parity,
}

impl fmt::Display for ShardType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ShardType::Data => write!(f, "D"),
            ShardType::Parity => write!(f, "P"),
        }
    }
}

/// A single shard with its data and metadata.
#[derive(Debug, Clone)]
pub struct Shard {
    /// Shard identifier.
    id: ShardId,
    /// Shard data.
    data: Vec<u8>,
}

impl Shard {
    /// Creates a new shard.
    #[must_use]
    pub fn new(id: ShardId, data: Vec<u8>) -> Self {
        Self { id, data }
    }

    /// Returns the shard ID.
    #[must_use]
    pub const fn id(&self) -> ShardId {
        self.id
    }

    /// Returns the shard data.
    #[must_use]
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Consumes the shard and returns the data.
    #[must_use]
    pub fn into_data(self) -> Vec<u8> {
        self.data
    }

    /// Returns the shard size in bytes.
    #[must_use]
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// A set of shards for an encoded object.
///
/// Some shards may be missing (None) if they were lost or haven't been fetched yet.
#[derive(Debug, Clone)]
pub struct ShardSet {
    /// Shards, indexed by shard number. None means the shard is missing.
    shards: Vec<Option<Vec<u8>>>,
    /// Number of data shards in the encoding.
    data_shards: usize,
    /// Number of parity shards in the encoding.
    parity_shards: usize,
}

impl ShardSet {
    /// Creates a new shard set with no shards present.
    #[must_use]
    pub fn new_empty(data_shards: usize, parity_shards: usize) -> Self {
        let total = data_shards + parity_shards;
        Self { shards: vec![None; total], data_shards, parity_shards }
    }

    /// Creates a shard set from a vector of optional shards.
    #[must_use]
    pub fn from_shards(
        shards: Vec<Option<Vec<u8>>>,
        data_shards: usize,
        parity_shards: usize,
    ) -> Self {
        Self { shards, data_shards, parity_shards }
    }

    /// Sets a shard at the given index.
    pub fn set_shard(&mut self, index: usize, data: Vec<u8>) {
        if index < self.shards.len() {
            self.shards[index] = Some(data);
        }
    }

    /// Gets a shard at the given index.
    #[must_use]
    pub fn get_shard(&self, index: usize) -> Option<&[u8]> {
        self.shards.get(index).and_then(|s| s.as_deref())
    }

    /// Returns the number of shards present.
    #[must_use]
    pub fn present_count(&self) -> usize {
        self.shards.iter().filter(|s| s.is_some()).count()
    }

    /// Returns the number of missing shards.
    #[must_use]
    pub fn missing_count(&self) -> usize {
        self.shards.iter().filter(|s| s.is_none()).count()
    }

    /// Returns true if enough shards are present for reconstruction.
    #[must_use]
    pub fn can_reconstruct(&self) -> bool {
        self.present_count() >= self.data_shards
    }

    /// Returns the total number of shards (data + parity).
    #[must_use]
    pub fn total_shards(&self) -> usize {
        self.shards.len()
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

    /// Consumes the set and returns the inner shards.
    #[must_use]
    pub fn into_shards(self) -> Vec<Option<Vec<u8>>> {
        self.shards
    }

    /// Returns an iterator over present shard indices.
    pub fn present_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.shards.iter().enumerate().filter(|(_, s)| s.is_some()).map(|(i, _)| i)
    }

    /// Returns an iterator over missing shard indices.
    pub fn missing_indices(&self) -> impl Iterator<Item = usize> + '_ {
        self.shards.iter().enumerate().filter(|(_, s)| s.is_none()).map(|(i, _)| i)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_id() {
        let data_shard = ShardId::data(0);
        assert!(data_shard.is_data());
        assert!(!data_shard.is_parity());
        assert_eq!(data_shard.index(), 0);

        let parity_shard = ShardId::parity(2);
        assert!(!parity_shard.is_data());
        assert!(parity_shard.is_parity());
        assert_eq!(parity_shard.index(), 2);
    }

    #[test]
    fn test_shard_display() {
        let data = ShardId::data(3);
        assert_eq!(data.to_string(), "D:3");

        let parity = ShardId::parity(1);
        assert_eq!(parity.to_string(), "P:1");
    }

    #[test]
    fn test_shard_set_empty() {
        let set = ShardSet::new_empty(8, 4);
        assert_eq!(set.total_shards(), 12);
        assert_eq!(set.present_count(), 0);
        assert_eq!(set.missing_count(), 12);
        assert!(!set.can_reconstruct());
    }

    #[test]
    fn test_shard_set_partial() {
        let mut set = ShardSet::new_empty(8, 4);
        for i in 0..8 {
            set.set_shard(i, vec![i as u8; 10]);
        }
        assert_eq!(set.present_count(), 8);
        assert_eq!(set.missing_count(), 4);
        assert!(set.can_reconstruct());
    }

    #[test]
    fn test_shard_set_indices() {
        let mut set = ShardSet::new_empty(4, 2);
        set.set_shard(0, vec![0]);
        set.set_shard(2, vec![2]);
        set.set_shard(4, vec![4]);

        let present: Vec<_> = set.present_indices().collect();
        assert_eq!(present, vec![0, 2, 4]);

        let missing: Vec<_> = set.missing_indices().collect();
        assert_eq!(missing, vec![1, 3, 5]);
    }
}
