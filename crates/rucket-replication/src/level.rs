//! Replication levels for durability guarantees.
//!
//! This module defines three replication levels that provide different
//! durability and latency trade-offs:
//!
//! - **Local**: Write to local node only (fastest, least durable)
//! - **Replicated**: Async replication to backups (balanced)
//! - **Durable**: Sync replication with quorum acknowledgment (slowest, most durable)

use serde::{Deserialize, Serialize};

/// Replication level determining durability guarantees.
///
/// The replication level controls how writes are acknowledged:
///
/// ```text
/// Level       | Write Ack          | Durability Guarantee
/// ------------|--------------------|-----------------------
/// Local       | After local write  | Single node
/// Replicated  | After local write  | Eventual (async backup)
/// Durable     | After quorum ack   | Quorum nodes (RF/2+1)
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ReplicationLevel {
    /// Write acknowledged after local persistence only.
    ///
    /// This is the fastest option but provides no redundancy.
    /// Data may be lost if the local node fails before replication.
    Local,

    /// Write acknowledged after local persistence, replicated asynchronously.
    ///
    /// Provides a good balance between performance and durability.
    /// The write is acknowledged immediately, but backups receive
    /// the data asynchronously. Small window of data loss possible
    /// if primary fails before replication completes.
    #[default]
    Replicated,

    /// Write acknowledged only after quorum of replicas confirm.
    ///
    /// Provides the strongest durability guarantee at the cost of
    /// higher latency. The write is not acknowledged until RF/2+1
    /// nodes (including the primary) have persisted the data.
    Durable,
}

impl ReplicationLevel {
    /// Returns true if this level requires synchronous replication.
    #[inline]
    pub fn is_synchronous(&self) -> bool {
        matches!(self, Self::Durable)
    }

    /// Returns true if this level performs any replication.
    #[inline]
    pub fn replicates(&self) -> bool {
        !matches!(self, Self::Local)
    }

    /// Returns the name of this replication level.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Local => "local",
            Self::Replicated => "replicated",
            Self::Durable => "durable",
        }
    }
}

impl std::fmt::Display for ReplicationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl std::str::FromStr for ReplicationLevel {
    type Err = ParseReplicationLevelError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "local" => Ok(Self::Local),
            "replicated" => Ok(Self::Replicated),
            "durable" => Ok(Self::Durable),
            _ => Err(ParseReplicationLevelError(s.to_string())),
        }
    }
}

/// Error parsing a replication level string.
#[derive(Debug, Clone)]
pub struct ParseReplicationLevelError(String);

impl std::fmt::Display for ParseReplicationLevelError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "invalid replication level '{}': expected 'local', 'replicated', or 'durable'",
            self.0
        )
    }
}

impl std::error::Error for ParseReplicationLevelError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replication_level_default() {
        assert_eq!(ReplicationLevel::default(), ReplicationLevel::Replicated);
    }

    #[test]
    fn test_is_synchronous() {
        assert!(!ReplicationLevel::Local.is_synchronous());
        assert!(!ReplicationLevel::Replicated.is_synchronous());
        assert!(ReplicationLevel::Durable.is_synchronous());
    }

    #[test]
    fn test_replicates() {
        assert!(!ReplicationLevel::Local.replicates());
        assert!(ReplicationLevel::Replicated.replicates());
        assert!(ReplicationLevel::Durable.replicates());
    }

    #[test]
    fn test_name() {
        assert_eq!(ReplicationLevel::Local.name(), "local");
        assert_eq!(ReplicationLevel::Replicated.name(), "replicated");
        assert_eq!(ReplicationLevel::Durable.name(), "durable");
    }

    #[test]
    fn test_display() {
        assert_eq!(format!("{}", ReplicationLevel::Local), "local");
        assert_eq!(format!("{}", ReplicationLevel::Replicated), "replicated");
        assert_eq!(format!("{}", ReplicationLevel::Durable), "durable");
    }

    #[test]
    fn test_from_str() {
        assert_eq!("local".parse::<ReplicationLevel>().unwrap(), ReplicationLevel::Local);
        assert_eq!("replicated".parse::<ReplicationLevel>().unwrap(), ReplicationLevel::Replicated);
        assert_eq!("durable".parse::<ReplicationLevel>().unwrap(), ReplicationLevel::Durable);
        assert_eq!("LOCAL".parse::<ReplicationLevel>().unwrap(), ReplicationLevel::Local);
        assert_eq!("DURABLE".parse::<ReplicationLevel>().unwrap(), ReplicationLevel::Durable);
        assert!("invalid".parse::<ReplicationLevel>().is_err());
    }

    #[test]
    fn test_serialize_deserialize() {
        let level = ReplicationLevel::Durable;
        let json = serde_json::to_string(&level).unwrap();
        assert_eq!(json, "\"durable\"");

        let parsed: ReplicationLevel = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, level);
    }
}
