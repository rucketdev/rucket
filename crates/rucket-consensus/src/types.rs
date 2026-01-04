//! OpenRaft type configuration for Rucket.
//!
//! This module defines the core type aliases and configuration for OpenRaft,
//! including the node identifier type, entry types, and runtime selection.

use std::io::Cursor;

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use crate::command::MetadataCommand;
use crate::response::MetadataResponse;

/// Node identifier type for Raft cluster.
///
/// Uses u64 for OpenRaft compatibility. Node IDs must be unique across
/// the cluster and stable across restarts.
pub type RaftNodeId = u64;

/// OpenRaft type configuration for Rucket metadata consensus.
///
/// This struct configures all the generic types used by OpenRaft:
/// - Command type (MetadataCommand)
/// - Response type (MetadataResponse)
/// - Node identifier (u64)
/// - Snapshot data format (bincode-serialized state)
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub struct RaftTypeConfig;

impl openraft::RaftTypeConfig for RaftTypeConfig {
    /// The command type sent to the Raft state machine.
    ///
    /// All metadata mutations (bucket creation, object metadata writes, etc.)
    /// are encoded as `MetadataCommand` and replicated through Raft.
    type D = MetadataCommand;

    /// The response type returned after applying a command.
    ///
    /// Contains the result of the operation (success, error, or specific data
    /// like version IDs for delete markers).
    type R = MetadataResponse;

    /// Node identifier type.
    ///
    /// Uses u64 for simplicity and OpenRaft compatibility.
    type NodeId = RaftNodeId;

    /// Node information (address, metadata).
    ///
    /// Uses OpenRaft's BasicNode which contains just an address string.
    type Node = BasicNode;

    /// Log entry type.
    ///
    /// Uses OpenRaft's default Entry type parameterized by our config.
    type Entry = openraft::Entry<RaftTypeConfig>;

    /// Snapshot data type.
    ///
    /// Snapshots are serialized as bincode and wrapped in a Cursor for streaming.
    type SnapshotData = Cursor<Vec<u8>>;

    /// Async runtime.
    ///
    /// Uses Tokio runtime for async operations.
    type AsyncRuntime = openraft::TokioRuntime;

    /// Responder type for client write requests.
    ///
    /// Uses the standard oneshot channel responder for simplicity.
    type Responder = openraft::impls::OneshotResponder<RaftTypeConfig>;
}

/// Type alias for the Raft instance with Rucket configuration.
pub type RucketRaft = openraft::Raft<RaftTypeConfig>;

/// Type alias for Raft log entries.
pub type RaftEntry = openraft::Entry<RaftTypeConfig>;

/// Type alias for Raft log ID.
pub type RaftLogId = openraft::LogId<RaftNodeId>;

/// Type alias for Raft vote.
pub type RaftVote = openraft::Vote<RaftNodeId>;

/// Type alias for stored membership configuration.
pub type RaftMembership = openraft::StoredMembership<RaftNodeId, BasicNode>;

/// Type alias for snapshot metadata.
pub type RaftSnapshotMeta = openraft::SnapshotMeta<RaftNodeId, BasicNode>;

/// Type alias for Raft snapshot.
pub type RaftSnapshot = openraft::storage::Snapshot<RaftTypeConfig>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_type_config_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RaftTypeConfig>();
    }

    #[test]
    fn test_node_id_serialization() {
        let node_id: RaftNodeId = 42;
        let serialized = bincode::serialize(&node_id).unwrap();
        let deserialized: RaftNodeId = bincode::deserialize(&serialized).unwrap();
        assert_eq!(node_id, deserialized);
    }
}
