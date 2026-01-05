//! Cluster management and failure detection for Rucket distributed storage.
//!
//! This crate provides:
//! - Phi Accrual Failure Detector for probabilistic failure detection
//! - Heartbeat manager for cluster health monitoring
//! - Node state tracking and event emission
//! - Shard repair loop for automatic data recovery
//! - Rebalancing on node join/leave
//!
//! # Architecture
//!
//! The cluster management system uses a gossip-based approach where:
//! 1. Each node periodically sends heartbeats to all known peers
//! 2. Heartbeats include membership information for peer discovery
//! 3. The Phi Accrual detector computes failure suspicion levels
//! 4. Events are emitted when nodes change state (healthy/warning/failed)
//! 5. Rebalancing redistributes data when nodes join or leave
//!
//! # Example
//!
//! ```ignore
//! use rucket_cluster::{HeartbeatManager, HeartbeatConfig, NoOpHeartbeatSender};
//! use std::sync::Arc;
//!
//! // Create heartbeat manager
//! let config = HeartbeatConfig {
//!     local_node_id: "node-1".to_string(),
//!     ..Default::default()
//! };
//! let sender = Arc::new(NoOpHeartbeatSender);
//! let mut manager = HeartbeatManager::new(config, sender);
//!
//! // Subscribe to events
//! let mut events = manager.subscribe();
//!
//! // Add peers to monitor
//! manager.add_peer("node-2".to_string()).await;
//!
//! // Start the heartbeat loop
//! manager.start().await;
//!
//! // Handle events
//! while let Ok(event) = events.recv().await {
//!     match event {
//!         HeartbeatEvent::NodeFailed { node_id, phi } => {
//!             println!("Node {} failed with phi={}", node_id, phi);
//!         }
//!         _ => {}
//!     }
//! }
//! ```

#![deny(unsafe_code)]
#![warn(missing_docs)]

pub mod heartbeat;
pub mod phi_detector;
pub mod rebalance;
pub mod repair;

// Re-export main types
pub use heartbeat::{
    Heartbeat, HeartbeatConfig, HeartbeatEvent, HeartbeatManager, HeartbeatMetadata,
    HeartbeatSender, NoOpHeartbeatSender, NodeState,
};
pub use phi_detector::{NodeStats, PhiAccrualDetector, PhiDetectorConfig};
pub use rebalance::{
    MigrationResult, NoOpPlacementComputer, NoOpShardMover, PlacementComputer, RebalanceConfig,
    RebalanceEvent, RebalanceManager, RebalancePlan, RebalanceReason, RebalanceTask, ShardInfo,
    ShardMover, TaskStatus as RebalanceTaskStatus,
};
pub use repair::{
    NoOpShardLocator, NoOpShardRepairer, ObjectInfo, RepairConfig, RepairEvent, RepairManager,
    RepairResult, ShardLocation, ShardLocator, ShardRepairTask, ShardRepairer, TaskStatus,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exports() {
        // Verify all public types are accessible
        let _config = PhiDetectorConfig::default();
        let _hb_config = HeartbeatConfig::default();
    }

    #[test]
    fn test_phi_detector_defaults() {
        let config = PhiDetectorConfig::default();
        assert_eq!(config.threshold, 8.0);
        assert_eq!(config.heartbeat_interval_ms, 1000);
    }

    #[test]
    fn test_heartbeat_config_defaults() {
        let config = HeartbeatConfig::default();
        assert_eq!(config.warning_threshold, 5.0);
        assert_eq!(config.failure_threshold, 8.0);
    }

    #[tokio::test]
    async fn test_integration() {
        use std::sync::Arc;

        // Create phi detector
        let detector = PhiAccrualDetector::with_defaults();
        detector.heartbeat("node-1");
        assert!(detector.is_healthy("node-1"));

        // Create heartbeat manager
        let config = HeartbeatConfig { local_node_id: "local".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);

        manager.add_peer("peer-1".to_string()).await;
        let members = manager.members().await;
        assert_eq!(members.len(), 1);
    }
}
