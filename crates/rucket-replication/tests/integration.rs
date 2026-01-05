// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Integration tests for replication.

use rucket_replication::{
    LagTracker, NoOpReplicator, ReplicationConfig, ReplicationEntry, ReplicationError,
    ReplicationLevel, ReplicationOperation, Replicator, DEFAULT_REPLICATION_FACTOR,
};

#[test]
fn test_exports() {
    // Verify all public types are accessible
    let _level = ReplicationLevel::default();
    let _config = ReplicationConfig::default();

    assert_eq!(_level, ReplicationLevel::Replicated);
    assert_eq!(_config.replication_factor, DEFAULT_REPLICATION_FACTOR);
}

#[test]
fn test_replication_level_serialization() {
    let level = ReplicationLevel::Durable;
    let json = serde_json::to_string(&level).unwrap();
    assert_eq!(json, "\"durable\"");

    let parsed: ReplicationLevel = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed, ReplicationLevel::Durable);
}

#[test]
fn test_config_builder() {
    let config = ReplicationConfig::new()
        .enabled(true)
        .replication_factor(5)
        .default_level(ReplicationLevel::Durable);

    assert!(config.enabled);
    assert_eq!(config.replication_factor, 5);
    assert_eq!(config.default_level, ReplicationLevel::Durable);
    assert_eq!(config.quorum_size(), 3); // 5/2 + 1
}

#[tokio::test]
async fn test_noop_replicator() {
    let replicator = NoOpReplicator;

    let entry = ReplicationEntry::new(
        ReplicationOperation::CreateBucket { bucket: "test".to_string() },
        12345,
        ReplicationLevel::Durable,
    );

    let result = replicator.replicate(entry).await.unwrap();
    assert!(result.quorum_achieved);
    assert_eq!(result.acks_received, 1);

    let replicas = replicator.get_replicas().await;
    assert!(replicas.is_empty());
}

#[test]
fn test_error_display() {
    let error = ReplicationError::QuorumNotAchieved { required: 3, achieved: 1 };
    assert_eq!(error.to_string(), "quorum not achieved: required 3, got 1");

    let error = ReplicationError::Timeout { node_id: "node1".to_string(), timeout_ms: 5000 };
    assert_eq!(error.to_string(), "replication to node1 timed out after 5000ms");
}

#[test]
fn test_lag_tracker_basic() {
    let tracker = LagTracker::new();
    tracker.add_replica("node1".to_string());

    assert!(tracker.get_lag_ms("node1").is_none());

    tracker.record_success("node1", 100);
    assert!(tracker.get_lag_ms("node1").is_some());
    assert_eq!(tracker.get_last_hlc("node1"), Some(100));

    tracker.record_failure("node1");
    assert_eq!(tracker.get_failure_count("node1"), Some(1));
}
