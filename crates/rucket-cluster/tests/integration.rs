// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (c) 2025 The Rucket Authors

//! Integration tests for cluster management.

use std::sync::Arc;

use rucket_cluster::{
    HeartbeatConfig, HeartbeatManager, NoOpHeartbeatSender, PhiAccrualDetector, PhiDetectorConfig,
};

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
