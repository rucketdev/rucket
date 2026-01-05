//! Heartbeat manager for cluster node monitoring.
//!
//! This module provides a heartbeat system that:
//! - Periodically sends heartbeats to peer nodes
//! - Processes incoming heartbeats
//! - Integrates with the Phi Accrual Failure Detector
//! - Tracks node health and triggers failure callbacks

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use dashmap::DashMap;
use metrics::{counter, gauge, histogram};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, RwLock};
use tracing::{debug, error, info, trace, warn};

use super::phi_detector::{PhiAccrualDetector, PhiDetectorConfig};

/// A heartbeat message exchanged between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Heartbeat {
    /// The ID of the node sending the heartbeat.
    pub sender_id: String,
    /// The current HLC timestamp of the sender.
    pub hlc_timestamp: u64,
    /// The sender's view of cluster membership.
    pub known_members: Vec<String>,
    /// Optional metadata (load, capacity, etc.).
    pub metadata: Option<HeartbeatMetadata>,
}

/// Optional metadata included in heartbeats.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HeartbeatMetadata {
    /// CPU load (0.0 to 1.0+).
    pub cpu_load: f64,
    /// Memory usage (0.0 to 1.0).
    pub memory_usage: f64,
    /// Disk usage (0.0 to 1.0).
    pub disk_usage: f64,
    /// Number of active connections.
    pub active_connections: u64,
    /// Custom tags.
    pub tags: Vec<(String, String)>,
}

/// Events emitted by the heartbeat manager.
#[derive(Debug, Clone)]
pub enum HeartbeatEvent {
    /// A node was detected as healthy.
    NodeHealthy {
        /// The node ID.
        node_id: String,
    },
    /// A node was detected as potentially failing (phi > warning threshold).
    NodeWarning {
        /// The node ID.
        node_id: String,
        /// The current phi value.
        phi: f64,
    },
    /// A node was detected as failed (phi > failure threshold).
    NodeFailed {
        /// The node ID.
        node_id: String,
        /// The current phi value.
        phi: f64,
    },
    /// A node has recovered after being marked as failed.
    NodeRecovered {
        /// The node ID.
        node_id: String,
    },
    /// A new node joined the cluster.
    NodeJoined {
        /// The node ID.
        node_id: String,
    },
    /// A node left the cluster.
    NodeLeft {
        /// The node ID.
        node_id: String,
    },
}

/// Trait for sending heartbeats to peer nodes.
#[async_trait]
pub trait HeartbeatSender: Send + Sync {
    /// Sends a heartbeat to the specified node.
    async fn send_heartbeat(&self, target_node: &str, heartbeat: &Heartbeat) -> Result<(), String>;
}

/// Configuration for the heartbeat manager.
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// The local node's ID.
    pub local_node_id: String,
    /// Interval between heartbeat sends.
    pub heartbeat_interval: Duration,
    /// Interval between failure detection checks.
    pub check_interval: Duration,
    /// Phi threshold for warning state.
    pub warning_threshold: f64,
    /// Phi threshold for failure state.
    pub failure_threshold: f64,
    /// Grace period before marking a node as failed (for flapping prevention).
    pub grace_period: Duration,
    /// Phi detector configuration.
    pub phi_config: PhiDetectorConfig,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            local_node_id: String::new(),
            heartbeat_interval: Duration::from_secs(1),
            check_interval: Duration::from_millis(500),
            warning_threshold: 5.0,
            failure_threshold: 8.0,
            grace_period: Duration::from_secs(5),
            phi_config: PhiDetectorConfig::default(),
        }
    }
}

/// State of a tracked node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeState {
    /// Node is healthy and responding.
    Healthy,
    /// Node is showing signs of trouble.
    Warning,
    /// Node is considered failed.
    Failed,
}

/// Heartbeat manager for cluster health monitoring.
pub struct HeartbeatManager {
    config: HeartbeatConfig,
    detector: Arc<PhiAccrualDetector>,
    node_states: DashMap<String, NodeState>,
    known_members: Arc<RwLock<HashSet<String>>>,
    sender: Arc<dyn HeartbeatSender>,
    event_tx: broadcast::Sender<HeartbeatEvent>,
    shutdown_tx: Option<mpsc::Sender<()>>,
}

impl HeartbeatManager {
    /// Creates a new heartbeat manager.
    pub fn new(config: HeartbeatConfig, sender: Arc<dyn HeartbeatSender>) -> Self {
        let (event_tx, _) = broadcast::channel(1024);
        let detector = Arc::new(PhiAccrualDetector::new(config.phi_config.clone()));

        Self {
            config,
            detector,
            node_states: DashMap::new(),
            known_members: Arc::new(RwLock::new(HashSet::new())),
            sender,
            event_tx,
            shutdown_tx: None,
        }
    }

    /// Subscribes to heartbeat events.
    pub fn subscribe(&self) -> broadcast::Receiver<HeartbeatEvent> {
        self.event_tx.subscribe()
    }

    /// Adds a peer node to monitor.
    pub async fn add_peer(&self, node_id: String) {
        let mut members = self.known_members.write().await;
        if members.insert(node_id.clone()) {
            self.node_states.insert(node_id.clone(), NodeState::Healthy);
            let _ = self.event_tx.send(HeartbeatEvent::NodeJoined { node_id: node_id.clone() });
            info!(node_id = %node_id, "Added peer to heartbeat monitoring");
        }
    }

    /// Removes a peer node from monitoring.
    pub async fn remove_peer(&self, node_id: &str) {
        let mut members = self.known_members.write().await;
        if members.remove(node_id) {
            self.node_states.remove(node_id);
            self.detector.remove_node(node_id);
            let _ = self.event_tx.send(HeartbeatEvent::NodeLeft { node_id: node_id.to_string() });
            info!(node_id = %node_id, "Removed peer from heartbeat monitoring");
        }
    }

    /// Processes an incoming heartbeat.
    pub async fn on_heartbeat(&self, heartbeat: Heartbeat) {
        let node_id = &heartbeat.sender_id;

        // Record heartbeat for failure detection
        self.detector.heartbeat(node_id);

        // Update known members
        {
            let mut members = self.known_members.write().await;
            for member in &heartbeat.known_members {
                if member != &self.config.local_node_id && !members.contains(member) {
                    members.insert(member.clone());
                    self.node_states.insert(member.clone(), NodeState::Healthy);
                    debug!(node_id = %member, "Discovered new peer via gossip");
                }
            }
        }

        // Update metrics
        if let Some(metadata) = &heartbeat.metadata {
            gauge!("rucket_cluster_peer_cpu_load", "node_id" => node_id.clone())
                .set(metadata.cpu_load);
            gauge!("rucket_cluster_peer_memory_usage", "node_id" => node_id.clone())
                .set(metadata.memory_usage);
            gauge!("rucket_cluster_peer_disk_usage", "node_id" => node_id.clone())
                .set(metadata.disk_usage);
        }

        counter!("rucket_cluster_heartbeats_received", "from" => node_id.clone()).increment(1);

        trace!(
            sender = %node_id,
            hlc = heartbeat.hlc_timestamp,
            members = heartbeat.known_members.len(),
            "Processed heartbeat"
        );

        // Check if node recovered from failure
        if let Some(current_state) = self.node_states.get(node_id) {
            if *current_state == NodeState::Failed {
                self.node_states.insert(node_id.clone(), NodeState::Healthy);
                let _ =
                    self.event_tx.send(HeartbeatEvent::NodeRecovered { node_id: node_id.clone() });
                info!(node_id = %node_id, "Node recovered from failure");
            }
        }
    }

    /// Starts the heartbeat send loop.
    pub async fn start(&mut self) {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);
        self.shutdown_tx = Some(shutdown_tx);

        let config = self.config.clone();
        let detector = Arc::clone(&self.detector);
        let known_members = Arc::clone(&self.known_members);
        let sender = Arc::clone(&self.sender);
        let node_states = self.node_states.clone();
        let event_tx = self.event_tx.clone();

        // Heartbeat send task
        let send_config = config.clone();
        let send_members = Arc::clone(&known_members);
        let send_sender = Arc::clone(&sender);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(send_config.heartbeat_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::send_heartbeats(&send_config, &send_members, &send_sender).await;
                    }
                    _ = shutdown_rx.recv() => {
                        info!("Heartbeat manager shutting down");
                        break;
                    }
                }
            }
        });

        // Failure detection check task
        let check_config = config.clone();
        let check_detector = Arc::clone(&detector);
        let check_states = node_states.clone();
        let check_events = event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(check_config.check_interval);

            loop {
                interval.tick().await;
                Self::check_node_health(
                    &check_config,
                    &check_detector,
                    &check_states,
                    &check_events,
                );
            }
        });

        info!(
            local_node = %self.config.local_node_id,
            interval_ms = self.config.heartbeat_interval.as_millis(),
            "Heartbeat manager started"
        );
    }

    /// Sends heartbeats to all known peers.
    async fn send_heartbeats(
        config: &HeartbeatConfig,
        known_members: &RwLock<HashSet<String>>,
        sender: &Arc<dyn HeartbeatSender>,
    ) {
        let members: Vec<String> = {
            let members = known_members.read().await;
            members.iter().cloned().collect()
        };

        if members.is_empty() {
            return;
        }

        let heartbeat = Heartbeat {
            sender_id: config.local_node_id.clone(),
            hlc_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            known_members: members.clone(),
            metadata: None, // Could be filled with system stats
        };

        for peer in &members {
            let start = std::time::Instant::now();
            match sender.send_heartbeat(peer, &heartbeat).await {
                Ok(()) => {
                    let duration = start.elapsed();
                    histogram!("rucket_cluster_heartbeat_send_duration_ms", "target" => peer.clone())
                        .record(duration.as_secs_f64() * 1000.0);
                    counter!("rucket_cluster_heartbeats_sent", "to" => peer.clone()).increment(1);
                    trace!(target = %peer, duration_ms = ?duration.as_millis(), "Sent heartbeat");
                }
                Err(e) => {
                    counter!("rucket_cluster_heartbeat_send_errors", "to" => peer.clone())
                        .increment(1);
                    warn!(target = %peer, error = %e, "Failed to send heartbeat");
                }
            }
        }
    }

    /// Checks the health of all nodes and emits events.
    fn check_node_health(
        config: &HeartbeatConfig,
        detector: &PhiAccrualDetector,
        node_states: &DashMap<String, NodeState>,
        event_tx: &broadcast::Sender<HeartbeatEvent>,
    ) {
        // Collect nodes to update to avoid holding borrow during modification
        let updates: Vec<(String, NodeState, NodeState, f64)> = node_states
            .iter()
            .filter_map(|entry| {
                let node_id = entry.key().clone();
                let current_state = *entry.value();

                let phi = detector.phi(&node_id).unwrap_or(0.0);

                let new_state = if phi > config.failure_threshold {
                    NodeState::Failed
                } else if phi > config.warning_threshold {
                    NodeState::Warning
                } else {
                    NodeState::Healthy
                };

                if new_state != current_state {
                    Some((node_id, current_state, new_state, phi))
                } else {
                    // Still record the phi metric even without state change
                    gauge!("rucket_cluster_node_phi", "node_id" => node_id).set(phi);
                    None
                }
            })
            .collect();

        // Apply updates and emit events
        for (node_id, current_state, new_state, phi) in updates {
            gauge!("rucket_cluster_node_phi", "node_id" => node_id.clone()).set(phi);

            match new_state {
                NodeState::Healthy => {
                    if current_state == NodeState::Failed {
                        let _ = event_tx
                            .send(HeartbeatEvent::NodeRecovered { node_id: node_id.clone() });
                        info!(node_id = %node_id, "Node recovered");
                    }
                    let _ = event_tx.send(HeartbeatEvent::NodeHealthy { node_id: node_id.clone() });
                }
                NodeState::Warning => {
                    let _ = event_tx
                        .send(HeartbeatEvent::NodeWarning { node_id: node_id.clone(), phi });
                    warn!(node_id = %node_id, phi = phi, "Node showing signs of trouble");
                }
                NodeState::Failed => {
                    let _ =
                        event_tx.send(HeartbeatEvent::NodeFailed { node_id: node_id.clone(), phi });
                    error!(node_id = %node_id, phi = phi, "Node detected as failed");
                }
            }

            node_states.insert(node_id, new_state);
        }
    }

    /// Stops the heartbeat manager.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(()).await;
        }
    }

    /// Returns the current state of a node.
    pub fn node_state(&self, node_id: &str) -> Option<NodeState> {
        self.node_states.get(node_id).map(|s| *s)
    }

    /// Returns the phi value for a node.
    pub fn node_phi(&self, node_id: &str) -> Option<f64> {
        self.detector.phi(node_id)
    }

    /// Returns all known members.
    pub async fn members(&self) -> Vec<String> {
        self.known_members.read().await.iter().cloned().collect()
    }

    /// Returns all healthy members.
    pub fn healthy_members(&self) -> Vec<String> {
        self.node_states
            .iter()
            .filter_map(|entry| {
                if *entry.value() == NodeState::Healthy {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns all failed members.
    pub fn failed_members(&self) -> Vec<String> {
        self.node_states
            .iter()
            .filter_map(|entry| {
                if *entry.value() == NodeState::Failed {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect()
    }

    /// Returns the local node ID.
    pub fn local_node_id(&self) -> &str {
        &self.config.local_node_id
    }
}

/// A no-op heartbeat sender for testing.
pub struct NoOpHeartbeatSender;

#[async_trait]
impl HeartbeatSender for NoOpHeartbeatSender {
    async fn send_heartbeat(&self, _target: &str, _heartbeat: &Heartbeat) -> Result<(), String> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockHeartbeatSender {
        sent: Arc<RwLock<Vec<(String, Heartbeat)>>>,
    }

    impl MockHeartbeatSender {
        fn new() -> Self {
            Self { sent: Arc::new(RwLock::new(Vec::new())) }
        }

        async fn sent_count(&self) -> usize {
            self.sent.read().await.len()
        }
    }

    #[async_trait]
    impl HeartbeatSender for MockHeartbeatSender {
        async fn send_heartbeat(&self, target: &str, heartbeat: &Heartbeat) -> Result<(), String> {
            self.sent.write().await.push((target.to_string(), heartbeat.clone()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_heartbeat_manager_creation() {
        let config = HeartbeatConfig { local_node_id: "node-1".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);

        assert_eq!(manager.local_node_id(), "node-1");
    }

    #[tokio::test]
    async fn test_add_peer() {
        let config = HeartbeatConfig { local_node_id: "node-1".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);

        manager.add_peer("node-2".to_string()).await;
        manager.add_peer("node-3".to_string()).await;

        let members = manager.members().await;
        assert_eq!(members.len(), 2);
        assert!(members.contains(&"node-2".to_string()));
        assert!(members.contains(&"node-3".to_string()));
    }

    #[tokio::test]
    async fn test_remove_peer() {
        let config = HeartbeatConfig { local_node_id: "node-1".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);

        manager.add_peer("node-2".to_string()).await;
        assert_eq!(manager.members().await.len(), 1);

        manager.remove_peer("node-2").await;
        assert_eq!(manager.members().await.len(), 0);
    }

    #[tokio::test]
    async fn test_on_heartbeat_updates_detector() {
        let config = HeartbeatConfig { local_node_id: "node-1".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);

        manager.add_peer("node-2".to_string()).await;

        let heartbeat = Heartbeat {
            sender_id: "node-2".to_string(),
            hlc_timestamp: 12345,
            known_members: vec!["node-1".to_string()],
            metadata: None,
        };

        manager.on_heartbeat(heartbeat).await;

        // Phi should be available now
        assert!(manager.node_phi("node-2").is_some());
    }

    #[tokio::test]
    async fn test_gossip_discovery() {
        let config = HeartbeatConfig { local_node_id: "node-1".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);

        // Node-2 tells us about node-3
        let heartbeat = Heartbeat {
            sender_id: "node-2".to_string(),
            hlc_timestamp: 12345,
            known_members: vec!["node-1".to_string(), "node-3".to_string()],
            metadata: None,
        };

        manager.add_peer("node-2".to_string()).await;
        manager.on_heartbeat(heartbeat).await;

        let members = manager.members().await;
        assert!(members.contains(&"node-3".to_string()));
    }

    #[tokio::test]
    async fn test_event_subscription() {
        let config = HeartbeatConfig { local_node_id: "node-1".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);
        let mut rx = manager.subscribe();

        manager.add_peer("node-2".to_string()).await;

        // Should receive NodeJoined event
        let event = rx.try_recv().unwrap();
        match event {
            HeartbeatEvent::NodeJoined { node_id } => {
                assert_eq!(node_id, "node-2");
            }
            _ => panic!("Expected NodeJoined event"),
        }
    }

    #[tokio::test]
    async fn test_healthy_members() {
        let config = HeartbeatConfig { local_node_id: "node-1".to_string(), ..Default::default() };

        let sender = Arc::new(NoOpHeartbeatSender);
        let manager = HeartbeatManager::new(config, sender);

        manager.add_peer("node-2".to_string()).await;
        manager.add_peer("node-3".to_string()).await;

        let healthy = manager.healthy_members();
        assert_eq!(healthy.len(), 2);
    }

    #[tokio::test]
    async fn test_send_heartbeats() {
        let config = HeartbeatConfig {
            local_node_id: "node-1".to_string(),
            heartbeat_interval: Duration::from_millis(10),
            ..Default::default()
        };

        let sender = Arc::new(MockHeartbeatSender::new());
        let manager = HeartbeatManager::new(config.clone(), sender.clone());

        manager.add_peer("node-2".to_string()).await;

        // Manually trigger heartbeat send
        let members = Arc::clone(&manager.known_members);
        HeartbeatManager::send_heartbeats(
            &config,
            &members,
            &(sender.clone() as Arc<dyn HeartbeatSender>),
        )
        .await;

        assert_eq!(sender.sent_count().await, 1);
    }
}
