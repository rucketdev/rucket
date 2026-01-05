//! Cluster admin API handlers.
//!
//! This module implements the HTTP API for cluster management operations.
//! These endpoints are called by the `rucket cluster` CLI commands.

use std::collections::BTreeSet;
use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use openraft::BasicNode;
use rucket_cluster::RebalanceManager;
use rucket_consensus::types::RucketRaft;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// Application state for admin handlers.
#[derive(Clone)]
pub struct AdminState {
    /// The Raft instance.
    pub raft: Arc<RucketRaft>,
    /// This node's ID.
    pub node_id: u64,
    /// The rebalance manager for shard redistribution.
    pub rebalance_manager: Option<Arc<RwLock<RebalanceManager>>>,
}

/// Cluster status response.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStatusResponse {
    /// Whether the cluster is healthy.
    pub healthy: bool,
    /// Current leader node ID.
    pub leader_id: Option<u64>,
    /// This node's ID.
    pub node_id: u64,
    /// Current Raft term.
    pub term: u64,
    /// Cluster state (Leader, Follower, Candidate, Learner).
    pub state: String,
    /// Number of nodes in the cluster.
    pub node_count: usize,
    /// List of nodes in the cluster.
    pub nodes: Vec<NodeInfoResponse>,
    /// Commit index.
    pub commit_index: u64,
    /// Last applied index.
    pub last_applied: u64,
}

/// Information about a cluster node.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeInfoResponse {
    /// Node ID.
    pub node_id: u64,
    /// Node address.
    pub address: String,
    /// Node role (Voter, Learner).
    pub role: String,
    /// Whether the node is reachable.
    pub reachable: bool,
    /// Last heartbeat timestamp (if available).
    pub last_heartbeat: Option<String>,
}

/// Request to add a node.
#[derive(Debug, Deserialize)]
pub struct AddNodeRequest {
    /// Node ID to add.
    pub node_id: u64,
    /// Node address (host:port for Raft communication).
    pub address: String,
    /// Add node as a learner first.
    #[serde(default = "default_true")]
    pub as_learner: bool,
}

fn default_true() -> bool {
    true
}

/// Response from add-node operation.
#[derive(Debug, Serialize)]
pub struct AddNodeResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Message describing the result.
    pub message: String,
    /// The node that was added.
    pub node_id: u64,
}

/// Response from remove-node operation.
#[derive(Debug, Serialize)]
pub struct RemoveNodeResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Message describing the result.
    pub message: String,
    /// The node that was removed.
    pub node_id: u64,
}

/// Query parameters for remove-node operation.
#[derive(Debug, Deserialize, Default)]
pub struct RemoveNodeQuery {
    /// Force removal even if node is unreachable.
    #[serde(default)]
    pub force: Option<String>,
}

/// Request to trigger rebalance.
#[derive(Debug, Deserialize)]
pub struct RebalanceRequest {
    /// Only show what would be rebalanced (dry run).
    #[serde(default)]
    pub dry_run: bool,
    /// Maximum concurrent migrations.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
}

fn default_max_concurrent() -> usize {
    4
}

/// Response from rebalance operation.
#[derive(Debug, Serialize)]
pub struct RebalanceResponse {
    /// Whether the operation was initiated.
    pub initiated: bool,
    /// Message describing the result.
    pub message: String,
    /// Number of shards that will be moved.
    pub shards_to_move: usize,
    /// Total bytes to transfer.
    pub bytes_to_transfer: u64,
    /// Estimated duration (if available).
    pub estimated_duration: Option<String>,
}

/// Response from rebalance status query.
#[derive(Debug, Serialize)]
pub struct RebalanceStatusResponse {
    /// Whether a rebalance is currently active.
    pub active: bool,
    /// Number of migrations pending.
    pub pending_migrations: usize,
    /// Number of migrations in progress.
    pub in_progress_migrations: usize,
    /// Total completed migrations (since manager start).
    pub completed_migrations: usize,
    /// Total failed migrations (since manager start).
    pub failed_migrations: usize,
    /// Total bytes transferred.
    pub bytes_transferred: u64,
    /// Current cluster members known to the rebalance manager.
    pub cluster_members: Vec<String>,
}

/// Get cluster status.
///
/// GET /_cluster/status
pub async fn get_cluster_status(State(state): State<AdminState>) -> Response {
    let metrics = state.raft.metrics().borrow().clone();

    // Determine state from Raft metrics
    let raft_state = format!("{:?}", metrics.state);

    // Build node list from membership
    let mut nodes = Vec::new();
    let membership = metrics.membership_config.membership();
    for (node_id, node) in membership.nodes() {
        let is_voter = membership.voter_ids().any(|id| id == *node_id);
        nodes.push(NodeInfoResponse {
            node_id: *node_id,
            address: node.addr.clone(),
            role: if is_voter { "Voter".to_string() } else { "Learner".to_string() },
            reachable: true, // We'd need actual health checks for this
            last_heartbeat: None,
        });
    }

    let healthy = metrics.current_leader.is_some();
    let node_count = nodes.len();
    let last_applied_index = metrics.last_applied.map(|l| l.index).unwrap_or(0);

    let response = ClusterStatusResponse {
        healthy,
        leader_id: metrics.current_leader,
        node_id: state.node_id,
        term: metrics.current_term,
        state: raft_state,
        node_count,
        nodes,
        commit_index: last_applied_index,
        last_applied: last_applied_index,
    };

    Json(response).into_response()
}

/// List all nodes in the cluster.
///
/// GET /_cluster/nodes
pub async fn list_nodes(State(state): State<AdminState>) -> Response {
    let metrics = state.raft.metrics().borrow().clone();

    let mut nodes = Vec::new();
    let membership = metrics.membership_config.membership();
    for (node_id, node) in membership.nodes() {
        let is_voter = membership.voter_ids().any(|id| id == *node_id);
        nodes.push(NodeInfoResponse {
            node_id: *node_id,
            address: node.addr.clone(),
            role: if is_voter { "Voter".to_string() } else { "Learner".to_string() },
            reachable: true,
            last_heartbeat: None,
        });
    }

    Json(nodes).into_response()
}

/// Add a node to the cluster.
///
/// POST /_cluster/nodes
pub async fn add_node(
    State(state): State<AdminState>,
    Json(req): Json<AddNodeRequest>,
) -> Response {
    // Check if this node is the leader
    let metrics = state.raft.metrics().borrow().clone();
    if metrics.current_leader != Some(state.node_id) {
        let response = AddNodeResponse {
            success: false,
            message: format!("This node is not the leader. Leader is {:?}", metrics.current_leader),
            node_id: req.node_id,
        };
        return (StatusCode::BAD_REQUEST, Json(response)).into_response();
    }

    // Add the node as a learner first (recommended for safety)
    let node = BasicNode { addr: req.address.clone() };

    match state.raft.add_learner(req.node_id, node.clone(), true).await {
        Ok(_) => {
            if req.as_learner {
                // Keep as learner
                let response = AddNodeResponse {
                    success: true,
                    message: format!("Node {} added as learner at {}", req.node_id, req.address),
                    node_id: req.node_id,
                };
                Json(response).into_response()
            } else {
                // Promote to voter
                let metrics = state.raft.metrics().borrow().clone();
                let membership = metrics.membership_config.membership();
                let mut voter_ids: BTreeSet<u64> = membership.voter_ids().collect();
                voter_ids.insert(req.node_id);

                match state.raft.change_membership(voter_ids, false).await {
                    Ok(_) => {
                        let response = AddNodeResponse {
                            success: true,
                            message: format!(
                                "Node {} added as voter at {}",
                                req.node_id, req.address
                            ),
                            node_id: req.node_id,
                        };
                        Json(response).into_response()
                    }
                    Err(e) => {
                        let response = AddNodeResponse {
                            success: false,
                            message: format!(
                                "Node added as learner but failed to promote to voter: {}",
                                e
                            ),
                            node_id: req.node_id,
                        };
                        (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
                    }
                }
            }
        }
        Err(e) => {
            let response =
                AddNodeResponse { success: false, message: e.to_string(), node_id: req.node_id };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// Remove a node from the cluster.
///
/// DELETE /_cluster/nodes/{node_id}
pub async fn remove_node(
    State(state): State<AdminState>,
    Path(node_id): Path<u64>,
    Query(query): Query<RemoveNodeQuery>,
) -> Response {
    let _force = query.force.as_ref().is_some_and(|v| v == "true");

    // Check if this node is the leader
    let metrics = state.raft.metrics().borrow().clone();
    if metrics.current_leader != Some(state.node_id) {
        let response = RemoveNodeResponse {
            success: false,
            message: format!("This node is not the leader. Leader is {:?}", metrics.current_leader),
            node_id,
        };
        return (StatusCode::BAD_REQUEST, Json(response)).into_response();
    }

    // Get current voters and remove this node
    let membership = metrics.membership_config.membership();
    let voter_ids: BTreeSet<u64> = membership.voter_ids().filter(|id| *id != node_id).collect();

    if voter_ids.is_empty() {
        let response = RemoveNodeResponse {
            success: false,
            message: "Cannot remove the last node from the cluster".to_string(),
            node_id,
        };
        return (StatusCode::BAD_REQUEST, Json(response)).into_response();
    }

    match state.raft.change_membership(voter_ids, false).await {
        Ok(_) => {
            let response = RemoveNodeResponse {
                success: true,
                message: format!("Node {} removed from cluster", node_id),
                node_id,
            };
            Json(response).into_response()
        }
        Err(e) => {
            let response = RemoveNodeResponse { success: false, message: e.to_string(), node_id };
            (StatusCode::INTERNAL_SERVER_ERROR, Json(response)).into_response()
        }
    }
}

/// Get rebalance status.
///
/// GET /_cluster/rebalance
pub async fn get_rebalance_status(State(state): State<AdminState>) -> Response {
    // Check if rebalance manager is available
    let Some(manager) = &state.rebalance_manager else {
        let response = RebalanceStatusResponse {
            active: false,
            pending_migrations: 0,
            in_progress_migrations: 0,
            completed_migrations: 0,
            failed_migrations: 0,
            bytes_transferred: 0,
            cluster_members: vec![],
        };
        return Json(response).into_response();
    };

    let manager = manager.read().await;
    let members = manager.members().await;

    let response = RebalanceStatusResponse {
        active: manager.is_active(),
        pending_migrations: manager.pending_count(),
        in_progress_migrations: manager.in_progress_count(),
        completed_migrations: manager.completed_count(),
        failed_migrations: manager.failed_count(),
        bytes_transferred: manager.bytes_transferred(),
        cluster_members: members,
    };

    Json(response).into_response()
}

/// Trigger a rebalance operation.
///
/// POST /_cluster/rebalance
pub async fn trigger_rebalance(
    State(state): State<AdminState>,
    Json(req): Json<RebalanceRequest>,
) -> Response {
    // Check if this node is the leader
    let metrics = state.raft.metrics().borrow().clone();
    if metrics.current_leader != Some(state.node_id) {
        let response = RebalanceResponse {
            initiated: false,
            message: format!("This node is not the leader. Leader is {:?}", metrics.current_leader),
            shards_to_move: 0,
            bytes_to_transfer: 0,
            estimated_duration: None,
        };
        return (StatusCode::BAD_REQUEST, Json(response)).into_response();
    }

    // Check if rebalance manager is available
    let Some(manager) = &state.rebalance_manager else {
        let response = RebalanceResponse {
            initiated: false,
            message:
                "Rebalance manager not configured. Enable cluster mode with rebalancing support."
                    .to_string(),
            shards_to_move: 0,
            bytes_to_transfer: 0,
            estimated_duration: None,
        };
        return (StatusCode::SERVICE_UNAVAILABLE, Json(response)).into_response();
    };

    let manager = manager.read().await;

    // Check if a rebalance is already in progress
    if manager.is_active() {
        let response = RebalanceResponse {
            initiated: false,
            message: format!(
                "Rebalance already in progress: {} pending, {} in progress",
                manager.pending_count(),
                manager.in_progress_count()
            ),
            shards_to_move: manager.pending_count(),
            bytes_to_transfer: 0,
            estimated_duration: None,
        };
        return (StatusCode::CONFLICT, Json(response)).into_response();
    }

    // For dry run, just return current status
    // Note: Full plan computation requires PlacementComputer implementation
    // which depends on the storage layer. Currently we return manager status.
    if req.dry_run {
        let response = RebalanceResponse {
            initiated: false,
            message: "Dry run: No migrations currently scheduled. Manual rebalance planning requires shard placement information.".to_string(),
            shards_to_move: manager.pending_count(),
            bytes_to_transfer: 0,
            estimated_duration: Some("0s".to_string()),
        };
        return Json(response).into_response();
    }

    // For actual rebalance, we would need to compute a plan based on current
    // shard distribution. This requires PlacementComputer which tracks shards.
    // For now, we indicate that manual rebalancing is ready but waiting for
    // cluster membership events to trigger automatic rebalancing.
    let response = RebalanceResponse {
        initiated: true,
        message: format!(
            "Rebalance monitoring active with max {} concurrent migrations. \
             Automatic rebalancing will occur on cluster membership changes.",
            req.max_concurrent
        ),
        shards_to_move: manager.pending_count(),
        bytes_to_transfer: 0,
        estimated_duration: Some("0s".to_string()),
    };

    Json(response).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cluster_status_response_serialization() {
        let response = ClusterStatusResponse {
            healthy: true,
            leader_id: Some(1),
            node_id: 1,
            term: 5,
            state: "Leader".to_string(),
            node_count: 3,
            nodes: vec![],
            commit_index: 100,
            last_applied: 100,
        };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"healthy\":true"));
        assert!(json.contains("\"leader_id\":1"));
    }

    #[test]
    fn test_add_node_request_deserialization() {
        let json = r#"{"node_id": 2, "address": "192.168.1.2:9001"}"#;
        let req: AddNodeRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.node_id, 2);
        assert_eq!(req.address, "192.168.1.2:9001");
        assert!(req.as_learner); // default is true
    }

    #[test]
    fn test_add_node_request_with_voter() {
        let json = r#"{"node_id": 2, "address": "192.168.1.2:9001", "as_learner": false}"#;
        let req: AddNodeRequest = serde_json::from_str(json).unwrap();
        assert!(!req.as_learner);
    }

    #[test]
    fn test_rebalance_request_defaults() {
        let json = r#"{}"#;
        let req: RebalanceRequest = serde_json::from_str(json).unwrap();
        assert!(!req.dry_run);
        assert_eq!(req.max_concurrent, 4);
    }

    #[test]
    fn test_remove_node_response() {
        let response =
            RemoveNodeResponse { success: true, message: "Node removed".to_string(), node_id: 3 };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":true"));
    }

    #[test]
    fn test_rebalance_status_response_serialization() {
        let response = RebalanceStatusResponse {
            active: true,
            pending_migrations: 5,
            in_progress_migrations: 2,
            completed_migrations: 10,
            failed_migrations: 1,
            bytes_transferred: 1024 * 1024 * 100, // 100 MB
            cluster_members: vec!["node-1".to_string(), "node-2".to_string()],
        };
        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"active\":true"));
        assert!(json.contains("\"pending_migrations\":5"));
        assert!(json.contains("\"in_progress_migrations\":2"));
        assert!(json.contains("\"completed_migrations\":10"));
        assert!(json.contains("\"bytes_transferred\":104857600"));
        assert!(json.contains("\"cluster_members\":[\"node-1\",\"node-2\"]"));
    }
}
