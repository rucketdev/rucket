//! Cluster CLI command handlers.
//!
//! This module implements the handlers for cluster management commands.

use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::cli::{
    AddNodeArgs, ClusterStatusArgs, ListNodesArgs, OutputFormat, RebalanceArgs, RemoveNodeArgs,
};

/// Cluster status response from the admin API.
#[derive(Debug, Serialize, Deserialize)]
pub struct ClusterStatus {
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
    pub nodes: Vec<NodeInfo>,
    /// Commit index.
    pub commit_index: u64,
    /// Last applied index.
    pub last_applied: u64,
}

/// Information about a cluster node.
#[derive(Debug, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Node ID.
    pub node_id: u64,
    /// Node address.
    pub address: String,
    /// Node state (Voter, Learner).
    pub role: String,
    /// Whether the node is reachable.
    pub reachable: bool,
    /// Last heartbeat timestamp (if available).
    pub last_heartbeat: Option<String>,
}

/// Response from add-node operation.
#[derive(Debug, Serialize, Deserialize)]
pub struct AddNodeResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Message describing the result.
    pub message: String,
    /// The node that was added.
    pub node_id: u64,
}

/// Response from remove-node operation.
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoveNodeResponse {
    /// Whether the operation succeeded.
    pub success: bool,
    /// Message describing the result.
    pub message: String,
    /// The node that was removed.
    pub node_id: u64,
}

/// Response from rebalance operation.
#[derive(Debug, Serialize, Deserialize)]
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
#[derive(Debug, Serialize, Deserialize)]
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

/// Create an HTTP client for cluster operations.
fn create_client() -> Result<Client> {
    Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .context("Failed to create HTTP client")
}

/// Handle the cluster status command.
pub async fn handle_cluster_status(args: ClusterStatusArgs) -> Result<()> {
    let client = create_client()?;
    let url = format!("{}/_cluster/status", args.endpoint.trim_end_matches('/'));

    let response =
        client.get(&url).send().await.context("Failed to connect to cluster endpoint")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Cluster status request failed ({}): {}", status, body);
    }

    let status: ClusterStatus =
        response.json().await.context("Failed to parse cluster status response")?;

    match args.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        OutputFormat::Text => {
            print_cluster_status(&status);
        }
    }

    Ok(())
}

/// Print cluster status in human-readable format.
fn print_cluster_status(status: &ClusterStatus) {
    let health_icon = if status.healthy { "✓" } else { "✗" };
    let health_status = if status.healthy { "HEALTHY" } else { "UNHEALTHY" };

    println!("\n  Cluster Status: {} {}", health_icon, health_status);
    println!("  ─────────────────────────────────────────────");
    println!("  Node ID:        {}", status.node_id);
    println!("  State:          {}", status.state);
    println!(
        "  Leader:         {}",
        status.leader_id.map(|id| id.to_string()).unwrap_or_else(|| "none".to_string())
    );
    println!("  Term:           {}", status.term);
    println!("  Commit Index:   {}", status.commit_index);
    println!("  Last Applied:   {}", status.last_applied);
    println!("  Node Count:     {}", status.node_count);
    println!();

    if !status.nodes.is_empty() {
        println!("  Nodes:");
        println!("  ─────────────────────────────────────────────");
        println!("  {:>6}  {:>8}  {:>10}  {:>10}", "ID", "ROLE", "REACHABLE", "ADDRESS");
        for node in &status.nodes {
            let reachable = if node.reachable { "yes" } else { "no" };
            println!(
                "  {:>6}  {:>8}  {:>10}  {}",
                node.node_id, node.role, reachable, node.address
            );
        }
    }
    println!();
}

/// Handle the cluster add-node command.
pub async fn handle_add_node(args: AddNodeArgs) -> Result<()> {
    let client = create_client()?;
    let url = format!("{}/_cluster/nodes", args.endpoint.trim_end_matches('/'));

    let body = serde_json::json!({
        "node_id": args.node_id,
        "address": args.address.to_string(),
        "as_learner": args.as_learner,
    });

    let response = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .context("Failed to connect to cluster endpoint")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Add node request failed ({}): {}", status, body);
    }

    let result: AddNodeResponse =
        response.json().await.context("Failed to parse add-node response")?;

    if result.success {
        println!("✓ Node {} added successfully", args.node_id);
        if args.as_learner {
            println!("  Node was added as a learner.");
            println!(
                "  To promote to voter, run: rucket cluster promote-node --node-id {}",
                args.node_id
            );
        }
    } else {
        println!("✗ Failed to add node: {}", result.message);
    }

    Ok(())
}

/// Handle the cluster remove-node command.
pub async fn handle_remove_node(args: RemoveNodeArgs) -> Result<()> {
    let client = create_client()?;
    let url = format!("{}/_cluster/nodes/{}", args.endpoint.trim_end_matches('/'), args.node_id);

    let mut request = client.delete(&url);
    if args.force {
        request = request.query(&[("force", "true")]);
    }

    let response = request.send().await.context("Failed to connect to cluster endpoint")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Remove node request failed ({}): {}", status, body);
    }

    let result: RemoveNodeResponse =
        response.json().await.context("Failed to parse remove-node response")?;

    if result.success {
        println!("✓ Node {} removed successfully", args.node_id);
    } else {
        println!("✗ Failed to remove node: {}", result.message);
    }

    Ok(())
}

/// Handle the cluster rebalance command.
pub async fn handle_rebalance(args: RebalanceArgs) -> Result<()> {
    let client = create_client()?;
    let url = format!("{}/_cluster/rebalance", args.endpoint.trim_end_matches('/'));

    // If --status flag is set, GET the status instead of triggering rebalance
    if args.status {
        let response =
            client.get(&url).send().await.context("Failed to connect to cluster endpoint")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("Rebalance status request failed ({}): {}", status, body);
        }

        let status: RebalanceStatusResponse =
            response.json().await.context("Failed to parse rebalance status response")?;

        print_rebalance_status(&status);
        return Ok(());
    }

    let body = serde_json::json!({
        "dry_run": args.dry_run,
        "max_concurrent": args.max_concurrent,
    });

    let response = client
        .post(&url)
        .json(&body)
        .send()
        .await
        .context("Failed to connect to cluster endpoint")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("Rebalance request failed ({}): {}", status, body);
    }

    let result: RebalanceResponse =
        response.json().await.context("Failed to parse rebalance response")?;

    if args.dry_run {
        println!("\n  Rebalance Plan (dry run):");
        println!("  ─────────────────────────────────────────────");
        println!("  Shards to move:     {}", result.shards_to_move);
        println!("  Bytes to transfer:  {}", format_bytes(result.bytes_to_transfer));
        if let Some(duration) = &result.estimated_duration {
            println!("  Estimated time:     {}", duration);
        }
        println!();
        println!("  To execute, run without --dry-run");
    } else if result.initiated {
        println!("✓ Rebalance initiated");
        println!("  {} shards will be moved", result.shards_to_move);
        println!("  Use 'rucket cluster rebalance --status' to monitor progress");
    } else {
        println!("✗ Failed to initiate rebalance: {}", result.message);
    }

    Ok(())
}

/// Print rebalance status in human-readable format.
fn print_rebalance_status(status: &RebalanceStatusResponse) {
    let status_icon = if status.active { "●" } else { "○" };
    let status_text = if status.active { "ACTIVE" } else { "IDLE" };

    println!("\n  Rebalance Status: {} {}", status_icon, status_text);
    println!("  ─────────────────────────────────────────────");
    println!("  Pending:          {}", status.pending_migrations);
    println!("  In Progress:      {}", status.in_progress_migrations);
    println!("  Completed:        {}", status.completed_migrations);
    println!("  Failed:           {}", status.failed_migrations);
    println!("  Bytes Transferred:{}", format_bytes(status.bytes_transferred));
    println!();

    if !status.cluster_members.is_empty() {
        println!("  Known Members ({}):", status.cluster_members.len());
        println!("  ─────────────────────────────────────────────");
        for member in &status.cluster_members {
            println!("    - {}", member);
        }
    } else {
        println!("  No cluster members known to rebalance manager.");
    }
    println!();
}

/// Handle the cluster list-nodes command.
pub async fn handle_list_nodes(args: ListNodesArgs) -> Result<()> {
    let client = create_client()?;
    let url = format!("{}/_cluster/nodes", args.endpoint.trim_end_matches('/'));

    let response =
        client.get(&url).send().await.context("Failed to connect to cluster endpoint")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("List nodes request failed ({}): {}", status, body);
    }

    let nodes: Vec<NodeInfo> =
        response.json().await.context("Failed to parse list-nodes response")?;

    match args.format {
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(&nodes)?);
        }
        OutputFormat::Text => {
            print_node_list(&nodes);
        }
    }

    Ok(())
}

/// Print node list in human-readable format.
fn print_node_list(nodes: &[NodeInfo]) {
    if nodes.is_empty() {
        println!("\n  No nodes found in the cluster.\n");
        return;
    }

    println!("\n  Cluster Nodes ({} total):", nodes.len());
    println!("  ─────────────────────────────────────────────────────");
    println!("  {:>6}  {:>8}  {:>10}  ADDRESS", "ID", "ROLE", "REACHABLE");
    println!("  ─────────────────────────────────────────────────────");

    for node in nodes {
        let reachable = if node.reachable { "yes" } else { "no" };
        println!("  {:>6}  {:>8}  {:>10}  {}", node.node_id, node.role, reachable, node.address);
    }
    println!();
}

/// Format bytes in human-readable format.
fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;
    const TB: u64 = GB * 1024;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 bytes");
        assert_eq!(format_bytes(512), "512 bytes");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
        assert_eq!(format_bytes(1099511627776), "1.00 TB");
    }

    #[test]
    fn test_cluster_status_serialization() {
        let status = ClusterStatus {
            healthy: true,
            leader_id: Some(1),
            node_id: 1,
            term: 5,
            state: "Leader".to_string(),
            node_count: 3,
            nodes: vec![NodeInfo {
                node_id: 1,
                address: "127.0.0.1:9001".to_string(),
                role: "Voter".to_string(),
                reachable: true,
                last_heartbeat: None,
            }],
            commit_index: 100,
            last_applied: 100,
        };

        let json = serde_json::to_string(&status).unwrap();
        let parsed: ClusterStatus = serde_json::from_str(&json).unwrap();

        assert!(parsed.healthy);
        assert_eq!(parsed.leader_id, Some(1));
        assert_eq!(parsed.node_count, 3);
    }

    #[test]
    fn test_node_info_serialization() {
        let node = NodeInfo {
            node_id: 2,
            address: "192.168.1.2:9001".to_string(),
            role: "Learner".to_string(),
            reachable: false,
            last_heartbeat: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let json = serde_json::to_string(&node).unwrap();
        let parsed: NodeInfo = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.node_id, 2);
        assert_eq!(parsed.role, "Learner");
        assert!(!parsed.reachable);
    }

    #[test]
    fn test_add_node_response() {
        let response =
            AddNodeResponse { success: true, message: "Node added".to_string(), node_id: 3 };

        let json = serde_json::to_string(&response).unwrap();
        assert!(json.contains("\"success\":true"));
    }

    #[test]
    fn test_rebalance_response() {
        let response = RebalanceResponse {
            initiated: true,
            message: "Rebalance started".to_string(),
            shards_to_move: 100,
            bytes_to_transfer: 1073741824,
            estimated_duration: Some("2 hours".to_string()),
        };

        let json = serde_json::to_string(&response).unwrap();
        let parsed: RebalanceResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.shards_to_move, 100);
    }

    #[test]
    fn test_rebalance_status_response() {
        let response = RebalanceStatusResponse {
            active: true,
            pending_migrations: 5,
            in_progress_migrations: 2,
            completed_migrations: 10,
            failed_migrations: 1,
            bytes_transferred: 1073741824,
            cluster_members: vec!["node-1".to_string(), "node-2".to_string()],
        };

        let json = serde_json::to_string(&response).unwrap();
        let parsed: RebalanceStatusResponse = serde_json::from_str(&json).unwrap();

        assert!(parsed.active);
        assert_eq!(parsed.pending_migrations, 5);
        assert_eq!(parsed.in_progress_migrations, 2);
        assert_eq!(parsed.completed_migrations, 10);
        assert_eq!(parsed.failed_migrations, 1);
        assert_eq!(parsed.bytes_transferred, 1073741824);
        assert_eq!(parsed.cluster_members.len(), 2);
    }
}
