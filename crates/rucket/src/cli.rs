//! Command line interface definition.

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// Rucket: A high-performance, S3-compatible object storage server.
#[derive(Parser)]
#[command(name = "rucket")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Command to execute.
    #[command(subcommand)]
    pub command: Commands,
}

/// Available commands.
#[derive(Subcommand)]
pub enum Commands {
    /// Start the server.
    Serve(ServeArgs),
    /// Cluster management commands.
    Cluster(ClusterCommand),
    /// Print version information.
    Version,
}

/// Cluster management commands.
#[derive(Args)]
pub struct ClusterCommand {
    /// Cluster subcommand.
    #[command(subcommand)]
    pub command: ClusterSubcommand,
}

/// Cluster subcommands.
#[derive(Subcommand)]
pub enum ClusterSubcommand {
    /// Show cluster status.
    Status(ClusterStatusArgs),
    /// Add a node to the cluster.
    AddNode(AddNodeArgs),
    /// Remove a node from the cluster.
    RemoveNode(RemoveNodeArgs),
    /// Trigger a rebalance operation.
    Rebalance(RebalanceArgs),
    /// List all nodes in the cluster.
    ListNodes(ListNodesArgs),
}

/// Arguments for cluster status command.
#[derive(Args)]
pub struct ClusterStatusArgs {
    /// Address of the cluster admin endpoint.
    #[arg(short, long, default_value = "http://127.0.0.1:9000")]
    pub endpoint: String,

    /// Output format (text, json).
    #[arg(short, long, default_value = "text")]
    pub format: OutputFormat,
}

/// Arguments for add-node command.
#[derive(Args)]
pub struct AddNodeArgs {
    /// Address of the cluster admin endpoint.
    #[arg(short, long, default_value = "http://127.0.0.1:9000")]
    pub endpoint: String,

    /// Node ID to add.
    #[arg(short = 'i', long)]
    pub node_id: u64,

    /// Node address (host:port for Raft communication).
    #[arg(short, long)]
    pub address: SocketAddr,

    /// Add node as a learner first (recommended for safety).
    #[arg(long, default_value = "true")]
    pub as_learner: bool,
}

/// Arguments for remove-node command.
#[derive(Args)]
pub struct RemoveNodeArgs {
    /// Address of the cluster admin endpoint.
    #[arg(short, long, default_value = "http://127.0.0.1:9000")]
    pub endpoint: String,

    /// Node ID to remove.
    #[arg(short = 'i', long)]
    pub node_id: u64,

    /// Force removal even if node is unreachable.
    #[arg(long)]
    pub force: bool,
}

/// Arguments for rebalance command.
#[derive(Args)]
pub struct RebalanceArgs {
    /// Address of the cluster admin endpoint.
    #[arg(short, long, default_value = "http://127.0.0.1:9000")]
    pub endpoint: String,

    /// Only show what would be rebalanced (dry run).
    #[arg(long)]
    pub dry_run: bool,

    /// Maximum concurrent migrations.
    #[arg(long, default_value = "4")]
    pub max_concurrent: usize,
}

/// Arguments for list-nodes command.
#[derive(Args)]
pub struct ListNodesArgs {
    /// Address of the cluster admin endpoint.
    #[arg(short, long, default_value = "http://127.0.0.1:9000")]
    pub endpoint: String,

    /// Output format (text, json).
    #[arg(short, long, default_value = "text")]
    pub format: OutputFormat,
}

/// Output format for CLI commands.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, clap::ValueEnum)]
pub enum OutputFormat {
    /// Human-readable text output.
    #[default]
    Text,
    /// JSON output for scripting.
    Json,
}

/// Arguments for the serve command.
#[derive(Args)]
pub struct ServeArgs {
    /// Path to configuration file.
    #[arg(short, long)]
    pub config: Option<PathBuf>,

    /// Bind address (overrides config).
    #[arg(short, long)]
    pub bind: Option<String>,

    /// Data directory (overrides config).
    #[arg(short, long)]
    pub data_dir: Option<PathBuf>,

    // ========================================================================
    // Cluster options
    // ========================================================================
    /// Enable cluster mode.
    ///
    /// When enabled, this node will participate in a Raft cluster for
    /// distributed metadata operations.
    #[arg(long)]
    pub cluster: bool,

    /// Node ID for cluster mode.
    ///
    /// Must be unique across the cluster and stable across restarts.
    /// Defaults to 1.
    #[arg(long, default_value = "1")]
    pub node_id: u64,

    /// Bind address for Raft cluster communication.
    ///
    /// This is the address other nodes use to reach this node.
    /// Defaults to 127.0.0.1:9001.
    #[arg(long, default_value = "127.0.0.1:9001")]
    pub cluster_bind: SocketAddr,

    /// Peer addresses for cluster formation.
    ///
    /// Specify as comma-separated host:port pairs.
    /// Example: --peers node2:9001,node3:9001
    #[arg(long, value_delimiter = ',')]
    pub peers: Vec<String>,

    /// Bootstrap a new cluster.
    ///
    /// Only use this on the first node of a new cluster.
    /// Other nodes should start without this flag and be added via
    /// add_learner/change_membership from the leader.
    #[arg(long)]
    pub bootstrap: bool,

    /// Service discovery URI for automatic peer discovery.
    ///
    /// Formats:
    ///   - dns://hostname:port       - DNS A record discovery
    ///   - dns-srv://service.domain  - DNS SRV record discovery
    ///   - gossip://peer1:port,peer2:port - Gossip-based (SPOF-free)
    ///   - cloud://tag:value         - Auto-detect cloud (AWS/GCP/Azure)
    ///   - mdns://service-name       - mDNS for LAN discovery
    ///
    /// Example: --discover dns://rucket.local:9001
    /// Example: --discover gossip://seed1:9002,seed2:9002
    /// Example: --discover cloud://rucket:cluster=production
    #[arg(long)]
    pub discover: Option<String>,

    /// Expected number of nodes for bootstrap coordination.
    ///
    /// When set with --bootstrap, waits for this many nodes to be
    /// discovered before forming the cluster. Enables coordinated
    /// multi-node bootstrap.
    ///
    /// Example: --bootstrap --expect-nodes 3
    #[arg(long, default_value = "1")]
    pub expect_nodes: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_parsing() {
        // Test version command
        let cli = Cli::parse_from(["rucket", "version"]);
        assert!(matches!(cli.command, Commands::Version));

        // Test serve command with no args
        let cli = Cli::parse_from(["rucket", "serve"]);
        assert!(matches!(cli.command, Commands::Serve(_)));

        // Test serve command with config
        let cli = Cli::parse_from(["rucket", "serve", "--config", "/path/to/config.toml"]);
        if let Commands::Serve(args) = cli.command {
            assert_eq!(args.config, Some(PathBuf::from("/path/to/config.toml")));
        } else {
            panic!("Expected Serve command");
        }
    }

    #[test]
    fn test_cluster_status_parsing() {
        let cli = Cli::parse_from(["rucket", "cluster", "status"]);
        if let Commands::Cluster(cmd) = cli.command {
            assert!(matches!(cmd.command, ClusterSubcommand::Status(_)));
        } else {
            panic!("Expected Cluster command");
        }
    }

    #[test]
    fn test_cluster_add_node_parsing() {
        let cli = Cli::parse_from([
            "rucket",
            "cluster",
            "add-node",
            "--node-id",
            "2",
            "--address",
            "192.168.1.2:9001",
        ]);
        if let Commands::Cluster(cmd) = cli.command {
            if let ClusterSubcommand::AddNode(args) = cmd.command {
                assert_eq!(args.node_id, 2);
                assert_eq!(args.address.to_string(), "192.168.1.2:9001");
            } else {
                panic!("Expected AddNode subcommand");
            }
        } else {
            panic!("Expected Cluster command");
        }
    }

    #[test]
    fn test_cluster_remove_node_parsing() {
        let cli =
            Cli::parse_from(["rucket", "cluster", "remove-node", "--node-id", "3", "--force"]);
        if let Commands::Cluster(cmd) = cli.command {
            if let ClusterSubcommand::RemoveNode(args) = cmd.command {
                assert_eq!(args.node_id, 3);
                assert!(args.force);
            } else {
                panic!("Expected RemoveNode subcommand");
            }
        } else {
            panic!("Expected Cluster command");
        }
    }

    #[test]
    fn test_cluster_rebalance_parsing() {
        let cli = Cli::parse_from(["rucket", "cluster", "rebalance", "--dry-run"]);
        if let Commands::Cluster(cmd) = cli.command {
            if let ClusterSubcommand::Rebalance(args) = cmd.command {
                assert!(args.dry_run);
            } else {
                panic!("Expected Rebalance subcommand");
            }
        } else {
            panic!("Expected Cluster command");
        }
    }

    #[test]
    fn test_cluster_list_nodes_parsing() {
        let cli = Cli::parse_from(["rucket", "cluster", "list-nodes", "--format", "json"]);
        if let Commands::Cluster(cmd) = cli.command {
            if let ClusterSubcommand::ListNodes(args) = cmd.command {
                assert_eq!(args.format, OutputFormat::Json);
            } else {
                panic!("Expected ListNodes subcommand");
            }
        } else {
            panic!("Expected Cluster command");
        }
    }
}
