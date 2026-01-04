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
    /// Print version information.
    Version,
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
}
