//! Rucket: A high-performance, S3-compatible object storage server.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use rucket_api::metrics::init_metrics;
use rucket_api::{create_admin_router, create_router, AdminState};
use rucket_cluster::{RebalanceConfig, RebalanceManager};
use rucket_consensus::{
    CloudConfig, CloudDiscovery, ClusterManager, DiscoveredPeer, Discovery, DiscoveryError,
    DiscoveryManager, DiscoveryManagerConfig, DnsDiscovery, MetadataStateMachine,
    RaftMetadataBackend, RedbLogStorage, StaticDiscovery,
};
use rucket_core::config::{BootstrapConfig, Config, LogFormat, SyncConfig, WalConfig};
use rucket_storage::metadata::MetadataBackend;
use rucket_storage::metrics::{init_storage_metrics, start_storage_metrics_collector};
use rucket_storage::{LocalStorage, RedbMetadataStore};
use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod cli;
mod cluster_cli;

use cli::{Cli, ClusterSubcommand, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    // Install the default rustls crypto provider (ring)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve(args) => run_server(args).await,
        Commands::Cluster(cmd) => run_cluster_command(cmd.command).await,
        Commands::Version => {
            println!("rucket {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

async fn run_cluster_command(cmd: ClusterSubcommand) -> Result<()> {
    match cmd {
        ClusterSubcommand::Status(args) => cluster_cli::handle_cluster_status(args).await,
        ClusterSubcommand::AddNode(args) => cluster_cli::handle_add_node(args).await,
        ClusterSubcommand::RemoveNode(args) => cluster_cli::handle_remove_node(args).await,
        ClusterSubcommand::Rebalance(args) => cluster_cli::handle_rebalance(args).await,
        ClusterSubcommand::ListNodes(args) => cluster_cli::handle_list_nodes(args).await,
    }
}

async fn run_server(args: cli::ServeArgs) -> Result<()> {
    // Load configuration and apply CLI overrides
    let mut config = load_config(&args.config)?;

    // Apply cluster CLI overrides
    if args.cluster {
        config.cluster.enabled = true;
        config.cluster.node_id = args.node_id;
        config.cluster.bind_cluster = args.cluster_bind;
        config.cluster.peers = args.peers.clone();
        if args.bootstrap {
            config.cluster.bootstrap = Some(BootstrapConfig {
                enabled: true,
                expect_nodes: 1, // Single-node bootstrap for now
                timeout_secs: 60,
            });
        }
    }

    // Initialize logging
    init_logging(&config)?;

    // Initialize metrics if enabled
    if config.metrics.enabled {
        init_metrics();
        init_storage_metrics();

        // Start Prometheus exporter on separate port
        let metrics_addr: std::net::SocketAddr =
            format!("{}:{}", config.metrics.bind, config.metrics.port)
                .parse()
                .context("Invalid metrics bind address")?;

        PrometheusBuilder::new()
            .with_http_listener(metrics_addr)
            .install()
            .context("Failed to install Prometheus exporter")?;

        info!("Metrics endpoint listening on http://{}/metrics", metrics_addr);
    }

    // Print banner
    print_banner(&config);

    // Create storage backend and optionally initialize cluster
    let data_dir = config.storage.data_dir.clone();
    let temp_dir = config.storage.temp_dir();

    let (storage, _cluster_manager, _discovery_manager) = if config.cluster.enabled {
        // Cluster mode: Create shared metadata backend, then wire up Raft
        info!(
            node_id = config.cluster.node_id,
            bind_addr = %config.cluster.bind_cluster,
            "Initializing cluster mode"
        );

        // Create directories
        tokio::fs::create_dir_all(&data_dir).await.context("Failed to create data directory")?;
        std::fs::create_dir_all(&config.cluster.raft_log_dir)
            .context("Failed to create Raft log directory")?;

        // Create the underlying metadata store (shared between state machine and RaftMetadataBackend)
        let metadata_path = data_dir.join("metadata.redb");
        let redb_store = Arc::new(
            RedbMetadataStore::open(&metadata_path, config.storage.sync.metadata)
                .context("Failed to open metadata store")?,
        );

        // Open Raft log storage
        let log_path = config.cluster.raft_log_dir.join("raft_log.db");
        let log_storage =
            RedbLogStorage::open(&log_path).context("Failed to open Raft log storage")?;
        log_storage.load_state().await.context("Failed to load Raft log state")?;

        // Create state machine with the local metadata backend
        let state_machine =
            MetadataStateMachine::new(redb_store.clone() as Arc<dyn MetadataBackend>);

        // Create cluster manager
        let cluster_manager =
            ClusterManager::new(config.cluster.clone(), log_storage, state_machine)
                .await
                .context("Failed to create cluster manager")?;

        // Start gRPC server for Raft communication
        cluster_manager.start_rpc_server().await.context("Failed to start Raft gRPC server")?;

        // Initialize discovery backend
        let discovery = create_discovery_backend(
            args.discover.as_deref(),
            &args.peers,
            config.cluster.bind_cluster.port(),
            config.cluster.bind_cluster,
        )
        .await?;

        // Create discovery manager for bootstrap coordination
        let self_info =
            DiscoveredPeer::with_node_id(config.cluster.node_id, config.cluster.bind_cluster);
        let mut discovery_manager = DiscoveryManager::new(
            discovery,
            DiscoveryManagerConfig {
                expect_nodes: args.expect_nodes,
                bootstrap_timeout: Duration::from_secs(60),
                ..Default::default()
            },
        )
        .with_self_info(self_info);

        // Bootstrap or join cluster
        if config.cluster.should_bootstrap() {
            // If expecting multiple nodes, use discovery for coordinated bootstrap
            if args.expect_nodes > 1 {
                info!(
                    expect_nodes = args.expect_nodes,
                    "Waiting for peers before coordinated bootstrap"
                );
                let peers = discovery_manager
                    .wait_for_bootstrap_peers()
                    .await
                    .context("Failed to discover bootstrap peers")?;
                info!(
                    discovered = peers.len(),
                    expect = args.expect_nodes,
                    "Found expected peers, proceeding with bootstrap"
                );
            }

            cluster_manager.bootstrap().await.context("Failed to bootstrap cluster")?;

            // Wait for this node to become leader
            cluster_manager
                .wait_for_leader(Duration::from_secs(10))
                .await
                .context("Timeout waiting to become leader after bootstrap")?;

            info!("Cluster bootstrapped, this node is the leader");
        } else if !args.peers.is_empty() || args.discover.is_some() {
            // Use discovery to find existing cluster
            if args.discover.is_some() {
                info!("Using discovery to find existing cluster");
                let peers = discovery_manager
                    .refresh()
                    .await
                    .context("Failed to discover cluster peers")?;
                if peers.is_empty() {
                    warn!("No peers found via discovery, waiting to be added manually");
                } else {
                    info!(peer_count = peers.len(), "Discovered existing cluster peers");
                }
            }

            cluster_manager.join_cluster().await.context("Failed to join cluster")?;

            // Wait for a leader to be elected
            let leader_id = cluster_manager
                .wait_for_any_leader(Duration::from_secs(30))
                .await
                .context("Timeout waiting for leader election")?;

            info!(leader_id = leader_id, "Joined cluster, leader found");
        } else {
            warn!(
                "Cluster mode enabled but no --bootstrap flag and no peers configured. \
                 This node will wait to be added to an existing cluster."
            );
        }

        // Start background discovery (continues monitoring for new peers)
        discovery_manager
            .start(Some(cluster_manager.raft()))
            .await
            .context("Failed to start discovery manager")?;

        // Create RaftMetadataBackend that routes writes through Raft consensus
        let raft_backend: Arc<dyn MetadataBackend> =
            Arc::new(RaftMetadataBackend::new(cluster_manager.raft(), redb_store));

        // Create LocalStorage with Raft-backed metadata
        let storage = LocalStorage::with_metadata_backend(
            raft_backend,
            data_dir,
            temp_dir,
            SyncConfig::default(),
            WalConfig::default(),
        )
        .await
        .context("Failed to initialize storage backend")?;

        // Configure encryption if enabled
        let storage = if config.storage.encryption.enabled {
            let master_key = config
                .storage
                .encryption
                .master_key
                .as_ref()
                .context("Encryption is enabled but master_key is not set. \
                         Set RUCKET__STORAGE__ENCRYPTION__MASTER_KEY or storage.encryption.master_key in config file. \
                         Generate a key with: openssl rand -hex 32")?;

            storage.with_encryption_hex(master_key).context(
                "Failed to configure encryption - ensure master_key is a valid 64-character hex string",
            )?
        } else {
            storage
        };

        info!("Cluster mode initialized - metadata operations replicated via Raft");
        (Arc::new(storage), Some(cluster_manager), Some(discovery_manager))
    } else {
        // Single-node mode: Create LocalStorage directly
        let storage = LocalStorage::new(data_dir, temp_dir)
            .await
            .context("Failed to initialize storage backend")?;

        // Configure encryption if enabled
        let storage = if config.storage.encryption.enabled {
            let master_key = config
                .storage
                .encryption
                .master_key
                .as_ref()
                .context("Encryption is enabled but master_key is not set. \
                         Set RUCKET__STORAGE__ENCRYPTION__MASTER_KEY or storage.encryption.master_key in config file. \
                         Generate a key with: openssl rand -hex 32")?;

            storage.with_encryption_hex(master_key).context(
                "Failed to configure encryption - ensure master_key is a valid 64-character hex string",
            )?
        } else {
            storage
        };

        (Arc::new(storage), None, None)
    };

    // Start storage metrics collector if enabled
    let _metrics_task = if config.metrics.enabled && config.metrics.include_storage_metrics {
        Some(start_storage_metrics_collector(
            Arc::clone(&storage),
            config.metrics.storage_metrics_interval_secs,
        ))
    } else {
        None
    };

    // Create router with body size limit and compatibility mode
    let s3_router = create_router(
        storage,
        config.server.max_body_size,
        config.api.compatibility_mode,
        config.logging.log_requests,
        Some(&config.auth),
    );

    // Merge admin router if cluster mode is enabled
    let app = if let Some(cluster_manager) = &_cluster_manager {
        // Create rebalance manager for shard redistribution
        let rebalance_config = RebalanceConfig::default();
        let rebalance_manager = RebalanceManager::new(rebalance_config);

        let admin_state = AdminState {
            raft: cluster_manager.raft(),
            node_id: config.cluster.node_id,
            rebalance_manager: Some(Arc::new(RwLock::new(rebalance_manager))),
        };
        let admin_router = create_admin_router(admin_state);
        s3_router.merge(admin_router)
    } else {
        s3_router
    };

    // Bind to address
    let addr = config.server.bind;

    // Check if TLS is configured
    let tls_config = match (&config.server.tls_cert, &config.server.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            let rustls_config =
                RustlsConfig::from_pem_file(cert_path, key_path).await.with_context(|| {
                    format!(
                        "Failed to load TLS certificates from {} and {}",
                        cert_path.display(),
                        key_path.display()
                    )
                })?;
            Some(rustls_config)
        }
        (Some(_), None) => {
            anyhow::bail!("TLS certificate specified but key is missing");
        }
        (None, Some(_)) => {
            anyhow::bail!("TLS key specified but certificate is missing");
        }
        (None, None) => None,
    };

    if let Some(tls_config) = tls_config {
        // Run with TLS
        info!("Server listening on https://{} (TLS enabled)", addr);
        println!("\n  Ready to accept connections (HTTPS).\n");

        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();

        // Spawn shutdown handler
        tokio::spawn(async move {
            shutdown_signal().await;
            shutdown_handle.graceful_shutdown(Some(std::time::Duration::from_secs(10)));
        });

        axum_server::bind_rustls(addr, tls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .context("Server error")?;
    } else {
        // Run without TLS
        let listener = TcpListener::bind(addr).await.context("Failed to bind to address")?;

        info!("Server listening on http://{}", addr);
        println!("\n  Ready to accept connections.\n");

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await
            .context("Server error")?;
    }

    info!("Server shutdown complete");
    Ok(())
}

fn load_config(path: &Option<PathBuf>) -> Result<Config> {
    Config::load(path.as_deref()).context("Failed to load configuration")
}

fn init_logging(config: &Config) -> Result<()> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&config.logging.level));

    let fmt_layer = tracing_subscriber::fmt::layer();

    match config.logging.format {
        LogFormat::Json => {
            tracing_subscriber::registry().with(filter).with(fmt_layer.json()).init();
        }
        LogFormat::Pretty => {
            tracing_subscriber::registry().with(filter).with(fmt_layer).init();
        }
    }

    Ok(())
}

fn print_banner(config: &Config) {
    let scheme = if config.server.tls_cert.is_some() { "https" } else { "http" };
    let cluster_status = if config.cluster.enabled {
        format!("enabled (node {}, {})", config.cluster.node_id, config.cluster.bind_cluster)
    } else {
        "disabled (single-node)".to_string()
    };

    println!(
        r#"
         ____             __        __
        / __ \__  _______/ /_____  / /_
       / /_/ / / / / ___/ //_/ _ \/ __/
      / _, _/ /_/ / /__/ ,< /  __/ /_
     /_/ |_|\__,_/\___/_/|_|\___/\__/

      S3-Compatible Object Storage  v{}

  Endpoint:    {}://{}
  Access Key:  {}
  Secret Key:  {}
  Data Dir:    {}
  TLS:         {}
  Encryption:  {}
  Cluster:     {}

  Test with:
    aws --endpoint-url {}://{} s3 mb s3://my-bucket
    aws --endpoint-url {}://{} s3 cp file.txt s3://my-bucket/
"#,
        env!("CARGO_PKG_VERSION"),
        scheme,
        config.server.bind,
        config.auth.access_key,
        mask_secret(&config.auth.secret_key),
        config.storage.data_dir.display(),
        if config.server.tls_cert.is_some() { "enabled" } else { "disabled" },
        if config.storage.encryption.enabled { "SSE-S3 (AES-256-GCM)" } else { "disabled" },
        cluster_status,
        scheme,
        config.server.bind,
        scheme,
        config.server.bind
    );
}

fn mask_secret(secret: &str) -> String {
    if secret.len() <= 4 {
        "*".repeat(secret.len())
    } else {
        format!("{}****", &secret[..4])
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Received Ctrl+C, initiating shutdown");
        }
        _ = terminate => {
            info!("Received SIGTERM, initiating shutdown");
        }
    }
}

/// Parses a discovery URI into a Discovery backend.
///
/// Supported formats:
/// - `dns://hostname:port` - DNS A record discovery
/// - `dns-srv://service.domain` - DNS SRV record discovery
/// - `cloud://tag:value` - Cloud auto-detect (AWS/GCP/Azure) with tag filter
/// - Static peers via `--peers` flag (handled separately)
///
/// # Errors
///
/// Returns an error if the URI format is invalid or the scheme is not supported.
async fn parse_discovery_uri(
    uri: &str,
    raft_port: u16,
) -> Result<Arc<dyn Discovery>, DiscoveryError> {
    // Parse the URI
    let uri = uri.trim();

    if uri.starts_with("dns-srv://") {
        // DNS SRV record discovery
        let service = uri.strip_prefix("dns-srv://").unwrap();
        if service.is_empty() {
            return Err(DiscoveryError::Config(
                "DNS SRV discovery requires a service name: dns-srv://service.domain".to_string(),
            ));
        }
        debug!(service = service, "Using DNS SRV discovery");
        Ok(Arc::new(DnsDiscovery::with_srv(service)))
    } else if uri.starts_with("dns://") {
        // DNS A record discovery
        let host_port = uri.strip_prefix("dns://").unwrap();
        if host_port.is_empty() {
            return Err(DiscoveryError::Config(
                "DNS discovery requires hostname: dns://hostname:port".to_string(),
            ));
        }

        // Parse hostname and optional port
        let (hostname, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let (h, p) = host_port.split_at(colon_pos);
            let port_str = &p[1..]; // Skip the colon
            let port: u16 = port_str.parse().map_err(|_| {
                DiscoveryError::Config(format!("Invalid port in DNS URI: {}", port_str))
            })?;
            (h.to_string(), port)
        } else {
            (host_port.to_string(), raft_port)
        };

        debug!(hostname = %hostname, port = port, "Using DNS A record discovery");
        Ok(Arc::new(DnsDiscovery::new(&hostname, port)))
    } else if uri.starts_with("cloud://") {
        // Cloud auto-detection with tag filter
        let tag_value = uri.strip_prefix("cloud://").unwrap();
        if tag_value.is_empty() {
            return Err(DiscoveryError::Config(
                "Cloud discovery requires tag:value: cloud://rucket:cluster=production".to_string(),
            ));
        }

        // Parse tag:value format (e.g., "rucket:cluster=production")
        let (cluster_tag, cluster_value) = if let Some(eq_pos) = tag_value.find('=') {
            let (tag, value) = tag_value.split_at(eq_pos);
            (tag.to_string(), value[1..].to_string())
        } else {
            // Default: use the whole string as tag, "default" as value
            (tag_value.to_string(), "default".to_string())
        };

        debug!(
            tag = %cluster_tag,
            value = %cluster_value,
            "Using cloud auto-detect discovery"
        );

        let config = CloudConfig { cluster_tag, cluster_value, raft_port, ..Default::default() };

        let discovery = CloudDiscovery::auto_detect(config).await?;
        Ok(Arc::new(discovery))
    } else {
        Err(DiscoveryError::Config(format!(
            "Unknown discovery URI scheme: {}. Supported: dns://, dns-srv://, cloud://",
            uri
        )))
    }
}

/// Creates a discovery backend from CLI arguments and configuration.
async fn create_discovery_backend(
    discover_uri: Option<&str>,
    peers: &[String],
    raft_port: u16,
    self_addr: std::net::SocketAddr,
) -> Result<Arc<dyn Discovery>> {
    let discovery: Arc<dyn Discovery> = if let Some(uri) = discover_uri {
        // Use discovery URI
        parse_discovery_uri(uri, raft_port).await.context("Failed to parse discovery URI")?
    } else if !peers.is_empty() {
        // Use static peer list
        Arc::new(
            StaticDiscovery::from_strings(peers)
                .context("Failed to parse peer addresses")?
                .with_self_addr(self_addr),
        )
    } else {
        // No discovery configured - return empty static discovery
        Arc::new(StaticDiscovery::new(vec![]))
    };

    info!(backend = discovery.name(), "Discovery backend initialized");
    Ok(discovery)
}
