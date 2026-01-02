//! Rucket: A high-performance, S3-compatible object storage server.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use axum_server::tls_rustls::RustlsConfig;
use clap::Parser;
use metrics_exporter_prometheus::PrometheusBuilder;
use rucket_api::create_router;
use rucket_api::metrics::init_metrics;
use rucket_core::config::{Config, LogFormat};
use rucket_storage::metrics::{init_storage_metrics, start_storage_metrics_collector};
use rucket_storage::LocalStorage;
use tokio::net::TcpListener;
use tokio::signal;
use tracing::info;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

mod cli;

use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> Result<()> {
    // Install the default rustls crypto provider (ring)
    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install rustls crypto provider");

    let cli = Cli::parse();

    match cli.command {
        Commands::Serve(args) => run_server(args).await,
        Commands::Version => {
            println!("rucket {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
    }
}

async fn run_server(args: cli::ServeArgs) -> Result<()> {
    // Load configuration
    let config = load_config(&args.config)?;

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

    // Create storage backend
    let data_dir = config.storage.data_dir.clone();
    let temp_dir = config.storage.temp_dir();

    let storage = LocalStorage::new(data_dir, temp_dir)
        .await
        .context("Failed to initialize storage backend")?;

    let storage = Arc::new(storage);

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
    let app = create_router(
        storage,
        config.server.max_body_size,
        config.api.compatibility_mode,
        config.logging.log_requests,
        Some(&config.auth),
    );

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
