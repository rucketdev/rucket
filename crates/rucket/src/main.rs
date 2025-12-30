// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Rucket: A high-performance, S3-compatible object storage server.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use rucket_api::create_router;
use rucket_core::config::{Config, LogFormat};
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

    // Print banner
    print_banner(&config);

    // Create storage backend
    let data_dir = config.storage.data_dir.clone();
    let temp_dir = config.storage.temp_dir();

    let storage = LocalStorage::new(data_dir, temp_dir)
        .await
        .context("Failed to initialize storage backend")?;

    let storage = Arc::new(storage);

    // Create router with body size limit
    let app = create_router(storage, config.server.max_body_size);

    // Bind to address
    let addr = config.server.bind;

    let listener = TcpListener::bind(addr).await.context("Failed to bind to address")?;

    info!("Server listening on {}", addr);
    println!("\n  Ready to accept connections.\n");

    // Run server with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .context("Server error")?;

    info!("Server shutdown complete");
    Ok(())
}

fn load_config(path: &Option<PathBuf>) -> Result<Config> {
    match path {
        Some(p) => {
            let content = std::fs::read_to_string(p)
                .with_context(|| format!("Failed to read config file: {}", p.display()))?;
            toml::from_str(&content).context("Failed to parse config file")
        }
        None => {
            // Try default locations
            let default_paths =
                [PathBuf::from("rucket.toml"), PathBuf::from("/etc/rucket/rucket.toml")];

            for p in &default_paths {
                if p.exists() {
                    let content = std::fs::read_to_string(p)?;
                    return toml::from_str(&content).context("Failed to parse config file");
                }
            }

            // Use defaults
            Ok(Config::default())
        }
    }
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
    println!(
        r#"
  ╦═╗╦ ╦╔═╗╦╔═╔═╗╔╦╗
  ╠╦╝║ ║║  ╠╩╗║╣  ║
  ╩╚═╚═╝╚═╝╩ ╩╚═╝ ╩  v{}

  Endpoint:    http://{}
  Access Key:  {}
  Secret Key:  {}
  Data Dir:    {}

  Test with:
    aws --endpoint-url http://{} s3 mb s3://my-bucket
    aws --endpoint-url http://{} s3 cp file.txt s3://my-bucket/
"#,
        env!("CARGO_PKG_VERSION"),
        config.server.bind,
        config.auth.access_key,
        mask_secret(&config.auth.secret_key),
        config.storage.data_dir.display(),
        config.server.bind,
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
