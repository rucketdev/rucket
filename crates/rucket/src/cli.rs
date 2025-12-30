// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Command line interface definition.

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
