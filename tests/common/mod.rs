// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Common test utilities for integration tests.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::Client;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use rucket_api::create_router;
use rucket_storage::LocalStorage;

/// A test server instance.
pub struct TestServer {
    /// The address the server is listening on.
    pub addr: SocketAddr,
    /// Handle to the server task.
    _handle: JoinHandle<()>,
    /// Shutdown signal sender.
    _shutdown_tx: oneshot::Sender<()>,
    /// Temporary directory for test data.
    _temp_dir: TempDir,
}

impl TestServer {
    /// Start a new test server.
    pub async fn start() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        let storage = LocalStorage::new_in_memory(data_dir, tmp_dir)
            .await
            .expect("Failed to create storage");

        let app = create_router(Arc::new(storage));

        // Bind to a random available port
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind");
        let addr = listener.local_addr().expect("Failed to get local addr");

        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("Server error");
        });

        // Give the server a moment to start
        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        Self {
            addr,
            _handle: handle,
            _shutdown_tx: shutdown_tx,
            _temp_dir: temp_dir,
        }
    }

    /// Get the endpoint URL for this server.
    pub fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Create an S3 client configured for this server.
    pub async fn client(&self) -> Client {
        create_s3_client(&self.endpoint()).await
    }
}

/// Create an S3 client configured for the given endpoint.
pub async fn create_s3_client(endpoint: &str) -> Client {
    let credentials = Credentials::new("rucket", "rucket123", None, None, "test");

    let config = aws_sdk_s3::Config::builder()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-east-1"))
        .endpoint_url(endpoint)
        .credentials_provider(credentials)
        .force_path_style(true)
        .build();

    Client::from_conf(config)
}
