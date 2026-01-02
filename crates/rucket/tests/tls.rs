//! TLS Support Tests
//!
//! Tests for HTTPS/TLS functionality in Rucket.

use std::io::Write;
use std::net::SocketAddr;
use std::sync::{Arc, Once};
use std::time::Duration;

use axum_server::tls_rustls::RustlsConfig;
use rcgen::{generate_simple_self_signed, CertifiedKey};
use reqwest::ClientBuilder;
use rucket_api::create_router;
use rucket_core::config::ApiCompatibilityMode;
use rucket_storage::LocalStorage;
use tempfile::TempDir;

// Install rustls crypto provider once for all tests
static INIT: Once = Once::new();

fn init_crypto_provider() {
    INIT.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

/// Generate self-signed certificate and key for testing.
fn generate_test_cert() -> CertifiedKey {
    let subject_alt_names = vec!["localhost".to_string(), "127.0.0.1".to_string()];
    generate_simple_self_signed(subject_alt_names).expect("Failed to generate cert")
}

/// Write certificate and key to temporary files.
fn write_cert_files(
    temp_dir: &TempDir,
    cert_key: &CertifiedKey,
) -> (std::path::PathBuf, std::path::PathBuf) {
    let cert_path = temp_dir.path().join("cert.pem");
    let key_path = temp_dir.path().join("key.pem");

    let mut cert_file = std::fs::File::create(&cert_path).expect("Failed to create cert file");
    cert_file.write_all(cert_key.cert.pem().as_bytes()).expect("Failed to write cert");

    let mut key_file = std::fs::File::create(&key_path).expect("Failed to create key file");
    key_file.write_all(cert_key.key_pair.serialize_pem().as_bytes()).expect("Failed to write key");

    (cert_path, key_path)
}

/// Start a TLS-enabled test server.
async fn start_tls_server(
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
    data_dir: std::path::PathBuf,
    tmp_dir: std::path::PathBuf,
) -> (SocketAddr, axum_server::Handle) {
    init_crypto_provider();

    let storage =
        LocalStorage::new_in_memory(data_dir, tmp_dir).await.expect("Failed to create storage");

    let app = create_router(
        Arc::new(storage),
        5 * 1024 * 1024 * 1024,
        ApiCompatibilityMode::Minio,
        false,
    );

    let rustls_config =
        RustlsConfig::from_pem_file(cert_path, key_path).await.expect("Failed to load TLS config");

    // Bind to random port
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let handle = axum_server::Handle::new();
    let server_handle = handle.clone();

    let server_handle_clone = server_handle.clone();
    tokio::spawn(async move {
        axum_server::bind_rustls(addr, rustls_config)
            .handle(handle)
            .serve(app.into_make_service())
            .await
            .expect("Server error");
    });

    // Wait for server to start listening
    let bound_addr = server_handle_clone.listening().await.expect("Failed to get bound address");

    (bound_addr, server_handle)
}

#[tokio::test]
async fn test_tls_server_starts() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (cert_path, key_path) = write_cert_files(&temp_dir, &cert_key);

    let data_dir = temp_dir.path().join("data");
    let tmp_dir = temp_dir.path().join("tmp");

    let (addr, handle) = start_tls_server(&cert_path, &key_path, data_dir, tmp_dir).await;

    // Verify server is listening on HTTPS
    assert!(addr.port() > 0, "Server should be bound to a port");

    // Shutdown
    handle.graceful_shutdown(Some(Duration::from_secs(1)));
}

#[tokio::test]
async fn test_tls_connection_with_self_signed_cert() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (cert_path, key_path) = write_cert_files(&temp_dir, &cert_key);

    let data_dir = temp_dir.path().join("data");
    let tmp_dir = temp_dir.path().join("tmp");

    let (addr, handle) = start_tls_server(&cert_path, &key_path, data_dir, tmp_dir).await;

    // Create a client that accepts the self-signed cert
    let cert_pem = std::fs::read(&cert_path).expect("Failed to read cert");
    let cert = reqwest::Certificate::from_pem(&cert_pem).expect("Failed to parse cert");

    let client =
        ClientBuilder::new().add_root_certificate(cert).build().expect("Failed to build client");

    // Make a request to the TLS server
    let url = format!("https://127.0.0.1:{}/minio/health/live", addr.port());
    let response = client.get(&url).send().await.expect("Failed to send request");

    assert!(response.status().is_success(), "Health check should succeed");

    // Shutdown
    handle.graceful_shutdown(Some(Duration::from_secs(1)));
}

#[tokio::test]
async fn test_tls_rejects_without_valid_cert() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (cert_path, key_path) = write_cert_files(&temp_dir, &cert_key);

    let data_dir = temp_dir.path().join("data");
    let tmp_dir = temp_dir.path().join("tmp");

    let (addr, handle) = start_tls_server(&cert_path, &key_path, data_dir, tmp_dir).await;

    // Create a client WITHOUT the self-signed cert - should fail
    let client = ClientBuilder::new()
        .danger_accept_invalid_certs(false)
        .build()
        .expect("Failed to build client");

    let url = format!("https://127.0.0.1:{}/minio/health/live", addr.port());
    let result = client.get(&url).send().await;

    // Should fail due to certificate verification
    assert!(result.is_err(), "Connection should fail without valid cert");

    // Shutdown
    handle.graceful_shutdown(Some(Duration::from_secs(1)));
}

#[tokio::test]
async fn test_tls_s3_api_endpoint() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (cert_path, key_path) = write_cert_files(&temp_dir, &cert_key);

    let data_dir = temp_dir.path().join("data");
    let tmp_dir = temp_dir.path().join("tmp");

    let (addr, handle) = start_tls_server(&cert_path, &key_path, data_dir, tmp_dir).await;

    // Create a client that accepts the self-signed cert
    let cert_pem = std::fs::read(&cert_path).expect("Failed to read cert");
    let cert = reqwest::Certificate::from_pem(&cert_pem).expect("Failed to parse cert");

    let client =
        ClientBuilder::new().add_root_certificate(cert).build().expect("Failed to build client");

    // Test the S3 ListBuckets endpoint (returns XML)
    let url = format!("https://127.0.0.1:{}/", addr.port());
    let response = client
        .get(&url)
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=rucket/20240101/us-east-1/s3/aws4_request",
        )
        .send()
        .await
        .expect("Failed to send request");

    // Should get a response (even if auth fails, it proves TLS works)
    assert!(
        response.status().is_success() || response.status().is_client_error(),
        "Should get HTTP response over TLS"
    );

    // Shutdown
    handle.graceful_shutdown(Some(Duration::from_secs(1)));
}

#[tokio::test]
async fn test_rustls_config_from_pem_file() {
    init_crypto_provider();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (cert_path, key_path) = write_cert_files(&temp_dir, &cert_key);

    // Test that RustlsConfig loads successfully
    let config = RustlsConfig::from_pem_file(&cert_path, &key_path).await;
    assert!(config.is_ok(), "Should load valid PEM files");
}

#[tokio::test]
async fn test_rustls_config_invalid_cert_path() {
    init_crypto_provider();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (_, key_path) = write_cert_files(&temp_dir, &cert_key);

    let invalid_cert_path = temp_dir.path().join("nonexistent.pem");

    // Test that RustlsConfig fails with invalid cert path
    let config = RustlsConfig::from_pem_file(&invalid_cert_path, &key_path).await;
    assert!(config.is_err(), "Should fail with invalid cert path");
}

#[tokio::test]
async fn test_rustls_config_invalid_key_path() {
    init_crypto_provider();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (cert_path, _) = write_cert_files(&temp_dir, &cert_key);

    let invalid_key_path = temp_dir.path().join("nonexistent.pem");

    // Test that RustlsConfig fails with invalid key path
    let config = RustlsConfig::from_pem_file(&cert_path, &invalid_key_path).await;
    assert!(config.is_err(), "Should fail with invalid key path");
}

#[tokio::test]
async fn test_rustls_config_mismatched_cert_key() {
    init_crypto_provider();
    let temp_dir = TempDir::new().expect("Failed to create temp dir");

    // Generate two different certs
    let cert_key1 = generate_test_cert();
    let cert_key2 = generate_test_cert();

    let (cert_path, _) = write_cert_files(&temp_dir, &cert_key1);

    // Write second key to different path
    let key_path2 = temp_dir.path().join("key2.pem");
    let mut key_file = std::fs::File::create(&key_path2).expect("Failed to create key file");
    key_file.write_all(cert_key2.key_pair.serialize_pem().as_bytes()).expect("Failed to write key");

    // Test that RustlsConfig fails with mismatched cert/key
    let config = RustlsConfig::from_pem_file(&cert_path, &key_path2).await;
    assert!(config.is_err(), "Should fail with mismatched cert and key");
}

#[tokio::test]
async fn test_tls_multiple_requests() {
    let temp_dir = TempDir::new().expect("Failed to create temp dir");
    let cert_key = generate_test_cert();
    let (cert_path, key_path) = write_cert_files(&temp_dir, &cert_key);

    let data_dir = temp_dir.path().join("data");
    let tmp_dir = temp_dir.path().join("tmp");

    let (addr, handle) = start_tls_server(&cert_path, &key_path, data_dir, tmp_dir).await;

    // Create a client that accepts the self-signed cert
    let cert_pem = std::fs::read(&cert_path).expect("Failed to read cert");
    let cert = reqwest::Certificate::from_pem(&cert_pem).expect("Failed to parse cert");

    let client =
        ClientBuilder::new().add_root_certificate(cert).build().expect("Failed to build client");

    let url = format!("https://127.0.0.1:{}/minio/health/live", addr.port());

    // Make multiple requests to verify connection reuse works
    for i in 0..5 {
        let response =
            client.get(&url).send().await.unwrap_or_else(|_| panic!("Request {} failed", i));
        assert!(response.status().is_success(), "Request {} should succeed", i);
    }

    // Shutdown
    handle.graceful_shutdown(Some(Duration::from_secs(1)));
}
