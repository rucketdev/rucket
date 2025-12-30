// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Rucket using aws-sdk-s3.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use rucket_api::create_router;
use rucket_storage::LocalStorage;

/// A test server instance.
struct TestServer {
    addr: SocketAddr,
    _handle: JoinHandle<()>,
    _shutdown_tx: oneshot::Sender<()>,
    _temp_dir: TempDir,
}

impl TestServer {
    async fn start() -> Self {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let data_dir = temp_dir.path().join("data");
        let tmp_dir = temp_dir.path().join("tmp");

        let storage = LocalStorage::new_in_memory(data_dir, tmp_dir)
            .await
            .expect("Failed to create storage");

        let app = create_router(Arc::new(storage));

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

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

        Self {
            addr,
            _handle: handle,
            _shutdown_tx: shutdown_tx,
            _temp_dir: temp_dir,
        }
    }

    fn endpoint(&self) -> String {
        format!("http://{}", self.addr)
    }

    async fn client(&self) -> Client {
        let credentials = Credentials::new("rucket", "rucket123", None, None, "test");

        let config = aws_sdk_s3::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .endpoint_url(self.endpoint())
            .credentials_provider(credentials)
            .force_path_style(true)
            .build();

        Client::from_conf(config)
    }
}

#[tokio::test]
async fn test_create_and_delete_bucket() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Failed to create bucket");

    client
        .head_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Bucket should exist");

    client
        .delete_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Failed to delete bucket");

    let result = client.head_bucket().bucket("test-bucket").send().await;
    assert!(result.is_err(), "Bucket should not exist after deletion");
}

#[tokio::test]
async fn test_list_buckets() {
    let server = TestServer::start().await;
    let client = server.client().await;

    for name in &["bucket-a", "bucket-b", "bucket-c"] {
        client
            .create_bucket()
            .bucket(*name)
            .send()
            .await
            .expect("Failed to create bucket");
    }

    let response = client
        .list_buckets()
        .send()
        .await
        .expect("Failed to list buckets");

    let buckets = response.buckets();
    assert_eq!(buckets.len(), 3);

    let names: Vec<_> = buckets.iter().filter_map(|b| b.name()).collect();
    assert!(names.contains(&"bucket-a"));
    assert!(names.contains(&"bucket-b"));
    assert!(names.contains(&"bucket-c"));
}

#[tokio::test]
async fn test_put_and_get_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Failed to create bucket");

    let body = ByteStream::from_static(b"Hello, World!");
    client
        .put_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .body(body)
        .content_type("text/plain")
        .send()
        .await
        .expect("Failed to put object");

    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .send()
        .await
        .expect("Failed to get object");

    let content_type = response.content_type().map(String::from);
    let body = response
        .body
        .collect()
        .await
        .expect("Failed to read body")
        .into_bytes();

    assert_eq!(body.as_ref(), b"Hello, World!");
    assert_eq!(content_type.as_deref(), Some("text/plain"));
}

#[tokio::test]
async fn test_head_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    let body = ByteStream::from_static(b"Test content for head");
    client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    let response = client
        .head_object()
        .bucket("test-bucket")
        .key("test.txt")
        .send()
        .await
        .expect("Failed to head object");

    assert_eq!(response.content_length(), Some(21));
    assert!(response.e_tag().is_some());
}

#[tokio::test]
async fn test_delete_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    let body = ByteStream::from_static(b"To be deleted");
    client
        .put_object()
        .bucket("test-bucket")
        .key("delete-me.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket("test-bucket")
        .key("delete-me.txt")
        .send()
        .await
        .expect("Failed to delete object");

    let result = client
        .head_object()
        .bucket("test-bucket")
        .key("delete-me.txt")
        .send()
        .await;

    assert!(result.is_err(), "Object should not exist after deletion");
}

#[tokio::test]
async fn test_list_objects_v2() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    for i in 0..5 {
        let body = ByteStream::from_static(b"content");
        client
            .put_object()
            .bucket("test-bucket")
            .key(format!("file{i}.txt"))
            .body(body)
            .send()
            .await
            .unwrap();
    }

    let response = client
        .list_objects_v2()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Failed to list objects");

    let contents = response.contents();
    assert_eq!(contents.len(), 5);
}

#[tokio::test]
async fn test_list_objects_with_prefix() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    for key in &["docs/readme.md", "docs/guide.md", "src/main.rs", "src/lib.rs"] {
        let body = ByteStream::from_static(b"content");
        client
            .put_object()
            .bucket("test-bucket")
            .key(*key)
            .body(body)
            .send()
            .await
            .unwrap();
    }

    let response = client
        .list_objects_v2()
        .bucket("test-bucket")
        .prefix("docs/")
        .send()
        .await
        .expect("Failed to list objects");

    let contents = response.contents();
    assert_eq!(contents.len(), 2);

    for obj in contents {
        assert!(obj.key().unwrap().starts_with("docs/"));
    }
}

#[tokio::test]
async fn test_copy_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("source-bucket")
        .send()
        .await
        .unwrap();

    client
        .create_bucket()
        .bucket("dest-bucket")
        .send()
        .await
        .unwrap();

    let body = ByteStream::from_static(b"Original content");
    client
        .put_object()
        .bucket("source-bucket")
        .key("original.txt")
        .body(body)
        .content_type("text/plain")
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket("dest-bucket")
        .key("copy.txt")
        .copy_source("source-bucket/original.txt")
        .send()
        .await
        .expect("Failed to copy object");

    let response = client
        .get_object()
        .bucket("dest-bucket")
        .key("copy.txt")
        .send()
        .await
        .expect("Failed to get copied object");

    let body = response.body.collect().await.unwrap().into_bytes();

    assert_eq!(body.as_ref(), b"Original content");
}

#[tokio::test]
async fn test_range_request() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    let body = ByteStream::from_static(b"Hello, World!");
    client
        .put_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Failed to get object range");

    let body = response.body.collect().await.unwrap().into_bytes();

    assert_eq!(body.as_ref(), b"Hello");
}

#[tokio::test]
async fn test_overwrite_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    let body = ByteStream::from_static(b"Version 1");
    client
        .put_object()
        .bucket("test-bucket")
        .key("file.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    let body = ByteStream::from_static(b"Version 2");
    client
        .put_object()
        .bucket("test-bucket")
        .key("file.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("file.txt")
        .send()
        .await
        .unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();

    assert_eq!(body.as_ref(), b"Version 2");
}

#[tokio::test]
async fn test_nonexistent_bucket() {
    let server = TestServer::start().await;
    let client = server.client().await;

    let result = client
        .get_object()
        .bucket("nonexistent-bucket")
        .key("file.txt")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_nonexistent_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket("test-bucket")
        .key("nonexistent.txt")
        .send()
        .await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_large_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    // Create a 1MB object
    let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
    let body = ByteStream::from(data.clone());

    client
        .put_object()
        .bucket("test-bucket")
        .key("large.bin")
        .body(body)
        .send()
        .await
        .expect("Failed to put large object");

    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("large.bin")
        .send()
        .await
        .expect("Failed to get large object");

    let body = response.body.collect().await.unwrap().into_bytes();

    assert_eq!(body.len(), 1024 * 1024);
    assert_eq!(body.as_ref(), data.as_slice());
}

#[tokio::test]
async fn test_special_characters_in_key() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    // Test keys with special characters
    let keys = [
        "path/to/file.txt",
        "file with spaces.txt",
        "file-with-dashes.txt",
        "file_with_underscores.txt",
    ];

    for key in &keys {
        let body = ByteStream::from_static(b"content");
        client
            .put_object()
            .bucket("test-bucket")
            .key(*key)
            .body(body)
            .send()
            .await
            .unwrap_or_else(|_| panic!("Failed to put object with key: {}", key));

        let response = client
            .get_object()
            .bucket("test-bucket")
            .key(*key)
            .send()
            .await
            .unwrap_or_else(|_| panic!("Failed to get object with key: {}", key));

        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(body.as_ref(), b"content");
    }
}
