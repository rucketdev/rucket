// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Integration tests for Rucket using aws-sdk-s3.

use std::net::SocketAddr;
use std::sync::Arc;

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Region;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;
use rucket_api::create_router;
use rucket_core::config::ApiCompatibilityMode;
use rucket_storage::LocalStorage;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

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

        let storage =
            LocalStorage::new_in_memory(data_dir, tmp_dir).await.expect("Failed to create storage");

        // 5 GiB max body size (S3 max single PUT)
        let app = create_router(
            Arc::new(storage),
            5 * 1024 * 1024 * 1024,
            ApiCompatibilityMode::Minio,
            false, // log_requests disabled for tests
        );

        let listener = TcpListener::bind("127.0.0.1:0").await.expect("Failed to bind");
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

        Self { addr, _handle: handle, _shutdown_tx: shutdown_tx, _temp_dir: temp_dir }
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

    client.create_bucket().bucket("test-bucket").send().await.expect("Failed to create bucket");

    client.head_bucket().bucket("test-bucket").send().await.expect("Bucket should exist");

    client.delete_bucket().bucket("test-bucket").send().await.expect("Failed to delete bucket");

    let result = client.head_bucket().bucket("test-bucket").send().await;
    assert!(result.is_err(), "Bucket should not exist after deletion");
}

#[tokio::test]
async fn test_list_buckets() {
    let server = TestServer::start().await;
    let client = server.client().await;

    for name in &["bucket-a", "bucket-b", "bucket-c"] {
        client.create_bucket().bucket(*name).send().await.expect("Failed to create bucket");
    }

    let response = client.list_buckets().send().await.expect("Failed to list buckets");

    let buckets = response.buckets();
    assert_eq!(buckets.len(), 3);

    let names: Vec<_> = buckets.iter().filter_map(|b| b.name()).collect();
    assert!(names.contains(&"bucket-a"));
    assert!(names.contains(&"bucket-b"));
    assert!(names.contains(&"bucket-c"));
}

// =============================================================================
// Conditional Request Tests (If-Match / If-None-Match)
// =============================================================================

#[tokio::test]
async fn test_if_none_match_star_prevents_overwrite() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put initial object
    let body = ByteStream::from_static(b"Initial content");
    client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Failed to put object");

    // Try to overwrite with If-None-Match: * (should fail - object exists)
    let body = ByteStream::from_static(b"New content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .if_none_match("*")
        .send()
        .await;

    assert!(result.is_err(), "Should fail with If-None-Match: * when object exists");

    // Verify original content is preserved
    let response = client.get_object().bucket("test-bucket").key("test.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Initial content");
}

#[tokio::test]
async fn test_if_none_match_star_allows_new_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put with If-None-Match: * (should succeed - object doesn't exist)
    let body = ByteStream::from_static(b"New content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("new-file.txt")
        .body(body)
        .if_none_match("*")
        .send()
        .await;

    assert!(result.is_ok(), "Should succeed with If-None-Match: * when object doesn't exist");

    // Verify content was written
    let response =
        client.get_object().bucket("test-bucket").key("new-file.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"New content");
}

#[tokio::test]
async fn test_if_none_match_specific_etag_prevents_overwrite() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put initial object
    let body = ByteStream::from_static(b"Initial content");
    let response = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Failed to put object");

    let etag = response.e_tag().expect("Should have ETag").to_string();

    // Try to overwrite with If-None-Match: <matching-etag> (should fail)
    let body = ByteStream::from_static(b"New content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .if_none_match(&etag)
        .send()
        .await;

    assert!(result.is_err(), "Should fail with If-None-Match when ETag matches");
}

#[tokio::test]
async fn test_if_none_match_different_etag_allows_overwrite() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put initial object
    let body = ByteStream::from_static(b"Initial content");
    client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Failed to put object");

    // Overwrite with If-None-Match: <different-etag> (should succeed)
    let body = ByteStream::from_static(b"New content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .if_none_match("\"different-etag-value\"")
        .send()
        .await;

    assert!(result.is_ok(), "Should succeed with If-None-Match when ETag differs");

    // Verify new content
    let response = client.get_object().bucket("test-bucket").key("test.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"New content");
}

#[tokio::test]
async fn test_if_match_correct_etag_succeeds() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put initial object
    let body = ByteStream::from_static(b"Initial content");
    let response = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Failed to put object");

    let etag = response.e_tag().expect("Should have ETag").to_string();

    // Overwrite with correct If-Match (should succeed)
    let body = ByteStream::from_static(b"Updated content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .if_match(&etag)
        .send()
        .await;

    assert!(result.is_ok(), "Should succeed with matching If-Match");

    // Verify content was updated
    let response = client.get_object().bucket("test-bucket").key("test.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Updated content");
}

#[tokio::test]
async fn test_if_match_wrong_etag_fails() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put initial object
    let body = ByteStream::from_static(b"Initial content");
    client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Failed to put object");

    // Try to overwrite with wrong If-Match (should fail)
    let body = ByteStream::from_static(b"Should not be written");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .if_match("\"wrongetag\"")
        .send()
        .await;

    assert!(result.is_err(), "Should fail with non-matching If-Match");

    // Verify original content is preserved
    let response = client.get_object().bucket("test-bucket").key("test.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Initial content");
}

#[tokio::test]
async fn test_if_match_nonexistent_object_fails() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Try to put with If-Match when object doesn't exist (should fail)
    let body = ByteStream::from_static(b"Content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("nonexistent.txt")
        .body(body)
        .if_match("\"someetag\"")
        .send()
        .await;

    assert!(result.is_err(), "Should fail with If-Match when object doesn't exist");
}

#[tokio::test]
async fn test_if_match_star_on_existing_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put initial object
    let body = ByteStream::from_static(b"Initial content");
    client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .send()
        .await
        .expect("Failed to put object");

    // Overwrite with If-Match: * (should succeed - object exists)
    let body = ByteStream::from_static(b"Updated content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("test.txt")
        .body(body)
        .if_match("*")
        .send()
        .await;

    assert!(result.is_ok(), "Should succeed with If-Match: * when object exists");

    // Verify content was updated
    let response = client.get_object().bucket("test-bucket").key("test.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Updated content");
}

#[tokio::test]
async fn test_if_match_star_on_nonexistent_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Try to put with If-Match: * when object doesn't exist (should fail)
    let body = ByteStream::from_static(b"Content");
    let result = client
        .put_object()
        .bucket("test-bucket")
        .key("nonexistent.txt")
        .body(body)
        .if_match("*")
        .send()
        .await;

    assert!(result.is_err(), "Should fail with If-Match: * when object doesn't exist");
}

#[tokio::test]
async fn test_put_and_get_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.expect("Failed to create bucket");

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
    let body = response.body.collect().await.expect("Failed to read body").into_bytes();

    assert_eq!(body.as_ref(), b"Hello, World!");
    assert_eq!(content_type.as_deref(), Some("text/plain"));
}

#[tokio::test]
async fn test_head_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let body = ByteStream::from_static(b"Test content for head");
    client.put_object().bucket("test-bucket").key("test.txt").body(body).send().await.unwrap();

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

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let body = ByteStream::from_static(b"To be deleted");
    client.put_object().bucket("test-bucket").key("delete-me.txt").body(body).send().await.unwrap();

    client
        .delete_object()
        .bucket("test-bucket")
        .key("delete-me.txt")
        .send()
        .await
        .expect("Failed to delete object");

    let result = client.head_object().bucket("test-bucket").key("delete-me.txt").send().await;

    assert!(result.is_err(), "Object should not exist after deletion");
}

#[tokio::test]
async fn test_list_objects_v2() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

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

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    for key in &["docs/readme.md", "docs/guide.md", "src/main.rs", "src/lib.rs"] {
        let body = ByteStream::from_static(b"content");
        client.put_object().bucket("test-bucket").key(*key).body(body).send().await.unwrap();
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

    client.create_bucket().bucket("source-bucket").send().await.unwrap();

    client.create_bucket().bucket("dest-bucket").send().await.unwrap();

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

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let body = ByteStream::from_static(b"Hello, World!");
    client.put_object().bucket("test-bucket").key("hello.txt").body(body).send().await.unwrap();

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

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let body = ByteStream::from_static(b"Version 1");
    client.put_object().bucket("test-bucket").key("file.txt").body(body).send().await.unwrap();

    let body = ByteStream::from_static(b"Version 2");
    client.put_object().bucket("test-bucket").key("file.txt").body(body).send().await.unwrap();

    let response = client.get_object().bucket("test-bucket").key("file.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();

    assert_eq!(body.as_ref(), b"Version 2");
}

#[tokio::test]
async fn test_nonexistent_bucket() {
    let server = TestServer::start().await;
    let client = server.client().await;

    let result = client.get_object().bucket("nonexistent-bucket").key("file.txt").send().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_nonexistent_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let result = client.get_object().bucket("test-bucket").key("nonexistent.txt").send().await;

    assert!(result.is_err());
}

#[tokio::test]
async fn test_large_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

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

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

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

// =============================================================================
// Read-After-Write Consistency Tests
// =============================================================================

#[tokio::test]
async fn test_read_after_write_consistency_get() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Write and immediately read - should always see the write
    for i in 0..10 {
        let content = format!("Content {}", i);
        let body = ByteStream::from(content.clone().into_bytes());

        client.put_object().bucket("test-bucket").key("test.txt").body(body).send().await.unwrap();

        // Immediate GET should see the new content
        let response = client
            .get_object()
            .bucket("test-bucket")
            .key("test.txt")
            .send()
            .await
            .expect("GET should succeed immediately after PUT");

        let body = response.body.collect().await.unwrap().into_bytes();
        assert_eq!(
            body.as_ref(),
            content.as_bytes(),
            "GET should return content from the latest PUT"
        );
    }
}

#[tokio::test]
async fn test_read_after_write_consistency_head() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Write and immediately HEAD - should see the new metadata
    let content = b"Test content for HEAD";
    let body = ByteStream::from_static(content);

    client.put_object().bucket("test-bucket").key("test.txt").body(body).send().await.unwrap();

    // Immediate HEAD should see the new content length
    let response = client
        .head_object()
        .bucket("test-bucket")
        .key("test.txt")
        .send()
        .await
        .expect("HEAD should succeed immediately after PUT");

    assert_eq!(
        response.content_length(),
        Some(content.len() as i64),
        "HEAD should return correct size immediately after PUT"
    );
}

#[tokio::test]
async fn test_read_after_write_consistency_list() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Write and immediately list - should include the new object
    let body = ByteStream::from_static(b"content");
    client
        .put_object()
        .bucket("test-bucket")
        .key("new-object.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    // Immediate LIST should include the new object
    let response = client
        .list_objects_v2()
        .bucket("test-bucket")
        .send()
        .await
        .expect("LIST should succeed immediately after PUT");

    let keys: Vec<_> = response.contents().iter().filter_map(|o| o.key()).collect();

    assert!(keys.contains(&"new-object.txt"), "LIST should include newly written object");
}

#[tokio::test]
async fn test_read_after_delete_consistency() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Create object first
    let body = ByteStream::from_static(b"content");
    client.put_object().bucket("test-bucket").key("to-delete.txt").body(body).send().await.unwrap();

    // Delete it
    client.delete_object().bucket("test-bucket").key("to-delete.txt").send().await.unwrap();

    // Immediate GET should return 404
    let result = client.get_object().bucket("test-bucket").key("to-delete.txt").send().await;

    assert!(result.is_err(), "GET should fail immediately after DELETE");

    // Immediate HEAD should return 404
    let result = client.head_object().bucket("test-bucket").key("to-delete.txt").send().await;

    assert!(result.is_err(), "HEAD should fail immediately after DELETE");

    // Immediate LIST should not include the deleted object
    let response = client.list_objects_v2().bucket("test-bucket").send().await.unwrap();

    let keys: Vec<_> = response.contents().iter().filter_map(|o| o.key()).collect();

    assert!(!keys.contains(&"to-delete.txt"), "LIST should not include deleted object");
}

// =============================================================================
// Delete Edge Case Tests
// =============================================================================

#[tokio::test]
async fn test_delete_nonexistent_object_is_idempotent() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // S3 DELETE is idempotent - deleting a non-existent object should succeed
    let result = client.delete_object().bucket("test-bucket").key("never-existed.txt").send().await;

    assert!(result.is_ok(), "DELETE on non-existent object should succeed (idempotent)");
}

#[tokio::test]
async fn test_double_delete_is_idempotent() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Create object
    let body = ByteStream::from_static(b"content");
    client
        .put_object()
        .bucket("test-bucket")
        .key("delete-twice.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    // First delete
    client
        .delete_object()
        .bucket("test-bucket")
        .key("delete-twice.txt")
        .send()
        .await
        .expect("First DELETE should succeed");

    // Second delete should also succeed
    let result = client.delete_object().bucket("test-bucket").key("delete-twice.txt").send().await;

    assert!(result.is_ok(), "Second DELETE should succeed (idempotent)");
}

// =============================================================================
// Copy Object Edge Case Tests
// =============================================================================

#[tokio::test]
async fn test_copy_object_within_same_bucket() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let body = ByteStream::from_static(b"Original content");
    client.put_object().bucket("test-bucket").key("original.txt").body(body).send().await.unwrap();

    // Copy within the same bucket
    client
        .copy_object()
        .bucket("test-bucket")
        .key("copy.txt")
        .copy_source("test-bucket/original.txt")
        .send()
        .await
        .expect("Copy within same bucket should succeed");

    // Verify copy exists and has correct content
    let response = client.get_object().bucket("test-bucket").key("copy.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Original content");

    // Original should still exist
    let response =
        client.get_object().bucket("test-bucket").key("original.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Original content");
}

#[tokio::test]
async fn test_copy_object_from_nonexistent_source() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Try to copy from non-existent source
    let result = client
        .copy_object()
        .bucket("test-bucket")
        .key("dest.txt")
        .copy_source("test-bucket/nonexistent.txt")
        .send()
        .await;

    assert!(result.is_err(), "Copy from non-existent source should fail");
}

#[tokio::test]
async fn test_copy_object_overwrite_destination() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Create source and destination
    let body = ByteStream::from_static(b"Source content");
    client.put_object().bucket("test-bucket").key("source.txt").body(body).send().await.unwrap();

    let body = ByteStream::from_static(b"Old destination content");
    client.put_object().bucket("test-bucket").key("dest.txt").body(body).send().await.unwrap();

    // Copy should overwrite destination
    client
        .copy_object()
        .bucket("test-bucket")
        .key("dest.txt")
        .copy_source("test-bucket/source.txt")
        .send()
        .await
        .expect("Copy should succeed and overwrite destination");

    // Verify destination has new content
    let response = client.get_object().bucket("test-bucket").key("dest.txt").send().await.unwrap();

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Source content");
}

// =============================================================================
// Concurrent Write Tests
// =============================================================================

#[tokio::test]
async fn test_concurrent_writes_to_same_key() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Spawn multiple concurrent writes to the same key
    let mut handles = Vec::new();
    for i in 0..5 {
        let client = server.client().await;
        let handle = tokio::spawn(async move {
            let content = format!("Content from writer {}", i);
            let body = ByteStream::from(content.into_bytes());
            client.put_object().bucket("test-bucket").key("contested.txt").body(body).send().await
        });
        handles.push(handle);
    }

    // All writes should succeed (serialized by per-key locking)
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "All concurrent writes should succeed");
    }

    // The object should exist and have content from one of the writers
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("contested.txt")
        .send()
        .await
        .expect("Object should exist after concurrent writes");

    let body = response.body.collect().await.unwrap().into_bytes();
    let content = String::from_utf8_lossy(&body);
    assert!(
        content.starts_with("Content from writer "),
        "Content should be from one of the writers"
    );
}

#[tokio::test]
async fn test_concurrent_writes_to_different_keys() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Spawn concurrent writes to different keys (should run in parallel)
    let mut handles = Vec::new();
    for i in 0..5 {
        let client = server.client().await;
        let handle = tokio::spawn(async move {
            let content = format!("Content {}", i);
            let body = ByteStream::from(content.into_bytes());
            client
                .put_object()
                .bucket("test-bucket")
                .key(format!("file{}.txt", i))
                .body(body)
                .send()
                .await
        });
        handles.push(handle);
    }

    // All writes should succeed
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "All concurrent writes to different keys should succeed");
    }

    // All objects should exist
    let response = client.list_objects_v2().bucket("test-bucket").send().await.unwrap();

    assert_eq!(response.contents().len(), 5, "All 5 objects should exist");
}

// =============================================================================
// ETag Format Edge Cases
// =============================================================================

#[tokio::test]
async fn test_etag_changes_on_content_change() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put first version
    let body = ByteStream::from_static(b"Version 1");
    let response1 =
        client.put_object().bucket("test-bucket").key("test.txt").body(body).send().await.unwrap();

    let etag1 = response1.e_tag().expect("Should have ETag");

    // Put second version with different content
    let body = ByteStream::from_static(b"Version 2");
    let response2 =
        client.put_object().bucket("test-bucket").key("test.txt").body(body).send().await.unwrap();

    let etag2 = response2.e_tag().expect("Should have ETag");

    assert_ne!(etag1, etag2, "ETags should differ for different content");
}

#[tokio::test]
async fn test_etag_same_for_same_content() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put same content twice
    let body = ByteStream::from_static(b"Same content");
    let response1 =
        client.put_object().bucket("test-bucket").key("file1.txt").body(body).send().await.unwrap();

    let body = ByteStream::from_static(b"Same content");
    let response2 =
        client.put_object().bucket("test-bucket").key("file2.txt").body(body).send().await.unwrap();

    let etag1 = response1.e_tag().expect("Should have ETag");
    let etag2 = response2.e_tag().expect("Should have ETag");

    assert_eq!(etag1, etag2, "ETags should be same for identical content");
}

// =============================================================================
// Range Request Edge Cases
// =============================================================================

#[tokio::test]
async fn test_range_request_at_end_of_file() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let body = ByteStream::from_static(b"Hello, World!");
    client.put_object().bucket("test-bucket").key("hello.txt").body(body).send().await.unwrap();

    // Request from offset 7 to end
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .range("bytes=7-")
        .send()
        .await
        .expect("Range request should succeed");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"World!");
}

#[tokio::test]
async fn test_range_request_middle_of_file() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    let body = ByteStream::from_static(b"Hello, World!");
    client.put_object().bucket("test-bucket").key("hello.txt").body(body).send().await.unwrap();

    // Request bytes 7-11 ("World")
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .range("bytes=7-11")
        .send()
        .await
        .expect("Range request should succeed");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"World");
}

// =============================================================================
// Empty Object Edge Cases
// =============================================================================

#[tokio::test]
async fn test_empty_object() {
    let server = TestServer::start().await;
    let client = server.client().await;

    client.create_bucket().bucket("test-bucket").send().await.unwrap();

    // Put empty object
    let body = ByteStream::from_static(b"");
    client
        .put_object()
        .bucket("test-bucket")
        .key("empty.txt")
        .body(body)
        .send()
        .await
        .expect("Empty object PUT should succeed");

    // HEAD should show 0 bytes
    let response = client
        .head_object()
        .bucket("test-bucket")
        .key("empty.txt")
        .send()
        .await
        .expect("HEAD on empty object should succeed");

    assert_eq!(response.content_length(), Some(0));

    // GET should return empty body
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("empty.txt")
        .send()
        .await
        .expect("GET on empty object should succeed");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert!(body.is_empty());
}
