// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Basic S3 operation integration tests.

mod common;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::BucketLocationConstraint;

#[path = "../common/mod.rs"]
mod test_common;

use test_common::TestServer;

#[tokio::test]
async fn test_create_and_delete_bucket() {
    let server = TestServer::start().await;
    let client = server.client().await;

    // Create bucket
    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Failed to create bucket");

    // Verify bucket exists via head
    client
        .head_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Bucket should exist");

    // Delete bucket
    client
        .delete_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Failed to delete bucket");

    // Verify bucket is gone
    let result = client.head_bucket().bucket("test-bucket").send().await;
    assert!(result.is_err(), "Bucket should not exist after deletion");
}

#[tokio::test]
async fn test_list_buckets() {
    let server = TestServer::start().await;
    let client = server.client().await;

    // Create multiple buckets
    for name in &["bucket-a", "bucket-b", "bucket-c"] {
        client
            .create_bucket()
            .bucket(*name)
            .send()
            .await
            .expect("Failed to create bucket");
    }

    // List buckets
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

    // Create bucket
    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .expect("Failed to create bucket");

    // Put object
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

    // Get object
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .send()
        .await
        .expect("Failed to get object");

    let body = response
        .body
        .collect()
        .await
        .expect("Failed to read body")
        .into_bytes();

    assert_eq!(body.as_ref(), b"Hello, World!");
    assert_eq!(response.content_type(), Some("text/plain"));
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

    // Delete object
    client
        .delete_object()
        .bucket("test-bucket")
        .key("delete-me.txt")
        .send()
        .await
        .expect("Failed to delete object");

    // Verify object is gone
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

    // Put multiple objects
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

    // List objects
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

    // Put objects with different prefixes
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

    // List with prefix
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

    // Put source object
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

    // Copy object
    client
        .copy_object()
        .bucket("dest-bucket")
        .key("copy.txt")
        .copy_source("source-bucket/original.txt")
        .send()
        .await
        .expect("Failed to copy object");

    // Verify copy exists
    let response = client
        .get_object()
        .bucket("dest-bucket")
        .key("copy.txt")
        .send()
        .await
        .expect("Failed to get copied object");

    let body = response
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();

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

    // Get range
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("hello.txt")
        .range("bytes=0-4")
        .send()
        .await
        .expect("Failed to get object range");

    let body = response
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();

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

    // Put initial object
    let body = ByteStream::from_static(b"Version 1");
    client
        .put_object()
        .bucket("test-bucket")
        .key("file.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    // Overwrite
    let body = ByteStream::from_static(b"Version 2");
    client
        .put_object()
        .bucket("test-bucket")
        .key("file.txt")
        .body(body)
        .send()
        .await
        .unwrap();

    // Verify new content
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("file.txt")
        .send()
        .await
        .unwrap();

    let body = response
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();

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

    // Get it back
    let response = client
        .get_object()
        .bucket("test-bucket")
        .key("large.bin")
        .send()
        .await
        .expect("Failed to get large object");

    let body = response
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();

    assert_eq!(body.len(), 1024 * 1024);
    assert_eq!(body.as_ref(), data.as_slice());
}
