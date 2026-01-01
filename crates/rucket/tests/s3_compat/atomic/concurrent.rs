//! Concurrent operation tests.

use crate::S3TestContext;
use aws_sdk_s3::primitives::ByteStream;

/// Test concurrent writes to same key.
#[tokio::test]
async fn test_concurrent_writes_same_key() {
    let ctx = S3TestContext::new().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let content = format!("content from writer {}", i);
            client
                .put_object()
                .bucket(&bucket)
                .key("contested.txt")
                .body(ByteStream::from(content.into_bytes()))
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "All writes should succeed");
    }

    // Object should exist with content from one writer
    let exists = ctx.exists("contested.txt").await;
    assert!(exists);
}

/// Test concurrent writes to different keys.
#[tokio::test]
async fn test_concurrent_writes_different_keys() {
    let ctx = S3TestContext::new().await;

    let mut handles = Vec::new();
    for i in 0..20 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .put_object()
                .bucket(&bucket)
                .key(format!("file{}.txt", i))
                .body(ByteStream::from_static(b"content"))
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    let list = ctx.list_objects().await;
    assert_eq!(list.contents().len(), 20);
}

/// Test concurrent reads.
#[tokio::test]
async fn test_concurrent_reads() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"shared content").await;

    let mut handles = Vec::new();
    for _ in 0..20 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .get_object()
                .bucket(&bucket)
                .key("test.txt")
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

/// Test concurrent read while write.
#[tokio::test]
async fn test_concurrent_read_write() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"initial").await;

    let client1 = ctx.client.clone();
    let client2 = ctx.client.clone();
    let bucket = ctx.bucket.clone();

    let write_handle = tokio::spawn({
        let bucket = bucket.clone();
        async move {
            for i in 0..5 {
                let content = format!("version {}", i);
                client1
                    .put_object()
                    .bucket(&bucket)
                    .key("test.txt")
                    .body(ByteStream::from(content.into_bytes()))
                    .send()
                    .await
                    .unwrap();
            }
        }
    });

    let read_handle = tokio::spawn(async move {
        for _ in 0..10 {
            let _ = client2.get_object().bucket(&bucket).key("test.txt").send().await;
        }
    });

    let _ = write_handle.await;
    let _ = read_handle.await;

    // Final read should succeed
    let _ = ctx.get("test.txt").await;
}

/// Test concurrent deletes.
#[tokio::test]
async fn test_concurrent_deletes() {
    let ctx = S3TestContext::new().await;

    // Create objects
    for i in 0..10 {
        ctx.put(&format!("file{}.txt", i), b"content").await;
    }

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .delete_object()
                .bucket(&bucket)
                .key(format!("file{}.txt", i))
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let _ = handle.await.unwrap();
    }

    let list = ctx.list_objects().await;
    assert!(list.contents().is_empty());
}

/// Test concurrent bucket operations.
#[tokio::test]
async fn test_concurrent_bucket_create() {
    let ctx = S3TestContext::without_bucket().await;

    let mut handles = Vec::new();
    for i in 0..5 {
        let client = ctx.client.clone();
        let handle = tokio::spawn(async move {
            client
                .create_bucket()
                .bucket(format!("concurrent-bucket-{}", i))
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    let list = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list");
    assert_eq!(list.buckets().len(), 5);
}
