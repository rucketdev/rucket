//! Concurrent operation tests.

use aws_sdk_s3::primitives::ByteStream;

use crate::S3TestContext;

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
            client.get_object().bucket(&bucket).key("test.txt").send().await
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
            client.delete_object().bucket(&bucket).key(format!("file{}.txt", i)).send().await
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
            client.create_bucket().bucket(format!("concurrent-bucket-{}", i)).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }

    let list = ctx.client.list_buckets().send().await.expect("Should list");
    assert_eq!(list.buckets().len(), 5);
}

// =============================================================================
// Extended Concurrent Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test concurrent HEAD operations.
/// Ceph: test_concurrent_head
#[tokio::test]
async fn test_concurrent_head_operations() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content for head test").await;

    let mut handles = Vec::new();
    for _ in 0..20 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.head_object().bucket(&bucket).key("test.txt").send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "All HEAD operations should succeed");
    }
}

/// Test concurrent list operations.
/// Ceph: test_concurrent_list
#[tokio::test]
async fn test_concurrent_list_operations() {
    let ctx = S3TestContext::new().await;

    // Create some objects
    for i in 0..10 {
        ctx.put(&format!("obj{}.txt", i), b"content").await;
    }

    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.list_objects_v2().bucket(&bucket).send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().contents().len(), 10);
    }
}

/// Test concurrent multipart uploads.
/// Ceph: test_concurrent_multipart
#[tokio::test]
async fn test_concurrent_multipart_uploads() {
    let ctx = S3TestContext::new().await;

    let mut handles = Vec::new();
    for i in 0..5 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let create = client
                .create_multipart_upload()
                .bucket(&bucket)
                .key(format!("multipart-{}.bin", i))
                .send()
                .await
                .expect("Should create multipart");

            let upload_id = create.upload_id().unwrap();

            // Abort immediately (just testing concurrent creation)
            client
                .abort_multipart_upload()
                .bucket(&bucket)
                .key(format!("multipart-{}.bin", i))
                .upload_id(upload_id)
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

/// Test concurrent copy operations.
/// Ceph: test_concurrent_copy
#[tokio::test]
async fn test_concurrent_copy_operations() {
    let ctx = S3TestContext::new().await;

    ctx.put("source.txt", b"source content for copying").await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .copy_object()
                .bucket(&bucket)
                .key(format!("copy-{}.txt", i))
                .copy_source(format!("{}/source.txt", bucket))
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
    assert_eq!(list.contents().len(), 11); // source + 10 copies
}

/// Test concurrent tagging operations.
/// Ceph: test_concurrent_tagging
#[tokio::test]
async fn test_concurrent_tagging_operations() {
    use aws_sdk_s3::types::{Tag, Tagging};

    let ctx = S3TestContext::new().await;

    ctx.put("tagged.txt", b"content").await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let tagging = Tagging::builder()
                .tag_set(Tag::builder().key("iteration").value(format!("{}", i)).build().unwrap())
                .build()
                .unwrap();

            client
                .put_object_tagging()
                .bucket(&bucket)
                .key("tagged.txt")
                .tagging(tagging)
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

/// Test concurrent metadata operations.
/// Ceph: test_concurrent_metadata
#[tokio::test]
async fn test_concurrent_metadata_operations() {
    let ctx = S3TestContext::new().await;

    // Create objects with metadata concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .put_object()
                .bucket(&bucket)
                .key(format!("meta-{}.txt", i))
                .body(ByteStream::from_static(b"content"))
                .metadata("index", format!("{}", i))
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

/// Test concurrent versioned operations.
/// Ceph: test_concurrent_versioned
#[tokio::test]
async fn test_concurrent_versioned_operations() {
    let ctx = S3TestContext::with_versioning().await;

    let mut handles = Vec::new();
    for i in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            let content = format!("version {}", i);
            client
                .put_object()
                .bucket(&bucket)
                .key("versioned.txt")
                .body(ByteStream::from(content.into_bytes()))
                .send()
                .await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
        assert!(result.unwrap().version_id().is_some());
    }

    // Should have 10 versions
    let versions = ctx.list_versions().await;
    assert_eq!(versions.versions().len(), 10);
}

/// Test high concurrency stress test.
/// Ceph: test_concurrent_stress
#[tokio::test]
async fn test_concurrent_high_load() {
    let ctx = S3TestContext::new().await;

    let mut handles = Vec::new();
    for i in 0..50 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client
                .put_object()
                .bucket(&bucket)
                .key(format!("stress-{:04}.txt", i))
                .body(ByteStream::from_static(b"x"))
                .send()
                .await
        });
        handles.push(handle);
    }

    let mut success_count = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 50, "All concurrent operations should succeed");
}

/// Test concurrent delete and recreate.
/// Ceph: test_concurrent_delete_recreate
#[tokio::test]
async fn test_concurrent_delete_recreate() {
    let ctx = S3TestContext::new().await;

    ctx.put("target.txt", b"initial").await;

    let client1 = ctx.client.clone();
    let client2 = ctx.client.clone();
    let bucket = ctx.bucket.clone();

    let delete_handle = tokio::spawn({
        let bucket = bucket.clone();
        async move {
            for _ in 0..5 {
                let _ = client1.delete_object().bucket(&bucket).key("target.txt").send().await;
            }
        }
    });

    let recreate_handle = tokio::spawn(async move {
        for i in 0..5 {
            let content = format!("recreated {}", i);
            let _ = client2
                .put_object()
                .bucket(&bucket)
                .key("target.txt")
                .body(ByteStream::from(content.into_bytes()))
                .send()
                .await;
        }
    });

    let _ = delete_handle.await;
    let _ = recreate_handle.await;

    // Object may or may not exist depending on race outcome
    let _ = ctx.client.head_object().bucket(&ctx.bucket).key("target.txt").send().await;
}
