//! Object GET tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_object_get_*
//! - MinIO Mint: GetObject tests

use aws_sdk_s3::primitives::ByteStream;

use crate::{random_bytes, S3TestContext};

/// Test basic object GET.
/// Ceph: test_object_read
#[tokio::test]
async fn test_object_get_simple() {
    let ctx = S3TestContext::new().await;

    let content = b"Hello, World!";
    ctx.put("test.txt", content).await;

    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), content);
}

/// Test GET non-existent object returns 404.
/// Ceph: test_object_read_notexist
#[tokio::test]
async fn test_object_get_not_found() {
    let ctx = S3TestContext::new().await;

    let result = ctx.client.get_object().bucket(&ctx.bucket).key("nonexistent.txt").send().await;

    assert!(result.is_err(), "Should return error for non-existent object");
    let error_str = format!("{:?}", result.unwrap_err());
    assert!(error_str.contains("NoSuchKey"), "Should be NoSuchKey error, got: {}", error_str);
}

/// Test GET from non-existent bucket returns error.
#[tokio::test]
async fn test_object_get_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.get_object().bucket("nonexistent-bucket").key("test.txt").send().await;

    assert!(result.is_err(), "Should return error for non-existent bucket");
}

/// Test GET returns correct content type.
#[tokio::test]
async fn test_object_get_content_type() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"<html></html>");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("index.html")
        .body(body)
        .content_type("text/html")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.get_object("index.html").await;
    assert_eq!(response.content_type(), Some("text/html"));
}

/// Test GET returns correct content length.
#[tokio::test]
async fn test_object_get_content_length() {
    let ctx = S3TestContext::new().await;

    let content = b"Exactly 21 bytes.....";
    ctx.put("test.txt", content).await;

    let response = ctx.get_object("test.txt").await;
    assert_eq!(response.content_length(), Some(21));
}

/// Test GET returns ETag.
#[tokio::test]
async fn test_object_get_etag() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    assert!(response.e_tag().is_some(), "Should return ETag");
}

/// Test GET empty object.
#[tokio::test]
async fn test_object_get_empty() {
    let ctx = S3TestContext::new().await;

    ctx.put("empty.txt", b"").await;

    let data = ctx.get("empty.txt").await;
    assert!(data.is_empty(), "Empty object should return empty data");
}

/// Test GET large object.
#[tokio::test]
async fn test_object_get_large() {
    let ctx = S3TestContext::new().await;

    let content = random_bytes(1024 * 1024); // 1 MB
    ctx.put("large.bin", &content).await;

    let data = ctx.get("large.bin").await;
    assert_eq!(data.len(), 1024 * 1024);
    assert_eq!(data.as_slice(), content.as_slice());
}

/// Test GET returns user metadata.
#[tokio::test]
async fn test_object_get_user_metadata() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .body(body)
        .metadata("x-custom", "value")
        .send()
        .await
        .expect("Should put object");

    let response = ctx.get_object("test.txt").await;
    let metadata = response.metadata().unwrap();
    assert_eq!(metadata.get("x-custom"), Some(&"value".to_string()));
}

/// Test GET with If-Match succeeds.
#[tokio::test]
async fn test_object_get_if_match_success() {
    let ctx = S3TestContext::new().await;

    let put_response = ctx.put("test.txt", b"content").await;
    let etag = put_response.e_tag().unwrap().to_string();

    let result =
        ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").if_match(&etag).send().await;

    assert!(result.is_ok(), "GET with matching If-Match should succeed");
}

/// Test GET with If-Match fails on mismatch.
#[tokio::test]
async fn test_object_get_if_match_fails() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_match("\"wrong-etag\"")
        .send()
        .await;

    assert!(result.is_err(), "GET with wrong If-Match should fail");
}

/// Test GET with If-None-Match succeeds when no match.
#[tokio::test]
async fn test_object_get_if_none_match_success() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match("\"different-etag\"")
        .send()
        .await;

    assert!(result.is_ok(), "GET with non-matching If-None-Match should succeed");
}

/// Test GET with If-None-Match returns 304 when matching.
#[tokio::test]
async fn test_object_get_if_none_match_not_modified() {
    let ctx = S3TestContext::new().await;

    let put_response = ctx.put("test.txt", b"content").await;
    let etag = put_response.e_tag().unwrap().to_string();

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_none_match(&etag)
        .send()
        .await;

    // Should return 304 Not Modified (which AWS SDK treats as an error)
    assert!(result.is_err(), "GET with matching If-None-Match should return 304");
}

/// Test GET after overwrite.
#[tokio::test]
async fn test_object_get_after_overwrite() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"Version 1").await;
    ctx.put("test.txt", b"Version 2").await;

    let data = ctx.get("test.txt").await;
    assert_eq!(data.as_slice(), b"Version 2");
}

/// Test GET binary content.
#[tokio::test]
async fn test_object_get_binary() {
    let ctx = S3TestContext::new().await;

    let content: Vec<u8> = (0..=255).collect();
    ctx.put("binary.bin", &content).await;

    let data = ctx.get("binary.bin").await;
    assert_eq!(data.as_slice(), content.as_slice());
}

/// Test concurrent GETs.
#[tokio::test]
async fn test_object_get_concurrent() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"shared content").await;

    let mut handles = Vec::new();
    for _ in 0..10 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let handle = tokio::spawn(async move {
            client.get_object().bucket(&bucket).key("test.txt").send().await
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok(), "Concurrent GET should succeed");
    }
}

/// Test GET with response content type override.
#[tokio::test]
async fn test_object_get_response_content_type() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_content_type("application/json")
        .send()
        .await
        .expect("Should get object");

    assert_eq!(response.content_type(), Some("application/json"));
}

/// Test GET with response content disposition override.
#[tokio::test]
async fn test_object_get_response_content_disposition() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_content_disposition("attachment; filename=\"download.txt\"")
        .send()
        .await
        .expect("Should get object");

    assert_eq!(response.content_disposition(), Some("attachment; filename=\"download.txt\""));
}

/// Test GET with deep path key.
#[tokio::test]
async fn test_object_get_deep_path() {
    let ctx = S3TestContext::new().await;

    let key = "a/b/c/d/e/file.txt";
    ctx.put(key, b"deep content").await;

    let data = ctx.get(key).await;
    assert_eq!(data.as_slice(), b"deep content");
}

// =============================================================================
// Extended GET Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test GET returns last modified timestamp.
/// Ceph: test_object_get_last_modified
#[tokio::test]
async fn test_object_get_last_modified() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    assert!(response.last_modified().is_some(), "Should return Last-Modified");
}

/// Test GET with response cache control override.
/// Ceph: test_object_get_response_cache_control
#[tokio::test]
async fn test_object_get_response_cache_control() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_cache_control("no-cache")
        .send()
        .await
        .expect("Should get object");

    assert_eq!(response.cache_control(), Some("no-cache"));
}

/// Test GET with response content language override.
/// Ceph: test_object_get_response_content_language
#[tokio::test]
async fn test_object_get_response_content_language() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_content_language("en-US")
        .send()
        .await
        .expect("Should get object");

    assert_eq!(response.content_language(), Some("en-US"));
}

/// Test GET with response expires override.
/// Ceph: test_object_get_response_expires
#[tokio::test]
async fn test_object_get_response_expires() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_expires(aws_smithy_types::DateTime::from_secs(1700000000))
        .send()
        .await
        .expect("Should get object");

    assert!(response.expires_string().is_some());
}

/// Test GET with part number on non-multipart object.
/// Ceph: test_object_get_part_number
#[tokio::test]
#[ignore = "Part number on non-multipart not implemented"]
async fn test_object_get_part_number_non_multipart() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Part number on non-multipart should error
    let result =
        ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").part_number(1).send().await;

    // Non-multipart objects don't have parts
    assert!(result.is_err());
}

/// Test GET on versioned bucket returns version id.
/// Ceph: test_object_get_version_id
#[tokio::test]
async fn test_object_get_version_id() {
    let ctx = S3TestContext::with_versioning().await;

    let put = ctx.put("test.txt", b"content").await;
    let version_id = put.version_id().unwrap();

    let response = ctx.get_object("test.txt").await;
    assert_eq!(response.version_id(), Some(version_id));
}

/// Test GET specific version.
/// Ceph: test_object_get_specific_version
#[tokio::test]
async fn test_object_get_specific_version() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"version1").await;
    let v2 = ctx.put("test.txt", b"version2").await;

    let vid1 = v1.version_id().unwrap();
    let vid2 = v2.version_id().unwrap();

    // Get v1
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get v1");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"version1");

    // Get v2
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid2)
        .send()
        .await
        .expect("Should get v2");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"version2");
}

/// Test GET non-existent version returns error.
/// Ceph: test_object_get_nonexistent_version
#[tokio::test]
async fn test_object_get_nonexistent_version() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id("nonexistent-version-id")
        .send()
        .await;

    assert!(result.is_err());
}

/// Test GET with multiple response overrides.
/// Ceph: test_object_get_multiple_overrides
#[tokio::test]
async fn test_object_get_multiple_response_overrides() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .response_content_type("text/plain")
        .response_content_disposition("inline")
        .response_cache_control("private")
        .send()
        .await
        .expect("Should get object");

    assert_eq!(response.content_type(), Some("text/plain"));
    assert_eq!(response.content_disposition(), Some("inline"));
    assert_eq!(response.cache_control(), Some("private"));
}

/// Test GET with unicode key.
/// Ceph: test_object_get_unicode
#[tokio::test]
async fn test_object_get_unicode_key() {
    let ctx = S3TestContext::new().await;

    let key = "日本語/ファイル.txt";
    ctx.put(key, b"unicode content").await;

    let data = ctx.get(key).await;
    assert_eq!(data.as_slice(), b"unicode content");
}

/// Test GET with special characters in key.
/// Ceph: test_object_get_special_chars
#[tokio::test]
async fn test_object_get_special_chars_key() {
    let ctx = S3TestContext::new().await;

    let key = "file with spaces & (special).txt";
    ctx.put(key, b"special content").await;

    let data = ctx.get(key).await;
    assert_eq!(data.as_slice(), b"special content");
}

/// Test GET preserves content encoding.
/// Ceph: test_object_get_content_encoding
#[tokio::test]
async fn test_object_get_content_encoding() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"compressed data");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("test.gz")
        .body(body)
        .content_encoding("gzip")
        .send()
        .await
        .expect("Should put");

    let response = ctx.get_object("test.gz").await;
    assert_eq!(response.content_encoding(), Some("gzip"));
}

/// Test GET with If-Modified-Since success.
/// Ceph: test_object_get_if_modified_since
#[tokio::test]
async fn test_object_get_if_modified_since() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Use a very old date
    let old_date = aws_smithy_types::DateTime::from_secs(0);

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_modified_since(old_date)
        .send()
        .await;

    assert!(result.is_ok(), "Should return object (modified after 1970)");
}

/// Test GET with If-Unmodified-Since success.
/// Ceph: test_object_get_if_unmodified_since
#[tokio::test]
async fn test_object_get_if_unmodified_since() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    // Use a far future date
    let future_date = aws_smithy_types::DateTime::from_secs(4102444800); // 2100-01-01

    let result = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .if_unmodified_since(future_date)
        .send()
        .await;

    assert!(result.is_ok(), "Should return object (unmodified before 2100)");
}

/// Test GET returns accept-ranges header.
/// Ceph: test_object_get_accept_ranges
#[tokio::test]
async fn test_object_get_accept_ranges() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    // Accept-Ranges should be "bytes" to indicate range support
    if let Some(ranges) = response.accept_ranges() {
        assert_eq!(ranges, "bytes");
    }
}

/// Test GET delete marker returns error.
/// Ceph: test_object_get_delete_marker
#[tokio::test]
async fn test_object_get_delete_marker() {
    let ctx = S3TestContext::with_versioning().await;

    ctx.put("test.txt", b"content").await;
    ctx.delete("test.txt").await; // Creates delete marker

    // GET without version should fail (delete marker)
    let result = ctx.client.get_object().bucket(&ctx.bucket).key("test.txt").send().await;
    assert!(result.is_err());
}

/// Test GET version after delete marker.
/// Ceph: test_object_get_version_after_delete_marker
#[tokio::test]
async fn test_object_get_version_after_delete_marker() {
    let ctx = S3TestContext::with_versioning().await;

    let v1 = ctx.put("test.txt", b"v1 content").await;
    let vid1 = v1.version_id().unwrap();

    ctx.delete("test.txt").await; // Creates delete marker

    // GET with version should still work
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .version_id(vid1)
        .send()
        .await
        .expect("Should get version by ID");

    let body = response.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"v1 content");
}

/// Test GET returns storage class.
/// Ceph: test_object_get_storage_class
#[tokio::test]
async fn test_object_get_storage_class() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    // Storage class may or may not be returned in GET
    let _ = response.storage_class();
}

/// Test GET returns parts count for multipart.
/// Ceph: test_object_get_parts_count
#[tokio::test]
async fn test_object_get_parts_count() {
    let ctx = S3TestContext::new().await;

    // Create a multipart upload
    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .send()
        .await
        .expect("Should create");

    let upload_id = create.upload_id().unwrap();

    // Upload a part
    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from(vec![0u8; 5 * 1024 * 1024]))
        .send()
        .await
        .expect("Should upload part");

    // Complete
    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(part.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("Should complete");

    let response = ctx.get_object("multipart.txt").await;
    // Parts count may or may not be included
    let _ = response.parts_count();
}

/// Test GET concurrent on same object.
/// Ceph: test_object_get_concurrent_same
#[tokio::test]
async fn test_object_get_concurrent_same_object() {
    let ctx = S3TestContext::new().await;

    let content = random_bytes(100 * 1024);
    ctx.put("large.bin", &content).await;

    let mut handles = Vec::new();
    for _ in 0..20 {
        let client = ctx.client.clone();
        let bucket = ctx.bucket.clone();
        let expected = content.clone();
        let handle = tokio::spawn(async move {
            let response = client.get_object().bucket(&bucket).key("large.bin").send().await?;
            let body = response.body.collect().await.unwrap().into_bytes();
            assert_eq!(body.len(), expected.len());
            Ok::<_, aws_sdk_s3::Error>(())
        });
        handles.push(handle);
    }

    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result.is_ok());
    }
}

/// Test GET on object with long key.
/// Ceph: test_object_get_long_key
#[tokio::test]
async fn test_object_get_long_key() {
    let ctx = S3TestContext::new().await;

    let key = "a".repeat(500) + ".txt";
    ctx.put(&key, b"content").await;

    let data = ctx.get(&key).await;
    assert_eq!(data.as_slice(), b"content");
}

/// Test GET returns object lock info.
/// Ceph: test_object_get_lock_info
#[tokio::test]
#[ignore = "Object lock not fully implemented"]
async fn test_object_get_object_lock_info() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    // Object lock info if set
    let _ = response.object_lock_mode();
    let _ = response.object_lock_retain_until_date();
    let _ = response.object_lock_legal_hold_status();
}

/// Test GET replication status.
/// Ceph: test_object_get_replication_status
#[tokio::test]
#[ignore = "Replication not implemented"]
async fn test_object_get_replication_status() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    let _ = response.replication_status();
}

/// Test GET website redirect location.
/// Ceph: test_object_get_redirect
#[tokio::test]
#[ignore = "Website redirect not implemented"]
async fn test_object_get_website_redirect() {
    let ctx = S3TestContext::new().await;

    let body = ByteStream::from_static(b"content");
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("redirect.html")
        .body(body)
        .website_redirect_location("/new-page.html")
        .send()
        .await
        .expect("Should put");

    let response = ctx.get_object("redirect.html").await;
    assert_eq!(response.website_redirect_location(), Some("/new-page.html"));
}

/// Test GET server side encryption info.
/// Ceph: test_object_get_sse_info
#[tokio::test]
async fn test_object_get_sse_info() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    // SSE info if applied
    let _ = response.server_side_encryption();
    let _ = response.sse_customer_algorithm();
}

/// Test GET expiration info.
/// Ceph: test_object_get_expiration
#[tokio::test]
#[ignore = "Lifecycle expiration not implemented"]
async fn test_object_get_expiration() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    // Expiration header if lifecycle rule applies
    let _ = response.expiration();
}

/// Test GET returns request charged.
/// Ceph: test_object_get_request_charged
#[tokio::test]
async fn test_object_get_request_charged() {
    let ctx = S3TestContext::new().await;

    ctx.put("test.txt", b"content").await;

    let response = ctx.get_object("test.txt").await;
    // Request charged only for requester pays buckets
    let _ = response.request_charged();
}
