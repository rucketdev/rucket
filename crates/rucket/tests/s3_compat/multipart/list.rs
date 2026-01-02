//! Multipart upload listing tests.

use aws_sdk_s3::primitives::ByteStream;

use crate::S3TestContext;

/// Test list multipart uploads.
#[tokio::test]
async fn test_multipart_list_uploads() {
    let ctx = S3TestContext::new().await;

    // Create multiple uploads
    let mut upload_ids = Vec::new();
    for i in 0..3 {
        let create = ctx
            .client
            .create_multipart_upload()
            .bucket(&ctx.bucket)
            .key(format!("file{}.txt", i))
            .send()
            .await
            .expect("Should initiate");
        upload_ids.push(create.upload_id().unwrap().to_string());
    }

    let response = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should list uploads");

    let uploads = response.uploads();
    assert_eq!(uploads.len(), 3);

    // Cleanup
    for (i, upload_id) in upload_ids.iter().enumerate() {
        let _ = ctx
            .client
            .abort_multipart_upload()
            .bucket(&ctx.bucket)
            .key(format!("file{}.txt", i))
            .upload_id(upload_id)
            .send()
            .await;
    }
}

/// Test list multipart uploads empty.
#[tokio::test]
async fn test_multipart_list_uploads_empty() {
    let ctx = S3TestContext::new().await;

    let response = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .send()
        .await
        .expect("Should list uploads");

    assert!(response.uploads().is_empty());
}

/// Test list multipart uploads with prefix.
#[tokio::test]
async fn test_multipart_list_uploads_prefix() {
    let ctx = S3TestContext::new().await;

    // Create uploads with different prefixes
    let create1 = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("docs/file.txt")
        .send()
        .await
        .unwrap();
    let upload1 = create1.upload_id().unwrap().to_string();

    let create2 = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("src/file.txt")
        .send()
        .await
        .unwrap();
    let upload2 = create2.upload_id().unwrap().to_string();

    let response = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .prefix("docs/")
        .send()
        .await
        .expect("Should list uploads");

    let uploads = response.uploads();
    assert_eq!(uploads.len(), 1);
    assert!(uploads[0].key().unwrap().starts_with("docs/"));

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("docs/file.txt")
        .upload_id(&upload1)
        .send()
        .await;
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("src/file.txt")
        .upload_id(&upload2)
        .send()
        .await;
}

/// Test list parts.
#[tokio::test]
async fn test_multipart_list_parts() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    // Upload multiple parts
    for i in 1..=3 {
        ctx.client
            .upload_part()
            .bucket(&ctx.bucket)
            .key("test.txt")
            .upload_id(upload_id)
            .part_number(i)
            .body(ByteStream::from(format!("part{}", i).into_bytes()))
            .send()
            .await
            .unwrap();
    }

    let response = ctx
        .client
        .list_parts()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should list parts");

    let parts = response.parts();
    assert_eq!(parts.len(), 3);

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test list parts returns part info.
#[tokio::test]
async fn test_multipart_list_parts_info() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    ctx.client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    let response = ctx
        .client
        .list_parts()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await
        .unwrap();

    let part = &response.parts()[0];
    assert_eq!(part.part_number(), Some(1));
    assert!(part.e_tag().is_some());
    assert!(part.size().is_some());
    assert!(part.last_modified().is_some());

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

// =============================================================================
// Extended List Tests (ported from Ceph s3-tests)
// =============================================================================

/// Test list uploads with delimiter.
/// Ceph: test_multipart_list_delimiter
#[tokio::test]
async fn test_multipart_list_uploads_delimiter() {
    let ctx = S3TestContext::new().await;

    // Create uploads with directory-like structure
    let create1 = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("foo/bar/file.txt")
        .send()
        .await
        .unwrap();
    let upload1 = create1.upload_id().unwrap().to_string();

    let create2 = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("foo/baz.txt")
        .send()
        .await
        .unwrap();
    let upload2 = create2.upload_id().unwrap().to_string();

    let response = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .prefix("foo/")
        .delimiter("/")
        .send()
        .await
        .expect("Should list uploads");

    // Should have common prefixes
    let prefixes: Vec<&str> =
        response.common_prefixes().iter().filter_map(|p| p.prefix()).collect();
    assert!(prefixes.contains(&"foo/bar/"));

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("foo/bar/file.txt")
        .upload_id(&upload1)
        .send()
        .await;
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("foo/baz.txt")
        .upload_id(&upload2)
        .send()
        .await;
}

/// Test list uploads max-uploads limit.
/// Ceph: test_multipart_list_max_uploads
#[tokio::test]
async fn test_multipart_list_uploads_max_uploads() {
    let ctx = S3TestContext::new().await;

    // Create multiple uploads
    let mut upload_ids = Vec::new();
    for i in 0..5 {
        let create = ctx
            .client
            .create_multipart_upload()
            .bucket(&ctx.bucket)
            .key(format!("file{}.txt", i))
            .send()
            .await
            .unwrap();
        upload_ids.push((format!("file{}.txt", i), create.upload_id().unwrap().to_string()));
    }

    // List with max-uploads = 2
    let response = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .max_uploads(2)
        .send()
        .await
        .expect("Should list");

    assert_eq!(response.uploads().len(), 2);
    assert!(response.is_truncated().unwrap_or(false));

    // Cleanup
    for (key, upload_id) in upload_ids {
        let _ = ctx
            .client
            .abort_multipart_upload()
            .bucket(&ctx.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .send()
            .await;
    }
}

/// Test list uploads pagination.
/// Ceph: test_multipart_list_pagination
#[tokio::test]
async fn test_multipart_list_uploads_pagination() {
    let ctx = S3TestContext::new().await;

    // Create multiple uploads
    let mut upload_ids = Vec::new();
    for i in 0..5 {
        let create = ctx
            .client
            .create_multipart_upload()
            .bucket(&ctx.bucket)
            .key(format!("page{}.txt", i))
            .send()
            .await
            .unwrap();
        upload_ids.push((format!("page{}.txt", i), create.upload_id().unwrap().to_string()));
    }

    // First page
    let response1 = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .max_uploads(2)
        .send()
        .await
        .expect("Should list");

    assert_eq!(response1.uploads().len(), 2);
    assert!(response1.is_truncated().unwrap_or(false));

    // Second page using key marker
    let next_key = response1.next_key_marker().unwrap();
    let next_upload = response1.next_upload_id_marker().unwrap();

    let response2 = ctx
        .client
        .list_multipart_uploads()
        .bucket(&ctx.bucket)
        .max_uploads(2)
        .key_marker(next_key)
        .upload_id_marker(next_upload)
        .send()
        .await
        .expect("Should list next page");

    assert!(!response2.uploads().is_empty());

    // Cleanup
    for (key, upload_id) in upload_ids {
        let _ = ctx
            .client
            .abort_multipart_upload()
            .bucket(&ctx.bucket)
            .key(&key)
            .upload_id(&upload_id)
            .send()
            .await;
    }
}

/// Test list parts pagination.
/// Ceph: test_multipart_list_parts_pagination
#[tokio::test]
async fn test_multipart_list_parts_pagination() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    // Upload 5 parts
    for i in 1..=5 {
        ctx.client
            .upload_part()
            .bucket(&ctx.bucket)
            .key("test.txt")
            .upload_id(upload_id)
            .part_number(i)
            .body(ByteStream::from(format!("part{}", i).into_bytes()))
            .send()
            .await
            .unwrap();
    }

    // List with max-parts = 2
    let response = ctx
        .client
        .list_parts()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .max_parts(2)
        .send()
        .await
        .expect("Should list");

    assert_eq!(response.parts().len(), 2);
    assert!(response.is_truncated().unwrap_or(false));

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test list uploads returns upload info.
/// Ceph: test_multipart_list_upload_info
#[tokio::test]
async fn test_multipart_list_uploads_info() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    let response =
        ctx.client.list_multipart_uploads().bucket(&ctx.bucket).send().await.expect("Should list");

    let upload = &response.uploads()[0];
    assert!(upload.key().is_some());
    assert!(upload.upload_id().is_some());
    assert!(upload.initiated().is_some());

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(&upload_id)
        .send()
        .await;
}

/// Test list uploads on non-existent bucket fails.
/// Ceph: test_multipart_list_nonexistent_bucket
#[tokio::test]
async fn test_multipart_list_uploads_nonexistent_bucket() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.list_multipart_uploads().bucket("nonexistent-bucket").send().await;

    assert!(result.is_err());
}

/// Test list parts on non-existent upload fails.
/// Ceph: test_multipart_list_parts_nonexistent
#[tokio::test]
async fn test_multipart_list_parts_nonexistent_upload() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .list_parts()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id("nonexistent-upload")
        .send()
        .await;

    assert!(result.is_err());
}

/// Test list parts with part-number-marker.
/// Ceph: test_multipart_list_parts_marker
#[tokio::test]
async fn test_multipart_list_parts_part_marker() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    // Upload 5 parts
    for i in 1..=5 {
        ctx.client
            .upload_part()
            .bucket(&ctx.bucket)
            .key("test.txt")
            .upload_id(upload_id)
            .part_number(i)
            .body(ByteStream::from(format!("part{}", i).into_bytes()))
            .send()
            .await
            .unwrap();
    }

    // List parts after part 2
    let response = ctx
        .client
        .list_parts()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .part_number_marker("2")
        .send()
        .await
        .expect("Should list");

    // Should have parts 3, 4, 5
    assert_eq!(response.parts().len(), 3);
    assert_eq!(response.parts()[0].part_number(), Some(3));

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(upload_id)
        .send()
        .await;
}

/// Test list uploads returns bucket name.
/// Ceph: test_multipart_list_bucket
#[tokio::test]
async fn test_multipart_list_uploads_returns_bucket() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    let response =
        ctx.client.list_multipart_uploads().bucket(&ctx.bucket).send().await.expect("Should list");

    assert_eq!(response.bucket(), Some(ctx.bucket.as_str()));

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(&upload_id)
        .send()
        .await;
}

/// Test list parts returns upload ID.
/// Ceph: test_multipart_list_parts_upload_id
#[tokio::test]
async fn test_multipart_list_parts_returns_upload_id() {
    let ctx = S3TestContext::new().await;

    let create = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    ctx.client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"content"))
        .send()
        .await
        .unwrap();

    let response = ctx
        .client
        .list_parts()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(&upload_id)
        .send()
        .await
        .expect("Should list");

    assert_eq!(response.upload_id(), Some(upload_id.as_str()));

    // Cleanup
    let _ = ctx
        .client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key("test.txt")
        .upload_id(&upload_id)
        .send()
        .await;
}
