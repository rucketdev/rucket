//! Multipart upload listing tests.

use crate::S3TestContext;
use aws_sdk_s3::primitives::ByteStream;

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
