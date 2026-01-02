//! GetObjectAttributes tests.
//!
//! Ported from:
//! - MinIO Mint: testGetObjectAttributes*
//! - AWS S3 documentation: GetObjectAttributes API

use aws_sdk_s3::types::ObjectAttributes;

use crate::S3TestContext;

/// Test basic GetObjectAttributes call.
/// MinIO: testGetObjectAttributesBasic
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_basic() {
    let ctx = S3TestContext::new().await;
    let key = "test-object.txt";
    let content = b"Hello, World!";

    ctx.put(key, content).await;

    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::ObjectSize)
        .object_attributes(ObjectAttributes::Etag)
        .send()
        .await
        .expect("Should get object attributes");

    assert_eq!(response.object_size(), Some(content.len() as i64));
    assert!(response.e_tag().is_some(), "Should have ETag");
}

/// Test GetObjectAttributes returns ETag.
/// MinIO: testGetObjectAttributesETag
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_etag() {
    let ctx = S3TestContext::new().await;
    let key = "test-etag.txt";

    ctx.put(key, b"content").await;

    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .send()
        .await
        .expect("Should get object attributes");

    let etag = response.e_tag().expect("Should have ETag");
    assert!(!etag.is_empty(), "ETag should not be empty");
}

/// Test GetObjectAttributes returns ObjectSize.
/// MinIO: testGetObjectAttributesSize
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_size() {
    let ctx = S3TestContext::new().await;
    let key = "test-size.txt";
    let content = b"This is a test file with known content length.";

    ctx.put(key, content).await;

    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("Should get object attributes");

    assert_eq!(
        response.object_size(),
        Some(content.len() as i64),
        "Object size should match content length"
    );
}

/// Test GetObjectAttributes returns StorageClass.
/// MinIO: testGetObjectAttributesStorageClass
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_storage_class() {
    let ctx = S3TestContext::new().await;
    let key = "test-storage-class.txt";

    ctx.put(key, b"content").await;

    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::StorageClass)
        .send()
        .await
        .expect("Should get object attributes");

    // Default storage class is STANDARD
    assert!(response.storage_class().is_some(), "Should have storage class");
}

/// Test GetObjectAttributes on non-existent object.
/// MinIO: testGetObjectAttributesNonExistent
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_non_existent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key("nonexistent-key")
        .object_attributes(ObjectAttributes::Etag)
        .send()
        .await;

    assert!(result.is_err(), "Should fail on non-existent object");
}

/// Test GetObjectAttributes with multiple attributes.
/// MinIO: testGetObjectAttributesCombined
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_combined() {
    let ctx = S3TestContext::new().await;
    let key = "test-combined.txt";
    let content = b"Combined attributes test content";

    ctx.put(key, content).await;

    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Etag)
        .object_attributes(ObjectAttributes::ObjectSize)
        .object_attributes(ObjectAttributes::StorageClass)
        .send()
        .await
        .expect("Should get all requested attributes");

    assert!(response.e_tag().is_some(), "Should have ETag");
    assert_eq!(response.object_size(), Some(content.len() as i64));
    assert!(response.storage_class().is_some(), "Should have storage class");
}

/// Test GetObjectAttributes with checksum.
/// MinIO: testGetObjectAttributesWithChecksum
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_checksum() {
    let ctx = S3TestContext::new().await;
    let key = "test-checksum.txt";

    ctx.put(key, b"content for checksum").await;

    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::Checksum)
        .send()
        .await
        .expect("Should get object attributes");

    // Checksum may or may not be present depending on how object was uploaded
    // This test verifies the API works, not that checksum is always present
    let _ = response.checksum();
}

/// Test GetObjectAttributes on versioned object.
/// MinIO: testGetObjectAttributesVersion
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_version() {
    let ctx = S3TestContext::with_versioning().await;
    let key = "versioned-object.txt";

    // Create first version
    let put1 = ctx.put(key, b"version 1").await;
    let version1 = put1.version_id().expect("Should have version ID");

    // Create second version
    let put2 = ctx.put(key, b"version 2 - longer").await;
    let version2 = put2.version_id().expect("Should have version ID");

    // Get attributes for first version
    let response1 = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .version_id(version1)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("Should get attributes for version 1");

    assert_eq!(response1.object_size(), Some(9), "Version 1 should be 9 bytes");

    // Get attributes for second version
    let response2 = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .version_id(version2)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("Should get attributes for version 2");

    assert_eq!(response2.object_size(), Some(19), "Version 2 should be 19 bytes");
}

/// Test GetObjectAttributes for multipart uploaded object.
/// MinIO: testGetObjectAttributesMultipart
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_multipart() {
    let ctx = S3TestContext::new().await;
    let key = "multipart-object.txt";

    // Create a multipart upload
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .send()
        .await
        .expect("Should create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Upload a part (minimum 5MB for real S3, but our test server may accept smaller)
    let part_content = vec![b'a'; 5 * 1024 * 1024]; // 5MB
    let upload_response = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(part_content.into())
        .send()
        .await
        .expect("Should upload part");

    let etag = upload_response.e_tag().unwrap();

    // Complete the multipart upload
    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder().part_number(1).e_tag(etag).build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("Should complete multipart upload");

    // Get object attributes including parts info
    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::ObjectParts)
        .object_attributes(ObjectAttributes::ObjectSize)
        .send()
        .await
        .expect("Should get multipart object attributes");

    assert_eq!(response.object_size(), Some(5 * 1024 * 1024));
    assert!(response.object_parts().is_some(), "Should have object parts info");
}

/// Test GetObjectAttributes returns ObjectParts for multipart.
/// MinIO: testGetObjectAttributesParts
#[tokio::test]
#[ignore = "GetObjectAttributes not implemented"]
async fn test_get_object_attributes_parts() {
    let ctx = S3TestContext::new().await;
    let key = "multipart-parts.txt";

    // Create and complete a multipart upload with 2 parts
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .send()
        .await
        .expect("Should create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Upload two parts
    let part1 = vec![b'a'; 5 * 1024 * 1024];
    let part2 = vec![b'b'; 5 * 1024 * 1024];

    let upload1 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(part1.into())
        .send()
        .await
        .expect("Should upload part 1");

    let upload2 = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(2)
        .body(part2.into())
        .send()
        .await
        .expect("Should upload part 2");

    // Complete upload
    ctx.client
        .complete_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .multipart_upload(
            aws_sdk_s3::types::CompletedMultipartUpload::builder()
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(1)
                        .e_tag(upload1.e_tag().unwrap())
                        .build(),
                )
                .parts(
                    aws_sdk_s3::types::CompletedPart::builder()
                        .part_number(2)
                        .e_tag(upload2.e_tag().unwrap())
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("Should complete multipart upload");

    // Get parts info
    let response = ctx
        .client
        .get_object_attributes()
        .bucket(&ctx.bucket)
        .key(key)
        .object_attributes(ObjectAttributes::ObjectParts)
        .send()
        .await
        .expect("Should get object parts");

    let parts = response.object_parts().expect("Should have parts");
    assert_eq!(parts.total_parts_count(), Some(2), "Should have 2 parts");
}
