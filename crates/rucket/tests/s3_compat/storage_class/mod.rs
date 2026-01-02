//! Storage class tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_storage_class_*
//! - AWS documentation: Storage classes

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{ObjectStorageClass, StorageClass};

use crate::S3TestContext;

/// Test putting object with STANDARD storage class.
/// Ceph: test_storage_class_standard
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_standard() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("standard.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::Standard)
        .send()
        .await
        .expect("Should accept STANDARD storage class");

    let response = ctx.head("standard.txt").await;
    assert_eq!(response.storage_class(), Some(&StorageClass::Standard));
}

/// Test putting object with REDUCED_REDUNDANCY storage class.
/// Ceph: test_storage_class_reduced
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_reduced_redundancy() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("reduced.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::ReducedRedundancy)
        .send()
        .await
        .expect("Should accept REDUCED_REDUNDANCY storage class");
}

/// Test putting object with STANDARD_IA storage class.
/// Ceph: test_storage_class_ia
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_standard_ia() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("standard-ia.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .expect("Should accept STANDARD_IA storage class");
}

/// Test putting object with ONEZONE_IA storage class.
/// Ceph: test_storage_class_onezone_ia
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_onezone_ia() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("onezone-ia.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::OnezoneIa)
        .send()
        .await
        .expect("Should accept ONEZONE_IA storage class");
}

/// Test putting object with INTELLIGENT_TIERING storage class.
/// Ceph: test_storage_class_intelligent_tiering
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_intelligent_tiering() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("intelligent.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::IntelligentTiering)
        .send()
        .await
        .expect("Should accept INTELLIGENT_TIERING storage class");
}

/// Test putting object with GLACIER storage class.
/// Ceph: test_storage_class_glacier
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_glacier() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("glacier.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::Glacier)
        .send()
        .await
        .expect("Should accept GLACIER storage class");
}

/// Test putting object with DEEP_ARCHIVE storage class.
/// Ceph: test_storage_class_deep
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_deep_archive() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("deep-archive.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::DeepArchive)
        .send()
        .await
        .expect("Should accept DEEP_ARCHIVE storage class");
}

/// Test copy object preserves storage class.
/// Ceph: test_storage_class_copy
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_copy() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .expect("Should put source");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .send()
        .await
        .expect("Should copy object");

    let response = ctx.head("dest.txt").await;
    // Copy should preserve storage class
    assert_eq!(response.storage_class(), Some(&StorageClass::StandardIa));
}

/// Test copy object with different storage class.
/// Ceph: test_storage_class_copy_change
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_copy_change() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("source.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::Standard)
        .send()
        .await
        .expect("Should put source");

    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key("dest.txt")
        .copy_source(format!("{}/source.txt", ctx.bucket))
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .expect("Should copy with new storage class");

    let response = ctx.head("dest.txt").await;
    assert_eq!(response.storage_class(), Some(&StorageClass::StandardIa));
}

/// Test multipart upload with storage class.
/// Ceph: test_storage_class_multipart
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_multipart() {
    let ctx = S3TestContext::new().await;

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .expect("Should create multipart with storage class");

    let upload_id = create_response.upload_id().unwrap();

    let part = ctx
        .client
        .upload_part()
        .bucket(&ctx.bucket)
        .key("multipart.txt")
        .upload_id(upload_id)
        .part_number(1)
        .body(vec![b'x'; 5 * 1024 * 1024].into())
        .send()
        .await
        .expect("Should upload part");

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
        .expect("Should complete multipart");

    let response = ctx.head("multipart.txt").await;
    assert_eq!(response.storage_class(), Some(&StorageClass::StandardIa));
}

/// Test lifecycle transition changes storage class.
/// Ceph: test_storage_class_transition
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_lifecycle_transition() {
    let _ctx = S3TestContext::new().await;
    // Would need lifecycle configuration and waiting
}

/// Test default storage class.
/// Ceph: test_storage_class_default
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_default() {
    let ctx = S3TestContext::new().await;

    ctx.put("default.txt", b"content").await;

    let response = ctx.head("default.txt").await;
    // Default should be STANDARD (or None which means STANDARD)
    let class = response.storage_class();
    assert!(
        class.is_none() || class == Some(&StorageClass::Standard),
        "Default should be STANDARD"
    );
}

/// Test restore from GLACIER.
/// Ceph: test_storage_class_restore
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_restore() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("archived.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::Glacier)
        .send()
        .await
        .expect("Should put to GLACIER");

    // Initiate restore
    ctx.client
        .restore_object()
        .bucket(&ctx.bucket)
        .key("archived.txt")
        .restore_request(
            aws_sdk_s3::types::RestoreRequest::builder()
                .days(1)
                .glacier_job_parameters(
                    aws_sdk_s3::types::GlacierJobParameters::builder()
                        .tier(aws_sdk_s3::types::Tier::Standard)
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("Should initiate restore");
}

/// Test listing objects returns storage class.
/// Ceph: test_storage_class_list
#[tokio::test]
#[ignore = "Storage classes not implemented"]
async fn test_storage_class_list() {
    let ctx = S3TestContext::new().await;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key("listed.txt")
        .body(ByteStream::from(b"content".to_vec()))
        .storage_class(StorageClass::StandardIa)
        .send()
        .await
        .expect("Should put");

    let response = ctx.list_objects().await;
    let objects = response.contents();
    assert!(!objects.is_empty(), "Should have objects");
    assert_eq!(objects[0].storage_class(), Some(&ObjectStorageClass::StandardIa));
}
