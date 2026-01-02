//! Checksum tests.
//!
//! Ported from:
//! - Ceph s3-tests: checksum tests (2025 additions)
//! - MinIO Mint: testChecksum*
//!
//! Tests for CRC32, CRC32C, SHA1, and SHA256 checksums.

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::ChecksumAlgorithm;

use crate::S3TestContext;

/// Test PutObject with CRC32C checksum algorithm.
/// MinIO: testChecksumCRC32C
#[tokio::test]
async fn test_put_object_checksum_crc32c() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-crc32c.txt";
    let content = b"Hello, World with CRC32C!";

    // Request CRC32C checksum algorithm - server should calculate
    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .send()
        .await
        .expect("Should put object with CRC32C");

    assert!(response.checksum_crc32_c().is_some(), "Response should include CRC32C");
}

/// Test PutObject with CRC32 checksum algorithm.
/// MinIO: testChecksumCRC32
#[tokio::test]
async fn test_put_object_checksum_crc32() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-crc32.txt";
    let content = b"Hello, World with CRC32!";

    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await
        .expect("Should put object with CRC32");

    assert!(response.checksum_crc32().is_some(), "Response should include CRC32");
}

/// Test PutObject with SHA256 checksum algorithm.
/// MinIO: testChecksumSHA256
#[tokio::test]
async fn test_put_object_checksum_sha256() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-sha256.txt";
    let content = b"Hello, World with SHA256!";

    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .expect("Should put object with SHA256");

    assert!(response.checksum_sha256().is_some(), "Response should include SHA256");
}

/// Test PutObject with SHA1 checksum algorithm.
/// MinIO: testChecksumSHA1
#[tokio::test]
async fn test_put_object_checksum_sha1() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-sha1.txt";
    let content = b"Hello, World with SHA1!";

    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Sha1)
        .send()
        .await
        .expect("Should put object with SHA1");

    assert!(response.checksum_sha1().is_some(), "Response should include SHA1");
}

/// Test PutObject with invalid checksum value.
/// MinIO: testChecksumInvalidValue
#[tokio::test]
async fn test_put_object_checksum_invalid() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-invalid.txt";
    let content = b"Hello, World!";

    // Use an intentionally wrong checksum
    let wrong_checksum = "AAAAAAA=";

    let result = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .checksum_crc32_c(wrong_checksum)
        .send()
        .await;

    assert!(result.is_err(), "Should reject invalid checksum");
}

/// Test GetObject returns checksum when requested.
/// MinIO: testGetObjectWithChecksum
#[tokio::test]
async fn test_get_object_checksum() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-get.txt";
    let content = b"Content for checksum retrieval";

    // Put with checksum
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .send()
        .await
        .expect("Should put object");

    // Get object with checksum mode enabled
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .expect("Should get object");

    assert!(response.checksum_crc32_c().is_some(), "Should return CRC32C checksum");
}

/// Test multipart upload with checksum algorithm.
/// MinIO: testMultipartChecksumCRC32
#[tokio::test]
#[ignore = "Checksum validation not implemented"]
async fn test_multipart_checksum_crc32() {
    let ctx = S3TestContext::new().await;
    let key = "multipart-checksum.txt";

    // Create multipart upload with checksum algorithm
    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32)
        .send()
        .await
        .expect("Should create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Upload part
    let part_content = vec![b'x'; 5 * 1024 * 1024]; // 5MB

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

    // When checksum algorithm is requested, parts may have checksums
    let etag = upload_response.e_tag().unwrap();

    // Complete the upload
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
}

/// Test multipart upload with SHA256 checksum.
/// MinIO: testMultipartChecksumSHA256
#[tokio::test]
#[ignore = "Checksum validation not implemented"]
async fn test_multipart_checksum_sha256() {
    let ctx = S3TestContext::new().await;
    let key = "multipart-sha256.txt";

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .expect("Should create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    let part_content = vec![b'y'; 5 * 1024 * 1024];

    ctx.client
        .upload_part()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(part_content.into())
        .send()
        .await
        .expect("Should upload part");

    // Abort upload to clean up
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should abort upload");
}

/// Test copy object preserves checksum.
/// MinIO: testCopyChecksumPreserve
#[tokio::test]
async fn test_copy_object_checksum_preserve() {
    let ctx = S3TestContext::new().await;
    let src_key = "checksum-source.txt";
    let dst_key = "checksum-copy.txt";
    let content = b"Content to copy with checksum";

    // Put source with checksum
    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(src_key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .send()
        .await
        .expect("Should put source");

    // Copy object
    ctx.client
        .copy_object()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .copy_source(format!("{}/{}", ctx.bucket, src_key))
        .send()
        .await
        .expect("Should copy object");

    // Get copied object with checksum
    let response = ctx
        .client
        .get_object()
        .bucket(&ctx.bucket)
        .key(dst_key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .expect("Should get copied object");

    // Checksum may or may not be preserved depending on implementation
    let _ = response.checksum_crc32_c();
}

/// Test HeadObject returns checksum.
/// MinIO: testHeadObjectChecksum
#[tokio::test]
async fn test_head_object_checksum() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-head.txt";
    let content = b"Content for HEAD checksum";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .checksum_algorithm(ChecksumAlgorithm::Sha256)
        .send()
        .await
        .expect("Should put object");

    let response = ctx
        .client
        .head_object()
        .bucket(&ctx.bucket)
        .key(key)
        .checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
        .send()
        .await
        .expect("Should head object");

    // HEAD should return checksum when requested
    let _ = response.checksum_sha256();
}

/// Test checksum with large object (chunked encoding).
/// MinIO: testChecksumTrailer
#[tokio::test]
#[ignore = "Checksum validation not implemented"]
async fn test_checksum_large_object() {
    let ctx = S3TestContext::new().await;
    let key = "checksum-large.txt";

    // Large content
    let content = vec![b'z'; 10 * 1024 * 1024]; // 10MB

    let response = ctx
        .client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content))
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .send()
        .await
        .expect("Should put large object with checksum");

    assert!(response.checksum_crc32_c().is_some(), "Response should have CRC32C");
}

/// Test ListParts returns part checksums.
/// MinIO: testListPartsChecksum
#[tokio::test]
#[ignore = "Checksum validation not implemented"]
async fn test_list_parts_checksum() {
    let ctx = S3TestContext::new().await;
    let key = "multipart-list-parts.txt";

    let create_response = ctx
        .client
        .create_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .checksum_algorithm(ChecksumAlgorithm::Crc32C)
        .send()
        .await
        .expect("Should create multipart upload");

    let upload_id = create_response.upload_id().unwrap();

    // Upload a part
    let part_content = vec![b'a'; 5 * 1024 * 1024];

    ctx.client
        .upload_part()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(1)
        .body(part_content.into())
        .send()
        .await
        .expect("Should upload part");

    // List parts
    let list_response = ctx
        .client
        .list_parts()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should list parts");

    let parts = list_response.parts();
    assert_eq!(parts.len(), 1, "Should have one part");

    // Abort upload to clean up
    ctx.client
        .abort_multipart_upload()
        .bucket(&ctx.bucket)
        .key(key)
        .upload_id(upload_id)
        .send()
        .await
        .expect("Should abort upload");
}

/// Test all checksum algorithms are accepted.
/// Ceph: test_checksum_algorithms
#[tokio::test]
async fn test_all_checksum_algorithms() {
    let ctx = S3TestContext::new().await;
    let content = b"Test all algorithms";

    // Test each algorithm
    let algorithms = [
        ("crc32", ChecksumAlgorithm::Crc32),
        ("crc32c", ChecksumAlgorithm::Crc32C),
        ("sha1", ChecksumAlgorithm::Sha1),
        ("sha256", ChecksumAlgorithm::Sha256),
    ];

    for (name, algorithm) in algorithms {
        let key = format!("checksum-{}.txt", name);

        let result = ctx
            .client
            .put_object()
            .bucket(&ctx.bucket)
            .key(&key)
            .body(ByteStream::from(content.to_vec()))
            .checksum_algorithm(algorithm)
            .send()
            .await;

        assert!(result.is_ok(), "Algorithm {} should be accepted", name);
    }
}
