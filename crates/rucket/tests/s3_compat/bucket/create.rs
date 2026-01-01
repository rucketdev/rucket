//! Bucket creation tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_create_*
//! - MinIO Mint: aws-sdk-go, minio-go bucket creation tests

use crate::{random_bucket_name, S3TestContext};

/// Test creating a bucket with a simple name.
/// Ceph: test_bucket_create_naming_good_simple
#[tokio::test]
async fn test_bucket_create_simple() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    ctx.client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should create bucket");

    // Verify bucket exists
    ctx.client
        .head_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Bucket should exist");
}

/// Test creating a bucket with the minimum valid name length (3 chars).
/// Ceph: test_bucket_create_naming_good_long (adapted)
#[tokio::test]
async fn test_bucket_create_name_min_length() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "aaa"; // 3 chars - minimum

    ctx.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Should create bucket with 3-char name");

    ctx.client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Bucket should exist");
}

/// Test creating a bucket with the maximum valid name length (63 chars).
/// Ceph: test_bucket_create_naming_good_long
#[tokio::test]
async fn test_bucket_create_name_max_length() {
    let ctx = S3TestContext::without_bucket().await;
    // 63 characters - maximum
    let bucket = "a".repeat(63);

    ctx.client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should create bucket with 63-char name");

    ctx.client
        .head_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Bucket should exist");
}

/// Test that bucket names with dots are valid.
/// Ceph: test_bucket_create_naming_good_contains_period
#[tokio::test]
async fn test_bucket_create_name_with_dots() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "my.bucket.name";

    ctx.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Should create bucket with dots in name");

    ctx.client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Bucket should exist");
}

/// Test that bucket names with hyphens are valid.
/// Ceph: test_bucket_create_naming_good_contains_hyphen
#[tokio::test]
async fn test_bucket_create_name_with_hyphens() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "my-bucket-name";

    ctx.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Should create bucket with hyphens in name");

    ctx.client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Bucket should exist");
}

/// Test that bucket names can start with a letter.
/// Ceph: test_bucket_create_naming_good_starts_alpha
#[tokio::test]
async fn test_bucket_create_name_starts_with_letter() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "mybucket123";

    ctx.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Should create bucket starting with letter");

    ctx.client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Bucket should exist");
}

/// Test that bucket names can start with a digit.
/// Ceph: test_bucket_create_naming_good_starts_digit
#[tokio::test]
async fn test_bucket_create_name_starts_with_digit() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "123bucket";

    ctx.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Should create bucket starting with digit");

    ctx.client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Bucket should exist");
}

/// Test creating the same bucket twice returns BucketAlreadyOwnedByYou.
/// Ceph: test_bucket_create_exists
#[tokio::test]
async fn test_bucket_create_already_exists_same_owner() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = random_bucket_name();

    // Create bucket first time
    ctx.client
        .create_bucket()
        .bucket(&bucket)
        .send()
        .await
        .expect("Should create bucket");

    // Create same bucket again - should succeed (idempotent) or return BucketAlreadyOwnedByYou
    let result = ctx.client.create_bucket().bucket(&bucket).send().await;

    // AWS S3 returns 200 OK for same owner re-creating bucket
    // Some implementations return BucketAlreadyOwnedByYou
    // We accept both as valid
    match result {
        Ok(_) => {} // Idempotent success
        Err(e) => {
            let error_str = format!("{:?}", e);
            assert!(
                error_str.contains("BucketAlreadyOwnedByYou")
                    || error_str.contains("BucketAlreadyExists"),
                "Expected BucketAlreadyOwnedByYou or BucketAlreadyExists, got: {}",
                error_str
            );
        }
    }
}

/// Test that bucket names too short fail.
/// Ceph: test_bucket_create_naming_bad_short
#[tokio::test]
async fn test_bucket_create_name_too_short() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "ab"; // 2 chars - too short

    let result = ctx.client.create_bucket().bucket(bucket).send().await;

    assert!(result.is_err(), "Should reject bucket name too short");
}

/// Test that bucket names too long fail.
/// Ceph: test_bucket_create_naming_bad_long
#[tokio::test]
async fn test_bucket_create_name_too_long() {
    let ctx = S3TestContext::without_bucket().await;
    // 64 characters - too long
    let bucket = "a".repeat(64);

    let result = ctx.client.create_bucket().bucket(&bucket).send().await;

    assert!(result.is_err(), "Should reject bucket name too long");
}

/// Test that bucket names with uppercase fail.
/// Ceph: test_bucket_create_naming_bad_short (adapted)
#[tokio::test]
async fn test_bucket_create_name_uppercase_rejected() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "MyBucket";

    let result = ctx.client.create_bucket().bucket(bucket).send().await;

    assert!(result.is_err(), "Should reject bucket name with uppercase");
}

/// Test that bucket names starting with hyphen fail.
/// Ceph: test_bucket_create_naming_bad_starts_hyphen
#[tokio::test]
async fn test_bucket_create_name_starts_with_hyphen() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "-mybucket";

    let result = ctx.client.create_bucket().bucket(bucket).send().await;

    assert!(result.is_err(), "Should reject bucket name starting with hyphen");
}

/// Test that bucket names ending with hyphen fail.
/// Ceph: test_bucket_create_naming_bad_ends_hyphen
#[tokio::test]
async fn test_bucket_create_name_ends_with_hyphen() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "mybucket-";

    let result = ctx.client.create_bucket().bucket(bucket).send().await;

    assert!(result.is_err(), "Should reject bucket name ending with hyphen");
}

/// Test that bucket names with underscores fail (path-style).
/// S3 DNS naming rules don't allow underscores.
#[tokio::test]
async fn test_bucket_create_name_with_underscore() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "my_bucket";

    let result = ctx.client.create_bucket().bucket(bucket).send().await;

    assert!(result.is_err(), "Should reject bucket name with underscore");
}

/// Test that bucket names that look like IP addresses fail.
/// Ceph: test_bucket_create_naming_bad_ip
#[tokio::test]
async fn test_bucket_create_name_looks_like_ip() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "192.168.1.1";

    let result = ctx.client.create_bucket().bucket(bucket).send().await;

    assert!(result.is_err(), "Should reject bucket name that looks like IP");
}

/// Test that empty bucket name fails.
/// Ceph: test_bucket_create_naming_bad_empty
#[tokio::test]
async fn test_bucket_create_empty_name() {
    let ctx = S3TestContext::without_bucket().await;

    let result = ctx.client.create_bucket().bucket("").send().await;

    assert!(result.is_err(), "Should reject empty bucket name");
}

/// Test creating multiple buckets.
/// Ceph: test_bucket_list_many
#[tokio::test]
async fn test_bucket_create_multiple() {
    let ctx = S3TestContext::without_bucket().await;

    let buckets: Vec<String> = (0..5).map(|_| random_bucket_name()).collect();

    for bucket in &buckets {
        ctx.client
            .create_bucket()
            .bucket(bucket)
            .send()
            .await
            .expect("Should create bucket");
    }

    // Verify all buckets exist
    let response = ctx.client.list_buckets().send().await.expect("Should list buckets");

    let listed_names: Vec<&str> = response
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .collect();

    for bucket in &buckets {
        assert!(
            listed_names.contains(&bucket.as_str()),
            "Bucket {} should be listed",
            bucket
        );
    }
}

/// Test that bucket with special but valid characters works.
/// Combination of dots, hyphens, and numbers.
#[tokio::test]
async fn test_bucket_create_complex_valid_name() {
    let ctx = S3TestContext::without_bucket().await;
    let bucket = "my-bucket.v2.test-123";

    ctx.client
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Should create bucket with complex valid name");

    ctx.client
        .head_bucket()
        .bucket(bucket)
        .send()
        .await
        .expect("Bucket should exist");
}
