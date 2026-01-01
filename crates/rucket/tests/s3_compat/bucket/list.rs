//! Bucket listing tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_bucket_list_*
//! - MinIO Mint: ListBuckets tests

use crate::{random_bucket_name, S3TestContext};

/// Test listing buckets when none exist.
/// Ceph: test_bucket_list_empty
#[tokio::test]
async fn test_bucket_list_empty() {
    let ctx = S3TestContext::without_bucket().await;

    let response = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list buckets");

    assert!(response.buckets().is_empty(), "Should have no buckets");
}

/// Test listing a single bucket.
/// Ceph: test_bucket_list_one
#[tokio::test]
async fn test_bucket_list_one() {
    let ctx = S3TestContext::new().await;

    let response = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list buckets");

    let buckets = response.buckets();
    assert_eq!(buckets.len(), 1, "Should have exactly one bucket");
    assert_eq!(
        buckets[0].name(),
        Some(ctx.bucket.as_str()),
        "Bucket name should match"
    );
}

/// Test listing multiple buckets.
/// Ceph: test_bucket_list_many
#[tokio::test]
async fn test_bucket_list_many() {
    let ctx = S3TestContext::without_bucket().await;

    let bucket_names: Vec<String> = (0..5).map(|_| random_bucket_name()).collect();

    for name in &bucket_names {
        ctx.client
            .create_bucket()
            .bucket(name)
            .send()
            .await
            .expect("Should create bucket");
    }

    let response = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list buckets");

    let listed: Vec<&str> = response
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .collect();

    assert_eq!(listed.len(), 5, "Should have 5 buckets");

    for name in &bucket_names {
        assert!(
            listed.contains(&name.as_str()),
            "Bucket {} should be in list",
            name
        );
    }
}

/// Test that buckets are returned in sorted order.
/// Ceph: test_bucket_list_ordered
#[tokio::test]
async fn test_bucket_list_sorted() {
    let ctx = S3TestContext::without_bucket().await;

    // Create buckets in reverse order
    let names = vec!["zzz-bucket", "mmm-bucket", "aaa-bucket"];
    for name in &names {
        ctx.client
            .create_bucket()
            .bucket(*name)
            .send()
            .await
            .expect("Should create bucket");
    }

    let response = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list buckets");

    let listed: Vec<&str> = response
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .collect();

    // Verify sorted order
    let mut sorted = listed.clone();
    sorted.sort();
    assert_eq!(listed, sorted, "Buckets should be in sorted order");
}

/// Test that bucket listing includes creation date.
/// Ceph: test_bucket_list_creation_date
#[tokio::test]
async fn test_bucket_list_has_creation_date() {
    let ctx = S3TestContext::new().await;

    let response = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list buckets");

    let bucket = response.buckets().first().expect("Should have a bucket");
    assert!(
        bucket.creation_date().is_some(),
        "Bucket should have creation date"
    );
}

/// Test listing buckets after one is deleted.
/// Ceph: test_bucket_list_after_delete
#[tokio::test]
async fn test_bucket_list_after_delete() {
    let ctx = S3TestContext::without_bucket().await;

    let bucket1 = random_bucket_name();
    let bucket2 = random_bucket_name();

    ctx.client
        .create_bucket()
        .bucket(&bucket1)
        .send()
        .await
        .expect("Should create bucket1");
    ctx.client
        .create_bucket()
        .bucket(&bucket2)
        .send()
        .await
        .expect("Should create bucket2");

    // Delete first bucket
    ctx.client
        .delete_bucket()
        .bucket(&bucket1)
        .send()
        .await
        .expect("Should delete bucket1");

    let response = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list buckets");

    let listed: Vec<&str> = response
        .buckets()
        .iter()
        .filter_map(|b| b.name())
        .collect();

    assert_eq!(listed.len(), 1, "Should have 1 bucket");
    assert!(
        listed.contains(&bucket2.as_str()),
        "bucket2 should still exist"
    );
    assert!(
        !listed.contains(&bucket1.as_str()),
        "bucket1 should be gone"
    );
}

/// Test listing many buckets (stress test).
#[tokio::test]
async fn test_bucket_list_many_buckets() {
    let ctx = S3TestContext::without_bucket().await;

    let count = 20;
    let bucket_names: Vec<String> = (0..count).map(|_| random_bucket_name()).collect();

    for name in &bucket_names {
        ctx.client
            .create_bucket()
            .bucket(name)
            .send()
            .await
            .expect("Should create bucket");
    }

    let response = ctx
        .client
        .list_buckets()
        .send()
        .await
        .expect("Should list buckets");

    assert_eq!(
        response.buckets().len(),
        count,
        "Should list all {} buckets",
        count
    );
}
