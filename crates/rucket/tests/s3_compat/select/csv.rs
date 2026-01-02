//! S3 Select CSV tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_s3select.py CSV tests
//! - MinIO Mint: testSelectObject CSV tests

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompressionType, CsvInput, CsvOutput, ExpressionType, FileHeaderInfo, InputSerialization,
    OutputSerialization, SelectObjectContentEventStream,
};

use crate::S3TestContext;

/// Create a simple CSV file content.
fn create_csv_content() -> Vec<u8> {
    b"id,name,value\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n".to_vec()
}

/// Create a CSV file without headers.
fn create_csv_no_header() -> Vec<u8> {
    b"1,Alice,100\n2,Bob,200\n3,Charlie,300\n".to_vec()
}

/// Test basic S3 Select on CSV.
/// Ceph: test_s3select_count
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_count() {
    let ctx = S3TestContext::new().await;
    let key = "data.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_content()))
        .send()
        .await
        .expect("Should upload CSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT COUNT(*) FROM s3object")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select from CSV");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("3"), "Count should be 3");
}

/// Test S3 Select with WHERE clause.
/// Ceph: test_s3select_where
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_where() {
    let ctx = S3TestContext::new().await;
    let key = "data.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_content()))
        .send()
        .await
        .expect("Should upload CSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT name FROM s3object WHERE CAST(value AS INT) > 150")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select with WHERE");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("Bob"), "Bob has value > 150");
    assert!(result.contains("Charlie"), "Charlie has value > 150");
    assert!(!result.contains("Alice"), "Alice has value <= 150");
}

/// Test S3 Select with SUM aggregation.
/// Ceph: test_s3select_sum
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_sum() {
    let ctx = S3TestContext::new().await;
    let key = "data.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_content()))
        .send()
        .await
        .expect("Should upload CSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT SUM(CAST(value AS INT)) FROM s3object")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select SUM");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("600"), "Sum should be 600 (100+200+300)");
}

/// Test S3 Select with MIN/MAX.
/// Ceph: test_s3select_min_max
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_min_max() {
    let ctx = S3TestContext::new().await;
    let key = "data.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_content()))
        .send()
        .await
        .expect("Should upload CSV");

    // Test MIN
    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT MIN(CAST(value AS INT)) FROM s3object")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select MIN");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("100"), "MIN should be 100");
}

/// Test S3 Select with AVG.
/// Ceph: test_s3select_avg
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_avg() {
    let ctx = S3TestContext::new().await;
    let key = "data.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_content()))
        .send()
        .await
        .expect("Should upload CSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT AVG(CAST(value AS INT)) FROM s3object")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select AVG");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("200"), "AVG should be 200");
}

/// Test S3 Select with custom delimiter.
/// Ceph: test_s3select_delimiter
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_custom_delimiter() {
    let ctx = S3TestContext::new().await;
    let key = "data.tsv";

    // Tab-separated values
    let content = b"id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.to_vec()))
        .send()
        .await
        .expect("Should upload TSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT * FROM s3object")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .field_delimiter("\t")
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select from TSV");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("Alice"), "Should find Alice");
}

/// Test S3 Select on CSV without headers.
/// Ceph: test_s3select_no_header
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_no_header() {
    let ctx = S3TestContext::new().await;
    let key = "data_no_header.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_no_header()))
        .send()
        .await
        .expect("Should upload CSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT _1, _2 FROM s3object WHERE CAST(_1 AS INT) = 2")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::None)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select with positional columns");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("2"), "Should find row with id=2");
    assert!(result.contains("Bob"), "Should find Bob");
}

/// Test S3 Select with LIKE operator.
/// Ceph: test_s3select_like
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_like() {
    let ctx = S3TestContext::new().await;
    let key = "data.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_content()))
        .send()
        .await
        .expect("Should upload CSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT name FROM s3object WHERE name LIKE '%li%'")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select with LIKE");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("Alice"), "Alice matches %li%");
    assert!(result.contains("Charlie"), "Charlie matches %li%");
    assert!(!result.contains("Bob"), "Bob doesn't match %li%");
}

/// Test S3 Select with BETWEEN.
/// Ceph: test_s3select_between
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_between() {
    let ctx = S3TestContext::new().await;
    let key = "data.csv";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_csv_content()))
        .send()
        .await
        .expect("Should upload CSV");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT name FROM s3object WHERE CAST(value AS INT) BETWEEN 150 AND 250")
        .input_serialization(
            InputSerialization::builder()
                .csv(
                    CsvInput::builder()
                        .file_header_info(FileHeaderInfo::Use)
                        .build(),
                )
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await
        .expect("Should select with BETWEEN");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("Bob"), "Bob's value (200) is between 150 and 250");
    assert!(!result.contains("Alice"), "Alice's value (100) is not in range");
    assert!(!result.contains("Charlie"), "Charlie's value (300) is not in range");
}

/// Test S3 Select on non-existent object.
/// Ceph: test_s3select_nonexistent
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_csv_nonexistent() {
    let ctx = S3TestContext::new().await;

    let result = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key("nonexistent.csv")
        .expression_type(ExpressionType::Sql)
        .expression("SELECT * FROM s3object")
        .input_serialization(
            InputSerialization::builder()
                .csv(CsvInput::builder().build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(OutputSerialization::builder().csv(CsvOutput::builder().build()).build())
        .send()
        .await;

    assert!(result.is_err(), "Should fail on non-existent object");
}
