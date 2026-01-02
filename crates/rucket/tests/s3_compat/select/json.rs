//! S3 Select JSON tests.
//!
//! Ported from:
//! - Ceph s3-tests: test_s3select.py JSON tests
//! - MinIO Mint: testSelectObjectJSON

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{
    CompressionType, ExpressionType, InputSerialization, JsonInput, JsonOutput, JsonType,
    OutputSerialization, SelectObjectContentEventStream,
};

use crate::S3TestContext;

/// Create a JSON Lines file content.
fn create_jsonl_content() -> Vec<u8> {
    r#"{"id": 1, "name": "Alice", "value": 100}
{"id": 2, "name": "Bob", "value": 200}
{"id": 3, "name": "Charlie", "value": 300}
"#
    .as_bytes()
    .to_vec()
}

/// Create a JSON document (single object with array).
fn create_json_document() -> Vec<u8> {
    r#"{
  "records": [
    {"id": 1, "name": "Alice", "value": 100},
    {"id": 2, "name": "Bob", "value": 200},
    {"id": 3, "name": "Charlie", "value": 300}
  ]
}"#
    .as_bytes()
    .to_vec()
}

/// Test S3 Select on JSON Lines.
/// Ceph: test_s3select_json_count
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_lines_count() {
    let ctx = S3TestContext::new().await;
    let key = "data.jsonl";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_jsonl_content()))
        .send()
        .await
        .expect("Should upload JSON Lines");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT COUNT(*) FROM s3object s")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("Should select from JSON Lines");

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

/// Test S3 Select on JSON with field access.
/// Ceph: test_s3select_json_field
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_field_access() {
    let ctx = S3TestContext::new().await;
    let key = "data.jsonl";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_jsonl_content()))
        .send()
        .await
        .expect("Should upload JSON Lines");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT s.name FROM s3object s WHERE s.id = 2")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("Should select JSON field");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("Bob"), "Should find Bob for id=2");
}

/// Test S3 Select on JSON with WHERE clause.
/// Ceph: test_s3select_json_where
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_where() {
    let ctx = S3TestContext::new().await;
    let key = "data.jsonl";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_jsonl_content()))
        .send()
        .await
        .expect("Should upload JSON Lines");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT s.name FROM s3object s WHERE s.value > 150")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
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

/// Test S3 Select on JSON Document type.
/// Ceph: test_s3select_json_document
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_document() {
    let ctx = S3TestContext::new().await;
    let key = "data.json";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_json_document()))
        .send()
        .await
        .expect("Should upload JSON Document");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT s.name FROM s3object[*].records[*] s")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Document).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("Should select from JSON Document");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("Alice"), "Should find Alice");
    assert!(result.contains("Bob"), "Should find Bob");
    assert!(result.contains("Charlie"), "Should find Charlie");
}

/// Test S3 Select with JSON aggregation.
/// Ceph: test_s3select_json_sum
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_sum() {
    let ctx = S3TestContext::new().await;
    let key = "data.jsonl";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(create_jsonl_content()))
        .send()
        .await
        .expect("Should upload JSON Lines");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT SUM(s.value) FROM s3object s")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
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

    assert!(result.contains("600"), "Sum should be 600");
}

/// Test S3 Select with nested JSON.
/// Ceph: test_s3select_json_nested
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_nested() {
    let ctx = S3TestContext::new().await;
    let key = "nested.jsonl";

    let content = r#"{"user": {"name": "Alice", "address": {"city": "Seattle"}}, "score": 100}
{"user": {"name": "Bob", "address": {"city": "Portland"}}, "score": 200}
"#;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .send()
        .await
        .expect("Should upload nested JSON");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT s.user.name, s.user.address.city FROM s3object s")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("Should select nested fields");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    assert!(result.contains("Alice"), "Should find Alice");
    assert!(result.contains("Seattle"), "Should find Seattle");
}

/// Test S3 Select with invalid JSON.
/// Ceph: test_s3select_json_invalid
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_invalid() {
    let ctx = S3TestContext::new().await;
    let key = "invalid.jsonl";

    let content = "not valid json\n";

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .send()
        .await
        .expect("Should upload file");

    let result = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT * FROM s3object s")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Lines).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
        .send()
        .await;

    // The request may succeed but streaming will fail, or it may fail immediately
    // depending on implementation
    if let Ok(mut response) = result {
        let stream_result = response.payload.recv().await;
        // Either no events or an error event expected
        match stream_result {
            Ok(Some(SelectObjectContentEventStream::Records(_))) => {
                panic!("Should not get valid records from invalid JSON")
            }
            _ => {} // Error or end of stream is expected
        }
    }
}

/// Test S3 Select with empty JSON array.
/// Ceph: test_s3select_json_empty
#[tokio::test]
#[ignore = "S3 Select not implemented"]
async fn test_select_json_empty() {
    let ctx = S3TestContext::new().await;
    let key = "empty.json";

    let content = r#"{"records": []}"#;

    ctx.client
        .put_object()
        .bucket(&ctx.bucket)
        .key(key)
        .body(ByteStream::from(content.as_bytes().to_vec()))
        .send()
        .await
        .expect("Should upload empty JSON");

    let mut response = ctx
        .client
        .select_object_content()
        .bucket(&ctx.bucket)
        .key(key)
        .expression_type(ExpressionType::Sql)
        .expression("SELECT * FROM s3object[*].records[*] s")
        .input_serialization(
            InputSerialization::builder()
                .json(JsonInput::builder().r#type(JsonType::Document).build())
                .compression_type(CompressionType::None)
                .build(),
        )
        .output_serialization(
            OutputSerialization::builder()
                .json(JsonOutput::builder().build())
                .build(),
        )
        .send()
        .await
        .expect("Should handle empty array");

    let mut result = String::new();
    while let Some(event) = response.payload.recv().await.expect("Should receive event") {
        if let SelectObjectContentEventStream::Records(records) = event {
            if let Some(payload) = records.payload() {
                result.push_str(&String::from_utf8_lossy(payload.as_ref()));
            }
        }
    }

    // Empty array should return no records
    assert!(result.trim().is_empty() || result == "{}", "Should return empty result");
}
