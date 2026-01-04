//! Integration tests for cloud discovery with mock metadata servers.
//!
//! These tests use wiremock to simulate AWS, GCP, and Azure metadata endpoints.

use std::time::Duration;

use rucket_consensus::discovery::{
    CloudConfig, CloudDiscovery, CloudProvider, Discovery, MetadataEndpoints,
};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Helper to create a CloudConfig with mock endpoints.
fn config_with_mock(mock_url: &str) -> CloudConfig {
    CloudConfig {
        endpoints: MetadataEndpoints::for_testing(mock_url),
        metadata_timeout: Duration::from_secs(5),
        ..Default::default()
    }
}

// =============================================================================
// AWS IMDS Mock Helpers
// =============================================================================

/// Mock AWS IMDSv1 endpoint (simple GET).
async fn mock_aws_imdsv1(server: &MockServer, instance_id: &str) {
    Mock::given(method("GET"))
        .and(path("/latest/meta-data/instance-id"))
        .respond_with(ResponseTemplate::new(200).set_body_string(instance_id))
        .mount(server)
        .await;
}

/// Mock AWS IMDSv2 token endpoint.
async fn mock_aws_imdsv2_token(server: &MockServer, token: &str) {
    Mock::given(method("PUT"))
        .and(path("/latest/api/token"))
        .and(header("X-aws-ec2-metadata-token-ttl-seconds", "21600"))
        .respond_with(ResponseTemplate::new(200).set_body_string(token))
        .mount(server)
        .await;
}

/// Mock AWS IMDSv2 metadata endpoint (requires token).
async fn mock_aws_imdsv2_instance_id(server: &MockServer, token: &str, instance_id: &str) {
    Mock::given(method("GET"))
        .and(path("/latest/meta-data/instance-id"))
        .and(header("X-aws-ec2-metadata-token", token))
        .respond_with(ResponseTemplate::new(200).set_body_string(instance_id))
        .mount(server)
        .await;
}

// =============================================================================
// GCP Metadata Mock Helpers
// =============================================================================

/// Mock GCP metadata endpoint.
async fn mock_gcp_metadata(server: &MockServer, zone: &str) {
    Mock::given(method("GET"))
        .and(path("/computeMetadata/v1/instance/zone"))
        .and(header("Metadata-Flavor", "Google"))
        .respond_with(ResponseTemplate::new(200).set_body_string(zone))
        .mount(server)
        .await;
}

// =============================================================================
// Azure IMDS Mock Helpers
// =============================================================================

/// Mock Azure IMDS endpoint.
async fn mock_azure_metadata(server: &MockServer) {
    let response_json = serde_json::json!({
        "compute": {
            "resourceGroupName": "my-resource-group",
            "subscriptionId": "12345678-1234-1234-1234-123456789abc",
            "vmId": "12345678-1234-1234-1234-123456789abc",
            "location": "eastus"
        }
    });

    Mock::given(method("GET"))
        .and(path("/metadata/instance"))
        .and(header("Metadata", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(response_json))
        .mount(server)
        .await;
}

// =============================================================================
// AWS Detection Tests
// =============================================================================

#[tokio::test]
async fn test_aws_detection_imdsv2() {
    let server = MockServer::start().await;
    let token = "test-session-token";
    let instance_id = "i-1234567890abcdef0";

    // Mock IMDSv2 flow: token + metadata with token
    mock_aws_imdsv2_token(&server, token).await;
    mock_aws_imdsv2_instance_id(&server, token, instance_id).await;

    let config = CloudConfig {
        endpoints: MetadataEndpoints::for_testing(&server.uri()),
        aws_use_imdsv2: true,
        metadata_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    assert_eq!(discovery.provider(), CloudProvider::Aws);
}

#[tokio::test]
async fn test_aws_detection_imdsv1() {
    let server = MockServer::start().await;
    let instance_id = "i-1234567890abcdef0";

    // Mock IMDSv1 (simple GET without token)
    mock_aws_imdsv1(&server, instance_id).await;

    let config = CloudConfig {
        endpoints: MetadataEndpoints::for_testing(&server.uri()),
        aws_use_imdsv2: false, // Use IMDSv1
        metadata_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    assert_eq!(discovery.provider(), CloudProvider::Aws);
}

#[tokio::test]
async fn test_aws_detection_imdsv2_fallback_to_v1() {
    let server = MockServer::start().await;
    let instance_id = "i-1234567890abcdef0";

    // Only mock IMDSv1 - IMDSv2 token request will fail and fall back
    mock_aws_imdsv1(&server, instance_id).await;

    let config = CloudConfig {
        endpoints: MetadataEndpoints::for_testing(&server.uri()),
        aws_use_imdsv2: true, // Try IMDSv2 first, but it will fall back to v1
        metadata_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    assert_eq!(discovery.provider(), CloudProvider::Aws);
}

// =============================================================================
// GCP Detection Tests
// =============================================================================

#[tokio::test]
async fn test_gcp_detection() {
    let server = MockServer::start().await;

    // Mock GCP metadata endpoint
    mock_gcp_metadata(&server, "projects/123456789/zones/us-central1-a").await;

    let config = config_with_mock(&server.uri());
    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    assert_eq!(discovery.provider(), CloudProvider::Gcp);
}

// =============================================================================
// Azure Detection Tests
// =============================================================================

#[tokio::test]
async fn test_azure_detection() {
    let server = MockServer::start().await;

    // Mock Azure IMDS endpoint
    mock_azure_metadata(&server).await;

    let config = config_with_mock(&server.uri());
    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    assert_eq!(discovery.provider(), CloudProvider::Azure);
}

// =============================================================================
// Non-Cloud Detection Tests
// =============================================================================

#[tokio::test]
async fn test_non_cloud_environment() {
    let server = MockServer::start().await;
    // No mocks - all requests will 404

    let config = CloudConfig {
        endpoints: MetadataEndpoints::for_testing(&server.uri()),
        metadata_timeout: Duration::from_secs(1), // Short timeout for faster test
        ..Default::default()
    };

    let result = CloudDiscovery::auto_detect(config).await;
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.to_string().contains("Not running on a supported cloud provider"));
}

// =============================================================================
// Priority Tests (AWS > GCP > Azure)
// =============================================================================

#[tokio::test]
async fn test_aws_takes_priority_over_gcp() {
    let server = MockServer::start().await;

    // Mock both AWS and GCP
    mock_aws_imdsv1(&server, "i-12345").await;
    mock_gcp_metadata(&server, "projects/123/zones/us-central1-a").await;

    let config = CloudConfig {
        endpoints: MetadataEndpoints::for_testing(&server.uri()),
        aws_use_imdsv2: false,
        metadata_timeout: Duration::from_secs(5),
        ..Default::default()
    };

    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    // AWS should take priority
    assert_eq!(discovery.provider(), CloudProvider::Aws);
}

#[tokio::test]
async fn test_gcp_takes_priority_over_azure() {
    let server = MockServer::start().await;

    // Mock both GCP and Azure (but not AWS)
    mock_gcp_metadata(&server, "projects/123/zones/us-central1-a").await;
    mock_azure_metadata(&server).await;

    let config = config_with_mock(&server.uri());
    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    // GCP should take priority over Azure
    assert_eq!(discovery.provider(), CloudProvider::Gcp);
}

// =============================================================================
// Force Provider Tests
// =============================================================================

#[tokio::test]
async fn test_force_provider_skips_detection() {
    let server = MockServer::start().await;
    // No mocks - but force_provider should skip detection

    let config = CloudConfig {
        endpoints: MetadataEndpoints::for_testing(&server.uri()),
        force_provider: Some(CloudProvider::Aws),
        ..Default::default()
    };

    // With force_provider, auto_detect should succeed without probing
    let discovery = CloudDiscovery::auto_detect(config).await.unwrap();
    assert_eq!(discovery.provider(), CloudProvider::Aws);
}

#[tokio::test]
async fn test_for_provider_creates_discovery_directly() {
    let config = CloudConfig::default();
    let discovery = CloudDiscovery::for_provider(CloudProvider::Gcp, config);
    assert_eq!(discovery.provider(), CloudProvider::Gcp);
    assert_eq!(discovery.name(), "cloud:gcp");
}

// =============================================================================
// Timeout Tests
// =============================================================================

#[tokio::test]
async fn test_metadata_timeout() {
    let server = MockServer::start().await;

    // Mock with a 10 second delay (longer than our timeout)
    Mock::given(method("GET"))
        .and(path("/latest/meta-data/instance-id"))
        .respond_with(ResponseTemplate::new(200).set_delay(Duration::from_secs(10)))
        .mount(&server)
        .await;

    let config = CloudConfig {
        endpoints: MetadataEndpoints::for_testing(&server.uri()),
        aws_use_imdsv2: false,
        metadata_timeout: Duration::from_millis(100), // Very short timeout
        ..Default::default()
    };

    // Should fail due to timeout
    let result = CloudDiscovery::auto_detect(config).await;
    assert!(result.is_err());
}
