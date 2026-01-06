//! S3 XML request parsing.

use serde::Deserialize;

/// `VersioningConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "VersioningConfiguration")]
pub struct VersioningConfiguration {
    /// Versioning status: "Enabled" or "Suspended".
    #[serde(rename = "Status")]
    pub status: Option<String>,
}

/// `CreateBucketConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "CreateBucketConfiguration")]
pub struct CreateBucketConfiguration {
    /// Location constraint for the bucket.
    #[serde(rename = "LocationConstraint")]
    pub location_constraint: Option<String>,
}

/// `CompleteMultipartUpload` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "CompleteMultipartUpload")]
pub struct CompleteMultipartUpload {
    /// List of parts to complete.
    #[serde(rename = "Part", default)]
    pub parts: Vec<CompletePart>,
}

/// A part in the `CompleteMultipartUpload` request.
#[derive(Debug, Deserialize)]
pub struct CompletePart {
    /// Part number.
    #[serde(rename = "PartNumber")]
    pub part_number: u32,

    /// ETag of the part.
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// `Delete` request body for `DeleteObjects`.
#[derive(Debug, Deserialize)]
#[serde(rename = "Delete")]
pub struct DeleteObjects {
    /// Whether to use quiet mode.
    #[serde(rename = "Quiet", default)]
    pub quiet: bool,

    /// Objects to delete.
    #[serde(rename = "Object", default)]
    pub objects: Vec<ObjectIdentifier>,
}

/// Object identifier for deletion.
#[derive(Debug, Deserialize)]
pub struct ObjectIdentifier {
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,

    /// Version ID (optional).
    #[serde(rename = "VersionId")]
    pub version_id: Option<String>,
}

/// `Tagging` request body for `PutObjectTagging`.
#[derive(Debug, Deserialize)]
#[serde(rename = "Tagging")]
pub struct Tagging {
    /// The tag set.
    #[serde(rename = "TagSet")]
    pub tag_set: TagSetRequest,
}

/// Tag set in tagging request.
#[derive(Debug, Deserialize)]
#[serde(rename = "TagSet")]
pub struct TagSetRequest {
    /// List of tags.
    #[serde(rename = "Tag", default)]
    pub tags: Vec<TagRequest>,
}

/// A single tag in the request.
#[derive(Debug, Deserialize)]
pub struct TagRequest {
    /// Tag key.
    #[serde(rename = "Key")]
    pub key: String,

    /// Tag value.
    #[serde(rename = "Value")]
    pub value: String,
}

/// `CORSConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "CORSConfiguration")]
pub struct CorsConfigurationRequest {
    /// CORS rules.
    #[serde(rename = "CORSRule", default)]
    pub rules: Vec<CorsRuleRequest>,
}

/// A CORS rule in the request.
#[derive(Debug, Deserialize)]
pub struct CorsRuleRequest {
    /// Optional ID for this rule.
    #[serde(rename = "ID")]
    pub id: Option<String>,

    /// Allowed origins (e.g., `*` or `http://example.com`).
    #[serde(rename = "AllowedOrigin", default)]
    pub allowed_origins: Vec<String>,

    /// Allowed HTTP methods (GET, PUT, POST, DELETE, HEAD).
    #[serde(rename = "AllowedMethod", default)]
    pub allowed_methods: Vec<String>,

    /// Allowed headers in the request.
    #[serde(rename = "AllowedHeader", default)]
    pub allowed_headers: Vec<String>,

    /// Headers exposed to the browser.
    #[serde(rename = "ExposeHeader", default)]
    pub expose_headers: Vec<String>,

    /// Max age for preflight cache in seconds.
    #[serde(rename = "MaxAgeSeconds")]
    pub max_age_seconds: Option<u32>,
}

/// `PublicAccessBlockConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "PublicAccessBlockConfiguration")]
pub struct PublicAccessBlockConfigurationRequest {
    /// Block public ACLs on this bucket and objects in this bucket.
    #[serde(rename = "BlockPublicAcls")]
    pub block_public_acls: Option<bool>,

    /// Ignore public ACLs on this bucket and objects in this bucket.
    #[serde(rename = "IgnorePublicAcls")]
    pub ignore_public_acls: Option<bool>,

    /// Block public bucket policies for this bucket.
    #[serde(rename = "BlockPublicPolicy")]
    pub block_public_policy: Option<bool>,

    /// Restrict public bucket policies for this bucket.
    #[serde(rename = "RestrictPublicBuckets")]
    pub restrict_public_buckets: Option<bool>,
}

/// `LifecycleConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "LifecycleConfiguration")]
pub struct LifecycleConfigurationRequest {
    /// List of lifecycle rules.
    #[serde(rename = "Rule", default)]
    pub rules: Vec<LifecycleRuleRequest>,
}

/// A lifecycle rule in the request.
#[derive(Debug, Deserialize)]
pub struct LifecycleRuleRequest {
    /// Unique identifier for the rule.
    #[serde(rename = "ID")]
    pub id: Option<String>,

    /// Status of the rule.
    #[serde(rename = "Status")]
    pub status: String,

    /// Filter to select objects.
    #[serde(rename = "Filter")]
    pub filter: Option<LifecycleFilterRequest>,

    /// Expiration settings.
    #[serde(rename = "Expiration")]
    pub expiration: Option<ExpirationRequest>,

    /// Noncurrent version expiration settings.
    #[serde(rename = "NoncurrentVersionExpiration")]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpirationRequest>,

    /// Abort incomplete multipart upload settings.
    #[serde(rename = "AbortIncompleteMultipartUpload")]
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUploadRequest>,
}

/// Lifecycle filter in the request.
#[derive(Debug, Deserialize)]
pub struct LifecycleFilterRequest {
    /// Key name prefix.
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,

    /// Tag filter.
    #[serde(rename = "Tag")]
    pub tag: Option<LifecycleTagRequest>,

    /// Logical AND of filters.
    #[serde(rename = "And")]
    pub and: Option<LifecycleFilterAndRequest>,
}

/// Logical AND filter for lifecycle.
#[derive(Debug, Deserialize)]
pub struct LifecycleFilterAndRequest {
    /// Key name prefix.
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,

    /// Tags to match.
    #[serde(rename = "Tag", default)]
    pub tags: Vec<LifecycleTagRequest>,
}

/// Tag in lifecycle filter.
#[derive(Debug, Deserialize)]
pub struct LifecycleTagRequest {
    /// Tag key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Tag value.
    #[serde(rename = "Value")]
    pub value: String,
}

/// Expiration settings in request.
#[derive(Debug, Deserialize)]
pub struct ExpirationRequest {
    /// Days after creation when object expires.
    #[serde(rename = "Days")]
    pub days: Option<u32>,
    /// Date when objects expire.
    #[serde(rename = "Date")]
    pub date: Option<String>,
    /// Whether to remove expired delete markers.
    #[serde(rename = "ExpiredObjectDeleteMarker")]
    pub expired_object_delete_marker: Option<bool>,
}

/// Noncurrent version expiration settings in request.
#[derive(Debug, Deserialize)]
pub struct NoncurrentVersionExpirationRequest {
    /// Days after becoming noncurrent when version expires.
    #[serde(rename = "NoncurrentDays")]
    pub noncurrent_days: u32,
    /// Number of noncurrent versions to retain.
    #[serde(rename = "NewerNoncurrentVersions")]
    pub newer_noncurrent_versions: Option<u32>,
}

/// Abort incomplete multipart upload settings in request.
#[derive(Debug, Deserialize)]
pub struct AbortIncompleteMultipartUploadRequest {
    /// Days after initiation when incomplete uploads are aborted.
    #[serde(rename = "DaysAfterInitiation")]
    pub days_after_initiation: u32,
}

/// `ServerSideEncryptionConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "ServerSideEncryptionConfiguration")]
pub struct ServerSideEncryptionConfigurationRequest {
    /// List of encryption rules.
    #[serde(rename = "Rule", default)]
    pub rules: Vec<ServerSideEncryptionRuleRequest>,
}

/// A server-side encryption rule in the request.
#[derive(Debug, Deserialize)]
pub struct ServerSideEncryptionRuleRequest {
    /// Default encryption settings.
    #[serde(rename = "ApplyServerSideEncryptionByDefault")]
    pub apply_server_side_encryption_by_default: ApplyServerSideEncryptionByDefaultRequest,
    /// Whether to use bucket key for SSE-KMS.
    #[serde(rename = "BucketKeyEnabled")]
    pub bucket_key_enabled: Option<bool>,
}

/// Default encryption settings in request.
#[derive(Debug, Deserialize)]
pub struct ApplyServerSideEncryptionByDefaultRequest {
    /// Server-side encryption algorithm.
    #[serde(rename = "SSEAlgorithm")]
    pub sse_algorithm: String,
    /// KMS master key ID (for aws:kms algorithm).
    #[serde(rename = "KMSMasterKeyID")]
    pub kms_master_key_id: Option<String>,
}

/// `ReplicationConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "ReplicationConfiguration")]
pub struct ReplicationConfigurationRequest {
    /// IAM role ARN for replication.
    #[serde(rename = "Role")]
    pub role: String,
    /// List of replication rules.
    #[serde(rename = "Rule", default)]
    pub rules: Vec<ReplicationRuleRequest>,
}

/// A replication rule in the request.
#[derive(Debug, Deserialize)]
pub struct ReplicationRuleRequest {
    /// Unique identifier for the rule.
    #[serde(rename = "ID")]
    pub id: Option<String>,
    /// Priority of the rule.
    #[serde(rename = "Priority")]
    pub priority: Option<u32>,
    /// Status of the rule.
    #[serde(rename = "Status")]
    pub status: String,
    /// Filter to select objects.
    #[serde(rename = "Filter")]
    pub filter: Option<ReplicationFilterRequest>,
    /// Destination bucket configuration.
    #[serde(rename = "Destination")]
    pub destination: ReplicationDestinationRequest,
    /// Delete marker replication settings.
    #[serde(rename = "DeleteMarkerReplication")]
    pub delete_marker_replication: Option<DeleteMarkerReplicationRequest>,
    /// Source selection criteria.
    #[serde(rename = "SourceSelectionCriteria")]
    pub source_selection_criteria: Option<SourceSelectionCriteriaRequest>,
    /// Existing object replication settings.
    #[serde(rename = "ExistingObjectReplication")]
    pub existing_object_replication: Option<ExistingObjectReplicationRequest>,
}

/// Replication filter in the request.
#[derive(Debug, Deserialize)]
pub struct ReplicationFilterRequest {
    /// Key name prefix.
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,
    /// Tag filter.
    #[serde(rename = "Tag")]
    pub tag: Option<ReplicationTagRequest>,
    /// Logical AND of filters.
    #[serde(rename = "And")]
    pub and: Option<ReplicationFilterAndRequest>,
}

/// Logical AND filter for replication.
#[derive(Debug, Deserialize)]
pub struct ReplicationFilterAndRequest {
    /// Key name prefix.
    #[serde(rename = "Prefix")]
    pub prefix: Option<String>,
    /// Tags to match.
    #[serde(rename = "Tag", default)]
    pub tags: Vec<ReplicationTagRequest>,
}

/// Tag in replication filter.
#[derive(Debug, Deserialize)]
pub struct ReplicationTagRequest {
    /// Tag key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Tag value.
    #[serde(rename = "Value")]
    pub value: String,
}

/// Destination bucket configuration in request.
#[derive(Debug, Deserialize)]
pub struct ReplicationDestinationRequest {
    /// Destination bucket ARN.
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Optional account ID for cross-account replication.
    #[serde(rename = "Account")]
    pub account: Option<String>,
    /// Storage class for replicated objects.
    #[serde(rename = "StorageClass")]
    pub storage_class: Option<String>,
    /// Access control translation settings.
    #[serde(rename = "AccessControlTranslation")]
    pub access_control_translation: Option<AccessControlTranslationRequest>,
    /// Encryption configuration.
    #[serde(rename = "EncryptionConfiguration")]
    pub encryption_configuration: Option<ReplicationEncryptionConfigurationRequest>,
    /// Replication time control settings.
    #[serde(rename = "ReplicationTime")]
    pub replication_time: Option<ReplicationTimeRequest>,
    /// Metrics configuration.
    #[serde(rename = "Metrics")]
    pub metrics: Option<ReplicationMetricsRequest>,
}

/// Access control translation settings in request.
#[derive(Debug, Deserialize)]
pub struct AccessControlTranslationRequest {
    /// Owner override (Destination).
    #[serde(rename = "Owner")]
    pub owner: String,
}

/// Encryption configuration in request.
#[derive(Debug, Deserialize)]
pub struct ReplicationEncryptionConfigurationRequest {
    /// KMS key ID for destination encryption.
    #[serde(rename = "ReplicaKmsKeyID")]
    pub replica_kms_key_id: Option<String>,
}

/// Replication time control in request.
#[derive(Debug, Deserialize)]
pub struct ReplicationTimeRequest {
    /// Status of replication time control.
    #[serde(rename = "Status")]
    pub status: String,
    /// Time threshold.
    #[serde(rename = "Time")]
    pub time: Option<ReplicationTimeValueRequest>,
}

/// Time value in request.
#[derive(Debug, Deserialize)]
pub struct ReplicationTimeValueRequest {
    /// Minutes threshold.
    #[serde(rename = "Minutes")]
    pub minutes: u32,
}

/// Metrics configuration in request.
#[derive(Debug, Deserialize)]
pub struct ReplicationMetricsRequest {
    /// Status of metrics.
    #[serde(rename = "Status")]
    pub status: String,
    /// Event threshold.
    #[serde(rename = "EventThreshold")]
    pub event_threshold: Option<ReplicationTimeValueRequest>,
}

/// Delete marker replication settings in request.
#[derive(Debug, Deserialize)]
pub struct DeleteMarkerReplicationRequest {
    /// Status of delete marker replication.
    #[serde(rename = "Status")]
    pub status: String,
}

/// Source selection criteria in request.
#[derive(Debug, Deserialize)]
pub struct SourceSelectionCriteriaRequest {
    /// SSE-KMS encrypted objects settings.
    #[serde(rename = "SseKmsEncryptedObjects")]
    pub sse_kms_encrypted_objects: Option<SseKmsEncryptedObjectsRequest>,
    /// Replica modifications settings.
    #[serde(rename = "ReplicaModifications")]
    pub replica_modifications: Option<ReplicaModificationsRequest>,
}

/// SSE-KMS encrypted objects settings in request.
#[derive(Debug, Deserialize)]
pub struct SseKmsEncryptedObjectsRequest {
    /// Status.
    #[serde(rename = "Status")]
    pub status: String,
}

/// Replica modifications settings in request.
#[derive(Debug, Deserialize)]
pub struct ReplicaModificationsRequest {
    /// Status.
    #[serde(rename = "Status")]
    pub status: String,
}

/// Existing object replication settings in request.
#[derive(Debug, Deserialize)]
pub struct ExistingObjectReplicationRequest {
    /// Status.
    #[serde(rename = "Status")]
    pub status: String,
}

/// `WebsiteConfiguration` request body.
#[derive(Debug, Deserialize)]
#[serde(rename = "WebsiteConfiguration")]
pub struct WebsiteConfigurationRequest {
    /// Index document configuration.
    #[serde(rename = "IndexDocument")]
    pub index_document: Option<IndexDocumentRequest>,

    /// Error document configuration.
    #[serde(rename = "ErrorDocument")]
    pub error_document: Option<ErrorDocumentRequest>,

    /// Redirect all requests to another host.
    #[serde(rename = "RedirectAllRequestsTo")]
    pub redirect_all_requests_to: Option<RedirectAllRequestsToRequest>,

    /// Routing rules for conditional redirects.
    #[serde(rename = "RoutingRules")]
    pub routing_rules: Option<RoutingRulesRequest>,
}

/// Index document configuration in request.
#[derive(Debug, Deserialize)]
pub struct IndexDocumentRequest {
    /// The suffix appended to requests for a directory.
    #[serde(rename = "Suffix")]
    pub suffix: String,
}

/// Error document configuration in request.
#[derive(Debug, Deserialize)]
pub struct ErrorDocumentRequest {
    /// The object key to return when an error occurs.
    #[serde(rename = "Key")]
    pub key: String,
}

/// Redirect all requests to another host in request.
#[derive(Debug, Deserialize)]
pub struct RedirectAllRequestsToRequest {
    /// The host name to redirect to.
    #[serde(rename = "HostName")]
    pub host_name: String,

    /// The protocol to use for the redirect.
    #[serde(rename = "Protocol")]
    pub protocol: Option<String>,
}

/// Routing rules wrapper in request.
#[derive(Debug, Deserialize)]
pub struct RoutingRulesRequest {
    /// List of routing rules.
    #[serde(rename = "RoutingRule", default)]
    pub rules: Vec<RoutingRuleRequest>,
}

/// A routing rule in the request.
#[derive(Debug, Deserialize)]
pub struct RoutingRuleRequest {
    /// The condition that must be met for the redirect to apply.
    #[serde(rename = "Condition")]
    pub condition: Option<RoutingRuleConditionRequest>,

    /// The redirect action to take.
    #[serde(rename = "Redirect")]
    pub redirect: RoutingRuleRedirectRequest,
}

/// Condition for a routing rule in request.
#[derive(Debug, Deserialize)]
pub struct RoutingRuleConditionRequest {
    /// Redirect only if the object key starts with this prefix.
    #[serde(rename = "KeyPrefixEquals")]
    pub key_prefix_equals: Option<String>,

    /// Redirect only if the HTTP error code matches.
    #[serde(rename = "HttpErrorCodeReturnedEquals")]
    pub http_error_code_returned_equals: Option<String>,
}

/// Redirect action for a routing rule in request.
#[derive(Debug, Deserialize)]
pub struct RoutingRuleRedirectRequest {
    /// The host name to redirect to.
    #[serde(rename = "HostName")]
    pub host_name: Option<String>,

    /// The HTTP redirect code.
    #[serde(rename = "HttpRedirectCode")]
    pub http_redirect_code: Option<String>,

    /// The protocol to use for the redirect.
    #[serde(rename = "Protocol")]
    pub protocol: Option<String>,

    /// Replace the key prefix with this value.
    #[serde(rename = "ReplaceKeyPrefixWith")]
    pub replace_key_prefix_with: Option<String>,

    /// Replace the entire key with this value.
    #[serde(rename = "ReplaceKeyWith")]
    pub replace_key_with: Option<String>,
}

#[cfg(test)]
mod tests {
    use quick_xml::de::from_str;

    use super::*;

    #[test]
    fn test_parse_complete_multipart() {
        let xml = r#"
            <CompleteMultipartUpload>
                <Part>
                    <PartNumber>1</PartNumber>
                    <ETag>"a54357aff0632cce46d942af68356b38"</ETag>
                </Part>
                <Part>
                    <PartNumber>2</PartNumber>
                    <ETag>"0c78aef83f66abc1fa1e8477f296d394"</ETag>
                </Part>
            </CompleteMultipartUpload>
        "#;

        let parsed: CompleteMultipartUpload = from_str(xml).unwrap();
        assert_eq!(parsed.parts.len(), 2);
        assert_eq!(parsed.parts[0].part_number, 1);
        assert_eq!(parsed.parts[1].part_number, 2);
    }

    #[test]
    fn test_parse_delete_objects() {
        let xml = r#"
            <Delete>
                <Quiet>true</Quiet>
                <Object>
                    <Key>key1</Key>
                </Object>
                <Object>
                    <Key>key2</Key>
                </Object>
            </Delete>
        "#;

        let parsed: DeleteObjects = from_str(xml).unwrap();
        assert!(parsed.quiet);
        assert_eq!(parsed.objects.len(), 2);
    }

    #[test]
    fn test_parse_tagging() {
        let xml = r#"
            <Tagging>
                <TagSet>
                    <Tag>
                        <Key>env</Key>
                        <Value>production</Value>
                    </Tag>
                    <Tag>
                        <Key>project</Key>
                        <Value>rucket</Value>
                    </Tag>
                </TagSet>
            </Tagging>
        "#;

        let parsed: Tagging = from_str(xml).unwrap();
        assert_eq!(parsed.tag_set.tags.len(), 2);
        assert_eq!(parsed.tag_set.tags[0].key, "env");
        assert_eq!(parsed.tag_set.tags[0].value, "production");
        assert_eq!(parsed.tag_set.tags[1].key, "project");
        assert_eq!(parsed.tag_set.tags[1].value, "rucket");
    }

    #[test]
    fn test_parse_cors_configuration() {
        let xml = r#"
            <CORSConfiguration>
                <CORSRule>
                    <ID>rule1</ID>
                    <AllowedOrigin>*</AllowedOrigin>
                    <AllowedMethod>GET</AllowedMethod>
                    <AllowedMethod>PUT</AllowedMethod>
                    <AllowedHeader>*</AllowedHeader>
                    <ExposeHeader>x-amz-request-id</ExposeHeader>
                    <MaxAgeSeconds>3000</MaxAgeSeconds>
                </CORSRule>
                <CORSRule>
                    <AllowedOrigin>http://example.com</AllowedOrigin>
                    <AllowedOrigin>http://www.example.com</AllowedOrigin>
                    <AllowedMethod>POST</AllowedMethod>
                </CORSRule>
            </CORSConfiguration>
        "#;

        let parsed: CorsConfigurationRequest = from_str(xml).unwrap();
        assert_eq!(parsed.rules.len(), 2);

        // Check first rule
        assert_eq!(parsed.rules[0].id, Some("rule1".to_string()));
        assert_eq!(parsed.rules[0].allowed_origins, vec!["*"]);
        assert_eq!(parsed.rules[0].allowed_methods, vec!["GET", "PUT"]);
        assert_eq!(parsed.rules[0].allowed_headers, vec!["*"]);
        assert_eq!(parsed.rules[0].expose_headers, vec!["x-amz-request-id"]);
        assert_eq!(parsed.rules[0].max_age_seconds, Some(3000));

        // Check second rule
        assert_eq!(parsed.rules[1].id, None);
        assert_eq!(
            parsed.rules[1].allowed_origins,
            vec!["http://example.com", "http://www.example.com"]
        );
        assert_eq!(parsed.rules[1].allowed_methods, vec!["POST"]);
    }

    #[test]
    fn test_parse_replication_configuration() {
        let xml = r#"
            <ReplicationConfiguration>
                <Role>arn:aws:iam::123456789:role/replication</Role>
                <Rule>
                    <ID>rule-1</ID>
                    <Status>Enabled</Status>
                    <Priority>1</Priority>
                    <Filter>
                        <Prefix>logs/</Prefix>
                    </Filter>
                    <Destination>
                        <Bucket>arn:aws:s3:::dest-bucket</Bucket>
                        <StorageClass>STANDARD_IA</StorageClass>
                    </Destination>
                    <DeleteMarkerReplication>
                        <Status>Enabled</Status>
                    </DeleteMarkerReplication>
                </Rule>
                <Rule>
                    <ID>rule-2</ID>
                    <Status>Disabled</Status>
                    <Destination>
                        <Bucket>backup-bucket</Bucket>
                    </Destination>
                </Rule>
            </ReplicationConfiguration>
        "#;

        let parsed: ReplicationConfigurationRequest = from_str(xml).unwrap();
        assert_eq!(parsed.role, "arn:aws:iam::123456789:role/replication");
        assert_eq!(parsed.rules.len(), 2);

        // Check first rule
        assert_eq!(parsed.rules[0].id, Some("rule-1".to_string()));
        assert_eq!(parsed.rules[0].status, "Enabled");
        assert_eq!(parsed.rules[0].priority, Some(1));
        assert!(parsed.rules[0].filter.is_some());
        assert_eq!(parsed.rules[0].filter.as_ref().unwrap().prefix, Some("logs/".to_string()));
        assert_eq!(parsed.rules[0].destination.bucket, "arn:aws:s3:::dest-bucket");
        assert_eq!(parsed.rules[0].destination.storage_class, Some("STANDARD_IA".to_string()));
        assert!(parsed.rules[0].delete_marker_replication.is_some());
        assert_eq!(parsed.rules[0].delete_marker_replication.as_ref().unwrap().status, "Enabled");

        // Check second rule
        assert_eq!(parsed.rules[1].id, Some("rule-2".to_string()));
        assert_eq!(parsed.rules[1].status, "Disabled");
        assert_eq!(parsed.rules[1].destination.bucket, "backup-bucket");
    }
}
