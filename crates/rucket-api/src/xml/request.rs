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

    /// Allowed origins (e.g., "*" or "http://example.com").
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
}
