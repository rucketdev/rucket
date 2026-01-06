//! S3 XML response generation.

use chrono::{DateTime, Utc};
use rucket_core::types::{BucketInfo, ObjectMetadata};
use serde::Serialize;

/// Check if an Option<String> is None or empty.
fn is_none_or_empty(s: &Option<String>) -> bool {
    s.as_ref().map_or(true, |v| v.is_empty())
}

/// Format a DateTime<Utc> in S3-compatible ISO 8601 format with 'Z' suffix.
///
/// AWS S3 SDKs expect timestamps in the format `2024-01-01T00:00:00.000Z`,
/// not `2024-01-01T00:00:00.000+00:00` which `to_rfc3339()` produces.
fn format_s3_timestamp(dt: &DateTime<Utc>) -> String {
    dt.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// `ListAllMyBucketsResult` response.
#[derive(Debug, Serialize)]
#[serde(rename = "ListAllMyBucketsResult")]
pub struct ListBucketsResponse {
    /// Owner information.
    #[serde(rename = "Owner")]
    pub owner: Owner,
    /// List of buckets.
    #[serde(rename = "Buckets")]
    pub buckets: Buckets,
}

/// Owner information.
#[derive(Debug, Serialize)]
pub struct Owner {
    /// Owner ID.
    #[serde(rename = "ID")]
    pub id: String,
    /// Display name.
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

impl Default for Owner {
    fn default() -> Self {
        Self { id: "rucket".to_string(), display_name: "rucket".to_string() }
    }
}

/// Wrapper for bucket list.
#[derive(Debug, Serialize)]
pub struct Buckets {
    /// Individual buckets.
    #[serde(rename = "Bucket", default)]
    pub bucket: Vec<BucketEntry>,
}

/// Individual bucket entry.
#[derive(Debug, Serialize)]
pub struct BucketEntry {
    /// Bucket name.
    #[serde(rename = "Name")]
    pub name: String,
    /// Creation date.
    #[serde(rename = "CreationDate")]
    pub creation_date: String,
}

impl From<&BucketInfo> for BucketEntry {
    fn from(info: &BucketInfo) -> Self {
        Self { name: info.name.clone(), creation_date: format_s3_timestamp(&info.created_at) }
    }
}

/// `ListBucketResult` response (ListObjectsV1).
#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListObjectsV1Response {
    /// Bucket name.
    #[serde(rename = "Name")]
    pub name: String,
    /// Prefix filter.
    #[serde(rename = "Prefix")]
    pub prefix: String,
    /// Marker from the request.
    #[serde(rename = "Marker")]
    pub marker: String,
    /// Next marker for pagination (only when truncated and using delimiter).
    #[serde(rename = "NextMarker", skip_serializing_if = "Option::is_none")]
    pub next_marker: Option<String>,
    /// Delimiter used for grouping.
    #[serde(rename = "Delimiter", skip_serializing_if = "is_none_or_empty")]
    pub delimiter: Option<String>,
    /// Max keys requested.
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    /// Encoding type (url).
    #[serde(rename = "EncodingType", skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
    /// Whether results are truncated.
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    /// Objects.
    #[serde(rename = "Contents", default)]
    pub contents: Vec<ObjectEntry>,
    /// Common prefixes (when using delimiter).
    #[serde(rename = "CommonPrefixes", default, skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
}

/// `ListBucketResult` response (ListObjectsV2).
#[derive(Debug, Serialize)]
#[serde(rename = "ListBucketResult")]
pub struct ListObjectsV2Response {
    /// Bucket name.
    #[serde(rename = "Name")]
    pub name: String,
    /// Prefix filter.
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Delimiter used for grouping.
    #[serde(rename = "Delimiter", skip_serializing_if = "is_none_or_empty")]
    pub delimiter: Option<String>,
    /// Max keys requested.
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    /// Encoding type (url).
    #[serde(rename = "EncodingType", skip_serializing_if = "Option::is_none")]
    pub encoding_type: Option<String>,
    /// Whether results are truncated.
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    /// Continuation token from the request.
    #[serde(rename = "ContinuationToken", skip_serializing_if = "Option::is_none")]
    pub continuation_token: Option<String>,
    /// Continuation token for next page.
    #[serde(rename = "NextContinuationToken", skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
    /// StartAfter from the request.
    #[serde(rename = "StartAfter", skip_serializing_if = "Option::is_none")]
    pub start_after: Option<String>,
    /// Number of keys returned.
    #[serde(rename = "KeyCount")]
    pub key_count: u32,
    /// Objects.
    #[serde(rename = "Contents", default)]
    pub contents: Vec<ObjectEntry>,
    /// Common prefixes (when using delimiter).
    #[serde(rename = "CommonPrefixes", default, skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
}

/// Object entry in list response.
#[derive(Debug, Serialize)]
pub struct ObjectEntry {
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Last modified timestamp.
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    /// ETag.
    #[serde(rename = "ETag")]
    pub etag: String,
    /// Size in bytes.
    #[serde(rename = "Size")]
    pub size: u64,
    /// Owner (included when FetchOwner=true).
    #[serde(rename = "Owner", skip_serializing_if = "Option::is_none")]
    pub owner: Option<Owner>,
    /// Storage class.
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

impl From<&ObjectMetadata> for ObjectEntry {
    fn from(meta: &ObjectMetadata) -> Self {
        Self {
            key: meta.key.clone(),
            last_modified: format_s3_timestamp(&meta.last_modified),
            etag: meta.etag.as_str().to_string(),
            size: meta.size,
            owner: None,
            storage_class: meta.storage_class.as_str().to_string(),
        }
    }
}

impl ObjectEntry {
    /// Create an ObjectEntry with owner information.
    pub fn with_owner(meta: &ObjectMetadata) -> Self {
        Self {
            key: meta.key.clone(),
            last_modified: format_s3_timestamp(&meta.last_modified),
            etag: meta.etag.as_str().to_string(),
            size: meta.size,
            owner: Some(Owner::default()),
            storage_class: meta.storage_class.as_str().to_string(),
        }
    }

    /// Return a copy with a URL-encoded key.
    #[must_use]
    pub fn with_encoded_key(mut self, encoded_key: &str) -> Self {
        self.key = encoded_key.to_string();
        self
    }
}

/// Common prefix entry.
#[derive(Debug, Serialize)]
pub struct CommonPrefix {
    /// Prefix value.
    #[serde(rename = "Prefix")]
    pub prefix: String,
}

/// `InitiateMultipartUploadResult` response.
#[derive(Debug, Serialize)]
#[serde(rename = "InitiateMultipartUploadResult")]
pub struct InitiateMultipartUploadResponse {
    /// Bucket name.
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Upload ID.
    #[serde(rename = "UploadId")]
    pub upload_id: String,
}

/// `CompleteMultipartUploadResult` response.
#[derive(Debug, Serialize)]
#[serde(rename = "CompleteMultipartUploadResult")]
pub struct CompleteMultipartUploadResponse {
    /// Location URL.
    #[serde(rename = "Location")]
    pub location: String,
    /// Bucket name.
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Final ETag.
    #[serde(rename = "ETag")]
    pub etag: String,
}

/// `ListMultipartUploadsResult` response.
#[derive(Debug, Serialize)]
#[serde(rename = "ListMultipartUploadsResult")]
pub struct ListMultipartUploadsResponse {
    /// Bucket name.
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Key marker from request.
    #[serde(rename = "KeyMarker", skip_serializing_if = "Option::is_none")]
    pub key_marker: Option<String>,
    /// Upload ID marker from request.
    #[serde(rename = "UploadIdMarker", skip_serializing_if = "Option::is_none")]
    pub upload_id_marker: Option<String>,
    /// Next key marker for pagination.
    #[serde(rename = "NextKeyMarker", skip_serializing_if = "Option::is_none")]
    pub next_key_marker: Option<String>,
    /// Next upload ID marker for pagination.
    #[serde(rename = "NextUploadIdMarker", skip_serializing_if = "Option::is_none")]
    pub next_upload_id_marker: Option<String>,
    /// Maximum number of uploads returned.
    #[serde(rename = "MaxUploads")]
    pub max_uploads: u32,
    /// Whether results are truncated.
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    /// Prefix filter.
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Delimiter used for grouping.
    #[serde(rename = "Delimiter", skip_serializing_if = "Option::is_none")]
    pub delimiter: Option<String>,
    /// Common prefixes when using delimiter.
    #[serde(rename = "CommonPrefixes", skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
    /// Upload entries.
    #[serde(rename = "Upload", default)]
    pub uploads: Vec<MultipartUploadEntry>,
}

/// Multipart upload entry.
#[derive(Debug, Serialize)]
pub struct MultipartUploadEntry {
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Upload ID.
    #[serde(rename = "UploadId")]
    pub upload_id: String,
    /// Initiator information.
    #[serde(rename = "Initiator")]
    pub initiator: Owner,
    /// Owner information.
    #[serde(rename = "Owner")]
    pub owner: Owner,
    /// Storage class.
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    /// Initiation timestamp.
    #[serde(rename = "Initiated")]
    pub initiated: String,
}

impl From<&rucket_core::types::MultipartUpload> for MultipartUploadEntry {
    fn from(upload: &rucket_core::types::MultipartUpload) -> Self {
        Self {
            key: upload.key.clone(),
            upload_id: upload.upload_id.clone(),
            initiator: Owner::default(),
            owner: Owner::default(),
            storage_class: upload.storage_class.as_str().to_string(),
            initiated: format_s3_timestamp(&upload.initiated),
        }
    }
}

/// `ListPartsResult` response.
#[derive(Debug, Serialize)]
#[serde(rename = "ListPartsResult")]
pub struct ListPartsResponse {
    /// Bucket name.
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Upload ID.
    #[serde(rename = "UploadId")]
    pub upload_id: String,
    /// Part number marker from request.
    #[serde(rename = "PartNumberMarker")]
    pub part_number_marker: u32,
    /// Next part number marker for pagination.
    #[serde(rename = "NextPartNumberMarker", skip_serializing_if = "Option::is_none")]
    pub next_part_number_marker: Option<u32>,
    /// Maximum number of parts returned.
    #[serde(rename = "MaxParts")]
    pub max_parts: u32,
    /// Whether results are truncated.
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    /// Initiator information.
    #[serde(rename = "Initiator")]
    pub initiator: Owner,
    /// Owner information.
    #[serde(rename = "Owner")]
    pub owner: Owner,
    /// Storage class.
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
    /// Parts.
    #[serde(rename = "Part", default)]
    pub parts: Vec<PartEntry>,
}

/// Part entry.
#[derive(Debug, Serialize)]
pub struct PartEntry {
    /// Part number.
    #[serde(rename = "PartNumber")]
    pub part_number: u32,
    /// ETag.
    #[serde(rename = "ETag")]
    pub etag: String,
    /// Size in bytes.
    #[serde(rename = "Size")]
    pub size: u64,
    /// Last modified.
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

impl From<&rucket_core::types::Part> for PartEntry {
    fn from(part: &rucket_core::types::Part) -> Self {
        Self {
            part_number: part.part_number,
            etag: part.etag.to_string(),
            size: part.size,
            last_modified: format_s3_timestamp(&part.last_modified),
        }
    }
}

/// `CopyObjectResult` response.
#[derive(Debug, Serialize)]
#[serde(rename = "CopyObjectResult")]
pub struct CopyObjectResponse {
    /// ETag of the copy.
    #[serde(rename = "ETag")]
    pub etag: String,
    /// Last modified timestamp.
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

impl CopyObjectResponse {
    /// Create a new copy response.
    #[must_use]
    pub fn new(etag: String, last_modified: DateTime<Utc>) -> Self {
        Self { etag, last_modified: format_s3_timestamp(&last_modified) }
    }
}

/// `CopyPartResult` response for upload part copy.
#[derive(Debug, Serialize)]
#[serde(rename = "CopyPartResult")]
pub struct CopyPartResult {
    /// ETag of the copied part.
    #[serde(rename = "ETag")]
    pub etag: String,
    /// Last modified timestamp.
    #[serde(rename = "LastModified")]
    pub last_modified: String,
}

impl CopyPartResult {
    /// Create a new copy part result.
    #[must_use]
    pub fn new(etag: String, last_modified: DateTime<Utc>) -> Self {
        Self { etag, last_modified: format_s3_timestamp(&last_modified) }
    }
}

/// `DeleteResult` response for multi-object delete.
#[derive(Debug, Serialize)]
#[serde(rename = "DeleteResult")]
pub struct DeleteObjectsResponse {
    /// Successfully deleted objects.
    #[serde(rename = "Deleted", default, skip_serializing_if = "Vec::is_empty")]
    pub deleted: Vec<DeletedObject>,
    /// Objects that failed to delete.
    #[serde(rename = "Error", default, skip_serializing_if = "Vec::is_empty")]
    pub errors: Vec<DeleteError>,
}

/// A successfully deleted object.
#[derive(Debug, Serialize)]
pub struct DeletedObject {
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Version ID (if versioning enabled).
    #[serde(rename = "VersionId", skip_serializing_if = "Option::is_none")]
    pub version_id: Option<String>,
}

/// Error deleting an object.
#[derive(Debug, Serialize)]
pub struct DeleteError {
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Error code.
    #[serde(rename = "Code")]
    pub code: String,
    /// Error message.
    #[serde(rename = "Message")]
    pub message: String,
}

/// `ListVersionsResult` response (ListObjectVersions).
#[derive(Debug, Serialize)]
#[serde(rename = "ListVersionsResult")]
pub struct ListObjectVersionsResponse {
    /// Bucket name.
    #[serde(rename = "Name")]
    pub name: String,
    /// Prefix filter.
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Key marker for pagination (always present, empty string if not set).
    #[serde(rename = "KeyMarker")]
    pub key_marker: String,
    /// Version ID marker for pagination (always present, empty string if not set).
    #[serde(rename = "VersionIdMarker")]
    pub version_id_marker: String,
    /// Next key marker for pagination.
    #[serde(rename = "NextKeyMarker", skip_serializing_if = "Option::is_none")]
    pub next_key_marker: Option<String>,
    /// Next version ID marker for pagination.
    #[serde(rename = "NextVersionIdMarker", skip_serializing_if = "Option::is_none")]
    pub next_version_id_marker: Option<String>,
    /// Max keys requested.
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    /// Whether results are truncated.
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    /// Object versions.
    #[serde(rename = "Version", default, skip_serializing_if = "Vec::is_empty")]
    pub versions: Vec<ObjectVersion>,
    /// Delete markers.
    #[serde(rename = "DeleteMarker", default, skip_serializing_if = "Vec::is_empty")]
    pub delete_markers: Vec<DeleteMarker>,
    /// Common prefixes (when using delimiter).
    #[serde(rename = "CommonPrefixes", default, skip_serializing_if = "Vec::is_empty")]
    pub common_prefixes: Vec<CommonPrefix>,
}

/// Object version entry.
#[derive(Debug, Serialize)]
pub struct ObjectVersion {
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Version ID.
    #[serde(rename = "VersionId")]
    pub version_id: String,
    /// Whether this is the latest version.
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
    /// Last modified timestamp.
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    /// ETag.
    #[serde(rename = "ETag")]
    pub etag: String,
    /// Size in bytes.
    #[serde(rename = "Size")]
    pub size: u64,
    /// Owner information.
    #[serde(rename = "Owner")]
    pub owner: Owner,
    /// Storage class.
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

impl ObjectVersion {
    /// Create a new ObjectVersion from metadata.
    ///
    /// Uses the stored version_id if present, otherwise "null" for non-versioned objects.
    pub fn from_metadata(meta: &ObjectMetadata) -> Self {
        Self {
            key: meta.key.clone(),
            version_id: meta.version_id.clone().unwrap_or_else(|| "null".to_string()),
            is_latest: meta.is_latest,
            last_modified: format_s3_timestamp(&meta.last_modified),
            etag: meta.etag.as_str().to_string(),
            size: meta.size,
            owner: Owner::default(),
            storage_class: meta.storage_class.as_str().to_string(),
        }
    }
}

/// Delete marker entry.
#[derive(Debug, Serialize)]
pub struct DeleteMarker {
    /// Object key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Version ID.
    #[serde(rename = "VersionId")]
    pub version_id: String,
    /// Whether this is the latest version.
    #[serde(rename = "IsLatest")]
    pub is_latest: bool,
    /// Last modified timestamp.
    #[serde(rename = "LastModified")]
    pub last_modified: String,
    /// Owner information.
    #[serde(rename = "Owner")]
    pub owner: Owner,
}

impl DeleteMarker {
    /// Create a new DeleteMarker from metadata.
    pub fn from_metadata(meta: &ObjectMetadata) -> Self {
        Self {
            key: meta.key.clone(),
            version_id: meta.version_id.clone().unwrap_or_else(|| "null".to_string()),
            is_latest: meta.is_latest,
            last_modified: format_s3_timestamp(&meta.last_modified),
            owner: Owner::default(),
        }
    }
}

/// `Tagging` response for `GetObjectTagging`.
#[derive(Debug, Serialize)]
#[serde(rename = "Tagging")]
pub struct TaggingResponse {
    /// The tag set.
    #[serde(rename = "TagSet")]
    pub tag_set: TagSetResponse,
}

/// Tag set in tagging response.
#[derive(Debug, Serialize)]
#[serde(rename = "TagSet")]
pub struct TagSetResponse {
    /// List of tags.
    #[serde(rename = "Tag", default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<TagResponse>,
}

/// A single tag in the response.
#[derive(Debug, Serialize)]
pub struct TagResponse {
    /// Tag key.
    #[serde(rename = "Key")]
    pub key: String,

    /// Tag value.
    #[serde(rename = "Value")]
    pub value: String,
}

/// `CORSConfiguration` response for `GetBucketCors`.
#[derive(Debug, Serialize)]
#[serde(rename = "CORSConfiguration")]
pub struct CorsConfigurationResponse {
    /// CORS rules.
    #[serde(rename = "CORSRule", default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<CorsRuleResponse>,
}

/// A CORS rule in the response.
#[derive(Debug, Serialize)]
pub struct CorsRuleResponse {
    /// Optional ID for this rule.
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Allowed origins.
    #[serde(rename = "AllowedOrigin", default)]
    pub allowed_origins: Vec<String>,

    /// Allowed HTTP methods.
    #[serde(rename = "AllowedMethod", default)]
    pub allowed_methods: Vec<String>,

    /// Allowed headers in the request.
    #[serde(rename = "AllowedHeader", default, skip_serializing_if = "Vec::is_empty")]
    pub allowed_headers: Vec<String>,

    /// Headers exposed to the browser.
    #[serde(rename = "ExposeHeader", default, skip_serializing_if = "Vec::is_empty")]
    pub expose_headers: Vec<String>,

    /// Max age for preflight cache in seconds.
    #[serde(rename = "MaxAgeSeconds", skip_serializing_if = "Option::is_none")]
    pub max_age_seconds: Option<u32>,
}

impl From<&rucket_core::types::CorsRule> for CorsRuleResponse {
    fn from(rule: &rucket_core::types::CorsRule) -> Self {
        Self {
            id: rule.id.clone(),
            allowed_origins: rule.allowed_origins.clone(),
            allowed_methods: rule.allowed_methods.clone(),
            allowed_headers: rule.allowed_headers.clone(),
            expose_headers: rule.expose_headers.clone(),
            max_age_seconds: rule.max_age_seconds,
        }
    }
}

impl From<&rucket_core::types::CorsConfiguration> for CorsConfigurationResponse {
    fn from(config: &rucket_core::types::CorsConfiguration) -> Self {
        Self { rules: config.rules.iter().map(CorsRuleResponse::from).collect() }
    }
}

/// `GetObjectAttributesResponse` response.
#[derive(Debug, Serialize)]
#[serde(rename = "GetObjectAttributesResponse")]
pub struct GetObjectAttributesResponse {
    /// ETag of the object.
    #[serde(rename = "ETag", skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// Last modified timestamp.
    #[serde(rename = "LastModified", skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    /// Object size in bytes.
    #[serde(rename = "ObjectSize", skip_serializing_if = "Option::is_none")]
    pub object_size: Option<i64>,
    /// Storage class.
    #[serde(rename = "StorageClass", skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Checksum information.
    #[serde(rename = "Checksum", skip_serializing_if = "Option::is_none")]
    pub checksum: Option<ObjectChecksum>,
    /// Object parts (for multipart uploads).
    #[serde(rename = "ObjectParts", skip_serializing_if = "Option::is_none")]
    pub object_parts: Option<ObjectParts>,
}

/// Checksum information for an object.
#[derive(Debug, Serialize)]
pub struct ObjectChecksum {
    /// CRC32 checksum.
    #[serde(rename = "ChecksumCRC32", skip_serializing_if = "Option::is_none")]
    pub crc32: Option<String>,
    /// CRC32C checksum.
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    pub crc32c: Option<String>,
    /// SHA1 checksum.
    #[serde(rename = "ChecksumSHA1", skip_serializing_if = "Option::is_none")]
    pub sha1: Option<String>,
    /// SHA256 checksum.
    #[serde(rename = "ChecksumSHA256", skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,
}

/// Object parts information (for multipart uploaded objects).
#[derive(Debug, Serialize)]
pub struct ObjectParts {
    /// Total number of parts.
    #[serde(rename = "TotalPartsCount", skip_serializing_if = "Option::is_none")]
    pub total_parts_count: Option<i32>,
    /// Part number marker.
    #[serde(rename = "PartNumberMarker", skip_serializing_if = "Option::is_none")]
    pub part_number_marker: Option<i32>,
    /// Next part number marker.
    #[serde(rename = "NextPartNumberMarker", skip_serializing_if = "Option::is_none")]
    pub next_part_number_marker: Option<i32>,
    /// Maximum parts.
    #[serde(rename = "MaxParts", skip_serializing_if = "Option::is_none")]
    pub max_parts: Option<i32>,
    /// Whether the list is truncated.
    #[serde(rename = "IsTruncated", skip_serializing_if = "Option::is_none")]
    pub is_truncated: Option<bool>,
    /// Part details.
    #[serde(rename = "Part", default, skip_serializing_if = "Vec::is_empty")]
    pub parts: Vec<ObjectPartInfo>,
}

/// Individual part information.
#[derive(Debug, Serialize)]
pub struct ObjectPartInfo {
    /// Part number.
    #[serde(rename = "PartNumber")]
    pub part_number: i32,
    /// Part size in bytes.
    #[serde(rename = "Size")]
    pub size: i64,
    /// Part checksum.
    #[serde(rename = "ChecksumCRC32C", skip_serializing_if = "Option::is_none")]
    pub checksum_crc32c: Option<String>,
}

/// `AccessControlPolicy` response for ACL operations.
#[derive(Debug, Serialize)]
#[serde(rename = "AccessControlPolicy")]
pub struct AccessControlPolicy {
    /// Owner of the bucket/object.
    #[serde(rename = "Owner")]
    pub owner: Owner,
    /// Access control list.
    #[serde(rename = "AccessControlList")]
    pub access_control_list: AccessControlList,
}

/// Access control list.
#[derive(Debug, Serialize)]
pub struct AccessControlList {
    /// List of grants.
    #[serde(rename = "Grant")]
    pub grants: Vec<Grant>,
}

/// Individual grant in ACL.
#[derive(Debug, Serialize)]
pub struct Grant {
    /// The grantee (who gets the permission).
    #[serde(rename = "Grantee")]
    pub grantee: Grantee,
    /// The permission being granted.
    #[serde(rename = "Permission")]
    pub permission: String,
}

/// Grantee in ACL.
#[derive(Debug, Serialize)]
pub struct Grantee {
    /// Type of grantee (CanonicalUser).
    #[serde(rename = "@xsi:type")]
    pub grantee_type: String,
    /// Namespace declaration for xsi.
    #[serde(rename = "@xmlns:xsi")]
    pub xmlns_xsi: String,
    /// User ID.
    #[serde(rename = "ID")]
    pub id: String,
    /// Display name.
    #[serde(rename = "DisplayName")]
    pub display_name: String,
}

impl AccessControlPolicy {
    /// Creates a minimal ACL response with owner having FULL_CONTROL.
    pub fn owner_full_control() -> Self {
        let owner = Owner::default();
        Self {
            access_control_list: AccessControlList {
                grants: vec![Grant {
                    grantee: Grantee {
                        grantee_type: "CanonicalUser".to_string(),
                        xmlns_xsi: "http://www.w3.org/2001/XMLSchema-instance".to_string(),
                        id: owner.id.clone(),
                        display_name: owner.display_name.clone(),
                    },
                    permission: "FULL_CONTROL".to_string(),
                }],
            },
            owner,
        }
    }
}

/// `PublicAccessBlockConfiguration` response.
#[derive(Debug, Serialize)]
#[serde(rename = "PublicAccessBlockConfiguration")]
pub struct PublicAccessBlockConfiguration {
    /// Block public ACLs on this bucket and objects in this bucket.
    #[serde(rename = "BlockPublicAcls")]
    pub block_public_acls: bool,

    /// Ignore public ACLs on this bucket and objects in this bucket.
    #[serde(rename = "IgnorePublicAcls")]
    pub ignore_public_acls: bool,

    /// Block public bucket policies for this bucket.
    #[serde(rename = "BlockPublicPolicy")]
    pub block_public_policy: bool,

    /// Restrict public bucket policies for this bucket.
    #[serde(rename = "RestrictPublicBuckets")]
    pub restrict_public_buckets: bool,
}

/// `LifecycleConfiguration` response.
#[derive(Debug, Serialize)]
#[serde(rename = "LifecycleConfiguration")]
pub struct LifecycleConfigurationResponse {
    /// List of lifecycle rules.
    #[serde(rename = "Rule", default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<LifecycleRuleResponse>,
}

/// A lifecycle rule in the response.
#[derive(Debug, Serialize)]
pub struct LifecycleRuleResponse {
    /// Unique identifier for the rule.
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Status of the rule.
    #[serde(rename = "Status")]
    pub status: String,

    /// Filter to select objects.
    #[serde(rename = "Filter", skip_serializing_if = "Option::is_none")]
    pub filter: Option<LifecycleFilterResponse>,

    /// Expiration settings.
    #[serde(rename = "Expiration", skip_serializing_if = "Option::is_none")]
    pub expiration: Option<ExpirationResponse>,

    /// Noncurrent version expiration settings.
    #[serde(rename = "NoncurrentVersionExpiration", skip_serializing_if = "Option::is_none")]
    pub noncurrent_version_expiration: Option<NoncurrentVersionExpirationResponse>,

    /// Abort incomplete multipart upload settings.
    #[serde(rename = "AbortIncompleteMultipartUpload", skip_serializing_if = "Option::is_none")]
    pub abort_incomplete_multipart_upload: Option<AbortIncompleteMultipartUploadResponse>,
}

/// Lifecycle filter in the response.
#[derive(Debug, Serialize)]
pub struct LifecycleFilterResponse {
    /// Key name prefix.
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    /// Tag filter.
    #[serde(rename = "Tag", skip_serializing_if = "Option::is_none")]
    pub tag: Option<LifecycleTagResponse>,

    /// Logical AND of filters.
    #[serde(rename = "And", skip_serializing_if = "Option::is_none")]
    pub and: Option<LifecycleFilterAndResponse>,
}

/// Logical AND filter for lifecycle.
#[derive(Debug, Serialize)]
pub struct LifecycleFilterAndResponse {
    /// Key name prefix.
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,

    /// Tags to match.
    #[serde(rename = "Tag", default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<LifecycleTagResponse>,
}

/// Tag in lifecycle filter.
#[derive(Debug, Serialize)]
pub struct LifecycleTagResponse {
    /// Tag key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Tag value.
    #[serde(rename = "Value")]
    pub value: String,
}

/// Expiration settings in response.
#[derive(Debug, Serialize)]
pub struct ExpirationResponse {
    /// Days after creation when object expires.
    #[serde(rename = "Days", skip_serializing_if = "Option::is_none")]
    pub days: Option<u32>,
    /// Date when objects expire.
    #[serde(rename = "Date", skip_serializing_if = "Option::is_none")]
    pub date: Option<String>,
    /// Whether to remove expired delete markers.
    #[serde(rename = "ExpiredObjectDeleteMarker", skip_serializing_if = "Option::is_none")]
    pub expired_object_delete_marker: Option<bool>,
}

/// Noncurrent version expiration settings in response.
#[derive(Debug, Serialize)]
pub struct NoncurrentVersionExpirationResponse {
    /// Days after becoming noncurrent when version expires.
    #[serde(rename = "NoncurrentDays")]
    pub noncurrent_days: u32,
    /// Number of noncurrent versions to retain.
    #[serde(rename = "NewerNoncurrentVersions", skip_serializing_if = "Option::is_none")]
    pub newer_noncurrent_versions: Option<u32>,
}

/// Abort incomplete multipart upload settings in response.
#[derive(Debug, Serialize)]
pub struct AbortIncompleteMultipartUploadResponse {
    /// Days after initiation when incomplete uploads are aborted.
    #[serde(rename = "DaysAfterInitiation")]
    pub days_after_initiation: u32,
}

/// `ServerSideEncryptionConfiguration` response.
#[derive(Debug, Serialize)]
#[serde(rename = "ServerSideEncryptionConfiguration")]
pub struct ServerSideEncryptionConfigurationResponse {
    /// List of encryption rules.
    #[serde(rename = "Rule", default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<ServerSideEncryptionRuleResponse>,
}

/// A server-side encryption rule in the response.
#[derive(Debug, Serialize)]
pub struct ServerSideEncryptionRuleResponse {
    /// Default encryption settings.
    #[serde(rename = "ApplyServerSideEncryptionByDefault")]
    pub apply_server_side_encryption_by_default: ApplyServerSideEncryptionByDefaultResponse,
    /// Whether to use bucket key for SSE-KMS.
    #[serde(rename = "BucketKeyEnabled", skip_serializing_if = "Option::is_none")]
    pub bucket_key_enabled: Option<bool>,
}

/// Default encryption settings in response.
#[derive(Debug, Serialize)]
pub struct ApplyServerSideEncryptionByDefaultResponse {
    /// Server-side encryption algorithm.
    #[serde(rename = "SSEAlgorithm")]
    pub sse_algorithm: String,
    /// KMS master key ID (for aws:kms algorithm).
    #[serde(rename = "KMSMasterKeyID", skip_serializing_if = "Option::is_none")]
    pub kms_master_key_id: Option<String>,
}

/// `ReplicationConfiguration` response.
#[derive(Debug, Serialize)]
#[serde(rename = "ReplicationConfiguration")]
pub struct ReplicationConfigurationResponse {
    /// IAM role ARN for replication.
    #[serde(rename = "Role")]
    pub role: String,
    /// List of replication rules.
    #[serde(rename = "Rule", default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<ReplicationRuleResponse>,
}

/// A replication rule in the response.
#[derive(Debug, Serialize)]
pub struct ReplicationRuleResponse {
    /// Unique identifier for the rule.
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    /// Priority of the rule.
    #[serde(rename = "Priority", skip_serializing_if = "Option::is_none")]
    pub priority: Option<u32>,
    /// Status of the rule.
    #[serde(rename = "Status")]
    pub status: String,
    /// Filter to select objects.
    #[serde(rename = "Filter", skip_serializing_if = "Option::is_none")]
    pub filter: Option<ReplicationFilterResponse>,
    /// Destination bucket configuration.
    #[serde(rename = "Destination")]
    pub destination: ReplicationDestinationResponse,
    /// Delete marker replication settings.
    #[serde(rename = "DeleteMarkerReplication", skip_serializing_if = "Option::is_none")]
    pub delete_marker_replication: Option<DeleteMarkerReplicationResponse>,
    /// Source selection criteria.
    #[serde(rename = "SourceSelectionCriteria", skip_serializing_if = "Option::is_none")]
    pub source_selection_criteria: Option<SourceSelectionCriteriaResponse>,
    /// Existing object replication settings.
    #[serde(rename = "ExistingObjectReplication", skip_serializing_if = "Option::is_none")]
    pub existing_object_replication: Option<ExistingObjectReplicationResponse>,
}

/// Replication filter in the response.
#[derive(Debug, Serialize)]
pub struct ReplicationFilterResponse {
    /// Key name prefix.
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Tag filter.
    #[serde(rename = "Tag", skip_serializing_if = "Option::is_none")]
    pub tag: Option<ReplicationTagResponse>,
    /// Logical AND of filters.
    #[serde(rename = "And", skip_serializing_if = "Option::is_none")]
    pub and: Option<ReplicationFilterAndResponse>,
}

/// Logical AND filter for replication.
#[derive(Debug, Serialize)]
pub struct ReplicationFilterAndResponse {
    /// Key name prefix.
    #[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Tags to match.
    #[serde(rename = "Tag", default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<ReplicationTagResponse>,
}

/// Tag in replication filter.
#[derive(Debug, Serialize)]
pub struct ReplicationTagResponse {
    /// Tag key.
    #[serde(rename = "Key")]
    pub key: String,
    /// Tag value.
    #[serde(rename = "Value")]
    pub value: String,
}

/// Destination bucket configuration in response.
#[derive(Debug, Serialize)]
pub struct ReplicationDestinationResponse {
    /// Destination bucket ARN.
    #[serde(rename = "Bucket")]
    pub bucket: String,
    /// Optional account ID for cross-account replication.
    #[serde(rename = "Account", skip_serializing_if = "Option::is_none")]
    pub account: Option<String>,
    /// Storage class for replicated objects.
    #[serde(rename = "StorageClass", skip_serializing_if = "Option::is_none")]
    pub storage_class: Option<String>,
    /// Access control translation settings.
    #[serde(rename = "AccessControlTranslation", skip_serializing_if = "Option::is_none")]
    pub access_control_translation: Option<AccessControlTranslationResponse>,
    /// Encryption configuration.
    #[serde(rename = "EncryptionConfiguration", skip_serializing_if = "Option::is_none")]
    pub encryption_configuration: Option<ReplicationEncryptionConfigurationResponse>,
    /// Replication time control settings.
    #[serde(rename = "ReplicationTime", skip_serializing_if = "Option::is_none")]
    pub replication_time: Option<ReplicationTimeResponse>,
    /// Metrics configuration.
    #[serde(rename = "Metrics", skip_serializing_if = "Option::is_none")]
    pub metrics: Option<ReplicationMetricsResponse>,
}

/// Access control translation settings in response.
#[derive(Debug, Serialize)]
pub struct AccessControlTranslationResponse {
    /// Owner override.
    #[serde(rename = "Owner")]
    pub owner: String,
}

/// Encryption configuration in response.
#[derive(Debug, Serialize)]
pub struct ReplicationEncryptionConfigurationResponse {
    /// KMS key ID for destination encryption.
    #[serde(rename = "ReplicaKmsKeyID", skip_serializing_if = "Option::is_none")]
    pub replica_kms_key_id: Option<String>,
}

/// Replication time control in response.
#[derive(Debug, Serialize)]
pub struct ReplicationTimeResponse {
    /// Status of replication time control.
    #[serde(rename = "Status")]
    pub status: String,
    /// Time threshold.
    #[serde(rename = "Time", skip_serializing_if = "Option::is_none")]
    pub time: Option<ReplicationTimeValueResponse>,
}

/// Time value in response.
#[derive(Debug, Serialize)]
pub struct ReplicationTimeValueResponse {
    /// Minutes threshold.
    #[serde(rename = "Minutes")]
    pub minutes: u32,
}

/// Metrics configuration in response.
#[derive(Debug, Serialize)]
pub struct ReplicationMetricsResponse {
    /// Status of metrics.
    #[serde(rename = "Status")]
    pub status: String,
    /// Event threshold.
    #[serde(rename = "EventThreshold", skip_serializing_if = "Option::is_none")]
    pub event_threshold: Option<ReplicationTimeValueResponse>,
}

/// Delete marker replication settings in response.
#[derive(Debug, Serialize)]
pub struct DeleteMarkerReplicationResponse {
    /// Status of delete marker replication.
    #[serde(rename = "Status")]
    pub status: String,
}

/// Source selection criteria in response.
#[derive(Debug, Serialize)]
pub struct SourceSelectionCriteriaResponse {
    /// SSE-KMS encrypted objects settings.
    #[serde(rename = "SseKmsEncryptedObjects", skip_serializing_if = "Option::is_none")]
    pub sse_kms_encrypted_objects: Option<SseKmsEncryptedObjectsResponse>,
    /// Replica modifications settings.
    #[serde(rename = "ReplicaModifications", skip_serializing_if = "Option::is_none")]
    pub replica_modifications: Option<ReplicaModificationsResponse>,
}

/// SSE-KMS encrypted objects settings in response.
#[derive(Debug, Serialize)]
pub struct SseKmsEncryptedObjectsResponse {
    /// Status.
    #[serde(rename = "Status")]
    pub status: String,
}

/// Replica modifications settings in response.
#[derive(Debug, Serialize)]
pub struct ReplicaModificationsResponse {
    /// Status.
    #[serde(rename = "Status")]
    pub status: String,
}

/// Existing object replication settings in response.
#[derive(Debug, Serialize)]
pub struct ExistingObjectReplicationResponse {
    /// Status.
    #[serde(rename = "Status")]
    pub status: String,
}

/// `WebsiteConfiguration` response.
#[derive(Debug, Serialize)]
#[serde(rename = "WebsiteConfiguration")]
pub struct WebsiteConfigurationResponse {
    /// Index document configuration.
    #[serde(rename = "IndexDocument", skip_serializing_if = "Option::is_none")]
    pub index_document: Option<IndexDocumentResponse>,

    /// Error document configuration.
    #[serde(rename = "ErrorDocument", skip_serializing_if = "Option::is_none")]
    pub error_document: Option<ErrorDocumentResponse>,

    /// Redirect all requests to another host.
    #[serde(rename = "RedirectAllRequestsTo", skip_serializing_if = "Option::is_none")]
    pub redirect_all_requests_to: Option<RedirectAllRequestsToResponse>,

    /// Routing rules for conditional redirects.
    #[serde(rename = "RoutingRules", skip_serializing_if = "Option::is_none")]
    pub routing_rules: Option<RoutingRulesResponse>,
}

/// Index document configuration in response.
#[derive(Debug, Serialize)]
pub struct IndexDocumentResponse {
    /// The suffix appended to requests for a directory.
    #[serde(rename = "Suffix")]
    pub suffix: String,
}

/// Error document configuration in response.
#[derive(Debug, Serialize)]
pub struct ErrorDocumentResponse {
    /// The object key to return when an error occurs.
    #[serde(rename = "Key")]
    pub key: String,
}

/// Redirect all requests to another host in response.
#[derive(Debug, Serialize)]
pub struct RedirectAllRequestsToResponse {
    /// The host name to redirect to.
    #[serde(rename = "HostName")]
    pub host_name: String,

    /// The protocol to use for the redirect.
    #[serde(rename = "Protocol", skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,
}

/// Routing rules wrapper in response.
#[derive(Debug, Serialize)]
pub struct RoutingRulesResponse {
    /// List of routing rules.
    #[serde(rename = "RoutingRule", default, skip_serializing_if = "Vec::is_empty")]
    pub rules: Vec<RoutingRuleResponse>,
}

/// A routing rule in the response.
#[derive(Debug, Serialize)]
pub struct RoutingRuleResponse {
    /// The condition that must be met for the redirect to apply.
    #[serde(rename = "Condition", skip_serializing_if = "Option::is_none")]
    pub condition: Option<RoutingRuleConditionResponse>,

    /// The redirect action to take.
    #[serde(rename = "Redirect")]
    pub redirect: RoutingRuleRedirectResponse,
}

/// Condition for a routing rule in response.
#[derive(Debug, Serialize)]
pub struct RoutingRuleConditionResponse {
    /// Redirect only if the object key starts with this prefix.
    #[serde(rename = "KeyPrefixEquals", skip_serializing_if = "Option::is_none")]
    pub key_prefix_equals: Option<String>,

    /// Redirect only if the HTTP error code matches.
    #[serde(rename = "HttpErrorCodeReturnedEquals", skip_serializing_if = "Option::is_none")]
    pub http_error_code_returned_equals: Option<String>,
}

/// Redirect action for a routing rule in response.
#[derive(Debug, Serialize)]
pub struct RoutingRuleRedirectResponse {
    /// The host name to redirect to.
    #[serde(rename = "HostName", skip_serializing_if = "Option::is_none")]
    pub host_name: Option<String>,

    /// The HTTP redirect code.
    #[serde(rename = "HttpRedirectCode", skip_serializing_if = "Option::is_none")]
    pub http_redirect_code: Option<String>,

    /// The protocol to use for the redirect.
    #[serde(rename = "Protocol", skip_serializing_if = "Option::is_none")]
    pub protocol: Option<String>,

    /// Replace the key prefix with this value.
    #[serde(rename = "ReplaceKeyPrefixWith", skip_serializing_if = "Option::is_none")]
    pub replace_key_prefix_with: Option<String>,

    /// Replace the entire key with this value.
    #[serde(rename = "ReplaceKeyWith", skip_serializing_if = "Option::is_none")]
    pub replace_key_with: Option<String>,
}

/// `BucketLoggingStatus` response.
#[derive(Debug, Serialize)]
#[serde(rename = "BucketLoggingStatus")]
pub struct BucketLoggingStatusResponse {
    /// XML namespace.
    #[serde(rename = "@xmlns")]
    pub xmlns: &'static str,

    /// Logging enabled configuration.
    #[serde(rename = "LoggingEnabled", skip_serializing_if = "Option::is_none")]
    pub logging_enabled: Option<LoggingEnabledResponse>,
}

impl Default for BucketLoggingStatusResponse {
    fn default() -> Self {
        Self { xmlns: "http://s3.amazonaws.com/doc/2006-03-01/", logging_enabled: None }
    }
}

/// Logging enabled configuration in response.
#[derive(Debug, Serialize)]
pub struct LoggingEnabledResponse {
    /// Target bucket for log files.
    #[serde(rename = "TargetBucket")]
    pub target_bucket: String,

    /// Prefix for log object keys.
    #[serde(rename = "TargetPrefix")]
    pub target_prefix: String,

    /// Grants for access to log files.
    #[serde(rename = "TargetGrants", skip_serializing_if = "Option::is_none")]
    pub target_grants: Option<TargetGrantsResponse>,
}

/// Target grants container in response.
#[derive(Debug, Serialize)]
pub struct TargetGrantsResponse {
    /// List of grants.
    #[serde(rename = "Grant", default)]
    pub grants: Vec<LoggingGrantResponse>,
}

/// A grant for log file access in response.
#[derive(Debug, Serialize)]
pub struct LoggingGrantResponse {
    /// The grantee.
    #[serde(rename = "Grantee")]
    pub grantee: LoggingGranteeResponse,

    /// The permission granted.
    #[serde(rename = "Permission")]
    pub permission: String,
}

/// Grantee in a logging grant response.
#[derive(Debug, Serialize)]
pub struct LoggingGranteeResponse {
    /// Type of grantee (xsi:type attribute).
    #[serde(rename = "@xsi:type", skip_serializing_if = "Option::is_none")]
    pub grantee_type: Option<String>,

    /// XML namespace for XSI.
    #[serde(rename = "@xmlns:xsi", skip_serializing_if = "Option::is_none")]
    pub xmlns_xsi: Option<&'static str>,

    /// Canonical user ID.
    #[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

    /// Display name.
    #[serde(rename = "DisplayName", skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,

    /// Email address.
    #[serde(rename = "EmailAddress", skip_serializing_if = "Option::is_none")]
    pub email_address: Option<String>,

    /// Group URI.
    #[serde(rename = "URI", skip_serializing_if = "Option::is_none")]
    pub uri: Option<String>,
}

/// Serialize a response to XML.
///
/// # Errors
///
/// Returns an error if serialization fails.
pub fn to_xml<T: Serialize>(value: &T) -> Result<String, String> {
    let mut buffer = String::from("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n");
    quick_xml::se::to_writer(&mut buffer, value).map_err(|e| e.to_string())?;
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_list_buckets_xml() {
        let response = ListBucketsResponse {
            owner: Owner::default(),
            buckets: Buckets {
                bucket: vec![BucketEntry {
                    name: "test-bucket".to_string(),
                    creation_date: "2024-01-01T00:00:00Z".to_string(),
                }],
            },
        };

        let xml = to_xml(&response).unwrap();
        assert!(xml.contains("<Name>test-bucket</Name>"));
        assert!(xml.contains("<DisplayName>rucket</DisplayName>"));
    }

    #[test]
    fn test_tagging_response_xml() {
        let response = TaggingResponse {
            tag_set: TagSetResponse {
                tags: vec![
                    TagResponse { key: "env".to_string(), value: "test".to_string() },
                    TagResponse { key: "project".to_string(), value: "rucket".to_string() },
                ],
            },
        };

        let xml = to_xml(&response).unwrap();
        assert!(xml.contains("<Tagging"));
        assert!(xml.contains("<TagSet>"));
        assert!(xml.contains("<Tag>"));
        assert!(xml.contains("<Key>env</Key>"));
        assert!(xml.contains("<Value>test</Value>"));
    }
}
