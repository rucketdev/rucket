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
            storage_class: "STANDARD".to_string(),
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
            storage_class: "STANDARD".to_string(),
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
            storage_class: "STANDARD".to_string(),
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
            storage_class: "STANDARD".to_string(),
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
}
