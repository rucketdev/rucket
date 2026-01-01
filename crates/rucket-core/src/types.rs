//! Common types used throughout Rucket.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// An S3 ETag value.
///
/// ETags are MD5 hashes of object content for single-part uploads,
/// or `MD5(concat(part_md5s))-{num_parts}` for multipart uploads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ETag(String);

impl ETag {
    /// Creates a new ETag from a string value.
    ///
    /// The value should be quoted (e.g., `"d41d8cd98f00b204e9800998ecf8427e"`).
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Creates an ETag from an MD5 hash (single-part upload).
    #[must_use]
    pub fn from_md5(hash: &[u8; 16]) -> Self {
        Self(format!("\"{}\"", hex::encode(hash)))
    }

    /// Creates an ETag for a multipart upload.
    #[must_use]
    pub fn from_multipart(hash: &[u8; 16], num_parts: usize) -> Self {
        Self(format!("\"{}-{}\"", hex::encode(hash), num_parts))
    }

    /// Returns the ETag value as a string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns whether this ETag is from a multipart upload.
    #[must_use]
    pub fn is_multipart(&self) -> bool {
        self.0.contains('-')
    }
}

impl std::fmt::Display for ETag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<String> for ETag {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&str> for ETag {
    fn from(value: &str) -> Self {
        Self(value.to_string())
    }
}

/// Versioning status for a bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VersioningStatus {
    /// Versioning is enabled - new objects get unique version IDs.
    Enabled,
    /// Versioning is suspended - new objects get version ID "null".
    Suspended,
}

impl VersioningStatus {
    /// Returns the status as a string for S3 API responses.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Enabled => "Enabled",
            Self::Suspended => "Suspended",
        }
    }

    /// Parses a versioning status from a string.
    #[must_use]
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(Self::Enabled),
            "Suspended" => Some(Self::Suspended),
            _ => None,
        }
    }
}

/// Metadata for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    /// Bucket name.
    pub name: String,
    /// When the bucket was created.
    pub created_at: DateTime<Utc>,
    /// Versioning status (None = never enabled, Some = enabled or suspended).
    #[serde(default)]
    pub versioning_status: Option<VersioningStatus>,
}

impl BucketInfo {
    /// Creates a new bucket info.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), created_at: Utc::now(), versioning_status: None }
    }

    /// Sets the versioning status.
    #[must_use]
    pub fn with_versioning(mut self, status: VersioningStatus) -> Self {
        self.versioning_status = Some(status);
        self
    }
}

/// Metadata for an object.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectMetadata {
    /// Object key.
    pub key: String,
    /// UUID for the stored file.
    pub uuid: Uuid,
    /// Object size in bytes.
    pub size: u64,
    /// Object ETag.
    pub etag: ETag,
    /// CRC32C checksum for data integrity verification.
    /// Uses IEEE polynomial with hardware acceleration when available.
    #[serde(default)]
    pub crc32c: Option<u32>,
    /// Content type (MIME type).
    pub content_type: Option<String>,
    /// Cache-Control header.
    #[serde(default)]
    pub cache_control: Option<String>,
    /// Content-Disposition header.
    #[serde(default)]
    pub content_disposition: Option<String>,
    /// Content-Encoding header.
    #[serde(default)]
    pub content_encoding: Option<String>,
    /// Expires header.
    #[serde(default)]
    pub expires: Option<String>,
    /// When the object was last modified.
    pub last_modified: DateTime<Utc>,
    /// Custom user metadata.
    #[serde(default)]
    pub user_metadata: std::collections::HashMap<String, String>,
    /// Version ID for this object (None = "null" for non-versioned buckets).
    #[serde(default)]
    pub version_id: Option<String>,
    /// Whether this is a delete marker (for versioned buckets).
    #[serde(default)]
    pub is_delete_marker: bool,
    /// Whether this is the latest version of the object.
    #[serde(default = "default_is_latest")]
    pub is_latest: bool,
}

fn default_is_latest() -> bool {
    true
}

impl ObjectMetadata {
    /// Creates new object metadata.
    #[must_use]
    pub fn new(key: impl Into<String>, uuid: Uuid, size: u64, etag: ETag) -> Self {
        Self {
            key: key.into(),
            uuid,
            size,
            etag,
            crc32c: None,
            content_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            expires: None,
            last_modified: Utc::now(),
            user_metadata: std::collections::HashMap::new(),
            version_id: None,
            is_delete_marker: false,
            is_latest: true,
        }
    }

    /// Creates a delete marker for versioned buckets.
    #[must_use]
    pub fn new_delete_marker(key: impl Into<String>, version_id: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            uuid: Uuid::nil(),
            size: 0,
            etag: ETag::new(""),
            crc32c: None,
            content_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            expires: None,
            last_modified: Utc::now(),
            user_metadata: std::collections::HashMap::new(),
            version_id: Some(version_id.into()),
            is_delete_marker: true,
            is_latest: true,
        }
    }

    /// Sets the version ID.
    #[must_use]
    pub fn with_version_id(mut self, version_id: impl Into<String>) -> Self {
        self.version_id = Some(version_id.into());
        self
    }

    /// Sets the content type.
    #[must_use]
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
        self
    }

    /// Sets the CRC32C checksum.
    #[must_use]
    pub fn with_checksum(mut self, crc32c: u32) -> Self {
        self.crc32c = Some(crc32c);
        self
    }

    /// Sets the user metadata.
    #[must_use]
    pub fn with_user_metadata(
        mut self,
        user_metadata: std::collections::HashMap<String, String>,
    ) -> Self {
        self.user_metadata = user_metadata;
        self
    }
}

/// Owner information for S3 responses.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Owner {
    /// Owner ID.
    pub id: String,
    /// Owner display name.
    pub display_name: String,
}

impl Default for Owner {
    fn default() -> Self {
        Self { id: "rucket".to_string(), display_name: "Rucket".to_string() }
    }
}

/// Represents the status of a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultipartUpload {
    /// Upload ID.
    pub upload_id: String,
    /// Bucket name.
    pub bucket: String,
    /// Object key.
    pub key: String,
    /// When the upload was initiated.
    pub initiated: DateTime<Utc>,
    /// Content type (MIME type) for the final object.
    #[serde(default)]
    pub content_type: Option<String>,
    /// Custom user metadata for the final object.
    #[serde(default)]
    pub user_metadata: std::collections::HashMap<String, String>,
}

/// Represents a part in a multipart upload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Part {
    /// Part number (1-10000).
    pub part_number: u32,
    /// Part ETag.
    pub etag: ETag,
    /// Part size in bytes.
    pub size: u64,
    /// When the part was uploaded.
    pub last_modified: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_etag_from_md5() {
        let hash: [u8; 16] = [
            0xd4, 0x1d, 0x8c, 0xd9, 0x8f, 0x00, 0xb2, 0x04, 0xe9, 0x80, 0x09, 0x98, 0xec, 0xf8,
            0x42, 0x7e,
        ];
        let etag = ETag::from_md5(&hash);
        assert_eq!(etag.as_str(), "\"d41d8cd98f00b204e9800998ecf8427e\"");
        assert!(!etag.is_multipart());
    }

    #[test]
    fn test_etag_multipart() {
        let hash: [u8; 16] = [0; 16];
        let etag = ETag::from_multipart(&hash, 3);
        assert!(etag.as_str().ends_with("-3\""));
        assert!(etag.is_multipart());
    }

    #[test]
    fn test_bucket_info() {
        let bucket = BucketInfo::new("test-bucket");
        assert_eq!(bucket.name, "test-bucket");
    }

    #[test]
    fn test_object_metadata() {
        let uuid = Uuid::new_v4();
        let etag = ETag::new("\"abc123\"");
        let meta =
            ObjectMetadata::new("test/key.txt", uuid, 1024, etag).with_content_type("text/plain");

        assert_eq!(meta.key, "test/key.txt");
        assert_eq!(meta.size, 1024);
        assert_eq!(meta.content_type, Some("text/plain".to_string()));
    }
}
