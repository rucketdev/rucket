// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

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

/// Metadata for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    /// Bucket name.
    pub name: String,
    /// When the bucket was created.
    pub created_at: DateTime<Utc>,
}

impl BucketInfo {
    /// Creates a new bucket info.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            created_at: Utc::now(),
        }
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
    /// Content type (MIME type).
    pub content_type: Option<String>,
    /// When the object was last modified.
    pub last_modified: DateTime<Utc>,
    /// Custom user metadata.
    #[serde(default)]
    pub user_metadata: std::collections::HashMap<String, String>,
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
            content_type: None,
            last_modified: Utc::now(),
            user_metadata: std::collections::HashMap::new(),
        }
    }

    /// Sets the content type.
    #[must_use]
    pub fn with_content_type(mut self, content_type: impl Into<String>) -> Self {
        self.content_type = Some(content_type.into());
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
        Self {
            id: "rucket".to_string(),
            display_name: "Rucket".to_string(),
        }
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
        let meta = ObjectMetadata::new("test/key.txt", uuid, 1024, etag)
            .with_content_type("text/plain");

        assert_eq!(meta.key, "test/key.txt");
        assert_eq!(meta.size, 1024);
        assert_eq!(meta.content_type, Some("text/plain".to_string()));
    }
}
