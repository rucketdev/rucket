// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! S3 XML response generation.

use chrono::{DateTime, Utc};
use rucket_core::types::{BucketInfo, ObjectMetadata};
use serde::Serialize;

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
        Self {
            id: "rucket".to_string(),
            display_name: "rucket".to_string(),
        }
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
        Self {
            name: info.name.clone(),
            creation_date: info.created_at.to_rfc3339(),
        }
    }
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
    /// Max keys requested.
    #[serde(rename = "MaxKeys")]
    pub max_keys: u32,
    /// Whether results are truncated.
    #[serde(rename = "IsTruncated")]
    pub is_truncated: bool,
    /// Continuation token for next page.
    #[serde(rename = "NextContinuationToken", skip_serializing_if = "Option::is_none")]
    pub next_continuation_token: Option<String>,
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
    /// Storage class.
    #[serde(rename = "StorageClass")]
    pub storage_class: String,
}

impl From<&ObjectMetadata> for ObjectEntry {
    fn from(meta: &ObjectMetadata) -> Self {
        Self {
            key: meta.key.clone(),
            last_modified: meta.last_modified.to_rfc3339(),
            etag: meta.etag.as_str().to_string(),
            size: meta.size,
            storage_class: "STANDARD".to_string(),
        }
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
    /// Initiation timestamp.
    #[serde(rename = "Initiated")]
    pub initiated: String,
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
        Self {
            etag,
            last_modified: last_modified.to_rfc3339(),
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
