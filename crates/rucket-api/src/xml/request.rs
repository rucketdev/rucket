//! S3 XML request parsing.

use serde::Deserialize;

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
}
