//! Server-side encryption configuration for S3 buckets.
//!
//! This module provides types for S3's bucket-level server-side encryption
//! configuration, which can specify default encryption for all objects.

use serde::{Deserialize, Serialize};

/// Server-side encryption configuration for a bucket.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerSideEncryptionConfiguration {
    /// List of encryption rules (typically just one).
    pub rules: Vec<ServerSideEncryptionRule>,
}

/// A server-side encryption rule.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerSideEncryptionRule {
    /// The default server-side encryption to apply.
    pub apply_server_side_encryption_by_default: ApplyServerSideEncryptionByDefault,
    /// Whether to use the bucket key for SSE-KMS.
    #[serde(default)]
    pub bucket_key_enabled: bool,
}

/// Default encryption settings to apply to new objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApplyServerSideEncryptionByDefault {
    /// The server-side encryption algorithm (AES256 or aws:kms).
    pub sse_algorithm: SseAlgorithm,
    /// KMS master key ID (only for aws:kms algorithm).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kms_master_key_id: Option<String>,
}

/// Server-side encryption algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SseAlgorithm {
    /// AES256 encryption (S3-managed keys).
    #[serde(rename = "AES256")]
    Aes256,
    /// AWS KMS encryption.
    #[serde(rename = "aws:kms")]
    AwsKms,
}

impl SseAlgorithm {
    /// Parse from string representation.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "AES256" => Some(Self::Aes256),
            "aws:kms" => Some(Self::AwsKms),
            _ => None,
        }
    }

    /// Convert to string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aes256 => "AES256",
            Self::AwsKms => "aws:kms",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sse_algorithm_parsing() {
        assert_eq!(SseAlgorithm::parse("AES256"), Some(SseAlgorithm::Aes256));
        assert_eq!(SseAlgorithm::parse("aws:kms"), Some(SseAlgorithm::AwsKms));
        assert_eq!(SseAlgorithm::parse("invalid"), None);
    }

    #[test]
    fn test_serde_roundtrip() {
        let config = ServerSideEncryptionConfiguration {
            rules: vec![ServerSideEncryptionRule {
                apply_server_side_encryption_by_default: ApplyServerSideEncryptionByDefault {
                    sse_algorithm: SseAlgorithm::Aes256,
                    kms_master_key_id: None,
                },
                bucket_key_enabled: false,
            }],
        };

        let json = serde_json::to_string(&config).unwrap();
        let parsed: ServerSideEncryptionConfiguration = serde_json::from_str(&json).unwrap();
        assert_eq!(config, parsed);
    }
}
