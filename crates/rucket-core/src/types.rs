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

/// Checksum algorithm used for object integrity verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    /// CRC32 checksum.
    Crc32,
    /// CRC32C checksum (Castagnoli polynomial).
    Crc32C,
    /// SHA-1 checksum.
    Sha1,
    /// SHA-256 checksum.
    Sha256,
}

impl ChecksumAlgorithm {
    /// Returns the algorithm name as used in S3 API headers.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32C => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
        }
    }

    /// Parses a checksum algorithm from a string.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "CRC32" => Some(Self::Crc32),
            "CRC32C" => Some(Self::Crc32C),
            "SHA1" => Some(Self::Sha1),
            "SHA256" => Some(Self::Sha256),
            _ => None,
        }
    }

    /// Returns the header name for this checksum algorithm.
    #[must_use]
    pub fn header_name(&self) -> &'static str {
        match self {
            Self::Crc32 => "x-amz-checksum-crc32",
            Self::Crc32C => "x-amz-checksum-crc32c",
            Self::Sha1 => "x-amz-checksum-sha1",
            Self::Sha256 => "x-amz-checksum-sha256",
        }
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
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "Enabled" => Some(Self::Enabled),
            "Suspended" => Some(Self::Suspended),
            _ => None,
        }
    }
}

/// Storage class for objects, determining durability and access patterns.
///
/// These map to S3-compatible storage classes and will be used for
/// tiered storage in distributed deployments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum StorageClass {
    /// Standard storage with high durability and availability.
    /// Default for all objects.
    #[default]
    Standard,
    /// Reduced redundancy storage (deprecated but still supported).
    ReducedRedundancy,
    /// Infrequent access storage with lower cost but higher retrieval latency.
    StandardIa,
    /// One-zone infrequent access storage.
    OnezoneIa,
    /// Intelligent tiering that automatically moves objects between tiers.
    IntelligentTiering,
    /// Archive storage for long-term retention with high retrieval latency.
    Glacier,
    /// Deep archive storage for very long-term retention.
    DeepArchive,
    /// Glacier Instant Retrieval storage class.
    GlacierIr,
}

impl StorageClass {
    /// Returns the storage class as a string for S3 API responses.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::ReducedRedundancy => "REDUCED_REDUNDANCY",
            Self::StandardIa => "STANDARD_IA",
            Self::OnezoneIa => "ONEZONE_IA",
            Self::IntelligentTiering => "INTELLIGENT_TIERING",
            Self::Glacier => "GLACIER",
            Self::DeepArchive => "DEEP_ARCHIVE",
            Self::GlacierIr => "GLACIER_IR",
        }
    }

    /// Parses a storage class from a string.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "STANDARD" => Some(Self::Standard),
            "REDUCED_REDUNDANCY" => Some(Self::ReducedRedundancy),
            "STANDARD_IA" => Some(Self::StandardIa),
            "ONEZONE_IA" => Some(Self::OnezoneIa),
            "INTELLIGENT_TIERING" => Some(Self::IntelligentTiering),
            "GLACIER" => Some(Self::Glacier),
            "DEEP_ARCHIVE" => Some(Self::DeepArchive),
            "GLACIER_IR" => Some(Self::GlacierIr),
            _ => None,
        }
    }
}

/// Replication status for an object in geo-distributed deployments.
///
/// Tracks the replication state across regions for cross-region replication (CRR).
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum ReplicationStatus {
    /// Object has not been replicated (single-region or replication disabled).
    #[default]
    None,
    /// Object is pending replication to target regions.
    Pending {
        /// Target regions awaiting replication.
        target_regions: Vec<String>,
    },
    /// Object has been successfully replicated.
    Replicated {
        /// Regions where the object is replicated.
        regions: Vec<String>,
        /// HLC timestamp when replication completed.
        hlc: u64,
    },
    /// Replication failed.
    Failed {
        /// Reason for the failure.
        reason: String,
    },
}

/// Server-side encryption algorithm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    /// AES-256 encryption using S3-managed keys (SSE-S3).
    Aes256,
    /// AWS KMS encryption (SSE-KMS) - not yet implemented.
    AwsKms,
}

impl EncryptionAlgorithm {
    /// Returns the algorithm as a string for S3 API responses.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Aes256 => "AES256",
            Self::AwsKms => "aws:kms",
        }
    }
}

/// Bucket-level encryption configuration.
///
/// Specifies the default server-side encryption to apply to new objects.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EncryptionConfig {
    /// The encryption algorithm to use.
    pub algorithm: EncryptionAlgorithm,
    /// KMS key ID (only for SSE-KMS).
    pub kms_key_id: Option<String>,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self { algorithm: EncryptionAlgorithm::Aes256, kms_key_id: None }
    }
}

/// Object Lock retention mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionMode {
    /// Governance mode - can be overridden by users with special permissions.
    Governance,
    /// Compliance mode - cannot be overridden by any user, including root.
    Compliance,
}

impl RetentionMode {
    /// Returns the mode as a string for S3 API responses.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Governance => "GOVERNANCE",
            Self::Compliance => "COMPLIANCE",
        }
    }

    /// Parses a retention mode from a string.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_uppercase().as_str() {
            "GOVERNANCE" => Some(Self::Governance),
            "COMPLIANCE" => Some(Self::Compliance),
            _ => None,
        }
    }
}

/// Object-level retention configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectRetention {
    /// The retention mode.
    pub mode: RetentionMode,
    /// The date until which the object is retained.
    pub retain_until_date: DateTime<Utc>,
}

/// Default retention configuration for Object Lock.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DefaultRetention {
    /// The retention mode.
    pub mode: RetentionMode,
    /// Retention period in days (mutually exclusive with years).
    pub days: Option<u32>,
    /// Retention period in years (mutually exclusive with days).
    pub years: Option<u32>,
}

/// Object Lock configuration for a bucket.
///
/// Once enabled on a bucket, Object Lock cannot be disabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectLockConfig {
    /// Whether Object Lock is enabled.
    pub enabled: bool,
    /// Default retention settings applied to new objects.
    pub default_retention: Option<DefaultRetention>,
}

impl Default for ObjectLockConfig {
    fn default() -> Self {
        Self { enabled: true, default_retention: None }
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

    // --- Forward-compatible fields for distributed deployment ---
    /// Server-side encryption configuration.
    #[serde(default)]
    pub encryption_config: Option<EncryptionConfig>,
    /// Object Lock configuration (immutability).
    #[serde(default)]
    pub lock_config: Option<ObjectLockConfig>,
}

impl BucketInfo {
    /// Creates a new bucket info.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            created_at: Utc::now(),
            versioning_status: None,
            encryption_config: None,
            lock_config: None,
        }
    }

    /// Sets the versioning status.
    #[must_use]
    pub fn with_versioning(mut self, status: VersioningStatus) -> Self {
        self.versioning_status = Some(status);
        self
    }

    /// Sets the encryption configuration.
    #[must_use]
    pub fn with_encryption(mut self, config: EncryptionConfig) -> Self {
        self.encryption_config = Some(config);
        self
    }

    /// Sets the Object Lock configuration.
    #[must_use]
    pub fn with_lock_config(mut self, config: ObjectLockConfig) -> Self {
        self.lock_config = Some(config);
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
    /// Uses Castagnoli polynomial with hardware acceleration when available.
    #[serde(default)]
    pub crc32c: Option<u32>,
    /// CRC32 checksum (ISO 3309 polynomial).
    #[serde(default)]
    pub crc32: Option<u32>,
    /// SHA-1 checksum (20 bytes).
    #[serde(default)]
    pub sha1: Option<[u8; 20]>,
    /// SHA-256 checksum (32 bytes).
    #[serde(default)]
    pub sha256: Option<[u8; 32]>,
    /// The checksum algorithm that was requested during upload.
    #[serde(default)]
    pub checksum_algorithm: Option<ChecksumAlgorithm>,
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
    /// Content-Language header.
    #[serde(default)]
    pub content_language: Option<String>,
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

    // --- Forward-compatible fields for distributed deployment ---
    /// Hybrid logical clock timestamp for causality ordering.
    /// Default: 0 (will be set by HLC when distributed features are enabled).
    #[serde(default)]
    pub hlc_timestamp: u64,
    /// Placement group for data distribution.
    /// Default: 0 (single-node deployment uses PG 0 for all objects).
    #[serde(default)]
    pub placement_group: u32,
    /// Home region for geo-distributed deployments.
    /// Default: "local" (single-region deployment).
    #[serde(default = "default_home_region")]
    pub home_region: String,
    /// Storage class for tiered storage.
    #[serde(default)]
    pub storage_class: StorageClass,
    /// Replication status for cross-region replication.
    #[serde(default)]
    pub replication_status: Option<ReplicationStatus>,

    // --- Object Lock fields ---
    /// Object-level retention configuration.
    #[serde(default)]
    pub retention: Option<ObjectRetention>,
    /// Legal hold status (prevents deletion until removed).
    #[serde(default)]
    pub legal_hold: bool,

    // --- Encryption fields ---
    /// Server-side encryption algorithm (e.g., "AES256" for SSE-S3).
    #[serde(default)]
    pub server_side_encryption: Option<String>,
    /// Encryption nonce (for AES-GCM).
    #[serde(default)]
    pub encryption_nonce: Option<Vec<u8>>,
    /// SSE-C algorithm (customer-provided encryption) - always "AES256" if SSE-C is used.
    #[serde(default)]
    pub sse_customer_algorithm: Option<String>,
    /// MD5 hash of the customer-provided encryption key (base64-encoded).
    /// Used to verify the correct key is provided during retrieval.
    #[serde(default)]
    pub sse_customer_key_md5: Option<String>,
}

fn default_is_latest() -> bool {
    true
}

fn default_home_region() -> String {
    "local".to_string()
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
            crc32: None,
            sha1: None,
            sha256: None,
            checksum_algorithm: None,
            content_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            expires: None,
            content_language: None,
            last_modified: Utc::now(),
            user_metadata: std::collections::HashMap::new(),
            version_id: None,
            is_delete_marker: false,
            is_latest: true,
            // Forward-compatible fields
            hlc_timestamp: 0,
            placement_group: 0,
            home_region: "local".to_string(),
            storage_class: StorageClass::Standard,
            replication_status: None,
            // Object Lock fields
            retention: None,
            legal_hold: false,
            // Encryption fields
            server_side_encryption: None,
            encryption_nonce: None,
            sse_customer_algorithm: None,
            sse_customer_key_md5: None,
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
            crc32: None,
            sha1: None,
            sha256: None,
            checksum_algorithm: None,
            content_type: None,
            cache_control: None,
            content_disposition: None,
            content_encoding: None,
            expires: None,
            content_language: None,
            last_modified: Utc::now(),
            user_metadata: std::collections::HashMap::new(),
            version_id: Some(version_id.into()),
            is_delete_marker: true,
            is_latest: true,
            // Forward-compatible fields
            hlc_timestamp: 0,
            placement_group: 0,
            home_region: "local".to_string(),
            storage_class: StorageClass::Standard,
            replication_status: None,
            // Object Lock fields
            retention: None,
            legal_hold: false,
            // Encryption fields
            server_side_encryption: None,
            encryption_nonce: None,
            sse_customer_algorithm: None,
            sse_customer_key_md5: None,
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

    /// Sets the CRC32C checksum (for internal integrity verification).
    #[must_use]
    pub fn with_checksum(mut self, crc32c: u32) -> Self {
        self.crc32c = Some(crc32c);
        self
    }

    /// Sets the checksum for a specific algorithm.
    #[must_use]
    pub fn with_algorithm_checksum(
        mut self,
        algorithm: ChecksumAlgorithm,
        checksum: Checksum,
    ) -> Self {
        self.checksum_algorithm = Some(algorithm);
        match checksum {
            Checksum::Crc32(v) => self.crc32 = Some(v),
            Checksum::Crc32C(v) => self.crc32c = Some(v),
            Checksum::Sha1(v) => self.sha1 = Some(v),
            Checksum::Sha256(v) => self.sha256 = Some(v),
        }
        self
    }

    /// Gets the checksum for the requested algorithm, if stored.
    #[must_use]
    pub fn get_checksum(&self, algorithm: ChecksumAlgorithm) -> Option<Checksum> {
        match algorithm {
            ChecksumAlgorithm::Crc32 => self.crc32.map(Checksum::Crc32),
            ChecksumAlgorithm::Crc32C => self.crc32c.map(Checksum::Crc32C),
            ChecksumAlgorithm::Sha1 => self.sha1.map(Checksum::Sha1),
            ChecksumAlgorithm::Sha256 => self.sha256.map(Checksum::Sha256),
        }
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

    /// Sets the object retention.
    #[must_use]
    pub fn with_retention(mut self, retention: ObjectRetention) -> Self {
        self.retention = Some(retention);
        self
    }

    /// Sets the legal hold status.
    #[must_use]
    pub fn with_legal_hold(mut self, enabled: bool) -> Self {
        self.legal_hold = enabled;
        self
    }

    /// Sets the server-side encryption metadata.
    #[must_use]
    pub fn with_encryption(mut self, algorithm: &str, nonce: Vec<u8>) -> Self {
        self.server_side_encryption = Some(algorithm.to_string());
        self.encryption_nonce = Some(nonce);
        self
    }

    /// Returns true if the object is encrypted.
    #[must_use]
    pub fn is_encrypted(&self) -> bool {
        self.server_side_encryption.is_some()
    }

    /// Returns true if the object is protected by retention or legal hold.
    #[must_use]
    pub fn is_locked(&self) -> bool {
        self.legal_hold || self.retention.is_some()
    }

    /// Returns true if the object's retention has expired.
    #[must_use]
    pub fn is_retention_expired(&self) -> bool {
        match &self.retention {
            Some(retention) => Utc::now() >= retention.retain_until_date,
            None => true,
        }
    }

    /// Returns true if the object can be deleted.
    /// Objects cannot be deleted if they have legal hold or unexpired retention.
    #[must_use]
    pub fn can_delete(&self, bypass_governance: bool) -> bool {
        // Legal hold always blocks deletion
        if self.legal_hold {
            return false;
        }

        // Check retention
        match &self.retention {
            None => true,
            Some(retention) => {
                // Expired retention allows deletion
                if Utc::now() >= retention.retain_until_date {
                    return true;
                }
                // Governance mode can be bypassed with special permission
                if bypass_governance && retention.mode == RetentionMode::Governance {
                    return true;
                }
                // Compliance mode cannot be bypassed
                false
            }
        }
    }
}

/// A checksum value for an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checksum {
    /// CRC32 checksum value.
    Crc32(u32),
    /// CRC32C checksum value.
    Crc32C(u32),
    /// SHA-1 checksum value (20 bytes).
    Sha1([u8; 20]),
    /// SHA-256 checksum value (32 bytes).
    Sha256([u8; 32]),
}

impl Checksum {
    /// Returns the algorithm for this checksum.
    #[must_use]
    pub fn algorithm(&self) -> ChecksumAlgorithm {
        match self {
            Self::Crc32(_) => ChecksumAlgorithm::Crc32,
            Self::Crc32C(_) => ChecksumAlgorithm::Crc32C,
            Self::Sha1(_) => ChecksumAlgorithm::Sha1,
            Self::Sha256(_) => ChecksumAlgorithm::Sha256,
        }
    }

    /// Encodes the checksum as base64.
    #[must_use]
    pub fn to_base64(&self) -> String {
        use base64::Engine as _;
        match self {
            Self::Crc32(v) | Self::Crc32C(v) => {
                base64::engine::general_purpose::STANDARD.encode(v.to_be_bytes())
            }
            Self::Sha1(v) => base64::engine::general_purpose::STANDARD.encode(v),
            Self::Sha256(v) => base64::engine::general_purpose::STANDARD.encode(v),
        }
    }

    /// Decodes a checksum from base64 for the given algorithm.
    pub fn from_base64(algorithm: ChecksumAlgorithm, encoded: &str) -> Result<Self, &'static str> {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| "invalid base64")?;

        match algorithm {
            ChecksumAlgorithm::Crc32 => {
                if bytes.len() != 4 {
                    return Err("CRC32 must be 4 bytes");
                }
                let arr: [u8; 4] = bytes.try_into().expect("length validated above");
                Ok(Self::Crc32(u32::from_be_bytes(arr)))
            }
            ChecksumAlgorithm::Crc32C => {
                if bytes.len() != 4 {
                    return Err("CRC32C must be 4 bytes");
                }
                let arr: [u8; 4] = bytes.try_into().expect("length validated above");
                Ok(Self::Crc32C(u32::from_be_bytes(arr)))
            }
            ChecksumAlgorithm::Sha1 => {
                if bytes.len() != 20 {
                    return Err("SHA-1 must be 20 bytes");
                }
                let arr: [u8; 20] = bytes.try_into().expect("length validated above");
                Ok(Self::Sha1(arr))
            }
            ChecksumAlgorithm::Sha256 => {
                if bytes.len() != 32 {
                    return Err("SHA-256 must be 32 bytes");
                }
                let arr: [u8; 32] = bytes.try_into().expect("length validated above");
                Ok(Self::Sha256(arr))
            }
        }
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
    /// Cache-Control header for the final object.
    #[serde(default)]
    pub cache_control: Option<String>,
    /// Content-Disposition header for the final object.
    #[serde(default)]
    pub content_disposition: Option<String>,
    /// Content-Encoding header for the final object.
    #[serde(default)]
    pub content_encoding: Option<String>,
    /// Content-Language header for the final object.
    #[serde(default)]
    pub content_language: Option<String>,
    /// Expires header for the final object.
    #[serde(default)]
    pub expires: Option<String>,
    /// Storage class for the final object.
    #[serde(default)]
    pub storage_class: StorageClass,
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

/// CORS configuration rule for a bucket.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CorsRule {
    /// Allowed origins (e.g., "*" or specific domains).
    pub allowed_origins: Vec<String>,
    /// Allowed HTTP methods (e.g., GET, PUT, POST).
    pub allowed_methods: Vec<String>,
    /// Allowed headers.
    pub allowed_headers: Vec<String>,
    /// Headers exposed to the client.
    pub expose_headers: Vec<String>,
    /// Max age for preflight cache in seconds.
    pub max_age_seconds: Option<u32>,
    /// Optional unique ID for this rule.
    #[serde(default)]
    pub id: Option<String>,
}

/// CORS configuration for a bucket.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CorsConfiguration {
    /// List of CORS rules.
    pub rules: Vec<CorsRule>,
}

/// A single tag (key-value pair).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    /// Tag key (1-128 Unicode characters).
    pub key: String,
    /// Tag value (0-256 Unicode characters).
    pub value: String,
}

impl Tag {
    /// Creates a new tag.
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { key: key.into(), value: value.into() }
    }
}

/// A set of tags for an object or bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagSet {
    /// The tags in this set.
    pub tags: Vec<Tag>,
}

impl TagSet {
    /// Creates a new empty tag set.
    #[must_use]
    pub fn new() -> Self {
        Self { tags: Vec::new() }
    }

    /// Creates a tag set with the given tags.
    #[must_use]
    pub fn with_tags(tags: Vec<Tag>) -> Self {
        Self { tags }
    }

    /// Returns true if the tag set is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Returns the number of tags.
    #[must_use]
    pub fn len(&self) -> usize {
        self.tags.len()
    }
}

/// Protocol for website redirects.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WebsiteRedirectProtocol {
    /// HTTP protocol.
    Http,
    /// HTTPS protocol.
    Https,
}

impl WebsiteRedirectProtocol {
    /// Returns the protocol as a string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Http => "http",
            Self::Https => "https",
        }
    }

    /// Parse from string.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "http" => Some(Self::Http),
            "https" => Some(Self::Https),
            _ => None,
        }
    }
}

/// Index document configuration for website hosting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDocument {
    /// The suffix appended to requests for a directory (e.g., "index.html").
    pub suffix: String,
}

/// Error document configuration for website hosting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorDocument {
    /// The object key to return when an error occurs.
    pub key: String,
}

/// Configuration to redirect all requests to another host.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RedirectAllRequestsTo {
    /// The host name to redirect to.
    pub host_name: String,
    /// The protocol to use for the redirect (http or https).
    #[serde(default)]
    pub protocol: Option<WebsiteRedirectProtocol>,
}

/// Condition for a routing rule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingRuleCondition {
    /// Redirect only if the object key starts with this prefix.
    #[serde(default)]
    pub key_prefix_equals: Option<String>,
    /// Redirect only if the HTTP error code matches.
    #[serde(default)]
    pub http_error_code_returned_equals: Option<String>,
}

/// Redirect action for a routing rule.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingRuleRedirect {
    /// The host name to redirect to.
    #[serde(default)]
    pub host_name: Option<String>,
    /// The HTTP redirect code (e.g., 301, 302).
    #[serde(default)]
    pub http_redirect_code: Option<String>,
    /// The protocol to use for the redirect.
    #[serde(default)]
    pub protocol: Option<WebsiteRedirectProtocol>,
    /// Replace the key prefix with this value.
    #[serde(default)]
    pub replace_key_prefix_with: Option<String>,
    /// Replace the entire key with this value.
    #[serde(default)]
    pub replace_key_with: Option<String>,
}

/// A routing rule for website hosting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoutingRule {
    /// The condition that must be met for the redirect to apply.
    #[serde(default)]
    pub condition: Option<RoutingRuleCondition>,
    /// The redirect action to take.
    pub redirect: RoutingRuleRedirect,
}

/// Website configuration for a bucket.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebsiteConfiguration {
    /// The index document configuration.
    #[serde(default)]
    pub index_document: Option<IndexDocument>,
    /// The error document configuration.
    #[serde(default)]
    pub error_document: Option<ErrorDocument>,
    /// Redirect all requests to another host.
    #[serde(default)]
    pub redirect_all_requests_to: Option<RedirectAllRequestsTo>,
    /// Routing rules for conditional redirects.
    #[serde(default)]
    pub routing_rules: Vec<RoutingRule>,
}

// =============================================================================
// Bucket Logging Types
// =============================================================================

/// Permission for a logging grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoggingPermission {
    /// Full control over log files.
    FullControl,
    /// Read access to log files.
    Read,
    /// Write access to log files.
    Write,
}

impl LoggingPermission {
    /// Returns the permission as a string.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::FullControl => "FULL_CONTROL",
            Self::Read => "READ",
            Self::Write => "WRITE",
        }
    }

    /// Parse from string representation.
    #[must_use]
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "FULL_CONTROL" => Some(Self::FullControl),
            "READ" => Some(Self::Read),
            "WRITE" => Some(Self::Write),
            _ => None,
        }
    }
}

/// Type of grantee for logging grants.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LoggingGrantee {
    /// Canonical user identified by ID.
    CanonicalUser {
        /// The canonical user ID.
        id: String,
        /// Optional display name.
        #[serde(default)]
        display_name: Option<String>,
    },
    /// Email address of the user.
    AmazonCustomerByEmail {
        /// The email address.
        email_address: String,
    },
    /// A predefined group.
    Group {
        /// The group URI.
        uri: String,
    },
}

/// A grant for access to log files.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggingGrant {
    /// The grantee.
    pub grantee: LoggingGrantee,
    /// The permission granted.
    pub permission: LoggingPermission,
}

/// Logging configuration when logging is enabled.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoggingEnabled {
    /// The target bucket where logs are stored.
    pub target_bucket: String,
    /// The prefix for log object keys.
    #[serde(default)]
    pub target_prefix: String,
    /// Optional grants for log file access.
    #[serde(default)]
    pub target_grants: Vec<LoggingGrant>,
}

/// Bucket logging configuration.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketLoggingStatus {
    /// Logging configuration when enabled. None means logging is disabled.
    #[serde(default)]
    pub logging_enabled: Option<LoggingEnabled>,
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

    #[test]
    fn test_tag_new() {
        let tag = Tag::new("env", "production");
        assert_eq!(tag.key, "env");
        assert_eq!(tag.value, "production");
    }

    #[test]
    fn test_tagset_new() {
        let tagset = TagSet::new();
        assert!(tagset.is_empty());
        assert_eq!(tagset.len(), 0);
    }

    #[test]
    fn test_tagset_with_tags() {
        let tagset =
            TagSet::with_tags(vec![Tag::new("env", "test"), Tag::new("project", "rucket")]);
        assert!(!tagset.is_empty());
        assert_eq!(tagset.len(), 2);
        assert_eq!(tagset.tags[0].key, "env");
        assert_eq!(tagset.tags[1].key, "project");
    }

    // --- Migration tests for backward compatibility ---
    // These tests verify that old metadata (without new forward-compatible fields)
    // can be deserialized correctly with appropriate defaults.

    #[test]
    fn test_bucket_info_backward_compat() {
        // Old BucketInfo format (without encryption_config and lock_config)
        let old_json = r#"{
            "name": "test-bucket",
            "created_at": "2024-01-01T00:00:00Z",
            "versioning_status": null
        }"#;

        let bucket: BucketInfo = serde_json::from_str(old_json).unwrap();
        assert_eq!(bucket.name, "test-bucket");
        assert!(bucket.versioning_status.is_none());
        // New fields should have defaults
        assert!(bucket.encryption_config.is_none());
        assert!(bucket.lock_config.is_none());
    }

    #[test]
    fn test_object_metadata_backward_compat() {
        // Old ObjectMetadata format (without forward-compatible fields)
        let old_json = r#"{
            "key": "test/key.txt",
            "uuid": "00000000-0000-0000-0000-000000000000",
            "size": 1024,
            "etag": "\"abc123\"",
            "content_type": "text/plain",
            "last_modified": "2024-01-01T00:00:00Z"
        }"#;

        let meta: ObjectMetadata = serde_json::from_str(old_json).unwrap();
        assert_eq!(meta.key, "test/key.txt");
        assert_eq!(meta.size, 1024);
        // New fields should have defaults
        assert_eq!(meta.hlc_timestamp, 0);
        assert_eq!(meta.placement_group, 0);
        assert_eq!(meta.home_region, "local");
        assert_eq!(meta.storage_class, StorageClass::Standard);
        assert!(meta.replication_status.is_none());
    }

    #[test]
    fn test_storage_class_default() {
        assert_eq!(StorageClass::default(), StorageClass::Standard);
    }

    #[test]
    fn test_storage_class_parsing() {
        assert_eq!(StorageClass::parse("STANDARD"), Some(StorageClass::Standard));
        assert_eq!(StorageClass::parse("STANDARD_IA"), Some(StorageClass::StandardIa));
        assert_eq!(StorageClass::parse("GLACIER"), Some(StorageClass::Glacier));
        assert_eq!(StorageClass::parse("unknown"), None);
    }

    #[test]
    fn test_replication_status_default() {
        assert_eq!(ReplicationStatus::default(), ReplicationStatus::None);
    }

    #[test]
    fn test_encryption_config_default() {
        let config = EncryptionConfig::default();
        assert_eq!(config.algorithm, EncryptionAlgorithm::Aes256);
        assert!(config.kms_key_id.is_none());
    }

    #[test]
    fn test_object_lock_config_default() {
        let config = ObjectLockConfig::default();
        assert!(config.enabled);
        assert!(config.default_retention.is_none());
    }
}
