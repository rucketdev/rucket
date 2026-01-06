//! Server-side encryption (SSE-S3) implementation.
//!
//! This module provides AES-256-GCM encryption for object data at rest,
//! compatible with S3's SSE-S3 (Server-Side Encryption with S3-managed keys).
//!
//! # Architecture
//!
//! - **Master Key**: A 32-byte key configured at server startup
//! - **Data Encryption Key (DEK)**: Derived per-object using HKDF-SHA256
//! - **Encryption**: AES-256-GCM with a random 12-byte nonce per object
//!
//! # Security Properties
//!
//! - Each object has a unique DEK derived from (master_key, object_uuid)
//! - Random nonces ensure ciphertext differs even for identical plaintexts
//! - GCM provides authenticated encryption (confidentiality + integrity)

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use bytes::Bytes;
use hkdf::Hkdf;
use md5::{Digest, Md5};
use rand::RngCore;
use sha2::Sha256;
use thiserror::Error;
use uuid::Uuid;
use zeroize::Zeroize;

/// AES-256-GCM nonce size (96 bits = 12 bytes).
const NONCE_SIZE: usize = 12;

/// AES-256 key size (256 bits = 32 bytes).
pub const KEY_SIZE: usize = 32;

/// Encryption errors.
#[derive(Debug, Error)]
pub enum CryptoError {
    /// Encryption failed.
    #[error("encryption failed: {0}")]
    EncryptionFailed(String),

    /// Decryption failed (likely tampered data or wrong key).
    #[error("decryption failed: data may be corrupted or tampered")]
    DecryptionFailed,

    /// Invalid master key.
    #[error("invalid master key: must be exactly 32 bytes")]
    InvalidMasterKey,

    /// Invalid nonce.
    #[error("invalid nonce: must be exactly 12 bytes")]
    InvalidNonce,

    /// Invalid customer key for SSE-C.
    #[error("invalid customer key: must be exactly 32 bytes")]
    InvalidCustomerKey,

    /// MD5 mismatch for SSE-C key.
    #[error("SSE-C key MD5 mismatch")]
    KeyMd5Mismatch,

    /// Wrong key provided for decryption.
    #[error("access denied: wrong encryption key")]
    WrongKey,
}

/// Result type for crypto operations.
pub type CryptoResult<T> = Result<T, CryptoError>;

/// Encryption metadata stored alongside encrypted objects.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncryptionMetadata {
    /// Encryption algorithm used.
    pub algorithm: EncryptionAlgorithm,
    /// Random nonce used for this object (base64 encoded for storage).
    pub nonce: Vec<u8>,
}

/// Supported encryption algorithms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (default for SSE-S3).
    #[serde(rename = "AES256")]
    Aes256Gcm,
}

impl EncryptionAlgorithm {
    /// Returns the S3 header value for this algorithm.
    #[must_use]
    pub fn as_s3_header(&self) -> &'static str {
        match self {
            Self::Aes256Gcm => "AES256",
        }
    }
}

/// SSE-S3 encryption provider.
///
/// Manages encryption/decryption using a master key with per-object
/// key derivation via HKDF-SHA256.
///
/// The master key is securely zeroed from memory when this provider is dropped.
#[derive(Clone)]
pub struct SseS3Provider {
    master_key: [u8; KEY_SIZE],
}

/// Securely zero the master key when the provider is dropped.
impl Drop for SseS3Provider {
    fn drop(&mut self) {
        self.master_key.zeroize();
    }
}

impl SseS3Provider {
    /// Creates a new SSE-S3 provider with the given master key.
    ///
    /// # Arguments
    ///
    /// * `master_key` - A 32-byte master key for encryption
    ///
    /// # Errors
    ///
    /// Returns an error if the master key is not exactly 32 bytes.
    pub fn new(master_key: &[u8]) -> CryptoResult<Self> {
        if master_key.len() != KEY_SIZE {
            return Err(CryptoError::InvalidMasterKey);
        }

        let mut key = [0u8; KEY_SIZE];
        key.copy_from_slice(master_key);

        Ok(Self { master_key: key })
    }

    /// Creates a provider from a hex-encoded master key string.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is not a valid 64-character hex string.
    pub fn from_hex(hex_key: &str) -> CryptoResult<Self> {
        let key_bytes = hex::decode(hex_key).map_err(|_| CryptoError::InvalidMasterKey)?;
        Self::new(&key_bytes)
    }

    /// Derives a data encryption key (DEK) for a specific object.
    ///
    /// Uses HKDF-SHA256 with the object UUID as info to derive a unique
    /// 256-bit key for each object.
    ///
    /// # Errors
    ///
    /// Returns an error if key derivation fails (should never happen with valid parameters).
    fn derive_object_key(&self, object_uuid: Uuid) -> CryptoResult<[u8; KEY_SIZE]> {
        let hk = Hkdf::<Sha256>::new(None, &self.master_key);
        let info = object_uuid.as_bytes();

        let mut okm = [0u8; KEY_SIZE];
        hk.expand(info, &mut okm)
            .map_err(|_| CryptoError::EncryptionFailed("HKDF key derivation failed".to_string()))?;

        Ok(okm)
    }

    /// Generates a random nonce for encryption.
    fn generate_nonce() -> [u8; NONCE_SIZE] {
        let mut nonce = [0u8; NONCE_SIZE];
        rand::thread_rng().fill_bytes(&mut nonce);
        nonce
    }

    /// Derives a deterministic nonce from a UUID.
    ///
    /// Used for multipart upload parts where we can't store the nonce separately.
    /// Safety: Since each part has a unique UUID and its own derived key,
    /// using a deterministic nonce per UUID is safe (no key+nonce reuse).
    fn derive_nonce_from_uuid(uuid: Uuid) -> [u8; NONCE_SIZE] {
        // Use first 12 bytes of UUID as nonce
        // UUIDs are 16 bytes, we need 12 for AES-GCM nonce
        let uuid_bytes = uuid.as_bytes();
        let mut nonce = [0u8; NONCE_SIZE];
        nonce.copy_from_slice(&uuid_bytes[..NONCE_SIZE]);
        nonce
    }

    /// Encrypts object data.
    ///
    /// # Arguments
    ///
    /// * `object_uuid` - Unique identifier for the object (used for key derivation)
    /// * `plaintext` - The data to encrypt
    ///
    /// # Returns
    ///
    /// A tuple of (ciphertext, encryption_metadata).
    ///
    /// # Errors
    ///
    /// Returns an error if encryption fails.
    pub fn encrypt(
        &self,
        object_uuid: Uuid,
        plaintext: &[u8],
    ) -> CryptoResult<(Bytes, EncryptionMetadata)> {
        let dek = self.derive_object_key(object_uuid)?;
        let nonce_bytes = Self::generate_nonce();
        let nonce = Nonce::from_slice(&nonce_bytes);

        let cipher = Aes256Gcm::new_from_slice(&dek)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        let metadata = EncryptionMetadata {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            nonce: nonce_bytes.to_vec(),
        };

        Ok((Bytes::from(ciphertext), metadata))
    }

    /// Encrypts data using a deterministic nonce derived from the UUID.
    ///
    /// Used for multipart upload parts where nonce cannot be stored separately.
    /// Safe because each part has a unique UUID and unique derived key.
    pub fn encrypt_part(&self, part_uuid: Uuid, plaintext: &[u8]) -> CryptoResult<Bytes> {
        let dek = self.derive_object_key(part_uuid)?;
        let nonce_bytes = Self::derive_nonce_from_uuid(part_uuid);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let cipher = Aes256Gcm::new_from_slice(&dek)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        Ok(Bytes::from(ciphertext))
    }

    /// Decrypts data that was encrypted with `encrypt_part`.
    pub fn decrypt_part(&self, part_uuid: Uuid, ciphertext: &[u8]) -> CryptoResult<Bytes> {
        let dek = self.derive_object_key(part_uuid)?;
        let nonce_bytes = Self::derive_nonce_from_uuid(part_uuid);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let cipher = Aes256Gcm::new_from_slice(&dek).map_err(|_| CryptoError::DecryptionFailed)?;

        let plaintext =
            cipher.decrypt(nonce, ciphertext).map_err(|_| CryptoError::DecryptionFailed)?;

        Ok(Bytes::from(plaintext))
    }

    /// Decrypts object data.
    ///
    /// # Arguments
    ///
    /// * `object_uuid` - Unique identifier for the object (used for key derivation)
    /// * `ciphertext` - The encrypted data
    /// * `metadata` - Encryption metadata containing the nonce
    ///
    /// # Returns
    ///
    /// The decrypted plaintext.
    ///
    /// # Errors
    ///
    /// Returns an error if decryption fails (wrong key, tampered data, etc.).
    pub fn decrypt(
        &self,
        object_uuid: Uuid,
        ciphertext: &[u8],
        metadata: &EncryptionMetadata,
    ) -> CryptoResult<Bytes> {
        if metadata.nonce.len() != NONCE_SIZE {
            return Err(CryptoError::InvalidNonce);
        }

        let dek = self.derive_object_key(object_uuid)?;
        let nonce = Nonce::from_slice(&metadata.nonce);

        let cipher = Aes256Gcm::new_from_slice(&dek).map_err(|_| CryptoError::DecryptionFailed)?;

        let plaintext =
            cipher.decrypt(nonce, ciphertext).map_err(|_| CryptoError::DecryptionFailed)?;

        Ok(Bytes::from(plaintext))
    }
}

// =============================================================================
// SSE-C (Customer-Provided Keys) Support
// =============================================================================

/// Parsed SSE-C headers from a request.
#[derive(Debug, Clone, PartialEq)]
pub struct SseCHeaders {
    /// The encryption algorithm (must be "AES256").
    pub algorithm: String,
    /// The 256-bit customer-provided encryption key.
    pub key: [u8; KEY_SIZE],
    /// The base64-encoded MD5 hash of the key.
    pub key_md5: String,
}

impl SseCHeaders {
    /// Parse SSE-C headers from a request.
    ///
    /// # Arguments
    ///
    /// * `algorithm` - The x-amz-server-side-encryption-customer-algorithm header (must be "AES256")
    /// * `key_base64` - The x-amz-server-side-encryption-customer-key header (base64-encoded 256-bit key)
    /// * `key_md5_base64` - The x-amz-server-side-encryption-customer-key-MD5 header (base64-encoded MD5)
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The algorithm is not "AES256"
    /// - The key is not exactly 32 bytes when decoded
    /// - The provided MD5 doesn't match the computed MD5 of the key
    pub fn parse(algorithm: &str, key_base64: &str, key_md5_base64: &str) -> CryptoResult<Self> {
        // Validate algorithm
        if algorithm != "AES256" {
            return Err(CryptoError::EncryptionFailed(format!(
                "Invalid SSE-C algorithm: {}. Must be AES256",
                algorithm
            )));
        }

        // Decode the key
        let key_bytes = base64::engine::general_purpose::STANDARD
            .decode(key_base64)
            .map_err(|_| CryptoError::InvalidCustomerKey)?;

        if key_bytes.len() != KEY_SIZE {
            return Err(CryptoError::InvalidCustomerKey);
        }

        let mut key = [0u8; KEY_SIZE];
        key.copy_from_slice(&key_bytes);

        // Compute and verify MD5
        let mut hasher = Md5::new();
        hasher.update(key);
        let computed_md5 = hasher.finalize();
        let computed_md5_base64 = base64::engine::general_purpose::STANDARD.encode(computed_md5);

        if computed_md5_base64 != key_md5_base64 {
            return Err(CryptoError::KeyMd5Mismatch);
        }

        Ok(Self { algorithm: algorithm.to_string(), key, key_md5: key_md5_base64.to_string() })
    }

    /// Compute the MD5 hash of a key and return it base64-encoded.
    #[must_use]
    pub fn compute_key_md5(key: &[u8; KEY_SIZE]) -> String {
        let mut hasher = Md5::new();
        hasher.update(key);
        let md5 = hasher.finalize();
        base64::engine::general_purpose::STANDARD.encode(md5)
    }
}

/// Securely zero the key when dropped.
impl Drop for SseCHeaders {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

/// SSE-C encryption provider.
///
/// Encrypts/decrypts data using a customer-provided 256-bit key.
/// Unlike SSE-S3, no key derivation is performed - the customer key is used directly.
pub struct SseCProvider {
    key: [u8; KEY_SIZE],
}

impl SseCProvider {
    /// Creates a new SSE-C provider from parsed headers.
    #[must_use]
    pub fn new(headers: &SseCHeaders) -> Self {
        Self { key: headers.key }
    }

    /// Creates a new SSE-C provider from a raw key.
    ///
    /// # Errors
    ///
    /// Returns an error if the key is not exactly 32 bytes.
    pub fn from_key(key: &[u8]) -> CryptoResult<Self> {
        if key.len() != KEY_SIZE {
            return Err(CryptoError::InvalidCustomerKey);
        }

        let mut key_arr = [0u8; KEY_SIZE];
        key_arr.copy_from_slice(key);

        Ok(Self { key: key_arr })
    }

    /// Generates a random nonce for encryption.
    fn generate_nonce() -> [u8; NONCE_SIZE] {
        let mut nonce = [0u8; NONCE_SIZE];
        rand::thread_rng().fill_bytes(&mut nonce);
        nonce
    }

    /// Encrypts data using the customer-provided key.
    ///
    /// # Arguments
    ///
    /// * `plaintext` - The data to encrypt
    ///
    /// # Returns
    ///
    /// A tuple of (ciphertext, nonce).
    ///
    /// # Errors
    ///
    /// Returns an error if encryption fails.
    pub fn encrypt(&self, plaintext: &[u8]) -> CryptoResult<(Bytes, Vec<u8>)> {
        let nonce_bytes = Self::generate_nonce();
        let nonce = Nonce::from_slice(&nonce_bytes);

        let cipher = Aes256Gcm::new_from_slice(&self.key)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|e| CryptoError::EncryptionFailed(e.to_string()))?;

        Ok((Bytes::from(ciphertext), nonce_bytes.to_vec()))
    }

    /// Decrypts data using the customer-provided key.
    ///
    /// # Arguments
    ///
    /// * `ciphertext` - The encrypted data
    /// * `nonce` - The nonce used during encryption
    ///
    /// # Returns
    ///
    /// The decrypted plaintext.
    ///
    /// # Errors
    ///
    /// Returns an error if decryption fails (wrong key or tampered data).
    pub fn decrypt(&self, ciphertext: &[u8], nonce: &[u8]) -> CryptoResult<Bytes> {
        if nonce.len() != NONCE_SIZE {
            return Err(CryptoError::InvalidNonce);
        }

        let nonce = Nonce::from_slice(nonce);
        let cipher = Aes256Gcm::new_from_slice(&self.key).map_err(|_| CryptoError::WrongKey)?;

        let plaintext = cipher.decrypt(nonce, ciphertext).map_err(|_| CryptoError::WrongKey)?;

        Ok(Bytes::from(plaintext))
    }
}

/// Securely zero the key when the provider is dropped.
impl Drop for SseCProvider {
    fn drop(&mut self) {
        self.key.zeroize();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_master_key() -> [u8; 32] {
        // A test master key (DO NOT use in production)
        [
            0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d,
            0x0e, 0x0f, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b,
            0x1c, 0x1d, 0x1e, 0x1f,
        ]
    }

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let provider = SseS3Provider::new(&test_master_key()).unwrap();
        let uuid = Uuid::new_v4();
        let plaintext = b"Hello, World! This is a test message.";

        let (ciphertext, metadata) = provider.encrypt(uuid, plaintext).unwrap();
        let decrypted = provider.decrypt(uuid, &ciphertext, &metadata).unwrap();

        assert_eq!(decrypted.as_ref(), plaintext);
    }

    #[test]
    fn test_different_uuids_different_ciphertexts() {
        let provider = SseS3Provider::new(&test_master_key()).unwrap();
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let plaintext = b"Same plaintext";

        let (ciphertext1, _) = provider.encrypt(uuid1, plaintext).unwrap();
        let (ciphertext2, _) = provider.encrypt(uuid2, plaintext).unwrap();

        // Different UUIDs should produce different ciphertexts
        assert_ne!(ciphertext1.as_ref(), ciphertext2.as_ref());
    }

    #[test]
    fn test_same_uuid_different_nonces() {
        let provider = SseS3Provider::new(&test_master_key()).unwrap();
        let uuid = Uuid::new_v4();
        let plaintext = b"Same plaintext";

        let (ciphertext1, _) = provider.encrypt(uuid, plaintext).unwrap();
        let (ciphertext2, _) = provider.encrypt(uuid, plaintext).unwrap();

        // Same UUID but random nonces should still produce different ciphertexts
        assert_ne!(ciphertext1.as_ref(), ciphertext2.as_ref());
    }

    #[test]
    fn test_wrong_uuid_decryption_fails() {
        let provider = SseS3Provider::new(&test_master_key()).unwrap();
        let uuid1 = Uuid::new_v4();
        let uuid2 = Uuid::new_v4();
        let plaintext = b"Secret data";

        let (ciphertext, metadata) = provider.encrypt(uuid1, plaintext).unwrap();

        // Decrypting with wrong UUID should fail
        let result = provider.decrypt(uuid2, &ciphertext, &metadata);
        assert!(result.is_err());
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let provider = SseS3Provider::new(&test_master_key()).unwrap();
        let uuid = Uuid::new_v4();
        let plaintext = b"Secret data";

        let (ciphertext, metadata) = provider.encrypt(uuid, plaintext).unwrap();

        // Tamper with ciphertext
        let mut tampered = ciphertext.to_vec();
        if !tampered.is_empty() {
            tampered[0] ^= 0xff;
        }

        let result = provider.decrypt(uuid, &tampered, &metadata);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_master_key_length() {
        let short_key = [0u8; 16];
        let result = SseS3Provider::new(&short_key);
        assert!(result.is_err());

        let long_key = [0u8; 64];
        let result = SseS3Provider::new(&long_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_hex() {
        let hex_key = "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";
        let provider = SseS3Provider::from_hex(hex_key).unwrap();

        let uuid = Uuid::new_v4();
        let plaintext = b"Test";

        let (ciphertext, metadata) = provider.encrypt(uuid, plaintext).unwrap();
        let decrypted = provider.decrypt(uuid, &ciphertext, &metadata).unwrap();

        assert_eq!(decrypted.as_ref(), plaintext);
    }

    #[test]
    fn test_empty_plaintext() {
        let provider = SseS3Provider::new(&test_master_key()).unwrap();
        let uuid = Uuid::new_v4();
        let plaintext = b"";

        let (ciphertext, metadata) = provider.encrypt(uuid, plaintext).unwrap();
        let decrypted = provider.decrypt(uuid, &ciphertext, &metadata).unwrap();

        assert_eq!(decrypted.as_ref(), plaintext);
    }

    #[test]
    fn test_large_plaintext() {
        let provider = SseS3Provider::new(&test_master_key()).unwrap();
        let uuid = Uuid::new_v4();
        let plaintext = vec![0xab; 10 * 1024 * 1024]; // 10 MB

        let (ciphertext, metadata) = provider.encrypt(uuid, &plaintext).unwrap();
        let decrypted = provider.decrypt(uuid, &ciphertext, &metadata).unwrap();

        assert_eq!(decrypted.as_ref(), plaintext.as_slice());
    }

    #[test]
    fn test_encryption_algorithm_s3_header() {
        assert_eq!(EncryptionAlgorithm::Aes256Gcm.as_s3_header(), "AES256");
    }
}
