//! Streaming I/O utilities.

use std::path::Path;

use crc32fast::Hasher as Crc32Hasher;
use md5::{Digest, Md5};
use rucket_core::types::{Checksum, ChecksumAlgorithm, ETag};
use rucket_core::{Result, SyncStrategy};
use sha1::Sha1;
use sha2::Sha256;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Result of a write operation, containing both ETag and CRC32C checksum.
#[derive(Debug, Clone)]
pub struct WriteResult {
    /// The ETag (MD5 hash) of the written data.
    pub etag: ETag,
    /// CRC32C checksum for data integrity verification.
    pub crc32c: u32,
}

/// Write data to a file and compute its MD5 hash (ETag).
///
/// This function always syncs the file to disk for maximum durability.
/// For configurable sync behavior, use [`crate::sync::write_and_hash_with_strategy`].
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub async fn write_and_hash(path: &Path, data: &[u8]) -> Result<ETag> {
    let result = write_and_hash_with_sync(path, data, SyncStrategy::Always).await?;
    Ok(result.etag)
}

/// Write data to a file and compute both MD5 hash (ETag) and CRC32C checksum.
///
/// Returns a `WriteResult` containing both the ETag and CRC32C checksum.
/// The CRC32C uses hardware acceleration (SSE4.2/ARM CRC) when available.
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub async fn write_and_hash_with_sync(
    path: &Path,
    data: &[u8],
    strategy: SyncStrategy,
) -> Result<WriteResult> {
    // Compute MD5 hash and CRC32C in parallel (single pass over data)
    let mut md5_hasher = Md5::new();
    let mut crc_hasher = Crc32Hasher::new();

    md5_hasher.update(data);
    crc_hasher.update(data);

    let md5_hash: [u8; 16] = md5_hasher.finalize().into();
    let crc32c = crc_hasher.finalize();

    // Write file
    let mut file = File::create(path).await?;
    file.write_all(data).await?;

    // Sync based on strategy
    match strategy {
        SyncStrategy::Always => {
            // Use sync_data() (fdatasync) - faster than sync_all() as it skips metadata
            file.sync_data().await?;
            #[cfg(test)]
            crate::sync::test_stats::record_data_sync();
        }
        SyncStrategy::None | SyncStrategy::Periodic | SyncStrategy::Threshold => {
            // Flush buffer but don't fsync
            file.flush().await?;
        }
    }

    Ok(WriteResult { etag: ETag::from_md5(&md5_hash), crc32c })
}

/// Compute MD5 hash of data without writing.
#[must_use]
pub fn compute_md5(data: &[u8]) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute CRC32C checksum of data.
///
/// Uses hardware acceleration (SSE4.2/ARM CRC) when available.
#[must_use]
pub fn compute_crc32c(data: &[u8]) -> u32 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Verify data integrity using CRC32C checksum.
///
/// Returns `true` if the checksum matches, `false` otherwise.
#[must_use]
pub fn verify_crc32c(data: &[u8], expected: u32) -> bool {
    compute_crc32c(data) == expected
}

/// Compute CRC32 checksum of data (IEEE polynomial).
#[must_use]
pub fn compute_crc32(data: &[u8]) -> u32 {
    let mut hasher = Crc32Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// Compute CRC32C checksum of data (Castagnoli polynomial).
///
/// Uses hardware acceleration (SSE4.2/ARM CRC) when available.
#[must_use]
pub fn compute_crc32c_castagnoli(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Compute SHA-1 hash of data.
#[must_use]
pub fn compute_sha1(data: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute SHA-256 hash of data.
#[must_use]
pub fn compute_sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

/// Compute a checksum for the given algorithm.
#[must_use]
pub fn compute_checksum(data: &[u8], algorithm: ChecksumAlgorithm) -> Checksum {
    match algorithm {
        ChecksumAlgorithm::Crc32 => Checksum::Crc32(compute_crc32(data)),
        ChecksumAlgorithm::Crc32C => Checksum::Crc32C(compute_crc32c_castagnoli(data)),
        ChecksumAlgorithm::Sha1 => Checksum::Sha1(compute_sha1(data)),
        ChecksumAlgorithm::Sha256 => Checksum::Sha256(compute_sha256(data)),
    }
}

/// Compute ETag for multipart upload from part ETags.
#[must_use]
pub fn compute_multipart_etag(part_etags: &[ETag]) -> ETag {
    let mut hasher = Md5::new();

    for etag in part_etags {
        // Extract hex string from ETag (remove quotes and any suffix)
        let etag_str = etag.as_str().trim_matches('"');
        let hex_part = etag_str.split('-').next().unwrap_or(etag_str);

        if let Ok(bytes) = hex::decode(hex_part) {
            hasher.update(&bytes);
        }
    }

    let hash: [u8; 16] = hasher.finalize().into();
    ETag::from_multipart(&hash, part_etags.len())
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_write_and_hash() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        let data = b"Hello, World!";
        let etag = write_and_hash(&path, data).await.unwrap();

        // Verify file was written
        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(content, data);

        // Verify ETag format
        assert!(!etag.is_multipart());
        assert!(etag.as_str().starts_with('"'));
        assert!(etag.as_str().ends_with('"'));
    }

    #[tokio::test]
    async fn test_write_and_hash_with_crc32c() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        let data = b"Hello, World!";
        let result = write_and_hash_with_sync(&path, data, SyncStrategy::Always).await.unwrap();

        // Verify file was written
        let content = tokio::fs::read(&path).await.unwrap();
        assert_eq!(content, data);

        // Verify ETag format
        assert!(!result.etag.is_multipart());

        // Verify CRC32C can be used to verify integrity
        assert!(verify_crc32c(&content, result.crc32c));
    }

    #[test]
    fn test_compute_md5() {
        let data = b"Hello, World!";
        let hash = compute_md5(data);

        // Known MD5 hash for "Hello, World!"
        let expected = "65a8e27d8879283831b664bd8b7f0ad4";
        assert_eq!(hex::encode(hash), expected);
    }

    #[test]
    fn test_compute_crc32c() {
        let data = b"Hello, World!";
        let crc = compute_crc32c(data);

        // CRC32 should be non-zero for non-empty data
        assert_ne!(crc, 0);

        // Verify deterministic
        assert_eq!(crc, compute_crc32c(data));

        // Verify changes with different data
        assert_ne!(crc, compute_crc32c(b"Hello, World"));
    }

    #[test]
    fn test_verify_crc32c() {
        let data = b"Hello, World!";
        let crc = compute_crc32c(data);

        assert!(verify_crc32c(data, crc));
        assert!(!verify_crc32c(data, crc + 1)); // Wrong checksum
        assert!(!verify_crc32c(b"Hello, World", crc)); // Different data
    }

    #[test]
    fn test_compute_multipart_etag() {
        let part1 = ETag::new("\"d41d8cd98f00b204e9800998ecf8427e\"");
        let part2 = ETag::new("\"098f6bcd4621d373cade4e832627b4f6\"");

        let etag = compute_multipart_etag(&[part1, part2]);

        assert!(etag.is_multipart());
        assert!(etag.as_str().ends_with("-2\""));
    }
}
