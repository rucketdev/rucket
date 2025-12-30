// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Streaming I/O utilities.

use std::path::Path;

use md5::{Digest, Md5};
use rucket_core::types::ETag;
use rucket_core::{Result, SyncStrategy};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;

/// Write data to a file and compute its MD5 hash (ETag).
///
/// This function always syncs the file to disk for maximum durability.
/// For configurable sync behavior, use [`crate::sync::write_and_hash_with_strategy`].
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub async fn write_and_hash(path: &Path, data: &[u8]) -> Result<ETag> {
    write_and_hash_with_sync(path, data, SyncStrategy::Always).await
}

/// Write data to a file and compute its MD5 hash (ETag) with configurable sync.
///
/// # Errors
///
/// Returns an error if the file cannot be written.
pub async fn write_and_hash_with_sync(
    path: &Path,
    data: &[u8],
    strategy: SyncStrategy,
) -> Result<ETag> {
    // Compute MD5 hash
    let mut hasher = Md5::new();
    hasher.update(data);
    let hash: [u8; 16] = hasher.finalize().into();

    // Write file
    let mut file = File::create(path).await?;
    file.write_all(data).await?;

    // Sync based on strategy
    match strategy {
        SyncStrategy::Always => {
            file.sync_all().await?;
        }
        SyncStrategy::None | SyncStrategy::Periodic | SyncStrategy::Threshold => {
            // Flush buffer but don't fsync
            file.flush().await?;
        }
    }

    Ok(ETag::from_md5(&hash))
}

/// Compute MD5 hash of data without writing.
#[must_use]
pub fn compute_md5(data: &[u8]) -> [u8; 16] {
    let mut hasher = Md5::new();
    hasher.update(data);
    hasher.finalize().into()
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

    #[test]
    fn test_compute_md5() {
        let data = b"Hello, World!";
        let hash = compute_md5(data);

        // Known MD5 hash for "Hello, World!"
        let expected = "65a8e27d8879283831b664bd8b7f0ad4";
        assert_eq!(hex::encode(hash), expected);
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
