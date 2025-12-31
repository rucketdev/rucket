//! Direct I/O utilities for cache-bypassing file operations.
//!
//! This module provides functions to read and write files while bypassing the OS page cache.
//! This is useful for:
//! - Benchmarking to measure true disk I/O performance
//! - Predictable latency (no page cache eviction spikes)
//! - Large sequential writes where caching provides little benefit
//!
//! # Alignment Requirements
//!
//! Direct I/O requires proper alignment:
//! - Buffer address must be aligned to 4096 bytes
//! - Read/write size must be a multiple of 512 bytes (4096 for best performance)
//! - File offset must be aligned to 512 bytes

#![allow(unsafe_code)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

/// Open a file with direct I/O (bypasses OS page cache).
///
/// - On Linux: Uses `O_DIRECT` flag
/// - On macOS: Uses `F_NOCACHE` via fcntl
#[allow(clippy::needless_return)]
pub fn open_direct(path: &Path) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        return std::fs::OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(path);
    }

    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let file = File::open(path)?;
        // Disable caching with F_NOCACHE
        unsafe {
            libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
        }
        Ok(file)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        // Fallback: regular file open (no cache bypass)
        File::open(path)
    }
}

/// Read entire file with direct I/O.
///
/// Note: On Linux with O_DIRECT, buffer must be aligned to 512/4096 bytes.
#[allow(clippy::needless_return)]
pub fn read_direct(path: &Path) -> std::io::Result<Vec<u8>> {
    let size = std::fs::metadata(path)?.len() as usize;

    #[cfg(target_os = "linux")]
    {
        // O_DIRECT requires aligned reads. For small files (< 4096 bytes),
        // fall back to regular I/O to avoid EINVAL errors.
        if size < 4096 {
            let mut file = File::open(path)?;
            let mut buffer = vec![0u8; size];
            file.read_exact(&mut buffer)?;
            return Ok(buffer);
        }

        // O_DIRECT requires aligned buffer and aligned read size
        let aligned_size = (size + 4095) & !4095;
        let mut buffer = aligned_buffer(aligned_size);
        let mut file = open_direct(path)?;
        file.read_exact(&mut buffer[..size])?;
        buffer.truncate(size);
        return Ok(buffer);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut file = open_direct(path)?;
        let mut buffer = vec![0u8; size];
        file.read_exact(&mut buffer)?;
        Ok(buffer)
    }
}

/// Create an aligned buffer for O_DIRECT on Linux.
#[cfg(target_os = "linux")]
fn aligned_buffer(size: usize) -> Vec<u8> {
    use std::alloc::{alloc_zeroed, Layout};

    if size == 0 {
        return Vec::new();
    }

    unsafe {
        let layout = Layout::from_size_align(size, 4096).expect("Invalid layout");
        let ptr = alloc_zeroed(layout);
        if ptr.is_null() {
            panic!("Failed to allocate aligned buffer");
        }
        Vec::from_raw_parts(ptr, size, size)
    }
}

/// Create a file for writing with direct I/O (bypasses OS page cache).
///
/// - On Linux: Uses `O_DIRECT` flag
/// - On macOS: Uses `F_NOCACHE` via fcntl
#[allow(clippy::needless_return)]
pub fn create_direct(path: &Path) -> std::io::Result<File> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::fs::OpenOptionsExt;
        return std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DIRECT)
            .open(path);
    }

    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let file =
            std::fs::OpenOptions::new().write(true).create(true).truncate(true).open(path)?;
        // Disable caching with F_NOCACHE
        unsafe {
            libc::fcntl(file.as_raw_fd(), libc::F_NOCACHE, 1);
        }
        Ok(file)
    }

    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        // Fallback: regular file create (no cache bypass)
        File::create(path)
    }
}

/// Write data to a file using direct I/O.
///
/// On Linux, this requires:
/// - Data size to be a multiple of 512 bytes (padded if needed)
/// - Buffer to be aligned to 4096 bytes
///
/// For small files (< 4096 bytes), falls back to regular I/O.
///
/// Returns the actual bytes written (before padding).
#[allow(clippy::needless_return)]
pub fn write_direct(path: &Path, data: &[u8]) -> std::io::Result<usize> {
    let size = data.len();

    #[cfg(target_os = "linux")]
    {
        // O_DIRECT requires aligned writes. For small files (< 4096 bytes),
        // fall back to regular I/O to avoid complexity.
        if size < 4096 {
            let mut file = File::create(path)?;
            file.write_all(data)?;
            file.sync_data()?;
            return Ok(size);
        }

        // Align size to 4096 bytes for O_DIRECT
        let aligned_size = (size + 4095) & !4095;
        let mut buffer = aligned_buffer(aligned_size);
        buffer[..size].copy_from_slice(data);

        let mut file = create_direct(path)?;
        file.write_all(&buffer)?;
        file.sync_data()?;

        // Truncate to actual size (removes padding)
        file.set_len(size as u64)?;

        return Ok(size);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let mut file = create_direct(path)?;
        file.write_all(data)?;
        file.sync_data()?;
        Ok(size)
    }
}

/// Minimum file size for direct I/O to be beneficial.
/// Below this size, regular I/O is faster due to alignment overhead.
pub const DIRECT_IO_MIN_SIZE: u64 = 4096;

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::TempDir;

    use super::*;

    #[test]
    fn test_read_direct() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        // Write test data
        let data = b"Hello, World!";
        let mut file = File::create(&path).unwrap();
        file.write_all(data).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Read with direct I/O
        let result = read_direct(&path).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_read_direct_large() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("large.dat");

        // Write 1MB of data
        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let mut file = File::create(&path).unwrap();
        file.write_all(&data).unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Read with direct I/O
        let result = read_direct(&path).unwrap();
        assert_eq!(result.len(), data.len());
        assert_eq!(result, data);
    }

    #[test]
    fn test_write_direct_small() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("small.dat");

        // Write small data (falls back to regular I/O)
        let data = b"Hello, World!";
        let written = write_direct(&path, data).unwrap();
        assert_eq!(written, data.len());

        // Verify content
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content, data);
    }

    #[test]
    fn test_write_direct_large() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("large.dat");

        // Write 1MB of data (uses direct I/O on Linux)
        let data: Vec<u8> = (0..1024 * 1024).map(|i| (i % 256) as u8).collect();
        let written = write_direct(&path, &data).unwrap();
        assert_eq!(written, data.len());

        // Verify content
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content.len(), data.len());
        assert_eq!(content, data);
    }

    #[test]
    fn test_write_direct_unaligned_size() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("unaligned.dat");

        // Write data with unaligned size (not a multiple of 4096)
        let data: Vec<u8> = (0..10_000).map(|i| (i % 256) as u8).collect();
        let written = write_direct(&path, &data).unwrap();
        assert_eq!(written, data.len());

        // Verify content - should be exactly the original size
        let content = std::fs::read(&path).unwrap();
        assert_eq!(content.len(), data.len());
        assert_eq!(content, data);
    }
}
