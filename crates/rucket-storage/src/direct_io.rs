// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Direct I/O utilities for cache-bypassing file operations.
//!
//! This module provides functions to read files while bypassing the OS page cache,
//! which is useful for benchmarking to measure true disk I/O performance.

#![allow(unsafe_code)]

use std::fs::File;
use std::io::Read;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

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
}
