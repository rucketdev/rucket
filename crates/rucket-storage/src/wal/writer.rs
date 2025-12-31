//! Append-only WAL writer.
//!
//! The writer appends entries to a log file with optional syncing.
//! It supports rotation and checkpointing for log management.

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use super::entry::WalEntry;

/// WAL file format version.
const WAL_VERSION: u32 = 1;

/// Magic bytes for WAL file header.
const WAL_MAGIC: &[u8; 4] = b"RWAL";

/// Sync mode for WAL writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WalSyncMode {
    /// No explicit sync - rely on OS.
    None,
    /// Use fdatasync after each write.
    #[default]
    Fdatasync,
    /// Use full fsync after each write.
    Fsync,
}

/// Configuration for the WAL writer.
#[derive(Debug, Clone)]
pub struct WalWriterConfig {
    /// Directory for WAL files.
    pub wal_dir: PathBuf,
    /// Sync mode for durability.
    pub sync_mode: WalSyncMode,
    /// Buffer size for writes (default: 64KB).
    pub buffer_size: usize,
}

impl Default for WalWriterConfig {
    fn default() -> Self {
        Self {
            wal_dir: PathBuf::from("wal"),
            sync_mode: WalSyncMode::Fdatasync,
            buffer_size: 64 * 1024,
        }
    }
}

/// The WAL writer appends entries to a log file.
///
/// Thread-safe: uses internal locking for concurrent access.
pub struct WalWriter {
    /// Path to current WAL file.
    current_path: PathBuf,
    /// Current file handle (behind mutex for sync writes).
    file: Arc<Mutex<BufWriter<std::fs::File>>>,
    /// Next sequence number.
    sequence: AtomicU64,
    /// Sync mode.
    sync_mode: WalSyncMode,
    /// Bytes written since last checkpoint.
    bytes_since_checkpoint: AtomicU64,
    /// Entries written since last checkpoint.
    entries_since_checkpoint: AtomicU64,
}

impl WalWriter {
    /// Open or create a WAL file.
    ///
    /// If the file exists, the sequence number is recovered from the last entry.
    pub fn open(config: &WalWriterConfig) -> std::io::Result<Self> {
        std::fs::create_dir_all(&config.wal_dir)?;

        let current_path = config.wal_dir.join("current.wal");
        let (file, sequence) = if current_path.exists() {
            // Recover sequence from existing file
            let seq = Self::recover_sequence(&current_path)?;
            let file = std::fs::OpenOptions::new().append(true).open(&current_path)?;
            (file, seq)
        } else {
            // Create new file with header
            let mut file = std::fs::File::create(&current_path)?;
            Self::write_header(&mut file)?;
            (file, 0)
        };

        let writer = BufWriter::with_capacity(config.buffer_size, file);

        Ok(Self {
            current_path,
            file: Arc::new(Mutex::new(writer)),
            sequence: AtomicU64::new(sequence),
            sync_mode: config.sync_mode,
            bytes_since_checkpoint: AtomicU64::new(0),
            entries_since_checkpoint: AtomicU64::new(0),
        })
    }

    /// Write WAL file header.
    fn write_header(file: &mut std::fs::File) -> std::io::Result<()> {
        file.write_all(WAL_MAGIC)?;
        file.write_all(&WAL_VERSION.to_le_bytes())?;
        Ok(())
    }

    /// Recover the last sequence number from an existing WAL file.
    fn recover_sequence(path: &Path) -> std::io::Result<u64> {
        use std::io::{BufReader, Read};

        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);

        // Read and verify header
        let mut magic = [0u8; 4];
        reader.read_exact(&mut magic)?;
        if &magic != WAL_MAGIC {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid WAL magic"));
        }

        let mut version_bytes = [0u8; 4];
        reader.read_exact(&mut version_bytes)?;
        let version = u32::from_le_bytes(version_bytes);
        if version != WAL_VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported WAL version: {version}"),
            ));
        }

        // Read all entries to find the last sequence number
        let mut last_seq = 0u64;
        loop {
            // Read sequence number (8 bytes)
            let mut seq_bytes = [0u8; 8];
            match reader.read_exact(&mut seq_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let seq = u64::from_le_bytes(seq_bytes);

            // Read entry length (4 bytes)
            let mut len_bytes = [0u8; 4];
            match reader.read_exact(&mut len_bytes) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_bytes) as usize;

            // Skip the entry data
            let mut skip_buf = vec![0u8; len];
            match reader.read_exact(&mut skip_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }

            last_seq = seq;
        }

        Ok(last_seq)
    }

    /// Append an entry to the WAL.
    ///
    /// Returns the sequence number assigned to this entry.
    pub async fn append(&self, entry: WalEntry) -> std::io::Result<u64> {
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;

        // Serialize the entry
        let data = bincode::serialize(&entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let entry_size = 8 + 4 + data.len(); // seq + len + data

        // Write to file
        {
            let mut file = self.file.lock().await;

            // Write sequence number
            file.write_all(&seq.to_le_bytes())?;

            // Write length
            file.write_all(&(data.len() as u32).to_le_bytes())?;

            // Write entry data
            file.write_all(&data)?;
        }

        // Track bytes written
        self.bytes_since_checkpoint.fetch_add(entry_size as u64, Ordering::Relaxed);
        self.entries_since_checkpoint.fetch_add(1, Ordering::Relaxed);

        Ok(seq)
    }

    /// Force sync the WAL to disk.
    pub async fn sync(&self) -> std::io::Result<()> {
        let mut file = self.file.lock().await;
        file.flush()?;

        let inner = file.get_ref();
        match self.sync_mode {
            WalSyncMode::None => Ok(()),
            WalSyncMode::Fdatasync => inner.sync_data(),
            WalSyncMode::Fsync => inner.sync_all(),
        }
    }

    /// Append an entry and immediately sync.
    pub async fn append_sync(&self, entry: WalEntry) -> std::io::Result<u64> {
        let seq = self.append(entry).await?;
        self.sync().await?;
        Ok(seq)
    }

    /// Get the current sequence number.
    #[must_use]
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }

    /// Get bytes written since last checkpoint.
    #[must_use]
    pub fn bytes_since_checkpoint(&self) -> u64 {
        self.bytes_since_checkpoint.load(Ordering::Relaxed)
    }

    /// Get entries written since last checkpoint.
    #[must_use]
    pub fn entries_since_checkpoint(&self) -> u64 {
        self.entries_since_checkpoint.load(Ordering::Relaxed)
    }

    /// Path to the current WAL file.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.current_path
    }

    /// Rotate the WAL file.
    ///
    /// Renames the current file and starts a new one.
    /// Returns the path to the old (rotated) file.
    pub async fn rotate(&self) -> std::io::Result<PathBuf> {
        // Sync before rotation
        self.sync().await?;

        let wal_dir = self.current_path.parent().unwrap();
        let timestamp = chrono::Utc::now().timestamp_millis();
        let old_path = wal_dir.join(format!("wal-{timestamp}.old"));

        // Rename current file
        std::fs::rename(&self.current_path, &old_path)?;

        // Create new file
        let mut new_file = std::fs::File::create(&self.current_path)?;
        Self::write_header(&mut new_file)?;

        // Replace the file handle
        {
            let mut file = self.file.lock().await;
            *file = BufWriter::new(new_file);
        }

        // Reset counters
        self.bytes_since_checkpoint.store(0, Ordering::Relaxed);
        self.entries_since_checkpoint.store(0, Ordering::Relaxed);

        Ok(old_path)
    }

    /// Write a checkpoint entry and optionally rotate.
    ///
    /// Returns the old WAL path if rotation was performed.
    pub async fn checkpoint(&self, rotate: bool) -> std::io::Result<Option<PathBuf>> {
        let seq = self.sequence.load(Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();

        // Write checkpoint entry
        self.append_sync(WalEntry::Checkpoint { sequence: seq, timestamp }).await?;

        if rotate {
            let old_path = self.rotate().await?;
            Ok(Some(old_path))
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;

    #[tokio::test]
    async fn test_wal_writer_basic() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let writer = WalWriter::open(&config).unwrap();

        // Write some entries
        let seq1 = writer
            .append(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key1".to_string(),
                uuid: Uuid::new_v4(),
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();

        let seq2 = writer
            .append(WalEntry::PutCommit {
                bucket: "bucket".to_string(),
                key: "key1".to_string(),
                uuid: Uuid::new_v4(),
            })
            .await
            .unwrap();

        assert_eq!(seq1, 1);
        assert_eq!(seq2, 2);
        assert_eq!(writer.sequence(), 2);
    }

    #[tokio::test]
    async fn test_wal_writer_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        // Write some entries
        {
            let writer = WalWriter::open(&config).unwrap();
            writer
                .append(WalEntry::PutIntent {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid: Uuid::new_v4(),
                    size: 100,
                    crc32c: 0,
                    timestamp: 0,
                })
                .await
                .unwrap();
            writer
                .append(WalEntry::PutCommit {
                    bucket: "bucket".to_string(),
                    key: "key1".to_string(),
                    uuid: Uuid::new_v4(),
                })
                .await
                .unwrap();
            writer.sync().await.unwrap();
        }

        // Reopen and verify sequence recovery
        let writer = WalWriter::open(&config).unwrap();
        assert_eq!(writer.sequence(), 2);

        // New entries should continue from 3
        let seq = writer
            .append(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key2".to_string(),
                uuid: Uuid::new_v4(),
                size: 200,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();
        assert_eq!(seq, 3);
    }

    #[tokio::test]
    async fn test_wal_rotation() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalWriterConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            sync_mode: WalSyncMode::None,
            ..Default::default()
        };

        let writer = WalWriter::open(&config).unwrap();

        // Write an entry
        writer
            .append(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key1".to_string(),
                uuid: Uuid::new_v4(),
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();

        // Rotate
        let old_path = writer.rotate().await.unwrap();
        assert!(old_path.exists());

        // Counters should be reset
        assert_eq!(writer.bytes_since_checkpoint(), 0);
        assert_eq!(writer.entries_since_checkpoint(), 0);

        // But sequence should continue
        let seq = writer
            .append(WalEntry::PutIntent {
                bucket: "bucket".to_string(),
                key: "key2".to_string(),
                uuid: Uuid::new_v4(),
                size: 100,
                crc32c: 0,
                timestamp: 0,
            })
            .await
            .unwrap();
        assert_eq!(seq, 2);
    }
}
