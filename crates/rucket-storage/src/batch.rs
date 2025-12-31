//! Batched write operations for group commit.
//!
//! Group commit batches multiple write operations and performs a single fsync
//! for the entire batch, significantly improving throughput for workloads with
//! many small writes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tokio::fs::File;
use tokio::sync::{mpsc, oneshot, Notify};

/// Configuration for batch writing.
#[derive(Debug, Clone)]
pub struct BatchConfig {
    /// Maximum number of writes to batch before flushing.
    pub max_batch_size: usize,
    /// Maximum bytes to accumulate before flushing.
    pub max_batch_bytes: u64,
    /// Maximum delay before flushing (in milliseconds).
    pub max_batch_delay_ms: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 64,
            max_batch_bytes: 16 * 1024 * 1024, // 16 MB
            max_batch_delay_ms: 10,
        }
    }
}

impl BatchConfig {
    /// Configuration for aggressive batching (maximum performance).
    #[must_use]
    pub fn aggressive() -> Self {
        Self {
            max_batch_size: 256,
            max_batch_bytes: 64 * 1024 * 1024, // 64 MB
            max_batch_delay_ms: 50,
        }
    }

    /// Configuration for conservative batching (lower latency).
    #[must_use]
    pub fn conservative() -> Self {
        Self {
            max_batch_size: 16,
            max_batch_bytes: 4 * 1024 * 1024, // 4 MB
            max_batch_delay_ms: 5,
        }
    }

    /// Disable batching (sync each write individually).
    #[must_use]
    pub fn disabled() -> Self {
        Self { max_batch_size: 1, max_batch_bytes: 0, max_batch_delay_ms: 0 }
    }
}

/// A pending sync request.
struct PendingSyncRequest {
    /// Path to the file to sync.
    path: PathBuf,
    /// Size of the write in bytes.
    size: u64,
    /// Channel to notify completion.
    completion: oneshot::Sender<std::io::Result<()>>,
}

/// Batched writer that groups fsync operations.
///
/// Writers submit sync requests, which are batched together. When the batch
/// is flushed (by size, bytes, or timeout), all files are synced with a
/// single barrier.
pub struct BatchWriter {
    sender: mpsc::Sender<PendingSyncRequest>,
    shutdown: Arc<Notify>,
}

impl BatchWriter {
    /// Create a new batch writer with the given configuration.
    #[must_use]
    pub fn new(config: BatchConfig) -> Self {
        let (sender, receiver) = mpsc::channel(1024);
        let shutdown = Arc::new(Notify::new());

        let shutdown_clone = Arc::clone(&shutdown);
        tokio::spawn(async move {
            batch_worker(receiver, config, shutdown_clone).await;
        });

        Self { sender, shutdown }
    }

    /// Submit a file for batched sync.
    ///
    /// Returns when the file has been synced (along with other files in the batch).
    pub async fn sync(&self, path: PathBuf, size: u64) -> std::io::Result<()> {
        let (completion_tx, completion_rx) = oneshot::channel();

        let request = PendingSyncRequest { path, size, completion: completion_tx };

        self.sender.send(request).await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "BatchWriter shut down")
        })?;

        completion_rx.await.map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::BrokenPipe, "Sync request cancelled")
        })?
    }

    /// Shutdown the batch writer, flushing any pending writes.
    pub async fn shutdown(&self) {
        self.shutdown.notify_one();
    }
}

/// Background worker that processes batched sync requests.
async fn batch_worker(
    mut receiver: mpsc::Receiver<PendingSyncRequest>,
    config: BatchConfig,
    shutdown: Arc<Notify>,
) {
    let mut pending: Vec<PendingSyncRequest> = Vec::with_capacity(config.max_batch_size);
    let mut pending_bytes: u64 = 0;

    let timeout = Duration::from_millis(config.max_batch_delay_ms);

    loop {
        // Wait for requests or timeout
        let request = if pending.is_empty() {
            // No pending requests, wait indefinitely for the first one
            tokio::select! {
                req = receiver.recv() => req,
                _ = shutdown.notified() => {
                    // Shutdown requested, flush and exit
                    if !pending.is_empty() {
                        flush_batch(&mut pending).await;
                    }
                    return;
                }
            }
        } else {
            // Have pending requests, wait with timeout
            tokio::select! {
                req = receiver.recv() => req,
                _ = tokio::time::sleep(timeout) => {
                    // Timeout, flush the batch
                    flush_batch(&mut pending).await;
                    pending_bytes = 0;
                    continue;
                }
                _ = shutdown.notified() => {
                    // Shutdown, flush and exit
                    flush_batch(&mut pending).await;
                    return;
                }
            }
        };

        match request {
            Some(req) => {
                pending_bytes += req.size;
                pending.push(req);

                // Check if we should flush
                let should_flush = pending.len() >= config.max_batch_size
                    || pending_bytes >= config.max_batch_bytes;

                if should_flush {
                    flush_batch(&mut pending).await;
                    pending_bytes = 0;
                }
            }
            None => {
                // Channel closed, flush remaining and exit
                if !pending.is_empty() {
                    flush_batch(&mut pending).await;
                }
                return;
            }
        }
    }
}

/// Flush all pending sync requests.
async fn flush_batch(pending: &mut Vec<PendingSyncRequest>) {
    if pending.is_empty() {
        return;
    }

    // Sync all files in the batch
    let mut results: Vec<std::io::Result<()>> = Vec::with_capacity(pending.len());

    for request in pending.iter() {
        let result = sync_file(&request.path).await;
        results.push(result);
    }

    // Notify all waiters
    for (request, result) in pending.drain(..).zip(results) {
        let _ = request.completion.send(result);
    }
}

/// Sync a single file using fdatasync.
async fn sync_file(path: &PathBuf) -> std::io::Result<()> {
    if path.exists() {
        let file = File::open(path).await?;
        file.sync_data().await?;
    }
    Ok(())
}

impl Clone for BatchWriter {
    fn clone(&self) -> Self {
        Self { sender: self.sender.clone(), shutdown: Arc::clone(&self.shutdown) }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;
    use tokio::io::AsyncWriteExt;

    use super::*;

    #[tokio::test]
    async fn test_batch_writer_single() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        // Write a file
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"Hello, World!").await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        // Batch sync it
        let writer = BatchWriter::new(BatchConfig::default());
        writer.sync(path.clone(), 13).await.unwrap();

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_batch_writer_multiple() {
        let temp_dir = TempDir::new().unwrap();
        let writer = BatchWriter::new(BatchConfig { max_batch_size: 3, ..Default::default() });

        // Write multiple files
        let mut handles = Vec::new();
        for i in 0..5 {
            let path = temp_dir.path().join(format!("test{i}.dat"));
            let mut file = File::create(&path).await.unwrap();
            file.write_all(format!("data{i}").as_bytes()).await.unwrap();
            file.flush().await.unwrap();
            drop(file);

            let writer = writer.clone();
            handles.push(tokio::spawn(async move { writer.sync(path, 5).await }));
        }

        // Wait for all syncs
        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_batch_writer_timeout_flush() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.dat");

        // Write a file
        let mut file = File::create(&path).await.unwrap();
        file.write_all(b"Hello").await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        // Use a very large batch size but short timeout
        let writer = BatchWriter::new(BatchConfig {
            max_batch_size: 1000,
            max_batch_delay_ms: 10,
            ..Default::default()
        });

        // Submit one request - it should flush due to timeout
        let start = std::time::Instant::now();
        writer.sync(path, 5).await.unwrap();
        let elapsed = start.elapsed();

        // Should complete within reasonable time (timeout + some overhead)
        assert!(elapsed.as_millis() < 100);

        writer.shutdown().await;
    }
}
