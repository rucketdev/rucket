// Copyright 2026 Rucket Dev
// SPDX-License-Identifier: Apache-2.0

//! Storage metrics collection.
//!
//! This module provides storage-level metrics for monitoring:
//! - Total number of buckets
//! - Total number of objects
//! - Total storage bytes used
//! - Active multipart uploads

use std::sync::Arc;

use metrics::{describe_gauge, gauge};
use tokio::time::{interval, Duration};

use crate::backend::StorageBackend;
use crate::local::LocalStorage;

/// Initialize storage metric descriptions (call once at startup).
pub fn init_storage_metrics() {
    describe_gauge!("rucket_buckets_total", "Total number of buckets");
    describe_gauge!("rucket_objects_total", "Total number of objects");
    describe_gauge!(
        "rucket_storage_bytes_total",
        "Total storage used in bytes"
    );
    describe_gauge!(
        "rucket_multipart_uploads_active",
        "Number of active multipart uploads"
    );
}

/// Start a background task to periodically collect storage metrics.
///
/// This task runs indefinitely and collects storage metrics at the specified interval.
/// The metrics are exposed via the Prometheus endpoint.
///
/// # Arguments
///
/// * `storage` - The storage backend to collect metrics from.
/// * `interval_secs` - The interval in seconds between metric collections.
///
/// # Returns
///
/// A `JoinHandle` for the background task.
pub fn start_storage_metrics_collector(
    storage: Arc<LocalStorage>,
    interval_secs: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = interval(Duration::from_secs(interval_secs));

        // Wait for the first tick immediately
        ticker.tick().await;

        loop {
            if let Err(e) = collect_storage_metrics(&storage).await {
                tracing::warn!(error = %e, "Failed to collect storage metrics");
            }
            ticker.tick().await;
        }
    })
}

async fn collect_storage_metrics(
    storage: &LocalStorage,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Count buckets
    let buckets = storage.list_buckets().await?;
    gauge!("rucket_buckets_total").set(buckets.len() as f64);

    let mut total_objects = 0u64;
    let mut total_bytes = 0u64;
    let mut total_uploads = 0u64;

    for bucket in &buckets {
        // Count objects (paginated)
        let mut token = None;
        loop {
            let result =
                storage.list_objects(&bucket.name, None, None, token.as_deref(), 1000).await?;

            total_objects += result.objects.len() as u64;
            total_bytes += result.objects.iter().map(|o| o.size).sum::<u64>();

            if result.is_truncated {
                token = result.next_continuation_token;
            } else {
                break;
            }
        }

        // Count multipart uploads
        let uploads = storage.list_multipart_uploads(&bucket.name).await?;
        total_uploads += uploads.len() as u64;
    }

    gauge!("rucket_objects_total").set(total_objects as f64);
    gauge!("rucket_storage_bytes_total").set(total_bytes as f64);
    gauge!("rucket_multipart_uploads_active").set(total_uploads as f64);

    tracing::debug!(
        buckets = buckets.len(),
        objects = total_objects,
        bytes = total_bytes,
        uploads = total_uploads,
        "Storage metrics collected"
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_init_storage_metrics() {
        // Just ensure it doesn't panic
        init_storage_metrics();
    }
}
