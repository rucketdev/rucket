// Copyright 2024 The Rucket Authors
// SPDX-License-Identifier: Apache-2.0

//! Storage throughput benchmarks.

#![allow(missing_docs)]

use std::sync::Arc;

use bytes::Bytes;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use rucket_core::{SyncConfig, SyncStrategy};
use rucket_storage::{LocalStorage, StorageBackend};
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Benchmark fixture holding storage instance and temp directory.
struct BenchFixture {
    storage: Arc<LocalStorage>,
    _temp_dir: TempDir,
}

impl BenchFixture {
    /// Create a new benchmark fixture with in-memory metadata and default sync.
    fn new(rt: &Runtime) -> Self {
        Self::with_sync_config(rt, SyncConfig::default())
    }

    /// Create a benchmark fixture with custom sync configuration.
    fn with_sync_config(rt: &Runtime, sync_config: SyncConfig) -> Self {
        rt.block_on(async {
            let temp_dir = TempDir::new().expect("Failed to create temp dir");
            let data_dir = temp_dir.path().join("data");
            let tmp_dir = temp_dir.path().join("tmp");

            let storage = LocalStorage::new_in_memory_with_sync(data_dir, tmp_dir, sync_config)
                .await
                .expect("Failed to create storage");

            storage.create_bucket("bench").await.expect("Failed to create bucket");

            Self { storage: Arc::new(storage), _temp_dir: temp_dir }
        })
    }

    /// Create a fixture that never syncs (maximum performance).
    #[allow(dead_code)]
    fn never(rt: &Runtime) -> Self {
        Self::with_sync_config(rt, SyncConfig::never())
    }

    /// Create a fixture with periodic sync.
    #[allow(dead_code)]
    fn periodic(rt: &Runtime) -> Self {
        Self::with_sync_config(rt, SyncConfig::periodic())
    }

    /// Create a fixture that always syncs (maximum durability).
    #[allow(dead_code)]
    fn always(rt: &Runtime) -> Self {
        Self::with_sync_config(rt, SyncConfig::always())
    }
}

/// Generate test data of specified size.
fn generate_data(size: usize) -> Bytes {
    let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
    Bytes::from(data)
}

/// Benchmark PUT object operations with various sizes.
fn bench_put_object(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    let sizes: [(usize, &str); 3] = [(1024, "1KB"), (64 * 1024, "64KB"), (1024 * 1024, "1MB")];

    let mut group = c.benchmark_group("put_object");

    for (size, name) in sizes {
        let data = generate_data(size);
        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("size", name), &data, |b, data| {
            let storage = fixture.storage.clone();
            let mut counter = 0u64;

            b.iter(|| {
                let key = format!("bench-put-{counter}");
                counter += 1;
                rt.block_on(async {
                    storage
                        .put_object("bench", &key, data.clone(), Some("application/octet-stream"))
                        .await
                        .expect("put_object failed");
                });
            });
        });
    }

    group.finish();
}

/// Benchmark GET object operations with various sizes.
fn bench_get_object(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    let sizes: [(usize, &str); 3] = [(1024, "1KB"), (64 * 1024, "64KB"), (1024 * 1024, "1MB")];

    let mut group = c.benchmark_group("get_object");

    for (size, name) in sizes {
        let data = generate_data(size);
        let key = format!("bench-get-{name}");

        // Pre-populate the object
        rt.block_on(async {
            fixture
                .storage
                .put_object("bench", &key, data, Some("application/octet-stream"))
                .await
                .expect("Failed to put object");
        });

        group.throughput(Throughput::Bytes(size as u64));

        group.bench_with_input(BenchmarkId::new("size", name), &key, |b, key| {
            let storage = fixture.storage.clone();

            b.iter(|| {
                rt.block_on(async {
                    let (_meta, _data) =
                        storage.get_object("bench", key).await.expect("get_object failed");
                });
            });
        });
    }

    group.finish();
}

/// Benchmark range GET operations.
fn bench_get_object_range(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    // Create a 1MB object for range requests
    let data = generate_data(1024 * 1024);
    let key = "bench-range-object";

    rt.block_on(async {
        fixture
            .storage
            .put_object("bench", key, data, Some("application/octet-stream"))
            .await
            .expect("Failed to put object");
    });

    let mut group = c.benchmark_group("get_object_range");

    // Benchmark 1KB range reads from different positions
    let range_size = 1024u64;
    group.throughput(Throughput::Bytes(range_size));

    group.bench_function("1KB_range", |b| {
        let storage = fixture.storage.clone();

        b.iter(|| {
            rt.block_on(async {
                let (_meta, _data) = storage
                    .get_object_range("bench", key, 0, range_size - 1)
                    .await
                    .expect("get_object_range failed");
            });
        });
    });

    group.finish();
}

/// Benchmark HEAD object operations (metadata lookup).
fn bench_head_object(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    // Create an object to head
    let data = generate_data(1024);
    let key = "bench-head-object";

    rt.block_on(async {
        fixture
            .storage
            .put_object("bench", key, data, Some("application/octet-stream"))
            .await
            .expect("Failed to put object");
    });

    c.bench_function("head_object", |b| {
        let storage = fixture.storage.clone();

        b.iter(|| {
            rt.block_on(async {
                let _meta = storage.head_object("bench", key).await.expect("head_object failed");
            });
        });
    });
}

/// Benchmark DELETE object operations.
fn bench_delete_object(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    let data = generate_data(1024);

    c.bench_function("delete_object", |b| {
        let storage = fixture.storage.clone();
        let mut counter = 0u64;

        b.iter(|| {
            let key = format!("bench-delete-{counter}");
            counter += 1;

            rt.block_on(async {
                // Create object
                storage
                    .put_object("bench", &key, data.clone(), None)
                    .await
                    .expect("put_object failed");

                // Delete it
                storage.delete_object("bench", &key).await.expect("delete_object failed");
            });
        });
    });
}

/// Benchmark COPY object operations.
fn bench_copy_object(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    // Create source object (64KB)
    let data = generate_data(64 * 1024);
    let src_key = "bench-copy-source";

    rt.block_on(async {
        fixture
            .storage
            .put_object("bench", src_key, data, Some("application/octet-stream"))
            .await
            .expect("Failed to put source object");
    });

    let mut group = c.benchmark_group("copy_object");
    group.throughput(Throughput::Bytes(64 * 1024));

    group.bench_function("64KB", |b| {
        let storage = fixture.storage.clone();
        let mut counter = 0u64;

        b.iter(|| {
            let dst_key = format!("bench-copy-dest-{counter}");
            counter += 1;

            rt.block_on(async {
                storage
                    .copy_object("bench", src_key, "bench", &dst_key)
                    .await
                    .expect("copy_object failed");
            });
        });
    });

    group.finish();
}

/// Benchmark LIST objects operations with varying object counts.
fn bench_list_objects(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    let counts: [usize; 3] = [10, 100, 1000];

    // Pre-populate objects for largest count
    let max_count = *counts.iter().max().unwrap();
    let data = generate_data(64); // Small objects

    rt.block_on(async {
        for i in 0..max_count {
            let key = format!("list-obj-{i:05}");
            fixture
                .storage
                .put_object("bench", &key, data.clone(), None)
                .await
                .expect("Failed to put object");
        }
    });

    let mut group = c.benchmark_group("list_objects");

    for count in counts {
        group.bench_with_input(BenchmarkId::new("count", count), &count, |b, &count| {
            let storage = fixture.storage.clone();

            b.iter(|| {
                rt.block_on(async {
                    let result = storage
                        .list_objects("bench", None, None, None, count as u32)
                        .await
                        .expect("list_objects failed");
                    assert!(result.objects.len() <= count);
                });
            });
        });
    }

    group.finish();
}

/// Benchmark LIST objects with prefix filter.
fn bench_list_objects_with_prefix(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let fixture = BenchFixture::new(&rt);

    // Create objects with different prefixes
    let data = generate_data(64);
    let prefixes = ["alpha/", "beta/", "gamma/"];

    rt.block_on(async {
        for prefix in prefixes {
            for i in 0..100 {
                let key = format!("{prefix}obj-{i:03}");
                fixture
                    .storage
                    .put_object("bench", &key, data.clone(), None)
                    .await
                    .expect("Failed to put object");
            }
        }
    });

    c.bench_function("list_objects_with_prefix", |b| {
        let storage = fixture.storage.clone();

        b.iter(|| {
            rt.block_on(async {
                let result = storage
                    .list_objects("bench", Some("alpha/"), None, None, 1000)
                    .await
                    .expect("list_objects failed");
                assert_eq!(result.objects.len(), 100);
            });
        });
    });
}

/// Benchmark PUT operations with different sync strategies.
/// This helps compare performance vs durability trade-offs.
fn bench_sync_strategies(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let strategies = [
        ("none", SyncStrategy::None),
        ("periodic", SyncStrategy::Periodic),
        ("threshold", SyncStrategy::Threshold),
        ("always", SyncStrategy::Always),
    ];

    let data = generate_data(64 * 1024); // 64KB objects
    let mut group = c.benchmark_group("sync_strategy_comparison");
    group.throughput(Throughput::Bytes(64 * 1024));

    for (name, strategy) in strategies {
        let config = SyncConfig {
            data: strategy,
            metadata: SyncStrategy::Always, // Keep metadata consistent
            ..Default::default()
        };
        let fixture = BenchFixture::with_sync_config(&rt, config);

        group.bench_with_input(BenchmarkId::new("put_64KB", name), &data, |b, data| {
            let storage = fixture.storage.clone();
            let mut counter = 0u64;

            b.iter(|| {
                let key = format!("sync-bench-{counter}");
                counter += 1;
                rt.block_on(async {
                    storage
                        .put_object("bench", &key, data.clone(), None)
                        .await
                        .expect("put_object failed");
                });
            });
        });
    }

    group.finish();
}

/// Sync profile definition for comprehensive benchmarking.
struct SyncProfile {
    name: &'static str,
    data: SyncStrategy,
    metadata: SyncStrategy,
}

/// Predefined sync profiles representing common use cases.
const SYNC_PROFILES: &[SyncProfile] = &[
    SyncProfile { name: "never", data: SyncStrategy::None, metadata: SyncStrategy::Periodic },
    SyncProfile { name: "periodic", data: SyncStrategy::Periodic, metadata: SyncStrategy::Always },
    SyncProfile { name: "always", data: SyncStrategy::Always, metadata: SyncStrategy::Always },
];

/// Object sizes for profile matrix benchmarks.
const PROFILE_SIZES: &[(usize, &str)] = &[(1024, "1KB"), (64 * 1024, "64KB"), (1024 * 1024, "1MB")];

/// Benchmark PUT operations across all profile × size combinations.
/// This creates a matrix of 3 profiles × 3 sizes = 9 benchmark variants.
fn bench_profile_matrix_put(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("profile_put");

    for profile in SYNC_PROFILES {
        let config =
            SyncConfig { data: profile.data, metadata: profile.metadata, ..Default::default() };
        let fixture = BenchFixture::with_sync_config(&rt, config);

        for &(size, size_name) in PROFILE_SIZES {
            let data = generate_data(size);
            group.throughput(Throughput::Bytes(size as u64));

            let bench_id = format!("{}/{}", profile.name, size_name);
            group.bench_with_input(BenchmarkId::new("throughput", &bench_id), &data, |b, data| {
                let storage = fixture.storage.clone();
                let mut counter = 0u64;

                b.iter(|| {
                    let key = format!("profile-put-{counter}");
                    counter += 1;
                    rt.block_on(async {
                        storage
                            .put_object("bench", &key, data.clone(), None)
                            .await
                            .expect("put_object failed");
                    });
                });
            });
        }
    }

    group.finish();
}

/// Benchmark GET operations across all profile × size combinations.
/// This creates a matrix of 3 profiles × 3 sizes = 9 benchmark variants.
fn bench_profile_matrix_get(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("profile_get");

    for profile in SYNC_PROFILES {
        let config =
            SyncConfig { data: profile.data, metadata: profile.metadata, ..Default::default() };
        let fixture = BenchFixture::with_sync_config(&rt, config);

        for &(size, size_name) in PROFILE_SIZES {
            let data = generate_data(size);
            let key = format!("profile-get-{}-{}", profile.name, size_name);

            // Pre-populate the object
            rt.block_on(async {
                fixture
                    .storage
                    .put_object("bench", &key, data, None)
                    .await
                    .expect("Failed to put object");
            });

            group.throughput(Throughput::Bytes(size as u64));

            let bench_id = format!("{}/{}", profile.name, size_name);
            group.bench_with_input(BenchmarkId::new("throughput", &bench_id), &key, |b, key| {
                let storage = fixture.storage.clone();

                b.iter(|| {
                    rt.block_on(async {
                        // Use direct I/O to bypass OS page cache for accurate measurements
                        let (_meta, _data) = storage
                            .get_object_direct("bench", key)
                            .await
                            .expect("get_object failed");
                    });
                });
            });
        }
    }

    group.finish();
}

criterion_group!(
    benches,
    // Run GET benchmarks first (clean system state, no prior fsync activity)
    bench_profile_matrix_get,
    bench_get_object,
    bench_get_object_range,
    bench_head_object,
    // Then PUT benchmarks (fsync activity won't affect prior GET results)
    bench_profile_matrix_put,
    bench_put_object,
    bench_delete_object,
    bench_copy_object,
    bench_list_objects,
    bench_list_objects_with_prefix,
    bench_sync_strategies,
);

criterion_main!(benches);
