use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::time::Duration;
use tokio::runtime::Runtime;

use rlightning::storage::engine::{StorageConfig, StorageEngine};

fn bench_string_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Storage - String Operations");

    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("SET operation", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig::default();
            StorageEngine::new(config)
        });
        b.iter(|| {
            rt.block_on(async {
                storage
                    .set(
                        black_box(b"test_key".to_vec()),
                        black_box(b"test_value".to_vec()),
                        None,
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("GET operation", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig::default();
            let s = StorageEngine::new(config);
            s.set(b"bench_key".to_vec(), b"bench_value".to_vec(), None)
                .await
                .unwrap();
            s
        });

        b.iter(|| {
            rt.block_on(async {
                let _ = storage.get(black_box(b"bench_key")).await.unwrap();
            })
        })
    });

    group.bench_function("SET with TTL", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig::default();
            StorageEngine::new(config)
        });
        b.iter(|| {
            rt.block_on(async {
                storage
                    .set(
                        black_box(b"ttl_key".to_vec()),
                        black_box(b"ttl_value".to_vec()),
                        Some(Duration::from_secs(10)),
                    )
                    .await
                    .unwrap();
            })
        })
    });

    group.bench_function("DELETE operation", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig::default();
            StorageEngine::new(config)
        });
        let counter = std::sync::atomic::AtomicU64::new(0);
        b.iter(|| {
            let i = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            rt.block_on(async {
                let key = format!("del_key_{}", i);
                storage
                    .set(key.as_bytes().to_vec(), b"value".to_vec(), None)
                    .await
                    .unwrap();
                storage.del(black_box(key.as_bytes())).await.unwrap();
            })
        });
    });

    group.bench_function("EXISTS operation", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig::default();
            let s = StorageEngine::new(config);
            s.set(b"exists_key".to_vec(), b"value".to_vec(), None)
                .await
                .unwrap();
            s
        });

        b.iter(|| {
            rt.block_on(async {
                let _ = storage.exists(black_box(b"exists_key")).await.unwrap();
            })
        })
    });

    group.finish();
}

fn bench_concurrency_performance(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Storage - Concurrency Performance");

    group.sample_size(10);
    group.measurement_time(Duration::from_secs(15));

    group.bench_function("Concurrent SET operations", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig::default();
            StorageEngine::new(config)
        });
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();

                // Spawn 20 concurrent SET operations
                for i in 0..20 {
                    let storage_ref = storage.clone();
                    let handle = tokio::spawn(async move {
                        let key = format!("concurrent_key_{}", i);
                        let value = format!("concurrent_value_{}", i);
                        storage_ref
                            .set(key.into_bytes(), value.into_bytes(), None)
                            .await
                            .unwrap();
                    });
                    handles.push(handle);
                }

                // Wait for all operations to complete
                for handle in handles {
                    handle.await.unwrap();
                }
            })
        });
    });

    group.bench_function("Concurrent GET operations", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig::default();
            let s = StorageEngine::new(config);
            for i in 0..20 {
                let key = format!("get_key_{}", i);
                let value = format!("get_value_{}", i);
                s.set(key.into_bytes(), value.into_bytes(), None)
                    .await
                    .unwrap();
            }
            s
        });

        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();

                // Spawn 20 concurrent GET operations
                for i in 0..20 {
                    let storage_ref = storage.clone();
                    let handle = tokio::spawn(async move {
                        let key = format!("get_key_{}", i);
                        storage_ref.get(key.as_bytes()).await.unwrap();
                    });
                    handles.push(handle);
                }

                // Wait for all operations to complete
                for handle in handles {
                    handle.await.unwrap();
                }
            })
        });
    });

    group.finish();
}

fn bench_memory_management(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Storage - Memory Management");

    group.sample_size(20);
    group.measurement_time(Duration::from_secs(10));

    group.bench_function("Memory usage tracking", |b| {
        let storage = rt.block_on(async {
            let config = StorageConfig {
                max_memory: 100 * 1024 * 1024, // 100MB
                ..Default::default()
            };
            StorageEngine::new(config)
        });
        let counter = std::sync::atomic::AtomicU64::new(0);
        b.iter(|| {
            let batch = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            rt.block_on(async {
                // Add several items and check memory usage
                for i in 0..50 {
                    let key = format!("mem_key_{}_{}", batch, i);
                    let value = vec![b'x'; 1000]; // 1KB values
                    let _ = storage.set(key.into_bytes(), value, None).await;
                }

                // Check that data was stored
                black_box(
                    storage
                        .exists(format!("mem_key_{}_25", batch).as_bytes())
                        .await
                        .unwrap(),
                );
            })
        });
    });

    group.finish();
}

criterion_group!(string_ops, bench_string_operations);

criterion_group!(concurrency_tests, bench_concurrency_performance);

criterion_group!(memory_tests, bench_memory_management);

criterion_main!(string_ops, concurrency_tests, memory_tests);
