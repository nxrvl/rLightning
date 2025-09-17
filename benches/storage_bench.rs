use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::time::Duration;
use tokio::runtime::Runtime;

use rlightning::storage::engine::{StorageConfig, StorageEngine};

fn bench_string_operations(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("Storage - String Operations");
    
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(10));
    
    group.bench_function("SET operation", |b| {
        b.iter(|| {
            rt.block_on(async {
                let config = StorageConfig::default();
                let storage = StorageEngine::new(config);
                
                storage.set(
                    black_box(b"test_key".to_vec()),
                    black_box(b"test_value".to_vec()),
                    None,
                ).await.unwrap();
            })
        })
    });
    
    group.bench_function("GET operation", |b| {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        
        rt.block_on(async {
            storage.set(b"bench_key".to_vec(), b"bench_value".to_vec(), None).await.unwrap();
        });
        
        b.iter(|| {
            rt.block_on(async {
                let _ = storage.get(black_box(b"bench_key")).await.unwrap();
            })
        })
    });
    
    group.bench_function("SET with TTL", |b| {
        b.iter(|| {
            rt.block_on(async {
                let config = StorageConfig::default();
                let storage = StorageEngine::new(config);
                
                storage.set(
                    black_box(b"ttl_key".to_vec()),
                    black_box(b"ttl_value".to_vec()),
                    Some(Duration::from_secs(10)),
                ).await.unwrap();
            })
        })
    });
    
    group.bench_function("DELETE operation", |b| {
        b.iter_with_setup(
            || {
                rt.block_on(async {
                    let config = StorageConfig::default();
                    let storage = StorageEngine::new(config);
                    let key = format!("del_key_{}", fastrand::u32(..1000000));
                    storage.set(key.as_bytes().to_vec(), b"value".to_vec(), None).await.unwrap();
                    (storage, key)
                })
            },
            |(storage, key)| {
                rt.block_on(async {
                    storage.del(black_box(key.as_bytes())).await.unwrap();
                })
            }
        );
    });
    
    group.bench_function("EXISTS operation", |b| {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        
        rt.block_on(async {
            storage.set(b"exists_key".to_vec(), b"value".to_vec(), None).await.unwrap();
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
        b.iter(|| {
            rt.block_on(async {
                let config = StorageConfig::default();
                let storage = StorageEngine::new(config);
                
                let mut handles = Vec::new();
                
                // Spawn 20 concurrent SET operations
                for i in 0..20 {
                    let storage_ref = storage.clone();
                    let handle = tokio::spawn(async move {
                        let key = format!("concurrent_key_{}", i);
                        let value = format!("concurrent_value_{}", i);
                        storage_ref.set(key.into_bytes(), value.into_bytes(), None).await.unwrap();
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
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        
        // Pre-populate data
        rt.block_on(async {
            for i in 0..20 {
                let key = format!("get_key_{}", i);
                let value = format!("get_value_{}", i);
                storage.set(key.into_bytes(), value.into_bytes(), None).await.unwrap();
            }
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
        b.iter(|| {
            rt.block_on(async {
                let config = StorageConfig {
                    max_memory: 10 * 1024 * 1024, // 10MB
                    ..Default::default()
                };
                let storage = StorageEngine::new(config);
                
                // Add several items and check memory usage
                for i in 0..50 {
                    let key = format!("mem_key_{}", i);
                    let value = vec![b'x'; 1000]; // 1KB values
                    storage.set(key.into_bytes(), value, None).await.unwrap();
                }
                
                // Check that data was stored
                black_box(storage.exists(b"mem_key_25").await.unwrap());
            })
        });
    });
    
    group.finish();
}

criterion_group!(
    string_ops,
    bench_string_operations
);

criterion_group!(
    concurrency_tests,
    bench_concurrency_performance
);

criterion_group!(
    memory_tests,
    bench_memory_management
);

criterion_main!(
    string_ops,
    concurrency_tests,
    memory_tests
);