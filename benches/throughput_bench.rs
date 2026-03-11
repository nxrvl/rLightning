use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

use rlightning::command::Command;
use rlightning::command::handler::CommandHandler;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

fn bench_string_operations_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("String Operations Throughput");

    // Configure the benchmark to use fewer samples and shorter measurement time
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for concurrency in [1, 10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("SET Operations", concurrency),
            concurrency,
            |b, &concurrency| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: 64 * 1024 * 1024, // 64MB
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            for i in 0..500 {
                                let key = format!("key{}", i).into_bytes();
                                let value = format!("value{}", i).into_bytes();

                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let set_cmd = Command {
                                        name: "set".to_string(),
                                        args: vec![key, value],
                                    };

                                    black_box(handler_ref.process(set_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_hash_operations_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("Hash Operations Throughput");

    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for concurrency in [1, 10, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("HSET Operations", concurrency),
            concurrency,
            |b, &concurrency| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: 64 * 1024 * 1024,
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            for i in 0..200 {
                                let key = format!("hash{}", i % 50).into_bytes();
                                let field = format!("field{}", i).into_bytes();
                                let value = format!("value{}", i).into_bytes();

                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let hset_cmd = Command {
                                        name: "hset".to_string(),
                                        args: vec![key, field, value],
                                    };

                                    black_box(handler_ref.process(hset_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_list_operations_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("List Operations Throughput");

    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for concurrency in [1, 10, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("LPUSH Operations", concurrency),
            concurrency,
            |b, &concurrency| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: 64 * 1024 * 1024,
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            for i in 0..200 {
                                let key = format!("list{}", i % 20).into_bytes();
                                let value = format!("item{}", i).into_bytes();

                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let lpush_cmd = Command {
                                        name: "lpush".to_string(),
                                        args: vec![key, value],
                                    };

                                    black_box(handler_ref.process(lpush_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_set_operations_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("Set Operations Throughput");

    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for concurrency in [1, 10, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("SADD Operations", concurrency),
            concurrency,
            |b, &concurrency| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: 64 * 1024 * 1024,
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            for i in 0..200 {
                                let key = format!("set{}", i % 20).into_bytes();
                                let member = format!("member{}", i).into_bytes();

                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let sadd_cmd = Command {
                                        name: "sadd".to_string(),
                                        args: vec![key, member],
                                    };

                                    black_box(handler_ref.process(sadd_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_sorted_set_operations_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("Sorted Set Operations Throughput");

    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for concurrency in [1, 10, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("ZADD Operations", concurrency),
            concurrency,
            |b, &concurrency| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: 64 * 1024 * 1024,
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            for i in 0..200 {
                                let key = format!("zset{}", i % 20).into_bytes();
                                let score = format!("{}.0", i).into_bytes();
                                let member = format!("member{}", i).into_bytes();

                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let zadd_cmd = Command {
                                        name: "zadd".to_string(),
                                        args: vec![key, score, member],
                                    };

                                    black_box(handler_ref.process(zadd_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_mixed_workload_throughput(c: &mut Criterion) {
    let mut group = c.benchmark_group("Mixed Workload Throughput");

    group.sample_size(30);
    group.measurement_time(Duration::from_secs(5));

    for concurrency in [10, 50, 100].iter() {
        group.bench_with_input(
            BenchmarkId::new("Mixed Operations", concurrency),
            concurrency,
            |b, &concurrency| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: 128 * 1024 * 1024, // 128MB for mixed workload
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            // Populate initial data
                            for i in 0..100 {
                                let key = format!("initial_key{}", i).into_bytes();
                                let value = format!("initial_value{}", i).into_bytes();

                                let set_cmd = Command {
                                    name: "set".to_string(),
                                    args: vec![key, value],
                                };

                                handler.process(set_cmd, 0).await.unwrap();
                            }

                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            // Mixed workload: 40% SET, 50% GET, 10% other operations
                            for i in 0..500 {
                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let operation_type = i % 10;
                                    let cmd = match operation_type {
                                        0..=3 => {
                                            // 40% SET operations
                                            let key = format!("key{}", i).into_bytes();
                                            let value = format!("value{}", i).into_bytes();
                                            Command {
                                                name: "set".to_string(),
                                                args: vec![key, value],
                                            }
                                        }
                                        4..=8 => {
                                            // 50% GET operations
                                            let key =
                                                format!("initial_key{}", i % 100).into_bytes();
                                            Command {
                                                name: "get".to_string(),
                                                args: vec![key],
                                            }
                                        }
                                        9 => {
                                            // 10% other operations (INCR, HSET, LPUSH, etc.)
                                            match i % 5 {
                                                0 => {
                                                    let key =
                                                        format!("counter{}", i % 10).into_bytes();
                                                    Command {
                                                        name: "incr".to_string(),
                                                        args: vec![key],
                                                    }
                                                }
                                                1 => {
                                                    let key =
                                                        format!("hash{}", i % 10).into_bytes();
                                                    let field = format!("field{}", i).into_bytes();
                                                    let value = format!("hvalue{}", i).into_bytes();
                                                    Command {
                                                        name: "hset".to_string(),
                                                        args: vec![key, field, value],
                                                    }
                                                }
                                                2 => {
                                                    let key =
                                                        format!("list{}", i % 10).into_bytes();
                                                    let value = format!("item{}", i).into_bytes();
                                                    Command {
                                                        name: "lpush".to_string(),
                                                        args: vec![key, value],
                                                    }
                                                }
                                                3 => {
                                                    let key = format!("set{}", i % 10).into_bytes();
                                                    let member =
                                                        format!("member{}", i).into_bytes();
                                                    Command {
                                                        name: "sadd".to_string(),
                                                        args: vec![key, member],
                                                    }
                                                }
                                                _ => {
                                                    let key =
                                                        format!("zset{}", i % 10).into_bytes();
                                                    let score = format!("{}.0", i).into_bytes();
                                                    let member =
                                                        format!("zmember{}", i).into_bytes();
                                                    Command {
                                                        name: "zadd".to_string(),
                                                        args: vec![key, score, member],
                                                    }
                                                }
                                            }
                                        }
                                        _ => unreachable!(),
                                    };

                                    black_box(handler_ref.process(cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_expiry_performance(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Expiry Performance");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for concurrency in [1, 10, 50].iter() {
        group.bench_with_input(
            BenchmarkId::new("EXPIRE Operations", concurrency),
            concurrency,
            |b, &concurrency| {
                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: 64 * 1024 * 1024,
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            for i in 0..200 {
                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let key = format!("expire_key{}", i).into_bytes();
                                    let value = format!("expire_value{}", i).into_bytes();
                                    let ttl = format!("{}", (i % 10) + 1).into_bytes(); // TTLs from 1-10 seconds

                                    // SET the key first
                                    let set_cmd = Command {
                                        name: "set".to_string(),
                                        args: vec![key.clone(), value],
                                    };
                                    handler_ref.process(set_cmd, 0).await.unwrap();

                                    // Then EXPIRE it
                                    let expire_cmd = Command {
                                        name: "expire".to_string(),
                                        args: vec![key, ttl],
                                    };

                                    black_box(handler_ref.process(expire_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_large_values_throughput(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Large Values Throughput");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));

    for &value_size in &[1_000, 10_000, 50_000] {
        group.bench_with_input(
            BenchmarkId::new("Large SET Operations", value_size),
            &value_size,
            |b, &value_size| {
                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_value_size: value_size * 2,
                                max_memory: 256 * 1024 * 1024, // 256MB for large values
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(10)); // Lower concurrency for large values
                            let mut handles = Vec::new();

                            for i in 0..50 {
                                // Fewer operations for large values
                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let key = format!("large_key{}", i).into_bytes();
                                    let value = vec![b'x'; value_size];

                                    let set_cmd = Command {
                                        name: "set".to_string(),
                                        args: vec![key, value],
                                    };

                                    black_box(handler_ref.process(set_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

fn bench_json_operations_throughput(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("JSON Operations Throughput");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for concurrency in [1, 10, 25].iter() {
        group.bench_with_input(BenchmarkId::new("JSON.SET Operations", concurrency), concurrency, |b, &concurrency| {
            b.iter_with_setup(
                || (),
                |_| {
                    runtime.block_on(async {
                        let config = StorageConfig {
                            max_memory: 64 * 1024 * 1024,
                            ..Default::default()
                        };
                        let storage = StorageEngine::new(config);
                        let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                        let semaphore = Arc::new(Semaphore::new(concurrency));
                        let mut handles = Vec::new();

                        for i in 0..100 {
                            let handler_ref = Arc::clone(&handler);
                            let semaphore_ref = Arc::clone(&semaphore);

                            let handle = tokio::spawn(async move {
                                let _permit = semaphore_ref.acquire().await.unwrap();

                                let key = format!("json_key{}", i).into_bytes();
                                let path = b"$".to_vec();
                                let json_value = format!(
                                    r#"{{"id": {}, "name": "test{}", "active": true, "score": {}.5}}"#,
                                    i, i, i
                                ).into_bytes();

                                let json_set_cmd = Command {
                                    name: "json_set".to_string(),
                                    args: vec![key, path, json_value],
                                };

                                let _ = black_box(handler_ref.process(json_set_cmd, 0).await);
                            });

                            handles.push(handle);
                        }

                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                }
            );
        });
    }

    group.finish();
}

fn bench_memory_usage_patterns(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("Memory Usage Patterns");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(4));

    // Test different memory limits and their impact on performance
    for &memory_limit_mb in &[32, 64, 128] {
        group.bench_with_input(
            BenchmarkId::new("Memory Pressure", memory_limit_mb),
            &memory_limit_mb,
            |b, &memory_limit_mb| {
                b.iter_with_setup(
                    || (),
                    |_| {
                        runtime.block_on(async {
                            let config = StorageConfig {
                                max_memory: (memory_limit_mb * 1024 * 1024) as usize,
                                ..Default::default()
                            };
                            let storage = StorageEngine::new(config);
                            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));

                            let semaphore = Arc::new(Semaphore::new(50));
                            let mut handles = Vec::new();

                            // Try to use close to memory limit
                            let num_operations = memory_limit_mb * 5; // Adjust based on memory limit

                            for i in 0..num_operations {
                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let key = format!("mem_key{}", i).into_bytes();
                                    let value = vec![b'x'; 1000]; // 1KB values

                                    let set_cmd = Command {
                                        name: "set".to_string(),
                                        args: vec![key, value],
                                    };

                                    black_box(handler_ref.process(set_cmd, 0).await.unwrap());
                                });

                                handles.push(handle);
                            }

                            for handle in handles {
                                handle.await.unwrap();
                            }
                        })
                    },
                );
            },
        );
    }

    group.finish();
}

criterion_group!(string_throughput, bench_string_operations_throughput);

criterion_group!(
    datatype_throughput,
    bench_hash_operations_throughput,
    bench_list_operations_throughput,
    bench_set_operations_throughput,
    bench_sorted_set_operations_throughput
);

criterion_group!(mixed_workload, bench_mixed_workload_throughput);

criterion_group!(
    specialized_throughput,
    bench_expiry_performance,
    bench_large_values_throughput,
    bench_json_operations_throughput
);

criterion_group!(resource_management, bench_memory_usage_patterns);

criterion_main!(
    string_throughput,
    datatype_throughput,
    mixed_workload,
    specialized_throughput,
    resource_management
);
