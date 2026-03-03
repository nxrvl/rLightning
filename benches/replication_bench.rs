use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::sync::Semaphore;

use rlightning::command::Command;
use rlightning::command::handler::CommandHandler;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

fn bench_replication_setup(c: &mut Criterion) {
    let mut group = c.benchmark_group("Replication - Setup Performance");

    group.bench_function("Master initialization simulation", |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        b.iter(|| {
            runtime.block_on(async {
                let storage_config = StorageConfig::default();
                let storage = StorageEngine::new(storage_config);
                let handler = Arc::new(CommandHandler::new(storage));

                // Simulate replication manager setup overhead
                black_box(handler);
            })
        });
    });

    group.bench_function("Replica initialization simulation", |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        b.iter(|| {
            runtime.block_on(async {
                let storage_config = StorageConfig::default();
                let storage = StorageEngine::new(storage_config);
                let handler = Arc::new(CommandHandler::new(storage));

                // Simulate replica setup overhead
                black_box(handler);
            })
        });
    });

    group.finish();
}

fn bench_replication_command_propagation(c: &mut Criterion) {
    let mut group = c.benchmark_group("Replication - Command Propagation");

    for concurrency in [1, 5, 10].iter() {
        group.bench_with_input(
            BenchmarkId::new("Master command processing", concurrency),
            concurrency,
            |b, &concurrency| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || {
                        runtime.block_on(async {
                            let storage_config = StorageConfig::default();
                            let storage = StorageEngine::new(storage_config);
                            let handler = Arc::new(CommandHandler::new(storage));
                            handler
                        })
                    },
                    |handler| {
                        runtime.block_on(async {
                            let semaphore = Arc::new(Semaphore::new(concurrency));
                            let mut handles = Vec::new();

                            for i in 0..100 {
                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let key = format!("repl_key_{}", i).into_bytes();
                                    let value = format!("repl_value_{}", i).into_bytes();

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

fn bench_replication_sync_performance(c: &mut Criterion) {
    let mut group = c.benchmark_group("Replication - Sync Performance");

    group.bench_function("Full sync simulation", |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        b.iter_with_setup(
            || {
                runtime.block_on(async {
                    let storage_config = StorageConfig::default();
                    let storage = StorageEngine::new(storage_config);
                    let handler = Arc::new(CommandHandler::new(storage.clone()));

                    // Populate master with data
                    for i in 0..1000 {
                        let key = format!("sync_key_{}", i);
                        let value = format!("sync_value_{}", i);
                        storage
                            .set(key.into_bytes(), value.into_bytes(), None)
                            .await
                            .unwrap();
                    }

                    storage
                })
            },
            |storage| {
                runtime.block_on(async {
                    // Simulate sync operation - reading all keys
                    let all_keys = storage.keys("*").await.unwrap();
                    black_box(all_keys);
                })
            },
        );
    });

    group.finish();
}

fn bench_replication_memory_usage(c: &mut Criterion) {
    let mut group = c.benchmark_group("Replication - Memory Usage");

    for &data_size in &[100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("Memory usage with replicas", data_size),
            &data_size,
            |b, &data_size| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter(|| {
                    runtime.block_on(async {
                        let storage_config = StorageConfig {
                            max_memory: 64 * 1024 * 1024, // 64MB
                            ..Default::default()
                        };
                        let storage = StorageEngine::new(storage_config);
                        let handler = Arc::new(CommandHandler::new(storage.clone()));

                        // Add data
                        for i in 0..data_size {
                            let key = format!("mem_key_{}", i);
                            let value = vec![b'x'; 1000]; // 1KB values
                            storage.set(key.into_bytes(), value, None).await.unwrap();
                        }

                        // Check memory usage (simulated)
                        let all_keys = storage.keys("*").await.unwrap();
                        black_box(all_keys.len());
                    })
                });
            },
        );
    }

    group.finish();
}

fn bench_replication_failover(c: &mut Criterion) {
    let mut group = c.benchmark_group("Replication - Failover Performance");

    group.bench_function("Master-to-replica promotion", |b| {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        b.iter_with_setup(
            || {
                runtime.block_on(async {
                    let storage_config = StorageConfig::default();
                    let storage = StorageEngine::new(storage_config);
                    let handler = Arc::new(CommandHandler::new(storage.clone()));

                    // Add some data
                    for i in 0..100 {
                        let key = format!("failover_key_{}", i);
                        let value = format!("failover_value_{}", i);
                        storage
                            .set(key.into_bytes(), value.into_bytes(), None)
                            .await
                            .unwrap();
                    }

                    handler
                })
            },
            |_handler| {
                runtime.block_on(async {
                    // Simulate promotion to master (configuration change)
                    let new_storage = StorageEngine::new(StorageConfig::default());
                    let new_handler = Arc::new(CommandHandler::new(new_storage));

                    black_box(new_handler);
                })
            },
        );
    });

    group.finish();
}

fn bench_replication_lag_simulation(c: &mut Criterion) {
    let mut group = c.benchmark_group("Replication - Lag Simulation");

    for &command_rate in &[10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("Command processing under load", command_rate),
            &command_rate,
            |b, &command_rate| {
                let runtime = tokio::runtime::Runtime::new().unwrap();

                b.iter_with_setup(
                    || {
                        runtime.block_on(async {
                            let storage_config = StorageConfig::default();
                            let storage = StorageEngine::new(storage_config);
                            let handler = Arc::new(CommandHandler::new(storage));
                            handler
                        })
                    },
                    |handler| {
                        runtime.block_on(async {
                            let semaphore = Arc::new(Semaphore::new(10)); // Limit concurrency
                            let mut handles = Vec::new();

                            for i in 0..command_rate {
                                let handler_ref = Arc::clone(&handler);
                                let semaphore_ref = Arc::clone(&semaphore);

                                let handle = tokio::spawn(async move {
                                    let _permit = semaphore_ref.acquire().await.unwrap();

                                    let key = format!("lag_key_{}", i).into_bytes();
                                    let value = format!("lag_value_{}", i).into_bytes();

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

criterion_group!(replication_setup, bench_replication_setup);

criterion_group!(
    replication_operations,
    bench_replication_command_propagation,
    bench_replication_sync_performance
);

criterion_group!(
    replication_performance,
    bench_replication_memory_usage,
    bench_replication_failover,
    bench_replication_lag_simulation
);

criterion_main!(
    replication_setup,
    replication_operations,
    replication_performance
);
