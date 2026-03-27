use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

use rlightning::command::Command;
use rlightning::command::handler::CommandHandler;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

fn make_handler(rt: &tokio::runtime::Runtime) -> Arc<CommandHandler> {
    let _guard = rt.enter();
    let config = StorageConfig {
        max_memory: 256 * 1024 * 1024,
        max_value_size: 256 * 1024,
        ..Default::default()
    };
    let storage = StorageEngine::new(config);
    Arc::new(CommandHandler::new(storage))
}

// --- Stream command benchmarks ---

fn bench_stream_xadd(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Stream XADD");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &count in &[100, 500, 1000] {
        group.bench_with_input(BenchmarkId::new("XADD", count), &count, |b, &count| {
            b.iter(|| {
                // Fresh handler per iteration to avoid accumulating entries
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..count {
                        let cmd = Command {
                            name: "xadd".to_string(),
                            args: vec![
                                b"mystream".to_vec(),
                                b"*".to_vec(),
                                b"field".to_vec(),
                                format!("value:{}", i).into_bytes(),
                            ],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    }
                })
            });
        });
    }
    group.finish();
}

fn bench_stream_xread_xrange(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Stream XREAD/XRANGE");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &stream_size in &[100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("XRANGE", stream_size),
            &stream_size,
            |b, &stream_size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..stream_size {
                        let cmd = Command {
                            name: "xadd".to_string(),
                            args: vec![
                                b"readstream".to_vec(),
                                b"*".to_vec(),
                                b"key".to_vec(),
                                format!("val:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd, 0).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "xrange".to_string(),
                            args: vec![b"readstream".to_vec(), b"-".to_vec(), b"+".to_vec()],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

// --- Pub/Sub benchmarks ---

fn bench_pubsub_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("PubSub Throughput");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    use rlightning::pubsub::PubSubManager;

    // Fan-out to different subscriber counts
    for &subscribers in &[1, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("Publish fan-out", subscribers),
            &subscribers,
            |b, &subscribers| {
                let pubsub = Arc::new(PubSubManager::new());
                let mut _receivers = Vec::new();
                rt.block_on(async {
                    for _ in 0..subscribers {
                        let (id, rx) = pubsub.register_client().await;
                        pubsub.subscribe(id, vec![b"bench-channel".to_vec()]).await;
                        _receivers.push((id, rx));
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..100 {
                            black_box(
                                pubsub
                                    .publish(
                                        b"bench-channel".to_vec(),
                                        format!("msg:{}", i).into_bytes(),
                                    )
                                    .await,
                            );
                        }
                    })
                });
            },
        );
    }

    // Message throughput (single subscriber)
    group.bench_function("Message throughput 1000", |b| {
        let pubsub = Arc::new(PubSubManager::new());
        let _rx = rt.block_on(async {
            let (id, rx) = pubsub.register_client().await;
            pubsub.subscribe(id, vec![b"throughput".to_vec()]).await;
            (id, rx)
        });
        b.iter(|| {
            rt.block_on(async {
                for i in 0..1000 {
                    black_box(
                        pubsub
                            .publish(
                                b"throughput".to_vec(),
                                format!("message:{}", i).into_bytes(),
                            )
                            .await,
                    );
                }
            })
        });
    });

    group.finish();
}

// --- Transaction benchmarks ---

fn bench_transactions(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Transaction Overhead");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    use rlightning::command::transaction::{
        TransactionState, handle_exec, handle_multi, queue_command,
    };

    // Non-transactional baseline: 10 SET commands
    group.bench_function("10 SETs without MULTI/EXEC", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                for i in 0..10 {
                    let cmd = Command {
                        name: "set".to_string(),
                        args: vec![
                            format!("txkey:{}", i).into_bytes(),
                            format!("val:{}", i).into_bytes(),
                        ],
                    };
                    black_box(handler.process(cmd, 0).await.unwrap());
                }
            })
        });
    });

    // Transactional: MULTI + 10 SET + EXEC
    group.bench_function("MULTI + 10 SETs + EXEC", |b| {
        let (storage, handler) = {
            let _guard = rt.enter();
            let config = StorageConfig {
                max_memory: 256 * 1024 * 1024,
                ..Default::default()
            };
            let storage = StorageEngine::new(config);
            let handler = Arc::new(CommandHandler::new(Arc::clone(&storage)));
            (storage, handler)
        };
        b.iter(|| {
            rt.block_on(async {
                let mut tx_state = TransactionState::new();
                handle_multi(&mut tx_state).unwrap();
                for i in 0..10 {
                    let cmd = Command {
                        name: "set".to_string(),
                        args: vec![
                            format!("txkey:{}", i).into_bytes(),
                            format!("val:{}", i).into_bytes(),
                        ],
                    };
                    queue_command(&mut tx_state, cmd).unwrap();
                }
                black_box(
                    handle_exec(&mut tx_state, &handler, &storage, 0)
                        .await
                        .unwrap(),
                );
            })
        });
    });

    group.finish();
}

// --- Pipeline benchmarks ---

fn bench_pipeline(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Pipeline Throughput");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &batch_size in &[10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::new("Pipelined SETs", batch_size),
            &batch_size,
            |b, &batch_size| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        let mut futures = Vec::with_capacity(batch_size);
                        for i in 0..batch_size {
                            let handler_ref = handler.clone();
                            futures.push(async move {
                                let cmd = Command {
                                    name: "set".to_string(),
                                    args: vec![
                                        format!("pipekey:{}", i).into_bytes(),
                                        format!("val:{}", i).into_bytes(),
                                    ],
                                };
                                handler_ref.process(cmd, 0).await.unwrap()
                            });
                        }
                        let results: Vec<_> = futures::future::join_all(futures).await;
                        black_box(results);
                    })
                });
            },
        );
    }
    group.finish();
}

// --- Persistence benchmarks ---

fn bench_persistence(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Persistence");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(5));

    use rlightning::persistence::rdb::RdbPersistence;

    // RDB save with varying key counts
    for &key_count in &[100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("RDB Save", key_count),
            &key_count,
            |b, &key_count| {
                b.iter_with_setup(
                    || {
                        let _guard = rt.enter();
                        let config = StorageConfig {
                            max_memory: 256 * 1024 * 1024,
                            ..Default::default()
                        };
                        let storage = StorageEngine::new(config);
                        let handler = CommandHandler::new(Arc::clone(&storage));
                        rt.block_on(async {
                            for i in 0..key_count {
                                let cmd = Command {
                                    name: "set".to_string(),
                                    args: vec![
                                        format!("rdbkey:{}", i).into_bytes(),
                                        format!("rdbvalue:{}", i).into_bytes(),
                                    ],
                                };
                                handler.process(cmd, 0).await.unwrap();
                            }
                        });
                        let tmp = tempfile::NamedTempFile::new().unwrap();
                        let path = tmp.path().to_path_buf();
                        (storage, path, tmp)
                    },
                    |(storage, path, _tmp)| {
                        rt.block_on(async {
                            let rdb = RdbPersistence::new(storage, path);
                            rdb.save().await.unwrap();
                            black_box(());
                        })
                    },
                );
            },
        );
    }

    // RDB load benchmark
    group.bench_function("RDB Load 500 keys", |b| {
        b.iter_with_setup(
            || {
                let _guard = rt.enter();
                let config = StorageConfig {
                    max_memory: 256 * 1024 * 1024,
                    ..Default::default()
                };
                let storage = StorageEngine::new(config);
                let handler = CommandHandler::new(Arc::clone(&storage));
                rt.block_on(async {
                    for i in 0..500 {
                        let cmd = Command {
                            name: "set".to_string(),
                            args: vec![
                                format!("loadkey:{}", i).into_bytes(),
                                format!("loadvalue:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd, 0).await.unwrap();
                    }
                });
                let tmp = tempfile::NamedTempFile::new().unwrap();
                let path = tmp.path().to_path_buf();
                rt.block_on(async {
                    let rdb = RdbPersistence::new(Arc::clone(&storage), path.clone());
                    rdb.save().await.unwrap();
                });
                (path, tmp)
            },
            |(path, _tmp)| {
                let _guard = rt.enter();
                let fresh_config = StorageConfig {
                    max_memory: 256 * 1024 * 1024,
                    ..Default::default()
                };
                let fresh_storage = StorageEngine::new(fresh_config);
                rt.block_on(async {
                    let rdb = RdbPersistence::new(fresh_storage, path);
                    rdb.load().await.unwrap();
                    black_box(());
                })
            },
        );
    });

    // AOF append throughput
    group.bench_function("AOF Append 100 commands", |b| {
        use rlightning::networking::resp::RespCommand;
        use rlightning::persistence::aof::AofPersistence;
        use rlightning::persistence::config::AofSyncPolicy;

        // Create AOF once for all iterations to avoid spawning excessive background tasks
        let _guard = rt.enter();
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_path_buf();
        let aof = AofPersistence::new(storage, path);
        let counter = std::sync::atomic::AtomicU64::new(0);

        b.iter(|| {
            let batch = counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            rt.block_on(async {
                for i in 0..100 {
                    let resp_cmd = RespCommand {
                        name: b"SET".to_vec(),
                        args: vec![
                            format!("aofkey:{}:{}", batch, i).into_bytes(),
                            format!("aofval:{}:{}", batch, i).into_bytes(),
                        ],
                    };
                    aof.append_command(resp_cmd, AofSyncPolicy::None)
                        .await
                        .unwrap();
                }
            })
        });
    });

    group.finish();
}

// --- Memory efficiency benchmarks ---

fn bench_memory_efficiency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Memory Efficiency");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(3));

    // Memory usage per data type
    #[allow(clippy::type_complexity)]
    let data_types: Vec<(&str, Box<dyn Fn(usize) -> Command + Send + Sync>)> = vec![
        (
            "String",
            Box::new(|i| Command {
                name: "set".to_string(),
                args: vec![
                    format!("memstr:{}", i).into_bytes(),
                    format!("value:{}", i).into_bytes(),
                ],
            }),
        ),
        (
            "Hash",
            Box::new(|i| Command {
                name: "hset".to_string(),
                args: vec![
                    format!("memhash:{}", i % 100).into_bytes(),
                    format!("field:{}", i).into_bytes(),
                    format!("value:{}", i).into_bytes(),
                ],
            }),
        ),
        (
            "List",
            Box::new(|i| Command {
                name: "lpush".to_string(),
                args: vec![
                    format!("memlist:{}", i % 100).into_bytes(),
                    format!("item:{}", i).into_bytes(),
                ],
            }),
        ),
        (
            "Set",
            Box::new(|i| Command {
                name: "sadd".to_string(),
                args: vec![
                    format!("memset:{}", i % 100).into_bytes(),
                    format!("member:{}", i).into_bytes(),
                ],
            }),
        ),
        (
            "SortedSet",
            Box::new(|i| Command {
                name: "zadd".to_string(),
                args: vec![
                    format!("memzset:{}", i % 100).into_bytes(),
                    format!("{}", i as f64).into_bytes(),
                    format!("member:{}", i).into_bytes(),
                ],
            }),
        ),
    ];

    for (type_name, cmd_fn) in &data_types {
        group.bench_function(format!("{} 1000 ops", type_name), |b| {
            b.iter(|| {
                rt.block_on(async {
                    let config = StorageConfig {
                        max_memory: 128 * 1024 * 1024,
                        ..Default::default()
                    };
                    let storage = StorageEngine::new(config);
                    let handler = CommandHandler::new(Arc::clone(&storage));
                    for i in 0..1000 {
                        let cmd = cmd_fn(i);
                        black_box(handler.process(cmd, 0).await.unwrap());
                    }
                })
            });
        });
    }

    group.finish();
}

// --- Concurrent client benchmarks ---

fn bench_concurrent_clients(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Concurrent Clients");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &client_count in &[1, 10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("Clients", client_count),
            &client_count,
            |b, &client_count| {
                let handler = make_handler(&rt);
                // Pre-populate data for GETs
                rt.block_on(async {
                    for i in 0..100 {
                        let cmd = Command {
                            name: "set".to_string(),
                            args: vec![
                                format!("conckey:{}", i).into_bytes(),
                                format!("concval:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd, 0).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        let semaphore = Arc::new(Semaphore::new(client_count));
                        let mut handles = Vec::with_capacity(client_count);
                        for i in 0..client_count {
                            let handler_ref = Arc::clone(&handler);
                            let sem = Arc::clone(&semaphore);
                            let handle = tokio::spawn(async move {
                                let _permit = sem.acquire().await.unwrap();
                                // Each "client" does a SET then a GET
                                let set_cmd = Command {
                                    name: "set".to_string(),
                                    args: vec![
                                        format!("clientkey:{}", i).into_bytes(),
                                        format!("clientval:{}", i).into_bytes(),
                                    ],
                                };
                                handler_ref.process(set_cmd, 0).await.unwrap();
                                let get_cmd = Command {
                                    name: "get".to_string(),
                                    args: vec![format!("conckey:{}", i % 100).into_bytes()],
                                };
                                black_box(handler_ref.process(get_cmd, 0).await.unwrap());
                            });
                            handles.push(handle);
                        }
                        for handle in handles {
                            handle.await.unwrap();
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

// --- Latency benchmarks (p50/p95/p99) ---

fn bench_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Latency Percentiles");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(5));

    // Single-op latency for SET
    group.bench_function("SET single-op latency", |b| {
        let handler = make_handler(&rt);
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            rt.block_on(async {
                let cmd = Command {
                    name: "set".to_string(),
                    args: vec![
                        format!("latkey:{}", counter).into_bytes(),
                        b"latvalue".to_vec(),
                    ],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    // Single-op latency for GET
    group.bench_function("GET single-op latency", |b| {
        let handler = make_handler(&rt);
        rt.block_on(async {
            for i in 0..1000 {
                let cmd = Command {
                    name: "set".to_string(),
                    args: vec![
                        format!("latgetkey:{}", i).into_bytes(),
                        b"latvalue".to_vec(),
                    ],
                };
                handler.process(cmd, 0).await.unwrap();
            }
        });
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            rt.block_on(async {
                let cmd = Command {
                    name: "get".to_string(),
                    args: vec![format!("latgetkey:{}", counter % 1000).into_bytes()],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    // Single-op latency for HSET
    group.bench_function("HSET single-op latency", |b| {
        let handler = make_handler(&rt);
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            rt.block_on(async {
                let cmd = Command {
                    name: "hset".to_string(),
                    args: vec![
                        b"lathash".to_vec(),
                        format!("field:{}", counter).into_bytes(),
                        b"latvalue".to_vec(),
                    ],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    // Single-op latency for ZADD
    group.bench_function("ZADD single-op latency", |b| {
        let handler = make_handler(&rt);
        let mut counter = 0u64;
        b.iter(|| {
            counter += 1;
            rt.block_on(async {
                let cmd = Command {
                    name: "zadd".to_string(),
                    args: vec![
                        b"latzset".to_vec(),
                        format!("{}", counter as f64).into_bytes(),
                        format!("member:{}", counter).into_bytes(),
                    ],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    group.finish();
}

// --- Blocking command benchmarks ---

fn bench_blocking_commands(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Blocking Commands");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    // BLPOP wakeup latency: push data then immediately BLPOP (should not block)
    group.bench_function("BLPOP immediate wakeup", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                // Push an element first
                let push_cmd = Command {
                    name: "lpush".to_string(),
                    args: vec![b"blpoplist".to_vec(), b"item".to_vec()],
                };
                handler.process(push_cmd, 0).await.unwrap();

                // BLPOP should return immediately since data is available
                let blpop_cmd = Command {
                    name: "blpop".to_string(),
                    args: vec![b"blpoplist".to_vec(), b"0".to_vec()],
                };
                black_box(handler.process(blpop_cmd, 0).await.unwrap());
            })
        });
    });

    // BRPOP immediate wakeup
    group.bench_function("BRPOP immediate wakeup", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                let push_cmd = Command {
                    name: "rpush".to_string(),
                    args: vec![b"brpoplist".to_vec(), b"item".to_vec()],
                };
                handler.process(push_cmd, 0).await.unwrap();

                let brpop_cmd = Command {
                    name: "brpop".to_string(),
                    args: vec![b"brpoplist".to_vec(), b"0".to_vec()],
                };
                black_box(handler.process(brpop_cmd, 0).await.unwrap());
            })
        });
    });

    // Blocking notification throughput via BlockingManager
    group.bench_function("BlockingManager notify throughput", |b| {
        use rlightning::command::types::blocking::BlockingManager;
        let mgr = BlockingManager::new();
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("notifykey:{}", i % 100);
                mgr.notify_key(key.as_bytes());
            }
        });
    });

    group.finish();
}

// --- Lua scripting benchmarks ---

fn bench_lua_scripting(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Lua Scripting");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    // Simple script execution overhead
    group.bench_function("Simple EVAL (return 1)", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                let cmd = Command {
                    name: "eval".to_string(),
                    args: vec![b"return 1".to_vec(), b"0".to_vec()],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    // Script with redis.call
    group.bench_function("EVAL with redis.call SET/GET", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                let script =
                    b"redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])"
                        .to_vec();
                let cmd = Command {
                    name: "eval".to_string(),
                    args: vec![
                        script,
                        b"1".to_vec(),
                        b"scriptkey".to_vec(),
                        b"scriptval".to_vec(),
                    ],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    // EVALSHA: cached script execution
    group.bench_function("EVALSHA cached script", |b| {
        let handler = make_handler(&rt);
        let sha = rt.block_on(async {
            let cmd = Command {
                name: "script".to_string(),
                args: vec![b"load".to_vec(), b"return 42".to_vec()],
            };
            let result = handler.process(cmd, 0).await.unwrap();
            // Extract SHA from BulkString response
            match result {
                rlightning::networking::resp::RespValue::BulkString(Some(sha)) => sha,
                _ => b"unknown".to_vec(),
            }
        });
        b.iter(|| {
            rt.block_on(async {
                let cmd = Command {
                    name: "evalsha".to_string(),
                    args: vec![sha.clone(), b"0".to_vec()],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    // Script with loop (computation overhead)
    group.bench_function("EVAL loop computation", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                let script =
                    b"local sum = 0; for i = 1, 1000 do sum = sum + i end; return sum".to_vec();
                let cmd = Command {
                    name: "eval".to_string(),
                    args: vec![script, b"0".to_vec()],
                };
                black_box(handler.process(cmd, 0).await.unwrap());
            })
        });
    });

    group.finish();
}

criterion_group!(stream_benches, bench_stream_xadd, bench_stream_xread_xrange);

criterion_group!(pubsub_benches, bench_pubsub_throughput);

criterion_group!(transaction_benches, bench_transactions);

criterion_group!(pipeline_benches, bench_pipeline);

criterion_group!(persistence_benches, bench_persistence);

criterion_group!(memory_benches, bench_memory_efficiency);

criterion_group!(concurrent_benches, bench_concurrent_clients);

criterion_group!(latency_benches, bench_latency);

criterion_group!(blocking_benches, bench_blocking_commands);

criterion_group!(scripting_benches, bench_lua_scripting);

criterion_main!(
    stream_benches,
    pubsub_benches,
    transaction_benches,
    pipeline_benches,
    persistence_benches,
    memory_benches,
    concurrent_benches,
    latency_benches,
    blocking_benches,
    scripting_benches
);
