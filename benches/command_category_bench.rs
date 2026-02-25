use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use tokio::time::Duration;

use rlightning::command::handler::CommandHandler;
use rlightning::command::Command;
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

fn make_value(size: usize) -> Vec<u8> {
    vec![b'x'; size]
}

// --- String command benchmarks with varying value sizes ---

fn bench_string_set_value_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("String SET Value Sizes");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &size in &[64, 1024, 10_240, 102_400] {
        group.bench_with_input(
            BenchmarkId::new("SET", format!("{}B", size)),
            &size,
            |b, &size| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..100 {
                            let cmd = Command {
                                name: "set".to_string(),
                                args: vec![
                                    format!("strkey:{}", i).into_bytes(),
                                    make_value(size),
                                ],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_string_get_value_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("String GET Value Sizes");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &size in &[64, 1024, 10_240, 102_400] {
        group.bench_with_input(
            BenchmarkId::new("GET", format!("{}B", size)),
            &size,
            |b, &size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..100 {
                        let cmd = Command {
                            name: "set".to_string(),
                            args: vec![
                                format!("strkey:{}", i).into_bytes(),
                                make_value(size),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..100 {
                            let cmd = Command {
                                name: "get".to_string(),
                                args: vec![format!("strkey:{}", i).into_bytes()],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_string_mset_mget(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("String MSET/MGET");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &batch_size in &[5, 10, 50] {
        group.bench_with_input(
            BenchmarkId::new("MSET", batch_size),
            &batch_size,
            |b, &batch_size| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        let mut args = Vec::with_capacity(batch_size * 2);
                        for i in 0..batch_size {
                            args.push(format!("msetkey:{}", i).into_bytes());
                            args.push(format!("val:{}", i).into_bytes());
                        }
                        let cmd = Command {
                            name: "mset".to_string(),
                            args,
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("MGET", batch_size),
            &batch_size,
            |b, &batch_size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    let mut args = Vec::with_capacity(batch_size * 2);
                    for i in 0..batch_size {
                        args.push(format!("mgetkey:{}", i).into_bytes());
                        args.push(format!("val:{}", i).into_bytes());
                    }
                    let cmd = Command {
                        name: "mset".to_string(),
                        args,
                    };
                    handler.process(cmd).await.unwrap();
                });
                b.iter(|| {
                    rt.block_on(async {
                        let args: Vec<Vec<u8>> = (0..batch_size)
                            .map(|i| format!("mgetkey:{}", i).into_bytes())
                            .collect();
                        let cmd = Command {
                            name: "mget".to_string(),
                            args,
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_string_incr_append(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("String INCR/APPEND");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    group.bench_function("INCR x1000", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..1000 {
                    let cmd = Command {
                        name: "incr".to_string(),
                        args: vec![b"counter".to_vec()],
                    };
                    black_box(handler.process(cmd).await.unwrap());
                }
            })
        });
    });

    group.bench_function("APPEND x500", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                for i in 0..500 {
                    let cmd = Command {
                        name: "append".to_string(),
                        args: vec![
                            format!("appendkey:{}", i % 50).into_bytes(),
                            b"data".to_vec(),
                        ],
                    };
                    black_box(handler.process(cmd).await.unwrap());
                }
            })
        });
    });

    group.finish();
}

// --- List command benchmarks with varying list sizes ---

fn bench_list_push_pop(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("List PUSH/POP");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &list_size in &[10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("LPUSH", list_size),
            &list_size,
            |b, &list_size| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..list_size {
                            let cmd = Command {
                                name: "lpush".to_string(),
                                args: vec![
                                    b"pushlist".to_vec(),
                                    format!("item:{}", i).into_bytes(),
                                ],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("RPUSH", list_size),
            &list_size,
            |b, &list_size| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..list_size {
                            let cmd = Command {
                                name: "rpush".to_string(),
                                args: vec![
                                    b"rpushlist".to_vec(),
                                    format!("item:{}", i).into_bytes(),
                                ],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );
    }

    group.bench_function("LPOP x500", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                // Push 500 items then pop them all
                for i in 0..500 {
                    let cmd = Command {
                        name: "lpush".to_string(),
                        args: vec![b"poplist".to_vec(), format!("item:{}", i).into_bytes()],
                    };
                    handler.process(cmd).await.unwrap();
                }
                for _ in 0..500 {
                    let cmd = Command {
                        name: "lpop".to_string(),
                        args: vec![b"poplist".to_vec()],
                    };
                    black_box(handler.process(cmd).await.unwrap());
                }
            })
        });
    });

    group.bench_function("RPOP x500", |b| {
        let handler = make_handler(&rt);
        b.iter(|| {
            rt.block_on(async {
                for i in 0..500 {
                    let cmd = Command {
                        name: "rpush".to_string(),
                        args: vec![b"rpoplist".to_vec(), format!("item:{}", i).into_bytes()],
                    };
                    handler.process(cmd).await.unwrap();
                }
                for _ in 0..500 {
                    let cmd = Command {
                        name: "rpop".to_string(),
                        args: vec![b"rpoplist".to_vec()],
                    };
                    black_box(handler.process(cmd).await.unwrap());
                }
            })
        });
    });

    group.finish();
}

fn bench_list_lrange(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("List LRANGE");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &list_size in &[100, 500, 1000] {
        group.bench_with_input(
            BenchmarkId::new("LRANGE full", list_size),
            &list_size,
            |b, &list_size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..list_size {
                        let cmd = Command {
                            name: "rpush".to_string(),
                            args: vec![
                                b"rangelist".to_vec(),
                                format!("item:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "lrange".to_string(),
                            args: vec![
                                b"rangelist".to_vec(),
                                b"0".to_vec(),
                                b"-1".to_vec(),
                            ],
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

// --- Hash command benchmarks with varying field counts ---

fn bench_hash_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Hash Operations");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &field_count in &[10, 50, 100, 500] {
        group.bench_with_input(
            BenchmarkId::new("HSET", field_count),
            &field_count,
            |b, &field_count| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..field_count {
                            let cmd = Command {
                                name: "hset".to_string(),
                                args: vec![
                                    b"myhash".to_vec(),
                                    format!("field:{}", i).into_bytes(),
                                    format!("value:{}", i).into_bytes(),
                                ],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("HGET", field_count),
            &field_count,
            |b, &field_count| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..field_count {
                        let cmd = Command {
                            name: "hset".to_string(),
                            args: vec![
                                b"gethash".to_vec(),
                                format!("field:{}", i).into_bytes(),
                                format!("value:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..field_count {
                            let cmd = Command {
                                name: "hget".to_string(),
                                args: vec![
                                    b"gethash".to_vec(),
                                    format!("field:{}", i).into_bytes(),
                                ],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("HGETALL", field_count),
            &field_count,
            |b, &field_count| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..field_count {
                        let cmd = Command {
                            name: "hset".to_string(),
                            args: vec![
                                b"allhash".to_vec(),
                                format!("field:{}", i).into_bytes(),
                                format!("value:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "hgetall".to_string(),
                            args: vec![b"allhash".to_vec()],
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

// --- Set command benchmarks with varying set sizes ---

fn bench_set_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Set Operations");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &set_size in &[10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("SADD", set_size),
            &set_size,
            |b, &set_size| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..set_size {
                            let cmd = Command {
                                name: "sadd".to_string(),
                                args: vec![
                                    b"addset".to_vec(),
                                    format!("member:{}", i).into_bytes(),
                                ],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );
    }

    // SINTER benchmark - two sets of varying sizes
    for &set_size in &[50, 200, 500] {
        group.bench_with_input(
            BenchmarkId::new("SINTER", set_size),
            &set_size,
            |b, &set_size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..set_size {
                        let cmd = Command {
                            name: "sadd".to_string(),
                            args: vec![
                                b"interset1".to_vec(),
                                format!("member:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                        // Overlapping set
                        let cmd = Command {
                            name: "sadd".to_string(),
                            args: vec![
                                b"interset2".to_vec(),
                                format!("member:{}", i + set_size / 2).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "sinter".to_string(),
                            args: vec![b"interset1".to_vec(), b"interset2".to_vec()],
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );
    }

    // SUNION benchmark
    for &set_size in &[50, 200, 500] {
        group.bench_with_input(
            BenchmarkId::new("SUNION", set_size),
            &set_size,
            |b, &set_size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..set_size {
                        let cmd = Command {
                            name: "sadd".to_string(),
                            args: vec![
                                b"unionset1".to_vec(),
                                format!("member:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                        let cmd = Command {
                            name: "sadd".to_string(),
                            args: vec![
                                b"unionset2".to_vec(),
                                format!("member:{}", i + set_size).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "sunion".to_string(),
                            args: vec![b"unionset1".to_vec(), b"unionset2".to_vec()],
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );
    }

    group.finish();
}

// --- Sorted set command benchmarks with varying sizes ---

fn bench_sorted_set_operations(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Sorted Set Operations");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &zset_size in &[10, 100, 1000] {
        group.bench_with_input(
            BenchmarkId::new("ZADD", zset_size),
            &zset_size,
            |b, &zset_size| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..zset_size {
                            let cmd = Command {
                                name: "zadd".to_string(),
                                args: vec![
                                    b"zaddset".to_vec(),
                                    format!("{}", i as f64).into_bytes(),
                                    format!("member:{}", i).into_bytes(),
                                ],
                            };
                            black_box(handler.process(cmd).await.unwrap());
                        }
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ZRANGE", zset_size),
            &zset_size,
            |b, &zset_size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..zset_size {
                        let cmd = Command {
                            name: "zadd".to_string(),
                            args: vec![
                                b"zrangeset".to_vec(),
                                format!("{}", i as f64).into_bytes(),
                                format!("member:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "zrange".to_string(),
                            args: vec![b"zrangeset".to_vec(), b"0".to_vec(), b"-1".to_vec()],
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("ZRANGEBYSCORE", zset_size),
            &zset_size,
            |b, &zset_size| {
                let handler = make_handler(&rt);
                rt.block_on(async {
                    for i in 0..zset_size {
                        let cmd = Command {
                            name: "zadd".to_string(),
                            args: vec![
                                b"zscoreset".to_vec(),
                                format!("{}", i as f64).into_bytes(),
                                format!("member:{}", i).into_bytes(),
                            ],
                        };
                        handler.process(cmd).await.unwrap();
                    }
                });
                let mid = zset_size / 4;
                let upper = zset_size * 3 / 4;
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "zrangebyscore".to_string(),
                            args: vec![
                                b"zscoreset".to_vec(),
                                format!("{}", mid).into_bytes(),
                                format!("{}", upper).into_bytes(),
                            ],
                        };
                        black_box(handler.process(cmd).await.unwrap());
                    })
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    string_benches,
    bench_string_set_value_sizes,
    bench_string_get_value_sizes,
    bench_string_mset_mget,
    bench_string_incr_append
);

criterion_group!(list_benches, bench_list_push_pop, bench_list_lrange);

criterion_group!(hash_benches, bench_hash_operations);

criterion_group!(set_benches, bench_set_operations);

criterion_group!(sorted_set_benches, bench_sorted_set_operations);

criterion_main!(
    string_benches,
    list_benches,
    hash_benches,
    set_benches,
    sorted_set_benches
);
