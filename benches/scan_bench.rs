use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
use tokio::time::Duration;

use rlightning::command::Command;
use rlightning::command::handler::CommandHandler;
use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

fn make_handler(rt: &tokio::runtime::Runtime) -> Arc<CommandHandler> {
    let _guard = rt.enter();
    let config = StorageConfig {
        max_memory: 512 * 1024 * 1024,
        max_value_size: 256 * 1024,
        ..Default::default()
    };
    let storage = StorageEngine::new(config);
    Arc::new(CommandHandler::new(storage))
}

fn extract_cursor(result: &RespValue) -> u64 {
    match result {
        RespValue::Array(Some(parts)) if parts.len() >= 2 => match &parts[0] {
            RespValue::BulkString(Some(cursor_bytes)) => String::from_utf8_lossy(cursor_bytes)
                .parse::<u64>()
                .unwrap_or(0),
            _ => 0,
        },
        _ => 0,
    }
}

fn populate_keys(rt: &tokio::runtime::Runtime, handler: &Arc<CommandHandler>, count: usize) {
    rt.block_on(async {
        for i in 0..count {
            let cmd = Command {
                name: "set".to_string(),
                args: vec![
                    format!("scankey:{:06}", i).into_bytes(),
                    format!("val:{}", i).into_bytes(),
                ],
            };
            handler.process(cmd, 0).await.unwrap();
        }
    });
}

// --- SCAN benchmarks ---

fn bench_scan_single_page(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("SCAN Single Page");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &keyspace in &[1_000, 10_000, 50_000] {
        let handler = make_handler(&rt);
        populate_keys(&rt, &handler, keyspace);

        group.bench_with_input(
            BenchmarkId::new("SCAN", format!("{}keys", keyspace)),
            &keyspace,
            |b, _| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "scan".to_string(),
                            args: vec![b"0".to_vec(), b"COUNT".to_vec(), b"100".to_vec()],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_scan_full_iteration(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("SCAN Full Iteration");
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(10));

    for &keyspace in &[1_000, 10_000] {
        let handler = make_handler(&rt);
        populate_keys(&rt, &handler, keyspace);

        group.bench_with_input(
            BenchmarkId::new("full scan", format!("{}keys", keyspace)),
            &keyspace,
            |b, _| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let mut cursor = 0u64;
                        loop {
                            let cmd = Command {
                                name: "scan".to_string(),
                                args: vec![
                                    cursor.to_string().into_bytes(),
                                    b"COUNT".to_vec(),
                                    b"100".to_vec(),
                                ],
                            };
                            let result = handler.process(cmd, 0).await.unwrap();
                            cursor = extract_cursor(&result);
                            if cursor == 0 {
                                break;
                            }
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_scan_with_match(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("SCAN with MATCH");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    let handler = make_handler(&rt);
    populate_keys(&rt, &handler, 10_000);

    for pattern in &["scankey:000*", "scankey:*99*", "*"] {
        group.bench_with_input(
            BenchmarkId::new("MATCH", *pattern),
            pattern,
            |b, pattern| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "scan".to_string(),
                            args: vec![
                                b"0".to_vec(),
                                b"MATCH".to_vec(),
                                pattern.as_bytes().to_vec(),
                                b"COUNT".to_vec(),
                                b"100".to_vec(),
                            ],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_scan_count_sizes(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("SCAN COUNT Sizes");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    let handler = make_handler(&rt);
    populate_keys(&rt, &handler, 10_000);

    for &count in &[10, 100, 1000] {
        group.bench_with_input(BenchmarkId::new("COUNT", count), &count, |b, &count| {
            let handler = handler.clone();
            b.iter(|| {
                rt.block_on(async {
                    let cmd = Command {
                        name: "scan".to_string(),
                        args: vec![
                            b"0".to_vec(),
                            b"COUNT".to_vec(),
                            count.to_string().into_bytes(),
                        ],
                    };
                    black_box(handler.process(cmd, 0).await.unwrap());
                })
            });
        });
    }
    group.finish();
}

// --- HSCAN benchmarks ---

fn bench_hscan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("HSCAN");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &field_count in &[10, 100, 500, 1000] {
        let handler = make_handler(&rt);
        rt.block_on(async {
            for i in 0..field_count {
                let cmd = Command {
                    name: "hset".to_string(),
                    args: vec![
                        b"hscan:hash".to_vec(),
                        format!("field:{:04}", i).into_bytes(),
                        format!("value:{}", i).into_bytes(),
                    ],
                };
                handler.process(cmd, 0).await.unwrap();
            }
        });

        // Single page
        group.bench_with_input(
            BenchmarkId::new("single page", format!("{}fields", field_count)),
            &field_count,
            |b, _| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "hscan".to_string(),
                            args: vec![
                                b"hscan:hash".to_vec(),
                                b"0".to_vec(),
                                b"COUNT".to_vec(),
                                b"100".to_vec(),
                            ],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );

        // Full iteration
        if field_count >= 100 {
            group.bench_with_input(
                BenchmarkId::new("full iteration", format!("{}fields", field_count)),
                &field_count,
                |b, _| {
                    let handler = handler.clone();
                    b.iter(|| {
                        rt.block_on(async {
                            let mut cursor = 0u64;
                            loop {
                                let cmd = Command {
                                    name: "hscan".to_string(),
                                    args: vec![
                                        b"hscan:hash".to_vec(),
                                        cursor.to_string().into_bytes(),
                                        b"COUNT".to_vec(),
                                        b"50".to_vec(),
                                    ],
                                };
                                let result = handler.process(cmd, 0).await.unwrap();
                                cursor = extract_cursor(&result);
                                if cursor == 0 {
                                    break;
                                }
                            }
                        })
                    });
                },
            );
        }
    }
    group.finish();
}

// --- SSCAN benchmarks ---

fn bench_sscan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("SSCAN");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &member_count in &[100, 500, 1000] {
        let handler = make_handler(&rt);
        rt.block_on(async {
            for i in 0..member_count {
                let cmd = Command {
                    name: "sadd".to_string(),
                    args: vec![
                        b"sscan:set".to_vec(),
                        format!("member:{:04}", i).into_bytes(),
                    ],
                };
                handler.process(cmd, 0).await.unwrap();
            }
        });

        group.bench_with_input(
            BenchmarkId::new("full iteration", format!("{}members", member_count)),
            &member_count,
            |b, _| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let mut cursor = 0u64;
                        loop {
                            let cmd = Command {
                                name: "sscan".to_string(),
                                args: vec![
                                    b"sscan:set".to_vec(),
                                    cursor.to_string().into_bytes(),
                                    b"COUNT".to_vec(),
                                    b"100".to_vec(),
                                ],
                            };
                            let result = handler.process(cmd, 0).await.unwrap();
                            cursor = extract_cursor(&result);
                            if cursor == 0 {
                                break;
                            }
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

// --- ZSCAN benchmarks ---

fn bench_zscan(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("ZSCAN");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &member_count in &[100, 500, 1000] {
        let handler = make_handler(&rt);
        rt.block_on(async {
            for i in 0..member_count {
                let cmd = Command {
                    name: "zadd".to_string(),
                    args: vec![
                        b"zscan:zset".to_vec(),
                        format!("{}", i as f64).into_bytes(),
                        format!("member:{:04}", i).into_bytes(),
                    ],
                };
                handler.process(cmd, 0).await.unwrap();
            }
        });

        group.bench_with_input(
            BenchmarkId::new("full iteration", format!("{}members", member_count)),
            &member_count,
            |b, _| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let mut cursor = 0u64;
                        loop {
                            let cmd = Command {
                                name: "zscan".to_string(),
                                args: vec![
                                    b"zscan:zset".to_vec(),
                                    cursor.to_string().into_bytes(),
                                    b"COUNT".to_vec(),
                                    b"100".to_vec(),
                                ],
                            };
                            let result = handler.process(cmd, 0).await.unwrap();
                            cursor = extract_cursor(&result);
                            if cursor == 0 {
                                break;
                            }
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    scan_benches,
    bench_scan_single_page,
    bench_scan_full_iteration,
    bench_scan_with_match,
    bench_scan_count_sizes
);
criterion_group!(
    collection_scan_benches,
    bench_hscan,
    bench_sscan,
    bench_zscan
);
criterion_main!(scan_benches, collection_scan_benches);
