use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use std::sync::Arc;
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

// --- Geospatial benchmarks ---

fn bench_geoadd(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Geospatial GEOADD");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &count in &[10, 100, 1000, 5000] {
        group.bench_with_input(BenchmarkId::new("GEOADD", count), &count, |b, &count| {
            let handler = make_handler(&rt);
            b.iter(|| {
                rt.block_on(async {
                    let mut args: Vec<Vec<u8>> = vec![b"geokey:add".to_vec()];
                    for i in 0..count {
                        let lon = -74.0 + (i as f64 * 0.001) % 1.0;
                        let lat = 40.0 + (i as f64 * 0.0007) % 1.0;
                        args.push(format!("{:.6}", lon).into_bytes());
                        args.push(format!("{:.6}", lat).into_bytes());
                        args.push(format!("place:{}", i).into_bytes());
                    }
                    let cmd = Command {
                        name: "geoadd".to_string(),
                        args,
                    };
                    black_box(handler.process(cmd, 0).await.unwrap());
                })
            });
        });
    }
    group.finish();
}

fn bench_geodist(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Geospatial GEODIST");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &size in &[100, 1000] {
        group.bench_with_input(BenchmarkId::new("GEODIST", size), &size, |b, &size| {
            let handler = make_handler(&rt);
            // Setup: populate points
            rt.block_on(async {
                let mut args: Vec<Vec<u8>> = vec![b"geodist:key".to_vec()];
                for i in 0..size {
                    let lon = -74.0 + (i as f64 * 0.001) % 1.0;
                    let lat = 40.0 + (i as f64 * 0.0007) % 1.0;
                    args.push(format!("{:.6}", lon).into_bytes());
                    args.push(format!("{:.6}", lat).into_bytes());
                    args.push(format!("place:{}", i).into_bytes());
                }
                let cmd = Command {
                    name: "geoadd".to_string(),
                    args,
                };
                handler.process(cmd, 0).await.unwrap();
            });
            b.iter(|| {
                rt.block_on(async {
                    for i in 0..50 {
                        let cmd = Command {
                            name: "geodist".to_string(),
                            args: vec![
                                b"geodist:key".to_vec(),
                                format!("place:{}", i).into_bytes(),
                                format!("place:{}", i + 1).into_bytes(),
                                b"km".to_vec(),
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

fn bench_geosearch(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Geospatial GEOSEARCH");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &size in &[100, 1000, 5000] {
        // Setup: populate points spread across ~1 degree
        let handler = make_handler(&rt);
        rt.block_on(async {
            let mut args: Vec<Vec<u8>> = vec![b"geosearch:key".to_vec()];
            for i in 0..size {
                let lon = -74.0 + (i as f64 * 0.001) % 1.0;
                let lat = 40.0 + (i as f64 * 0.0007) % 1.0;
                args.push(format!("{:.6}", lon).into_bytes());
                args.push(format!("{:.6}", lat).into_bytes());
                args.push(format!("place:{}", i).into_bytes());
            }
            let cmd = Command {
                name: "geoadd".to_string(),
                args,
            };
            handler.process(cmd, 0).await.unwrap();
        });

        // Small radius (~1km, few results)
        group.bench_with_input(BenchmarkId::new("BYRADIUS small", size), &size, |b, _| {
            let handler = handler.clone();
            b.iter(|| {
                rt.block_on(async {
                    let cmd = Command {
                        name: "geosearch".to_string(),
                        args: vec![
                            b"geosearch:key".to_vec(),
                            b"FROMLONLAT".to_vec(),
                            b"-73.5".to_vec(),
                            b"40.5".to_vec(),
                            b"BYRADIUS".to_vec(),
                            b"1".to_vec(),
                            b"km".to_vec(),
                        ],
                    };
                    black_box(handler.process(cmd, 0).await.unwrap());
                })
            });
        });

        // Large radius (~50km, many results)
        group.bench_with_input(BenchmarkId::new("BYRADIUS large", size), &size, |b, _| {
            let handler = handler.clone();
            b.iter(|| {
                rt.block_on(async {
                    let cmd = Command {
                        name: "geosearch".to_string(),
                        args: vec![
                            b"geosearch:key".to_vec(),
                            b"FROMLONLAT".to_vec(),
                            b"-73.5".to_vec(),
                            b"40.5".to_vec(),
                            b"BYRADIUS".to_vec(),
                            b"50".to_vec(),
                            b"km".to_vec(),
                        ],
                    };
                    black_box(handler.process(cmd, 0).await.unwrap());
                })
            });
        });
    }
    group.finish();
}

fn bench_geopos(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Geospatial GEOPOS");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    let handler = make_handler(&rt);
    rt.block_on(async {
        let mut args: Vec<Vec<u8>> = vec![b"geopos:key".to_vec()];
        for i in 0..1000 {
            let lon = -74.0 + (i as f64 * 0.001) % 1.0;
            let lat = 40.0 + (i as f64 * 0.0007) % 1.0;
            args.push(format!("{:.6}", lon).into_bytes());
            args.push(format!("{:.6}", lat).into_bytes());
            args.push(format!("place:{}", i).into_bytes());
        }
        let cmd = Command {
            name: "geoadd".to_string(),
            args,
        };
        handler.process(cmd, 0).await.unwrap();
    });

    for &batch in &[1, 10, 50] {
        group.bench_with_input(BenchmarkId::new("GEOPOS", batch), &batch, |b, &batch| {
            let handler = handler.clone();
            b.iter(|| {
                rt.block_on(async {
                    let mut args: Vec<Vec<u8>> = vec![b"geopos:key".to_vec()];
                    for i in 0..batch {
                        args.push(format!("place:{}", i).into_bytes());
                    }
                    let cmd = Command {
                        name: "geopos".to_string(),
                        args,
                    };
                    black_box(handler.process(cmd, 0).await.unwrap());
                })
            });
        });
    }
    group.finish();
}

// --- Bitmap benchmarks ---

fn bench_bitmap_setbit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Bitmap SETBIT");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &max_offset in &[1_000, 10_000, 100_000] {
        group.bench_with_input(
            BenchmarkId::new("sequential", format!("{}bits", max_offset)),
            &max_offset,
            |b, &max_offset| {
                let handler = make_handler(&rt);
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..100 {
                            let offset = (i * max_offset / 100) as u64;
                            let cmd = Command {
                                name: "setbit".to_string(),
                                args: vec![
                                    b"bitmap:seq".to_vec(),
                                    offset.to_string().into_bytes(),
                                    b"1".to_vec(),
                                ],
                            };
                            black_box(handler.process(cmd, 0).await.unwrap());
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_bitmap_getbit(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Bitmap GETBIT");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &max_offset in &[1_000, 10_000, 100_000] {
        let handler = make_handler(&rt);
        // Setup: set some bits
        rt.block_on(async {
            for i in 0..max_offset {
                if i % 3 == 0 {
                    let cmd = Command {
                        name: "setbit".to_string(),
                        args: vec![
                            b"bitmap:get".to_vec(),
                            i.to_string().into_bytes(),
                            b"1".to_vec(),
                        ],
                    };
                    handler.process(cmd, 0).await.unwrap();
                }
            }
        });

        group.bench_with_input(
            BenchmarkId::new("GETBIT", format!("{}bits", max_offset)),
            &max_offset,
            |b, &max_offset| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        for i in 0..100 {
                            let offset = (i * max_offset / 100) as u64;
                            let cmd = Command {
                                name: "getbit".to_string(),
                                args: vec![b"bitmap:get".to_vec(), offset.to_string().into_bytes()],
                            };
                            black_box(handler.process(cmd, 0).await.unwrap());
                        }
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_bitmap_bitcount(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Bitmap BITCOUNT");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &size_bytes in &[1024, 10_240, 102_400] {
        let handler = make_handler(&rt);
        // Setup: create a string value of given size
        rt.block_on(async {
            let val = vec![0xAA_u8; size_bytes]; // alternating bits
            let cmd = Command {
                name: "set".to_string(),
                args: vec![b"bitcount:key".to_vec(), val],
            };
            handler.process(cmd, 0).await.unwrap();
        });

        group.bench_with_input(
            BenchmarkId::new("full", format!("{}B", size_bytes)),
            &size_bytes,
            |b, _| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "bitcount".to_string(),
                            args: vec![b"bitcount:key".to_vec()],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_bitmap_bitop(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Bitmap BITOP");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &size_bytes in &[1024, 10_240, 102_400] {
        let handler = make_handler(&rt);
        // Setup: create two string values
        rt.block_on(async {
            let val1 = vec![0xAA_u8; size_bytes];
            let val2 = vec![0x55_u8; size_bytes];
            let cmd1 = Command {
                name: "set".to_string(),
                args: vec![b"bitop:src1".to_vec(), val1],
            };
            let cmd2 = Command {
                name: "set".to_string(),
                args: vec![b"bitop:src2".to_vec(), val2],
            };
            handler.process(cmd1, 0).await.unwrap();
            handler.process(cmd2, 0).await.unwrap();
        });

        for op in &["AND", "OR", "XOR"] {
            group.bench_with_input(
                BenchmarkId::new(*op, format!("{}B", size_bytes)),
                &size_bytes,
                |b, _| {
                    let handler = handler.clone();
                    b.iter(|| {
                        rt.block_on(async {
                            let cmd = Command {
                                name: "bitop".to_string(),
                                args: vec![
                                    op.as_bytes().to_vec(),
                                    b"bitop:dest".to_vec(),
                                    b"bitop:src1".to_vec(),
                                    b"bitop:src2".to_vec(),
                                ],
                            };
                            black_box(handler.process(cmd, 0).await.unwrap());
                        })
                    });
                },
            );
        }
    }
    group.finish();
}

fn bench_bitmap_bitpos(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Bitmap BITPOS");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &size_bytes in &[1024, 10_240, 102_400] {
        let handler = make_handler(&rt);
        // Setup: mostly zeros with a few bits set at the end
        rt.block_on(async {
            let mut val = vec![0x00_u8; size_bytes];
            val[size_bytes - 1] = 0x01; // last bit set
            let cmd = Command {
                name: "set".to_string(),
                args: vec![b"bitpos:key".to_vec(), val],
            };
            handler.process(cmd, 0).await.unwrap();
        });

        group.bench_with_input(
            BenchmarkId::new("find 1", format!("{}B", size_bytes)),
            &size_bytes,
            |b, _| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let cmd = Command {
                            name: "bitpos".to_string(),
                            args: vec![b"bitpos:key".to_vec(), b"1".to_vec()],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

// --- HyperLogLog benchmarks ---

fn bench_hll_pfadd(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("HyperLogLog PFADD");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    for &count in &[100, 1000, 10_000] {
        group.bench_with_input(BenchmarkId::new("PFADD", count), &count, |b, &count| {
            let handler = make_handler(&rt);
            b.iter(|| {
                rt.block_on(async {
                    for i in 0..count {
                        let cmd = Command {
                            name: "pfadd".to_string(),
                            args: vec![b"hll:add".to_vec(), format!("element:{}", i).into_bytes()],
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    }
                })
            });
        });
    }
    group.finish();
}

fn bench_hll_pfcount(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("HyperLogLog PFCOUNT");
    group.sample_size(30);
    group.measurement_time(Duration::from_secs(3));

    // Single key with varying cardinality
    for &count in &[100, 1000, 10_000] {
        let handler = make_handler(&rt);
        rt.block_on(async {
            for i in 0..count {
                let cmd = Command {
                    name: "pfadd".to_string(),
                    args: vec![b"hll:count".to_vec(), format!("elem:{}", i).into_bytes()],
                };
                handler.process(cmd, 0).await.unwrap();
            }
        });

        group.bench_with_input(BenchmarkId::new("single key", count), &count, |b, _| {
            let handler = handler.clone();
            b.iter(|| {
                rt.block_on(async {
                    let cmd = Command {
                        name: "pfcount".to_string(),
                        args: vec![b"hll:count".to_vec()],
                    };
                    black_box(handler.process(cmd, 0).await.unwrap());
                })
            });
        });
    }

    // Multiple keys union count
    for &num_keys in &[2, 5, 10] {
        let handler = make_handler(&rt);
        rt.block_on(async {
            for k in 0..num_keys {
                for i in 0..1000 {
                    let cmd = Command {
                        name: "pfadd".to_string(),
                        args: vec![
                            format!("hll:multi:{}", k).into_bytes(),
                            format!("elem:{}:{}", k, i).into_bytes(),
                        ],
                    };
                    handler.process(cmd, 0).await.unwrap();
                }
            }
        });

        group.bench_with_input(
            BenchmarkId::new("multi key", format!("{}keys", num_keys)),
            &num_keys,
            |b, &num_keys| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let args: Vec<Vec<u8>> = (0..num_keys)
                            .map(|k| format!("hll:multi:{}", k).into_bytes())
                            .collect();
                        let cmd = Command {
                            name: "pfcount".to_string(),
                            args,
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

fn bench_hll_pfmerge(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("HyperLogLog PFMERGE");
    group.sample_size(20);
    group.measurement_time(Duration::from_secs(5));

    for &num_sources in &[2, 5, 10] {
        let handler = make_handler(&rt);
        rt.block_on(async {
            for k in 0..num_sources {
                for i in 0..1000 {
                    let cmd = Command {
                        name: "pfadd".to_string(),
                        args: vec![
                            format!("hll:merge:src:{}", k).into_bytes(),
                            format!("mergeelem:{}:{}", k, i).into_bytes(),
                        ],
                    };
                    handler.process(cmd, 0).await.unwrap();
                }
            }
        });

        group.bench_with_input(
            BenchmarkId::new("PFMERGE", format!("{}sources", num_sources)),
            &num_sources,
            |b, &num_sources| {
                let handler = handler.clone();
                b.iter(|| {
                    rt.block_on(async {
                        let mut args: Vec<Vec<u8>> = vec![b"hll:merge:dest".to_vec()];
                        for k in 0..num_sources {
                            args.push(format!("hll:merge:src:{}", k).into_bytes());
                        }
                        let cmd = Command {
                            name: "pfmerge".to_string(),
                            args,
                        };
                        black_box(handler.process(cmd, 0).await.unwrap());
                    })
                });
            },
        );
    }
    group.finish();
}

criterion_group!(
    geo_benches,
    bench_geoadd,
    bench_geodist,
    bench_geosearch,
    bench_geopos
);
criterion_group!(
    bitmap_benches,
    bench_bitmap_setbit,
    bench_bitmap_getbit,
    bench_bitmap_bitcount,
    bench_bitmap_bitop,
    bench_bitmap_bitpos
);
criterion_group!(
    hll_benches,
    bench_hll_pfadd,
    bench_hll_pfcount,
    bench_hll_pfmerge
);
criterion_main!(geo_benches, bitmap_benches, hll_benches);
