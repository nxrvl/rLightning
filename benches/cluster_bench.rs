use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use tokio::time::Duration;

use rlightning::cluster::slot::{crc16, key_hash_slot};

// --- Hash slot calculation benchmarks ---

fn bench_slot_calculation(c: &mut Criterion) {
    let mut group = c.benchmark_group("Cluster Slot Calculation");
    group.sample_size(100);
    group.measurement_time(Duration::from_secs(3));

    // CRC16 raw computation
    group.bench_function("CRC16 short key", |b| {
        b.iter(|| {
            black_box(crc16(b"mykey"));
        });
    });

    group.bench_function("CRC16 long key", |b| {
        let key = "a".repeat(256);
        b.iter(|| {
            black_box(crc16(key.as_bytes()));
        });
    });

    // key_hash_slot (includes hash tag extraction)
    group.bench_function("key_hash_slot simple", |b| {
        b.iter(|| {
            black_box(key_hash_slot(b"user:1001"));
        });
    });

    group.bench_function("key_hash_slot with hash tag", |b| {
        b.iter(|| {
            black_box(key_hash_slot(b"{user}.name"));
        });
    });

    // Batch slot calculation
    for &count in &[100, 1000, 10000] {
        group.bench_with_input(
            BenchmarkId::new("Batch slot calc", count),
            &count,
            |b, &count| {
                let keys: Vec<String> = (0..count).map(|i| format!("key:{}", i)).collect();
                b.iter(|| {
                    for key in &keys {
                        black_box(key_hash_slot(key.as_bytes()));
                    }
                });
            },
        );
    }

    group.finish();
}

// --- Cluster topology benchmarks ---

fn bench_cluster_topology(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("Cluster Topology");
    group.sample_size(50);
    group.measurement_time(Duration::from_secs(3));

    use rlightning::cluster::slot::SlotRange;

    // SlotRange operations
    group.bench_function("SlotRange contains", |b| {
        let range = SlotRange::new(0, 5460);
        b.iter(|| {
            for slot in 0..16384u16 {
                black_box(range.contains(slot));
            }
        });
    });

    // Cluster manager initialization and keyslot
    group.bench_function("ClusterManager keyslot", |b| {
        use rlightning::cluster::{ClusterConfig, ClusterManager};
        use rlightning::storage::engine::{StorageConfig, StorageEngine};
        use std::sync::Arc;

        let _guard = rt.enter();
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let cluster_config = ClusterConfig::default();
        let cluster = ClusterManager::new(Arc::clone(&storage), cluster_config);
        b.iter(|| {
            for i in 0..1000 {
                let key = format!("key:{}", i);
                black_box(cluster.keyslot(key.as_bytes()));
            }
        });
    });

    // Slot range lookups with varying node counts
    for &node_count in &[3, 6, 12] {
        group.bench_with_input(
            BenchmarkId::new("Slot lookup across nodes", node_count),
            &node_count,
            |b, &node_count| {
                let slots_per_node = 16384 / node_count;
                let ranges: Vec<SlotRange> = (0..node_count)
                    .map(|i| {
                        let start = (i * slots_per_node) as u16;
                        let end = if i == node_count - 1 {
                            16383
                        } else {
                            ((i + 1) * slots_per_node - 1) as u16
                        };
                        SlotRange::new(start, end)
                    })
                    .collect();
                b.iter(|| {
                    for slot in 0..16384u16 {
                        for range in &ranges {
                            if range.contains(slot) {
                                black_box(true);
                                break;
                            }
                        }
                    }
                });
            },
        );
    }

    // Count keys in slot (via ClusterManager)
    group.bench_function("count_keys_in_slot", |b| {
        use rlightning::cluster::{ClusterConfig, ClusterManager};
        use rlightning::storage::engine::{StorageConfig, StorageEngine};
        use std::sync::Arc;

        let _guard = rt.enter();
        let config = StorageConfig {
            max_memory: 128 * 1024 * 1024,
            ..Default::default()
        };
        let storage = StorageEngine::new(config);
        let handler =
            rlightning::command::handler::CommandHandler::new(Arc::clone(&storage));
        let cluster_config = ClusterConfig {
            enabled: true,
            ..Default::default()
        };
        let cluster = ClusterManager::new(Arc::clone(&storage), cluster_config);

        // Pre-populate some data
        rt.block_on(async {
            cluster
                .init(std::net::SocketAddr::from(([127, 0, 0, 1], 7000)))
                .await;
            // Add all slots so we can count keys
            let all_slots: Vec<u16> = (0..16384).collect();
            let _ = cluster.add_slots(&all_slots).await;
            for i in 0..200 {
                let cmd = rlightning::command::Command {
                    name: "set".to_string(),
                    args: vec![
                        format!("slotkey:{}", i).into_bytes(),
                        format!("val:{}", i).into_bytes(),
                    ],
                };
                handler.process(cmd).await.unwrap();
            }
        });

        b.iter(|| {
            rt.block_on(async {
                // Count keys in a few different slots
                for slot in [0u16, 100, 1000, 5000, 10000, 16000] {
                    black_box(cluster.count_keys_in_slot(slot).await.unwrap());
                }
            });
        });
    });

    group.finish();
}

criterion_group!(slot_benches, bench_slot_calculation);
criterion_group!(topology_benches, bench_cluster_topology);

criterion_main!(slot_benches, topology_benches);
