//! Benchmark validation tests
//!
//! These tests verify that the benchmark infrastructure works correctly by
//! exercising the same code paths used in benchmarks. They don't measure
//! performance but ensure benchmarks can complete without errors.

use std::sync::Arc;
use rlightning::command::handler::CommandHandler;
use rlightning::command::Command;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

fn make_handler() -> Arc<CommandHandler> {
    let config = StorageConfig {
        max_memory: 128 * 1024 * 1024,
        max_value_size: 256 * 1024,
        ..Default::default()
    };
    let storage = StorageEngine::new(config);
    Arc::new(CommandHandler::new(storage))
}

#[tokio::test]
async fn test_bench_string_operations_complete() {
    let handler = make_handler();

    // SET with varying value sizes
    for size in [64, 1024, 10_240, 102_400] {
        let cmd = Command {
            name: "set".to_string(),
            args: vec![
                format!("bench_str:{}", size).into_bytes(),
                vec![b'x'; size],
            ],
        };
        handler.process(cmd).await.unwrap();
    }

    // GET
    for size in [64, 1024, 10_240, 102_400] {
        let cmd = Command {
            name: "get".to_string(),
            args: vec![format!("bench_str:{}", size).into_bytes()],
        };
        handler.process(cmd).await.unwrap();
    }

    // MSET/MGET
    let mut mset_args = Vec::new();
    for i in 0..10 {
        mset_args.push(format!("msetkey:{}", i).into_bytes());
        mset_args.push(format!("val:{}", i).into_bytes());
    }
    handler.process(Command { name: "mset".to_string(), args: mset_args }).await.unwrap();

    let mget_args: Vec<Vec<u8>> = (0..10).map(|i| format!("msetkey:{}", i).into_bytes()).collect();
    handler.process(Command { name: "mget".to_string(), args: mget_args }).await.unwrap();

    // INCR
    handler.process(Command { name: "incr".to_string(), args: vec![b"counter".to_vec()] }).await.unwrap();

    // APPEND
    handler.process(Command {
        name: "append".to_string(),
        args: vec![b"appendkey".to_vec(), b"data".to_vec()],
    }).await.unwrap();
}

#[tokio::test]
async fn test_bench_list_operations_complete() {
    let handler = make_handler();

    // LPUSH/RPUSH
    for i in 0..50 {
        handler.process(Command {
            name: "lpush".to_string(),
            args: vec![b"benchlist".to_vec(), format!("item:{}", i).into_bytes()],
        }).await.unwrap();
    }
    for i in 0..50 {
        handler.process(Command {
            name: "rpush".to_string(),
            args: vec![b"benchlist2".to_vec(), format!("item:{}", i).into_bytes()],
        }).await.unwrap();
    }

    // LPOP/RPOP
    handler.process(Command { name: "lpop".to_string(), args: vec![b"benchlist".to_vec()] }).await.unwrap();
    handler.process(Command { name: "rpop".to_string(), args: vec![b"benchlist".to_vec()] }).await.unwrap();

    // LRANGE
    handler.process(Command {
        name: "lrange".to_string(),
        args: vec![b"benchlist".to_vec(), b"0".to_vec(), b"-1".to_vec()],
    }).await.unwrap();
}

#[tokio::test]
async fn test_bench_hash_operations_complete() {
    let handler = make_handler();

    for i in 0..100 {
        handler.process(Command {
            name: "hset".to_string(),
            args: vec![
                b"benchhash".to_vec(),
                format!("field:{}", i).into_bytes(),
                format!("value:{}", i).into_bytes(),
            ],
        }).await.unwrap();
    }

    handler.process(Command {
        name: "hget".to_string(),
        args: vec![b"benchhash".to_vec(), b"field:50".to_vec()],
    }).await.unwrap();

    handler.process(Command {
        name: "hgetall".to_string(),
        args: vec![b"benchhash".to_vec()],
    }).await.unwrap();
}

#[tokio::test]
async fn test_bench_set_operations_complete() {
    let handler = make_handler();

    // SADD to two sets
    for i in 0..100 {
        handler.process(Command {
            name: "sadd".to_string(),
            args: vec![b"benchset1".to_vec(), format!("member:{}", i).into_bytes()],
        }).await.unwrap();
        handler.process(Command {
            name: "sadd".to_string(),
            args: vec![b"benchset2".to_vec(), format!("member:{}", i + 50).into_bytes()],
        }).await.unwrap();
    }

    // SINTER
    handler.process(Command {
        name: "sinter".to_string(),
        args: vec![b"benchset1".to_vec(), b"benchset2".to_vec()],
    }).await.unwrap();

    // SUNION
    handler.process(Command {
        name: "sunion".to_string(),
        args: vec![b"benchset1".to_vec(), b"benchset2".to_vec()],
    }).await.unwrap();
}

#[tokio::test]
async fn test_bench_sorted_set_operations_complete() {
    let handler = make_handler();

    for i in 0..100 {
        handler.process(Command {
            name: "zadd".to_string(),
            args: vec![
                b"benchzset".to_vec(),
                format!("{}", i as f64).into_bytes(),
                format!("member:{}", i).into_bytes(),
            ],
        }).await.unwrap();
    }

    // ZRANGE
    handler.process(Command {
        name: "zrange".to_string(),
        args: vec![b"benchzset".to_vec(), b"0".to_vec(), b"-1".to_vec()],
    }).await.unwrap();

    // ZRANGEBYSCORE
    handler.process(Command {
        name: "zrangebyscore".to_string(),
        args: vec![b"benchzset".to_vec(), b"25".to_vec(), b"75".to_vec()],
    }).await.unwrap();
}

#[tokio::test]
async fn test_bench_stream_operations_complete() {
    let handler = make_handler();

    // XADD
    for i in 0..50 {
        handler.process(Command {
            name: "xadd".to_string(),
            args: vec![
                b"benchstream".to_vec(),
                b"*".to_vec(),
                b"field".to_vec(),
                format!("value:{}", i).into_bytes(),
            ],
        }).await.unwrap();
    }

    // XLEN
    handler.process(Command {
        name: "xlen".to_string(),
        args: vec![b"benchstream".to_vec()],
    }).await.unwrap();

    // XRANGE
    handler.process(Command {
        name: "xrange".to_string(),
        args: vec![b"benchstream".to_vec(), b"-".to_vec(), b"+".to_vec()],
    }).await.unwrap();
}

#[tokio::test]
async fn test_bench_pubsub_operations_complete() {
    use rlightning::pubsub::PubSubManager;

    let pubsub = PubSubManager::new();
    let (id, _rx) = pubsub.register_client().await;
    pubsub.subscribe(id, vec![b"benchchannel".to_vec()]).await;

    // Publish
    let recipients = pubsub.publish(b"benchchannel".to_vec(), b"hello".to_vec()).await;
    assert_eq!(recipients, 1);

    pubsub.unregister_client(id).await;
}

#[tokio::test]
async fn test_bench_transaction_operations_complete() {
    use rlightning::command::transaction::{TransactionState, handle_multi, handle_exec, queue_command};

    let handler = make_handler();
    let config = StorageConfig {
        max_memory: 128 * 1024 * 1024,
        ..Default::default()
    };
    let storage = StorageEngine::new(config);

    let mut tx_state = TransactionState::new();

    // MULTI
    let result = handle_multi(&mut tx_state);
    assert!(result.is_ok());

    // Queue commands
    for i in 0..5 {
        let cmd = Command {
            name: "set".to_string(),
            args: vec![
                format!("txkey:{}", i).into_bytes(),
                format!("txval:{}", i).into_bytes(),
            ],
        };
        let result = queue_command(&mut tx_state, cmd);
        assert!(result.is_ok());
    }

    // EXEC
    let result = handle_exec(&mut tx_state, &handler, &storage).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_bench_scripting_operations_complete() {
    let handler = make_handler();

    // Simple EVAL
    handler.process(Command {
        name: "eval".to_string(),
        args: vec![b"return 1".to_vec(), b"0".to_vec()],
    }).await.unwrap();

    // EVAL with redis.call
    handler.process(Command {
        name: "eval".to_string(),
        args: vec![
            b"redis.call('SET', KEYS[1], ARGV[1]); return redis.call('GET', KEYS[1])".to_vec(),
            b"1".to_vec(),
            b"scriptkey".to_vec(),
            b"scriptval".to_vec(),
        ],
    }).await.unwrap();

    // SCRIPT LOAD + EVALSHA
    let result = handler.process(Command {
        name: "script".to_string(),
        args: vec![b"load".to_vec(), b"return 42".to_vec()],
    }).await.unwrap();

    if let rlightning::networking::resp::RespValue::BulkString(Some(sha)) = result {
        handler.process(Command {
            name: "evalsha".to_string(),
            args: vec![sha, b"0".to_vec()],
        }).await.unwrap();
    }
}

#[tokio::test]
async fn test_bench_persistence_operations_complete() {
    use rlightning::persistence::rdb::RdbPersistence;

    let config = StorageConfig {
        max_memory: 64 * 1024 * 1024,
        ..Default::default()
    };
    let storage = StorageEngine::new(config);
    let handler = CommandHandler::new(Arc::clone(&storage));

    // Populate data
    for i in 0..100 {
        handler.process(Command {
            name: "set".to_string(),
            args: vec![
                format!("rdbkey:{}", i).into_bytes(),
                format!("rdbval:{}", i).into_bytes(),
            ],
        }).await.unwrap();
    }

    // RDB save
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let rdb = RdbPersistence::new(Arc::clone(&storage), tmp.path().to_path_buf());
    rdb.save().await.unwrap();

    // RDB load into fresh storage
    let fresh_config = StorageConfig {
        max_memory: 64 * 1024 * 1024,
        ..Default::default()
    };
    let fresh_storage = StorageEngine::new(fresh_config);
    let fresh_rdb = RdbPersistence::new(fresh_storage, tmp.path().to_path_buf());
    fresh_rdb.load().await.unwrap();
}

#[tokio::test]
async fn test_bench_aof_operations_complete() {
    use rlightning::persistence::aof::AofPersistence;
    use rlightning::persistence::config::AofSyncPolicy;
    use rlightning::networking::resp::RespCommand;

    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    let tmp = tempfile::NamedTempFile::new().unwrap();
    let aof = AofPersistence::new(storage, tmp.path().to_path_buf());

    for i in 0..50 {
        let resp_cmd = RespCommand {
            name: b"SET".to_vec(),
            args: vec![
                format!("aofkey:{}", i).into_bytes(),
                format!("aofval:{}", i).into_bytes(),
            ],
        };
        aof.append_command(resp_cmd, AofSyncPolicy::None).await.unwrap();
    }
}

#[tokio::test]
async fn test_bench_blocking_operations_complete() {
    let handler = make_handler();

    // Push then BLPOP (should return immediately)
    handler.process(Command {
        name: "lpush".to_string(),
        args: vec![b"blpoplist".to_vec(), b"item".to_vec()],
    }).await.unwrap();

    handler.process(Command {
        name: "blpop".to_string(),
        args: vec![b"blpoplist".to_vec(), b"0".to_vec()],
    }).await.unwrap();

    // Push then BRPOP
    handler.process(Command {
        name: "rpush".to_string(),
        args: vec![b"brpoplist".to_vec(), b"item".to_vec()],
    }).await.unwrap();

    handler.process(Command {
        name: "brpop".to_string(),
        args: vec![b"brpoplist".to_vec(), b"0".to_vec()],
    }).await.unwrap();
}

#[tokio::test]
async fn test_bench_cluster_operations_complete() {
    use rlightning::cluster::slot::{crc16, key_hash_slot, SlotRange};

    // CRC16
    let crc = crc16(b"mykey");
    assert!(crc > 0);

    // key_hash_slot
    let slot = key_hash_slot(b"user:1001");
    assert!(slot < 16384);

    // Hash tag
    let slot_tagged = key_hash_slot(b"{user}.name");
    let slot_tag_only = key_hash_slot(b"user");
    assert_eq!(slot_tagged, slot_tag_only);

    // SlotRange
    let range = SlotRange::new(0, 5460);
    assert!(range.contains(0));
    assert!(range.contains(5460));
    assert!(!range.contains(5461));
}

#[tokio::test]
async fn test_bench_concurrent_clients_complete() {
    let handler = make_handler();

    // Pre-populate
    for i in 0..10 {
        handler.process(Command {
            name: "set".to_string(),
            args: vec![
                format!("conckey:{}", i).into_bytes(),
                format!("concval:{}", i).into_bytes(),
            ],
        }).await.unwrap();
    }

    // Simulate concurrent access
    let mut handles = Vec::new();
    for i in 0..10 {
        let h = handler.clone();
        let handle = tokio::spawn(async move {
            h.process(Command {
                name: "set".to_string(),
                args: vec![
                    format!("clientkey:{}", i).into_bytes(),
                    format!("clientval:{}", i).into_bytes(),
                ],
            }).await.unwrap();
            h.process(Command {
                name: "get".to_string(),
                args: vec![format!("conckey:{}", i % 10).into_bytes()],
            }).await.unwrap();
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_bench_memory_efficiency_complete() {
    let config = StorageConfig {
        max_memory: 64 * 1024 * 1024,
        ..Default::default()
    };
    let storage = StorageEngine::new(config);
    let handler = CommandHandler::new(Arc::clone(&storage));

    // Store different data types
    for i in 0..50 {
        handler.process(Command {
            name: "set".to_string(),
            args: vec![format!("memstr:{}", i).into_bytes(), format!("val:{}", i).into_bytes()],
        }).await.unwrap();
        handler.process(Command {
            name: "hset".to_string(),
            args: vec![b"memhash".to_vec(), format!("f:{}", i).into_bytes(), format!("v:{}", i).into_bytes()],
        }).await.unwrap();
        handler.process(Command {
            name: "lpush".to_string(),
            args: vec![b"memlist".to_vec(), format!("item:{}", i).into_bytes()],
        }).await.unwrap();
        handler.process(Command {
            name: "sadd".to_string(),
            args: vec![b"memset".to_vec(), format!("member:{}", i).into_bytes()],
        }).await.unwrap();
        handler.process(Command {
            name: "zadd".to_string(),
            args: vec![b"memzset".to_vec(), format!("{}", i as f64).into_bytes(), format!("m:{}", i).into_bytes()],
        }).await.unwrap();
    }
}

#[test]
fn test_benchmark_comparison_script_exists() {
    let script_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("benchmark_comparison.sh");
    assert!(script_path.exists(), "benchmark_comparison.sh should exist");

    // Verify it's executable (on Unix)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let metadata = std::fs::metadata(&script_path).unwrap();
        let permissions = metadata.permissions();
        assert!(permissions.mode() & 0o111 != 0, "script should be executable");
    }
}

#[test]
fn test_all_bench_files_exist() {
    let bench_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("benches");

    let expected_benches = [
        "storage_bench.rs",
        "protocol_bench.rs",
        "throughput_bench.rs",
        "redis_comparison_bench.rs",
        "replication_bench.rs",
        "auth_bench.rs",
        "command_category_bench.rs",
        "advanced_features_bench.rs",
        "cluster_bench.rs",
    ];

    for bench in &expected_benches {
        let path = bench_dir.join(bench);
        assert!(path.exists(), "Benchmark file {} should exist", bench);
    }
}
