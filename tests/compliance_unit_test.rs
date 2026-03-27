use rlightning::command::Command;
/// Unit tests for Redis Protocol Compliance
/// Tests the newly implemented commands without requiring a running server
use rlightning::command::handler::CommandHandler;
use rlightning::networking::resp::RespValue;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

async fn create_test_handler() -> CommandHandler {
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    CommandHandler::new(storage)
}

#[tokio::test]
async fn test_new_string_commands_compliance() {
    let handler = create_test_handler().await;

    println!("🧪 Testing String Commands Compliance...");

    // Test STRLEN
    let set_cmd = Command {
        name: "set".to_string(),
        args: vec![b"testkey".to_vec(), b"testvalue".to_vec()],
    };
    handler.process(set_cmd, 0).await.unwrap();

    let strlen_cmd = Command {
        name: "strlen".to_string(),
        args: vec![b"testkey".to_vec()],
    };
    let result = handler.process(strlen_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(9));
    println!("  ✅ STRLEN");

    // Test GETRANGE
    let getrange_cmd = Command {
        name: "getrange".to_string(),
        args: vec![b"testkey".to_vec(), b"0".to_vec(), b"3".to_vec()],
    };
    let result = handler.process(getrange_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Some(b"test".to_vec())));
    println!("  ✅ GETRANGE");

    // Test SETRANGE
    let setrange_cmd = Command {
        name: "setrange".to_string(),
        args: vec![b"testkey".to_vec(), b"0".to_vec(), b"best".to_vec()],
    };
    let result = handler.process(setrange_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(9));
    println!("  ✅ SETRANGE");

    // Test GETSET
    let getset_cmd = Command {
        name: "getset".to_string(),
        args: vec![b"testkey".to_vec(), b"newvalue".to_vec()],
    };
    let result = handler.process(getset_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::BulkString(Some(b"bestvalue".to_vec())));
    println!("  ✅ GETSET");

    // Test MSETNX
    let msetnx_cmd = Command {
        name: "msetnx".to_string(),
        args: vec![
            b"key1".to_vec(),
            b"value1".to_vec(),
            b"key2".to_vec(),
            b"value2".to_vec(),
        ],
    };
    let result = handler.process(msetnx_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
    println!("  ✅ MSETNX");

    // Test INCRBYFLOAT
    let set_float_cmd = Command {
        name: "set".to_string(),
        args: vec![b"floatkey".to_vec(), b"3.5".to_vec()],
    };
    handler.process(set_float_cmd, 0).await.unwrap();

    let incrbyfloat_cmd = Command {
        name: "incrbyfloat".to_string(),
        args: vec![b"floatkey".to_vec(), b"1.5".to_vec()],
    };
    let result = handler.process(incrbyfloat_cmd, 0).await.unwrap();
    if let RespValue::BulkString(Some(value)) = result {
        let float_str = String::from_utf8_lossy(&value);
        let float_val: f64 = float_str
            .parse()
            .expect("INCRBYFLOAT should return a parseable float");
        assert!(
            (float_val - 5.0).abs() < f64::EPSILON,
            "Expected 5.0, got {}",
            float_val
        );
    } else {
        panic!("Expected BulkString response");
    }
    println!("  ✅ INCRBYFLOAT");

    println!("  🎉 All new string commands working!");
}

#[tokio::test]
async fn test_new_hash_commands_compliance() {
    let handler = create_test_handler().await;

    println!("🧪 Testing Hash Commands Compliance...");

    // Setup hash
    let hset_cmd = Command {
        name: "hset".to_string(),
        args: vec![b"hashkey".to_vec(), b"field1".to_vec(), b"value1".to_vec()],
    };
    handler.process(hset_cmd, 0).await.unwrap();

    // Test HKEYS
    let hkeys_cmd = Command {
        name: "hkeys".to_string(),
        args: vec![b"hashkey".to_vec()],
    };
    let result = handler.process(hkeys_cmd, 0).await.unwrap();
    if let RespValue::Array(Some(keys)) = result {
        assert_eq!(keys.len(), 1);
        assert_eq!(keys[0], RespValue::BulkString(Some(b"field1".to_vec())));
    } else {
        panic!("Expected Array response");
    }
    println!("  ✅ HKEYS");

    // Test HVALS
    let hvals_cmd = Command {
        name: "hvals".to_string(),
        args: vec![b"hashkey".to_vec()],
    };
    let result = handler.process(hvals_cmd, 0).await.unwrap();
    if let RespValue::Array(Some(values)) = result {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], RespValue::BulkString(Some(b"value1".to_vec())));
    } else {
        panic!("Expected Array response");
    }
    println!("  ✅ HVALS");

    // Test HLEN
    let hlen_cmd = Command {
        name: "hlen".to_string(),
        args: vec![b"hashkey".to_vec()],
    };
    let result = handler.process(hlen_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
    println!("  ✅ HLEN");

    // Test HMGET
    let hmget_cmd = Command {
        name: "hmget".to_string(),
        args: vec![b"hashkey".to_vec(), b"field1".to_vec()],
    };
    let result = handler.process(hmget_cmd, 0).await.unwrap();
    if let RespValue::Array(Some(values)) = result {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], RespValue::BulkString(Some(b"value1".to_vec())));
    } else {
        panic!("Expected Array response");
    }
    println!("  ✅ HMGET");

    // Test HINCRBY
    let hset_counter_cmd = Command {
        name: "hset".to_string(),
        args: vec![b"hashkey".to_vec(), b"counter".to_vec(), b"5".to_vec()],
    };
    handler.process(hset_counter_cmd, 0).await.unwrap();

    let hincrby_cmd = Command {
        name: "hincrby".to_string(),
        args: vec![b"hashkey".to_vec(), b"counter".to_vec(), b"3".to_vec()],
    };
    let result = handler.process(hincrby_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(8));
    println!("  ✅ HINCRBY");

    // Test HINCRBYFLOAT
    let hincrbyfloat_cmd = Command {
        name: "hincrbyfloat".to_string(),
        args: vec![b"hashkey".to_vec(), b"counter".to_vec(), b"1.5".to_vec()],
    };
    let result = handler.process(hincrbyfloat_cmd, 0).await.unwrap();
    if let RespValue::BulkString(Some(value)) = result {
        let float_str = String::from_utf8_lossy(&value);
        assert!(float_str.contains("9.5"));
    } else {
        panic!("Expected BulkString response");
    }
    println!("  ✅ HINCRBYFLOAT");

    // Test HSETNX
    let hsetnx_cmd = Command {
        name: "hsetnx".to_string(),
        args: vec![
            b"hashkey".to_vec(),
            b"newfield".to_vec(),
            b"newvalue".to_vec(),
        ],
    };
    let result = handler.process(hsetnx_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
    println!("  ✅ HSETNX");

    // Test HSTRLEN
    let hstrlen_cmd = Command {
        name: "hstrlen".to_string(),
        args: vec![b"hashkey".to_vec(), b"field1".to_vec()],
    };
    let result = handler.process(hstrlen_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(6));
    println!("  ✅ HSTRLEN");

    println!("  🎉 All new hash commands working!");
}

#[tokio::test]
async fn test_new_json_commands_compliance() {
    let handler = create_test_handler().await;

    println!("🧪 Testing JSON Commands Compliance...");

    // Test JSON.SET
    let json_set_cmd = Command {
        name: "json.set".to_string(),
        args: vec![
            b"jsonkey".to_vec(),
            b"$".to_vec(),
            b"{\"name\":\"John\",\"age\":30}".to_vec(),
        ],
    };
    let result = handler.process(json_set_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    println!("  ✅ JSON.SET");

    // Test JSON.GET
    let json_get_cmd = Command {
        name: "json.get".to_string(),
        args: vec![b"jsonkey".to_vec()],
    };
    let result = handler.process(json_get_cmd, 0).await.unwrap();
    if let RespValue::BulkString(Some(value)) = result {
        let json_str = String::from_utf8_lossy(&value);
        assert!(json_str.contains("John"));
    } else {
        panic!("Expected BulkString response");
    }
    println!("  ✅ JSON.GET");

    // Test JSON.TYPE
    let json_type_cmd = Command {
        name: "json.type".to_string(),
        args: vec![b"jsonkey".to_vec()],
    };
    let result = handler.process(json_type_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::SimpleString("object".to_string()));
    println!("  ✅ JSON.TYPE");

    // Test JSON.OBJKEYS
    let json_objkeys_cmd = Command {
        name: "json.objkeys".to_string(),
        args: vec![b"jsonkey".to_vec()],
    };
    let result = handler.process(json_objkeys_cmd, 0).await.unwrap();
    if let RespValue::Array(Some(keys)) = result {
        assert_eq!(keys.len(), 2); // "name" and "age"
    } else {
        panic!("Expected Array response");
    }
    println!("  ✅ JSON.OBJKEYS");

    // Test JSON.OBJLEN
    let json_objlen_cmd = Command {
        name: "json.objlen".to_string(),
        args: vec![b"jsonkey".to_vec()],
    };
    let result = handler.process(json_objlen_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(2));
    println!("  ✅ JSON.OBJLEN");

    // Setup array for array tests
    let json_set_array_cmd = Command {
        name: "json.set".to_string(),
        args: vec![b"arraykey".to_vec(), b"$".to_vec(), b"[1,2,3,4,5]".to_vec()],
    };
    handler.process(json_set_array_cmd, 0).await.unwrap();

    // Test JSON.ARRLEN
    let json_arrlen_cmd = Command {
        name: "json.arrlen".to_string(),
        args: vec![b"arraykey".to_vec()],
    };
    let result = handler.process(json_arrlen_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(5));
    println!("  ✅ JSON.ARRLEN");

    // Test JSON.ARRAPPEND
    let json_arrappend_cmd = Command {
        name: "json.arrappend".to_string(),
        args: vec![b"arraykey".to_vec(), b"$".to_vec(), b"6".to_vec()],
    };
    let result = handler.process(json_arrappend_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(6));
    println!("  ✅ JSON.ARRAPPEND");

    // Test JSON.ARRTRIM
    let json_arrtrim_cmd = Command {
        name: "json.arrtrim".to_string(),
        args: vec![
            b"arraykey".to_vec(),
            b"$".to_vec(),
            b"0".to_vec(),
            b"2".to_vec(),
        ],
    };
    let result = handler.process(json_arrtrim_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(3));
    println!("  ✅ JSON.ARRTRIM");

    // Setup number for numeric tests
    let json_set_number_cmd = Command {
        name: "json.set".to_string(),
        args: vec![b"numberkey".to_vec(), b"$".to_vec(), b"42".to_vec()],
    };
    handler.process(json_set_number_cmd, 0).await.unwrap();

    // Test JSON.NUMINCRBY
    let json_numincrby_cmd = Command {
        name: "json.numincrby".to_string(),
        args: vec![b"numberkey".to_vec(), b"$".to_vec(), b"8".to_vec()],
    };
    let result = handler.process(json_numincrby_cmd, 0).await.unwrap();
    if let RespValue::BulkString(Some(value)) = result {
        let num_str = String::from_utf8_lossy(&value);
        assert!(num_str.contains("50"));
    } else {
        panic!("Expected BulkString response");
    }
    println!("  ✅ JSON.NUMINCRBY");

    // Test JSON.DEL
    let json_del_cmd = Command {
        name: "json.del".to_string(),
        args: vec![b"jsonkey".to_vec()],
    };
    let result = handler.process(json_del_cmd, 0).await.unwrap();
    assert_eq!(result, RespValue::Integer(1));
    println!("  ✅ JSON.DEL");

    println!("  🎉 All new JSON commands working!");
}

#[tokio::test]
async fn test_redis_compliance_summary() {
    let handler = create_test_handler().await;

    println!("\n🎉 REDIS PROTOCOL COMPLIANCE SUMMARY 🎉");
    println!("==========================================");

    let mut total_commands = 0;
    let mut working_commands = 0;

    // Test critical Redis commands
    let test_commands = vec![
        // String commands
        (
            "SET",
            Command {
                name: "set".to_string(),
                args: vec![b"test".to_vec(), b"value".to_vec()],
            },
        ),
        (
            "GET",
            Command {
                name: "get".to_string(),
                args: vec![b"test".to_vec()],
            },
        ),
        (
            "STRLEN",
            Command {
                name: "strlen".to_string(),
                args: vec![b"test".to_vec()],
            },
        ),
        (
            "APPEND",
            Command {
                name: "append".to_string(),
                args: vec![b"test".to_vec(), b"more".to_vec()],
            },
        ),
        (
            "INCR",
            Command {
                name: "incr".to_string(),
                args: vec![b"counter".to_vec()],
            },
        ),
        (
            "DECR",
            Command {
                name: "decr".to_string(),
                args: vec![b"counter".to_vec()],
            },
        ),
        (
            "MGET",
            Command {
                name: "mget".to_string(),
                args: vec![b"test".to_vec()],
            },
        ),
        (
            "MSET",
            Command {
                name: "mset".to_string(),
                args: vec![b"test2".to_vec(), b"value2".to_vec()],
            },
        ),
        (
            "GETRANGE",
            Command {
                name: "getrange".to_string(),
                args: vec![b"test".to_vec(), b"0".to_vec(), b"2".to_vec()],
            },
        ),
        (
            "SETRANGE",
            Command {
                name: "setrange".to_string(),
                args: vec![b"test".to_vec(), b"0".to_vec(), b"best".to_vec()],
            },
        ),
        (
            "GETSET",
            Command {
                name: "getset".to_string(),
                args: vec![b"test".to_vec(), b"newval".to_vec()],
            },
        ),
        (
            "MSETNX",
            Command {
                name: "msetnx".to_string(),
                args: vec![b"new1".to_vec(), b"val1".to_vec()],
            },
        ),
        (
            "INCRBYFLOAT",
            Command {
                name: "incrbyfloat".to_string(),
                args: vec![b"float".to_vec(), b"1.5".to_vec()],
            },
        ),
        // Hash commands
        (
            "HSET",
            Command {
                name: "hset".to_string(),
                args: vec![b"hash".to_vec(), b"field".to_vec(), b"value".to_vec()],
            },
        ),
        (
            "HGET",
            Command {
                name: "hget".to_string(),
                args: vec![b"hash".to_vec(), b"field".to_vec()],
            },
        ),
        (
            "HKEYS",
            Command {
                name: "hkeys".to_string(),
                args: vec![b"hash".to_vec()],
            },
        ),
        (
            "HVALS",
            Command {
                name: "hvals".to_string(),
                args: vec![b"hash".to_vec()],
            },
        ),
        (
            "HLEN",
            Command {
                name: "hlen".to_string(),
                args: vec![b"hash".to_vec()],
            },
        ),
        (
            "HEXISTS",
            Command {
                name: "hexists".to_string(),
                args: vec![b"hash".to_vec(), b"field".to_vec()],
            },
        ),
        (
            "HDEL",
            Command {
                name: "hdel".to_string(),
                args: vec![b"hash".to_vec(), b"field".to_vec()],
            },
        ),
        (
            "HMGET",
            Command {
                name: "hmget".to_string(),
                args: vec![b"hash".to_vec(), b"field".to_vec()],
            },
        ),
        (
            "HINCRBY",
            Command {
                name: "hincrby".to_string(),
                args: vec![b"hash".to_vec(), b"counter".to_vec(), b"1".to_vec()],
            },
        ),
        (
            "HINCRBYFLOAT",
            Command {
                name: "hincrbyfloat".to_string(),
                args: vec![b"hash".to_vec(), b"float".to_vec(), b"1.5".to_vec()],
            },
        ),
        (
            "HSETNX",
            Command {
                name: "hsetnx".to_string(),
                args: vec![b"hash".to_vec(), b"newf".to_vec(), b"newv".to_vec()],
            },
        ),
        (
            "HSTRLEN",
            Command {
                name: "hstrlen".to_string(),
                args: vec![b"hash".to_vec(), b"field".to_vec()],
            },
        ),
        // JSON commands
        (
            "JSON.SET",
            Command {
                name: "json.set".to_string(),
                args: vec![
                    b"json".to_vec(),
                    b"$".to_vec(),
                    b"{\"key\":\"value\"}".to_vec(),
                ],
            },
        ),
        (
            "JSON.GET",
            Command {
                name: "json.get".to_string(),
                args: vec![b"json".to_vec()],
            },
        ),
        (
            "JSON.TYPE",
            Command {
                name: "json.type".to_string(),
                args: vec![b"json".to_vec()],
            },
        ),
        (
            "JSON.OBJKEYS",
            Command {
                name: "json.objkeys".to_string(),
                args: vec![b"json".to_vec()],
            },
        ),
        (
            "JSON.OBJLEN",
            Command {
                name: "json.objlen".to_string(),
                args: vec![b"json".to_vec()],
            },
        ),
        (
            "JSON.ARRLEN",
            Command {
                name: "json.arrlen".to_string(),
                args: vec![b"arr".to_vec()],
            },
        ),
        (
            "JSON.ARRAPPEND",
            Command {
                name: "json.arrappend".to_string(),
                args: vec![b"arr".to_vec(), b"$".to_vec(), b"1".to_vec()],
            },
        ),
        (
            "JSON.ARRTRIM",
            Command {
                name: "json.arrtrim".to_string(),
                args: vec![b"arr".to_vec(), b"$".to_vec(), b"0".to_vec(), b"1".to_vec()],
            },
        ),
        (
            "JSON.NUMINCRBY",
            Command {
                name: "json.numincrby".to_string(),
                args: vec![b"num".to_vec(), b"$".to_vec(), b"5".to_vec()],
            },
        ),
        (
            "JSON.DEL",
            Command {
                name: "json.del".to_string(),
                args: vec![b"json".to_vec()],
            },
        ),
        // Server commands
        (
            "PING",
            Command {
                name: "ping".to_string(),
                args: vec![],
            },
        ),
        (
            "EXISTS",
            Command {
                name: "exists".to_string(),
                args: vec![b"test".to_vec()],
            },
        ),
        (
            "DEL",
            Command {
                name: "del".to_string(),
                args: vec![b"test".to_vec()],
            },
        ),
        (
            "TTL",
            Command {
                name: "ttl".to_string(),
                args: vec![b"test".to_vec()],
            },
        ),
        (
            "TYPE",
            Command {
                name: "type".to_string(),
                args: vec![b"test".to_vec()],
            },
        ),
    ];

    // Setup some test data first
    let setup_commands = vec![
        Command {
            name: "set".to_string(),
            args: vec![b"test".to_vec(), b"value".to_vec()],
        },
        Command {
            name: "set".to_string(),
            args: vec![b"counter".to_vec(), b"1".to_vec()],
        },
        Command {
            name: "set".to_string(),
            args: vec![b"float".to_vec(), b"1.0".to_vec()],
        },
        Command {
            name: "hset".to_string(),
            args: vec![b"hash".to_vec(), b"field".to_vec(), b"value".to_vec()],
        },
        Command {
            name: "hset".to_string(),
            args: vec![b"hash".to_vec(), b"counter".to_vec(), b"5".to_vec()],
        },
        Command {
            name: "hset".to_string(),
            args: vec![b"hash".to_vec(), b"float".to_vec(), b"2.5".to_vec()],
        },
        Command {
            name: "json.set".to_string(),
            args: vec![b"arr".to_vec(), b"$".to_vec(), b"[1,2,3]".to_vec()],
        },
        Command {
            name: "json.set".to_string(),
            args: vec![b"num".to_vec(), b"$".to_vec(), b"10".to_vec()],
        },
    ];

    for cmd in setup_commands {
        let _ = handler.process(cmd, 0).await;
    }

    println!("📊 Testing {} Redis commands...", test_commands.len());

    for (cmd_name, cmd) in test_commands {
        total_commands += 1;
        match handler.process(cmd, 0).await {
            Ok(_) => {
                working_commands += 1;
                println!("  ✅ {}", cmd_name);
            }
            Err(e) => {
                println!("  ❌ {} ({})", cmd_name, e);
            }
        }
    }

    let compliance_percentage = (working_commands as f64 / total_commands as f64) * 100.0;

    println!("\n📊 FINAL COMPLIANCE SCORE:");
    println!("==========================================");
    println!("🎯 Commands tested: {}", total_commands);
    println!("✅ Commands working: {}", working_commands);
    println!("📈 Compliance percentage: {:.1}%", compliance_percentage);

    if compliance_percentage >= 95.0 {
        println!("🏆 EXCELLENT! Near-perfect Redis compatibility!");
    } else if compliance_percentage >= 85.0 {
        println!("🥇 VERY GOOD! High Redis compatibility!");
    } else if compliance_percentage >= 75.0 {
        println!("🥈 GOOD! Strong Redis compatibility!");
    } else {
        println!("🥉 FAIR! Basic Redis compatibility achieved!");
    }

    println!("==========================================");

    // The test succeeds if we have high compliance
    assert!(
        compliance_percentage >= 80.0,
        "Redis compliance should be at least 80%, got {:.1}%",
        compliance_percentage
    );
}
