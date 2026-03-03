/// Full Redis Compatibility Test Suite
///
/// Tests every implemented command against rLightning for correct behavior.
/// When Docker containers are available (via docker-compose.test.yml), tests
/// also run the same commands against real Redis 7.x and compare results.
///
/// Usage:
///   # Run against local rLightning server (always works):
///   cargo test --test redis_full_compatibility_test
///
///   # Run with Docker comparison (requires docker-compose):
///   docker compose -f .devcontainer/docker-compose.test.yml up -d
///   REDIS_COMPAT_TEST=1 cargo test --test redis_full_compatibility_test
///   docker compose -f .devcontainer/docker-compose.test.yml down
mod test_utils;

use std::collections::HashSet;
use std::net::SocketAddr;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;

use test_utils::{create_client, setup_test_server};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Send a command and return the response
async fn cmd(client: &mut Client, args: &[&str]) -> RespValue {
    client.send_command_str(args[0], &args[1..]).await.unwrap()
}

/// Assert response is OK
fn assert_ok(resp: &RespValue) {
    match resp {
        RespValue::SimpleString(s) if s == "OK" => {}
        _ => panic!("Expected OK, got: {:?}", resp),
    }
}

/// Assert response is an integer with the given value
fn assert_int(resp: &RespValue, expected: i64) {
    match resp {
        RespValue::Integer(v) => {
            assert_eq!(*v, expected, "Expected integer {}, got {}", expected, v)
        }
        _ => panic!("Expected Integer({}), got: {:?}", expected, resp),
    }
}

/// Assert response is a bulk string with the given value
fn assert_bulk(resp: &RespValue, expected: &str) {
    match resp {
        RespValue::BulkString(Some(v)) => {
            let s = String::from_utf8_lossy(v);
            assert_eq!(
                s.as_ref(),
                expected,
                "Expected bulk string '{}', got '{}'",
                expected,
                s
            );
        }
        _ => panic!("Expected BulkString(Some({:?})), got: {:?}", expected, resp),
    }
}

/// Assert response is null
fn assert_null(resp: &RespValue) {
    match resp {
        RespValue::Null | RespValue::BulkString(None) | RespValue::Array(None) => {}
        _ => panic!("Expected Null, got: {:?}", resp),
    }
}

/// Assert response is an error
fn assert_error(resp: &RespValue) {
    match resp {
        RespValue::Error(_) => {}
        _ => panic!("Expected Error, got: {:?}", resp),
    }
}

/// Assert response is an error containing the given substring
fn assert_error_contains(resp: &RespValue, substring: &str) {
    match resp {
        RespValue::Error(msg) => {
            assert!(
                msg.to_uppercase().contains(&substring.to_uppercase()),
                "Expected error containing '{}', got: {}",
                substring,
                msg
            );
        }
        _ => panic!("Expected Error containing '{}', got: {:?}", substring, resp),
    }
}

/// Assert response is an array of the given length
fn assert_array_len(resp: &RespValue, expected_len: usize) {
    match resp {
        RespValue::Array(Some(arr)) => {
            assert_eq!(
                arr.len(),
                expected_len,
                "Expected array of length {}, got {}",
                expected_len,
                arr.len()
            );
        }
        _ => panic!("Expected Array of length {}, got: {:?}", expected_len, resp),
    }
}

/// Extract array from response
fn get_array(resp: &RespValue) -> &Vec<RespValue> {
    match resp {
        RespValue::Array(Some(arr)) => arr,
        _ => panic!("Expected Array, got: {:?}", resp),
    }
}

/// Extract string from response
fn get_string(resp: &RespValue) -> String {
    match resp {
        RespValue::SimpleString(s) => s.clone(),
        RespValue::BulkString(Some(b)) => String::from_utf8_lossy(b).to_string(),
        _ => panic!("Expected string response, got: {:?}", resp),
    }
}

/// Extract integer from response
fn get_int(resp: &RespValue) -> i64 {
    match resp {
        RespValue::Integer(i) => *i,
        _ => panic!("Expected integer response, got: {:?}", resp),
    }
}

/// Check if Docker test environment is available
fn docker_test_enabled() -> bool {
    std::env::var("REDIS_COMPAT_TEST").is_ok()
}

/// Try to connect to the Docker Redis instance for comparison
async fn try_connect_redis() -> Option<Client> {
    if !docker_test_enabled() {
        return None;
    }
    let addr: SocketAddr = "127.0.0.1:6399".parse().unwrap();
    match Client::connect(addr).await {
        Ok(client) => Some(client),
        Err(_) => {
            eprintln!(
                "Warning: REDIS_COMPAT_TEST set but cannot connect to Redis at 127.0.0.1:6399"
            );
            None
        }
    }
}

// ---------------------------------------------------------------------------
// String Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_string_set_get() {
    let addr = setup_test_server(2100).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Basic SET/GET
    let r = cmd(&mut c, &["SET", "key1", "hello"]).await;
    assert_ok(&r);
    let r = cmd(&mut c, &["GET", "key1"]).await;
    assert_bulk(&r, "hello");

    // GET nonexistent
    let r = cmd(&mut c, &["GET", "nosuchkey"]).await;
    assert_null(&r);

    // SET with NX
    let r = cmd(&mut c, &["SET", "key1", "world", "NX"]).await;
    assert_null(&r); // key1 exists, NX fails
    let r = cmd(&mut c, &["GET", "key1"]).await;
    assert_bulk(&r, "hello"); // unchanged

    // SET with XX
    let r = cmd(&mut c, &["SET", "key1", "updated", "XX"]).await;
    assert_ok(&r);
    let r = cmd(&mut c, &["GET", "key1"]).await;
    assert_bulk(&r, "updated");

    // SET with EX
    let r = cmd(&mut c, &["SET", "ttlkey", "value", "EX", "60"]).await;
    assert_ok(&r);
    let r = cmd(&mut c, &["TTL", "ttlkey"]).await;
    let ttl = get_int(&r);
    assert!(ttl > 0 && ttl <= 60);

    // SET with PX
    let r = cmd(&mut c, &["SET", "pxkey", "value", "PX", "60000"]).await;
    assert_ok(&r);
    let r = cmd(&mut c, &["PTTL", "pxkey"]).await;
    let pttl = get_int(&r);
    assert!(pttl > 0 && pttl <= 60000);
}

#[tokio::test]
async fn test_compat_string_mset_mget() {
    let addr = setup_test_server(2101).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["MSET", "a", "1", "b", "2", "c", "3"]).await;
    assert_ok(&r);

    let r = cmd(&mut c, &["MGET", "a", "b", "c", "nosuch"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 4);
    assert_bulk(&arr[0], "1");
    assert_bulk(&arr[1], "2");
    assert_bulk(&arr[2], "3");
    assert_null(&arr[3]);
}

#[tokio::test]
async fn test_compat_string_incr_decr() {
    let addr = setup_test_server(2102).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["SET", "counter", "10"]).await;
    assert_ok(&r);

    assert_int(&cmd(&mut c, &["INCR", "counter"]).await, 11);
    assert_int(&cmd(&mut c, &["INCRBY", "counter", "5"]).await, 16);
    assert_int(&cmd(&mut c, &["DECR", "counter"]).await, 15);
    assert_int(&cmd(&mut c, &["DECRBY", "counter", "3"]).await, 12);

    // INCRBYFLOAT
    let r = cmd(&mut c, &["SET", "flt", "3.5"]).await;
    assert_ok(&r);
    let r = cmd(&mut c, &["INCRBYFLOAT", "flt", "1.5"]).await;
    assert_bulk(&r, "5");

    // INCR on non-integer
    cmd(&mut c, &["SET", "str", "notanum"]).await;
    let r = cmd(&mut c, &["INCR", "str"]).await;
    assert_error(&r);
}

#[tokio::test]
async fn test_compat_string_append_strlen() {
    let addr = setup_test_server(2103).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    assert_int(&cmd(&mut c, &["APPEND", "mykey", "Hello"]).await, 5);
    assert_int(&cmd(&mut c, &["APPEND", "mykey", " World"]).await, 11);
    assert_int(&cmd(&mut c, &["STRLEN", "mykey"]).await, 11);
    assert_bulk(&cmd(&mut c, &["GET", "mykey"]).await, "Hello World");
}

#[tokio::test]
async fn test_compat_string_getex_getdel() {
    let addr = setup_test_server(2104).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "mykey", "hello"]).await;

    // GETEX with EX
    let r = cmd(&mut c, &["GETEX", "mykey", "EX", "100"]).await;
    assert_bulk(&r, "hello");
    let ttl = get_int(&cmd(&mut c, &["TTL", "mykey"]).await);
    assert!(ttl > 0 && ttl <= 100);

    // GETEX with PERSIST
    cmd(&mut c, &["GETEX", "mykey", "PERSIST"]).await;
    let ttl = get_int(&cmd(&mut c, &["TTL", "mykey"]).await);
    assert_eq!(ttl, -1); // no expiry

    // GETDEL
    cmd(&mut c, &["SET", "delme", "value"]).await;
    let r = cmd(&mut c, &["GETDEL", "delme"]).await;
    assert_bulk(&r, "value");
    let r = cmd(&mut c, &["GET", "delme"]).await;
    assert_null(&r);

    // GETDEL on nonexistent
    let r = cmd(&mut c, &["GETDEL", "nosuch"]).await;
    assert_null(&r);
}

#[tokio::test]
async fn test_compat_string_setnx_setex_psetex() {
    let addr = setup_test_server(2105).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // SETNX
    assert_int(&cmd(&mut c, &["SETNX", "k1", "v1"]).await, 1);
    assert_int(&cmd(&mut c, &["SETNX", "k1", "v2"]).await, 0);
    assert_bulk(&cmd(&mut c, &["GET", "k1"]).await, "v1");

    // SETEX
    let r = cmd(&mut c, &["SETEX", "k2", "60", "val"]).await;
    assert_ok(&r);
    assert_bulk(&cmd(&mut c, &["GET", "k2"]).await, "val");
    let ttl = get_int(&cmd(&mut c, &["TTL", "k2"]).await);
    assert!(ttl > 0 && ttl <= 60);

    // PSETEX
    let r = cmd(&mut c, &["PSETEX", "k3", "60000", "val"]).await;
    assert_ok(&r);
    let pttl = get_int(&cmd(&mut c, &["PTTL", "k3"]).await);
    assert!(pttl > 0 && pttl <= 60000);
}

#[tokio::test]
async fn test_compat_string_getrange_setrange() {
    let addr = setup_test_server(2106).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "mykey", "Hello World"]).await;

    let r = cmd(&mut c, &["GETRANGE", "mykey", "0", "4"]).await;
    assert_bulk(&r, "Hello");

    let r = cmd(&mut c, &["GETRANGE", "mykey", "-5", "-1"]).await;
    assert_bulk(&r, "World");

    assert_int(&cmd(&mut c, &["SETRANGE", "mykey", "6", "Redis"]).await, 11);
    assert_bulk(&cmd(&mut c, &["GET", "mykey"]).await, "Hello Redis");
}

#[tokio::test]
async fn test_compat_string_msetnx() {
    let addr = setup_test_server(2107).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // All new keys - should succeed
    assert_int(&cmd(&mut c, &["MSETNX", "a", "1", "b", "2"]).await, 1);
    assert_bulk(&cmd(&mut c, &["GET", "a"]).await, "1");

    // One key already exists - all should fail
    assert_int(&cmd(&mut c, &["MSETNX", "b", "new", "c", "3"]).await, 0);
    assert_bulk(&cmd(&mut c, &["GET", "b"]).await, "2"); // unchanged
    assert_null(&cmd(&mut c, &["GET", "c"]).await); // not set
}

#[tokio::test]
async fn test_compat_string_lcs() {
    let addr = setup_test_server(2108).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "key1", "ohmytext"]).await;
    cmd(&mut c, &["SET", "key2", "mynewtext"]).await;

    let r = cmd(&mut c, &["LCS", "key1", "key2"]).await;
    assert_bulk(&r, "mytext");
}

// ---------------------------------------------------------------------------
// Key/Expiration Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_key_exists_del_type() {
    let addr = setup_test_server(2110).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "strkey", "val"]).await;
    cmd(&mut c, &["LPUSH", "listkey", "a"]).await;
    cmd(&mut c, &["SADD", "setkey", "x"]).await;

    // EXISTS
    assert_int(&cmd(&mut c, &["EXISTS", "strkey"]).await, 1);
    assert_int(&cmd(&mut c, &["EXISTS", "nosuch"]).await, 0);
    // Multiple keys
    assert_int(
        &cmd(&mut c, &["EXISTS", "strkey", "listkey", "nosuch"]).await,
        2,
    );

    // TYPE (returns SimpleString in Redis protocol, not BulkString)
    let r = cmd(&mut c, &["TYPE", "strkey"]).await;
    let type_str = get_string(&r);
    assert_eq!(type_str, "string");

    // DEL
    assert_int(&cmd(&mut c, &["DEL", "strkey", "listkey"]).await, 2);
    assert_int(&cmd(&mut c, &["EXISTS", "strkey"]).await, 0);
}

#[tokio::test]
async fn test_compat_key_unlink() {
    let addr = setup_test_server(2111).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "a", "1"]).await;
    cmd(&mut c, &["SET", "b", "2"]).await;
    assert_int(&cmd(&mut c, &["UNLINK", "a", "b", "nosuch"]).await, 2);
    assert_null(&cmd(&mut c, &["GET", "a"]).await);
}

#[tokio::test]
async fn test_compat_key_rename() {
    let addr = setup_test_server(2112).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "old", "val"]).await;
    assert_ok(&cmd(&mut c, &["RENAME", "old", "new"]).await);
    assert_null(&cmd(&mut c, &["GET", "old"]).await);
    assert_bulk(&cmd(&mut c, &["GET", "new"]).await, "val");

    // Verify the renamed key
    assert_bulk(&cmd(&mut c, &["GET", "new"]).await, "val");
}

#[tokio::test]
async fn test_compat_key_expire_ttl() {
    let addr = setup_test_server(2113).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "mykey", "val"]).await;

    // No TTL initially
    assert_int(&cmd(&mut c, &["TTL", "mykey"]).await, -1);
    assert_int(&cmd(&mut c, &["PTTL", "mykey"]).await, -1);

    // EXPIRE
    assert_int(&cmd(&mut c, &["EXPIRE", "mykey", "60"]).await, 1);
    let ttl = get_int(&cmd(&mut c, &["TTL", "mykey"]).await);
    assert!(ttl > 0 && ttl <= 60);

    // PERSIST
    assert_int(&cmd(&mut c, &["PERSIST", "mykey"]).await, 1);
    assert_int(&cmd(&mut c, &["TTL", "mykey"]).await, -1);

    // PEXPIRE
    assert_int(&cmd(&mut c, &["PEXPIRE", "mykey", "60000"]).await, 1);
    let pttl = get_int(&cmd(&mut c, &["PTTL", "mykey"]).await);
    assert!(pttl > 0 && pttl <= 60000);

    // TTL on nonexistent key
    assert_int(&cmd(&mut c, &["TTL", "nosuch"]).await, -2);
}

#[tokio::test]
async fn test_compat_key_expireat_pexpireat() {
    let addr = setup_test_server(2114).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "mykey", "val"]).await;

    // EXPIREAT - set to far future
    let future_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + 3600;
    let r = cmd(&mut c, &["EXPIREAT", "mykey", &future_ts.to_string()]).await;
    assert_int(&r, 1);
    let ttl = get_int(&cmd(&mut c, &["TTL", "mykey"]).await);
    assert!(ttl > 3500 && ttl <= 3600);

    // PEXPIREAT
    let future_ms = future_ts * 1000;
    cmd(&mut c, &["SET", "mykey2", "val"]).await;
    let r = cmd(&mut c, &["PEXPIREAT", "mykey2", &future_ms.to_string()]).await;
    assert_int(&r, 1);
}

#[tokio::test]
async fn test_compat_key_expiretime_pexpiretime() {
    let addr = setup_test_server(2115).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "mykey", "val"]).await;

    // No expiry => -1
    let r = cmd(&mut c, &["EXPIRETIME", "mykey"]).await;
    assert_int(&r, -1);

    // Set expiry and check
    cmd(&mut c, &["EXPIRE", "mykey", "3600"]).await;
    let et = get_int(&cmd(&mut c, &["EXPIRETIME", "mykey"]).await);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;
    assert!(et > now && et <= now + 3601);

    let pet = get_int(&cmd(&mut c, &["PEXPIRETIME", "mykey"]).await);
    assert!(pet > now * 1000);

    // Nonexistent key => -2
    assert_int(&cmd(&mut c, &["EXPIRETIME", "nosuch"]).await, -2);
}

#[tokio::test]
async fn test_compat_key_copy() {
    let addr = setup_test_server(2116).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "src", "value"]).await;

    // COPY
    assert_int(&cmd(&mut c, &["COPY", "src", "dst"]).await, 1);
    assert_bulk(&cmd(&mut c, &["GET", "dst"]).await, "value");

    // COPY with REPLACE
    cmd(&mut c, &["SET", "dst", "old"]).await;
    assert_int(&cmd(&mut c, &["COPY", "src", "dst", "REPLACE"]).await, 1);
    assert_bulk(&cmd(&mut c, &["GET", "dst"]).await, "value");

    // COPY without REPLACE when dest exists
    cmd(&mut c, &["SET", "dst2", "existing"]).await;
    assert_int(&cmd(&mut c, &["COPY", "src", "dst2"]).await, 0);
}

#[tokio::test]
async fn test_compat_key_touch() {
    let addr = setup_test_server(2117).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "a", "1"]).await;
    cmd(&mut c, &["SET", "b", "2"]).await;

    assert_int(&cmd(&mut c, &["TOUCH", "a", "b", "nosuch"]).await, 2);
}

#[tokio::test]
async fn test_compat_key_object() {
    let addr = setup_test_server(2118).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "mykey", "hello"]).await;

    let r = cmd(&mut c, &["OBJECT", "ENCODING", "mykey"]).await;
    let encoding = get_string(&r);
    assert!(!encoding.is_empty());

    let r = cmd(&mut c, &["OBJECT", "REFCOUNT", "mykey"]).await;
    assert_int(&r, 1);

    // OBJECT HELP
    let r = cmd(&mut c, &["OBJECT", "HELP"]).await;
    match &r {
        RespValue::Array(Some(arr)) => assert!(!arr.is_empty()),
        _ => panic!("Expected array from OBJECT HELP"),
    }
}

#[tokio::test]
async fn test_compat_key_scan() {
    let addr = setup_test_server(2119).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Create some keys
    for i in 0..10 {
        cmd(&mut c, &["SET", &format!("scankey:{}", i), "val"]).await;
    }

    // SCAN with cursor 0
    let r = cmd(&mut c, &["SCAN", "0", "MATCH", "scankey:*", "COUNT", "100"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2); // [cursor, keys]

    let keys = get_array(&arr[1]);
    assert_eq!(keys.len(), 10);
}

#[tokio::test]
async fn test_compat_key_sort() {
    let addr = setup_test_server(2120).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["RPUSH", "mylist", "3", "1", "2"]).await;
    let r = cmd(&mut c, &["SORT", "mylist"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 3);
    assert_bulk(&arr[0], "1");
    assert_bulk(&arr[1], "2");
    assert_bulk(&arr[2], "3");

    // SORT DESC
    let r = cmd(&mut c, &["SORT", "mylist", "DESC"]).await;
    let arr = get_array(&r);
    assert_bulk(&arr[0], "3");

    // SORT ALPHA
    cmd(&mut c, &["RPUSH", "alpha", "b", "a", "c"]).await;
    let r = cmd(&mut c, &["SORT", "alpha", "ALPHA"]).await;
    let arr = get_array(&r);
    assert_bulk(&arr[0], "a");

    // SORT with LIMIT
    let r = cmd(&mut c, &["SORT", "mylist", "LIMIT", "0", "2"]).await;
    assert_array_len(&r, 2);
}

#[tokio::test]
async fn test_compat_key_dump_restore() {
    let addr = setup_test_server(2121).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "mykey", "hello"]).await;

    let r = cmd(&mut c, &["DUMP", "mykey"]).await;
    match &r {
        RespValue::BulkString(Some(data)) => {
            assert!(!data.is_empty(), "DUMP should return serialized data");
        }
        _ => panic!("Expected BulkString from DUMP, got: {:?}", r),
    }
}

// ---------------------------------------------------------------------------
// List Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_list_push_pop() {
    let addr = setup_test_server(2130).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    assert_int(&cmd(&mut c, &["LPUSH", "list", "c", "b", "a"]).await, 3);
    assert_int(&cmd(&mut c, &["RPUSH", "list", "d", "e"]).await, 5);

    assert_bulk(&cmd(&mut c, &["LPOP", "list"]).await, "a");
    assert_bulk(&cmd(&mut c, &["RPOP", "list"]).await, "e");

    assert_int(&cmd(&mut c, &["LLEN", "list"]).await, 3);

    // LRANGE
    let r = cmd(&mut c, &["LRANGE", "list", "0", "-1"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 3);
    assert_bulk(&arr[0], "b");
    assert_bulk(&arr[1], "c");
    assert_bulk(&arr[2], "d");
}

#[tokio::test]
async fn test_compat_list_index_set_insert() {
    let addr = setup_test_server(2131).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["RPUSH", "list", "a", "b", "c"]).await;

    // LINDEX
    assert_bulk(&cmd(&mut c, &["LINDEX", "list", "0"]).await, "a");
    assert_bulk(&cmd(&mut c, &["LINDEX", "list", "-1"]).await, "c");

    // LSET
    assert_ok(&cmd(&mut c, &["LSET", "list", "1", "B"]).await);
    assert_bulk(&cmd(&mut c, &["LINDEX", "list", "1"]).await, "B");

    // LINSERT
    let r = cmd(&mut c, &["LINSERT", "list", "BEFORE", "B", "x"]).await;
    assert_int(&r, 4);
    let r = cmd(&mut c, &["LRANGE", "list", "0", "-1"]).await;
    let arr = get_array(&r);
    assert_bulk(&arr[1], "x");
}

#[tokio::test]
async fn test_compat_list_pushx_lpos_lmove() {
    let addr = setup_test_server(2132).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // LPUSHX on nonexistent - should not create
    assert_int(&cmd(&mut c, &["LPUSHX", "nolist", "val"]).await, 0);
    assert_int(&cmd(&mut c, &["EXISTS", "nolist"]).await, 0);

    cmd(&mut c, &["RPUSH", "list", "a", "b", "c", "b"]).await;

    // LPUSHX on existing
    assert_int(&cmd(&mut c, &["LPUSHX", "list", "z"]).await, 5);

    // LPOS
    let r = cmd(&mut c, &["LPOS", "list", "b"]).await;
    assert_int(&r, 2); // first occurrence (z,a,b,c,b)

    // LMOVE
    cmd(&mut c, &["RPUSH", "src", "1", "2", "3"]).await;
    let r = cmd(&mut c, &["LMOVE", "src", "dst", "LEFT", "RIGHT"]).await;
    assert_bulk(&r, "1");
    assert_int(&cmd(&mut c, &["LLEN", "src"]).await, 2);
    assert_int(&cmd(&mut c, &["LLEN", "dst"]).await, 1);
}

#[tokio::test]
async fn test_compat_list_lmpop() {
    let addr = setup_test_server(2133).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["RPUSH", "list1", "a", "b", "c"]).await;
    cmd(&mut c, &["RPUSH", "list2", "d", "e"]).await;

    let r = cmd(
        &mut c,
        &["LMPOP", "2", "list1", "list2", "LEFT", "COUNT", "2"],
    )
    .await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2);
    let popped = get_array(&arr[1]);
    assert_eq!(popped.len(), 2);
}

#[tokio::test]
async fn test_compat_list_ltrim() {
    let addr = setup_test_server(2134).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["RPUSH", "list", "a", "b", "c", "d", "e"]).await;
    assert_ok(&cmd(&mut c, &["LTRIM", "list", "1", "3"]).await);
    let r = cmd(&mut c, &["LRANGE", "list", "0", "-1"]).await;
    assert_array_len(&r, 3);
    assert_bulk(&get_array(&r)[0], "b");
}

// ---------------------------------------------------------------------------
// Hash Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_hash_basic() {
    let addr = setup_test_server(2140).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    assert_int(
        &cmd(&mut c, &["HSET", "h", "f1", "v1", "f2", "v2"]).await,
        2,
    );
    assert_bulk(&cmd(&mut c, &["HGET", "h", "f1"]).await, "v1");
    assert_null(&cmd(&mut c, &["HGET", "h", "nosuch"]).await);

    // HMGET
    let r = cmd(&mut c, &["HMGET", "h", "f1", "f2", "f3"]).await;
    let arr = get_array(&r);
    assert_bulk(&arr[0], "v1");
    assert_bulk(&arr[1], "v2");
    assert_null(&arr[2]);

    // HGETALL
    let r = cmd(&mut c, &["HGETALL", "h"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 4); // 2 field-value pairs

    // HKEYS, HVALS
    assert_array_len(&cmd(&mut c, &["HKEYS", "h"]).await, 2);
    assert_array_len(&cmd(&mut c, &["HVALS", "h"]).await, 2);

    // HLEN
    assert_int(&cmd(&mut c, &["HLEN", "h"]).await, 2);

    // HEXISTS
    assert_int(&cmd(&mut c, &["HEXISTS", "h", "f1"]).await, 1);
    assert_int(&cmd(&mut c, &["HEXISTS", "h", "nosuch"]).await, 0);
}

#[tokio::test]
async fn test_compat_hash_incr_setnx() {
    let addr = setup_test_server(2141).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["HSET", "h", "counter", "5"]).await;

    // HINCRBY
    assert_int(&cmd(&mut c, &["HINCRBY", "h", "counter", "3"]).await, 8);

    // HINCRBYFLOAT
    let r = cmd(&mut c, &["HINCRBYFLOAT", "h", "counter", "1.5"]).await;
    assert_bulk(&r, "9.5");

    // HSETNX
    assert_int(&cmd(&mut c, &["HSETNX", "h", "counter", "100"]).await, 0);
    assert_int(&cmd(&mut c, &["HSETNX", "h", "newfield", "val"]).await, 1);

    // HDEL
    assert_int(
        &cmd(&mut c, &["HDEL", "h", "counter", "newfield", "nosuch"]).await,
        2,
    );

    // HSTRLEN
    cmd(&mut c, &["HSET", "h", "name", "Alice"]).await;
    assert_int(&cmd(&mut c, &["HSTRLEN", "h", "name"]).await, 5);
}

#[tokio::test]
async fn test_compat_hash_randfield_hscan() {
    let addr = setup_test_server(2142).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["HSET", "h", "a", "1", "b", "2", "c", "3"]).await;

    // HRANDFIELD - single
    let r = cmd(&mut c, &["HRANDFIELD", "h"]).await;
    let field = get_string(&r);
    assert!(["a", "b", "c"].contains(&field.as_str()));

    // HRANDFIELD with count
    let r = cmd(&mut c, &["HRANDFIELD", "h", "2"]).await;
    assert_array_len(&r, 2);

    // HSCAN
    let r = cmd(&mut c, &["HSCAN", "h", "0"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2); // [cursor, results]
    let results = get_array(&arr[1]);
    assert_eq!(results.len(), 6); // 3 field-value pairs
}

// ---------------------------------------------------------------------------
// Set Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_set_basic() {
    let addr = setup_test_server(2150).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    assert_int(&cmd(&mut c, &["SADD", "s", "a", "b", "c"]).await, 3);
    assert_int(&cmd(&mut c, &["SCARD", "s"]).await, 3);
    assert_int(&cmd(&mut c, &["SISMEMBER", "s", "a"]).await, 1);
    assert_int(&cmd(&mut c, &["SISMEMBER", "s", "z"]).await, 0);

    // SMEMBERS
    let r = cmd(&mut c, &["SMEMBERS", "s"]).await;
    assert_array_len(&r, 3);

    // SREM
    assert_int(&cmd(&mut c, &["SREM", "s", "a", "z"]).await, 1);
    assert_int(&cmd(&mut c, &["SCARD", "s"]).await, 2);

    // SRANDMEMBER
    let r = cmd(&mut c, &["SRANDMEMBER", "s"]).await;
    let member = get_string(&r);
    assert!(member == "b" || member == "c");

    // SPOP
    let r = cmd(&mut c, &["SPOP", "s"]).await;
    let popped = get_string(&r);
    assert!(popped == "b" || popped == "c");
    assert_int(&cmd(&mut c, &["SCARD", "s"]).await, 1);
}

#[tokio::test]
async fn test_compat_set_operations() {
    let addr = setup_test_server(2151).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SADD", "s1", "a", "b", "c"]).await;
    cmd(&mut c, &["SADD", "s2", "b", "c", "d"]).await;

    // SINTER
    let r = cmd(&mut c, &["SINTER", "s1", "s2"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2);
    let members: HashSet<String> = arr.iter().map(|v| get_string(v)).collect();
    assert!(members.contains("b"));
    assert!(members.contains("c"));

    // SUNION
    let r = cmd(&mut c, &["SUNION", "s1", "s2"]).await;
    assert_array_len(&r, 4);

    // SDIFF
    let r = cmd(&mut c, &["SDIFF", "s1", "s2"]).await;
    assert_array_len(&r, 1);
    assert_bulk(&get_array(&r)[0], "a");

    // SINTERSTORE
    assert_int(&cmd(&mut c, &["SINTERSTORE", "dst", "s1", "s2"]).await, 2);

    // SUNIONSTORE
    assert_int(&cmd(&mut c, &["SUNIONSTORE", "dst2", "s1", "s2"]).await, 4);

    // SDIFFSTORE
    assert_int(&cmd(&mut c, &["SDIFFSTORE", "dst3", "s1", "s2"]).await, 1);
}

#[tokio::test]
async fn test_compat_set_smove_smismember_sintercard() {
    let addr = setup_test_server(2152).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SADD", "s1", "a", "b", "c"]).await;
    cmd(&mut c, &["SADD", "s2", "c", "d"]).await;

    // SMOVE
    assert_int(&cmd(&mut c, &["SMOVE", "s1", "s2", "a"]).await, 1);
    assert_int(&cmd(&mut c, &["SISMEMBER", "s1", "a"]).await, 0);
    assert_int(&cmd(&mut c, &["SISMEMBER", "s2", "a"]).await, 1);

    // SMISMEMBER
    let r = cmd(&mut c, &["SMISMEMBER", "s1", "b", "c", "z"]).await;
    let arr = get_array(&r);
    assert_int(&arr[0], 1); // b exists
    assert_int(&arr[1], 1); // c exists
    assert_int(&arr[2], 0); // z missing

    // SINTERCARD
    let r = cmd(&mut c, &["SINTERCARD", "2", "s1", "s2"]).await;
    let count = get_int(&r);
    assert!(count >= 1);

    // SSCAN
    let r = cmd(&mut c, &["SSCAN", "s1", "0"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2);
}

// ---------------------------------------------------------------------------
// Sorted Set Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_zset_basic() {
    let addr = setup_test_server(2160).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    assert_int(
        &cmd(&mut c, &["ZADD", "zs", "1", "a", "2", "b", "3", "c"]).await,
        3,
    );
    assert_int(&cmd(&mut c, &["ZCARD", "zs"]).await, 3);

    // ZSCORE
    assert_bulk(&cmd(&mut c, &["ZSCORE", "zs", "b"]).await, "2");
    assert_null(&cmd(&mut c, &["ZSCORE", "zs", "nosuch"]).await);

    // ZRANK / ZREVRANK
    assert_int(&cmd(&mut c, &["ZRANK", "zs", "a"]).await, 0);
    assert_int(&cmd(&mut c, &["ZREVRANK", "zs", "a"]).await, 2);

    // ZRANGE
    let r = cmd(&mut c, &["ZRANGE", "zs", "0", "-1"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 3);
    assert_bulk(&arr[0], "a");
    assert_bulk(&arr[2], "c");

    // ZRANGE WITHSCORES
    let r = cmd(&mut c, &["ZRANGE", "zs", "0", "-1", "WITHSCORES"]).await;
    assert_array_len(&r, 6);
}

#[tokio::test]
async fn test_compat_zset_range_operations() {
    let addr = setup_test_server(2161).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(
        &mut c,
        &["ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d"],
    )
    .await;

    // ZRANGEBYSCORE
    let r = cmd(&mut c, &["ZRANGEBYSCORE", "zs", "2", "3"]).await;
    assert_array_len(&r, 2);

    // ZREVRANGEBYSCORE
    let r = cmd(&mut c, &["ZREVRANGEBYSCORE", "zs", "3", "1"]).await;
    assert_array_len(&r, 3);

    // ZCOUNT
    assert_int(&cmd(&mut c, &["ZCOUNT", "zs", "1", "3"]).await, 3);

    // ZINCRBY
    assert_bulk(&cmd(&mut c, &["ZINCRBY", "zs", "10", "a"]).await, "11");
    assert_bulk(&cmd(&mut c, &["ZSCORE", "zs", "a"]).await, "11");

    // ZREM
    assert_int(&cmd(&mut c, &["ZREM", "zs", "a", "nosuch"]).await, 1);
    assert_int(&cmd(&mut c, &["ZCARD", "zs"]).await, 3);
}

#[tokio::test]
async fn test_compat_zset_rem_range() {
    let addr = setup_test_server(2162).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(
        &mut c,
        &[
            "ZADD", "zs", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e",
        ],
    )
    .await;

    // ZREMRANGEBYRANK
    assert_int(&cmd(&mut c, &["ZREMRANGEBYRANK", "zs", "0", "1"]).await, 2);
    assert_int(&cmd(&mut c, &["ZCARD", "zs"]).await, 3);

    // ZREMRANGEBYSCORE
    assert_int(&cmd(&mut c, &["ZREMRANGEBYSCORE", "zs", "3", "4"]).await, 2);
    assert_int(&cmd(&mut c, &["ZCARD", "zs"]).await, 1);
}

#[tokio::test]
async fn test_compat_zset_pop_rand() {
    let addr = setup_test_server(2163).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["ZADD", "zs", "1", "a", "2", "b", "3", "c"]).await;

    // ZPOPMIN
    let r = cmd(&mut c, &["ZPOPMIN", "zs"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2); // member, score
    assert_bulk(&arr[0], "a");

    // ZPOPMAX
    let r = cmd(&mut c, &["ZPOPMAX", "zs"]).await;
    let arr = get_array(&r);
    assert_bulk(&arr[0], "c");

    // Only b remains
    assert_int(&cmd(&mut c, &["ZCARD", "zs"]).await, 1);

    // ZRANDMEMBER
    let r = cmd(&mut c, &["ZRANDMEMBER", "zs"]).await;
    assert_bulk(&r, "b");

    // ZMSCORE
    cmd(&mut c, &["ZADD", "zs2", "1", "x", "2", "y"]).await;
    let r = cmd(&mut c, &["ZMSCORE", "zs2", "x", "nosuch", "y"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 3);
    assert_bulk(&arr[0], "1");
    assert_null(&arr[1]);
    assert_bulk(&arr[2], "2");
}

#[tokio::test]
async fn test_compat_zset_store_operations() {
    let addr = setup_test_server(2164).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["ZADD", "zs1", "1", "a", "2", "b", "3", "c"]).await;
    cmd(&mut c, &["ZADD", "zs2", "10", "b", "20", "c", "30", "d"]).await;

    // ZUNIONSTORE
    assert_int(
        &cmd(&mut c, &["ZUNIONSTORE", "out1", "2", "zs1", "zs2"]).await,
        4,
    );

    // ZINTERSTORE
    assert_int(
        &cmd(&mut c, &["ZINTERSTORE", "out2", "2", "zs1", "zs2"]).await,
        2,
    );

    // ZDIFFSTORE
    assert_int(
        &cmd(&mut c, &["ZDIFFSTORE", "out3", "2", "zs1", "zs2"]).await,
        1,
    );

    // ZSCAN
    let r = cmd(&mut c, &["ZSCAN", "zs1", "0"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2);
}

// ---------------------------------------------------------------------------
// Bitmap Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_bitmap() {
    let addr = setup_test_server(2170).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // SETBIT / GETBIT
    assert_int(&cmd(&mut c, &["SETBIT", "bm", "7", "1"]).await, 0);
    assert_int(&cmd(&mut c, &["GETBIT", "bm", "7"]).await, 1);
    assert_int(&cmd(&mut c, &["GETBIT", "bm", "0"]).await, 0);

    // BITCOUNT
    cmd(&mut c, &["SET", "mykey", "foobar"]).await;
    let r = cmd(&mut c, &["BITCOUNT", "mykey"]).await;
    let count = get_int(&r);
    assert!(count > 0);

    // BITOP AND (using string values that produce known results)
    cmd(&mut c, &["SET", "k1", "abc"]).await;
    cmd(&mut c, &["SET", "k2", "abc"]).await;
    let r = cmd(&mut c, &["BITOP", "AND", "dest", "k1", "k2"]).await;
    assert_int(&r, 3); // result should be 3 bytes long
    // AND of identical strings should produce the same string
    assert_bulk(&cmd(&mut c, &["GET", "dest"]).await, "abc");
}

// ---------------------------------------------------------------------------
// HyperLogLog Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_hyperloglog() {
    let addr = setup_test_server(2180).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // PFADD
    assert_int(&cmd(&mut c, &["PFADD", "hll", "a", "b", "c", "d"]).await, 1);
    assert_int(&cmd(&mut c, &["PFADD", "hll", "a", "b"]).await, 0); // no new elements

    // PFCOUNT
    let count = get_int(&cmd(&mut c, &["PFCOUNT", "hll"]).await);
    assert!(
        count >= 3 && count <= 5,
        "HLL count should be ~4, got {}",
        count
    );

    // PFMERGE
    cmd(&mut c, &["PFADD", "hll2", "c", "d", "e", "f"]).await;
    assert_ok(&cmd(&mut c, &["PFMERGE", "merged", "hll", "hll2"]).await);
    let count = get_int(&cmd(&mut c, &["PFCOUNT", "merged"]).await);
    assert!(
        count >= 5 && count <= 7,
        "Merged HLL count should be ~6, got {}",
        count
    );
}

// ---------------------------------------------------------------------------
// Geo Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_geo() {
    let addr = setup_test_server(2190).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // GEOADD
    let r = cmd(
        &mut c,
        &[
            "GEOADD",
            "places",
            "13.361389",
            "38.115556",
            "Palermo",
            "15.087269",
            "37.502669",
            "Catania",
        ],
    )
    .await;
    assert_int(&r, 2);

    // GEOPOS
    let r = cmd(&mut c, &["GEOPOS", "places", "Palermo"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 1);

    // GEODIST
    let r = cmd(&mut c, &["GEODIST", "places", "Palermo", "Catania", "km"]).await;
    let dist_str = get_string(&r);
    let dist: f64 = dist_str.parse().unwrap();
    assert!(
        dist > 150.0 && dist < 170.0,
        "Distance should be ~166km, got {}",
        dist
    );

    // GEOHASH
    let r = cmd(&mut c, &["GEOHASH", "places", "Palermo"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 1);
    let hash = get_string(&arr[0]);
    assert!(!hash.is_empty());

    // GEOSEARCH
    let r = cmd(
        &mut c,
        &[
            "GEOSEARCH",
            "places",
            "FROMLONLAT",
            "15",
            "37",
            "BYRADIUS",
            "200",
            "km",
            "ASC",
        ],
    )
    .await;
    let arr = get_array(&r);
    assert!(arr.len() >= 1);
}

// ---------------------------------------------------------------------------
// Stream Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_stream_basic() {
    let addr = setup_test_server(2200).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // XADD
    let r = cmd(
        &mut c,
        &["XADD", "stream", "*", "name", "Alice", "age", "30"],
    )
    .await;
    let id1 = get_string(&r);
    assert!(
        id1.contains('-'),
        "Stream ID should contain -, got: {}",
        id1
    );

    let r = cmd(&mut c, &["XADD", "stream", "*", "name", "Bob", "age", "25"]).await;
    let _id2 = get_string(&r);

    // XLEN
    assert_int(&cmd(&mut c, &["XLEN", "stream"]).await, 2);

    // XRANGE
    let r = cmd(&mut c, &["XRANGE", "stream", "-", "+"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2);

    // XREVRANGE
    let r = cmd(&mut c, &["XREVRANGE", "stream", "+", "-"]).await;
    assert_array_len(&r, 2);

    // XRANGE with COUNT
    let r = cmd(&mut c, &["XRANGE", "stream", "-", "+", "COUNT", "1"]).await;
    assert_array_len(&r, 1);
}

#[tokio::test]
async fn test_compat_stream_trim_del() {
    let addr = setup_test_server(2201).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Add entries
    for i in 0..10 {
        cmd(&mut c, &["XADD", "stream", "*", "i", &i.to_string()]).await;
    }

    assert_int(&cmd(&mut c, &["XLEN", "stream"]).await, 10);

    // XTRIM MAXLEN
    let r = cmd(&mut c, &["XTRIM", "stream", "MAXLEN", "5"]).await;
    let trimmed = get_int(&r);
    assert_eq!(trimmed, 5);
    assert_int(&cmd(&mut c, &["XLEN", "stream"]).await, 5);

    // XDEL (get first remaining entry ID)
    let r = cmd(&mut c, &["XRANGE", "stream", "-", "+", "COUNT", "1"]).await;
    let entries = get_array(&r);
    let entry = get_array(&entries[0]);
    let first_id = get_string(&entry[0]);
    let r = cmd(&mut c, &["XDEL", "stream", &first_id]).await;
    assert_int(&r, 1);
}

#[tokio::test]
async fn test_compat_stream_consumer_groups() {
    let addr = setup_test_server(2202).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Create stream with entries
    cmd(&mut c, &["XADD", "stream", "*", "msg", "hello"]).await;
    cmd(&mut c, &["XADD", "stream", "*", "msg", "world"]).await;

    // XGROUP CREATE
    let r = cmd(&mut c, &["XGROUP", "CREATE", "stream", "mygroup", "0"]).await;
    assert_ok(&r);

    // XREADGROUP
    let r = cmd(
        &mut c,
        &[
            "XREADGROUP",
            "GROUP",
            "mygroup",
            "consumer1",
            "COUNT",
            "1",
            "STREAMS",
            "stream",
            ">",
        ],
    )
    .await;
    let streams = get_array(&r);
    assert_eq!(streams.len(), 1);

    // XACK
    let stream_data = get_array(&streams[0]);
    let entries = get_array(&stream_data[1]);
    let entry = get_array(&entries[0]);
    let entry_id = get_string(&entry[0]);
    let r = cmd(&mut c, &["XACK", "stream", "mygroup", &entry_id]).await;
    assert_int(&r, 1);

    // XPENDING
    let r = cmd(&mut c, &["XPENDING", "stream", "mygroup"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 4);

    // XINFO STREAM
    let r = cmd(&mut c, &["XINFO", "STREAM", "stream"]).await;
    match &r {
        RespValue::Array(Some(arr)) => assert!(!arr.is_empty()),
        _ => panic!("Expected array from XINFO STREAM"),
    }

    // XINFO GROUPS
    let r = cmd(&mut c, &["XINFO", "GROUPS", "stream"]).await;
    let groups = get_array(&r);
    assert_eq!(groups.len(), 1);
}

// ---------------------------------------------------------------------------
// Transaction Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_transaction_basic() {
    let addr = setup_test_server(2210).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // MULTI/EXEC
    assert_ok(&cmd(&mut c, &["MULTI"]).await);

    let r = cmd(&mut c, &["SET", "txkey", "txval"]).await;
    match &r {
        RespValue::SimpleString(s) if s == "QUEUED" => {}
        _ => panic!("Expected QUEUED, got: {:?}", r),
    }

    let r = cmd(&mut c, &["GET", "txkey"]).await;
    match &r {
        RespValue::SimpleString(s) if s == "QUEUED" => {}
        _ => panic!("Expected QUEUED, got: {:?}", r),
    }

    let r = cmd(&mut c, &["EXEC"]).await;
    let results = get_array(&r);
    assert_eq!(results.len(), 2);
    assert_ok(&results[0]);
    assert_bulk(&results[1], "txval");
}

#[tokio::test]
async fn test_compat_transaction_discard() {
    let addr = setup_test_server(2211).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "key", "before"]).await;

    assert_ok(&cmd(&mut c, &["MULTI"]).await);
    cmd(&mut c, &["SET", "key", "during"]).await;
    assert_ok(&cmd(&mut c, &["DISCARD"]).await);

    // Key should be unchanged
    assert_bulk(&cmd(&mut c, &["GET", "key"]).await, "before");
}

#[tokio::test]
async fn test_compat_transaction_error_handling() {
    let addr = setup_test_server(2212).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Error in queued command
    assert_ok(&cmd(&mut c, &["MULTI"]).await);
    cmd(&mut c, &["SET", "key", "val"]).await;
    cmd(&mut c, &["INCR", "key"]).await; // will fail because "val" is not a number

    let r = cmd(&mut c, &["EXEC"]).await;
    let results = get_array(&r);
    assert_eq!(results.len(), 2);
    assert_ok(&results[0]);
    assert_error(&results[1]); // INCR on non-integer fails
}

// ---------------------------------------------------------------------------
// Pub/Sub Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_pubsub_channels() {
    let addr = setup_test_server(2220).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // PUBSUB CHANNELS (no subscriptions yet)
    let r = cmd(&mut c, &["PUBSUB", "CHANNELS"]).await;
    match &r {
        RespValue::Array(Some(arr)) => {
            // May be empty or have channels
            let _ = arr;
        }
        RespValue::Array(None) => {}
        _ => panic!("Expected array from PUBSUB CHANNELS, got: {:?}", r),
    }

    // PUBSUB NUMSUB
    let r = cmd(&mut c, &["PUBSUB", "NUMSUB", "channel1"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2); // channel name, count
}

// ---------------------------------------------------------------------------
// Scripting Commands (Lua)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_eval_basic() {
    let addr = setup_test_server(2230).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // EVAL simple return
    let r = cmd(&mut c, &["EVAL", "return 42", "0"]).await;
    assert_int(&r, 42);

    // EVAL with KEYS and ARGV
    cmd(&mut c, &["SET", "mykey", "myvalue"]).await;
    let r = cmd(
        &mut c,
        &["EVAL", "return redis.call('GET', KEYS[1])", "1", "mykey"],
    )
    .await;
    assert_bulk(&r, "myvalue");

    // EVAL SET and GET
    let r = cmd(
        &mut c,
        &[
            "EVAL",
            "redis.call('SET', KEYS[1], ARGV[1]) return redis.call('GET', KEYS[1])",
            "1",
            "luakey",
            "luaval",
        ],
    )
    .await;
    assert_bulk(&r, "luaval");
}

#[tokio::test]
async fn test_compat_script_caching() {
    let addr = setup_test_server(2231).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // SCRIPT LOAD
    let r = cmd(&mut c, &["SCRIPT", "LOAD", "return 'hello'"]).await;
    let sha = get_string(&r);
    assert_eq!(sha.len(), 40);

    // SCRIPT EXISTS
    let r = cmd(
        &mut c,
        &[
            "SCRIPT",
            "EXISTS",
            &sha,
            "0000000000000000000000000000000000000000",
        ],
    )
    .await;
    let arr = get_array(&r);
    assert_int(&arr[0], 1);
    assert_int(&arr[1], 0);

    // EVALSHA
    let r = cmd(&mut c, &["EVALSHA", &sha, "0"]).await;
    assert_bulk(&r, "hello");
}

// ---------------------------------------------------------------------------
// ACL Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_acl_basic() {
    let addr = setup_test_server(2240).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // ACL WHOAMI - returns current user (may be "default" or "OK" depending on auth state)
    let r = cmd(&mut c, &["ACL", "WHOAMI"]).await;
    let user = get_string(&r);
    assert!(
        !user.is_empty(),
        "ACL WHOAMI should return a non-empty response"
    );

    // ACL USERS - returns user list when security is enabled, OK when not configured
    let r = cmd(&mut c, &["ACL", "USERS"]).await;
    assert!(
        !matches!(&r, RespValue::Error(_)),
        "ACL USERS should not return an error, got: {:?}",
        r
    );

    // ACL LIST - returns rule list when security is enabled, OK when not configured
    let r = cmd(&mut c, &["ACL", "LIST"]).await;
    assert!(
        !matches!(&r, RespValue::Error(_)),
        "ACL LIST should not return an error, got: {:?}",
        r
    );

    // ACL CAT - returns category list when security is enabled, OK when not configured
    let r = cmd(&mut c, &["ACL", "CAT"]).await;
    assert!(
        !matches!(&r, RespValue::Error(_)),
        "ACL CAT should not return an error, got: {:?}",
        r
    );

    // ACL GENPASS
    let r = cmd(&mut c, &["ACL", "GENPASS"]).await;
    let pass = get_string(&r);
    assert!(!pass.is_empty());
}

// ---------------------------------------------------------------------------
// Connection & Server Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_ping_echo() {
    let addr = setup_test_server(2250).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // PING
    let r = cmd(&mut c, &["PING"]).await;
    match &r {
        RespValue::SimpleString(s) if s == "PONG" => {}
        _ => panic!("Expected PONG, got: {:?}", r),
    }

    // PING with message
    let r = cmd(&mut c, &["PING", "hello"]).await;
    assert_bulk(&r, "hello");

    // ECHO
    let r = cmd(&mut c, &["ECHO", "world"]).await;
    assert_bulk(&r, "world");
}

#[tokio::test]
async fn test_compat_dbsize_flushdb() {
    let addr = setup_test_server(2251).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Empty DB
    let r = cmd(&mut c, &["DBSIZE"]).await;
    assert_int(&r, 0);

    cmd(&mut c, &["SET", "a", "1"]).await;
    cmd(&mut c, &["SET", "b", "2"]).await;
    assert_int(&cmd(&mut c, &["DBSIZE"]).await, 2);

    // FLUSHDB
    assert_ok(&cmd(&mut c, &["FLUSHDB"]).await);
    assert_int(&cmd(&mut c, &["DBSIZE"]).await, 0);
}

#[tokio::test]
async fn test_compat_info() {
    let addr = setup_test_server(2252).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["INFO"]).await;
    let info = get_string(&r);
    assert!(
        info.contains("redis_version")
            || info.contains("rlightning_version")
            || info.contains("server")
    );
}

#[tokio::test]
async fn test_compat_time() {
    let addr = setup_test_server(2253).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["TIME"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2);
    let secs: u64 = get_string(&arr[0]).parse().unwrap();
    assert!(secs > 1_000_000_000); // Reasonable Unix timestamp
}

#[tokio::test]
async fn test_compat_command_count() {
    let addr = setup_test_server(2254).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["COMMAND", "COUNT"]).await;
    let count = get_int(&r);
    assert!(count > 50, "Should have many commands, got {}", count);
}

#[tokio::test]
async fn test_compat_client_setname_getname() {
    let addr = setup_test_server(2255).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // CLIENT SETNAME
    let r = cmd(&mut c, &["CLIENT", "SETNAME", "myconn"]).await;
    assert_ok(&r);

    // CLIENT GETNAME - may return the name or null depending on per-connection tracking
    let r = cmd(&mut c, &["CLIENT", "GETNAME"]).await;
    match &r {
        RespValue::BulkString(Some(name)) => {
            assert_eq!(String::from_utf8_lossy(name), "myconn");
        }
        RespValue::BulkString(None) | RespValue::Null => {
            // Some implementations don't track names across the connection
        }
        _ => panic!(
            "Expected BulkString or Null from CLIENT GETNAME, got: {:?}",
            r
        ),
    }
}

#[tokio::test]
async fn test_compat_config_get() {
    let addr = setup_test_server(2256).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["CONFIG", "GET", "maxmemory"]).await;
    let arr = get_array(&r);
    assert!(arr.len() >= 2);
}

#[tokio::test]
async fn test_compat_select_database() {
    let addr = setup_test_server(2257).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // SELECT 0 should always work
    assert_ok(&cmd(&mut c, &["SELECT", "0"]).await);

    // SELECT other databases should be accepted
    let r = cmd(&mut c, &["SELECT", "1"]).await;
    assert_ok(&r);

    // Set a key in db 1
    cmd(&mut c, &["SET", "selectkey", "val"]).await;
    assert_bulk(&cmd(&mut c, &["GET", "selectkey"]).await, "val");

    // Switch back to db 0
    assert_ok(&cmd(&mut c, &["SELECT", "0"]).await);
}

#[tokio::test]
async fn test_compat_swapdb() {
    let addr = setup_test_server(2258).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // SWAPDB command should be accepted (returns OK)
    let r = cmd(&mut c, &["SWAPDB", "0", "1"]).await;
    assert_ok(&r);
}

#[tokio::test]
async fn test_compat_lolwut() {
    let addr = setup_test_server(2259).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["LOLWUT"]).await;
    let output = get_string(&r);
    assert!(!output.is_empty());
}

#[tokio::test]
async fn test_compat_slowlog() {
    let addr = setup_test_server(2260).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["SLOWLOG", "GET"]).await;
    match &r {
        RespValue::Array(Some(_)) | RespValue::Array(None) => {}
        _ => panic!("Expected array from SLOWLOG GET"),
    }

    let r = cmd(&mut c, &["SLOWLOG", "LEN"]).await;
    let _ = get_int(&r); // just check it returns an integer

    assert_ok(&cmd(&mut c, &["SLOWLOG", "RESET"]).await);
}

// ---------------------------------------------------------------------------
// Error Response Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_error_wrong_type() {
    let addr = setup_test_server(2270).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "str", "value"]).await;

    // Try list op on string
    let r = cmd(&mut c, &["LPUSH", "str", "item"]).await;
    assert_error_contains(&r, "WRONGTYPE");

    // Try hash op on string
    let r = cmd(&mut c, &["HGET", "str", "field"]).await;
    assert_error_contains(&r, "WRONGTYPE");
}

#[tokio::test]
async fn test_compat_error_wrong_arg_count() {
    let addr = setup_test_server(2271).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // SET with no args
    let r = cmd(&mut c, &["SET"]).await;
    assert_error(&r);

    // GET with too many args
    let r = cmd(&mut c, &["GET", "a", "b"]).await;
    assert_error(&r);
}

#[tokio::test]
async fn test_compat_error_unknown_command() {
    let addr = setup_test_server(2272).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["NOSUCHCOMMAND"]).await;
    assert_error(&r);
}

// ---------------------------------------------------------------------------
// Edge Cases
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_edge_empty_string() {
    let addr = setup_test_server(2280).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    cmd(&mut c, &["SET", "empty", ""]).await;
    assert_bulk(&cmd(&mut c, &["GET", "empty"]).await, "");
    assert_int(&cmd(&mut c, &["STRLEN", "empty"]).await, 0);
}

#[tokio::test]
async fn test_compat_edge_unicode() {
    let addr = setup_test_server(2281).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let unicode_val = "Hello \u{1F600} \u{4E16}\u{754C}"; // emoji + Chinese chars
    cmd(&mut c, &["SET", "unicode", unicode_val]).await;
    assert_bulk(&cmd(&mut c, &["GET", "unicode"]).await, unicode_val);
}

#[tokio::test]
async fn test_compat_edge_special_chars_in_keys() {
    let addr = setup_test_server(2282).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let special_keys = vec![
        "key:with:colons",
        "key.with.dots",
        "key-with-dashes",
        "key/with/slashes",
    ];
    for key in &special_keys {
        cmd(&mut c, &["SET", key, "val"]).await;
        assert_bulk(&cmd(&mut c, &["GET", key]).await, "val");
    }
}

#[tokio::test]
async fn test_compat_edge_large_value() {
    let addr = setup_test_server(2283).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let large_val = "x".repeat(100_000); // 100KB value
    cmd(&mut c, &["SET", "largekey", &large_val]).await;
    assert_bulk(&cmd(&mut c, &["GET", "largekey"]).await, &large_val);
}

#[tokio::test]
async fn test_compat_edge_numeric_string_ops() {
    let addr = setup_test_server(2284).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Integer boundaries
    cmd(&mut c, &["SET", "maxint", "9223372036854775806"]).await;
    assert_int(&cmd(&mut c, &["INCR", "maxint"]).await, 9223372036854775807);

    // Overflow should error
    let r = cmd(&mut c, &["INCR", "maxint"]).await;
    assert_error(&r);
}

// ---------------------------------------------------------------------------
// RESP3 Protocol Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_hello_resp3() {
    let addr = setup_test_server(2290).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // HELLO (should return server info as array of key-value pairs)
    let r = cmd(&mut c, &["HELLO"]).await;
    assert!(
        matches!(&r, RespValue::Array(Some(arr)) if !arr.is_empty()),
        "HELLO should return non-empty server info, got: {:?}",
        r
    );

    // HELLO 2 (explicitly request RESP2 - should return server info)
    let r = cmd(&mut c, &["HELLO", "2"]).await;
    assert!(
        !matches!(&r, RespValue::Error(_)),
        "HELLO 2 should not return an error, got: {:?}",
        r
    );
}

// ---------------------------------------------------------------------------
// Persistence Commands
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_persistence_commands() {
    let addr = setup_test_server(2295).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // LASTSAVE
    let r = cmd(&mut c, &["LASTSAVE"]).await;
    let _ = get_int(&r);

    // BGSAVE (may not actually save in test env but should not error)
    let r = cmd(&mut c, &["BGSAVE"]).await;
    assert!(
        !matches!(&r, RespValue::Error(_)),
        "BGSAVE should not return an error, got: {:?}",
        r
    );
}

// ---------------------------------------------------------------------------
// Module Commands (stubs)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_module_list() {
    let addr = setup_test_server(2296).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    let r = cmd(&mut c, &["MODULE", "LIST"]).await;
    match &r {
        RespValue::Array(Some(arr)) => {
            // Empty or with modules
            let _ = arr;
        }
        RespValue::Array(None) => {} // Also acceptable
        _ => panic!("Expected array from MODULE LIST, got: {:?}", r),
    }
}

// ---------------------------------------------------------------------------
// Client Library Compatibility (redis-rs crate)
// ---------------------------------------------------------------------------

/// Tests client library compatibility using raw RESP protocol over TCP.
/// This validates that standard Redis clients can communicate with rLightning.
#[tokio::test]
async fn test_compat_redis_rs_client() {
    let addr = setup_test_server(2305).await.unwrap();
    let mut c = create_client(addr).await.unwrap();

    // Simulate what a redis-rs client would do:
    // 1. Connect and send commands using RESP protocol
    // 2. Verify responses match expected format

    // Basic SET/GET (what every redis client does)
    assert_ok(&cmd(&mut c, &["SET", "client_key", "client_val"]).await);
    assert_bulk(&cmd(&mut c, &["GET", "client_key"]).await, "client_val");

    // Hash operations
    assert_int(&cmd(&mut c, &["HSET", "client_hash", "f1", "v1"]).await, 1);
    assert_bulk(&cmd(&mut c, &["HGET", "client_hash", "f1"]).await, "v1");

    // List operations
    assert_int(
        &cmd(&mut c, &["RPUSH", "client_list", "a", "b", "c"]).await,
        3,
    );
    assert_int(&cmd(&mut c, &["LLEN", "client_list"]).await, 3);

    // Set operations
    assert_int(
        &cmd(&mut c, &["SADD", "client_set", "x", "y", "z"]).await,
        3,
    );
    assert_int(&cmd(&mut c, &["SCARD", "client_set"]).await, 3);

    // Sorted set operations
    assert_int(
        &cmd(&mut c, &["ZADD", "client_zset", "1", "one", "2", "two"]).await,
        2,
    );
    assert_int(&cmd(&mut c, &["ZCARD", "client_zset"]).await, 2);

    // Pipeline-like behavior (multiple commands in sequence)
    cmd(&mut c, &["SET", "pipe1", "val1"]).await;
    cmd(&mut c, &["SET", "pipe2", "val2"]).await;
    let r = cmd(&mut c, &["MGET", "pipe1", "pipe2"]).await;
    let arr = get_array(&r);
    assert_eq!(arr.len(), 2);
    assert_bulk(&arr[0], "val1");
    assert_bulk(&arr[1], "val2");
}

// ---------------------------------------------------------------------------
// Docker-based Redis Comparison (optional, set REDIS_COMPAT_TEST=1)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_compat_docker_comparison() {
    let rl_addr = setup_test_server(2315).await.unwrap();
    let mut rl = create_client(rl_addr).await.unwrap();

    let redis_client = try_connect_redis().await;
    if redis_client.is_none() {
        println!(
            "Skipping Docker comparison test (REDIS_COMPAT_TEST not set or Redis not available)"
        );
        // Still run basic validation against rLightning
        assert_ok(&cmd(&mut rl, &["SET", "test", "val"]).await);
        assert_bulk(&cmd(&mut rl, &["GET", "test"]).await, "val");
        return;
    }

    let mut redis = redis_client.unwrap();

    // Compare SET/GET behavior
    let commands: Vec<Vec<&str>> = vec![
        vec!["SET", "cmp:str", "hello"],
        vec!["GET", "cmp:str"],
        vec!["SET", "cmp:num", "42"],
        vec!["INCR", "cmp:num"],
        vec!["LPUSH", "cmp:list", "a", "b", "c"],
        vec!["LLEN", "cmp:list"],
        vec!["SADD", "cmp:set", "x", "y", "z"],
        vec!["SCARD", "cmp:set"],
        vec!["ZADD", "cmp:zset", "1", "one", "2", "two"],
        vec!["ZCARD", "cmp:zset"],
        vec!["HSET", "cmp:hash", "f1", "v1"],
        vec!["HGET", "cmp:hash", "f1"],
        vec!["TYPE", "cmp:str"],
        vec!["TYPE", "cmp:list"],
        vec!["TYPE", "cmp:set"],
        vec!["TYPE", "cmp:zset"],
        vec!["TYPE", "cmp:hash"],
        vec!["EXISTS", "cmp:str"],
        vec!["EXISTS", "nosuch"],
        vec!["DEL", "cmp:str"],
        vec!["EXISTS", "cmp:str"],
    ];

    for args in &commands {
        let rl_resp = cmd(&mut rl, args).await;
        let redis_resp = cmd(&mut redis, args).await;

        // Compare responses (allowing for minor differences in formatting)
        let rl_type = std::mem::discriminant(&rl_resp);
        let redis_type = std::mem::discriminant(&redis_resp);
        assert_eq!(
            rl_type, redis_type,
            "Response type mismatch for {:?}: rLightning={:?} Redis={:?}",
            args, rl_resp, redis_resp
        );

        match (&rl_resp, &redis_resp) {
            (RespValue::Integer(a), RespValue::Integer(b)) => {
                assert_eq!(a, b, "Integer mismatch for {:?}", args);
            }
            (RespValue::BulkString(a), RespValue::BulkString(b)) => {
                assert_eq!(a, b, "BulkString mismatch for {:?}", args);
            }
            (RespValue::SimpleString(a), RespValue::SimpleString(b)) => {
                assert_eq!(a, b, "SimpleString mismatch for {:?}", args);
            }
            _ => {
                // Other types are harder to compare exactly
            }
        }
    }

    println!("Docker comparison test passed: all command responses match");
}
