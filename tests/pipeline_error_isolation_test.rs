use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

use std::sync::Arc;

/// Helper: serialize a RESP command from string arguments
fn build_resp_command(args: &[&str]) -> Vec<u8> {
    let items: Vec<RespValue> = args
        .iter()
        .map(|a| RespValue::BulkString(Some(a.as_bytes().to_vec())))
        .collect();
    let cmd = RespValue::Array(Some(items));
    cmd.serialize().unwrap()
}

/// Helper: read exactly N RESP responses from a buffer + stream
async fn read_n_responses(
    stream: &mut TcpStream,
    buffer: &mut BytesMut,
    n: usize,
) -> Result<Vec<RespValue>, Box<dyn std::error::Error + Send + Sync>> {
    let mut responses = Vec::with_capacity(n);
    for _ in 0..n {
        loop {
            if let Some(value) = RespValue::parse(buffer)? {
                responses.push(value);
                break;
            }
            let bytes_read = stream.read_buf(buffer).await?;
            if bytes_read == 0 {
                return Err("Connection closed before all responses received".into());
            }
        }
    }
    Ok(responses)
}

/// Test: pipeline [SET k "hello", LPUSH k "bad", GET k]
/// LPUSH should return WRONGTYPE error, SET and GET should succeed.
/// This verifies error isolation - one command's error doesn't kill the pipeline.
#[tokio::test]
async fn test_pipeline_error_isolation_wrongtype()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = "127.0.0.1:17200".parse()?;
    let config = StorageConfig::default();
    let storage = Arc::new(StorageEngine::new(config));

    let server = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024);

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    sleep(Duration::from_millis(200)).await;

    let mut stream = TcpStream::connect(addr).await?;
    let mut buffer = BytesMut::with_capacity(4096);

    // Build pipeline: SET k "hello" | LPUSH k "bad" | GET k
    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&build_resp_command(&["SET", "pipeline_test_key", "hello"]));
    pipeline.extend_from_slice(&build_resp_command(&["LPUSH", "pipeline_test_key", "bad"]));
    pipeline.extend_from_slice(&build_resp_command(&["GET", "pipeline_test_key"]));

    // Send all commands at once
    stream.write_all(&pipeline).await?;

    // Read all 3 responses
    let responses = timeout(
        Duration::from_secs(5),
        read_n_responses(&mut stream, &mut buffer, 3),
    )
    .await??;

    assert_eq!(
        responses.len(),
        3,
        "Expected exactly 3 responses from pipeline"
    );

    // Response 1: SET should succeed with OK
    assert_eq!(
        responses[0],
        RespValue::SimpleString("OK".to_string()),
        "SET should return OK"
    );

    // Response 2: LPUSH on a string key should return WRONGTYPE error
    match &responses[1] {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("WRONGTYPE"),
                "Expected WRONGTYPE error, got: {}",
                msg
            );
        }
        other => panic!(
            "Expected Error response for LPUSH on string key, got: {:?}",
            other
        ),
    }

    // Response 3: GET should still return "hello" (not affected by LPUSH error)
    match &responses[2] {
        RespValue::BulkString(Some(data)) => {
            assert_eq!(
                std::str::from_utf8(data)?,
                "hello",
                "GET should return the original value set by SET"
            );
        }
        other => panic!("Expected BulkString for GET response, got: {:?}", other),
    }

    Ok(())
}

/// Test: pipeline with multiple error types interspersed with successes
/// [SET a "1", INCR a, LPUSH a "x", INCR a, GET a]
/// Expected: OK, 2, WRONGTYPE, 3, "3"
#[tokio::test]
async fn test_pipeline_multiple_errors_interspersed()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = "127.0.0.1:17201".parse()?;
    let config = StorageConfig::default();
    let storage = Arc::new(StorageEngine::new(config));

    let server = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024);

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    sleep(Duration::from_millis(200)).await;

    let mut stream = TcpStream::connect(addr).await?;
    let mut buffer = BytesMut::with_capacity(4096);

    // Build pipeline
    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&build_resp_command(&["SET", "pipe_multi_key", "1"]));
    pipeline.extend_from_slice(&build_resp_command(&["INCR", "pipe_multi_key"]));
    pipeline.extend_from_slice(&build_resp_command(&["LPUSH", "pipe_multi_key", "x"]));
    pipeline.extend_from_slice(&build_resp_command(&["INCR", "pipe_multi_key"]));
    pipeline.extend_from_slice(&build_resp_command(&["GET", "pipe_multi_key"]));

    stream.write_all(&pipeline).await?;

    let responses = timeout(
        Duration::from_secs(5),
        read_n_responses(&mut stream, &mut buffer, 5),
    )
    .await??;

    assert_eq!(responses.len(), 5);

    // SET a "1" -> OK
    assert_eq!(responses[0], RespValue::SimpleString("OK".to_string()));

    // INCR a -> 2
    assert_eq!(responses[1], RespValue::Integer(2));

    // LPUSH a "x" -> WRONGTYPE error
    match &responses[2] {
        RespValue::Error(msg) => assert!(msg.contains("WRONGTYPE")),
        other => panic!("Expected WRONGTYPE error, got: {:?}", other),
    }

    // INCR a -> 3 (should still work after LPUSH error)
    assert_eq!(responses[3], RespValue::Integer(3));

    // GET a -> "3"
    match &responses[4] {
        RespValue::BulkString(Some(data)) => {
            assert_eq!(std::str::from_utf8(data)?, "3");
        }
        other => panic!("Expected BulkString '3', got: {:?}", other),
    }

    Ok(())
}

/// Test: pipeline with unknown command error isolation
/// [SET k "val", UNKNOWNCMD, GET k]
/// Expected: OK, ERR unknown command, "val"
#[tokio::test]
async fn test_pipeline_unknown_command_isolation()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = "127.0.0.1:17202".parse()?;
    let config = StorageConfig::default();
    let storage = Arc::new(StorageEngine::new(config));

    let server = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024);

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    sleep(Duration::from_millis(200)).await;

    let mut stream = TcpStream::connect(addr).await?;
    let mut buffer = BytesMut::with_capacity(4096);

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&build_resp_command(&["SET", "pipe_unk_key", "val"]));
    pipeline.extend_from_slice(&build_resp_command(&["UNKNOWNCMD", "arg1"]));
    pipeline.extend_from_slice(&build_resp_command(&["GET", "pipe_unk_key"]));

    stream.write_all(&pipeline).await?;

    let responses = timeout(
        Duration::from_secs(5),
        read_n_responses(&mut stream, &mut buffer, 3),
    )
    .await??;

    assert_eq!(responses.len(), 3);

    // SET -> OK
    assert_eq!(responses[0], RespValue::SimpleString("OK".to_string()));

    // UNKNOWNCMD -> ERR
    match &responses[1] {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("unknown command") || msg.contains("ERR"),
                "Expected unknown command error, got: {}",
                msg
            );
        }
        other => panic!("Expected Error for unknown command, got: {:?}", other),
    }

    // GET -> "val"
    match &responses[2] {
        RespValue::BulkString(Some(data)) => {
            assert_eq!(std::str::from_utf8(data)?, "val");
        }
        other => panic!("Expected BulkString 'val', got: {:?}", other),
    }

    Ok(())
}

/// Test: pipeline with wrong number of arguments error
/// [SET k, GET k "extra_arg", SET k "val", GET k]
/// Expected: ERR wrong args, ERR wrong args, OK, "val"
#[tokio::test]
async fn test_pipeline_wrong_args_isolation() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    let addr: SocketAddr = "127.0.0.1:17203".parse()?;
    let config = StorageConfig::default();
    let storage = Arc::new(StorageEngine::new(config));

    let server = Server::new(addr, Arc::clone(&storage))
        .with_connection_limit(100)
        .with_buffer_size(1024 * 1024);

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    sleep(Duration::from_millis(200)).await;

    let mut stream = TcpStream::connect(addr).await?;
    let mut buffer = BytesMut::with_capacity(4096);

    let mut pipeline = Vec::new();
    pipeline.extend_from_slice(&build_resp_command(&["SET", "pipe_args_key"])); // Missing value
    pipeline.extend_from_slice(&build_resp_command(&["SET", "pipe_args_key", "val"]));
    pipeline.extend_from_slice(&build_resp_command(&["GET", "pipe_args_key"]));

    stream.write_all(&pipeline).await?;

    let responses = timeout(
        Duration::from_secs(5),
        read_n_responses(&mut stream, &mut buffer, 3),
    )
    .await??;

    assert_eq!(responses.len(), 3);

    // SET with missing value -> ERR
    match &responses[0] {
        RespValue::Error(msg) => {
            assert!(
                msg.contains("ERR") || msg.contains("wrong number"),
                "Expected error for wrong args, got: {}",
                msg
            );
        }
        other => panic!("Expected Error for SET with missing arg, got: {:?}", other),
    }

    // SET k "val" -> OK
    assert_eq!(responses[1], RespValue::SimpleString("OK".to_string()));

    // GET k -> "val"
    match &responses[2] {
        RespValue::BulkString(Some(data)) => {
            assert_eq!(std::str::from_utf8(data)?, "val");
        }
        other => panic!("Expected BulkString 'val', got: {:?}", other),
    }

    Ok(())
}
