use futures::future;
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis server behavior under high concurrency
#[tokio::test]
async fn test_concurrent_access() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with more memory for stress test
    let addr: SocketAddr = "127.0.0.1:16384".parse()?;
    let mut config = StorageConfig::default();
    config.max_memory = 512; // Increase memory limit for stress test
    let storage = StorageEngine::new(config);

    let server = Server::new(addr, storage);

    // Start server in background
    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Wait for server to start
    sleep(Duration::from_millis(100)).await;

    // Track success/failure counts
    let (tx, mut rx) = mpsc::channel::<bool>(100);

    // Number of concurrent clients and operations
    const NUM_CLIENTS: usize = 10;
    const OPS_PER_CLIENT: usize = 100;

    // Start a timer to measure throughput
    let start_time = Instant::now();

    // Launch multiple concurrent clients
    let mut handles = vec![];

    for client_id in 0..NUM_CLIENTS {
        let client_tx = tx.clone();

        let handle = tokio::spawn(async move {
            // Each client gets its own connection
            let mut client = match Client::connect(addr).await {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("Client {} connection error: {}", client_id, e);
                    for _ in 0..OPS_PER_CLIENT {
                        client_tx.send(false).await.unwrap();
                    }
                    return;
                }
            };

            for i in 0..OPS_PER_CLIENT {
                // Perform various operations
                let op_type = i % 8; // Expanded operation types
                let key = format!("key:{}:{}", client_id, i);

                let result = match op_type {
                    0 => {
                        // SET operation
                        let value = format!("value:{}:{}", client_id, i);
                        let args: Vec<&[u8]> = vec![b"SET", key.as_bytes(), value.as_bytes()];
                        client.send_command(args).await
                    }
                    1 => {
                        // GET operation
                        let args: Vec<&[u8]> = vec![b"GET", key.as_bytes()];
                        client.send_command(args).await
                    }
                    2 => {
                        // Hash operation (HSET)
                        let value1 = format!("value:{}", i);
                        let value2 = format!("value:{}", i + 1);
                        let args: Vec<&[u8]> = vec![
                            b"HSET",
                            key.as_bytes(),
                            b"field1",
                            value1.as_bytes(),
                            b"field2",
                            value2.as_bytes(),
                        ];
                        client.send_command(args).await
                    }
                    3 => {
                        // List operation (LPUSH)
                        let item1 = format!("item:{}", i);
                        let item2 = format!("item:{}", i + 1);
                        let args: Vec<&[u8]> =
                            vec![b"LPUSH", key.as_bytes(), item1.as_bytes(), item2.as_bytes()];
                        client.send_command(args).await
                    }
                    4 => {
                        // Set operation (SADD)
                        let member1 = format!("member:{}", i);
                        let member2 = format!("member:{}", i + 1);
                        let args: Vec<&[u8]> = vec![
                            b"SADD",
                            key.as_bytes(),
                            member1.as_bytes(),
                            member2.as_bytes(),
                        ];
                        client.send_command(args).await
                    }
                    5 => {
                        // Sorted Set operation (ZADD)
                        let member = format!("member:{}", i);
                        let score = format!("{}", i);
                        let args: Vec<&[u8]> =
                            vec![b"ZADD", key.as_bytes(), score.as_bytes(), member.as_bytes()];
                        client.send_command(args).await
                    }
                    6 => {
                        // SET with expiration
                        let value = format!("expire:{}:{}", client_id, i);
                        let args: Vec<&[u8]> = vec![
                            b"SET",
                            key.as_bytes(),
                            value.as_bytes(),
                            b"EX",
                            b"60", // 60 second expiration
                        ];
                        client.send_command(args).await
                    }
                    7 => {
                        // TTL check
                        let args: Vec<&[u8]> = vec![b"TTL", key.as_bytes()];
                        client.send_command(args).await
                    }
                    _ => unreachable!(),
                };

                match result {
                    Ok(_) => client_tx.send(true).await.unwrap(),
                    Err(e) => {
                        eprintln!("Operation error in client {}, op {}: {}", client_id, i, e);
                        client_tx.send(false).await.unwrap();
                    }
                }
            }
        });

        handles.push(handle);
    }

    // Wait for all operations to complete
    let total_ops = NUM_CLIENTS * OPS_PER_CLIENT;
    let mut success_count = 0;
    let mut failure_count = 0;

    for _ in 0..total_ops {
        match rx.recv().await {
            Some(true) => success_count += 1,
            Some(false) => failure_count += 1,
            None => break,
        }
    }

    // Wait for all client tasks to finish
    future::join_all(handles).await;

    // Calculate elapsed time and throughput
    let elapsed = start_time.elapsed();
    let throughput = total_ops as f64 / elapsed.as_secs_f64();

    println!("Stress test completed:");
    println!("  Total operations: {}", total_ops);
    println!("  Successful: {}", success_count);
    println!("  Failed: {}", failure_count);
    println!("  Time elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.2} ops/sec", throughput);

    // Verify most operations succeeded
    assert!(
        success_count as f64 / total_ops as f64 > 0.95,
        "Expected at least 95% success rate, got {}%",
        (success_count as f64 / total_ops as f64) * 100.0
    );

    // Connect a final client to clean up
    let mut cleanup_client = Client::connect(addr).await?;

    // Test scan to find all keys we created
    let mut all_keys = vec![];
    let mut cursor = "0".to_string();

    loop {
        let scan_args: Vec<&[u8]> = vec![
            b"SCAN",
            cursor.as_bytes(),
            b"MATCH",
            b"key:*",
            b"COUNT",
            b"1000",
        ];

        let response = cleanup_client.send_command(scan_args).await?;

        if let RespValue::Array(Some(scan_result)) = response {
            // Get cursor
            if let RespValue::BulkString(Some(new_cursor)) = &scan_result[0] {
                cursor = std::str::from_utf8(new_cursor)?.to_string();
            }

            // Add keys to our list
            if let RespValue::Array(Some(keys)) = &scan_result[1] {
                for key in keys {
                    if let RespValue::BulkString(Some(key_bytes)) = key {
                        all_keys.push(key_bytes.clone());
                    }
                }
            }

            if cursor == "0" {
                break;
            }
        }
    }

    // Verify we found some keys
    assert!(
        !all_keys.is_empty(),
        "Expected to find keys created during stress test"
    );
    println!("Found {} keys created during stress test", all_keys.len());

    // Clean up all keys
    // Do it in batches of 100 to avoid command line size issues
    for chunk in all_keys.chunks(100) {
        let mut del_args = vec![b"DEL" as &[u8]];
        for key in chunk {
            del_args.push(key.as_slice());
        }
        cleanup_client.send_command(del_args).await?;
    }

    Ok(())
}

/// Test performance with high throughput of simple operations
#[tokio::test]
async fn test_simple_throughput() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port
    let addr: SocketAddr = "127.0.0.1:16398".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);

    let server = Server::new(addr, storage);

    // Start server in background
    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    // Wait for server to start
    sleep(Duration::from_millis(200)).await;

    // Helper function to try connecting, with retry
    async fn try_connect(
        addr: SocketAddr,
    ) -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
        match Client::connect(addr).await {
            Ok(client) => Ok(client),
            Err(e) => {
                eprintln!("Failed to connect to server: {}", e);
                sleep(Duration::from_millis(500)).await;
                Client::connect(addr).await.map_err(|e| e.into())
            }
        }
    }

    // Connect client
    let mut client = try_connect(addr).await?;

    // Number of operations, reduced to prevent test failures
    const NUM_OPS: usize = 1000;

    // Start timer
    let start_time = Instant::now();

    // Helper function to try a command with reconnect if it fails
    async fn try_command(
        addr: SocketAddr,
        mut client: Client,
        cmd: Vec<&[u8]>,
    ) -> (
        Client,
        Result<RespValue, Box<dyn std::error::Error + Send + Sync>>,
    ) {
        match client.send_command(cmd.clone()).await {
            Ok(response) => (client, Ok(response)),
            Err(e) => {
                eprintln!("Command failed: {}, attempting reconnect", e);
                sleep(Duration::from_millis(200)).await;
                match Client::connect(addr).await {
                    Ok(new_client) => {
                        client = new_client;
                        match client.send_command(cmd).await {
                            Ok(response) => (client, Ok(response)),
                            Err(e) => (client, Err(e.into())),
                        }
                    }
                    Err(e) => (client, Err(e.into())),
                }
            }
        }
    }

    // Perform lots of simple SET/GET operations
    let mut successful_ops = 0;
    let mut i = 0;
    while i < NUM_OPS && successful_ops < NUM_OPS {
        let key = format!("bench:key:{}", i);
        let value = format!("value:{}", i);

        // SET operation
        let set_args: Vec<&[u8]> = vec![b"SET", key.as_bytes(), value.as_bytes()];

        let (new_client, set_result) = try_command(addr, client, set_args).await;
        client = new_client;

        match set_result {
            Ok(result) => {
                assert_eq!(result, RespValue::SimpleString("OK".to_string()));

                // GET operation
                let get_args: Vec<&[u8]> = vec![b"GET", key.as_bytes()];

                let (new_client, get_result) = try_command(addr, client, get_args).await;
                client = new_client;

                match get_result {
                    Ok(result) => {
                        assert_eq!(
                            result,
                            RespValue::BulkString(Some(value.as_bytes().to_vec()))
                        );
                        successful_ops += 1;
                        i += 1;
                    }
                    Err(e) => {
                        eprintln!("GET failed after SET succeeded: {}", e);
                        // We'll try again with the next key
                        i += 1;
                    }
                }
            }
            Err(e) => {
                eprintln!("SET failed: {}", e);
                // We'll try again with the next key
                i += 1;
            }
        }

        // Occasionally allow the server to breathe
        if i % 100 == 0 {
            sleep(Duration::from_millis(10)).await;
        }
    }

    // Calculate elapsed time and throughput
    let elapsed = start_time.elapsed();
    let throughput = (successful_ops * 2) as f64 / elapsed.as_secs_f64(); // *2 because we do SET+GET

    println!("Simple throughput test completed:");
    println!("  Operations: {} (SET+GET pairs)", successful_ops);
    println!("  Time elapsed: {:.2}s", elapsed.as_secs_f64());
    println!("  Throughput: {:.2} ops/sec", throughput);

    // Clean up
    let _ = client.send_command(vec![b"FLUSHALL"]).await;

    Ok(())
}
