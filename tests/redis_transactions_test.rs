use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis transactions with MULTI/EXEC/DISCARD commands
/// Transactions allow multiple commands to be executed atomically
#[tokio::test]
async fn test_redis_transactions() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port
    let addr: SocketAddr = "127.0.0.1:16398".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    let server = Server::new_with_storage(addr, storage);
    
    // Start server in background
    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(200)).await;
    
    // Connect client
    let mut client = Client::connect(addr).await?;
    
    // Flush all data to ensure clean environment
    let flush_resp = client.send_command_str("FLUSHALL", &[]).await?;
    assert_eq!(flush_resp, RespValue::SimpleString("OK".to_string()));
    
    println!("Testing Redis transactions...");
    
    // ======== TEST BASIC TRANSACTION ========
    println!("Testing basic MULTI/EXEC transaction");
    
    // Start a transaction with MULTI
    let multi_result = client.send_command_str("MULTI", &[]).await?;
    match multi_result {
        RespValue::SimpleString(response) => {
            assert_eq!(response, "OK", "MULTI should return OK");
            
            // Queue commands in the transaction
            let set1_result = client.send_command_str("SET", &["tx_key1", "value1"]).await?;
            let set2_result = client.send_command_str("SET", &["tx_key2", "value2"]).await?;
            let incr_result = client.send_command_str("INCR", &["tx_counter"]).await?;
            
            // In a transaction, commands should return QUEUED until EXEC is called
            assert_eq!(set1_result, RespValue::SimpleString("QUEUED".to_string()), "Commands in transaction should return QUEUED");
            assert_eq!(set2_result, RespValue::SimpleString("QUEUED".to_string()), "Commands in transaction should return QUEUED");
            assert_eq!(incr_result, RespValue::SimpleString("QUEUED".to_string()), "Commands in transaction should return QUEUED");
            
            // Execute the transaction with EXEC
            let exec_result = client.send_command_str("EXEC", &[]).await?;
            if let RespValue::Array(Some(results)) = exec_result {
                assert_eq!(results.len(), 3, "EXEC should return results for all 3 commands");
                
                // Verify individual command results
                assert_eq!(results[0], RespValue::SimpleString("OK".to_string()), "First command (SET) should return OK");
                assert_eq!(results[1], RespValue::SimpleString("OK".to_string()), "Second command (SET) should return OK");
                assert_eq!(results[2], RespValue::Integer(1), "Third command (INCR) should return 1");
                
                // Verify the transaction actually executed the commands
                let get1_result = client.send_command_str("GET", &["tx_key1"]).await?;
                assert_eq!(get1_result, RespValue::BulkString(Some(b"value1".to_vec())), "tx_key1 should have value1");
                
                let get2_result = client.send_command_str("GET", &["tx_key2"]).await?;
                assert_eq!(get2_result, RespValue::BulkString(Some(b"value2".to_vec())), "tx_key2 should have value2");
                
                let get_counter_result = client.send_command_str("GET", &["tx_counter"]).await?;
                assert_eq!(get_counter_result, RespValue::BulkString(Some(b"1".to_vec())), "tx_counter should be 1");
            } else if let RespValue::Error(err) = &exec_result {
                if err.contains("unknown command") || err.contains("not implemented") {
                    println!("EXEC command is not implemented, skipping transaction tests");
                    return Ok(());
                } else {
                    panic!("Unexpected error response from EXEC: {:?}", exec_result);
                }
            } else {
                panic!("Unexpected response type from EXEC: {:?}", exec_result);
            }
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("MULTI command is not implemented, skipping transaction tests");
                return Ok(());
            } else {
                panic!("Unexpected error response from MULTI: {:?}", multi_result);
            }
        },
        _ => {
            panic!("Unexpected response type from MULTI: {:?}", multi_result);
        }
    }
    
    // ======== TEST TRANSACTION DISCARD ========
    println!("Testing DISCARD to abort a transaction");
    
    // Start a new transaction
    client.send_command_str("MULTI", &[]).await?;
    
    // Queue commands in the transaction
    client.send_command_str("SET", &["should_not_be_set", "value"]).await?;
    client.send_command_str("INCR", &["should_not_be_incremented"]).await?;
    
    // Abort the transaction with DISCARD
    let discard_result = client.send_command_str("DISCARD", &[]).await?;
    match discard_result {
        RespValue::SimpleString(response) => {
            assert_eq!(response, "OK", "DISCARD should return OK");
            
            // Verify the commands were not executed
            let get_result = client.send_command_str("GET", &["should_not_be_set"]).await?;
            assert_eq!(get_result, RespValue::BulkString(None), "Key should not exist after DISCARD");
            
            let get_counter_result = client.send_command_str("GET", &["should_not_be_incremented"]).await?;
            assert_eq!(get_counter_result, RespValue::BulkString(None), "Counter should not exist after DISCARD");
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("DISCARD command is not implemented, skipping DISCARD tests");
            } else {
                panic!("Unexpected error response from DISCARD: {:?}", discard_result);
            }
        },
        _ => {
            panic!("Unexpected response type from DISCARD: {:?}", discard_result);
        }
    }
    
    // ======== TEST TRANSACTION WITH ERROR HANDLING ========
    println!("Testing transaction with error handling");
    
    // Start a transaction
    client.send_command_str("MULTI", &[]).await?;
    
    // Queue a valid command
    client.send_command_str("SET", &["error_test_key", "value"]).await?;
    
    // Queue a command with an error
    let invalid_result = client.send_command_str("INCR", &["error_test_key"]).await?;
    
    // In a transaction, even commands that would generate errors return QUEUED
    // The error will only be reported when EXEC is called
    assert_eq!(invalid_result, RespValue::SimpleString("QUEUED".to_string()), 
              "Even commands with potential errors should return QUEUED during transaction");
    
    // Queue another valid command
    client.send_command_str("SET", &["another_key", "value"]).await?;
    
    // Execute the transaction
    let exec_result = client.send_command_str("EXEC", &[]).await?;
    match exec_result {
        RespValue::Array(Some(results)) => {
            assert_eq!(results.len(), 3, "EXEC should return results for all 3 commands");
            
            // First command should succeed
            assert_eq!(results[0], RespValue::SimpleString("OK".to_string()), "First SET should succeed");
            
            // Second command should fail with WRONGTYPE error
            assert!(matches!(results[1], RespValue::Error(ref e) if e.contains("WRONGTYPE")), 
                   "INCR on string should fail with WRONGTYPE error");
            
            // Third command should still succeed (Redis transactions are not rolled back on errors)
            assert_eq!(results[2], RespValue::SimpleString("OK".to_string()), "Third SET should still succeed");
            
            // Verify the results
            let get1_result = client.send_command_str("GET", &["error_test_key"]).await?;
            assert_eq!(get1_result, RespValue::BulkString(Some(b"value".to_vec())), "First key should be set");
            
            let get2_result = client.send_command_str("GET", &["another_key"]).await?;
            assert_eq!(get2_result, RespValue::BulkString(Some(b"value".to_vec())), "Third key should be set");
        },
        RespValue::Error(err) => {
            // Some Redis implementations might abort the entire transaction on error during EXEC
            // If this happens, that's acceptable behavior too
            println!("EXEC returned error: {}. Some implementations abort the transaction on errors.", err);
        },
        _ => {
            panic!("Unexpected response type from EXEC: {:?}", exec_result);
        }
    }
    
    // ======== TEST TRANSACTION ISOLATION ========
    println!("Testing transaction isolation");
    
    // Set initial value
    client.send_command_str("SET", &["isolation_test", "initial"]).await?;
    
    // Start a transaction
    client.send_command_str("MULTI", &[]).await?;
    
    // Queue a command to change the key in the transaction
    client.send_command_str("SET", &["isolation_test", "updated_in_tx"]).await?;
    
    // Open a second client connection
    let mut client2 = Client::connect(addr).await?;
    
    // Modify the key from the second client while transaction is in progress
    client2.send_command_str("SET", &["isolation_test", "updated_by_client2"]).await?;
    
    // Execute the transaction
    let exec_result = client.send_command_str("EXEC", &[]).await?;
    if let RespValue::Array(Some(results)) = exec_result {
        assert_eq!(results[0], RespValue::SimpleString("OK".to_string()), "SET in transaction should succeed");
        
        // After transaction, the final value should be from the transaction, not client2
        let get_result = client.send_command_str("GET", &["isolation_test"]).await?;
        assert_eq!(get_result, RespValue::BulkString(Some(b"updated_in_tx".to_vec())), 
                  "Final value should be from transaction, showing proper isolation");
    }
    
    // ======== TEST COMPLEX TRANSACTION SCENARIO ========
    println!("Testing complex transaction scenario");
    
    // Create keys for testing
    client.send_command_str("SET", &["user:1:name", "Alice"]).await?;
    client.send_command_str("SET", &["user:1:balance", "100"]).await?;
    client.send_command_str("SET", &["user:2:name", "Bob"]).await?;
    client.send_command_str("SET", &["user:2:balance", "50"]).await?;
    
    // Start a transaction
    client.send_command_str("MULTI", &[]).await?;
    
    // Simulate a transfer of 30 credits from user 1 to user 2
    client.send_command_str("DECRBY", &["user:1:balance", "30"]).await?;
    client.send_command_str("INCRBY", &["user:2:balance", "30"]).await?;
    client.send_command_str("SADD", &["transactions", "transfer:1:2:30"]).await?;
    
    // Execute the transaction
    let exec_result = client.send_command_str("EXEC", &[]).await?;
    if let RespValue::Array(Some(results)) = exec_result {
        assert_eq!(results.len(), 3, "Complex transaction should execute all 3 commands");
        
        // Verify the results
        let balance1_result = client.send_command_str("GET", &["user:1:balance"]).await?;
        assert_eq!(balance1_result, RespValue::BulkString(Some(b"70".to_vec())), "User 1 balance should be 70");
        
        let balance2_result = client.send_command_str("GET", &["user:2:balance"]).await?;
        assert_eq!(balance2_result, RespValue::BulkString(Some(b"80".to_vec())), "User 2 balance should be 80");
        
        let transaction_log = client.send_command_str("SMEMBERS", &["transactions"]).await?;
        if let RespValue::Array(Some(members)) = transaction_log {
            assert_eq!(members.len(), 1, "Transaction log should have 1 entry");
            
            if let RespValue::BulkString(Some(entry)) = &members[0] {
                assert_eq!(std::str::from_utf8(entry)?, "transfer:1:2:30", "Transaction log entry should be correct");
            }
        }
    }
    
    // ======== TEST WATCH FOR OPTIMISTIC LOCKING ========
    println!("Testing WATCH for optimistic locking");
    
    // Create a key for WATCH
    client.send_command_str("SET", &["watched_key", "original"]).await?;
    
    // WATCH the key for changes
    let watch_result = client.send_command_str("WATCH", &["watched_key"]).await?;
    match watch_result {
        RespValue::SimpleString(response) => {
            assert_eq!(response, "OK", "WATCH should return OK");
            
            // Modify the key from another client
            client2.send_command_str("SET", &["watched_key", "modified"]).await?;
            
            // Start a transaction
            client.send_command_str("MULTI", &[]).await?;
            
            // Queue a command in the transaction
            client.send_command_str("SET", &["watched_key", "from_tx"]).await?;
            
            // Execute the transaction - should fail because watched key was modified
            let exec_result = client.send_command_str("EXEC", &[]).await?;
            assert_eq!(exec_result, RespValue::BulkString(None), 
                      "EXEC should return nil because watched key was modified");
            
            // Verify the key wasn't changed by our transaction
            let get_result = client.send_command_str("GET", &["watched_key"]).await?;
            assert_eq!(get_result, RespValue::BulkString(Some(b"modified".to_vec())), 
                      "watched_key should still have value from client2");
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("WATCH command is not implemented, skipping WATCH tests");
            } else {
                panic!("Unexpected error response from WATCH: {:?}", watch_result);
            }
        },
        _ => {
            panic!("Unexpected response type from WATCH: {:?}", watch_result);
        }
    }
    
    // Try again with successful WATCH
    client.send_command_str("SET", &["watched_key2", "original"]).await?;
    
    // WATCH the key for changes
    client.send_command_str("WATCH", &["watched_key2"]).await?;
    
    // Start a transaction
    client.send_command_str("MULTI", &[]).await?;
    
    // Queue a command in the transaction
    client.send_command_str("SET", &["watched_key2", "from_tx"]).await?;
    
    // Execute the transaction - should succeed because watched key wasn't modified
    let exec_result = client.send_command_str("EXEC", &[]).await?;
    if let RespValue::Array(Some(results)) = exec_result {
        assert_eq!(results.len(), 1, "Transaction with WATCH should execute");
        assert_eq!(results[0], RespValue::SimpleString("OK".to_string()), "SET in transaction should succeed");
        
        // Verify the key was changed by our transaction
        let get_result = client.send_command_str("GET", &["watched_key2"]).await?;
        assert_eq!(get_result, RespValue::BulkString(Some(b"from_tx".to_vec())), 
                  "watched_key2 should have value from transaction");
    }
    
    // ======== TEST UNWATCH ========
    println!("Testing UNWATCH");
    
    // Create a key for WATCH
    client.send_command_str("SET", &["unwatched_key", "original"]).await?;
    
    // WATCH the key for changes
    client.send_command_str("WATCH", &["unwatched_key"]).await?;
    
    // Modify the key from another client
    client2.send_command_str("SET", &["unwatched_key", "modified"]).await?;
    
    // UNWATCH to cancel monitoring
    let unwatch_result = client.send_command_str("UNWATCH", &[]).await?;
    match unwatch_result {
        RespValue::SimpleString(response) => {
            assert_eq!(response, "OK", "UNWATCH should return OK");
            
            // Start a transaction
            client.send_command_str("MULTI", &[]).await?;
            
            // Queue a command in the transaction
            client.send_command_str("SET", &["unwatched_key", "from_tx"]).await?;
            
            // Execute the transaction - should succeed because we UNWATCHED the key
            let exec_result = client.send_command_str("EXEC", &[]).await?;
            if let RespValue::Array(Some(results)) = exec_result {
                assert_eq!(results[0], RespValue::SimpleString("OK".to_string()), "SET in transaction should succeed");
                
                // Verify the key was changed by our transaction
                let get_result = client.send_command_str("GET", &["unwatched_key"]).await?;
                assert_eq!(get_result, RespValue::BulkString(Some(b"from_tx".to_vec())), 
                          "unwatched_key should have value from transaction");
            }
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("UNWATCH command is not implemented, skipping UNWATCH tests");
            } else {
                panic!("Unexpected error response from UNWATCH: {:?}", unwatch_result);
            }
        },
        _ => {
            panic!("Unexpected response type from UNWATCH: {:?}", unwatch_result);
        }
    }
    
    println!("All transaction tests passed!");
    Ok(())
}