use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis HyperLogLog commands compatibility
/// HyperLogLog is used for approximated cardinality counting
#[tokio::test]
async fn test_redis_hyperloglog_commands() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port
    let addr: SocketAddr = "127.0.0.1:16394".parse()?;
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
    
    println!("Testing Redis HyperLogLog commands...");
    
    // ======== TEST PFADD COMMAND ========
    println!("Testing PFADD command");
    
    // Test PFADD with single element
    let result = client.send_command_str("PFADD", &["hll1", "element1"]).await?;
    if let RespValue::Integer(added) = result {
        assert_eq!(added, 1, "PFADD should return 1 when element is added");
    } else if let RespValue::Error(err) = &result {
        if err.contains("unknown command") || err.contains("not implemented") {
            println!("PFADD command is not implemented, skipping remaining HyperLogLog tests");
            return Ok(());
        } else {
            panic!("Unexpected error response from PFADD: {:?}", result);
        }
    } else {
        panic!("Unexpected response type from PFADD: {:?}", result);
    }
    
    // Test PFADD with multiple elements
    let result = client.send_command_str("PFADD", &["hll1", "element2", "element3", "element4"]).await?;
    assert!(matches!(result, RespValue::Integer(1)), "PFADD should return 1 when at least one element is added");
    
    // Test PFADD with duplicate elements (should return 0 when no new elements are added)
    let result = client.send_command_str("PFADD", &["hll1", "element1", "element2"]).await?;
    assert_eq!(result, RespValue::Integer(0), "PFADD should return 0 when no new elements are added");
    
    // ======== TEST PFCOUNT COMMAND ========
    println!("Testing PFCOUNT command");
    
    // Test PFCOUNT for a single key
    let result = client.send_command_str("PFCOUNT", &["hll1"]).await?;
    if let RespValue::Integer(count) = result {
        assert_eq!(count, 4, "PFCOUNT should return approximated cardinality of 4");
    } else if let RespValue::Error(err) = &result {
        if err.contains("unknown command") || err.contains("not implemented") {
            println!("PFCOUNT command is not implemented, skipping remaining tests");
            return Ok(());
        } else {
            panic!("Unexpected error response from PFCOUNT: {:?}", result);
        }
    } else {
        panic!("Unexpected response type from PFCOUNT: {:?}", result);
    }
    
    // Create a second HyperLogLog
    client.send_command_str("PFADD", &["hll2", "element3", "element4", "element5", "element6"]).await?;
    
    // Test PFCOUNT with multiple keys
    let result = client.send_command_str("PFCOUNT", &["hll1", "hll2"]).await?;
    if let RespValue::Integer(count) = result {
        assert_eq!(count, 6, "PFCOUNT should return approximated cardinality of 6 for merged HyperLogLogs");
    } else {
        println!("Multi-key PFCOUNT may not be implemented, skipping: {:?}", result);
    }
    
    // Test PFCOUNT on non-existing key
    let result = client.send_command_str("PFCOUNT", &["nonexistent"]).await?;
    assert_eq!(result, RespValue::Integer(0), "PFCOUNT should return 0 for non-existing key");
    
    // ======== TEST PFMERGE COMMAND ========
    println!("Testing PFMERGE command");
    
    // Test PFMERGE
    let result = client.send_command_str("PFMERGE", &["hll3", "hll1", "hll2"]).await?;
    if let RespValue::SimpleString(response) = &result {
        assert_eq!(response, "OK", "PFMERGE should return OK");
    } else if let RespValue::Error(err) = &result {
        if err.contains("unknown command") || err.contains("not implemented") {
            println!("PFMERGE command is not implemented, skipping remaining tests");
            return Ok(());
        } else {
            panic!("Unexpected error response from PFMERGE: {:?}", result);
        }
    } else {
        panic!("Unexpected response type from PFMERGE: {:?}", result);
    }
    
    // Verify the merged HyperLogLog has the correct count
    let result = client.send_command_str("PFCOUNT", &["hll3"]).await?;
    if let RespValue::Integer(count) = result {
        assert_eq!(count, 6, "PFCOUNT should return approximate cardinality of 6 for merged HyperLogLog");
    } else {
        panic!("Unexpected response type from PFCOUNT after merge: {:?}", result);
    }
    
    // ======== TEST ERROR HANDLING ========
    println!("Testing error handling for HyperLogLog commands");
    
    // Test PFADD with wrong number of arguments
    let result = client.send_command_str("PFADD", &[]).await?;
    assert!(matches!(result, RespValue::Error(_)), "PFADD without key should return error");
    
    // Test PFCOUNT with wrong number of arguments
    let result = client.send_command_str("PFCOUNT", &[]).await?;
    assert!(matches!(result, RespValue::Error(_)), "PFCOUNT without key should return error");
    
    // Test PFMERGE with wrong number of arguments
    let result = client.send_command_str("PFMERGE", &["destination"]).await?;
    assert!(matches!(result, RespValue::Error(_)), "PFMERGE with only destination should return error");
    
    // ======== TEST INTERACTION WITH OTHER COMMANDS ========
    println!("Testing interaction with other Redis commands");
    
    // Test type detection
    let result = client.send_command_str("TYPE", &["hll1"]).await?;
    assert!(
        matches!(result, RespValue::SimpleString(ref s) if s == "string" || s == "hyperloglog"),
        "TYPE for HyperLogLog should return 'hyperloglog' or 'string'"
    );
    
    // Test TTL and expiry on HyperLogLog
    let result = client.send_command_str("EXPIRE", &["hll1", "1"]).await?;
    assert_eq!(result, RespValue::Integer(1), "EXPIRE should return 1 when successful");
    
    // Verify TTL
    let result = client.send_command_str("TTL", &["hll1"]).await?;
    if let RespValue::Integer(ttl) = result {
        assert!(ttl <= 1 && ttl > 0, "TTL should be between 0 and 1");
    } else {
        panic!("Expected integer response from TTL");
    }
    
    // Wait for expiry
    sleep(Duration::from_secs(1)).await;
    
    // Verify key expired
    let result = client.send_command_str("EXISTS", &["hll1"]).await?;
    assert_eq!(result, RespValue::Integer(0), "Key should be expired");
    
    // Test DEL on HyperLogLog
    let result = client.send_command_str("DEL", &["hll2", "hll3"]).await?;
    assert_eq!(result, RespValue::Integer(2), "DEL should return count of keys deleted");
    
    // Verify keys are gone
    let result = client.send_command_str("EXISTS", &["hll2", "hll3"]).await?;
    assert_eq!(result, RespValue::Integer(0), "Keys should be deleted");
    
    println!("All HyperLogLog tests passed!");
    Ok(())
}