use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Test that GEOSEARCHSTORE destination key is properly tracked for transaction locking.
/// This verifies that WATCH on the destination key + MULTI + GEOSEARCHSTORE correctly
/// aborts the transaction if the destination key is modified between WATCH and EXEC.
#[tokio::test]
async fn test_watch_multi_geosearchstore_integrity() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr: SocketAddr = "127.0.0.1:16458".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);

    let server = Server::new(addr, storage);

    tokio::spawn(async move {
        if let Err(e) = server.start().await {
            eprintln!("Server error: {:?}", e);
        }
    });

    sleep(Duration::from_millis(200)).await;

    let mut client1 = Client::connect(addr).await?;
    let mut client2 = Client::connect(addr).await?;

    // Clean state
    let _ = client1.send_command_str("FLUSHALL", &[]).await?;

    // Add geo data to source key
    let _ = client1.send_command_str("GEOADD", &[
        "places", "13.361389", "38.115556", "Palermo",
    ]).await?;
    let _ = client1.send_command_str("GEOADD", &[
        "places", "15.087269", "37.502669", "Catania",
    ]).await?;
    let _ = client1.send_command_str("GEOADD", &[
        "places", "2.349014", "48.864716", "Paris",
    ]).await?;

    // === Test 1: GEOSEARCHSTORE succeeds inside MULTI when no concurrent modification ===
    println!("Test 1: GEOSEARCHSTORE in MULTI/EXEC without interference");

    let _ = client1.send_command_str("WATCH", &["nearby"]).await?;
    let _ = client1.send_command_str("MULTI", &[]).await?;
    let queued = client1.send_command_str("GEOSEARCHSTORE", &[
        "nearby", "places", "FROMLONLAT", "14.0", "38.0", "BYRADIUS", "200", "km",
    ]).await?;
    assert_eq!(queued, RespValue::SimpleString("QUEUED".to_string()));

    let exec_result = client1.send_command_str("EXEC", &[]).await?;
    // Transaction should succeed (not aborted)
    match &exec_result {
        RespValue::Array(Some(results)) => {
            assert_eq!(results.len(), 1, "EXEC should return 1 result");
            // GEOSEARCHSTORE returns the number of elements stored
            if let RespValue::Integer(count) = &results[0] {
                assert!(*count >= 1, "Should have stored at least 1 result (Palermo/Catania are near)");
                println!("  GEOSEARCHSTORE stored {} elements", count);
            } else {
                panic!("Expected integer result from GEOSEARCHSTORE, got: {:?}", results[0]);
            }
        }
        RespValue::BulkString(None) => {
            panic!("Transaction was aborted unexpectedly (nil EXEC)");
        }
        other => {
            panic!("Unexpected EXEC result: {:?}", other);
        }
    }

    // Verify the destination key has data
    let card = client1.send_command_str("ZCARD", &["nearby"]).await?;
    if let RespValue::Integer(n) = card {
        assert!(n >= 1, "nearby should have at least 1 member after GEOSEARCHSTORE");
        println!("  Destination key 'nearby' has {} members", n);
    }

    // Clean up for next test
    let _ = client1.send_command_str("DEL", &["nearby"]).await?;

    // === Test 2: WATCH on destination key detects concurrent modification ===
    println!("Test 2: WATCH detects concurrent modification of GEOSEARCHSTORE destination");

    // Client 1 watches the destination key
    let _ = client1.send_command_str("WATCH", &["nearby"]).await?;

    // Client 2 modifies the destination key before EXEC
    let _ = client2.send_command_str("SET", &["nearby", "interference"]).await?;

    // Client 1 starts transaction
    let _ = client1.send_command_str("MULTI", &[]).await?;
    let _ = client1.send_command_str("GEOSEARCHSTORE", &[
        "nearby", "places", "FROMLONLAT", "14.0", "38.0", "BYRADIUS", "200", "km",
    ]).await?;

    let exec_result = client1.send_command_str("EXEC", &[]).await?;
    // Transaction should be ABORTED (nil) because destination key was modified
    match &exec_result {
        RespValue::BulkString(None) => {
            println!("  Transaction correctly aborted (WATCH detected modification)");
        }
        RespValue::Array(Some(_)) => {
            panic!("Transaction should have been aborted due to WATCH key modification");
        }
        other => {
            panic!("Unexpected EXEC result: {:?}", other);
        }
    }

    // Clean up
    let _ = client1.send_command_str("DEL", &["nearby"]).await?;

    // === Test 3: GEORADIUS STORE destination key extraction in MULTI ===
    println!("Test 3: GEORADIUS STORE in MULTI/EXEC");

    let _ = client1.send_command_str("MULTI", &[]).await?;
    let queued = client1.send_command_str("GEORADIUS", &[
        "places", "14.0", "38.0", "200", "km", "STORE", "radius_result",
    ]).await?;
    assert_eq!(queued, RespValue::SimpleString("QUEUED".to_string()));

    let exec_result = client1.send_command_str("EXEC", &[]).await?;
    match &exec_result {
        RespValue::Array(Some(results)) => {
            assert_eq!(results.len(), 1, "EXEC should return 1 result");
            if let RespValue::Integer(count) = &results[0] {
                assert!(*count >= 1, "GEORADIUS STORE should store at least 1 result");
                println!("  GEORADIUS STORE stored {} elements", count);
            }
        }
        other => {
            panic!("Unexpected EXEC result: {:?}", other);
        }
    }

    // Verify destination key exists
    let card = client1.send_command_str("ZCARD", &["radius_result"]).await?;
    if let RespValue::Integer(n) = card {
        assert!(n >= 1, "radius_result should have members");
        println!("  Destination key 'radius_result' has {} members", n);
    }

    println!("All GEORADIUS/GEOSEARCHSTORE transaction key extraction tests passed!");
    Ok(())
}
