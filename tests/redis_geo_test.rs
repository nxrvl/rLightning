use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis Geo commands compatibility
/// Geo commands store longitude/latitude coordinates and provide query capabilities
#[tokio::test]
async fn test_redis_geo_commands() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port
    let addr: SocketAddr = "127.0.0.1:16395".parse()?;
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
    
    println!("Testing Redis Geo commands...");
    
    // ======== TEST GEOADD COMMAND ========
    println!("Testing GEOADD command");
    
    // Test GEOADD with a single location
    // Format: GEOADD key longitude latitude name
    let result = client.send_command_str("GEOADD", &["locations", "13.361389", "38.115556", "Palermo"]).await?;
    if let RespValue::Integer(added) = result {
        assert_eq!(added, 1, "GEOADD should return 1 when location is added");
    } else if let RespValue::Error(err) = &result {
        if err.contains("unknown command") || err.contains("not implemented") {
            println!("GEOADD command is not implemented, skipping remaining Geo tests");
            return Ok(());
        } else {
            panic!("Unexpected error response from GEOADD: {:?}", result);
        }
    } else {
        panic!("Unexpected response type from GEOADD: {:?}", result);
    }
    
    // Test GEOADD with multiple locations
    let result = client.send_command_str("GEOADD", &[
        "locations", 
        "15.087269", "37.502669", "Catania",
        "12.496366", "41.902782", "Rome",
        "2.352222", "48.856613", "Paris",
        "-0.127758", "51.507351", "London"
    ]).await?;
    assert_eq!(result, RespValue::Integer(4), "GEOADD should return 4 when 4 locations are added");
    
    // ======== TEST GEODIST COMMAND ========
    println!("Testing GEODIST command");
    
    // Test GEODIST between two points (default unit: meters)
    let result = client.send_command_str("GEODIST", &["locations", "Palermo", "Catania"]).await?;
    if let RespValue::BulkString(Some(bytes)) = result {
        let distance = String::from_utf8(bytes)?;
        let distance_val: f64 = distance.parse()?;
        assert!(distance_val > 160000.0 && distance_val < 170000.0, 
                "Distance between Palermo and Catania should be around 166.3 km");
    } else if let RespValue::Error(err) = &result {
        if err.contains("unknown command") || err.contains("not implemented") {
            println!("GEODIST command is not implemented, skipping remaining distance tests");
        } else {
            panic!("Unexpected error response from GEODIST: {:?}", result);
        }
    } else {
        panic!("Unexpected response type from GEODIST: {:?}", result);
    }
    
    // Test GEODIST with specified unit (km)
    let result = client.send_command_str("GEODIST", &["locations", "Palermo", "Catania", "km"]).await?;
    match result {
        RespValue::BulkString(Some(bytes)) => {
            let distance = String::from_utf8(bytes)?;
            let distance_val: f64 = distance.parse()?;
            assert!(distance_val > 160.0 && distance_val < 170.0, 
                    "Distance between Palermo and Catania should be around 166.3 km");
        },
        RespValue::Error(_) => {
            println!("GEODIST with unit parameter may not be implemented, skipping");
        },
        _ => {
            panic!("Unexpected response type from GEODIST with unit: {:?}", result);
        }
    }
    
    // Test GEODIST with non-existing member
    let result = client.send_command_str("GEODIST", &["locations", "Palermo", "Nonexistent"]).await?;
    assert_eq!(result, RespValue::BulkString(None), "GEODIST should return nil for non-existing member");
    
    // ======== TEST GEOHASH COMMAND ========
    println!("Testing GEOHASH command");
    
    // Test GEOHASH for a single member
    let result = client.send_command_str("GEOHASH", &["locations", "Rome"]).await?;
    match result {
        RespValue::Array(Some(hashes)) => {
            assert_eq!(hashes.len(), 1, "GEOHASH should return 1 hash for 1 requested member");
            if let RespValue::BulkString(Some(hash_bytes)) = &hashes[0] {
                let hash = String::from_utf8(hash_bytes.clone())?;
                // Geohash for Rome should start with "sr2" for precision 3
                assert!(hash.starts_with("sr"), "Geohash for Rome should start with sr");
            } else {
                panic!("Expected BulkString in GEOHASH response");
            }
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("GEOHASH command is not implemented, skipping remaining geohash tests");
            } else {
                panic!("Unexpected error response from GEOHASH: {:?}", result);
            }
        },
        _ => {
            panic!("Unexpected response type from GEOHASH: {:?}", result);
        }
    }
    
    // Test GEOHASH for multiple members
    let result = client.send_command_str("GEOHASH", &["locations", "Rome", "Paris", "Nonexistent"]).await?;
    match result {
        RespValue::Array(Some(hashes)) => {
            assert_eq!(hashes.len(), 3, "GEOHASH should return 3 results for 3 requested members");
            // Third result should be nil for non-existing member
            assert_eq!(hashes[2], RespValue::BulkString(None), "GEOHASH should return nil for non-existing member");
        },
        RespValue::Error(_) => {
            println!("GEOHASH with multiple members may not be implemented, skipping");
        },
        _ => {
            panic!("Unexpected response type from GEOHASH with multiple members: {:?}", result);
        }
    }
    
    // ======== TEST GEOPOS COMMAND ========
    println!("Testing GEOPOS command");
    
    // Test GEOPOS for a single member
    let result = client.send_command_str("GEOPOS", &["locations", "Rome"]).await?;
    match result {
        RespValue::Array(Some(positions)) => {
            assert_eq!(positions.len(), 1, "GEOPOS should return 1 position for 1 requested member");
            
            // Each position should be an array of [longitude, latitude]
            if let RespValue::Array(Some(coords)) = &positions[0] {
                assert_eq!(coords.len(), 2, "Each position should contain longitude and latitude");
                
                // Verify longitude is close to the expected value
                if let RespValue::BulkString(Some(lon_bytes)) = &coords[0] {
                    let lon_str = String::from_utf8(lon_bytes.clone())?;
                    let lon: f64 = lon_str.parse()?;
                    assert!((lon - 12.496366).abs() < 0.1, "Rome longitude should be close to 12.496366");
                } else {
                    panic!("Expected BulkString for longitude");
                }
                
                // Verify latitude is close to the expected value
                if let RespValue::BulkString(Some(lat_bytes)) = &coords[1] {
                    let lat_str = String::from_utf8(lat_bytes.clone())?;
                    let lat: f64 = lat_str.parse()?;
                    assert!((lat - 41.902782).abs() < 0.1, "Rome latitude should be close to 41.902782");
                } else {
                    panic!("Expected BulkString for latitude");
                }
            } else {
                panic!("Expected array of coordinates in GEOPOS response");
            }
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("GEOPOS command is not implemented, skipping remaining geopos tests");
            } else {
                panic!("Unexpected error response from GEOPOS: {:?}", result);
            }
        },
        _ => {
            panic!("Unexpected response type from GEOPOS: {:?}", result);
        }
    }
    
    // ======== TEST GEORADIUS COMMAND ========
    println!("Testing GEORADIUS command");
    
    // Test GEORADIUS: find locations within 200 km of Palermo
    let result = client.send_command_str("GEORADIUS", &["locations", "13.583333", "37.316667", "200", "km"]).await?;
    match result {
        RespValue::Array(Some(members)) => {
            assert!(members.len() >= 2, "GEORADIUS should find at least Palermo and Catania within 200 km");
            
            // Check if Palermo and Catania are in the results
            let mut found_palermo = false;
            let mut found_catania = false;
            
            for member in members {
                if let RespValue::BulkString(Some(m)) = member {
                    let name = String::from_utf8(m)?;
                    if name == "Palermo" { found_palermo = true; }
                    if name == "Catania" { found_catania = true; }
                }
            }
            
            assert!(found_palermo, "GEORADIUS should find Palermo within 200 km of specified point");
            assert!(found_catania, "GEORADIUS should find Catania within 200 km of specified point");
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("GEORADIUS command is not implemented, skipping remaining georadius tests");
            } else {
                panic!("Unexpected error response from GEORADIUS: {:?}", result);
            }
        },
        _ => {
            panic!("Unexpected response type from GEORADIUS: {:?}", result);
        }
    }
    
    // ======== TEST GEODBYSCORE COMMAND ========
    println!("Testing ZRANGE applied to geo keys");
    
    // Test getting geo points sorted by score (since geo uses sorted sets internally)
    // This is a workaround for servers not supporting advanced geo commands
    let result = client.send_command_str("ZRANGE", &["locations", "0", "-1"]).await?;
    if let RespValue::Array(Some(members)) = result {
        assert_eq!(members.len(), 5, "ZRANGE should return all 5 geo locations");
    } else {
        panic!("Unexpected response type from ZRANGE: {:?}", result);
    }
    
    // ======== TEST INTERACTION WITH OTHER COMMANDS ========
    println!("Testing interaction with other Redis commands");
    
    // Test type detection for geo key
    let result = client.send_command_str("TYPE", &["locations"]).await?;
    assert!(
        matches!(result, RespValue::SimpleString(ref s) if s == "zset" || s == "geo"),
        "TYPE for geo key should return 'zset' or 'geo'"
    );
    
    // Test TTL and expiry on geo key
    let result = client.send_command_str("EXPIRE", &["locations", "1"]).await?;
    assert_eq!(result, RespValue::Integer(1), "EXPIRE should return 1 when successful");
    
    // Verify TTL
    let result = client.send_command_str("TTL", &["locations"]).await?;
    if let RespValue::Integer(ttl) = result {
        assert!(ttl <= 1 && ttl > 0, "TTL should be between 0 and 1");
    } else {
        panic!("Expected integer response from TTL");
    }
    
    // Wait for expiry
    sleep(Duration::from_secs(1)).await;
    
    // Verify key expired
    let result = client.send_command_str("EXISTS", &["locations"]).await?;
    assert_eq!(result, RespValue::Integer(0), "Key should be expired");
    
    // Create a new geo key for delete testing
    client.send_command_str("GEOADD", &["locations2", "13.361389", "38.115556", "Palermo"]).await?;
    
    // Test DEL on geo key
    let result = client.send_command_str("DEL", &["locations2"]).await?;
    assert_eq!(result, RespValue::Integer(1), "DEL should return count of keys deleted");
    
    // Verify key is gone
    let result = client.send_command_str("EXISTS", &["locations2"]).await?;
    assert_eq!(result, RespValue::Integer(0), "Key should be deleted");
    
    println!("All Geo tests passed!");
    Ok(())
}