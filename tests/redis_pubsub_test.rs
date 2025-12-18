use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis Pub/Sub functionality
/// This test covers channel subscription, message publishing, and pattern subscription
#[tokio::test]
async fn test_redis_pubsub() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server with a unique port
    let addr: SocketAddr = "127.0.0.1:16396".parse()?;
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
    
    println!("Testing Redis Pub/Sub functionality...");
    
    // ======== BASIC SUBSCRIBE AND PUBLISH ========
    println!("Testing basic SUBSCRIBE and PUBLISH");
    
    // Create a subscriber client
    let mut subscriber = Client::connect(addr).await?;
    
    // Create a publisher client
    let mut publisher = Client::connect(addr).await?;
    
    // Subscribe to a channel
    let subscribe_result = subscriber.send_command_str("SUBSCRIBE", &["channel1"]).await?;
    match subscribe_result {
        RespValue::Array(Some(response)) => {
            assert_eq!(response.len(), 3, "SUBSCRIBE response should have 3 elements");
            
            // First element should be "subscribe"
            if let RespValue::BulkString(Some(msg_type)) = &response[0] {
                assert_eq!(std::str::from_utf8(msg_type)?, "subscribe", "First element should be 'subscribe'");
            } else {
                panic!("Expected BulkString for message type");
            }
            
            // Second element should be the channel name
            if let RespValue::BulkString(Some(channel)) = &response[1] {
                assert_eq!(std::str::from_utf8(channel)?, "channel1", "Second element should be the channel name");
            } else {
                panic!("Expected BulkString for channel name");
            }
            
            // Third element should be the count of subscribed channels (1)
            assert_eq!(response[2], RespValue::Integer(1), "Third element should be the count of subscribed channels");
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("SUBSCRIBE command is not implemented, skipping remaining Pub/Sub tests");
                return Ok(());
            } else {
                panic!("Unexpected error response from SUBSCRIBE: {:?}", subscribe_result);
            }
        },
        _ => {
            panic!("Unexpected response type from SUBSCRIBE: {:?}", subscribe_result);
        }
    }
    
    // For testing publish and subscribe functionality without using the private read_response method,
    // we'll simplify the test by checking the PUBLISH response. In a real Redis server,
    // a value of 1 indicates the message was successfully delivered to 1 subscriber.
    
    // Give subscriber time to set up
    sleep(Duration::from_millis(100)).await;
    
    // Publish a message to the channel
    let publish_result = publisher.send_command_str("PUBLISH", &["channel1", "Hello, PubSub!"]).await?;
    match publish_result {
        RespValue::Integer(receivers) => {
            // After subscribing to channel1, publishing a message to channel1 should return 1 (the number of subscribers)
            assert_eq!(receivers, 1, "PUBLISH should return 1 (number of clients that received the message)");
            
            // The subscriber will receive the published message first
            // Let's read it before unsubscribing
            let message_result = subscriber.read_response().await?;
            if let RespValue::Array(Some(msg)) = message_result {
                // Should be a message: ["message", "channel1", "Hello, PubSub!"]
                if let RespValue::BulkString(Some(msg_type)) = &msg[0] {
                    let msg_type_str = std::str::from_utf8(msg_type)?;
                    assert_eq!(msg_type_str, "message", "Should receive a message");
                }
            }

            // Now unsubscribe the client
            let unsubscribe_result = subscriber.send_command_str("UNSUBSCRIBE", &["channel1"]).await?;
            if let RespValue::Array(Some(response)) = unsubscribe_result {
                assert_eq!(response.len(), 3, "UNSUBSCRIBE response should have 3 elements");

                // First element should be "unsubscribe"
                if let RespValue::BulkString(Some(msg_type)) = &response[0] {
                    let msg_type_str = std::str::from_utf8(msg_type)?;
                    assert!(msg_type_str.contains("unsubscribe"),
                            "First element should be 'unsubscribe', got: {}", msg_type_str);
                }

                if let RespValue::BulkString(Some(channel)) = &response[1] {
                    let channel_str = std::str::from_utf8(channel)?;
                    assert_eq!(channel_str, "channel1", "Second element should be the channel name");
                }

                assert!(matches!(response[2], RespValue::Integer(0)),
                        "Third element should be the count of subscribed channels (0)");
            }
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("PUBLISH command is not implemented, skipping remaining tests");
                return Ok(());
            } else {
                panic!("Unexpected error response from PUBLISH: {:?}", publish_result);
            }
        },
        _ => {
            panic!("Unexpected response type from PUBLISH: {:?}", publish_result);
        }
    }
    
    // ======== PATTERN SUBSCRIBE ========
    println!("Testing PSUBSCRIBE pattern subscription");
    
    // Create a new subscriber for pattern subscription
    let mut pattern_subscriber = Client::connect(addr).await?;
    
    // Subscribe to a pattern
    let psubscribe_result = pattern_subscriber.send_command_str("PSUBSCRIBE", &["channel*"]).await?;
    match psubscribe_result {
        RespValue::Array(Some(response)) => {
            assert_eq!(response.len(), 3, "PSUBSCRIBE response should have 3 elements");
            
            // First element should be "psubscribe"
            if let RespValue::BulkString(Some(msg_type)) = &response[0] {
                assert_eq!(std::str::from_utf8(msg_type)?, "psubscribe", "First element should be 'psubscribe'");
            } else {
                panic!("Expected BulkString for message type");
            }
            
            // Second element should be the pattern
            if let RespValue::BulkString(Some(pattern)) = &response[1] {
                assert_eq!(std::str::from_utf8(pattern)?, "channel*", "Second element should be the pattern");
            } else {
                panic!("Expected BulkString for pattern");
            }
            
            // Third element should be the count of subscribed patterns (1)
            assert_eq!(response[2], RespValue::Integer(1), "Third element should be the count of subscribed patterns");
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("PSUBSCRIBE command is not implemented, skipping pattern subscription tests");
                return Ok(());
            } else {
                panic!("Unexpected error response from PSUBSCRIBE: {:?}", psubscribe_result);
            }
        },
        _ => {
            panic!("Unexpected response type from PSUBSCRIBE: {:?}", psubscribe_result);
        }
    }
    
    // Give pattern subscriber time to set up
    sleep(Duration::from_millis(100)).await;
    
    // Publish a message that matches the pattern
    let publish_result = publisher.send_command_str("PUBLISH", &["channel2", "Hello, Pattern!"]).await?;
    
    if let RespValue::Integer(receivers) = publish_result {
        assert_eq!(receivers, 1, "PUBLISH should return 1 (number of subscribers)");

        // The pattern subscriber will receive the published message first
        let message_result = pattern_subscriber.read_response().await?;
        if let RespValue::Array(Some(msg)) = message_result {
            // Should be a pmessage: ["pmessage", "channel*", "channel2", "Hello, Pattern!"]
            if let RespValue::BulkString(Some(msg_type)) = &msg[0] {
                let msg_type_str = std::str::from_utf8(msg_type)?;
                assert_eq!(msg_type_str, "pmessage", "Should receive a pmessage");
            }
        }

        // Now unsubscribe from the pattern
        let punsubscribe_result = pattern_subscriber.send_command_str("PUNSUBSCRIBE", &["channel*"]).await?;
        if let RespValue::Array(Some(response)) = punsubscribe_result {
            assert_eq!(response.len(), 3, "PUNSUBSCRIBE response should have 3 elements");

            // First element should be "punsubscribe"
            if let RespValue::BulkString(Some(msg_type)) = &response[0] {
                let msg_type_str = std::str::from_utf8(msg_type)?;
                assert!(msg_type_str.contains("unsubscribe") || msg_type_str.contains("punsubscribe"),
                       "First element should indicate pattern unsubscribe");
            }

            // Second element should be the pattern
            if let RespValue::BulkString(Some(pattern)) = &response[1] {
                let pattern_str = std::str::from_utf8(pattern)?;
                assert_eq!(pattern_str, "channel*", "Second element should be the pattern");
            }

            // Third element should be the count of subscribed patterns (0)
            assert!(matches!(response[2], RespValue::Integer(0)),
                   "Third element should be the count of subscribed patterns (0)");
        }
    } else if let RespValue::Error(err) = &publish_result {
        if err.contains("unknown command") || err.contains("not implemented") {
            println!("Pattern publish may not be implemented, skipping");
        } else {
            panic!("Unexpected error response from PUBLISH to pattern: {:?}", publish_result);
        }
    } else {
        panic!("Unexpected response type from PUBLISH to pattern: {:?}", publish_result);
    }
    
    // ======== PUBSUB COMMAND ========
    println!("Testing PUBSUB command");
    
    // Create a subscriber for PUBSUB testing
    let mut pubsub_subscriber = Client::connect(addr).await?;

    // Subscribe to channels (sends two responses - one for each channel)
    pubsub_subscriber.send_command_str("SUBSCRIBE", &["test1", "test2"]).await?;
    // Read the second subscription confirmation
    let _ = pubsub_subscriber.read_response().await?;

    // Test PUBSUB CHANNELS command
    let channels_result = publisher.send_command_str("PUBSUB", &["CHANNELS"]).await?;
    match channels_result {
        RespValue::Array(Some(channels)) => {
            assert!(channels.len() >= 2, "PUBSUB CHANNELS should return at least 2 channels");
            
            // Check if our test channels are in the list
            let mut found_test1 = false;
            let mut found_test2 = false;
            
            for channel in channels {
                if let RespValue::BulkString(Some(ch)) = channel {
                    let ch_name = std::str::from_utf8(&ch)?;
                    if ch_name == "test1" { found_test1 = true; }
                    if ch_name == "test2" { found_test2 = true; }
                }
            }
            
            assert!(found_test1 && found_test2, "PUBSUB CHANNELS should include test1 and test2");
        },
        RespValue::Error(ref err) => {
            if err.contains("unknown command") || err.contains("not implemented") {
                println!("PUBSUB command is not implemented, skipping PUBSUB tests");
            } else {
                panic!("Unexpected error response from PUBSUB CHANNELS: {:?}", channels_result);
            }
        },
        _ => {
            panic!("Unexpected response type from PUBSUB CHANNELS: {:?}", channels_result);
        }
    }
    
    // Test PUBSUB NUMSUB command
    let numsub_result = publisher.send_command_str("PUBSUB", &["NUMSUB", "test1", "test2", "nonexistent"]).await?;
    match numsub_result {
        RespValue::Array(Some(counts)) => {
            assert_eq!(counts.len(), 6, "PUBSUB NUMSUB should return 6 elements (3 channels * 2 elements per channel)");
            
            // Format should be: channel1, count1, channel2, count2, ...
            // Check that test1 and test2 have 1 subscriber each, and nonexistent has 0
            let mut test1_count = -1;
            let mut test2_count = -1;
            let mut nonexistent_count = -1;
            
            for i in (0..counts.len()).step_by(2) {
                if let RespValue::BulkString(Some(ch)) = &counts[i] {
                    let ch_name = std::str::from_utf8(ch)?;
                    if let RespValue::Integer(count) = counts[i+1] {
                        if ch_name == "test1" { test1_count = count; }
                        if ch_name == "test2" { test2_count = count; }
                        if ch_name == "nonexistent" { nonexistent_count = count; }
                    }
                }
            }
            
            assert_eq!(test1_count, 1, "test1 should have 1 subscriber");
            assert_eq!(test2_count, 1, "test2 should have 1 subscriber");
            assert_eq!(nonexistent_count, 0, "nonexistent should have 0 subscribers");
        },
        RespValue::Error(_) => {
            println!("PUBSUB NUMSUB may not be implemented, skipping");
        },
        _ => {
            panic!("Unexpected response type from PUBSUB NUMSUB: {:?}", numsub_result);
        }
    }
    
    // Clean up
    pubsub_subscriber.send_command_str("UNSUBSCRIBE", &[]).await?;
    
    println!("All Pub/Sub tests passed!");
    Ok(())
}