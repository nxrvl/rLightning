//! Integration tests for Pub/Sub functionality
//!
//! Tests Redis-compatible SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE commands

use std::net::TcpStream;
use std::io::{Write, BufRead, BufReader, Read};
use std::time::Duration;
use std::thread;
use std::sync::mpsc;
use std::process::{Command, Child};

/// Helper to start the server
fn start_server(port: u16) -> Child {
    Command::new("cargo")
        .args(["run", "--", "--port", &port.to_string()])
        .spawn()
        .expect("Failed to start server")
}

/// Helper to wait for server to be ready
fn wait_for_server(port: u16) -> bool {
    for _ in 0..50 {
        if TcpStream::connect(format!("127.0.0.1:{}", port)).is_ok() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Helper to send a command and read the response
fn send_command(stream: &mut TcpStream, cmd: &str) -> String {
    // Parse the command into RESP format
    let parts: Vec<&str> = cmd.split_whitespace().collect();
    let mut resp = format!("*{}\r\n", parts.len());
    for part in parts {
        resp.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }

    stream.write_all(resp.as_bytes()).unwrap();
    stream.flush().unwrap();

    // Read response
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    response
}

/// Helper to send raw RESP and read response
fn send_resp(stream: &mut TcpStream, resp: &str) -> String {
    stream.write_all(resp.as_bytes()).unwrap();
    stream.flush().unwrap();

    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut response = String::new();
    reader.read_line(&mut response).unwrap();
    response
}

/// Read a complete RESP array response
fn read_resp_array(stream: &mut TcpStream) -> Vec<String> {
    let mut reader = BufReader::new(stream.try_clone().unwrap());
    let mut result = Vec::new();

    // Read the array header
    let mut header = String::new();
    reader.read_line(&mut header).unwrap();

    if header.starts_with('*') {
        let count: usize = header[1..].trim().parse().unwrap_or(0);

        for _ in 0..count {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();

            if line.starts_with('$') {
                let len: usize = line[1..].trim().parse().unwrap_or(0);
                if len > 0 {
                    let mut value = vec![0u8; len + 2]; // +2 for \r\n
                    reader.read_exact(&mut value).unwrap();
                    let s = String::from_utf8_lossy(&value[..len]).to_string();
                    result.push(s);
                } else {
                    result.push(String::new());
                }
            } else if line.starts_with(':') {
                result.push(line[1..].trim().to_string());
            } else if line.starts_with('+') {
                result.push(line[1..].trim().to_string());
            }
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running server
    // Run with: cargo test --test pubsub_test -- --ignored

    #[test]
    #[ignore = "Requires running server"]
    fn test_subscribe_and_publish() {
        let port = 16379;

        // Connect subscriber
        let mut subscriber = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        subscriber.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

        // Connect publisher
        let mut publisher = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // Subscribe to channel
        let resp = "*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n";
        subscriber.write_all(resp.as_bytes()).unwrap();
        subscriber.flush().unwrap();

        // Read subscription confirmation
        let confirm = read_resp_array(&mut subscriber);
        assert_eq!(confirm[0], "subscribe");
        assert_eq!(confirm[1], "news");
        assert_eq!(confirm[2], "1");

        // Publish a message
        let resp = "*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n";
        publisher.write_all(resp.as_bytes()).unwrap();
        publisher.flush().unwrap();

        // Read publish response (should return 1 - number of subscribers)
        let mut pub_response = String::new();
        let mut reader = BufReader::new(publisher.try_clone().unwrap());
        reader.read_line(&mut pub_response).unwrap();
        assert!(pub_response.starts_with(":1")); // 1 subscriber received the message

        // Read the message on subscriber
        let message = read_resp_array(&mut subscriber);
        assert_eq!(message[0], "message");
        assert_eq!(message[1], "news");
        assert_eq!(message[2], "hello");
    }

    #[test]
    #[ignore = "Requires running server"]
    fn test_pattern_subscribe() {
        let port = 16379;

        // Connect subscriber
        let mut subscriber = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        subscriber.set_read_timeout(Some(Duration::from_secs(5))).unwrap();

        // Connect publisher
        let mut publisher = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();

        // Subscribe to pattern
        let resp = "*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews:*\r\n";
        subscriber.write_all(resp.as_bytes()).unwrap();
        subscriber.flush().unwrap();

        // Read subscription confirmation
        let confirm = read_resp_array(&mut subscriber);
        assert_eq!(confirm[0], "psubscribe");
        assert_eq!(confirm[1], "news:*");
        assert_eq!(confirm[2], "1");

        // Publish to a matching channel
        let resp = "*3\r\n$7\r\nPUBLISH\r\n$11\r\nnews:sports\r\n$4\r\ngoal\r\n";
        publisher.write_all(resp.as_bytes()).unwrap();
        publisher.flush().unwrap();

        // Read publish response
        let mut pub_response = String::new();
        let mut reader = BufReader::new(publisher.try_clone().unwrap());
        reader.read_line(&mut pub_response).unwrap();
        assert!(pub_response.starts_with(":1"));

        // Read the pmessage on subscriber
        let message = read_resp_array(&mut subscriber);
        assert_eq!(message[0], "pmessage");
        assert_eq!(message[1], "news:*");
        assert_eq!(message[2], "news:sports");
        assert_eq!(message[3], "goal");
    }

    #[test]
    #[ignore = "Requires running server"]
    fn test_pubsub_channels() {
        let port = 16379;

        // Connect and subscribe to create active channels
        let mut subscriber = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        subscriber.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

        let resp = "*3\r\n$9\r\nSUBSCRIBE\r\n$6\r\nchan:a\r\n$6\r\nchan:b\r\n";
        subscriber.write_all(resp.as_bytes()).unwrap();
        subscriber.flush().unwrap();

        // Consume subscription confirmations
        let _ = read_resp_array(&mut subscriber);
        let _ = read_resp_array(&mut subscriber);

        // Query active channels
        let mut query = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        let resp = "*2\r\n$6\r\nPUBSUB\r\n$8\r\nCHANNELS\r\n";
        query.write_all(resp.as_bytes()).unwrap();
        query.flush().unwrap();

        let channels = read_resp_array(&mut query);
        assert!(channels.len() >= 2);
        assert!(channels.contains(&"chan:a".to_string()) || channels.contains(&"chan:b".to_string()));
    }

    #[test]
    #[ignore = "Requires running server"]
    fn test_unsubscribe() {
        let port = 16379;

        let mut client = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        client.set_read_timeout(Some(Duration::from_secs(2))).unwrap();

        // Subscribe to channel
        let resp = "*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\ntest\r\n";
        client.write_all(resp.as_bytes()).unwrap();
        client.flush().unwrap();

        let confirm = read_resp_array(&mut client);
        assert_eq!(confirm[0], "subscribe");
        assert_eq!(confirm[2], "1");

        // Unsubscribe
        let resp = "*2\r\n$11\r\nUNSUBSCRIBE\r\n$4\r\ntest\r\n";
        client.write_all(resp.as_bytes()).unwrap();
        client.flush().unwrap();

        let unsub = read_resp_array(&mut client);
        assert_eq!(unsub[0], "unsubscribe");
        assert_eq!(unsub[1], "test");
        assert_eq!(unsub[2], "0"); // No more subscriptions
    }
}

// Unit tests that don't require a server
#[cfg(test)]
mod unit_tests {
    use rlightning::pubsub::PubSubManager;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_pubsub_manager_basic() {
        let manager = Arc::new(PubSubManager::new());

        // Register client
        let (client_id, mut rx) = manager.register_client().await;

        // Subscribe
        let results = manager.subscribe(client_id, vec![b"test".to_vec()]).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 1); // 1 subscription

        // Note: Subscription confirmations are sent via response buffer, not broadcast channel

        // Publish
        let count = manager.publish(b"test".to_vec(), b"hello".to_vec()).await;
        assert_eq!(count, 1);

        // Receive message
        let msg = rx.recv().await.unwrap();
        match msg {
            rlightning::pubsub::SubscriptionMessage::Message { channel, data } => {
                assert_eq!(channel, b"test".to_vec());
                assert_eq!(data, b"hello".to_vec());
            }
            _ => panic!("Expected Message"),
        }
    }

    #[tokio::test]
    async fn test_pattern_matching() {
        let manager = Arc::new(PubSubManager::new());

        let (client_id, mut rx) = manager.register_client().await;

        // Subscribe to pattern
        manager.psubscribe(client_id, vec![b"news:*".to_vec()]).await;

        // Note: Subscription confirmations are sent via response buffer, not broadcast channel

        // Publish to matching channel
        let count = manager.publish(b"news:sports".to_vec(), b"goal!".to_vec()).await;
        assert_eq!(count, 1);

        // Receive pmessage
        let msg = rx.recv().await.unwrap();
        match msg {
            rlightning::pubsub::SubscriptionMessage::PMessage { pattern, channel, data } => {
                assert_eq!(pattern, b"news:*".to_vec());
                assert_eq!(channel, b"news:sports".to_vec());
                assert_eq!(data, b"goal!".to_vec());
            }
            _ => panic!("Expected PMessage"),
        }
    }

    #[tokio::test]
    async fn test_multiple_subscribers() {
        let manager = Arc::new(PubSubManager::new());

        // Register 3 clients
        let (client1, mut rx1) = manager.register_client().await;
        let (client2, mut rx2) = manager.register_client().await;
        let (client3, _rx3) = manager.register_client().await;

        // Subscribe all to the same channel
        manager.subscribe(client1, vec![b"broadcast".to_vec()]).await;
        manager.subscribe(client2, vec![b"broadcast".to_vec()]).await;
        manager.subscribe(client3, vec![b"broadcast".to_vec()]).await;

        // Note: Subscription confirmations are sent via response buffer, not broadcast channel

        // Publish
        let count = manager.publish(b"broadcast".to_vec(), b"hello everyone".to_vec()).await;
        assert_eq!(count, 3);

        // All should receive
        let msg1 = rx1.recv().await.unwrap();
        let msg2 = rx2.recv().await.unwrap();

        match (msg1, msg2) {
            (
                rlightning::pubsub::SubscriptionMessage::Message { data: d1, .. },
                rlightning::pubsub::SubscriptionMessage::Message { data: d2, .. },
            ) => {
                assert_eq!(d1, b"hello everyone".to_vec());
                assert_eq!(d2, b"hello everyone".to_vec());
            }
            _ => panic!("Expected Messages"),
        }
    }

    #[tokio::test]
    async fn test_pubsub_numsub() {
        let manager = Arc::new(PubSubManager::new());

        let (client1, _rx1) = manager.register_client().await;
        let (client2, _rx2) = manager.register_client().await;

        manager.subscribe(client1, vec![b"chan".to_vec()]).await;
        manager.subscribe(client2, vec![b"chan".to_vec()]).await;

        let counts = manager.pubsub_numsub(&[b"chan".to_vec()]).await;
        assert_eq!(counts.len(), 1);
        assert_eq!(counts[0], (b"chan".to_vec(), 2));
    }

    #[tokio::test]
    async fn test_client_cleanup() {
        let manager = Arc::new(PubSubManager::new());

        let (client_id, _rx) = manager.register_client().await;
        manager.subscribe(client_id, vec![b"test".to_vec()]).await;

        // Verify channel exists
        let channels = manager.pubsub_channels(None).await;
        assert_eq!(channels.len(), 1);

        // Unregister client
        manager.unregister_client(client_id).await;

        // Verify channel is cleaned up
        let channels = manager.pubsub_channels(None).await;
        assert_eq!(channels.len(), 0);
    }
}
