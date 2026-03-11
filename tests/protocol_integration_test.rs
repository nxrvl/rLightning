use rlightning::command::handler::CommandHandler;
use rlightning::networking::server::Server;
use rlightning::storage::engine::StorageEngine;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

async fn send_and_recv(
    stream: &mut TcpStream,
    data: &[u8],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    stream.write_all(data).await?;

    let mut response = vec![0; 4096];
    let n = timeout(Duration::from_secs(1), stream.read(&mut response)).await??;
    response.truncate(n);
    Ok(response)
}

#[tokio::test]
async fn test_server_handles_json_like_data() {
    // Create storage and server
    let storage = StorageEngine::new(Default::default());
    let addr = "127.0.0.1:16380".parse().unwrap();
    let server = Server::new(addr, storage);

    // Start server
    let server_handle = tokio::spawn(async move { server.start().await });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Connect to server
    let mut stream = TcpStream::connect("127.0.0.1:16380")
        .await
        .expect("Failed to connect to server");

    // Test 1: Send a SET command with JSON-like data
    let json_data = r#"{"id": 1, "data": "test", "array": [1, 2, 3]}"#;
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n$8\r\njson_key\r\n${}\r\n{}\r\n",
        json_data.len(),
        json_data
    );

    let response = send_and_recv(&mut stream, set_cmd.as_bytes())
        .await
        .expect("Failed to send SET command");

    assert_eq!(&response, b"+OK\r\n", "SET command should return OK");

    // Test 2: Send a GET command to retrieve the data
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$8\r\njson_key\r\n";
    let response = send_and_recv(&mut stream, get_cmd)
        .await
        .expect("Failed to send GET command");

    let expected_response = format!("${}\r\n{}\r\n", json_data.len(), json_data);
    assert_eq!(
        response,
        expected_response.as_bytes(),
        "GET should return the exact JSON data stored"
    );

    // Test 3: Send Base64-encoded data
    let b64_data = "B64JSON:W3siaWQiOiAxLCAibmFtZSI6ICJ0ZXN0In1d";
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n$7\r\nb64_key\r\n${}\r\n{}\r\n",
        b64_data.len(),
        b64_data
    );

    let response = send_and_recv(&mut stream, set_cmd.as_bytes())
        .await
        .expect("Failed to send SET with B64 data");

    assert_eq!(&response, b"+OK\r\n", "SET with B64 data should return OK");

    // Test 4: Retrieve the B64 data
    let get_cmd = b"*2\r\n$3\r\nGET\r\n$7\r\nb64_key\r\n";
    let response = send_and_recv(&mut stream, get_cmd)
        .await
        .expect("Failed to send GET for B64 data");

    let expected_response = format!("${}\r\n{}\r\n", b64_data.len(), b64_data);
    assert_eq!(
        response,
        expected_response.as_bytes(),
        "GET should return the exact B64 data stored"
    );

    // Clean up
    drop(stream);
    server_handle.abort();
}

#[tokio::test]
async fn test_server_handles_invalid_commands_gracefully() {
    // This would normally require a running server instance
    // For unit testing, we'll test the command processing directly

    let storage = StorageEngine::new(Default::default());
    let handler = CommandHandler::new(storage);

    // With inline protocol support, non-RESP data without \r\n is treated as
    // incomplete (Ok(None)) rather than an error. Data with \r\n would be parsed
    // as an inline command.
    let invalid_inputs = vec![
        "B64JSON:W3siaWQiOiAx",
        "{\"test\": 1}",
        "[1, 2, 3]",
        "HTTP/1.1 200 OK",
    ];

    for input in invalid_inputs {
        let mut buffer = bytes::BytesMut::from(input);
        let parse_result = rlightning::networking::resp::RespValue::parse(&mut buffer);

        assert!(
            matches!(parse_result, Ok(None)),
            "Non-RESP data without \\r\\n should be incomplete, got {:?} for '{}'",
            parse_result,
            input
        );
    }
}

#[tokio::test]
async fn test_buffer_management_with_multiple_commands() {
    use bytes::BytesMut;
    use rlightning::networking::resp::RespValue;

    // Simulate what the server does with the buffer
    let mut buffer = BytesMut::new();
    let mut responses = Vec::new();

    // Add multiple commands to buffer
    buffer.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$4\r\nkey1\r\n");
    buffer.extend_from_slice(b"*3\r\n$3\r\nSET\r\n$4\r\nkey2\r\n$5\r\nvalue\r\n");
    buffer.extend_from_slice(b"*2\r\n$3\r\nGET\r\n$4\r\nkey2\r\n");

    // Process commands from buffer
    while !buffer.is_empty() {
        match RespValue::parse(&mut buffer) {
            Ok(Some(value)) => {
                responses.push(value);
            }
            Ok(None) => {
                // Incomplete command, would wait for more data
                break;
            }
            Err(_) => {
                // Error, clear buffer
                buffer.clear();
                break;
            }
        }
    }

    assert_eq!(responses.len(), 3, "Should parse all 3 commands");
    assert_eq!(
        buffer.len(),
        0,
        "Buffer should be empty after parsing all commands"
    );
}

#[tokio::test]
async fn test_large_payload_handling() {
    use bytes::BytesMut;
    use rlightning::networking::resp::RespValue;

    // Create a large JSON payload (similar to what the Python client sends)
    let large_json = serde_json::json!({
        "id": 1,
        "data": vec!["item"; 1000],
        "nested": {
            "field1": "value1",
            "field2": vec![1, 2, 3, 4, 5],
        }
    });

    let json_str = serde_json::to_string(&large_json).unwrap();

    // Create a SET command with the large payload
    let set_cmd = format!(
        "*3\r\n$3\r\nSET\r\n$9\r\nlarge_key\r\n${}\r\n{}\r\n",
        json_str.len(),
        json_str
    );

    let mut buffer = BytesMut::from(set_cmd.as_bytes());

    // Parse the command
    let result = RespValue::parse(&mut buffer);

    assert!(result.is_ok(), "Should parse large SET command");
    assert!(result.unwrap().is_some(), "Should return parsed command");
    assert_eq!(buffer.len(), 0, "Buffer should be consumed");
}

#[test]
fn test_resp_inline_protocol_support() {
    use bytes::BytesMut;
    use rlightning::networking::resp::RespValue;

    // With inline protocol support, data without \r\n is incomplete
    let data = "B64JSON:W3siaWQiOiAx";
    let mut buffer = BytesMut::from(data);
    let result = RespValue::parse(&mut buffer);
    assert!(
        matches!(result, Ok(None)),
        "Data without newline should be incomplete"
    );

    // Data with \r\n is parsed as an inline command
    let mut buffer = BytesMut::from("PING\r\n");
    let result = RespValue::parse(&mut buffer).unwrap();
    assert!(result.is_some(), "PING with \\r\\n should parse as inline command");
}
