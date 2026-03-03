//! Integration tests for CLIENT LIST sub/psub counts
//!
//! Verifies that CLIENT LIST accurately reflects subscription counts
//! when clients use SUBSCRIBE/PSUBSCRIBE.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Start an embedded test server on the given port
fn start_embedded_server(port: u16) {
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async move {
            let addr = format!("127.0.0.1:{}", port).parse().unwrap();
            let config = StorageConfig::default();
            let storage = StorageEngine::new(config);
            let server = Server::new(addr, storage)
                .with_connection_limit(100)
                .with_buffer_size(1024 * 1024);
            if let Err(e) = server.start().await {
                eprintln!("Test server error on port {}: {:?}", port, e);
            }
        });
    });
    std::thread::sleep(Duration::from_millis(500));
}

/// Send a raw RESP command and read back a complete response line
fn send_command(stream: &mut TcpStream, cmd: &str) -> String {
    stream.write_all(cmd.as_bytes()).unwrap();
    stream.flush().unwrap();
    std::thread::sleep(Duration::from_millis(100));

    let mut buf = vec![0u8; 8192];
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    match stream.read(&mut buf) {
        Ok(n) => String::from_utf8_lossy(&buf[..n]).to_string(),
        Err(_) => String::new(),
    }
}

/// Read all available data from a stream (non-blocking with timeout)
fn read_all(stream: &mut TcpStream) -> String {
    let mut buf = vec![0u8; 16384];
    stream
        .set_read_timeout(Some(Duration::from_millis(300)))
        .unwrap();
    let mut result = String::new();
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => result.push_str(&String::from_utf8_lossy(&buf[..n])),
            Err(_) => break,
        }
    }
    result
}

/// Build a RESP array command from parts
fn resp_cmd(parts: &[&str]) -> String {
    let mut cmd = format!("*{}\r\n", parts.len());
    for part in parts {
        cmd.push_str(&format!("${}\r\n{}\r\n", part.len(), part));
    }
    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_list_shows_subscribe_count() {
        let port = 18310;
        start_embedded_server(port);

        // Connection 1: subscriber
        let mut sub_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        sub_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        sub_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // Connection 2: observer (for CLIENT LIST)
        let mut obs_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        obs_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        obs_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // Subscribe to 3 channels on the subscriber connection
        let subscribe_cmd = resp_cmd(&["SUBSCRIBE", "chan1", "chan2", "chan3"]);
        sub_conn.write_all(subscribe_cmd.as_bytes()).unwrap();
        sub_conn.flush().unwrap();
        std::thread::sleep(Duration::from_millis(200));

        // Drain subscribe responses
        let _ = read_all(&mut sub_conn);

        // Now check CLIENT LIST from the observer
        let client_list_cmd = resp_cmd(&["CLIENT", "LIST"]);
        let response = send_command(&mut obs_conn, &client_list_cmd);

        // Parse the CLIENT LIST output to find the subscriber's line
        // The response is a bulk string containing lines for each client
        // We need to find the line with sub=3
        println!("CLIENT LIST response:\n{}", response);

        // Extract the bulk string content (skip RESP header)
        let lines: Vec<&str> = response.split("\r\n").collect();
        let mut found_sub3 = false;
        for line in &lines {
            if line.contains("sub=3") {
                found_sub3 = true;
                // Also verify psub=0 on the same line
                assert!(
                    line.contains("psub=0"),
                    "Expected psub=0 on subscriber line, got: {}",
                    line
                );
                break;
            }
        }
        assert!(
            found_sub3,
            "Expected to find a client with sub=3 in CLIENT LIST output: {:?}",
            lines
        );
    }

    #[test]
    fn test_client_list_shows_psubscribe_count() {
        let port = 18311;
        start_embedded_server(port);

        // Connection 1: pattern subscriber
        let mut sub_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        sub_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        sub_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // Connection 2: observer
        let mut obs_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        obs_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        obs_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // PSUBSCRIBE to 2 patterns
        let psubscribe_cmd = resp_cmd(&["PSUBSCRIBE", "news.*", "events.*"]);
        sub_conn.write_all(psubscribe_cmd.as_bytes()).unwrap();
        sub_conn.flush().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        let _ = read_all(&mut sub_conn);

        // Check CLIENT LIST
        let client_list_cmd = resp_cmd(&["CLIENT", "LIST"]);
        let response = send_command(&mut obs_conn, &client_list_cmd);
        println!("CLIENT LIST response:\n{}", response);

        let lines: Vec<&str> = response.split("\r\n").collect();
        let mut found_psub2 = false;
        for line in &lines {
            if line.contains("psub=2") {
                found_psub2 = true;
                // Also verify sub=0 since we only did PSUBSCRIBE
                assert!(
                    line.contains("sub=0"),
                    "Expected sub=0 on pattern subscriber line, got: {}",
                    line
                );
                break;
            }
        }
        assert!(
            found_psub2,
            "Expected to find a client with psub=2 in CLIENT LIST output: {:?}",
            lines
        );
    }

    #[test]
    fn test_client_list_mixed_sub_and_psub() {
        let port = 18312;
        start_embedded_server(port);

        // Connection 1: subscriber with both channels and patterns
        let mut sub_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        sub_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        sub_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // Connection 2: observer
        let mut obs_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        obs_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        obs_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // SUBSCRIBE to 2 channels
        let subscribe_cmd = resp_cmd(&["SUBSCRIBE", "chan1", "chan2"]);
        sub_conn.write_all(subscribe_cmd.as_bytes()).unwrap();
        sub_conn.flush().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        let _ = read_all(&mut sub_conn);

        // PSUBSCRIBE to 1 pattern (while in subscription mode)
        let psubscribe_cmd = resp_cmd(&["PSUBSCRIBE", "news.*"]);
        sub_conn.write_all(psubscribe_cmd.as_bytes()).unwrap();
        sub_conn.flush().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        let _ = read_all(&mut sub_conn);

        // Check CLIENT LIST
        let client_list_cmd = resp_cmd(&["CLIENT", "LIST"]);
        let response = send_command(&mut obs_conn, &client_list_cmd);
        println!("CLIENT LIST response:\n{}", response);

        let lines: Vec<&str> = response.split("\r\n").collect();
        let mut found_mixed = false;
        for line in &lines {
            if line.contains("sub=2") && line.contains("psub=1") {
                found_mixed = true;
                break;
            }
        }
        assert!(
            found_mixed,
            "Expected to find a client with sub=2 psub=1 in CLIENT LIST output: {:?}",
            lines
        );
    }

    #[test]
    fn test_client_list_sub_count_decrements_on_unsubscribe() {
        let port = 18313;
        start_embedded_server(port);

        // Connection 1: subscriber
        let mut sub_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        sub_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        sub_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // Connection 2: observer
        let mut obs_conn = TcpStream::connect(format!("127.0.0.1:{}", port)).unwrap();
        obs_conn
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        obs_conn
            .set_write_timeout(Some(Duration::from_millis(500)))
            .unwrap();

        // Subscribe to 3 channels
        let subscribe_cmd = resp_cmd(&["SUBSCRIBE", "chan1", "chan2", "chan3"]);
        sub_conn.write_all(subscribe_cmd.as_bytes()).unwrap();
        sub_conn.flush().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        let _ = read_all(&mut sub_conn);

        // Verify sub=3
        let client_list_cmd = resp_cmd(&["CLIENT", "LIST"]);
        let response = send_command(&mut obs_conn, &client_list_cmd);
        assert!(
            response.contains("sub=3"),
            "Expected sub=3 before unsubscribe, got: {}",
            response
        );

        // Unsubscribe from 1 channel
        let unsub_cmd = resp_cmd(&["UNSUBSCRIBE", "chan2"]);
        sub_conn.write_all(unsub_cmd.as_bytes()).unwrap();
        sub_conn.flush().unwrap();
        std::thread::sleep(Duration::from_millis(200));
        let _ = read_all(&mut sub_conn);

        // Verify sub=2
        let response = send_command(&mut obs_conn, &client_list_cmd);
        assert!(
            response.contains("sub=2"),
            "Expected sub=2 after unsubscribe from 1 channel, got: {}",
            response
        );
    }
}
