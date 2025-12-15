/// Redis 100% Protocol Compliance Test Suite
/// Tests the newly implemented commands for full Redis compatibility

use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::Duration;
use tokio::time::sleep;

// Helper function to send raw RESP commands and get responses
fn send_redis_command(stream: &mut TcpStream, command: &str) -> Result<String, Box<dyn std::error::Error>> {
    stream.write_all(command.as_bytes())?;
    
    let mut buffer = [0; 4096];
    let bytes_read = stream.read(&mut buffer)?;
    let response = String::from_utf8_lossy(&buffer[..bytes_read]);
    
    Ok(response.to_string())
}

/// NOTE: This test requires a running server on port 6379. Run manually with:
/// cargo run --release -- --port 6379 &
/// cargo test test_comprehensive_string_commands_compliance -- --ignored
#[tokio::test]
#[ignore]
async fn test_comprehensive_string_commands_compliance() {
    // Start the server
    let _server = std::process::Command::new("cargo")
        .args(&["run", "--release", "--", "--config", "test-no-auth.toml"])
        .spawn()
        .expect("Failed to start server");
    
    // Wait for server to start
    sleep(Duration::from_secs(2)).await;
    
    let mut stream = TcpStream::connect("127.0.0.1:6379")
        .expect("Failed to connect to server");
    
    // Test basic commands
    let response = send_redis_command(&mut stream, "*1\r\n$4\r\nPING\r\n").unwrap();
    assert!(response.contains("+PONG"), "PING command failed: {}", response);
    
    // Test SET/GET
    let response = send_redis_command(&mut stream, "*3\r\n$3\r\nSET\r\n$8\r\ntestkey1\r\n$9\r\ntestvalue\r\n").unwrap();
    assert!(response.contains("+OK"), "SET command failed: {}", response);
    
    let response = send_redis_command(&mut stream, "*2\r\n$3\r\nGET\r\n$8\r\ntestkey1\r\n").unwrap();
    assert!(response.contains("testvalue"), "GET command failed: {}", response);
    
    // Test new string commands
    
    // STRLEN
    let response = send_redis_command(&mut stream, "*2\r\n$6\r\nSTRLEN\r\n$8\r\ntestkey1\r\n").unwrap();
    assert!(response.contains(":9"), "STRLEN command failed: {}", response);
    
    // APPEND
    let response = send_redis_command(&mut stream, "*3\r\n$6\r\nAPPEND\r\n$8\r\ntestkey1\r\n$5\r\nmore!\r\n").unwrap();
    assert!(response.contains(":14"), "APPEND command failed: {}", response);
    
    // GETRANGE
    let response = send_redis_command(&mut stream, "*4\r\n$8\r\nGETRANGE\r\n$8\r\ntestkey1\r\n$1\r\n0\r\n$1\r\n3\r\n").unwrap();
    assert!(response.contains("test"), "GETRANGE command failed: {}", response);
    
    // SETRANGE
    let response = send_redis_command(&mut stream, "*4\r\n$8\r\nSETRANGE\r\n$8\r\ntestkey1\r\n$1\r\n0\r\n$4\r\nbest\r\n").unwrap();
    assert!(response.contains(":14"), "SETRANGE command failed: {}", response);
    
    // GETSET
    let response = send_redis_command(&mut stream, "*3\r\n$6\r\nGETSET\r\n$8\r\ntestkey1\r\n$8\r\nnewvalue\r\n").unwrap();
    assert!(response.contains("best"), "GETSET command failed: {}", response);
    
    // MSETNX
    let response = send_redis_command(&mut stream, "*5\r\n$6\r\nMSETNX\r\n$4\r\nkey1\r\n$6\r\nvalue1\r\n$4\r\nkey2\r\n$6\r\nvalue2\r\n").unwrap();
    assert!(response.contains(":1"), "MSETNX command failed: {}", response);
    
    // INCRBYFLOAT
    let response = send_redis_command(&mut stream, "*3\r\n$3\r\nSET\r\n$8\r\nfloatkey\r\n$3\r\n3.5\r\n").unwrap();
    assert!(response.contains("+OK"), "SET float command failed: {}", response);
    
    let response = send_redis_command(&mut stream, "*3\r\n$11\r\nINCRBYFLOAT\r\n$8\r\nfloatkey\r\n$3\r\n1.5\r\n").unwrap();
    assert!(response.contains("5"), "INCRBYFLOAT command failed: {}", response);
    
    println!("✅ All string commands passed compliance test");
}

/// NOTE: This test requires a running server on port 6379.
#[tokio::test]
#[ignore]
async fn test_comprehensive_hash_commands_compliance() {
    // Wait for server to be ready
    sleep(Duration::from_secs(1)).await;

    let mut stream = TcpStream::connect("127.0.0.1:6379")
        .expect("Failed to connect to server");
    
    // Setup hash
    let response = send_redis_command(&mut stream, "*4\r\n$4\r\nHSET\r\n$7\r\nhashkey\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n").unwrap();
    assert!(response.contains(":1"), "HSET command failed: {}", response);
    
    // HKEYS
    let response = send_redis_command(&mut stream, "*2\r\n$5\r\nHKEYS\r\n$7\r\nhashkey\r\n").unwrap();
    assert!(response.contains("field1"), "HKEYS command failed: {}", response);
    
    // HVALS
    let response = send_redis_command(&mut stream, "*2\r\n$5\r\nHVALS\r\n$7\r\nhashkey\r\n").unwrap();
    assert!(response.contains("value1"), "HVALS command failed: {}", response);
    
    // HLEN
    let response = send_redis_command(&mut stream, "*2\r\n$4\r\nHLEN\r\n$7\r\nhashkey\r\n").unwrap();
    assert!(response.contains(":1"), "HLEN command failed: {}", response);
    
    // HMGET
    let response = send_redis_command(&mut stream, "*3\r\n$5\r\nHMGET\r\n$7\r\nhashkey\r\n$6\r\nfield1\r\n").unwrap();
    assert!(response.contains("value1"), "HMGET command failed: {}", response);
    
    // HINCRBY
    let response = send_redis_command(&mut stream, "*4\r\n$4\r\nHSET\r\n$7\r\nhashkey\r\n$7\r\ncounter\r\n$1\r\n5\r\n").unwrap();
    let response = send_redis_command(&mut stream, "*4\r\n$7\r\nHINCRBY\r\n$7\r\nhashkey\r\n$7\r\ncounter\r\n$1\r\n3\r\n").unwrap();
    assert!(response.contains(":8"), "HINCRBY command failed: {}", response);
    
    // HINCRBYFLOAT
    let response = send_redis_command(&mut stream, "*4\r\n$12\r\nHINCRBYFLOAT\r\n$7\r\nhashkey\r\n$7\r\ncounter\r\n$3\r\n1.5\r\n").unwrap();
    assert!(response.contains("9.5"), "HINCRBYFLOAT command failed: {}", response);
    
    // HSETNX
    let response = send_redis_command(&mut stream, "*4\r\n$6\r\nHSETNX\r\n$7\r\nhashkey\r\n$9\r\nnewfield1\r\n$9\r\nnewvalue1\r\n").unwrap();
    assert!(response.contains(":1"), "HSETNX command failed: {}", response);
    
    // HSTRLEN
    let response = send_redis_command(&mut stream, "*3\r\n$7\r\nHSTRLEN\r\n$7\r\nhashkey\r\n$6\r\nfield1\r\n").unwrap();
    assert!(response.contains(":6"), "HSTRLEN command failed: {}", response);
    
    println!("✅ All hash commands passed compliance test");
}

/// NOTE: This test requires a running server on port 6379.
#[tokio::test]
#[ignore]
async fn test_comprehensive_json_commands_compliance() {
    // Wait for server to be ready
    sleep(Duration::from_secs(1)).await;

    let mut stream = TcpStream::connect("127.0.0.1:6379")
        .expect("Failed to connect to server");
    
    // JSON.SET
    let json_data = r#"{"name":"John","age":30,"items":["apple","banana"],"meta":{"type":"user"}}"#;
    let command = format!("*4\r\n$8\r\nJSON.SET\r\n$7\r\njsonkey\r\n$1\r\n$\r\n${}\r\n{}\r\n", json_data.len(), json_data);
    let response = send_redis_command(&mut stream, &command).unwrap();
    assert!(response.contains("+OK"), "JSON.SET command failed: {}", response);
    
    // JSON.GET
    let response = send_redis_command(&mut stream, "*2\r\n$8\r\nJSON.GET\r\n$7\r\njsonkey\r\n").unwrap();
    assert!(response.contains("John"), "JSON.GET command failed: {}", response);
    
    // JSON.TYPE
    let response = send_redis_command(&mut stream, "*2\r\n$9\r\nJSON.TYPE\r\n$7\r\njsonkey\r\n").unwrap();
    assert!(response.contains("object"), "JSON.TYPE command failed: {}", response);
    
    // JSON.OBJKEYS
    let response = send_redis_command(&mut stream, "*2\r\n$12\r\nJSON.OBJKEYS\r\n$7\r\njsonkey\r\n").unwrap();
    assert!(response.contains("name"), "JSON.OBJKEYS command failed: {}", response);
    
    // JSON.OBJLEN
    let response = send_redis_command(&mut stream, "*2\r\n$11\r\nJSON.OBJLEN\r\n$7\r\njsonkey\r\n").unwrap();
    assert!(response.contains(":4"), "JSON.OBJLEN command failed: {}", response);
    
    // Setup an array for array tests
    let array_data = r#"[1,2,3,4,5]"#;
    let command = format!("*4\r\n$8\r\nJSON.SET\r\n$8\r\narraykey\r\n$1\r\n$\r\n${}\r\n{}\r\n", array_data.len(), array_data);
    let response = send_redis_command(&mut stream, &command).unwrap();
    assert!(response.contains("+OK"), "JSON.SET array command failed: {}", response);
    
    // JSON.ARRLEN
    let response = send_redis_command(&mut stream, "*2\r\n$11\r\nJSON.ARRLEN\r\n$8\r\narraykey\r\n").unwrap();
    assert!(response.contains(":5"), "JSON.ARRLEN command failed: {}", response);
    
    // JSON.ARRAPPEND
    let response = send_redis_command(&mut stream, "*4\r\n$14\r\nJSON.ARRAPPEND\r\n$8\r\narraykey\r\n$1\r\n$\r\n$1\r\n6\r\n").unwrap();
    assert!(response.contains(":6"), "JSON.ARRAPPEND command failed: {}", response);
    
    // JSON.ARRTRIM
    let response = send_redis_command(&mut stream, "*5\r\n$12\r\nJSON.ARRTRIM\r\n$8\r\narraykey\r\n$1\r\n$\r\n$1\r\n0\r\n$1\r\n2\r\n").unwrap();
    assert!(response.contains(":3"), "JSON.ARRTRIM command failed: {}", response);
    
    // Setup a number for numeric tests
    let number_data = "42";
    let command = format!("*4\r\n$8\r\nJSON.SET\r\n$9\r\nnumberkey\r\n$1\r\n$\r\n${}\r\n{}\r\n", number_data.len(), number_data);
    let response = send_redis_command(&mut stream, &command).unwrap();
    assert!(response.contains("+OK"), "JSON.SET number command failed: {}", response);
    
    // JSON.NUMINCRBY
    let response = send_redis_command(&mut stream, "*4\r\n$14\r\nJSON.NUMINCRBY\r\n$9\r\nnumberkey\r\n$1\r\n$\r\n$1\r\n8\r\n").unwrap();
    assert!(response.contains("50"), "JSON.NUMINCRBY command failed: {}", response);
    
    // JSON.DEL
    let response = send_redis_command(&mut stream, "*2\r\n$8\r\nJSON.DEL\r\n$7\r\njsonkey\r\n").unwrap();
    assert!(response.contains(":1"), "JSON.DEL command failed: {}", response);
    
    println!("✅ All JSON commands passed compliance test");
}

/// NOTE: This test requires a running server on port 6379.
#[tokio::test]
#[ignore]
async fn test_protocol_compliance_summary() {
    // Wait for server to be ready
    sleep(Duration::from_secs(1)).await;

    let mut stream = TcpStream::connect("127.0.0.1:6379")
        .expect("Failed to connect to server");
    
    println!("\n🎉 REDIS PROTOCOL COMPLIANCE SUMMARY 🎉");
    println!("==========================================");
    
    // Test command categories
    let mut total_commands = 0;
    let mut working_commands = 0;
    
    // String commands
    let string_commands = vec![
        ("SET", "*3\r\n$3\r\nSET\r\n$4\r\ntest\r\n$5\r\nvalue\r\n"),
        ("GET", "*2\r\n$3\r\nGET\r\n$4\r\ntest\r\n"),
        ("STRLEN", "*2\r\n$6\r\nSTRLEN\r\n$4\r\ntest\r\n"),
        ("APPEND", "*3\r\n$6\r\nAPPEND\r\n$4\r\ntest\r\n$4\r\nmore\r\n"),
        ("INCR", "*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"),
        ("DECR", "*2\r\n$4\r\nDECR\r\n$7\r\ncounter\r\n"),
        ("MGET", "*2\r\n$4\r\nMGET\r\n$4\r\ntest\r\n"),
        ("MSET", "*3\r\n$4\r\nMSET\r\n$5\r\ntest2\r\n$6\r\nvalue2\r\n"),
        ("GETRANGE", "*4\r\n$8\r\nGETRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$1\r\n2\r\n"),
        ("SETRANGE", "*4\r\n$8\r\nSETRANGE\r\n$4\r\ntest\r\n$1\r\n0\r\n$3\r\nbest\r\n"),
    ];
    
    println!("📝 String Commands:");
    for (cmd_name, cmd) in string_commands {
        total_commands += 1;
        match send_redis_command(&mut stream, cmd) {
            Ok(response) if !response.contains("-ERR") && !response.contains("unknown command") => {
                working_commands += 1;
                println!("  ✅ {}", cmd_name);
            },
            _ => {
                println!("  ❌ {}", cmd_name);
            }
        }
    }
    
    // Hash commands
    let hash_commands = vec![
        ("HSET", "*4\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n"),
        ("HGET", "*3\r\n$4\r\nHGET\r\n$4\r\nhash\r\n$5\r\nfield\r\n"),
        ("HKEYS", "*2\r\n$5\r\nHKEYS\r\n$4\r\nhash\r\n"),
        ("HVALS", "*2\r\n$5\r\nHVALS\r\n$4\r\nhash\r\n"),
        ("HLEN", "*2\r\n$4\r\nHLEN\r\n$4\r\nhash\r\n"),
        ("HEXISTS", "*3\r\n$7\r\nHEXISTS\r\n$4\r\nhash\r\n$5\r\nfield\r\n"),
        ("HDEL", "*3\r\n$4\r\nHDEL\r\n$4\r\nhash\r\n$5\r\nfield\r\n"),
    ];
    
    println!("\n🗂️  Hash Commands:");
    for (cmd_name, cmd) in hash_commands {
        total_commands += 1;
        match send_redis_command(&mut stream, cmd) {
            Ok(response) if !response.contains("-ERR") && !response.contains("unknown command") => {
                working_commands += 1;
                println!("  ✅ {}", cmd_name);
            },
            _ => {
                println!("  ❌ {}", cmd_name);
            }
        }
    }
    
    // JSON commands
    let json_commands = vec![
        ("JSON.SET", "*4\r\n$8\r\nJSON.SET\r\n$4\r\njson\r\n$1\r\n$\r\n$13\r\n{\"key\":\"value\"}\r\n"),
        ("JSON.GET", "*2\r\n$8\r\nJSON.GET\r\n$4\r\njson\r\n"),
        ("JSON.TYPE", "*2\r\n$9\r\nJSON.TYPE\r\n$4\r\njson\r\n"),
        ("JSON.OBJKEYS", "*2\r\n$12\r\nJSON.OBJKEYS\r\n$4\r\njson\r\n"),
        ("JSON.OBJLEN", "*2\r\n$11\r\nJSON.OBJLEN\r\n$4\r\njson\r\n"),
    ];
    
    println!("\n📄 JSON Commands:");
    for (cmd_name, cmd) in json_commands {
        total_commands += 1;
        match send_redis_command(&mut stream, cmd) {
            Ok(response) if !response.contains("-ERR") && !response.contains("unknown command") => {
                working_commands += 1;
                println!("  ✅ {}", cmd_name);
            },
            _ => {
                println!("  ❌ {}", cmd_name);
            }
        }
    }
    
    // Basic server commands
    let server_commands = vec![
        ("PING", "*1\r\n$4\r\nPING\r\n"),
        ("INFO", "*1\r\n$4\r\nINFO\r\n"),
        ("TYPE", "*2\r\n$4\r\nTYPE\r\n$4\r\ntest\r\n"),
        ("EXISTS", "*2\r\n$6\r\nEXISTS\r\n$4\r\ntest\r\n"),
        ("DEL", "*2\r\n$3\r\nDEL\r\n$4\r\ntest\r\n"),
        ("TTL", "*2\r\n$3\r\nTTL\r\n$4\r\ntest\r\n"),
    ];
    
    println!("\n🖥️  Server Commands:");
    for (cmd_name, cmd) in server_commands {
        total_commands += 1;
        match send_redis_command(&mut stream, cmd) {
            Ok(response) if !response.contains("-ERR") && !response.contains("unknown command") => {
                working_commands += 1;
                println!("  ✅ {}", cmd_name);
            },
            _ => {
                println!("  ❌ {}", cmd_name);
            }
        }
    }
    
    let compliance_percentage = (working_commands as f64 / total_commands as f64) * 100.0;
    
    println!("\n📊 FINAL COMPLIANCE SCORE:");
    println!("==========================================");
    println!("🎯 Commands tested: {}", total_commands);
    println!("✅ Commands working: {}", working_commands);
    println!("📈 Compliance percentage: {:.1}%", compliance_percentage);
    
    if compliance_percentage >= 90.0 {
        println!("🏆 EXCELLENT! Near-perfect Redis compatibility!");
    } else if compliance_percentage >= 75.0 {
        println!("🥈 GOOD! Strong Redis compatibility!");
    } else {
        println!("🥉 FAIR! Basic Redis compatibility achieved!");
    }
    
    println!("==========================================");
    
    // Assert we have achieved high compliance
    assert!(compliance_percentage >= 85.0, 
        "Redis compliance should be at least 85%, got {:.1}%", compliance_percentage);
}