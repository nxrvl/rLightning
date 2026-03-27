use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

async fn start_server(port: u16) {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    let server = Server::new(addr, storage);
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    sleep(Duration::from_millis(100)).await;
}

async fn connect(port: u16) -> Client {
    let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
    Client::connect(addr).await.unwrap()
}

#[tokio::test]
async fn test_lpushx_rpushx_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17380).await;
    let mut client = connect(17380).await;

    // LPUSHX on non-existent key should return 0
    let resp = client.send_command_str("LPUSHX", &["mylist", "a"]).await?;
    assert_eq!(resp, RespValue::Integer(0));

    // Create the list
    client.send_command_str("RPUSH", &["mylist", "x"]).await?;

    // LPUSHX should work now
    let resp = client.send_command_str("LPUSHX", &["mylist", "a"]).await?;
    assert_eq!(resp, RespValue::Integer(2));

    // RPUSHX should work
    let resp = client.send_command_str("RPUSHX", &["mylist", "z"]).await?;
    assert_eq!(resp, RespValue::Integer(3));

    // Verify order: [a, x, z]
    let resp = client
        .send_command_str("LRANGE", &["mylist", "0", "-1"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
        assert_eq!(values[1], RespValue::BulkString(Some(b"x".to_vec())));
        assert_eq!(values[2], RespValue::BulkString(Some(b"z".to_vec())));
    } else {
        panic!("Expected array response");
    }

    client.send_command_str("DEL", &["mylist"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_linsert_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17381).await;
    let mut client = connect(17381).await;

    // Create list [a, b, d]
    client
        .send_command_str("RPUSH", &["ilist", "a", "b", "d"])
        .await?;

    // Insert "c" before "d"
    let resp = client
        .send_command_str("LINSERT", &["ilist", "BEFORE", "d", "c"])
        .await?;
    assert_eq!(resp, RespValue::Integer(4));

    // Insert "e" after "d"
    let resp = client
        .send_command_str("LINSERT", &["ilist", "AFTER", "d", "e"])
        .await?;
    assert_eq!(resp, RespValue::Integer(5));

    // Verify: [a, b, c, d, e]
    let resp = client
        .send_command_str("LRANGE", &["ilist", "0", "-1"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values.len(), 5);
        assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
        assert_eq!(values[2], RespValue::BulkString(Some(b"c".to_vec())));
        assert_eq!(values[4], RespValue::BulkString(Some(b"e".to_vec())));
    } else {
        panic!("Expected array");
    }

    // Pivot not found
    let resp = client
        .send_command_str("LINSERT", &["ilist", "BEFORE", "z", "x"])
        .await?;
    assert_eq!(resp, RespValue::Integer(-1));

    client.send_command_str("DEL", &["ilist"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_lset_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17382).await;
    let mut client = connect(17382).await;

    // Create list [a, b, c]
    client
        .send_command_str("RPUSH", &["slist", "a", "b", "c"])
        .await?;

    // Set index 1
    let resp = client
        .send_command_str("LSET", &["slist", "1", "B"])
        .await?;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    // Verify
    let resp = client.send_command_str("LINDEX", &["slist", "1"]).await?;
    assert_eq!(resp, RespValue::BulkString(Some(b"B".to_vec())));

    // Negative index
    let resp = client
        .send_command_str("LSET", &["slist", "-1", "C"])
        .await?;
    assert_eq!(resp, RespValue::SimpleString("OK".to_string()));

    client.send_command_str("DEL", &["slist"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_lpos_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17383).await;
    let mut client = connect(17383).await;

    // Create list [a, b, c, b, a]
    client
        .send_command_str("RPUSH", &["plist", "a", "b", "c", "b", "a"])
        .await?;

    // Find first "b"
    let resp = client.send_command_str("LPOS", &["plist", "b"]).await?;
    assert_eq!(resp, RespValue::Integer(1));

    // Find all "b" with COUNT 0
    let resp = client
        .send_command_str("LPOS", &["plist", "b", "COUNT", "0"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values, vec![RespValue::Integer(1), RespValue::Integer(3)]);
    } else {
        panic!("Expected array");
    }

    // Not found
    let resp = client.send_command_str("LPOS", &["plist", "z"]).await?;
    assert_eq!(resp, RespValue::BulkString(None));

    client.send_command_str("DEL", &["plist"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_lmove_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17384).await;
    let mut client = connect(17384).await;

    // Create source [a, b, c]
    client
        .send_command_str("RPUSH", &["msrc", "a", "b", "c"])
        .await?;

    // Move left of src to right of dst
    let resp = client
        .send_command_str("LMOVE", &["msrc", "mdst", "LEFT", "RIGHT"])
        .await?;
    assert_eq!(resp, RespValue::BulkString(Some(b"a".to_vec())));

    // src should be [b, c], dst should be [a]
    let resp = client
        .send_command_str("LRANGE", &["msrc", "0", "-1"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values.len(), 2);
    } else {
        panic!("Expected array");
    }

    let resp = client
        .send_command_str("LRANGE", &["mdst", "0", "-1"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
    } else {
        panic!("Expected array");
    }

    client.send_command_str("DEL", &["msrc", "mdst"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_lmpop_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17385).await;
    let mut client = connect(17385).await;

    // Create lists
    client
        .send_command_str("RPUSH", &["mp1", "a", "b", "c"])
        .await?;
    client.send_command_str("RPUSH", &["mp2", "x", "y"]).await?;

    // Pop 1 from left
    let resp = client
        .send_command_str("LMPOP", &["2", "mp1", "mp2", "LEFT"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values[0], RespValue::BulkString(Some(b"mp1".to_vec())));
        if let RespValue::Array(Some(elements)) = &values[1] {
            assert_eq!(elements[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected elements array");
        }
    } else {
        panic!("Expected array");
    }

    // Pop 2 from right with COUNT
    let resp = client
        .send_command_str("LMPOP", &["2", "mp1", "mp2", "RIGHT", "COUNT", "2"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values[0], RespValue::BulkString(Some(b"mp1".to_vec())));
        if let RespValue::Array(Some(elements)) = &values[1] {
            assert_eq!(elements.len(), 2);
        } else {
            panic!("Expected elements array");
        }
    } else {
        panic!("Expected array");
    }

    client.send_command_str("DEL", &["mp1", "mp2"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_blpop_immediate_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    start_server(17386).await;
    let mut client = connect(17386).await;

    // Create list with data
    client
        .send_command_str("RPUSH", &["blist", "hello", "world"])
        .await?;

    // BLPOP should return immediately
    let resp = client.send_command_str("BLPOP", &["blist", "1"]).await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values[0], RespValue::BulkString(Some(b"blist".to_vec())));
        assert_eq!(values[1], RespValue::BulkString(Some(b"hello".to_vec())));
    } else {
        panic!("Expected array, got {:?}", resp);
    }

    client.send_command_str("DEL", &["blist"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_blpop_timeout_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17387).await;
    let mut client = connect(17387).await;

    // BLPOP on empty key should timeout
    let start = Instant::now();
    let resp = client
        .send_command_str("BLPOP", &["emptykey", "0.2"])
        .await?;
    let elapsed = start.elapsed();

    assert_eq!(resp, RespValue::Array(None));
    assert!(
        elapsed >= Duration::from_millis(150),
        "Expected timeout, elapsed: {:?}",
        elapsed
    );

    Ok(())
}

#[tokio::test]
async fn test_blpop_wakeup_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    start_server(17388).await;

    // Client 1 will do BLPOP
    let mut client1 = connect(17388).await;
    // Client 2 will do LPUSH to wake up client 1
    let mut client2 = connect(17388).await;

    // Spawn BLPOP on client1 in background
    let handle = tokio::spawn(async move {
        client1.send_command_str("BLPOP", &["wakekey", "5"]).await
    });

    // Give client1 time to start blocking
    sleep(Duration::from_millis(200)).await;

    // Push data with client2
    client2
        .send_command_str("RPUSH", &["wakekey", "wakedata"])
        .await?;

    // Client1 should wake up
    let result = tokio::time::timeout(Duration::from_secs(3), handle).await??;
    let resp = result?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values[0], RespValue::BulkString(Some(b"wakekey".to_vec())));
        assert_eq!(values[1], RespValue::BulkString(Some(b"wakedata".to_vec())));
    } else {
        panic!("Expected array, got {:?}", resp);
    }

    Ok(())
}

#[tokio::test]
async fn test_brpop_immediate_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    start_server(17389).await;
    let mut client = connect(17389).await;

    client
        .send_command_str("RPUSH", &["brlist", "a", "b"])
        .await?;

    let resp = client.send_command_str("BRPOP", &["brlist", "1"]).await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values[0], RespValue::BulkString(Some(b"brlist".to_vec())));
        assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
    } else {
        panic!("Expected array, got {:?}", resp);
    }

    client.send_command_str("DEL", &["brlist"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_blmove_immediate_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    start_server(17390).await;
    let mut client = connect(17390).await;

    client
        .send_command_str("RPUSH", &["bmsrc", "a", "b"])
        .await?;

    let resp = client
        .send_command_str("BLMOVE", &["bmsrc", "bmdst", "LEFT", "RIGHT", "1"])
        .await?;
    assert_eq!(resp, RespValue::BulkString(Some(b"a".to_vec())));

    // Verify dst
    let resp = client
        .send_command_str("LRANGE", &["bmdst", "0", "-1"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
    } else {
        panic!("Expected array");
    }

    client.send_command_str("DEL", &["bmsrc", "bmdst"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_blmpop_immediate_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    start_server(17391).await;
    let mut client = connect(17391).await;

    client
        .send_command_str("RPUSH", &["bmplist", "a", "b"])
        .await?;

    let resp = client
        .send_command_str("BLMPOP", &["1", "1", "bmplist", "LEFT"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values[0], RespValue::BulkString(Some(b"bmplist".to_vec())));
        if let RespValue::Array(Some(elements)) = &values[1] {
            assert_eq!(elements[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected elements array");
        }
    } else {
        panic!("Expected array, got {:?}", resp);
    }

    client.send_command_str("DEL", &["bmplist"]).await?;
    Ok(())
}

#[tokio::test]
async fn test_blpop_multi_key_integration() -> Result<(), Box<dyn std::error::Error + Send + Sync>>
{
    start_server(17392).await;
    let mut client = connect(17392).await;

    // Only second key has data
    client.send_command_str("RPUSH", &["mk2", "hello"]).await?;

    let resp = client
        .send_command_str("BLPOP", &["mk1", "mk2", "1"])
        .await?;
    if let RespValue::Array(Some(values)) = resp {
        assert_eq!(values[0], RespValue::BulkString(Some(b"mk2".to_vec())));
        assert_eq!(values[1], RespValue::BulkString(Some(b"hello".to_vec())));
    } else {
        panic!("Expected array, got {:?}", resp);
    }

    client.send_command_str("DEL", &["mk1", "mk2"]).await?;
    Ok(())
}
