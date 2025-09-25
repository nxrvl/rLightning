use std::net::SocketAddr;
use std::time::Duration;
use tokio::time::sleep;

use rlightning::networking::client::Client;
use rlightning::networking::resp::RespValue;
use rlightning::networking::server::Server;
use rlightning::storage::engine::{StorageConfig, StorageEngine};

/// Tests Redis commands against official command specifications
#[tokio::test]
async fn test_redis_command_specifications() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Set up server
    let addr: SocketAddr = "127.0.0.1:16393".parse()?;
    let config = StorageConfig::default();
    let storage = StorageEngine::new(config);
    
    let server = Server::new_with_storage(addr, storage);
    
    // Start server in background
    tokio::spawn(async move {
        server.start().await.unwrap();
    });
    
    // Wait for server to start
    sleep(Duration::from_millis(100)).await;
    
    // Connect client
    let mut client = Client::connect(addr).await?;
    
    // ======== CORE KEY COMMANDS ========
    
    // DEL - Should return number of keys deleted
    let set_for_del: Vec<&[u8]> = vec![b"SET", b"del_key1", b"value"];
    client.send_command(set_for_del).await?;
    let set_for_del2: Vec<&[u8]> = vec![b"SET", b"del_key2", b"value"];
    client.send_command(set_for_del2).await?;
    
    let del_args: Vec<&[u8]> = vec![b"DEL", b"del_key1", b"nonexistent", b"del_key2"];
    let response = client.send_command(del_args).await?;
    assert_eq!(response, RespValue::Integer(2), "DEL should return count of keys actually deleted");
    
    // EXISTS - Should return count of existing keys
    let set_for_exists: Vec<&[u8]> = vec![b"SET", b"exists_key", b"value"];
    client.send_command(set_for_exists).await?;
    
    let exists_args: Vec<&[u8]> = vec![b"EXISTS", b"exists_key", b"nonexistent"];
    let response = client.send_command(exists_args).await?;
    assert_eq!(response, RespValue::Integer(1), "EXISTS should return count of keys that exist");
    
    // EXPIRE - Should return 1 if key exists, 0 if not
    let set_for_expire: Vec<&[u8]> = vec![b"SET", b"expire_key", b"value"];
    client.send_command(set_for_expire).await?;
    
    let expire_args: Vec<&[u8]> = vec![b"EXPIRE", b"expire_key", b"10"];
    let response = client.send_command(expire_args).await?;
    assert_eq!(response, RespValue::Integer(1), "EXPIRE should return 1 when key exists");
    
    let expire_args: Vec<&[u8]> = vec![b"EXPIRE", b"nonexistent", b"10"];
    let response = client.send_command(expire_args).await?;
    assert_eq!(response, RespValue::Integer(0), "EXPIRE should return 0 when key doesn't exist");
    
    // TTL - Should return -2 for nonexistent key, -1 for persistent key, >0 for key with TTL
    let ttl_args: Vec<&[u8]> = vec![b"TTL", b"nonexistent"];
    let response = client.send_command(ttl_args).await?;
    assert_eq!(response, RespValue::Integer(-2), "TTL should return -2 for nonexistent key");
    
    let set_persistent: Vec<&[u8]> = vec![b"SET", b"persistent", b"value"];
    client.send_command(set_persistent).await?;
    let ttl_args: Vec<&[u8]> = vec![b"TTL", b"persistent"];
    let response = client.send_command(ttl_args).await?;
    assert_eq!(response, RespValue::Integer(-1), "TTL should return -1 for persistent key");
    
    let ttl_args: Vec<&[u8]> = vec![b"TTL", b"expire_key"];
    let response = client.send_command(ttl_args).await?;
    if let RespValue::Integer(ttl) = response {
        assert!(ttl > 0 && ttl <= 10, "TTL should return >0 for key with expiry");
    } else {
        panic!("Expected integer response from TTL");
    }
    
    // ======== STRING COMMANDS ========
    
    // SET - Should return OK for simple SET
    let set_args: Vec<&[u8]> = vec![b"SET", b"string_key", b"value"];
    let response = client.send_command(set_args).await?;
    assert_eq!(response, RespValue::SimpleString("OK".to_string()), "SET should return OK");
    
    // SET with NX - Should return nil if key exists
    let set_nx_args: Vec<&[u8]> = vec![b"SET", b"string_key", b"new_value", b"NX"];
    let response = client.send_command(set_nx_args).await?;
    assert_eq!(response, RespValue::BulkString(None), "SET NX should return nil if key exists");
    
    // SET with XX - Should return nil if key doesn't exist
    let set_xx_args: Vec<&[u8]> = vec![b"SET", b"nonexistent_xx", b"value", b"XX"];
    let response = client.send_command(set_xx_args).await?;
    assert_eq!(response, RespValue::BulkString(None), "SET XX should return nil if key doesn't exist");
    
    // INCR - Should return new value
    let set_for_incr: Vec<&[u8]> = vec![b"SET", b"counter", b"10"];
    client.send_command(set_for_incr).await?;
    
    let incr_args: Vec<&[u8]> = vec![b"INCR", b"counter"];
    let response = client.send_command(incr_args).await?;
    assert_eq!(response, RespValue::Integer(11), "INCR should return new value");
    
    // INCR on non-existent key - Should create key with value 1
    let incr_args: Vec<&[u8]> = vec![b"INCR", b"new_counter"];
    let response = client.send_command(incr_args).await?;
    assert_eq!(response, RespValue::Integer(1), "INCR should create key with value 1");
    
    // ======== HASH COMMANDS ========
    
    // HSET - Should return number of fields added (not updated)
    let hset_args: Vec<&[u8]> = vec![
        b"HSET", b"hash_key", 
        b"field1", b"value1",
        b"field2", b"value2"
    ];
    let response = client.send_command(hset_args).await?;
    assert_eq!(response, RespValue::Integer(2), "HSET should return number of fields added");
    
    // Update one field, add one field
    let hset_update_args: Vec<&[u8]> = vec![
        b"HSET", b"hash_key", 
        b"field1", b"new_value1",
        b"field3", b"value3"
    ];
    let response = client.send_command(hset_update_args).await?;
    assert_eq!(response, RespValue::Integer(1), "HSET should return count of new fields only");
    
    // HMSET - Should return OK
    let hmset_args: Vec<&[u8]> = vec![
        b"HMSET", b"hmset_key", 
        b"field1", b"value1",
        b"field2", b"value2"
    ];
    let response = client.send_command(hmset_args).await?;
    assert_eq!(response, RespValue::SimpleString("OK".to_string()), "HMSET should return OK");
    
    // HEXISTS - Should return 1 for existing field, 0 for non-existent
    let hexists_args: Vec<&[u8]> = vec![b"HEXISTS", b"hash_key", b"field1"];
    let response = client.send_command(hexists_args).await?;
    assert_eq!(response, RespValue::Integer(1), "HEXISTS should return 1 for existing field");
    
    let hexists_args: Vec<&[u8]> = vec![b"HEXISTS", b"hash_key", b"nonexistent"];
    let response = client.send_command(hexists_args).await?;
    assert_eq!(response, RespValue::Integer(0), "HEXISTS should return 0 for non-existent field");
    
    // ======== LIST COMMANDS ========
    
    // LPUSH - Should return list length after push
    let lpush_args: Vec<&[u8]> = vec![b"LPUSH", b"list_key", b"value1", b"value2"];
    let response = client.send_command(lpush_args).await?;
    assert_eq!(response, RespValue::Integer(2), "LPUSH should return list length");
    
    // LLEN - Should return list length
    let llen_args: Vec<&[u8]> = vec![b"LLEN", b"list_key"];
    let response = client.send_command(llen_args).await?;
    assert_eq!(response, RespValue::Integer(2), "LLEN should return list length");
    
    let llen_args: Vec<&[u8]> = vec![b"LLEN", b"nonexistent_list"];
    let response = client.send_command(llen_args).await?;
    assert_eq!(response, RespValue::Integer(0), "LLEN on non-existent list should return 0");
    
    // LPOP - Should return value, nil for empty list
    let lpop_args: Vec<&[u8]> = vec![b"LPOP", b"list_key"];
    let response = client.send_command(lpop_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"value2".to_vec())), 
               "LPOP should return leftmost value");
    
    // ======== SET COMMANDS ========
    
    // SADD - Should return number of elements added
    let sadd_args: Vec<&[u8]> = vec![b"SADD", b"set_key", b"value1", b"value2", b"value2"];
    let response = client.send_command(sadd_args).await?;
    assert_eq!(response, RespValue::Integer(2), "SADD should return count of new elements");
    
    // SISMEMBER - Should return 1 for member, 0 for non-member
    let sismember_args: Vec<&[u8]> = vec![b"SISMEMBER", b"set_key", b"value1"];
    let response = client.send_command(sismember_args).await?;
    assert_eq!(response, RespValue::Integer(1), "SISMEMBER should return 1 for member");
    
    let sismember_args: Vec<&[u8]> = vec![b"SISMEMBER", b"set_key", b"nonexistent"];
    let response = client.send_command(sismember_args).await?;
    assert_eq!(response, RespValue::Integer(0), "SISMEMBER should return 0 for non-member");
    
    // ======== SORTED SET COMMANDS ========
    
    // ZADD - Should return number of elements added (not updated)
    let zadd_args: Vec<&[u8]> = vec![
        b"ZADD", b"zset_key", 
        b"10", b"member1", 
        b"20", b"member2"
    ];
    let response = client.send_command(zadd_args).await?;
    assert_eq!(response, RespValue::Integer(2), "ZADD should return count of new elements");
    
    let zadd_update_args: Vec<&[u8]> = vec![
        b"ZADD", b"zset_key", 
        b"15", b"member1",  // Update score
        b"30", b"member3"   // Add new
    ];
    let response = client.send_command(zadd_update_args).await?;
    assert_eq!(response, RespValue::Integer(1), "ZADD should return count of new elements only");
    
    // ZSCORE - Should return score as string
    let zscore_args: Vec<&[u8]> = vec![b"ZSCORE", b"zset_key", b"member1"];
    let response = client.send_command(zscore_args).await?;
    assert_eq!(response, RespValue::BulkString(Some(b"15".to_vec())), 
               "ZSCORE should return score as string");
    
    let zscore_args: Vec<&[u8]> = vec![b"ZSCORE", b"zset_key", b"nonexistent"];
    let response = client.send_command(zscore_args).await?;
    assert_eq!(response, RespValue::BulkString(None), 
               "ZSCORE should return nil for non-existent member");
    
    // Clean up test keys
    let keys = [
        "del_key1", "del_key2", "exists_key", "expire_key", 
        "persistent", "string_key", "nonexistent_xx", "counter", 
        "new_counter", "hash_key", "hmset_key", "list_key", 
        "set_key", "zset_key"
    ];
    
    for chunk in keys.chunks(5) {
        let mut del_args = vec![b"DEL" as &[u8]];
        for key in chunk {
            del_args.push(key.as_bytes());
        }
        client.send_command(del_args).await?;
    }
    
    Ok(())
} 