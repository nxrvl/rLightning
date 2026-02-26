use crate::command::error::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::command::utils::parse_ttl;

// List commands
pub use crate::command::types::list::{lpush, rpush, lpop, rpop, lindex, llen, lrange, ltrim, lpushx, rpushx, linsert, lset, lrem, lpos, lmove, rpoplpush, lmpop, blpop, brpop, blmove, blmpop};

// Blocking infrastructure
pub use crate::command::types::blocking::BlockingManager;

// String commands
pub use crate::command::types::string::{set, get, mget, mset, msetnx, incr, decr, incrby, decrby, incrbyfloat, append, strlen, getrange, setrange, getset, setnx, setex, pexpire, pttl, getex, getdel, psetex, lcs, substr};

// Hash commands
pub use crate::command::types::hash::{hset, hget, hgetall, hdel, hexists, hmset, hkeys, hvals, hlen, hmget, hincrby, hincrbyfloat, hsetnx, hstrlen, hrandfield, hscan};

// Set commands
pub use crate::command::types::set::{sadd, srem, smembers, sismember, scard, spop, srandmember, sinter, sinterstore, sunion, sunionstore, sdiff, sdiffstore, smove, sintercard, smismember, sscan};

// Sorted Set commands
pub use crate::command::types::sorted_set::{zadd, zrem, zscore, zrange, zcard, zcount, zrank, zrevrange, zincrby, zrangebyscore, zrevrangebyscore, zrangebylex, zrevrangebylex, zremrangebyrank, zremrangebyscore, zremrangebylex, zlexcount, zrevrank, zinterstore, zunionstore, zinter, zunion, zdiff, zdiffstore, zpopmin, zpopmax, bzpopmin, bzpopmax, zrandmember, zmscore, zmpop, bzmpop, zrangestore, zscan, zrange_unified};

// JSON commands
pub use crate::command::types::json::{json_get, json_set, json_type, json_arrappend, json_arrtrim, json_resp, json_del, json_objkeys, json_objlen, json_arrlen, json_numincrby, json_mget, json_arrindex};

// Key commands
pub use crate::command::types::key::{copy, move_cmd, unlink, touch, expireat, pexpireat, expiretime, pexpiretime, object, dump, restore, sort, sort_ro, wait_cmd, waitaof, select, scan_with_type};

// Bitmap commands
pub use crate::command::types::bitmap::{setbit, getbit, bitcount, bitpos, bitop, bitfield, bitfield_ro};

// HyperLogLog commands
pub use crate::command::types::hyperloglog::{pfadd, pfcount, pfmerge};

// Geo commands
pub use crate::command::types::geo::{geoadd, geodist, geohash, geopos, geosearch, geosearchstore, georadius, georadiusbymember};

// Stream commands
pub use crate::command::types::stream::{xadd, xlen, xrange, xrevrange, xread, xtrim, xdel, xinfo, xgroup, xreadgroup, xack, xpending, xclaim, xautoclaim};

// Server commands
pub use crate::command::types::server::{info, auth, config, keys, rename, renamenx, flushall, flushdb, monitor, dbsize, randomkey};

// Connection & server commands
pub use crate::command::types::connection::{
    quit, reset, echo, client, command_cmd, config_set, config_rewrite, config_resetstat,
    save, bgsave, bgrewriteaof, lastsave, shutdown, slowlog, latency, memory, debug,
    swapdb, time, lolwut, info_expanded,
};

// Cluster commands
pub use crate::command::types::cluster::{cluster_command, asking, readonly, readwrite, migrate};

// Sentinel commands
pub use crate::command::types::sentinel::sentinel_command;

// Module commands
pub use crate::command::types::module::module_command;

// Alias for type_command as get_type
pub use type_command as get_type;

// Define the aliases for GET_JSON and SET_JSON as async functions that call the original json_* functions
#[allow(dead_code)]
pub async fn get_json(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    use tracing::debug;
    debug!("GET_JSON command received with {} args", args.len());
    
    if !args.is_empty() && !args[0].is_empty() {
        let key_str = String::from_utf8_lossy(&args[0]);
        debug!("GET_JSON for key: {}", key_str);
    }
    
    json_get(engine, args).await
}

#[allow(dead_code)]
pub async fn set_json(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    use tracing::debug;
    debug!("SET_JSON command received with {} args", args.len());
    
    if !args.is_empty() && !args[0].is_empty() {
        let key_str = String::from_utf8_lossy(&args[0]);
        debug!("SET_JSON for key: {}", key_str);
        
        if args.len() > 2 {
            debug!("SET_JSON data size: {} bytes", args[2].len());
            
            // Apply extra sanitization for JSON data from Python clients
            if args[2].len() > 1000 {
                debug!("Large JSON data detected in SET_JSON command");
            }
        }
    }
    
    json_set(engine, args).await
}

// Server commands

/// Core Redis commands
/// Redis PING command
#[allow(dead_code)]
pub async fn ping(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    match args.len() {
        0 => Ok(RespValue::SimpleString("PONG".to_string())),
        1 => Ok(RespValue::BulkString(Some(args[0].clone()))),
        _ => Err(CommandError::WrongNumberOfArguments),
    }
}

/// Redis DEL command - Delete a key
pub async fn del(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let mut deleted = 0;
    for key in args {
        if engine.del(key).await? {
            deleted += 1;
        }
    }
    
    Ok(RespValue::Integer(deleted))
}

/// Redis EXISTS command - Determine if a key exists
pub async fn exists(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let mut count = 0;
    for key in args {
        if engine.exists(key).await? {
            count += 1;
        }
    }
    
    Ok(RespValue::Integer(count))
}

/// Redis EXPIRE command - Set a key's time to live in seconds
pub async fn expire(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = args[0].clone();
    let ttl = parse_ttl(&args[1])?;

    // Parse optional condition flags (NX, XX, GT, LT) - Redis 7.0+
    let mut nx = false;
    let mut xx = false;
    let mut gt = false;
    let mut lt = false;
    for arg in args.iter().skip(2) {
        match crate::command::utils::bytes_to_string(arg)?.to_uppercase().as_str() {
            "NX" => nx = true,
            "XX" => xx = true,
            "GT" => gt = true,
            "LT" => lt = true,
            _ => {
                return Err(CommandError::InvalidArgument(
                    "Unsupported option".to_string(),
                ));
            }
        }
    }

    // If TTL is None (negative), we need to delete the key
    if ttl.is_none() {
        let deleted = engine.del(&key).await?;
        return Ok(RespValue::Integer(if deleted { 1 } else { 0 }));
    }

    // Apply condition flags
    if nx || xx || gt || lt {
        let current_ttl = engine.ttl(&key).await?;
        let has_expiry = current_ttl.is_some();

        if nx && has_expiry {
            return Ok(RespValue::Integer(0));
        }
        if xx && !has_expiry {
            return Ok(RespValue::Integer(0));
        }
        if gt {
            if let Some(current) = current_ttl {
                if let Some(new_ttl) = &ttl {
                    if *new_ttl <= current {
                        return Ok(RespValue::Integer(0));
                    }
                }
            }
        }
        if lt {
            if let Some(current) = current_ttl {
                if let Some(new_ttl) = &ttl {
                    if *new_ttl >= current {
                        return Ok(RespValue::Integer(0));
                    }
                }
            }
        }
    }

    match engine.expire(&key, ttl).await? {
        true => Ok(RespValue::Integer(1)),
        false => Ok(RespValue::Integer(0)),
    }
}

/// Redis TTL command - Get the time to live for a key in seconds
pub async fn ttl(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // First check if the key exists
    let exists = engine.exists(&key).await?;
    if !exists {
        return Ok(RespValue::Integer(-2)); // Key does not exist
    }
    
    // If key exists, check its TTL
    let ttl = engine.ttl(&key).await?;
    
    match ttl {
        Some(duration) => {
            // Ensure we return at least 1 second if the duration is >0
            let seconds = duration.as_secs();
            Ok(RespValue::Integer(if seconds > 0 { seconds as i64 } else { 1 }))
        },
        None => Ok(RespValue::Integer(-1)), // Key exists but has no expiration
    }
}

/// Redis PERSIST command - Remove the expiration from a key
pub async fn persist(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // First check if the key exists and has a TTL
    let ttl = engine.ttl(&key).await?;
    if ttl.is_none() {
        // Key doesn't exist or has no TTL
        return Ok(RespValue::Integer(0));
    }
    
    // Key exists and has a TTL, so remove it
    match engine.expire(&key, None).await? {
        true => Ok(RespValue::Integer(1)),
        false => Ok(RespValue::Integer(0)),
    }
}

/// Redis TYPE command - Return the type of the value stored at key
pub async fn type_command(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = args[0].clone();
    
    // Check if the key exists
    if !engine.exists(&key).await? {
        return Ok(RespValue::SimpleString("none".to_string()));
    }
    
    // Get the type of the key
    let key_type = engine.get_type(&key).await?;
    
    Ok(RespValue::SimpleString(key_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::storage::engine::StorageConfig;
    use std::time::Duration;

    #[tokio::test]
    async fn test_persist_command() {
        let config = StorageConfig::default();
        let storage = Arc::new(StorageEngine::new(config));
        
        // Set a key with TTL
        let key = "test_key".as_bytes().to_vec();
        let value = "test_value".as_bytes().to_vec();
        storage.set(key.clone(), value, Some(Duration::from_secs(60))).await.unwrap();
        
        // Verify key has TTL
        let ttl = storage.ttl(&key).await.unwrap();
        assert!(ttl.is_some(), "Key should have TTL before PERSIST");
        
        // Call PERSIST command
        let result = persist(&storage, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1), "PERSIST should return 1 when TTL is removed");
        
        // Verify TTL is removed
        let ttl_after = storage.ttl(&key).await.unwrap();
        assert!(ttl_after.is_none(), "Key should not have TTL after PERSIST");
        
        // Test PERSIST on key without TTL
        let result = persist(&storage, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0), "PERSIST should return 0 when key has no TTL");
        
        // Test PERSIST on non-existent key
        let nonexistent = "nonexistent".as_bytes().to_vec();
        let result = persist(&storage, &[nonexistent]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0), "PERSIST should return 0 for non-existent key");
        
        // Test PERSIST with wrong number of arguments
        let result = persist(&storage, &[]).await;
        assert!(result.is_err(), "PERSIST with no args should return error");
        assert_eq!(result.unwrap_err().to_string(), "ERR wrong number of arguments", "Wrong error message");
    }
} 