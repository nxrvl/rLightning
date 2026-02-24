use std::sync::Arc;

use crate::command::{Command, CommandError, CommandResult};
use crate::command::commands;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// The CommandHandler routes incoming commands to their implementations
#[derive(Clone)]
pub struct CommandHandler {
    storage: Arc<StorageEngine>,
}

impl CommandHandler {
    /// Create a new CommandHandler with the given storage engine
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        CommandHandler { storage }
    }
    
    /// Process a command and return the result
    pub async fn process(&self, command: Command) -> CommandResult {
        let cmd_lowercase = command.name.to_lowercase();
        
        let result = match cmd_lowercase.as_str() {
            // Key expiration/TTL commands
            "expire" => commands::expire(&self.storage, &command.args).await,
            "pexpire" => commands::pexpire(&self.storage, &command.args).await,
            "ttl" => commands::ttl(&self.storage, &command.args).await,
            "pttl" => commands::pttl(&self.storage, &command.args).await,
            "persist" => commands::persist(&self.storage, &command.args).await,
            "expireat" => commands::expireat(&self.storage, &command.args).await,
            "pexpireat" => commands::pexpireat(&self.storage, &command.args).await,
            "expiretime" => commands::expiretime(&self.storage, &command.args).await,
            "pexpiretime" => commands::pexpiretime(&self.storage, &command.args).await,

            // Basic key operations
            "del" => commands::del(&self.storage, &command.args).await,
            "unlink" => commands::unlink(&self.storage, &command.args).await,
            "exists" => commands::exists(&self.storage, &command.args).await,
            "type" => commands::get_type(&self.storage, &command.args).await,
            "copy" => commands::copy(&self.storage, &command.args).await,
            "move" => commands::move_cmd(&self.storage, &command.args).await,
            "touch" => commands::touch(&self.storage, &command.args).await,
            "object" => commands::object(&self.storage, &command.args).await,
            "dump" => commands::dump(&self.storage, &command.args).await,
            "restore" => commands::restore(&self.storage, &command.args).await,
            "sort" => commands::sort(&self.storage, &command.args).await,
            "sort_ro" => commands::sort_ro(&self.storage, &command.args).await,
            "wait" => commands::wait_cmd(&self.storage, &command.args).await,
            "waitaof" => commands::waitaof(&self.storage, &command.args).await,
            "select" => commands::select(&self.storage, &command.args).await,
            
            // Server commands
            "ping" => {
                if command.args.is_empty() {
                    Ok(RespValue::SimpleString("PONG".to_string()))
                } else {
                    Ok(RespValue::BulkString(Some(command.args[0].clone())))
                }
            },
            
            // String commands
            "set" => commands::set(&self.storage, &command.args).await,
            "get" => commands::get(&self.storage, &command.args).await,
            "mget" => commands::mget(&self.storage, &command.args).await,
            "mset" => commands::mset(&self.storage, &command.args).await,
            "msetnx" => commands::msetnx(&self.storage, &command.args).await,
            "setnx" => commands::setnx(&self.storage, &command.args).await,
            "setex" => commands::setex(&self.storage, &command.args).await,
            "incr" => commands::incr(&self.storage, &command.args).await,
            "decr" => commands::decr(&self.storage, &command.args).await,
            "incrby" => commands::incrby(&self.storage, &command.args).await,
            "decrby" => commands::decrby(&self.storage, &command.args).await,
            "incrbyfloat" => commands::incrbyfloat(&self.storage, &command.args).await,
            "append" => commands::append(&self.storage, &command.args).await,
            "strlen" => commands::strlen(&self.storage, &command.args).await,
            "getrange" => commands::getrange(&self.storage, &command.args).await,
            "setrange" => commands::setrange(&self.storage, &command.args).await,
            "getset" => commands::getset(&self.storage, &command.args).await,
            
            // List commands
            "lpush" => commands::lpush(&self.storage, &command.args).await,
            "rpush" => commands::rpush(&self.storage, &command.args).await,
            "lpop" => commands::lpop(&self.storage, &command.args).await,
            "rpop" => commands::rpop(&self.storage, &command.args).await,
            "lrange" => commands::lrange(&self.storage, &command.args).await,
            "lindex" => commands::lindex(&self.storage, &command.args).await,
            "llen" => commands::llen(&self.storage, &command.args).await,
            "ltrim" => commands::ltrim(&self.storage, &command.args).await,
            
            // Hash commands
            "hset" => commands::hset(&self.storage, &command.args).await,
            "hget" => commands::hget(&self.storage, &command.args).await,
            "hgetall" => commands::hgetall(&self.storage, &command.args).await,
            "hdel" => commands::hdel(&self.storage, &command.args).await,
            "hexists" => commands::hexists(&self.storage, &command.args).await,
            "hmset" => commands::hmset(&self.storage, &command.args).await,
            "hkeys" => commands::hkeys(&self.storage, &command.args).await,
            "hvals" => commands::hvals(&self.storage, &command.args).await,
            "hlen" => commands::hlen(&self.storage, &command.args).await,
            "hmget" => commands::hmget(&self.storage, &command.args).await,
            "hincrby" => commands::hincrby(&self.storage, &command.args).await,
            "hincrbyfloat" => commands::hincrbyfloat(&self.storage, &command.args).await,
            "hsetnx" => commands::hsetnx(&self.storage, &command.args).await,
            "hstrlen" => commands::hstrlen(&self.storage, &command.args).await,
            
            // Set commands
            "sadd" => commands::sadd(&self.storage, &command.args).await,
            "srem" => commands::srem(&self.storage, &command.args).await,
            "smembers" => commands::smembers(&self.storage, &command.args).await,
            "sismember" => commands::sismember(&self.storage, &command.args).await,
            "scard" => commands::scard(&self.storage, &command.args).await,
            "spop" => commands::spop(&self.storage, &command.args).await,
            "srandmember" => commands::srandmember(&self.storage, &command.args).await,
            "sinter" => commands::sinter(&self.storage, &command.args).await,
            "sinterstore" => commands::sinterstore(&self.storage, &command.args).await,
            "sunion" => commands::sunion(&self.storage, &command.args).await,
            "sunionstore" => commands::sunionstore(&self.storage, &command.args).await,
            "sdiff" => commands::sdiff(&self.storage, &command.args).await,
            "sdiffstore" => commands::sdiffstore(&self.storage, &command.args).await,

            // Sorted Set commands
            "zadd" => commands::zadd(&self.storage, &command.args).await,
            "zrange" => commands::zrange(&self.storage, &command.args).await,
            "zrem" => commands::zrem(&self.storage, &command.args).await,
            "zscore" => commands::zscore(&self.storage, &command.args).await,
            "zcard" => commands::zcard(&self.storage, &command.args).await,
            "zcount" => commands::zcount(&self.storage, &command.args).await,
            "zrank" => commands::zrank(&self.storage, &command.args).await,
            "zrevrange" => commands::zrevrange(&self.storage, &command.args).await,
            "zincrby" => commands::zincrby(&self.storage, &command.args).await,
            
            // JSON commands
            "json.get" | "jsonget" | "get_json" | "getjson" => commands::json_get(&self.storage, &command.args).await,
            "json.set" | "jsonset" | "set_json" | "setjson" => commands::json_set(&self.storage, &command.args).await,
            "json.type" | "jsontype" => commands::json_type(&self.storage, &command.args).await,
            "json.arrappend" | "jsonarrappend" => commands::json_arrappend(&self.storage, &command.args).await,
            "json.arrtrim" | "jsonarrtrim" => commands::json_arrtrim(&self.storage, &command.args).await,
            "json.resp" | "jsonresp" => commands::json_resp(&self.storage, &command.args).await,
            "json.del" | "jsondel" => commands::json_del(&self.storage, &command.args).await,
            "json.objkeys" | "jsonobjkeys" => commands::json_objkeys(&self.storage, &command.args).await,
            "json.objlen" | "jsonobjlen" => commands::json_objlen(&self.storage, &command.args).await,
            "json.arrlen" | "jsonarrlen" => commands::json_arrlen(&self.storage, &command.args).await,
            "json.numincrby" | "jsonnumincrby" => commands::json_numincrby(&self.storage, &command.args).await,
            "json.mget" | "jsonmget" => commands::json_mget(&self.storage, &command.args).await,
            "json.arrindex" | "jsonarrindex" => commands::json_arrindex(&self.storage, &command.args).await,

            // Server commands
            "info" => commands::info(&self.storage, &command.args).await,
            "auth" => commands::auth(&self.storage, &command.args).await,
            "config" => commands::config(&self.storage, &command.args).await,
            "keys" => commands::keys(&self.storage, &command.args).await,
            "rename" => commands::rename(&self.storage, &command.args).await,
            "flushall" => commands::flushall(&self.storage, &command.args).await,
            "flushdb" => commands::flushdb(&self.storage, &command.args).await,
            "monitor" => commands::monitor(&self.storage, &command.args).await,
            "scan" => commands::scan_with_type(&self.storage, &command.args).await,
            "dbsize" => commands::dbsize(&self.storage, &command.args).await,
            "randomkey" => commands::randomkey(&self.storage, &command.args).await,
            
            // Unknown command
            _ => {
                return Err(CommandError::UnknownCommand(command.name))
            }
        };
        
        // Debug commands for development
        #[cfg(debug_assertions)]
        if command.name == "debug" {
            return self.handle_debug_command(command).await;
        }
        
        result
    }
    
    // Debug command handler for development builds
    #[cfg(debug_assertions)]
    async fn handle_debug_command(&self, command: Command) -> CommandResult {
        if command.args.is_empty() {
            return Err(CommandError::WrongNumberOfArguments);
        }
        
        let subcommand = String::from_utf8_lossy(&command.args[0]).to_lowercase();
        
        match subcommand.as_str() {
            "object" => {
                if command.args.len() < 3 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                
                let action = String::from_utf8_lossy(&command.args[1]).to_lowercase();
                let key = &command.args[2];
                
                if action == "info" {
                    // Get detailed info about a key
                    if !self.storage.exists(key).await? {
                        return Ok(RespValue::SimpleString("Key does not exist".to_string()));
                    }
                    
                    let key_type = self.storage.get_type(key).await?;
                    let ttl = self.storage.ttl(key).await?;
                    
                    let ttl_info = match ttl {
                        Some(t) => format!("{} seconds", t.as_secs()),
                        None => "No expiration".to_string(),
                    };
                    
                    return Ok(RespValue::SimpleString(format!(
                        "Type: {}\nTTL: {}", 
                        key_type, 
                        ttl_info
                    )));
                }
            },
            "help" => {
                return Ok(RespValue::SimpleString(
                    "DEBUG commands:\n\
                     DEBUG OBJECT INFO <key> - Get detailed information about a key"
                     .to_string()
                ));
            },
            _ => {
                return Err(CommandError::UnknownCommand(format!("DEBUG {}", subcommand)));
            }
        }
        
        Err(CommandError::UnknownCommand(format!("DEBUG {}", subcommand)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use crate::command::Command;
    use std::time::Duration;

    #[tokio::test]
    async fn test_ping_command() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test basic PING
        let command = Command {
            name: "ping".to_string(),
            args: vec![],
        };
        
        let result = handler.process(command).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("PONG".to_string()));
        
        // Test PING with argument
        let command = Command {
            name: "ping".to_string(),
            args: vec![b"hello".to_vec()],
        };
        
        let result = handler.process(command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
    }
    
    #[tokio::test]
    async fn test_set_get_command() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test SET command
        let set_command = Command {
            name: "set".to_string(),
            args: vec![b"mykey".to_vec(), b"myvalue".to_vec()],
        };
        
        let result = handler.process(set_command).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Test GET command
        let get_command = Command {
            name: "get".to_string(),
            args: vec![b"mykey".to_vec()],
        };
        
        let result = handler.process(get_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));
        
        // Test GET for non-existent key
        let get_missing = Command {
            name: "get".to_string(),
            args: vec![b"nonexistent".to_vec()],
        };
        
        let result = handler.process(get_missing).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }
    
    #[tokio::test]
    async fn test_set_with_expiry() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test SET with EX option
        let set_ex_command = Command {
            name: "set".to_string(),
            args: vec![
                b"ex_key".to_vec(), 
                b"ex_value".to_vec(),
                b"EX".to_vec(),
                b"1".to_vec(), // 1 second expiry
            ],
        };
        
        let result = handler.process(set_ex_command).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Check TTL
        let ttl_command = Command {
            name: "ttl".to_string(),
            args: vec![b"ex_key".to_vec()],
        };
        
        let result = handler.process(ttl_command).await.unwrap();
        if let RespValue::Integer(ttl) = result {
            assert!(ttl > 0 && ttl <= 1);
        } else {
            panic!("Expected integer response from TTL");
        }
        
        // Test SET with PX option
        let set_px_command = Command {
            name: "set".to_string(),
            args: vec![
                b"px_key".to_vec(), 
                b"px_value".to_vec(),
                b"PX".to_vec(),
                b"100".to_vec(), // 100ms expiry
            ],
        };
        
        let result = handler.process(set_px_command).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Verify that the key exists
        let get_command = Command {
            name: "get".to_string(),
            args: vec![b"px_key".to_vec()],
        };
        
        let result = handler.process(get_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"px_value".to_vec())));
        
        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;
        
        // Verify that the key is gone
        let get_command_after = Command {
            name: "get".to_string(),
            args: vec![b"px_key".to_vec()],
        };
        
        let result = handler.process(get_command_after).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }
    
    #[tokio::test]
    async fn test_del_exists_command() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Set up a few keys
        for i in 1..=3 {
            let set_command = Command {
                name: "set".to_string(),
                args: vec![
                    format!("delkey{}", i).into_bytes(),
                    format!("value{}", i).into_bytes(),
                ],
            };
            handler.process(set_command).await.unwrap();
        }
        
        // Test EXISTS command
        let exists_command = Command {
            name: "exists".to_string(),
            args: vec![
                b"delkey1".to_vec(),
                b"delkey2".to_vec(),
                b"nonexistent".to_vec(),
            ],
        };
        
        let result = handler.process(exists_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        
        // Test DEL command
        let del_command = Command {
            name: "del".to_string(),
            args: vec![
                b"delkey1".to_vec(),
                b"delkey2".to_vec(),
                b"nonexistent".to_vec(),
            ],
        };
        
        let result = handler.process(del_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        
        // Verify keys are gone with EXISTS
        let exists_command = Command {
            name: "exists".to_string(),
            args: vec![
                b"delkey1".to_vec(),
                b"delkey2".to_vec(),
            ],
        };
        
        let result = handler.process(exists_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
        
        // Verify delkey3 still exists
        let exists_command = Command {
            name: "exists".to_string(),
            args: vec![b"delkey3".to_vec()],
        };
        
        let result = handler.process(exists_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }
    
    #[tokio::test]
    async fn test_mget_command() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Set up a few keys
        let keys = ["mgetkey1", "mgetkey2", "mgetkey3"];
        for (i, key) in keys.iter().enumerate() {
            let set_command = Command {
                name: "set".to_string(),
                args: vec![
                    key.as_bytes().to_vec(),
                    format!("mgetvalue{}", i+1).into_bytes(),
                ],
            };
            handler.process(set_command).await.unwrap();
        }
        
        // Test MGET command
        let mget_command = Command {
            name: "mget".to_string(),
            args: vec![
                b"mgetkey1".to_vec(),
                b"mgetkey2".to_vec(),
                b"nonexistent".to_vec(),
                b"mgetkey3".to_vec(),
            ],
        };
        
        let result = handler.process(mget_command).await.unwrap();
        
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 4);
            assert_eq!(values[0], RespValue::BulkString(Some(b"mgetvalue1".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"mgetvalue2".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(None));
            assert_eq!(values[3], RespValue::BulkString(Some(b"mgetvalue3".to_vec())));
        } else {
            panic!("Expected Array response from MGET");
        }
    }
    
    #[tokio::test]
    async fn test_unknown_command() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test an unknown command
        let unknown_command = Command {
            name: "notacommand".to_string(),
            args: vec![],
        };
        
        let result = handler.process(unknown_command).await;
        
        // The result should be an error, not an unwrapped value
        assert!(result.is_err());
        match result {
            Err(CommandError::UnknownCommand(cmd)) => {
                assert_eq!(cmd, "notacommand");
            },
            _ => panic!("Expected UnknownCommand error")
        }
    }
    
    #[tokio::test]
    async fn test_hash_commands() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test HSET command
        let hset_cmd = Command {
            name: "hset".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"name".to_vec(),
                b"John".to_vec(),
            ],
        };
        
        let result = handler.process(hset_cmd).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // New field
        
        // Test HSET again with the same field
        let hset_again_cmd = Command {
            name: "hset".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"name".to_vec(),
                b"Jane".to_vec(),
            ],
        };
        
        let result = handler.process(hset_again_cmd).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Field already existed
        
        // Add another field
        let hset_email_cmd = Command {
            name: "hset".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"email".to_vec(),
                b"jane@example.com".to_vec(),
            ],
        };
        
        handler.process(hset_email_cmd).await.unwrap();
        
        // Test HGET command
        let hget_cmd = Command {
            name: "hget".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"name".to_vec(),
            ],
        };
        
        let result = handler.process(hget_cmd).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"Jane".to_vec())));
        
        // Test HGET for non-existent field
        let hget_missing_cmd = Command {
            name: "hget".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"phone".to_vec(),
            ],
        };
        
        let result = handler.process(hget_missing_cmd).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
        
        // Test HEXISTS command
        let hexists_cmd = Command {
            name: "hexists".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"name".to_vec(),
            ],
        };
        
        let result = handler.process(hexists_cmd).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Field exists
        
        // Test HEXISTS for non-existent field
        let hexists_missing_cmd = Command {
            name: "hexists".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"phone".to_vec(),
            ],
        };
        
        let result = handler.process(hexists_missing_cmd).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Field doesn't exist
        
        // Test HGETALL command
        let hgetall_cmd = Command {
            name: "hgetall".to_string(),
            args: vec![
                b"user:1".to_vec(),
            ],
        };
        
        let result = handler.process(hgetall_cmd).await.unwrap();
        if let RespValue::Array(Some(fields_and_values)) = result {
            assert_eq!(fields_and_values.len(), 4); // 2 fields = 4 items (field + value pairs)
            
            // Results should contain both fields and their values
            let mut found_name = false;
            let mut found_email = false;
            
            for i in (0..fields_and_values.len()).step_by(2) {
                if let RespValue::BulkString(Some(field)) = &fields_and_values[i] {
                    if field == b"name" {
                        found_name = true;
                        assert_eq!(fields_and_values[i+1], RespValue::BulkString(Some(b"Jane".to_vec())));
                    } else if field == b"email" {
                        found_email = true;
                        assert_eq!(fields_and_values[i+1], RespValue::BulkString(Some(b"jane@example.com".to_vec())));
                    }
                }
            }
            
            assert!(found_name, "Should find 'name' field");
            assert!(found_email, "Should find 'email' field");
        } else {
            panic!("Expected Array response from HGETALL");
        }
        
        // Test HDEL command
        let hdel_cmd = Command {
            name: "hdel".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"name".to_vec(),
                b"nonexistent".to_vec(),
            ],
        };
        
        let result = handler.process(hdel_cmd).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 field deleted
        
        // Verify the field was deleted
        let hget_after_del_cmd = Command {
            name: "hget".to_string(),
            args: vec![
                b"user:1".to_vec(),
                b"name".to_vec(),
            ],
        };
        
        let result = handler.process(hget_after_del_cmd).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None)); // Field should be gone
        
        // Check HGETALL after deletion
        let hgetall_after_cmd = Command {
            name: "hgetall".to_string(),
            args: vec![
                b"user:1".to_vec(),
            ],
        };
        
        let result = handler.process(hgetall_after_cmd).await.unwrap();
        if let RespValue::Array(Some(fields_and_values)) = result {
            assert_eq!(fields_and_values.len(), 2); // 1 field = 2 items (field + value)
            
            // Only email should remain
            if let RespValue::BulkString(Some(field)) = &fields_and_values[0] {
                assert_eq!(field, b"email");
                assert_eq!(fields_and_values[1], RespValue::BulkString(Some(b"jane@example.com".to_vec())));
            } else {
                panic!("Expected BulkString for field");
            }
        } else {
            panic!("Expected Array response from HGETALL");
        }
    }

    #[tokio::test]
    async fn test_list_commands() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Use a key with "list" in the name to clearly indicate type
        let list_key = b"list_key_for_test".to_vec();
        
        // Ensure the key doesn't exist
        let del_command = Command {
            name: "del".to_string(),
            args: vec![list_key.clone()],
        };
        let _ = handler.process(del_command).await.unwrap();
        
        // Initialize the key as a list
        let empty_list: Vec<Vec<u8>> = Vec::new();
        let serialized = bincode::serialize(&empty_list).unwrap();
        storage.set(list_key.clone(), serialized, None).await.unwrap();
        
        // Verify the key type
        let key_type = storage.get_type(&list_key).await.unwrap();
        assert_eq!(key_type, "list");
        
        // Test LPUSH
        let lpush_command = Command {
            name: "lpush".to_string(),
            args: vec![
                list_key.clone(),
                b"world".to_vec(),
            ],
        };
        let result = handler.process(lpush_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
        
        // Test LPUSH again (prepends)
        let lpush_command = Command {
            name: "lpush".to_string(),
            args: vec![
                list_key.clone(),
                b"hello".to_vec(),
            ],
        };
        let result = handler.process(lpush_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        
        // Test RPUSH (appends)
        let rpush_command = Command {
            name: "rpush".to_string(),
            args: vec![
                list_key.clone(),
                b"!".to_vec(),
            ],
        };
        let result = handler.process(rpush_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
        
        // Test LRANGE to get the entire list
        let lrange_command = Command {
            name: "lrange".to_string(),
            args: vec![
                list_key.clone(),
                b"0".to_vec(), 
                b"-1".to_vec(),
            ],
        };
        let result = handler.process(lrange_command).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"hello".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"world".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"!".to_vec())));
        } else {
            panic!("Expected array response");
        }
        
        // Test LPOP
        let lpop_command = Command {
            name: "lpop".to_string(),
            args: vec![list_key.clone()],
        };
        let result = handler.process(lpop_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
        
        // Test RPOP
        let rpop_command = Command {
            name: "rpop".to_string(),
            args: vec![list_key.clone()],
        };
        let result = handler.process(rpop_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"!".to_vec())));
        
        // Verify the list now has only one element
        let lrange_command = Command {
            name: "lrange".to_string(),
            args: vec![
                list_key.clone(),
                b"0".to_vec(), 
                b"-1".to_vec(),
            ],
        };
        let result = handler.process(lrange_command).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], RespValue::BulkString(Some(b"world".to_vec())));
        } else {
            panic!("Expected array response");
        }
        
        // Pop the last element and verify the key is removed
        let lpop_command = Command {
            name: "lpop".to_string(),
            args: vec![list_key.clone()],
        };
        let result = handler.process(lpop_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"world".to_vec())));
        
        // Test LPOP on empty list
        let lpop_command = Command {
            name: "lpop".to_string(),
            args: vec![list_key.clone()],
        };
        let result = handler.process(lpop_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_set_commands() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test SADD command
        let sadd_command = Command {
            name: "sadd".to_string(),
            args: vec![b"test_set_key".to_vec(), b"member1".to_vec(), b"member2".to_vec()],
        };
        
        let result = handler.process(sadd_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(2)); // 2 members added
        
        // Test SADD with duplicate member
        let sadd_command = Command {
            name: "sadd".to_string(),
            args: vec![b"test_set_key".to_vec(), b"member2".to_vec(), b"member3".to_vec()],
        };
        
        let result = handler.process(sadd_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Only 1 new member added
        
        // Test SISMEMBER with existing member
        let sismember_command = Command {
            name: "sismember".to_string(),
            args: vec![b"test_set_key".to_vec(), b"member1".to_vec()],
        };
        
        let result = handler.process(sismember_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Member exists
        
        // Test SISMEMBER with non-existing member
        let sismember_command = Command {
            name: "sismember".to_string(),
            args: vec![b"test_set_key".to_vec(), b"nonexistent".to_vec()],
        };
        
        let result = handler.process(sismember_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Member doesn't exist
        
        // Test SMEMBERS to get all members
        let smembers_command = Command {
            name: "smembers".to_string(),
            args: vec![b"test_set_key".to_vec()],
        };
        
        let result = handler.process(smembers_command).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 3); // Should have 3 members
            
            // Extract the members
            let mut member_values = Vec::new();
            for member in members {
                if let RespValue::BulkString(Some(value)) = member {
                    member_values.push(value);
                }
            }
            
            // Check that all expected members are present
            assert!(member_values.contains(&b"member1".to_vec()));
            assert!(member_values.contains(&b"member2".to_vec()));
            assert!(member_values.contains(&b"member3".to_vec()));
        } else {
            panic!("Expected array response from SMEMBERS");
        }
        
        // Test SREM to remove members
        let srem_command = Command {
            name: "srem".to_string(),
            args: vec![b"test_set_key".to_vec(), b"member1".to_vec(), b"nonexistent".to_vec()],
        };
        
        let result = handler.process(srem_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 member removed
        
        // Verify members after removal
        let smembers_command = Command {
            name: "smembers".to_string(),
            args: vec![b"test_set_key".to_vec()],
        };
        
        let result = handler.process(smembers_command).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 2); // Should have 2 members now
            
            // Extract the members
            let mut member_values = Vec::new();
            for member in members {
                if let RespValue::BulkString(Some(value)) = member {
                    member_values.push(value);
                }
            }
            
            // Check that the expected members are present
            assert!(!member_values.contains(&b"member1".to_vec())); // This was removed
            assert!(member_values.contains(&b"member2".to_vec()));
            assert!(member_values.contains(&b"member3".to_vec()));
        } else {
            panic!("Expected array response from SMEMBERS");
        }
        
        // Test SREM to remove all remaining members
        let srem_command = Command {
            name: "srem".to_string(),
            args: vec![b"test_set_key".to_vec(), b"member2".to_vec(), b"member3".to_vec()],
        };
        
        let result = handler.process(srem_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(2)); // 2 members removed
        
        // Verify the set is empty
        let smembers_command = Command {
            name: "smembers".to_string(),
            args: vec![b"test_set_key".to_vec()],
        };
        
        let result = handler.process(smembers_command).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 0); // Set should be empty
        } else {
            panic!("Expected array response from SMEMBERS");
        }
    }

    #[tokio::test]
    async fn test_sorted_set_commands() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test ZADD - Add members to a sorted set
        let zadd_command = Command {
            name: "zadd".to_string(),
            args: vec![
                b"myzset".to_vec(),
                b"1.0".to_vec(), b"one".to_vec(),
                b"2.0".to_vec(), b"two".to_vec(),
                b"3.0".to_vec(), b"three".to_vec(),
            ],
        };
        
        let result = handler.process(zadd_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(3)); // 3 members added
        
        // Test ZSCORE - Get score of a member
        let zscore_command = Command {
            name: "zscore".to_string(),
            args: vec![
                b"myzset".to_vec(),
                b"two".to_vec(),
            ],
        };
        
        let result = handler.process(zscore_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2".to_vec())));
        
        // Test ZRANGE - Get range of members by index
        let zrange_command = Command {
            name: "zrange".to_string(),
            args: vec![
                b"myzset".to_vec(),
                b"0".to_vec(),
                b"-1".to_vec(),
            ],
        };
        
        let result = handler.process(zrange_command).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 3);
            assert_eq!(members[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(members[1], RespValue::BulkString(Some(b"two".to_vec())));
            assert_eq!(members[2], RespValue::BulkString(Some(b"three".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
        
        // Test ZRANGE with WITHSCORES
        let zrange_with_scores_command = Command {
            name: "zrange".to_string(),
            args: vec![
                b"myzset".to_vec(),
                b"0".to_vec(),
                b"-1".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        };
        
        let result = handler.process(zrange_with_scores_command).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 6); // 3 members + 3 scores
            assert_eq!(members[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(members[1], RespValue::BulkString(Some(b"1".to_vec())));
            assert_eq!(members[2], RespValue::BulkString(Some(b"two".to_vec())));
            assert_eq!(members[3], RespValue::BulkString(Some(b"2".to_vec())));
            assert_eq!(members[4], RespValue::BulkString(Some(b"three".to_vec())));
            assert_eq!(members[5], RespValue::BulkString(Some(b"3".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
        
        // Test ZREM - Remove members
        let zrem_command = Command {
            name: "zrem".to_string(),
            args: vec![
                b"myzset".to_vec(),
                b"two".to_vec(),
                b"nonexistent".to_vec(),
            ],
        };
        
        let result = handler.process(zrem_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 member removed
        
        // Verify after removal
        let zrange_after_command = Command {
            name: "zrange".to_string(),
            args: vec![
                b"myzset".to_vec(),
                b"0".to_vec(),
                b"-1".to_vec(),
            ],
        };
        
        let result = handler.process(zrange_after_command).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 2);
            assert_eq!(members[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(members[1], RespValue::BulkString(Some(b"three".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
    }

    #[tokio::test]
    async fn test_new_commands() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let storage = Arc::new(storage);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Use specific key names to help with type detection
        let setnx_key = b"string_setnx_test_key".to_vec();
        let setex_key = b"string_setex_test_key".to_vec();
        let hash_key = b"hash_test_key".to_vec();
        let list_key = b"list_test_key".to_vec();
        let zset_key = b"zset_test_key".to_vec();
        
        // Clean up any keys that might exist from previous tests
        let _ = handler.storage.del(&setnx_key).await;
        let _ = handler.storage.del(&setex_key).await;
        let _ = handler.storage.del(&hash_key).await;
        let _ = handler.storage.del(&list_key).await;
        let _ = handler.storage.del(&zset_key).await;
        
        // Test SETNX command - key does not exist
        let setnx_command = Command {
            name: "setnx".to_string(),
            args: vec![setnx_key.clone(), b"setnx_value".to_vec()],
        };
        
        let result = handler.process(setnx_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Success, key was set
        
        // Test SETNX command - key exists
        let setnx_command = Command {
            name: "setnx".to_string(),
            args: vec![setnx_key.clone(), b"new_value".to_vec()],
        };
        
        let result = handler.process(setnx_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Failed, key exists
        
        // Verify the value wasn't changed
        let get_command = Command {
            name: "get".to_string(),
            args: vec![setnx_key.clone()],
        };
        
        let result = handler.process(get_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"setnx_value".to_vec())));
        
        // Test SETEX command
        let setex_command = Command {
            name: "setex".to_string(),
            args: vec![setex_key.clone(), b"2".to_vec(), b"setex_value".to_vec()],
        };
        
        let result = handler.process(setex_command).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Check TTL directly via storage engine
        let ttl = handler.storage.ttl(&setex_key).await.unwrap();
        assert!(ttl.is_some(), "TTL should be set on key");
        let ttl_secs = ttl.unwrap().as_secs();
        assert!(ttl_secs > 0 && ttl_secs <= 2, "TTL should be between 0 and 2 seconds");
        
        // Verify SETEX value was set
        let get_command = Command {
            name: "get".to_string(),
            args: vec![setex_key.clone()],
        };
        
        let result = handler.process(get_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"setex_value".to_vec())));
        
        // Test HMSET command
        let hmset_command = Command {
            name: "hmset".to_string(),
            args: vec![
                hash_key.clone(),
                b"field1".to_vec(), b"value1".to_vec(),
                b"field2".to_vec(), b"value2".to_vec(),
            ],
        };
        
        let result = handler.process(hmset_command).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Test HGET to verify
        let hget_command = Command {
            name: "hget".to_string(),
            args: vec![hash_key.clone(), b"field1".to_vec()],
        };
        
        let result = handler.process(hget_command).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"value1".to_vec())));
        
        // Test LTRIM command
        // First set up a list
        let lpush_command = Command {
            name: "lpush".to_string(),
            args: vec![
                list_key.clone(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
            ],
        };
        
        let _ = handler.process(lpush_command).await.unwrap();
        
        // Now trim the list
        let ltrim_command = Command {
            name: "ltrim".to_string(),
            args: vec![list_key.clone(), b"0".to_vec(), b"1".to_vec()],
        };
        
        let result = handler.process(ltrim_command).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
        
        // Check result with LRANGE
        let lrange_command = Command {
            name: "lrange".to_string(),
            args: vec![list_key.clone(), b"0".to_vec(), b"-1".to_vec()],
        };
        
        let result = handler.process(lrange_command).await.unwrap();
        
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array response");
        }
        
        // Test ZRANGE with WITHSCORES
        // First set up a sorted set
        let zadd_command = Command {
            name: "zadd".to_string(),
            args: vec![
                zset_key.clone(),
                b"1.1".to_vec(), b"one".to_vec(),
                b"2.2".to_vec(), b"two".to_vec(),
                b"3.3".to_vec(), b"three".to_vec(),
            ],
        };
        
        let _ = handler.process(zadd_command).await.unwrap();
        
        // Now get the range with scores
        let zrange_command = Command {
            name: "zrange".to_string(),
            args: vec![
                zset_key.clone(), b"0".to_vec(), b"-1".to_vec(), b"WITHSCORES".to_vec(),
            ],
        };
        
        let result = handler.process(zrange_command).await.unwrap();
        
        if let RespValue::Array(Some(items)) = result {
            // Check that we get 6 items (3 members + 3 scores)
            assert_eq!(items.len(), 6);
            
            // Check first member-score pair
            assert_eq!(items[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"1.1".to_vec())));
            
            // Check second member-score pair
            assert_eq!(items[2], RespValue::BulkString(Some(b"two".to_vec())));
            assert_eq!(items[3], RespValue::BulkString(Some(b"2.2".to_vec())));
            
            // Check third member-score pair
            assert_eq!(items[4], RespValue::BulkString(Some(b"three".to_vec())));
            assert_eq!(items[5], RespValue::BulkString(Some(b"3.3".to_vec())));
        } else {
            panic!("Expected array response");
        }
    }

    #[tokio::test]
    async fn test_pttl_pexpire_commands() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));
        
        // Test PEXPIRE command
        let set_command = Command {
            name: "set".to_string(),
            args: vec![b"pexpire_key".to_vec(), b"pexpire_value".to_vec()],
        };
        handler.process(set_command).await.unwrap();
        
        let pexpire_command = Command {
            name: "pexpire".to_string(),
            args: vec![b"pexpire_key".to_vec(), b"2000".to_vec()], // 2000 milliseconds
        };
        
        let result = handler.process(pexpire_command).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
        
        // Check PTTL
        let pttl_command = Command {
            name: "pttl".to_string(),
            args: vec![b"pexpire_key".to_vec()],
        };
        
        let result = handler.process(pttl_command).await.unwrap();
        if let RespValue::Integer(ttl) = result {
            assert!(ttl <= 2000 && ttl > 0);
        } else {
            panic!("Expected Integer response from PTTL command");
        }
        
        // Check regular TTL as well (should be in seconds)
        let ttl_command = Command {
            name: "ttl".to_string(),
            args: vec![b"pexpire_key".to_vec()],
        };
        
        let result = handler.process(ttl_command).await.unwrap();
        if let RespValue::Integer(ttl) = result {
            assert!(ttl <= 2 && ttl >= 1);
        } else {
            panic!("Expected Integer response from TTL command");
        }
        
        // Test PTTL on non-existent key
        let pttl_nonexistent = Command {
            name: "pttl".to_string(),
            args: vec![b"nonexistent_key".to_vec()],
        };
        
        let result = handler.process(pttl_nonexistent).await.unwrap();
        assert_eq!(result, RespValue::Integer(-2)); // -2 means key does not exist
        
        // Test PTTL on key with no expiration
        let set_noexpire = Command {
            name: "set".to_string(),
            args: vec![b"noexpire_key".to_vec(), b"value".to_vec()],
        };
        handler.process(set_noexpire).await.unwrap();
        
        let pttl_noexpire = Command {
            name: "pttl".to_string(),
            args: vec![b"noexpire_key".to_vec()],
        };
        
        let result = handler.process(pttl_noexpire).await.unwrap();
        assert_eq!(result, RespValue::Integer(-1)); // -1 means key exists but has no expiration
    }
} 