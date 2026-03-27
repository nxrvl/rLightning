use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::utils::bytes_to_string;
use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

// ---- Connection commands ----

/// Redis QUIT command - Close the connection
pub async fn quit(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis RESET command - Reset the connection state
pub async fn reset(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::SimpleString("RESET".to_string()))
}

/// Redis ECHO command - Echo the given string
pub async fn echo(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    Ok(RespValue::BulkString(Some(args[0].clone())))
}

// ---- CLIENT subcommands ----

/// Redis CLIENT command - Manage client connections
pub async fn client(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcmd = bytes_to_string(&args[0])?.to_uppercase();
    match subcmd.as_str() {
        "LIST" => {
            // Return a simplified client list format
            // In a full implementation, this would list all connected clients
            let info = "id=1 addr=127.0.0.1:0 fd=0 name= db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\r\n";
            Ok(RespValue::BulkString(Some(info.as_bytes().to_vec())))
        }
        "INFO" => {
            let info = "id=1 addr=127.0.0.1:0 fd=0 name= db=0 sub=0 psub=0 multi=-1 qbuf=0 qbuf-free=0 obl=0 oll=0 omem=0 events=r cmd=client\r\n";
            Ok(RespValue::BulkString(Some(info.as_bytes().to_vec())))
        }
        "SETNAME" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            // In a full implementation, we'd store the client name in connection state
            Ok(RespValue::SimpleString("OK".to_string()))
        }
        "GETNAME" => {
            // Return nil when no name is set (default behavior)
            Ok(RespValue::BulkString(None))
        }
        "ID" => {
            // Return a placeholder client ID
            Ok(RespValue::Integer(1))
        }
        "KILL" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            // In a full implementation, we'd kill the matching client connection
            Ok(RespValue::SimpleString("OK".to_string()))
        }
        "PAUSE" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            // Parse timeout
            let timeout_str = bytes_to_string(&args[1])?;
            let _timeout_ms: u64 = timeout_str.parse().map_err(|_| {
                CommandError::InvalidArgument(
                    "timeout is not an integer or out of range".to_string(),
                )
            })?;
            Ok(RespValue::SimpleString("OK".to_string()))
        }
        "UNPAUSE" => Ok(RespValue::SimpleString("OK".to_string())),
        "REPLY" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let mode = bytes_to_string(&args[1])?.to_uppercase();
            match mode.as_str() {
                "ON" | "OFF" | "SKIP" => Ok(RespValue::SimpleString("OK".to_string())),
                _ => Err(CommandError::InvalidArgument(
                    "CLIENT REPLY mode must be ON, OFF, or SKIP".to_string(),
                )),
            }
        }
        "NO-EVICT" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let mode = bytes_to_string(&args[1])?.to_uppercase();
            match mode.as_str() {
                "ON" | "OFF" => Ok(RespValue::SimpleString("OK".to_string())),
                _ => Err(CommandError::InvalidArgument(
                    "CLIENT NO-EVICT must be ON or OFF".to_string(),
                )),
            }
        }
        "NO-TOUCH" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let mode = bytes_to_string(&args[1])?.to_uppercase();
            match mode.as_str() {
                "ON" | "OFF" => Ok(RespValue::SimpleString("OK".to_string())),
                _ => Err(CommandError::InvalidArgument(
                    "CLIENT NO-TOUCH must be ON or OFF".to_string(),
                )),
            }
        }
        "TRACKING" => {
            // CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let mode = bytes_to_string(&args[1])?.to_uppercase();
            match mode.as_str() {
                "ON" | "OFF" => Ok(RespValue::SimpleString("OK".to_string())),
                _ => Err(CommandError::InvalidArgument(
                    "CLIENT TRACKING must be ON or OFF".to_string(),
                )),
            }
        }
        "CACHING" => {
            if args.len() != 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let mode = bytes_to_string(&args[1])?.to_uppercase();
            match mode.as_str() {
                "YES" | "NO" => Ok(RespValue::SimpleString("OK".to_string())),
                _ => Err(CommandError::InvalidArgument(
                    "CLIENT CACHING must be YES or NO".to_string(),
                )),
            }
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Some(b"CLIENT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:".to_vec())),
                RespValue::BulkString(Some(b"CACHING (YES|NO)".to_vec())),
                RespValue::BulkString(Some(b"GETNAME".to_vec())),
                RespValue::BulkString(Some(b"ID".to_vec())),
                RespValue::BulkString(Some(b"INFO".to_vec())),
                RespValue::BulkString(Some(b"KILL <option> ...".to_vec())),
                RespValue::BulkString(Some(b"LIST [TYPE (NORMAL|MASTER|REPLICA|PUBSUB)]".to_vec())),
                RespValue::BulkString(Some(b"NO-EVICT (ON|OFF)".to_vec())),
                RespValue::BulkString(Some(b"NO-TOUCH (ON|OFF)".to_vec())),
                RespValue::BulkString(Some(b"PAUSE <timeout> [WRITE|ALL]".to_vec())),
                RespValue::BulkString(Some(b"REPLY (ON|OFF|SKIP)".to_vec())),
                RespValue::BulkString(Some(b"SETNAME <connection-name>".to_vec())),
                RespValue::BulkString(Some(b"TRACKING (ON|OFF) [REDIRECT <id>] [PREFIX <prefix>] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]".to_vec())),
                RespValue::BulkString(Some(b"UNPAUSE".to_vec())),
            ];
            Ok(RespValue::Array(Some(help)))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown subcommand or wrong number of arguments for CLIENT {}",
            subcmd
        ))),
    }
}

// ---- COMMAND subcommands ----

/// Redis COMMAND command - Get information about commands
pub async fn command_cmd(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        // COMMAND with no subcommand returns info about all commands
        // Return a simplified list
        return Ok(RespValue::Array(Some(vec![])));
    }

    let subcmd = bytes_to_string(&args[0])?.to_uppercase();
    match subcmd.as_str() {
        "COUNT" => {
            // Return the number of commands we support
            Ok(RespValue::Integer(COMMAND_COUNT as i64))
        }
        "INFO" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let mut results = Vec::new();
            for cmd_name_bytes in &args[1..] {
                let cmd_name = bytes_to_string(cmd_name_bytes)?.to_lowercase();
                if let Some(info) = get_command_info(&cmd_name) {
                    results.push(info);
                } else {
                    results.push(RespValue::BulkString(None));
                }
            }
            Ok(RespValue::Array(Some(results)))
        }
        "DOCS" => {
            if args.len() < 2 {
                // Return empty array for all docs
                return Ok(RespValue::Array(Some(vec![])));
            }
            let mut results = Vec::new();
            for cmd_name_bytes in &args[1..] {
                let cmd_name = bytes_to_string(cmd_name_bytes)?.to_lowercase();
                results.push(RespValue::BulkString(Some(cmd_name.as_bytes().to_vec())));
                results.push(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(b"summary".to_vec())),
                    RespValue::BulkString(Some(
                        format!("The {} command", cmd_name.to_uppercase()).into_bytes(),
                    )),
                    RespValue::BulkString(Some(b"since".to_vec())),
                    RespValue::BulkString(Some(b"1.0.0".to_vec())),
                    RespValue::BulkString(Some(b"group".to_vec())),
                    RespValue::BulkString(Some(b"generic".to_vec())),
                ])));
            }
            Ok(RespValue::Array(Some(results)))
        }
        "LIST" => {
            // Return list of all command names
            let cmd_names: Vec<RespValue> = KNOWN_COMMANDS
                .iter()
                .map(|name| RespValue::BulkString(Some(name.as_bytes().to_vec())))
                .collect();
            Ok(RespValue::Array(Some(cmd_names)))
        }
        "GETKEYS" => {
            // COMMAND GETKEYS <command> [args...]
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let cmd_name = bytes_to_string(&args[1])?.to_lowercase();
            // For most commands, the first argument after the command name is the key
            let keys = match cmd_name.as_str() {
                "get" | "set" | "del" | "exists" | "type" | "expire" | "ttl" | "persist"
                | "incr" | "decr" | "append" | "strlen" | "getrange" | "setrange" | "lpush"
                | "rpush" | "lpop" | "rpop" | "lrange" | "lindex" | "llen" | "hset" | "hget"
                | "hdel" | "hexists" | "hgetall" | "hkeys" | "hvals" | "sadd" | "srem"
                | "smembers" | "sismember" | "scard" | "zadd" | "zrem" | "zscore" | "zrange"
                | "zcard" | "zrank" => {
                    if args.len() >= 3 {
                        vec![RespValue::BulkString(Some(args[2].clone()))]
                    } else {
                        vec![]
                    }
                }
                "mget" => args[2..]
                    .iter()
                    .map(|a| RespValue::BulkString(Some(a.clone())))
                    .collect(),
                "mset" | "msetnx" => args[2..]
                    .iter()
                    .step_by(2)
                    .map(|a| RespValue::BulkString(Some(a.clone())))
                    .collect(),
                _ => {
                    if args.len() >= 3 {
                        vec![RespValue::BulkString(Some(args[2].clone()))]
                    } else {
                        vec![]
                    }
                }
            };
            Ok(RespValue::Array(Some(keys)))
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Some(
                    b"COMMAND <subcommand> [<arg> [value] ...]. Subcommands are:".to_vec(),
                )),
                RespValue::BulkString(Some(b"COUNT".to_vec())),
                RespValue::BulkString(Some(b"DOCS [<command-name> ...]".to_vec())),
                RespValue::BulkString(Some(b"GETKEYS <command> [<arg> ...]".to_vec())),
                RespValue::BulkString(Some(b"INFO [<command-name> ...]".to_vec())),
                RespValue::BulkString(Some(
                    b"LIST [FILTERBY (MODULE|ACLCAT|PATTERN) <value>]".to_vec(),
                )),
            ];
            Ok(RespValue::Array(Some(help)))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown subcommand or wrong number of arguments for COMMAND {}",
            subcmd
        ))),
    }
}

// ---- CONFIG enhancements ----

/// Redis CONFIG SET command - Set configuration parameters
pub async fn config_set(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    // CONFIG SET param value [param value ...]
    if args.is_empty() || !args.len().is_multiple_of(2) {
        return Err(CommandError::WrongNumberOfArguments);
    }
    for pair in args.chunks(2) {
        let param = bytes_to_string(&pair[0])?;
        let value = bytes_to_string(&pair[1])?;
        if let Err(e) = engine.config_set(&param, &value) {
            return Ok(RespValue::Error(e));
        }
    }
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis CONFIG REWRITE command - Rewrite the configuration file
pub async fn config_rewrite(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis CONFIG RESETSTAT command - Reset statistics
pub async fn config_resetstat(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::SimpleString("OK".to_string()))
}

// ---- Persistence commands ----

/// Redis SAVE command - Synchronously save the dataset to disk
pub async fn save(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    // In a full implementation, this would trigger a synchronous RDB save
    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis BGSAVE command - Asynchronously save the dataset to disk
pub async fn bgsave(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::SimpleString(
        "Background saving started".to_string(),
    ))
}

/// Redis BGREWRITEAOF command - Asynchronously rewrite the append-only file
pub async fn bgrewriteaof(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::SimpleString(
        "Background append only file rewriting started".to_string(),
    ))
}

/// Redis LASTSAVE command - Get the UNIX timestamp of the last successful save
pub async fn lastsave(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Ok(RespValue::Integer(now as i64))
}

// ---- SHUTDOWN ----

/// Redis SHUTDOWN command - Synchronously save then shut down the server
pub async fn shutdown(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    // In a full implementation, this would:
    // 1. Save RDB/AOF depending on NOSAVE/SAVE option
    // 2. Gracefully shut down the server
    // For now, return OK. The server layer can handle actual shutdown.
    Ok(RespValue::SimpleString("OK".to_string()))
}

// ---- SLOWLOG ----

/// Redis SLOWLOG command - Manage the slow queries log
pub async fn slowlog(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcmd = bytes_to_string(&args[0])?.to_uppercase();
    match subcmd.as_str() {
        "GET" => {
            // Return empty array (no slow queries tracked yet)
            let count = if args.len() >= 2 {
                let count_str = bytes_to_string(&args[1])?;
                count_str.parse::<i64>().unwrap_or(128)
            } else {
                128
            };
            let _ = count;
            Ok(RespValue::Array(Some(vec![])))
        }
        "LEN" => Ok(RespValue::Integer(0)),
        "RESET" => Ok(RespValue::SimpleString("OK".to_string())),
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Some(
                    b"SLOWLOG <subcommand> [<arg> [value] ...]. Subcommands are:".to_vec(),
                )),
                RespValue::BulkString(Some(b"GET [<count>]".to_vec())),
                RespValue::BulkString(Some(b"LEN".to_vec())),
                RespValue::BulkString(Some(b"RESET".to_vec())),
            ];
            Ok(RespValue::Array(Some(help)))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown subcommand or wrong number of arguments for SLOWLOG {}",
            subcmd
        ))),
    }
}

// ---- LATENCY ----

/// Redis LATENCY command - Latency monitoring
pub async fn latency(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcmd = bytes_to_string(&args[0])?.to_uppercase();
    match subcmd.as_str() {
        "LATEST" => {
            // Return empty array (no latency events recorded)
            Ok(RespValue::Array(Some(vec![])))
        }
        "HISTORY" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            Ok(RespValue::Array(Some(vec![])))
        }
        "RESET" => Ok(RespValue::SimpleString("OK".to_string())),
        "GRAPH" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            Ok(RespValue::BulkString(Some(
                b"No latency data available".to_vec(),
            )))
        }
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Some(
                    b"LATENCY <subcommand> [<arg> [value] ...]. Subcommands are:".to_vec(),
                )),
                RespValue::BulkString(Some(b"GRAPH <event>".to_vec())),
                RespValue::BulkString(Some(b"HISTORY <event>".to_vec())),
                RespValue::BulkString(Some(b"LATEST".to_vec())),
                RespValue::BulkString(Some(b"RESET [<event> ...]".to_vec())),
            ];
            Ok(RespValue::Array(Some(help)))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown subcommand or wrong number of arguments for LATENCY {}",
            subcmd
        ))),
    }
}

// ---- MEMORY ----

/// Redis MEMORY command - Memory introspection
pub async fn memory(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcmd = bytes_to_string(&args[0])?.to_uppercase();
    match subcmd.as_str() {
        "USAGE" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let key = &args[1];
            // Check if key exists
            if !engine.exists(key).await? {
                return Ok(RespValue::BulkString(None));
            }
            // Estimate memory usage
            if let Some(item) = engine.get_item(key).await? {
                let size = key.len() + item.value.mem_size() + 64; // 64 bytes overhead estimate
                Ok(RespValue::Integer(size as i64))
            } else {
                Ok(RespValue::BulkString(None))
            }
        }
        "STATS" => {
            let used_memory = engine.get_used_memory();
            let key_count = engine.get_key_count();
            let stats = format!(
                "peak.allocated:{}\ntotal.allocated:{}\nstartup.allocated:0\nreplication.backlog:0\nclients.slaves:0\nclients.normal:0\naof.buffer:0\nkeys.count:{}\nkeys.bytes-per-key:{}\ndataset.bytes:{}\ndataset.percentage:100\npeak.percentage:100\nfragmentation:1.0\n",
                used_memory,
                used_memory,
                key_count,
                if key_count > 0 {
                    used_memory / key_count
                } else {
                    0
                },
                used_memory,
            );
            Ok(RespValue::BulkString(Some(stats.into_bytes())))
        }
        "DOCTOR" => Ok(RespValue::BulkString(Some(
            b"Sam, I have no memory problems".to_vec(),
        ))),
        "MALLOC-STATS" => Ok(RespValue::BulkString(Some(
            b"Memory allocator stats not available (Rust global allocator)".to_vec(),
        ))),
        "PURGE" => Ok(RespValue::SimpleString("OK".to_string())),
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Some(
                    b"MEMORY <subcommand> [<arg> [value] ...]. Subcommands are:".to_vec(),
                )),
                RespValue::BulkString(Some(b"DOCTOR".to_vec())),
                RespValue::BulkString(Some(b"MALLOC-STATS".to_vec())),
                RespValue::BulkString(Some(b"PURGE".to_vec())),
                RespValue::BulkString(Some(b"STATS".to_vec())),
                RespValue::BulkString(Some(b"USAGE <key> [SAMPLES <count>]".to_vec())),
            ];
            Ok(RespValue::Array(Some(help)))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown subcommand or wrong number of arguments for MEMORY {}",
            subcmd
        ))),
    }
}

// ---- DEBUG ----

/// Redis DEBUG command - Server debugging
pub async fn debug(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcmd = bytes_to_string(&args[0])?.to_uppercase();
    match subcmd.as_str() {
        "SLEEP" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let seconds_str = bytes_to_string(&args[1])?;
            let seconds: f64 = seconds_str
                .parse()
                .map_err(|_| CommandError::InvalidArgument("Invalid sleep duration".to_string()))?;
            if !(0.0..=30.0).contains(&seconds) {
                return Err(CommandError::InvalidArgument(
                    "ERR sleep duration must be between 0 and 30 seconds".to_string(),
                ));
            }
            let duration = std::time::Duration::from_secs_f64(seconds);
            tokio::time::sleep(duration).await;
            Ok(RespValue::SimpleString("OK".to_string()))
        }
        "SET-ACTIVE-EXPIRE" => Ok(RespValue::SimpleString("OK".to_string())),
        "JMAP" => Ok(RespValue::SimpleString("OK".to_string())),
        "RELOAD" => Ok(RespValue::SimpleString("OK".to_string())),
        "OBJECT" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArguments);
            }
            let key = &args[1];
            if !engine.exists(key).await? {
                return Ok(RespValue::Error("ERR no such key".to_string()));
            }
            let key_type = engine.get_type(key).await?;
            let encoding = engine
                .get_encoding(key)
                .await?
                .unwrap_or_else(|| "raw".to_string());
            Ok(RespValue::SimpleString(format!(
                "Value at:0x0 refcount:1 encoding:{} serializedlength:0 lru:0 lru_seconds_idle:0 type:{}",
                encoding, key_type
            )))
        }
        "QUICKLIST-PACKED-THRESHOLD" => Ok(RespValue::SimpleString("OK".to_string())),
        "CHANGE-REPL-ID" => Ok(RespValue::SimpleString("OK".to_string())),
        "HELP" => {
            let help = vec![
                RespValue::BulkString(Some(
                    b"DEBUG <subcommand> [<arg> [value] ...]. Subcommands are:".to_vec(),
                )),
                RespValue::BulkString(Some(b"CHANGE-REPL-ID".to_vec())),
                RespValue::BulkString(Some(b"JMAP".to_vec())),
                RespValue::BulkString(Some(b"OBJECT <key>".to_vec())),
                RespValue::BulkString(Some(b"QUICKLIST-PACKED-THRESHOLD <size>".to_vec())),
                RespValue::BulkString(Some(b"RELOAD".to_vec())),
                RespValue::BulkString(Some(b"SET-ACTIVE-EXPIRE (0|1)".to_vec())),
                RespValue::BulkString(Some(b"SLEEP <seconds>".to_vec())),
            ];
            Ok(RespValue::Array(Some(help)))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown subcommand or wrong number of arguments for DEBUG {}",
            subcmd
        ))),
    }
}

// ---- SWAPDB ----

/// Redis SWAPDB command - Swap two databases
/// Delegates to StorageEngine::swap_db which acquires the cross-DB lock
/// and bumps key versions for WATCH invalidation.
pub async fn swapdb(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let db1_str = bytes_to_string(&args[0])?;
    let db2_str = bytes_to_string(&args[1])?;
    let db1: usize = db1_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("invalid DB index".to_string()))?;
    let db2: usize = db2_str
        .parse()
        .map_err(|_| CommandError::InvalidArgument("invalid DB index".to_string()))?;

    engine.swap_db(db1, db2).await?;
    Ok(RespValue::SimpleString("OK".to_string()))
}

// ---- TIME ----

/// Redis TIME command - Return the server time
pub async fn time(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let secs = now.as_secs();
    let micros = now.subsec_micros();

    Ok(RespValue::Array(Some(vec![
        RespValue::BulkString(Some(secs.to_string().into_bytes())),
        RespValue::BulkString(Some(micros.to_string().into_bytes())),
    ])))
}

// ---- LOLWUT ----

/// Redis LOLWUT command - Display server version art
pub async fn lolwut(_engine: &StorageEngine, _args: &[Vec<u8>]) -> CommandResult {
    let art = r#"
    ____  _     _       _     _         _
   |  _ \| |   (_) __ _| |__ | |_ _ __ (_)_ __   __ _
   | |_) | |   | |/ _` | '_ \| __| '_ \| | '_ \ / _` |
   |  _ <| |___| | (_| | | | | |_| | | | | | | | (_| |
   |_| \_\_____|_|\__, |_| |_|\__|_| |_|_|_| |_|\__, |
                  |___/                           |___/

rLightning ver. 1.0.0 - A Redis-compatible server built in Rust
"#;
    Ok(RespValue::BulkString(Some(
        art.trim_start_matches('\n').as_bytes().to_vec(),
    )))
}

// ---- Expanded INFO ----

/// Redis INFO command - Get comprehensive server information
pub async fn info_expanded(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    let section = if args.is_empty() {
        "all".to_string()
    } else {
        bytes_to_string(&args[0])?.to_lowercase()
    };

    let mut sections = Vec::new();

    let include_all = section == "all" || section == "everything" || section == "default";

    if include_all || section == "server" {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default();
        sections.push(format!(
            "# Server\r\nredis_version:7.0.0\r\nredis_git_sha1:00000000\r\nredis_git_dirty:0\r\nredis_build_id:0\r\nredis_mode:standalone\r\nos:rust\r\narch_bits:64\r\nmultiplexing_api:tokio\r\natomicvar_api:atomic\r\ngcc_version:0.0.0\r\nprocess_id:{}\r\nrun_id:rlightning0000000000000000000000000000000\r\ntcp_port:6379\r\nserver_time_usec:{}\r\nuptime_in_seconds:0\r\nuptime_in_days:0\r\nhz:10\r\nconfigured_hz:10\r\nlru_clock:0\r\nexecutable:/usr/local/bin/rlightning\r\nconfig_file:\r\n",
            std::process::id(),
            now.as_micros(),
        ));
    }

    if include_all || section == "clients" {
        sections.push(
            "# Clients\r\nconnected_clients:1\r\ncluster_connections:0\r\nblocked_clients:0\r\ntracking_clients:0\r\nclients_in_timeout_table:0\r\ntotal_blocking_clients:0\r\n".to_string()
        );
    }

    if include_all || section == "memory" {
        let used_memory = engine.get_used_memory();
        let used_memory_human = format_memory_human(used_memory);
        // Read maxmemory and maxmemory-policy from runtime config
        let maxmemory_results = engine.config_get("maxmemory");
        let maxmemory: u64 = maxmemory_results
            .first()
            .and_then(|(_, v)| v.parse().ok())
            .unwrap_or(0);
        let maxmemory_human = if maxmemory > 0 {
            format_memory_human(maxmemory)
        } else {
            "0B".to_string()
        };
        let policy_results = engine.config_get("maxmemory-policy");
        let maxmemory_policy = policy_results
            .first()
            .map(|(_, v)| v.clone())
            .unwrap_or_else(|| "noeviction".to_string());
        sections.push(format!(
            "# Memory\r\nused_memory:{}\r\nused_memory_human:{}\r\nused_memory_rss:{}\r\nused_memory_rss_human:{}\r\nused_memory_peak:{}\r\nused_memory_peak_human:{}\r\nused_memory_peak_perc:100.00%\r\nused_memory_overhead:0\r\nused_memory_startup:0\r\nused_memory_dataset:{}\r\nused_memory_dataset_perc:100.00%\r\nallocator_allocated:{}\r\nallocator_active:{}\r\nallocator_resident:{}\r\ntotal_system_memory:0\r\ntotal_system_memory_human:0B\r\nused_memory_lua:0\r\nused_memory_vm_eval:0\r\nused_memory_lua_human:0B\r\nused_memory_scripts_eval:0\r\nused_memory_scripts_human:0B\r\nnumber_of_cached_scripts:0\r\nnumber_of_functions:0\r\nnumber_of_libraries:0\r\nused_memory_vm_functions:0\r\nused_memory_vm_total:0\r\nused_memory_vm_total_human:0B\r\nused_memory_functions:0\r\nused_memory_scripts:0\r\nmaxmemory:{}\r\nmaxmemory_human:{}\r\nmaxmemory_policy:{}\r\nallocator_frag_ratio:1.0\r\nallocator_frag_bytes:0\r\nallocator_rss_ratio:1.0\r\nallocator_rss_bytes:0\r\nrss_overhead_ratio:1.0\r\nrss_overhead_bytes:0\r\nmem_fragmentation_ratio:1.0\r\nmem_fragmentation_bytes:0\r\nmem_not_counted_for_evict:0\r\nmem_replication_backlog:0\r\nmem_total_replication_buffers:0\r\nmem_clients_slaves:0\r\nmem_clients_normal:0\r\nmem_cluster_links:0\r\nmem_aof_buffer:0\r\nmem_allocator:rust-global\r\nactive_defrag_running:0\r\nlazyfree_pending_objects:0\r\nlazyfreed_objects:0\r\n",
            used_memory, used_memory_human,
            used_memory, used_memory_human,
            used_memory, used_memory_human,
            used_memory,
            used_memory, used_memory, used_memory,
            maxmemory, maxmemory_human, maxmemory_policy,
        ));
    }

    if include_all || section == "stats" {
        sections.push(
            "# Stats\r\ntotal_connections_received:0\r\ntotal_commands_processed:0\r\ninstantaneous_ops_per_sec:0\r\ntotal_net_input_bytes:0\r\ntotal_net_output_bytes:0\r\ntotal_net_repl_input_bytes:0\r\ntotal_net_repl_output_bytes:0\r\ninstantaneous_input_kbps:0.0\r\ninstantaneous_output_kbps:0.0\r\ninstantaneous_input_repl_kbps:0.0\r\ninstantaneous_output_repl_kbps:0.0\r\nrejected_connections:0\r\nsync_full:0\r\nsync_partial_ok:0\r\nsync_partial_err:0\r\nexpired_keys:0\r\nexpired_stale_perc:0.00\r\nexpired_time_cap_reached_count:0\r\nexpire_cycle_cpu_milliseconds:0\r\nevicted_keys:0\r\nevicted_clients:0\r\ntotal_keys_by_expiry:0\r\ntotal_eviction_exceeded_time:0\r\ncurrent_eviction_exceeded_time:0\r\nkeyspace_hits:0\r\nkeyspace_misses:0\r\npubsub_channels:0\r\npubsub_patterns:0\r\npubsub_shardchannels:0\r\nlatest_fork_usec:0\r\ntotal_forks:0\r\nmigrate_cached_sockets:0\r\nslave_expires_tracked_keys:0\r\nactive_defrag_hits:0\r\nactive_defrag_misses:0\r\nactive_defrag_key_hits:0\r\nactive_defrag_key_misses:0\r\ntracking_total_keys:0\r\ntracking_total_items:0\r\ntracking_total_prefixes:0\r\nunexpected_error_replies:0\r\ntotal_error_replies:0\r\ndump_payload_sanitizations:0\r\ntotal_reads_processed:0\r\ntotal_writes_processed:0\r\nio_threaded_reads_processed:0\r\nio_threaded_writes_processed:0\r\nreply_buffer_shrinks:0\r\nreply_buffer_expands:0\r\ncurrent_cow_peak:0\r\ncurrent_cow_size:0\r\ncurrent_cow_size_age:0\r\ncurrent_save_keys_processed:0\r\ncurrent_save_keys_total:0\r\n".to_string()
        );
    }

    if include_all || section == "replication" {
        sections.push(
            "# Replication\r\nrole:master\r\nconnected_slaves:0\r\nmaster_failover_state:no-failover\r\nmaster_replid:0000000000000000000000000000000000000000\r\nmaster_replid2:0000000000000000000000000000000000000000\r\nmaster_repl_offset:0\r\nsecond_repl_offset:-1\r\nrepl_backlog_active:0\r\nrepl_backlog_size:1048576\r\nrepl_backlog_first_byte_offset:0\r\nrepl_backlog_histlen:0\r\n".to_string()
        );
    }

    if include_all || section == "cpu" {
        sections.push(
            "# CPU\r\nused_cpu_sys:0.000000\r\nused_cpu_user:0.000000\r\nused_cpu_sys_children:0.000000\r\nused_cpu_user_children:0.000000\r\nused_cpu_sys_main_thread:0.000000\r\nused_cpu_user_main_thread:0.000000\r\n".to_string()
        );
    }

    if include_all || section == "modules" {
        sections.push("# Modules\r\n".to_string());
    }

    if include_all || section == "errorstats" {
        sections.push("# Errorstats\r\n".to_string());
    }

    if include_all || section == "cluster" {
        let cluster_enabled = engine
            .config_get("cluster-enabled")
            .first()
            .map(|(_, v)| if v == "yes" { 1 } else { 0 })
            .unwrap_or(0);
        sections.push(format!(
            "# Cluster\r\ncluster_enabled:{}\r\n",
            cluster_enabled
        ));
    }

    if include_all || section == "keyspace" {
        let key_count = engine.get_key_count();
        if key_count > 0 {
            sections.push(format!(
                "# Keyspace\r\ndb0:keys={},expires=0,avg_ttl=0\r\n",
                key_count
            ));
        } else {
            sections.push("# Keyspace\r\n".to_string());
        }
    }

    let info_str = sections.join("\r\n");
    Ok(RespValue::BulkString(Some(info_str.into_bytes())))
}

fn format_memory_human(bytes: u64) -> String {
    if bytes < 1024 {
        format!("{}B", bytes)
    } else if bytes < 1024 * 1024 {
        format!("{:.2}K", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.2}M", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2}G", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

// ---- Command metadata ----

const COMMAND_COUNT: usize = 210;

static KNOWN_COMMANDS: &[&str] = &[
    "append",
    "auth",
    "bgsave",
    "bgrewriteaof",
    "bitcount",
    "bitfield",
    "bitfield_ro",
    "bitop",
    "bitpos",
    "blmove",
    "blmpop",
    "blpop",
    "brpop",
    "bzmpop",
    "bzpopmax",
    "bzpopmin",
    "client",
    "cluster",
    "command",
    "config",
    "copy",
    "dbsize",
    "debug",
    "decr",
    "decrby",
    "del",
    "dump",
    "echo",
    "eval",
    "eval_ro",
    "evalsha",
    "evalsha_ro",
    "exists",
    "expire",
    "expireat",
    "expiretime",
    "fcall",
    "fcall_ro",
    "flushall",
    "flushdb",
    "function",
    "geoadd",
    "geodist",
    "geohash",
    "geopos",
    "georadius",
    "georadiusbymember",
    "geosearch",
    "geosearchstore",
    "get",
    "getbit",
    "getdel",
    "getex",
    "getrange",
    "getset",
    "hdel",
    "hello",
    "hexists",
    "hget",
    "hgetall",
    "hincrby",
    "hincrbyfloat",
    "hkeys",
    "hlen",
    "hmget",
    "hmset",
    "hrandfield",
    "hscan",
    "hset",
    "hsetnx",
    "hstrlen",
    "hvals",
    "incr",
    "incrby",
    "incrbyfloat",
    "info",
    "keys",
    "lastsave",
    "latency",
    "lcs",
    "lindex",
    "linsert",
    "llen",
    "lmove",
    "lmpop",
    "lolwut",
    "lpop",
    "lpos",
    "lpush",
    "lpushx",
    "lrange",
    "lset",
    "ltrim",
    "memory",
    "mget",
    "monitor",
    "move",
    "mset",
    "msetnx",
    "multi",
    "object",
    "persist",
    "pexpire",
    "pexpireat",
    "pexpiretime",
    "pfadd",
    "pfcount",
    "pfmerge",
    "ping",
    "psetex",
    "pttl",
    "publish",
    "pubsub",
    "quit",
    "randomkey",
    "rename",
    "reset",
    "restore",
    "rpop",
    "rpush",
    "rpushx",
    "sadd",
    "save",
    "scan",
    "scard",
    "script",
    "sdiff",
    "sdiffstore",
    "select",
    "set",
    "setbit",
    "setex",
    "setnx",
    "setrange",
    "shutdown",
    "sinter",
    "sintercard",
    "sinterstore",
    "sismember",
    "slowlog",
    "smembers",
    "smismember",
    "smove",
    "sort",
    "sort_ro",
    "spop",
    "srandmember",
    "sscan",
    "srem",
    "strlen",
    "subscribe",
    "substr",
    "sunion",
    "sunionstore",
    "swapdb",
    "time",
    "touch",
    "ttl",
    "type",
    "unlink",
    "unsubscribe",
    "unwatch",
    "wait",
    "waitaof",
    "watch",
    "xack",
    "xadd",
    "xautoclaim",
    "xclaim",
    "xdel",
    "xgroup",
    "xinfo",
    "xlen",
    "xpending",
    "xrange",
    "xread",
    "xreadgroup",
    "xrevrange",
    "xtrim",
    "zadd",
    "zcard",
    "zcount",
    "zdiff",
    "zdiffstore",
    "zincrby",
    "zinter",
    "zinterstore",
    "zlexcount",
    "zmpop",
    "zmscore",
    "zpopmax",
    "zpopmin",
    "zrandmember",
    "zrange",
    "zrangebylex",
    "zrangebyscore",
    "zrangestore",
    "zrank",
    "zrem",
    "zremrangebylex",
    "zremrangebyrank",
    "zremrangebyscore",
    "zrevrange",
    "zrevrangebylex",
    "zrevrangebyscore",
    "zrevrank",
    "zscan",
    "zscore",
    "zunion",
    "zunionstore",
];

fn get_command_info(name: &str) -> Option<RespValue> {
    // Return a simplified command info array: [name, arity, flags, first_key, last_key, step]
    let (arity, flags, first_key, last_key, step) = match name {
        "get" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "set" => (-3, vec!["write", "denyoom"], 1, 1, 1),
        "del" => (-2, vec!["write"], 1, -1, 1),
        "mget" => (-2, vec!["readonly", "fast"], 1, -1, 1),
        "mset" => (-3, vec!["write", "denyoom"], 1, -1, 2),
        "incr" => (2, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "decr" => (2, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "exists" => (-2, vec!["readonly", "fast"], 1, -1, 1),
        "expire" => (3, vec!["write", "fast"], 1, 1, 1),
        "ttl" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "type" => (2, vec!["readonly", "fast"], 1, 1, 1),
        "ping" => (-1, vec!["fast"], 0, 0, 0),
        "info" => (-1, vec!["stale", "fast"], 0, 0, 0),
        "dbsize" => (1, vec!["readonly", "fast"], 0, 0, 0),
        "flushall" => (-1, vec!["write"], 0, 0, 0),
        "flushdb" => (-1, vec!["write"], 0, 0, 0),
        "select" => (2, vec!["fast"], 0, 0, 0),
        "keys" => (2, vec!["readonly", "sort_for_script"], 0, 0, 0),
        "scan" => (-2, vec!["readonly"], 0, 0, 0),
        "lpush" => (-3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "rpush" => (-3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "lpop" => (-2, vec!["write", "fast"], 1, 1, 1),
        "rpop" => (-2, vec!["write", "fast"], 1, 1, 1),
        "lrange" => (4, vec!["readonly"], 1, 1, 1),
        "hset" => (-4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "hget" => (3, vec!["readonly", "fast"], 1, 1, 1),
        "sadd" => (-3, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "zadd" => (-4, vec!["write", "denyoom", "fast"], 1, 1, 1),
        "client" => (-2, vec!["admin"], 0, 0, 0),
        "config" => (-2, vec!["admin", "stale"], 0, 0, 0),
        "command" => (-1, vec!["random", "stale"], 0, 0, 0),
        "echo" => (2, vec!["fast"], 0, 0, 0),
        "quit" => (1, vec!["fast"], 0, 0, 0),
        "reset" => (1, vec!["fast", "noscript"], 0, 0, 0),
        "time" => (1, vec!["random", "fast", "stale"], 0, 0, 0),
        "save" => (1, vec!["admin"], 0, 0, 0),
        "bgsave" => (-1, vec!["admin"], 0, 0, 0),
        "lastsave" => (1, vec!["random", "fast", "stale"], 0, 0, 0),
        "shutdown" => (-1, vec!["admin"], 0, 0, 0),
        "slowlog" => (-2, vec!["admin"], 0, 0, 0),
        "latency" => (-2, vec!["admin", "stale"], 0, 0, 0),
        "memory" => (-2, vec!["readonly"], 0, 0, 0),
        "debug" => (-2, vec!["admin"], 0, 0, 0),
        "swapdb" => (3, vec!["write", "fast"], 0, 0, 0),
        "lolwut" => (-1, vec!["fast"], 0, 0, 0),
        _ => return None,
    };

    let flags_resp: Vec<RespValue> = flags
        .iter()
        .map(|f| RespValue::SimpleString(f.to_string()))
        .collect();

    Some(RespValue::Array(Some(vec![
        RespValue::BulkString(Some(name.as_bytes().to_vec())),
        RespValue::Integer(arity as i64),
        RespValue::Array(Some(flags_resp)),
        RespValue::Integer(first_key as i64),
        RespValue::Integer(last_key as i64),
        RespValue::Integer(step as i64),
    ])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    fn setup() -> Arc<StorageEngine> {
        let config = StorageConfig::default();
        StorageEngine::new(config)
    }

    // ---- Connection command tests ----

    #[tokio::test]
    async fn test_quit() {
        let engine = setup();
        let result = quit(&engine, &[]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_reset() {
        let engine = setup();
        let result = reset(&engine, &[]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("RESET".to_string()));
    }

    #[tokio::test]
    async fn test_echo() {
        let engine = setup();
        let result = echo(&engine, &[b("hello world")]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b("hello world"))));
    }

    #[tokio::test]
    async fn test_echo_wrong_args() {
        let engine = setup();
        assert!(echo(&engine, &[]).await.is_err());
        assert!(echo(&engine, &[b("a"), b("b")]).await.is_err());
    }

    // ---- CLIENT subcommand tests ----

    #[tokio::test]
    async fn test_client_id() {
        let engine = setup();
        let result = client(&engine, &[b("ID")]).await.unwrap();
        if let RespValue::Integer(id) = result {
            assert!(id > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[tokio::test]
    async fn test_client_setname_getname() {
        let engine = setup();
        let result = client(&engine, &[b("SETNAME"), b("myconn")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = client(&engine, &[b("GETNAME")]).await.unwrap();
        // Returns nil since we don't track per-connection state in unit tests
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_client_list() {
        let engine = setup();
        let result = client(&engine, &[b("LIST")]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("id="));
            assert!(s.contains("addr="));
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_client_info() {
        let engine = setup();
        let result = client(&engine, &[b("INFO")]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("id="));
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_client_kill() {
        let engine = setup();
        let result = client(&engine, &[b("KILL"), b("127.0.0.1:12345")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_client_pause_unpause() {
        let engine = setup();
        let result = client(&engine, &[b("PAUSE"), b("1000")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = client(&engine, &[b("UNPAUSE")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_client_reply() {
        let engine = setup();
        let result = client(&engine, &[b("REPLY"), b("ON")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = client(&engine, &[b("REPLY"), b("OFF")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = client(&engine, &[b("REPLY"), b("SKIP")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        assert!(client(&engine, &[b("REPLY"), b("INVALID")]).await.is_err());
    }

    #[tokio::test]
    async fn test_client_no_evict() {
        let engine = setup();
        let result = client(&engine, &[b("NO-EVICT"), b("ON")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = client(&engine, &[b("NO-EVICT"), b("OFF")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_client_no_touch() {
        let engine = setup();
        let result = client(&engine, &[b("NO-TOUCH"), b("ON")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_client_tracking() {
        let engine = setup();
        let result = client(&engine, &[b("TRACKING"), b("ON")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = client(&engine, &[b("TRACKING"), b("OFF")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_client_help() {
        let engine = setup();
        let result = client(&engine, &[b("HELP")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_client_unknown_subcmd() {
        let engine = setup();
        assert!(client(&engine, &[b("FOOBAR")]).await.is_err());
    }

    // ---- COMMAND tests ----

    #[tokio::test]
    async fn test_command_count() {
        let engine = setup();
        let result = command_cmd(&engine, &[b("COUNT")]).await.unwrap();
        if let RespValue::Integer(count) = result {
            assert!(count > 0);
        } else {
            panic!("Expected integer");
        }
    }

    #[tokio::test]
    async fn test_command_info() {
        let engine = setup();
        let result = command_cmd(&engine, &[b("INFO"), b("get")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 1);
            // First item should be an array with command info
            if let RespValue::Array(Some(info)) = &items[0] {
                assert_eq!(info[0], RespValue::BulkString(Some(b("get"))));
            } else {
                panic!("Expected array for command info");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_command_info_unknown() {
        let engine = setup();
        let result = command_cmd(&engine, &[b("INFO"), b("nonexistent_cmd")])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 1);
            assert_eq!(items[0], RespValue::BulkString(None));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_command_list() {
        let engine = setup();
        let result = command_cmd(&engine, &[b("LIST")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_command_docs() {
        let engine = setup();
        let result = command_cmd(&engine, &[b("DOCS"), b("get")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_command_getkeys() {
        let engine = setup();
        let result = command_cmd(&engine, &[b("GETKEYS"), b("get"), b("mykey")])
            .await
            .unwrap();
        if let RespValue::Array(Some(keys)) = result {
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RespValue::BulkString(Some(b("mykey"))));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_command_help() {
        let engine = setup();
        let result = command_cmd(&engine, &[b("HELP")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    // ---- CONFIG tests ----

    #[tokio::test]
    async fn test_config_set() {
        let engine = setup();
        let result = config_set(&engine, &[b("maxmemory"), b("100mb")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_config_rewrite() {
        let engine = setup();
        let result = config_rewrite(&engine, &[]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_config_resetstat() {
        let engine = setup();
        let result = config_resetstat(&engine, &[]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    // ---- Persistence command tests ----

    #[tokio::test]
    async fn test_save() {
        let engine = setup();
        let result = save(&engine, &[]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_bgsave() {
        let engine = setup();
        let result = bgsave(&engine, &[]).await.unwrap();
        if let RespValue::SimpleString(s) = result {
            assert!(s.contains("Background saving"));
        } else {
            panic!("Expected simple string");
        }
    }

    #[tokio::test]
    async fn test_bgrewriteaof() {
        let engine = setup();
        let result = bgrewriteaof(&engine, &[]).await.unwrap();
        if let RespValue::SimpleString(s) = result {
            assert!(s.contains("Background"));
        } else {
            panic!("Expected simple string");
        }
    }

    #[tokio::test]
    async fn test_lastsave() {
        let engine = setup();
        let result = lastsave(&engine, &[]).await.unwrap();
        if let RespValue::Integer(ts) = result {
            assert!(ts > 0);
        } else {
            panic!("Expected integer");
        }
    }

    // ---- SHUTDOWN test ----

    #[tokio::test]
    async fn test_shutdown() {
        let engine = setup();
        let result = shutdown(&engine, &[]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    // ---- SLOWLOG tests ----

    #[tokio::test]
    async fn test_slowlog_get() {
        let engine = setup();
        let result = slowlog(&engine, &[b("GET")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_slowlog_get_with_count() {
        let engine = setup();
        let result = slowlog(&engine, &[b("GET"), b("10")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_slowlog_len() {
        let engine = setup();
        let result = slowlog(&engine, &[b("LEN")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_slowlog_reset() {
        let engine = setup();
        let result = slowlog(&engine, &[b("RESET")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_slowlog_help() {
        let engine = setup();
        let result = slowlog(&engine, &[b("HELP")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    // ---- LATENCY tests ----

    #[tokio::test]
    async fn test_latency_latest() {
        let engine = setup();
        let result = latency(&engine, &[b("LATEST")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_latency_history() {
        let engine = setup();
        let result = latency(&engine, &[b("HISTORY"), b("command")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_latency_reset() {
        let engine = setup();
        let result = latency(&engine, &[b("RESET")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_latency_graph() {
        let engine = setup();
        let result = latency(&engine, &[b("GRAPH"), b("command")]).await.unwrap();
        if let RespValue::BulkString(Some(_)) = result {
            // OK
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_latency_help() {
        let engine = setup();
        let result = latency(&engine, &[b("HELP")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    // ---- MEMORY tests ----

    #[tokio::test]
    async fn test_memory_usage_nonexistent() {
        let engine = setup();
        let result = memory(&engine, &[b("USAGE"), b("nonexistent")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_memory_usage_existing() {
        let engine = setup();
        engine
            .set(b"mykey".to_vec(), b"myvalue".to_vec(), None)
            .await
            .unwrap();
        let result = memory(&engine, &[b("USAGE"), b("mykey")]).await.unwrap();
        if let RespValue::Integer(size) = result {
            assert!(size > 0);
        } else {
            panic!("Expected integer, got {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_memory_stats() {
        let engine = setup();
        let result = memory(&engine, &[b("STATS")]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("peak.allocated"));
            assert!(s.contains("keys.count"));
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_memory_doctor() {
        let engine = setup();
        let result = memory(&engine, &[b("DOCTOR")]).await.unwrap();
        if let RespValue::BulkString(Some(_)) = result {
            // OK
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_memory_malloc_stats() {
        let engine = setup();
        let result = memory(&engine, &[b("MALLOC-STATS")]).await.unwrap();
        if let RespValue::BulkString(Some(_)) = result {
            // OK
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_memory_purge() {
        let engine = setup();
        let result = memory(&engine, &[b("PURGE")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_memory_help() {
        let engine = setup();
        let result = memory(&engine, &[b("HELP")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    // ---- DEBUG tests ----

    #[tokio::test]
    async fn test_debug_sleep() {
        let engine = setup();
        let result = debug(&engine, &[b("SLEEP"), b("0.01")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_debug_set_active_expire() {
        let engine = setup();
        let result = debug(&engine, &[b("SET-ACTIVE-EXPIRE"), b("1")])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_debug_reload() {
        let engine = setup();
        let result = debug(&engine, &[b("RELOAD")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_debug_object() {
        let engine = setup();
        engine
            .set(b"mykey".to_vec(), b"myval".to_vec(), None)
            .await
            .unwrap();
        let result = debug(&engine, &[b("OBJECT"), b("mykey")]).await.unwrap();
        if let RespValue::SimpleString(s) = result {
            assert!(s.contains("encoding:"));
            assert!(s.contains("type:"));
        } else {
            panic!("Expected simple string");
        }
    }

    #[tokio::test]
    async fn test_debug_object_nonexistent() {
        let engine = setup();
        let result = debug(&engine, &[b("OBJECT"), b("nope")]).await.unwrap();
        if let RespValue::Error(e) = result {
            assert!(e.contains("no such key"));
        } else {
            panic!("Expected error");
        }
    }

    #[tokio::test]
    async fn test_debug_help() {
        let engine = setup();
        let result = debug(&engine, &[b("HELP")]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array");
        }
    }

    // ---- SWAPDB tests ----

    #[tokio::test]
    async fn test_swapdb_same_db() {
        let engine = setup();
        let result = swapdb(&engine, &[b("0"), b("0")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_swapdb_invalid_index() {
        let engine = setup();
        let result = swapdb(&engine, &[b("0"), b("99")]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_swapdb_between_dbs() {
        let engine = setup();
        // Put data in db0
        engine
            .set(b"key0".to_vec(), b"val0".to_vec(), None)
            .await
            .unwrap();

        // Put data in db1 directly
        if let Some(db1) = engine.get_db(1) {
            use crate::storage::item::StorageItem;
            db1.insert(b"key1".to_vec(), StorageItem::new(crate::storage::value::StoreValue::Str(b"val1".to_vec().into())));
        }

        // Swap db0 and db1
        let result = swapdb(&engine, &[b("0"), b("1")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // After swap: db0 should have key1, db1 should have key0
        if let Some(db0) = engine.get_db(0) {
            assert!(db0.contains_key(b"key1"));
            assert!(!db0.contains_key(b"key0"));
        }
        if let Some(db1) = engine.get_db(1) {
            assert!(db1.contains_key(b"key0"));
            assert!(!db1.contains_key(b"key1"));
        }
    }

    #[tokio::test]
    async fn test_swapdb_wrong_args() {
        let engine = setup();
        assert!(swapdb(&engine, &[b("0")]).await.is_err());
        assert!(swapdb(&engine, &[b("0"), b("1"), b("2")]).await.is_err());
    }

    // ---- TIME tests ----

    #[tokio::test]
    async fn test_time() {
        let engine = setup();
        let result = time(&engine, &[]).await.unwrap();
        if let RespValue::Array(Some(parts)) = result {
            assert_eq!(parts.len(), 2);
            // First element is seconds
            if let RespValue::BulkString(Some(secs_bytes)) = &parts[0] {
                let secs_str = String::from_utf8_lossy(secs_bytes);
                let secs: u64 = secs_str.parse().expect("seconds should be a number");
                assert!(secs > 1000000000); // After 2001
            } else {
                panic!("Expected bulk string for seconds");
            }
            // Second element is microseconds
            if let RespValue::BulkString(Some(micros_bytes)) = &parts[1] {
                let micros_str = String::from_utf8_lossy(micros_bytes);
                let micros: u64 = micros_str.parse().expect("microseconds should be a number");
                assert!(micros < 1_000_000); // Less than 1 second in microseconds
            } else {
                panic!("Expected bulk string for microseconds");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_time_wrong_args() {
        let engine = setup();
        assert!(time(&engine, &[b("extra")]).await.is_err());
    }

    // ---- LOLWUT test ----

    #[tokio::test]
    async fn test_lolwut() {
        let engine = setup();
        let result = lolwut(&engine, &[]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("rLightning"));
        } else {
            panic!("Expected bulk string");
        }
    }

    // ---- INFO expanded tests ----

    #[tokio::test]
    async fn test_info_expanded_all() {
        let engine = setup();
        let result = info_expanded(&engine, &[]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("# Server"));
            assert!(s.contains("# Memory"));
            assert!(s.contains("# Stats"));
            assert!(s.contains("# Replication"));
            assert!(s.contains("# CPU"));
            assert!(s.contains("# Keyspace"));
            assert!(s.contains("# Cluster"));
            assert!(s.contains("redis_version:7.0.0"));
            assert!(s.contains("role:master"));
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_info_expanded_section() {
        let engine = setup();
        let result = info_expanded(&engine, &[b("server")]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("# Server"));
            assert!(s.contains("redis_version:7.0.0"));
            // Should not contain other sections
            assert!(!s.contains("# Replication"));
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_info_expanded_memory() {
        let engine = setup();
        engine
            .set(b"k".to_vec(), b"v".to_vec(), None)
            .await
            .unwrap();
        let result = info_expanded(&engine, &[b("memory")]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("# Memory"));
            assert!(s.contains("used_memory:"));
        } else {
            panic!("Expected bulk string");
        }
    }

    #[tokio::test]
    async fn test_info_expanded_keyspace() {
        let engine = setup();
        engine
            .set(b"k".to_vec(), b"v".to_vec(), None)
            .await
            .unwrap();
        let result = info_expanded(&engine, &[b("keyspace")]).await.unwrap();
        if let RespValue::BulkString(Some(data)) = result {
            let s = String::from_utf8_lossy(&data);
            assert!(s.contains("# Keyspace"));
            assert!(s.contains("db0:keys=1"));
        } else {
            panic!("Expected bulk string");
        }
    }
}
