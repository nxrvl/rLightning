use std::sync::Arc;

use crate::command::commands;
use crate::command::types::blocking::BlockingManager;
use crate::command::{Command, CommandError, CommandResult};
use crate::networking::raw_command::cmd_eq;
use crate::networking::resp::RespValue;
use crate::scripting::ScriptingEngine;
use crate::storage::engine::{CURRENT_DB_INDEX, StorageEngine};

/// The CommandHandler routes incoming commands to their implementations
#[derive(Clone)]
pub struct CommandHandler {
    storage: Arc<StorageEngine>,
    blocking_mgr: Arc<BlockingManager>,
    scripting: Arc<ScriptingEngine>,
}

impl CommandHandler {
    /// Create a new CommandHandler with the given storage engine
    pub fn new(storage: Arc<StorageEngine>) -> Self {
        let blocking_mgr = Arc::new(BlockingManager::new());
        Self::start_blocking_cleanup_task(Arc::clone(&blocking_mgr));
        CommandHandler {
            storage,
            blocking_mgr,
            scripting: Arc::new(ScriptingEngine::new()),
        }
    }

    /// Create a CommandHandler for replay/recovery contexts (AOF load, etc.)
    /// Does NOT spawn a background cleanup task, avoiding leaked tasks when
    /// the handler is short-lived.
    pub fn new_for_replay(storage: Arc<StorageEngine>) -> Self {
        CommandHandler {
            storage,
            blocking_mgr: Arc::new(BlockingManager::new()),
            scripting: Arc::new(ScriptingEngine::new()),
        }
    }

    /// Start periodic cleanup of stale blocking manager entries
    fn start_blocking_cleanup_task(blocking_mgr: Arc<BlockingManager>) {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                interval.tick().await;
                blocking_mgr.cleanup_stale_entries();
            }
        });
    }

    /// Get a reference to the storage engine
    pub fn storage(&self) -> &Arc<StorageEngine> {
        &self.storage
    }

    /// Get a reference to the blocking manager (for batch path notifications)
    pub fn blocking_mgr(&self) -> &Arc<BlockingManager> {
        &self.blocking_mgr
    }

    /// Get a reference to the scripting engine
    #[allow(dead_code)]
    pub fn scripting(&self) -> &Arc<ScriptingEngine> {
        &self.scripting
    }

    /// Process a command and return the result.
    /// Uses two-level byte dispatch for top-20 commands to avoid allocation.
    pub async fn process(&self, command: Command, db_index: usize) -> CommandResult {
        self.process_bytes(command.name.as_bytes(), &command.args, db_index)
            .await
    }

    /// Process a command using raw byte name - avoids lowercase allocation entirely
    /// for the top-20 most common commands. Used by the zero-copy RawCommand path.
    pub async fn process_bytes(
        &self,
        name: &[u8],
        args: &[Vec<u8>],
        db_index: usize,
    ) -> CommandResult {
        CURRENT_DB_INDEX
            .scope(db_index, async move { self.dispatch(name, args).await })
            .await
    }

    /// Two-level byte dispatch for fast command routing without allocation.
    ///
    /// Level 1: dispatch on first byte (26 letters, case-insensitive).
    /// Level 2: dispatch on command length + exact case-insensitive byte comparison via cmd_eq.
    ///
    /// Fast path covers the top-20 most common Redis commands.
    /// All remaining commands fall through to dispatch_slow which lowercases once and
    /// uses the full match table.
    async fn dispatch(&self, name: &[u8], args: &[Vec<u8>]) -> CommandResult {
        match name.first().map(|b| b.to_ascii_uppercase()) {
            Some(b'D') => {
                if name.len() == 3 && cmd_eq(name, b"DEL") {
                    return commands::del(&self.storage, args).await;
                }
            }
            Some(b'E') => {
                if name.len() == 6 {
                    if cmd_eq(name, b"EXISTS") {
                        return commands::exists(&self.storage, args).await;
                    }
                    if cmd_eq(name, b"EXPIRE") {
                        return commands::expire(&self.storage, args).await;
                    }
                }
            }
            Some(b'G') => {
                if name.len() == 3 && cmd_eq(name, b"GET") {
                    return commands::get(&self.storage, args).await;
                }
            }
            Some(b'H') => {
                if name.len() == 4 {
                    if cmd_eq(name, b"HSET") {
                        return commands::hset(&self.storage, args).await;
                    }
                    if cmd_eq(name, b"HGET") {
                        return commands::hget(&self.storage, args).await;
                    }
                }
            }
            Some(b'I') => {
                if name.len() == 4 {
                    if cmd_eq(name, b"INCR") {
                        return commands::incr(&self.storage, args).await;
                    }
                    if cmd_eq(name, b"INFO") {
                        return commands::info_expanded(&self.storage, args).await;
                    }
                }
            }
            Some(b'L') => {
                if name.len() == 5 && cmd_eq(name, b"LPUSH") {
                    let result = commands::lpush(&self.storage, args).await;
                    if result.is_ok() && !args.is_empty() {
                        self.blocking_mgr.notify_key(&args[0]);
                    }
                    return result;
                }
            }
            Some(b'M') => {
                if name.len() == 4 {
                    if cmd_eq(name, b"MGET") {
                        return commands::mget(&self.storage, args).await;
                    }
                    if cmd_eq(name, b"MSET") {
                        return commands::mset(&self.storage, args).await;
                    }
                }
            }
            Some(b'P') => {
                if name.len() == 4 && cmd_eq(name, b"PING") {
                    return if args.is_empty() {
                        Ok(RespValue::SimpleString("PONG".to_string()))
                    } else {
                        Ok(RespValue::BulkString(Some(args[0].clone())))
                    };
                }
                if name.len() == 7 && cmd_eq(name, b"PEXPIRE") {
                    return commands::pexpire(&self.storage, args).await;
                }
            }
            Some(b'R') => {
                if name.len() == 5 && cmd_eq(name, b"RPUSH") {
                    let result = commands::rpush(&self.storage, args).await;
                    if result.is_ok() && !args.is_empty() {
                        self.blocking_mgr.notify_key(&args[0]);
                    }
                    return result;
                }
            }
            Some(b'S') => {
                if name.len() == 3 && cmd_eq(name, b"SET") {
                    return commands::set(&self.storage, args).await;
                }
                if name.len() == 4 {
                    if cmd_eq(name, b"SADD") {
                        return commands::sadd(&self.storage, args).await;
                    }
                    if cmd_eq(name, b"SREM") {
                        return commands::srem(&self.storage, args).await;
                    }
                }
            }
            Some(b'T') => {
                if name.len() == 3 && cmd_eq(name, b"TTL") {
                    return commands::ttl(&self.storage, args).await;
                }
                if name.len() == 4 && cmd_eq(name, b"TYPE") {
                    return commands::get_type(&self.storage, args).await;
                }
            }
            Some(b'Z') => {
                if name.len() == 4 && cmd_eq(name, b"ZADD") {
                    return commands::zadd(&self.storage, args).await;
                }
            }
            _ => {}
        }

        // Slow path: lowercase once and dispatch via full match table
        self.dispatch_slow(name, args).await
    }

    /// Slow-path dispatch: lowercases the command name once and uses the full match
    /// table for all 400+ commands. Called when the fast path doesn't match.
    async fn dispatch_slow(&self, name: &[u8], args: &[Vec<u8>]) -> CommandResult {
        let cmd_lowercase = std::str::from_utf8(name)
            .map(|s| s.to_lowercase())
            .unwrap_or_else(|_| String::from_utf8_lossy(name).to_lowercase());

        match cmd_lowercase.as_str() {
            // Key expiration/TTL commands
            "expire" => commands::expire(&self.storage, args).await,
            "pexpire" => commands::pexpire(&self.storage, args).await,
            "ttl" => commands::ttl(&self.storage, args).await,
            "pttl" => commands::pttl(&self.storage, args).await,
            "persist" => commands::persist(&self.storage, args).await,
            "expireat" => commands::expireat(&self.storage, args).await,
            "pexpireat" => commands::pexpireat(&self.storage, args).await,
            "expiretime" => commands::expiretime(&self.storage, args).await,
            "pexpiretime" => commands::pexpiretime(&self.storage, args).await,

            // Basic key operations
            "del" => commands::del(&self.storage, args).await,
            "unlink" => commands::unlink(&self.storage, args).await,
            "exists" => commands::exists(&self.storage, args).await,
            "type" => commands::get_type(&self.storage, args).await,
            "copy" => commands::copy(&self.storage, args).await,
            "move" => commands::move_cmd(&self.storage, args).await,
            "touch" => commands::touch(&self.storage, args).await,
            "object" => commands::object(&self.storage, args).await,
            "dump" => commands::dump(&self.storage, args).await,
            "restore" => commands::restore(&self.storage, args).await,
            "sort" => commands::sort(&self.storage, args).await,
            "sort_ro" => commands::sort_ro(&self.storage, args).await,
            "waitaof" => commands::waitaof(&self.storage, args).await,
            "select" => commands::select(&self.storage, args).await,

            // Server commands
            "ping" => {
                if args.is_empty() {
                    Ok(RespValue::SimpleString("PONG".to_string()))
                } else {
                    Ok(RespValue::BulkString(Some(args[0].clone())))
                }
            }

            // String commands
            "set" => commands::set(&self.storage, args).await,
            "get" => commands::get(&self.storage, args).await,
            "mget" => commands::mget(&self.storage, args).await,
            "mset" => commands::mset(&self.storage, args).await,
            "msetnx" => commands::msetnx(&self.storage, args).await,
            "setnx" => commands::setnx(&self.storage, args).await,
            "setex" => commands::setex(&self.storage, args).await,
            "incr" => commands::incr(&self.storage, args).await,
            "decr" => commands::decr(&self.storage, args).await,
            "incrby" => commands::incrby(&self.storage, args).await,
            "decrby" => commands::decrby(&self.storage, args).await,
            "incrbyfloat" => commands::incrbyfloat(&self.storage, args).await,
            "append" => commands::append(&self.storage, args).await,
            "strlen" => commands::strlen(&self.storage, args).await,
            "getrange" => commands::getrange(&self.storage, args).await,
            "setrange" => commands::setrange(&self.storage, args).await,
            "getset" => commands::getset(&self.storage, args).await,
            "getex" => commands::getex(&self.storage, args).await,
            "getdel" => commands::getdel(&self.storage, args).await,
            "psetex" => commands::psetex(&self.storage, args).await,
            "lcs" => commands::lcs(&self.storage, args).await,
            "substr" => commands::substr(&self.storage, args).await,

            // Bitmap commands
            "setbit" => commands::setbit(&self.storage, args).await,
            "getbit" => commands::getbit(&self.storage, args).await,
            "bitcount" => commands::bitcount(&self.storage, args).await,
            "bitpos" => commands::bitpos(&self.storage, args).await,
            "bitop" => commands::bitop(&self.storage, args).await,
            "bitfield" => commands::bitfield(&self.storage, args).await,
            "bitfield_ro" => commands::bitfield_ro(&self.storage, args).await,

            // HyperLogLog commands
            "pfadd" => commands::pfadd(&self.storage, args).await,
            "pfcount" => commands::pfcount(&self.storage, args).await,
            "pfmerge" => commands::pfmerge(&self.storage, args).await,

            // Geo commands
            "geoadd" => commands::geoadd(&self.storage, args).await,
            "geodist" => commands::geodist(&self.storage, args).await,
            "geohash" => commands::geohash(&self.storage, args).await,
            "geopos" => commands::geopos(&self.storage, args).await,
            "geosearch" => commands::geosearch(&self.storage, args).await,
            "geosearchstore" => commands::geosearchstore(&self.storage, args).await,
            "georadius" => commands::georadius(&self.storage, args).await,
            "georadiusbymember" => commands::georadiusbymember(&self.storage, args).await,

            // List commands
            "lpush" => {
                let result = commands::lpush(&self.storage, args).await;
                if result.is_ok() && !args.is_empty() {
                    self.blocking_mgr.notify_key(&args[0]);
                }
                result
            }
            "rpush" => {
                let result = commands::rpush(&self.storage, args).await;
                if result.is_ok() && !args.is_empty() {
                    self.blocking_mgr.notify_key(&args[0]);
                }
                result
            }
            "lpushx" => {
                let result = commands::lpushx(&self.storage, args).await;
                if let Ok(RespValue::Integer(n)) = &result
                    && *n > 0
                    && !args.is_empty()
                {
                    self.blocking_mgr.notify_key(&args[0]);
                }
                result
            }
            "rpushx" => {
                let result = commands::rpushx(&self.storage, args).await;
                if let Ok(RespValue::Integer(n)) = &result
                    && *n > 0
                    && !args.is_empty()
                {
                    self.blocking_mgr.notify_key(&args[0]);
                }
                result
            }
            "linsert" => {
                let result = commands::linsert(&self.storage, args).await;
                if let Ok(RespValue::Integer(n)) = &result
                    && *n > 0
                    && !args.is_empty()
                {
                    self.blocking_mgr.notify_key(&args[0]);
                }
                result
            }
            "lpop" => commands::lpop(&self.storage, args).await,
            "rpop" => commands::rpop(&self.storage, args).await,
            "lrange" => commands::lrange(&self.storage, args).await,
            "lindex" => commands::lindex(&self.storage, args).await,
            "llen" => commands::llen(&self.storage, args).await,
            "ltrim" => commands::ltrim(&self.storage, args).await,
            "lset" => commands::lset(&self.storage, args).await,
            "lrem" => commands::lrem(&self.storage, args).await,
            "lpos" => commands::lpos(&self.storage, args).await,
            "lmove" => {
                let result = commands::lmove(&self.storage, args).await;
                if result.is_ok()
                    && args.len() >= 2
                    && let Ok(RespValue::BulkString(Some(_))) = &result
                {
                    self.blocking_mgr.notify_key(&args[1]);
                }
                result
            }
            "rpoplpush" => {
                let result = commands::rpoplpush(&self.storage, args).await;
                if result.is_ok()
                    && args.len() >= 2
                    && let Ok(RespValue::BulkString(Some(_))) = &result
                {
                    self.blocking_mgr.notify_key(&args[1]);
                }
                result
            }
            "lmpop" => commands::lmpop(&self.storage, args).await,
            "blpop" => commands::blpop(&self.storage, args, &self.blocking_mgr).await,
            "brpop" => commands::brpop(&self.storage, args, &self.blocking_mgr).await,
            "blmove" => {
                let result = commands::blmove(&self.storage, args, &self.blocking_mgr).await;
                if result.is_ok()
                    && args.len() >= 2
                    && let Ok(RespValue::BulkString(Some(_))) = &result
                {
                    self.blocking_mgr.notify_key(&args[1]);
                }
                result
            }
            "blmpop" => commands::blmpop(&self.storage, args, &self.blocking_mgr).await,

            // Hash commands
            "hset" => commands::hset(&self.storage, args).await,
            "hget" => commands::hget(&self.storage, args).await,
            "hgetall" => commands::hgetall(&self.storage, args).await,
            "hdel" => commands::hdel(&self.storage, args).await,
            "hexists" => commands::hexists(&self.storage, args).await,
            "hmset" => commands::hmset(&self.storage, args).await,
            "hkeys" => commands::hkeys(&self.storage, args).await,
            "hvals" => commands::hvals(&self.storage, args).await,
            "hlen" => commands::hlen(&self.storage, args).await,
            "hmget" => commands::hmget(&self.storage, args).await,
            "hincrby" => commands::hincrby(&self.storage, args).await,
            "hincrbyfloat" => commands::hincrbyfloat(&self.storage, args).await,
            "hsetnx" => commands::hsetnx(&self.storage, args).await,
            "hstrlen" => commands::hstrlen(&self.storage, args).await,
            "hrandfield" => commands::hrandfield(&self.storage, args).await,
            "hscan" => commands::hscan(&self.storage, args).await,

            // Set commands
            "sadd" => commands::sadd(&self.storage, args).await,
            "srem" => commands::srem(&self.storage, args).await,
            "smembers" => commands::smembers(&self.storage, args).await,
            "sismember" => commands::sismember(&self.storage, args).await,
            "scard" => commands::scard(&self.storage, args).await,
            "spop" => commands::spop(&self.storage, args).await,
            "srandmember" => commands::srandmember(&self.storage, args).await,
            "sinter" => commands::sinter(&self.storage, args).await,
            "sinterstore" => commands::sinterstore(&self.storage, args).await,
            "sunion" => commands::sunion(&self.storage, args).await,
            "sunionstore" => commands::sunionstore(&self.storage, args).await,
            "sdiff" => commands::sdiff(&self.storage, args).await,
            "sdiffstore" => commands::sdiffstore(&self.storage, args).await,
            "smove" => commands::smove(&self.storage, args).await,
            "sintercard" => commands::sintercard(&self.storage, args).await,
            "smismember" => commands::smismember(&self.storage, args).await,
            "sscan" => commands::sscan(&self.storage, args).await,

            // Sorted Set commands
            "zadd" => commands::zadd(&self.storage, args).await,
            "zrange" => commands::zrange_unified(&self.storage, args).await,
            "zrem" => commands::zrem(&self.storage, args).await,
            "zscore" => commands::zscore(&self.storage, args).await,
            "zcard" => commands::zcard(&self.storage, args).await,
            "zcount" => commands::zcount(&self.storage, args).await,
            "zrank" => commands::zrank(&self.storage, args).await,
            "zrevrange" => commands::zrevrange(&self.storage, args).await,
            "zincrby" => commands::zincrby(&self.storage, args).await,
            "zrangebyscore" => commands::zrangebyscore(&self.storage, args).await,
            "zrevrangebyscore" => commands::zrevrangebyscore(&self.storage, args).await,
            "zrangebylex" => commands::zrangebylex(&self.storage, args).await,
            "zrevrangebylex" => commands::zrevrangebylex(&self.storage, args).await,
            "zremrangebyrank" => commands::zremrangebyrank(&self.storage, args).await,
            "zremrangebyscore" => commands::zremrangebyscore(&self.storage, args).await,
            "zremrangebylex" => commands::zremrangebylex(&self.storage, args).await,
            "zlexcount" => commands::zlexcount(&self.storage, args).await,
            "zrevrank" => commands::zrevrank(&self.storage, args).await,
            "zinterstore" => commands::zinterstore(&self.storage, args).await,
            "zunionstore" => commands::zunionstore(&self.storage, args).await,
            "zinter" => commands::zinter(&self.storage, args).await,
            "zunion" => commands::zunion(&self.storage, args).await,
            "zdiff" => commands::zdiff(&self.storage, args).await,
            "zdiffstore" => commands::zdiffstore(&self.storage, args).await,
            "zpopmin" => commands::zpopmin(&self.storage, args).await,
            "zpopmax" => commands::zpopmax(&self.storage, args).await,
            "bzpopmin" => commands::bzpopmin(&self.storage, args, &self.blocking_mgr).await,
            "bzpopmax" => commands::bzpopmax(&self.storage, args, &self.blocking_mgr).await,
            "zrandmember" => commands::zrandmember(&self.storage, args).await,
            "zmscore" => commands::zmscore(&self.storage, args).await,
            "zmpop" => commands::zmpop(&self.storage, args).await,
            "bzmpop" => commands::bzmpop(&self.storage, args, &self.blocking_mgr).await,
            "zrangestore" => commands::zrangestore(&self.storage, args).await,
            "zscan" => commands::zscan(&self.storage, args).await,

            // JSON commands
            "json.get" | "jsonget" | "get_json" | "getjson" => {
                commands::json_get(&self.storage, args).await
            }
            "json.set" | "jsonset" | "set_json" | "setjson" => {
                commands::json_set(&self.storage, args).await
            }
            "json.type" | "jsontype" => commands::json_type(&self.storage, args).await,
            "json.arrappend" | "jsonarrappend" => {
                commands::json_arrappend(&self.storage, args).await
            }
            "json.arrtrim" | "jsonarrtrim" => {
                commands::json_arrtrim(&self.storage, args).await
            }
            "json.resp" | "jsonresp" => commands::json_resp(&self.storage, args).await,
            "json.del" | "jsondel" => commands::json_del(&self.storage, args).await,
            "json.objkeys" | "jsonobjkeys" => {
                commands::json_objkeys(&self.storage, args).await
            }
            "json.objlen" | "jsonobjlen" => commands::json_objlen(&self.storage, args).await,
            "json.arrlen" | "jsonarrlen" => commands::json_arrlen(&self.storage, args).await,
            "json.numincrby" | "jsonnumincrby" => {
                commands::json_numincrby(&self.storage, args).await
            }
            "json.mget" | "jsonmget" => commands::json_mget(&self.storage, args).await,
            "json.arrindex" | "jsonarrindex" => {
                commands::json_arrindex(&self.storage, args).await
            }

            // Server commands
            "info" => commands::info_expanded(&self.storage, args).await,
            "auth" => commands::auth(&self.storage, args).await,
            "config" => {
                if !args.is_empty() {
                    let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
                    match subcmd.as_str() {
                        "SET" => commands::config_set(&self.storage, &args[1..]).await,
                        "REWRITE" => commands::config_rewrite(&self.storage, &[]).await,
                        "RESETSTAT" => commands::config_resetstat(&self.storage, &[]).await,
                        _ => commands::config(&self.storage, args).await,
                    }
                } else {
                    commands::config(&self.storage, args).await
                }
            }
            "keys" => commands::keys(&self.storage, args).await,
            "rename" => commands::rename(&self.storage, args).await,
            "renamenx" => commands::renamenx(&self.storage, args).await,
            "flushall" => commands::flushall(&self.storage, args).await,
            "flushdb" => commands::flushdb(&self.storage, args).await,
            "monitor" => commands::monitor(&self.storage, args).await,
            "scan" => commands::scan_with_type(&self.storage, args).await,
            "dbsize" => commands::dbsize(&self.storage, args).await,
            "randomkey" => commands::randomkey(&self.storage, args).await,

            // Stream commands
            "xadd" => {
                let result = commands::xadd(&self.storage, args).await;
                if result.is_ok() && !args.is_empty() {
                    self.blocking_mgr.notify_key(&args[0]);
                }
                result
            }
            "xlen" => commands::xlen(&self.storage, args).await,
            "xrange" => commands::xrange(&self.storage, args).await,
            "xrevrange" => commands::xrevrange(&self.storage, args).await,
            "xread" => commands::xread(&self.storage, args, &self.blocking_mgr).await,
            "xtrim" => commands::xtrim(&self.storage, args).await,
            "xdel" => commands::xdel(&self.storage, args).await,
            "xinfo" => commands::xinfo(&self.storage, args).await,
            "xgroup" => commands::xgroup(&self.storage, args).await,
            "xreadgroup" => {
                commands::xreadgroup(&self.storage, args, &self.blocking_mgr).await
            }
            "xack" => commands::xack(&self.storage, args).await,
            "xpending" => commands::xpending(&self.storage, args).await,
            "xclaim" => commands::xclaim(&self.storage, args).await,
            "xautoclaim" => commands::xautoclaim(&self.storage, args).await,

            // Scripting commands
            "eval" => self.scripting.handle_eval(self, args, false).await,
            "eval_ro" => self.scripting.handle_eval(self, args, true).await,
            "evalsha" => self.scripting.handle_evalsha(self, args, false).await,
            "evalsha_ro" => self.scripting.handle_evalsha(self, args, true).await,
            "script" => self.scripting.handle_script(args),
            "function" => self.scripting.handle_function(args),
            "fcall" => self.scripting.handle_fcall(self, args, false).await,
            "fcall_ro" => self.scripting.handle_fcall(self, args, true).await,

            // Connection commands
            "quit" => commands::quit(&self.storage, args).await,
            "reset" => commands::reset(&self.storage, args).await,
            "echo" => commands::echo(&self.storage, args).await,
            "client" => commands::client(&self.storage, args).await,
            "command" => commands::command_cmd(&self.storage, args).await,

            // Persistence commands
            "save" => commands::save(&self.storage, args).await,
            "bgsave" => commands::bgsave(&self.storage, args).await,
            "bgrewriteaof" => commands::bgrewriteaof(&self.storage, args).await,
            "lastsave" => commands::lastsave(&self.storage, args).await,
            "shutdown" => commands::shutdown(&self.storage, args).await,

            // Monitoring commands
            "slowlog" => commands::slowlog(&self.storage, args).await,
            "latency" => commands::latency(&self.storage, args).await,
            "memory" => commands::memory(&self.storage, args).await,
            "debug" => commands::debug(&self.storage, args).await,

            // Database commands
            "swapdb" => commands::swapdb(&self.storage, args).await,
            "time" => commands::time(&self.storage, args).await,
            "lolwut" => commands::lolwut(&self.storage, args).await,

            // Cluster commands are handled at the server level
            "cluster" => Ok(RespValue::Error(
                "ERR Cluster commands must be handled at the server level".to_string(),
            )),
            "asking" => commands::asking(&self.storage, args).await,
            "readonly" => commands::readonly(&self.storage, args).await,
            "readwrite" => commands::readwrite(&self.storage, args).await,
            "migrate" => commands::migrate(&self.storage, args).await,

            // Module commands (stubs)
            "module" => commands::module_command(&self.storage, args).await,

            // ACL commands are handled in server.rs
            "acl" => Ok(RespValue::Error(
                "ERR ACL commands must be handled at the server level".to_string(),
            )),

            // Replication commands are handled at the server level
            "role" | "replicaof" | "slaveof" | "replconf" | "psync" | "failover" => {
                Ok(RespValue::Error(
                    "ERR Replication commands must be handled at the server level".to_string(),
                ))
            }

            // Sentinel commands are handled at the server level
            "sentinel" => Ok(RespValue::Error(
                "ERR Sentinel commands must be handled at the server level".to_string(),
            )),

            // WAIT is handled at the server level
            "wait" => commands::wait_cmd(&self.storage, args).await,

            // Unknown command
            _ => Err(CommandError::UnknownCommand(cmd_lowercase)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::Command;
    use crate::storage::engine::StorageConfig;
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

        let result = handler.process(command, 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("PONG".to_string()));

        // Test PING with argument
        let command = Command {
            name: "ping".to_string(),
            args: vec![b"hello".to_vec()],
        };

        let result = handler.process(command, 0).await.unwrap();
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

        let result = handler.process(set_command, 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Test GET command
        let get_command = Command {
            name: "get".to_string(),
            args: vec![b"mykey".to_vec()],
        };

        let result = handler.process(get_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));

        // Test GET for non-existent key
        let get_missing = Command {
            name: "get".to_string(),
            args: vec![b"nonexistent".to_vec()],
        };

        let result = handler.process(get_missing, 0).await.unwrap();
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

        let result = handler.process(set_ex_command, 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check TTL
        let ttl_command = Command {
            name: "ttl".to_string(),
            args: vec![b"ex_key".to_vec()],
        };

        let result = handler.process(ttl_command, 0).await.unwrap();
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

        let result = handler.process(set_px_command, 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify that the key exists
        let get_command = Command {
            name: "get".to_string(),
            args: vec![b"px_key".to_vec()],
        };

        let result = handler.process(get_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"px_value".to_vec())));

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Verify that the key is gone
        let get_command_after = Command {
            name: "get".to_string(),
            args: vec![b"px_key".to_vec()],
        };

        let result = handler.process(get_command_after, 0).await.unwrap();
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
            handler.process(set_command, 0).await.unwrap();
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

        let result = handler.process(exists_command, 0).await.unwrap();
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

        let result = handler.process(del_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // Verify keys are gone with EXISTS
        let exists_command = Command {
            name: "exists".to_string(),
            args: vec![b"delkey1".to_vec(), b"delkey2".to_vec()],
        };

        let result = handler.process(exists_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Verify delkey3 still exists
        let exists_command = Command {
            name: "exists".to_string(),
            args: vec![b"delkey3".to_vec()],
        };

        let result = handler.process(exists_command, 0).await.unwrap();
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
                    format!("mgetvalue{}", i + 1).into_bytes(),
                ],
            };
            handler.process(set_command, 0).await.unwrap();
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

        let result = handler.process(mget_command, 0).await.unwrap();

        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 4);
            assert_eq!(
                values[0],
                RespValue::BulkString(Some(b"mgetvalue1".to_vec()))
            );
            assert_eq!(
                values[1],
                RespValue::BulkString(Some(b"mgetvalue2".to_vec()))
            );
            assert_eq!(values[2], RespValue::BulkString(None));
            assert_eq!(
                values[3],
                RespValue::BulkString(Some(b"mgetvalue3".to_vec()))
            );
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

        let result = handler.process(unknown_command, 0).await;

        // The result should be an error, not an unwrapped value
        assert!(result.is_err());
        match result {
            Err(CommandError::UnknownCommand(cmd)) => {
                assert_eq!(cmd, "notacommand");
            }
            _ => panic!("Expected UnknownCommand error"),
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
            args: vec![b"user:1".to_vec(), b"name".to_vec(), b"John".to_vec()],
        };

        let result = handler.process(hset_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // New field

        // Test HSET again with the same field
        let hset_again_cmd = Command {
            name: "hset".to_string(),
            args: vec![b"user:1".to_vec(), b"name".to_vec(), b"Jane".to_vec()],
        };

        let result = handler.process(hset_again_cmd, 0).await.unwrap();
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

        handler.process(hset_email_cmd, 0).await.unwrap();

        // Test HGET command
        let hget_cmd = Command {
            name: "hget".to_string(),
            args: vec![b"user:1".to_vec(), b"name".to_vec()],
        };

        let result = handler.process(hget_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"Jane".to_vec())));

        // Test HGET for non-existent field
        let hget_missing_cmd = Command {
            name: "hget".to_string(),
            args: vec![b"user:1".to_vec(), b"phone".to_vec()],
        };

        let result = handler.process(hget_missing_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Test HEXISTS command
        let hexists_cmd = Command {
            name: "hexists".to_string(),
            args: vec![b"user:1".to_vec(), b"name".to_vec()],
        };

        let result = handler.process(hexists_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Field exists

        // Test HEXISTS for non-existent field
        let hexists_missing_cmd = Command {
            name: "hexists".to_string(),
            args: vec![b"user:1".to_vec(), b"phone".to_vec()],
        };

        let result = handler.process(hexists_missing_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Field doesn't exist

        // Test HGETALL command
        let hgetall_cmd = Command {
            name: "hgetall".to_string(),
            args: vec![b"user:1".to_vec()],
        };

        let result = handler.process(hgetall_cmd, 0).await.unwrap();
        if let RespValue::Array(Some(fields_and_values)) = result {
            assert_eq!(fields_and_values.len(), 4); // 2 fields = 4 items (field + value pairs)

            // Results should contain both fields and their values
            let mut found_name = false;
            let mut found_email = false;

            for i in (0..fields_and_values.len()).step_by(2) {
                if let RespValue::BulkString(Some(field)) = &fields_and_values[i] {
                    if field == b"name" {
                        found_name = true;
                        assert_eq!(
                            fields_and_values[i + 1],
                            RespValue::BulkString(Some(b"Jane".to_vec()))
                        );
                    } else if field == b"email" {
                        found_email = true;
                        assert_eq!(
                            fields_and_values[i + 1],
                            RespValue::BulkString(Some(b"jane@example.com".to_vec()))
                        );
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

        let result = handler.process(hdel_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 field deleted

        // Verify the field was deleted
        let hget_after_del_cmd = Command {
            name: "hget".to_string(),
            args: vec![b"user:1".to_vec(), b"name".to_vec()],
        };

        let result = handler.process(hget_after_del_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None)); // Field should be gone

        // Check HGETALL after deletion
        let hgetall_after_cmd = Command {
            name: "hgetall".to_string(),
            args: vec![b"user:1".to_vec()],
        };

        let result = handler.process(hgetall_after_cmd, 0).await.unwrap();
        if let RespValue::Array(Some(fields_and_values)) = result {
            assert_eq!(fields_and_values.len(), 2); // 1 field = 2 items (field + value)

            // Only email should remain
            if let RespValue::BulkString(Some(field)) = &fields_and_values[0] {
                assert_eq!(field, b"email");
                assert_eq!(
                    fields_and_values[1],
                    RespValue::BulkString(Some(b"jane@example.com".to_vec()))
                );
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
        let _ = handler.process(del_command, 0).await.unwrap();

        // Initialize the key as a list using set_with_type to properly track type
        storage
            .set_with_type(
                list_key.clone(),
                crate::storage::value::StoreValue::List(std::collections::VecDeque::new()),
                None,
            )
            .await
            .unwrap();

        // Verify the key type
        let key_type = storage.get_type(&list_key).await.unwrap();
        assert_eq!(key_type, "list");

        // Test LPUSH
        let lpush_command = Command {
            name: "lpush".to_string(),
            args: vec![list_key.clone(), b"world".to_vec()],
        };
        let result = handler.process(lpush_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Test LPUSH again (prepends)
        let lpush_command = Command {
            name: "lpush".to_string(),
            args: vec![list_key.clone(), b"hello".to_vec()],
        };
        let result = handler.process(lpush_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // Test RPUSH (appends)
        let rpush_command = Command {
            name: "rpush".to_string(),
            args: vec![list_key.clone(), b"!".to_vec()],
        };
        let result = handler.process(rpush_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Test LRANGE to get the entire list
        let lrange_command = Command {
            name: "lrange".to_string(),
            args: vec![list_key.clone(), b"0".to_vec(), b"-1".to_vec()],
        };
        let result = handler.process(lrange_command, 0).await.unwrap();
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
        let result = handler.process(lpop_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));

        // Test RPOP
        let rpop_command = Command {
            name: "rpop".to_string(),
            args: vec![list_key.clone()],
        };
        let result = handler.process(rpop_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"!".to_vec())));

        // Verify the list now has only one element
        let lrange_command = Command {
            name: "lrange".to_string(),
            args: vec![list_key.clone(), b"0".to_vec(), b"-1".to_vec()],
        };
        let result = handler.process(lrange_command, 0).await.unwrap();
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
        let result = handler.process(lpop_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"world".to_vec())));

        // Test LPOP on empty list
        let lpop_command = Command {
            name: "lpop".to_string(),
            args: vec![list_key.clone()],
        };
        let result = handler.process(lpop_command, 0).await.unwrap();
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
            args: vec![
                b"test_set_key".to_vec(),
                b"member1".to_vec(),
                b"member2".to_vec(),
            ],
        };

        let result = handler.process(sadd_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(2)); // 2 members added

        // Test SADD with duplicate member
        let sadd_command = Command {
            name: "sadd".to_string(),
            args: vec![
                b"test_set_key".to_vec(),
                b"member2".to_vec(),
                b"member3".to_vec(),
            ],
        };

        let result = handler.process(sadd_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Only 1 new member added

        // Test SISMEMBER with existing member
        let sismember_command = Command {
            name: "sismember".to_string(),
            args: vec![b"test_set_key".to_vec(), b"member1".to_vec()],
        };

        let result = handler.process(sismember_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Member exists

        // Test SISMEMBER with non-existing member
        let sismember_command = Command {
            name: "sismember".to_string(),
            args: vec![b"test_set_key".to_vec(), b"nonexistent".to_vec()],
        };

        let result = handler.process(sismember_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Member doesn't exist

        // Test SMEMBERS to get all members
        let smembers_command = Command {
            name: "smembers".to_string(),
            args: vec![b"test_set_key".to_vec()],
        };

        let result = handler.process(smembers_command, 0).await.unwrap();
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
            args: vec![
                b"test_set_key".to_vec(),
                b"member1".to_vec(),
                b"nonexistent".to_vec(),
            ],
        };

        let result = handler.process(srem_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 member removed

        // Verify members after removal
        let smembers_command = Command {
            name: "smembers".to_string(),
            args: vec![b"test_set_key".to_vec()],
        };

        let result = handler.process(smembers_command, 0).await.unwrap();
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
            args: vec![
                b"test_set_key".to_vec(),
                b"member2".to_vec(),
                b"member3".to_vec(),
            ],
        };

        let result = handler.process(srem_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(2)); // 2 members removed

        // Verify the set is empty
        let smembers_command = Command {
            name: "smembers".to_string(),
            args: vec![b"test_set_key".to_vec()],
        };

        let result = handler.process(smembers_command, 0).await.unwrap();
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
                b"1.0".to_vec(),
                b"one".to_vec(),
                b"2.0".to_vec(),
                b"two".to_vec(),
                b"3.0".to_vec(),
                b"three".to_vec(),
            ],
        };

        let result = handler.process(zadd_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(3)); // 3 members added

        // Test ZSCORE - Get score of a member
        let zscore_command = Command {
            name: "zscore".to_string(),
            args: vec![b"myzset".to_vec(), b"two".to_vec()],
        };

        let result = handler.process(zscore_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2".to_vec())));

        // Test ZRANGE - Get range of members by index
        let zrange_command = Command {
            name: "zrange".to_string(),
            args: vec![b"myzset".to_vec(), b"0".to_vec(), b"-1".to_vec()],
        };

        let result = handler.process(zrange_command, 0).await.unwrap();
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

        let result = handler
            .process(zrange_with_scores_command, 0)
            .await
            .unwrap();
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
            args: vec![b"myzset".to_vec(), b"two".to_vec(), b"nonexistent".to_vec()],
        };

        let result = handler.process(zrem_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // 1 member removed

        // Verify after removal
        let zrange_after_command = Command {
            name: "zrange".to_string(),
            args: vec![b"myzset".to_vec(), b"0".to_vec(), b"-1".to_vec()],
        };

        let result = handler.process(zrange_after_command, 0).await.unwrap();
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

        let result = handler.process(setnx_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Success, key was set

        // Test SETNX command - key exists
        let setnx_command = Command {
            name: "setnx".to_string(),
            args: vec![setnx_key.clone(), b"new_value".to_vec()],
        };

        let result = handler.process(setnx_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Failed, key exists

        // Verify the value wasn't changed
        let get_command = Command {
            name: "get".to_string(),
            args: vec![setnx_key.clone()],
        };

        let result = handler.process(get_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"setnx_value".to_vec())));

        // Test SETEX command
        let setex_command = Command {
            name: "setex".to_string(),
            args: vec![setex_key.clone(), b"2".to_vec(), b"setex_value".to_vec()],
        };

        let result = handler.process(setex_command, 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check TTL directly via storage engine
        let ttl = handler.storage.ttl(&setex_key).await.unwrap();
        assert!(ttl.is_some(), "TTL should be set on key");
        let ttl_secs = ttl.unwrap().as_secs();
        assert!(
            ttl_secs > 0 && ttl_secs <= 2,
            "TTL should be between 0 and 2 seconds"
        );

        // Verify SETEX value was set
        let get_command = Command {
            name: "get".to_string(),
            args: vec![setex_key.clone()],
        };

        let result = handler.process(get_command, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"setex_value".to_vec())));

        // Test HMSET command
        let hmset_command = Command {
            name: "hmset".to_string(),
            args: vec![
                hash_key.clone(),
                b"field1".to_vec(),
                b"value1".to_vec(),
                b"field2".to_vec(),
                b"value2".to_vec(),
            ],
        };

        let result = handler.process(hmset_command, 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Test HGET to verify
        let hget_command = Command {
            name: "hget".to_string(),
            args: vec![hash_key.clone(), b"field1".to_vec()],
        };

        let result = handler.process(hget_command, 0).await.unwrap();
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

        let _ = handler.process(lpush_command, 0).await.unwrap();

        // Now trim the list
        let ltrim_command = Command {
            name: "ltrim".to_string(),
            args: vec![list_key.clone(), b"0".to_vec(), b"1".to_vec()],
        };

        let result = handler.process(ltrim_command, 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check result with LRANGE
        let lrange_command = Command {
            name: "lrange".to_string(),
            args: vec![list_key.clone(), b"0".to_vec(), b"-1".to_vec()],
        };

        let result = handler.process(lrange_command, 0).await.unwrap();

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
                b"1.1".to_vec(),
                b"one".to_vec(),
                b"2.2".to_vec(),
                b"two".to_vec(),
                b"3.3".to_vec(),
                b"three".to_vec(),
            ],
        };

        let _ = handler.process(zadd_command, 0).await.unwrap();

        // Now get the range with scores
        let zrange_command = Command {
            name: "zrange".to_string(),
            args: vec![
                zset_key.clone(),
                b"0".to_vec(),
                b"-1".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        };

        let result = handler.process(zrange_command, 0).await.unwrap();

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
        handler.process(set_command, 0).await.unwrap();

        let pexpire_command = Command {
            name: "pexpire".to_string(),
            args: vec![b"pexpire_key".to_vec(), b"2000".to_vec()], // 2000 milliseconds
        };

        let result = handler.process(pexpire_command, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Check PTTL
        let pttl_command = Command {
            name: "pttl".to_string(),
            args: vec![b"pexpire_key".to_vec()],
        };

        let result = handler.process(pttl_command, 0).await.unwrap();
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

        let result = handler.process(ttl_command, 0).await.unwrap();
        if let RespValue::Integer(ttl) = result {
            assert!((1..=2).contains(&ttl));
        } else {
            panic!("Expected Integer response from TTL command");
        }

        // Test PTTL on non-existent key
        let pttl_nonexistent = Command {
            name: "pttl".to_string(),
            args: vec![b"nonexistent_key".to_vec()],
        };

        let result = handler.process(pttl_nonexistent, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(-2)); // -2 means key does not exist

        // Test PTTL on key with no expiration
        let set_noexpire = Command {
            name: "set".to_string(),
            args: vec![b"noexpire_key".to_vec(), b"value".to_vec()],
        };
        handler.process(set_noexpire, 0).await.unwrap();

        let pttl_noexpire = Command {
            name: "pttl".to_string(),
            args: vec![b"noexpire_key".to_vec()],
        };

        let result = handler.process(pttl_noexpire, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(-1)); // -1 means key exists but has no expiration
    }

    // ========== Multi-Database (SELECT) Tests ==========

    #[tokio::test]
    async fn test_select_database_isolation() {
        // SELECT 1 then SET/GET should isolate from database 0
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // SET key in DB 0
        let set_db0 = Command {
            name: "set".to_string(),
            args: vec![b"mykey".to_vec(), b"db0_value".to_vec()],
        };
        handler.process(set_db0, 0).await.unwrap();

        // SET same key in DB 1 with different value
        let set_db1 = Command {
            name: "set".to_string(),
            args: vec![b"mykey".to_vec(), b"db1_value".to_vec()],
        };
        handler.process(set_db1, 1).await.unwrap();

        // GET key from DB 0 should return db0_value
        let get_db0 = Command {
            name: "get".to_string(),
            args: vec![b"mykey".to_vec()],
        };
        let result = handler.process(get_db0, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"db0_value".to_vec())));

        // GET key from DB 1 should return db1_value
        let get_db1 = Command {
            name: "get".to_string(),
            args: vec![b"mykey".to_vec()],
        };
        let result = handler.process(get_db1, 1).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"db1_value".to_vec())));

        // GET key from DB 2 (never written) should return nil
        let get_db2 = Command {
            name: "get".to_string(),
            args: vec![b"mykey".to_vec()],
        };
        let result = handler.process(get_db2, 2).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_move_between_databases() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // SET key in DB 0
        let set_cmd = Command {
            name: "set".to_string(),
            args: vec![b"movekey".to_vec(), b"movevalue".to_vec()],
        };
        handler.process(set_cmd, 0).await.unwrap();

        // MOVE key from DB 0 to DB 3
        let move_cmd = Command {
            name: "move".to_string(),
            args: vec![b"movekey".to_vec(), b"3".to_vec()],
        };
        let result = handler.process(move_cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Key should no longer exist in DB 0
        let get_db0 = Command {
            name: "get".to_string(),
            args: vec![b"movekey".to_vec()],
        };
        let result = handler.process(get_db0, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Key should exist in DB 3
        let get_db3 = Command {
            name: "get".to_string(),
            args: vec![b"movekey".to_vec()],
        };
        let result = handler.process(get_db3, 3).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"movevalue".to_vec())));

        // MOVE from DB 1 to DB 5 (from non-zero source DB)
        let set_db1 = Command {
            name: "set".to_string(),
            args: vec![b"key2".to_vec(), b"val2".to_vec()],
        };
        handler.process(set_db1, 1).await.unwrap();

        let move_from_db1 = Command {
            name: "move".to_string(),
            args: vec![b"key2".to_vec(), b"5".to_vec()],
        };
        let result = handler.process(move_from_db1, 1).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Key should exist in DB 5
        let get_db5 = Command {
            name: "get".to_string(),
            args: vec![b"key2".to_vec()],
        };
        let result = handler.process(get_db5, 5).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val2".to_vec())));
    }

    #[tokio::test]
    async fn test_flushdb_only_flushes_selected_db() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // SET keys in DB 0
        let set_db0 = Command {
            name: "set".to_string(),
            args: vec![b"key0".to_vec(), b"val0".to_vec()],
        };
        handler.process(set_db0, 0).await.unwrap();

        // SET keys in DB 1
        let set_db1 = Command {
            name: "set".to_string(),
            args: vec![b"key1".to_vec(), b"val1".to_vec()],
        };
        handler.process(set_db1, 1).await.unwrap();

        // SET keys in DB 2
        let set_db2 = Command {
            name: "set".to_string(),
            args: vec![b"key2".to_vec(), b"val2".to_vec()],
        };
        handler.process(set_db2, 2).await.unwrap();

        // FLUSHDB on DB 1 only
        let flush_cmd = Command {
            name: "flushdb".to_string(),
            args: vec![],
        };
        handler.process(flush_cmd, 1).await.unwrap();

        // DB 0 key should still exist
        let get_db0 = Command {
            name: "get".to_string(),
            args: vec![b"key0".to_vec()],
        };
        let result = handler.process(get_db0, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val0".to_vec())));

        // DB 1 key should be gone
        let get_db1 = Command {
            name: "get".to_string(),
            args: vec![b"key1".to_vec()],
        };
        let result = handler.process(get_db1, 1).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // DB 2 key should still exist
        let get_db2 = Command {
            name: "get".to_string(),
            args: vec![b"key2".to_vec()],
        };
        let result = handler.process(get_db2, 2).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val2".to_vec())));
    }

    #[tokio::test]
    async fn test_dbsize_respects_db_index() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // Add 3 keys to DB 0
        for i in 0..3 {
            let set_cmd = Command {
                name: "set".to_string(),
                args: vec![format!("k{}", i).into_bytes(), b"v".to_vec()],
            };
            handler.process(set_cmd, 0).await.unwrap();
        }

        // Add 1 key to DB 1
        let set_db1 = Command {
            name: "set".to_string(),
            args: vec![b"only_key".to_vec(), b"v".to_vec()],
        };
        handler.process(set_db1, 1).await.unwrap();

        // DBSIZE on DB 0 should be 3
        let dbsize_db0 = Command {
            name: "dbsize".to_string(),
            args: vec![],
        };
        let result = handler.process(dbsize_db0, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // DBSIZE on DB 1 should be 1
        let dbsize_db1 = Command {
            name: "dbsize".to_string(),
            args: vec![],
        };
        let result = handler.process(dbsize_db1, 1).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // DBSIZE on DB 2 should be 0
        let dbsize_db2 = Command {
            name: "dbsize".to_string(),
            args: vec![],
        };
        let result = handler.process(dbsize_db2, 2).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_keys_scan_respect_db_index() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // Add keys to DB 0
        let set_db0 = Command {
            name: "set".to_string(),
            args: vec![b"alpha".to_vec(), b"v".to_vec()],
        };
        handler.process(set_db0, 0).await.unwrap();

        // Add different key to DB 1
        let set_db1 = Command {
            name: "set".to_string(),
            args: vec![b"beta".to_vec(), b"v".to_vec()],
        };
        handler.process(set_db1, 1).await.unwrap();

        // KEYS * on DB 0 should only find "alpha"
        let keys_db0 = Command {
            name: "keys".to_string(),
            args: vec![b"*".to_vec()],
        };
        let result = handler.process(keys_db0, 0).await.unwrap();
        if let RespValue::Array(Some(keys)) = result {
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RespValue::BulkString(Some(b"alpha".to_vec())));
        } else {
            panic!("Expected array response from KEYS");
        }

        // KEYS * on DB 1 should only find "beta"
        let keys_db1 = Command {
            name: "keys".to_string(),
            args: vec![b"*".to_vec()],
        };
        let result = handler.process(keys_db1, 1).await.unwrap();
        if let RespValue::Array(Some(keys)) = result {
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RespValue::BulkString(Some(b"beta".to_vec())));
        } else {
            panic!("Expected array response from KEYS");
        }
    }

    #[tokio::test]
    async fn test_randomkey_respects_db_index() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // Add key to DB 0 only
        let set_cmd = Command {
            name: "set".to_string(),
            args: vec![b"onlydb0".to_vec(), b"v".to_vec()],
        };
        handler.process(set_cmd, 0).await.unwrap();

        // RANDOMKEY on DB 0 should return a key
        let rk_db0 = Command {
            name: "randomkey".to_string(),
            args: vec![],
        };
        let result = handler.process(rk_db0, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"onlydb0".to_vec())));

        // RANDOMKEY on DB 1 (empty) should return nil
        let rk_db1 = Command {
            name: "randomkey".to_string(),
            args: vec![],
        };
        let result = handler.process(rk_db1, 1).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_hash_operations_across_databases() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // HSET in DB 0
        let hset_db0 = Command {
            name: "hset".to_string(),
            args: vec![b"myhash".to_vec(), b"field".to_vec(), b"val0".to_vec()],
        };
        handler.process(hset_db0, 0).await.unwrap();

        // HSET same hash name in DB 1 with different value
        let hset_db1 = Command {
            name: "hset".to_string(),
            args: vec![b"myhash".to_vec(), b"field".to_vec(), b"val1".to_vec()],
        };
        handler.process(hset_db1, 1).await.unwrap();

        // HGET from DB 0
        let hget_db0 = Command {
            name: "hget".to_string(),
            args: vec![b"myhash".to_vec(), b"field".to_vec()],
        };
        let result = handler.process(hget_db0, 0).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val0".to_vec())));

        // HGET from DB 1
        let hget_db1 = Command {
            name: "hget".to_string(),
            args: vec![b"myhash".to_vec(), b"field".to_vec()],
        };
        let result = handler.process(hget_db1, 1).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val1".to_vec())));
    }

    // ---- Fast byte dispatch tests ----

    #[tokio::test]
    async fn test_process_bytes_top20_fast_path() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // SET via process_bytes (uppercase)
        let result = handler
            .process_bytes(b"SET", &[b"key1".to_vec(), b"val1".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // GET via process_bytes (lowercase)
        let result = handler
            .process_bytes(b"get", &[b"key1".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val1".to_vec())));

        // GET via process_bytes (mixed case)
        let result = handler
            .process_bytes(b"GeT", &[b"key1".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"val1".to_vec())));

        // DEL
        let result = handler
            .process_bytes(b"DEL", &[b"key1".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // EXISTS (key deleted)
        let result = handler
            .process_bytes(b"EXISTS", &[b"key1".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_process_bytes_ping() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // PING no args
        let result = handler.process_bytes(b"PING", &[], 0).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("PONG".to_string()));

        // PING with arg
        let result = handler
            .process_bytes(b"ping", &[b"hello".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"hello".to_vec())));
    }

    #[tokio::test]
    async fn test_process_bytes_incr() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        handler
            .process_bytes(b"SET", &[b"counter".to_vec(), b"10".to_vec()], 0)
            .await
            .unwrap();

        let result = handler
            .process_bytes(b"INCR", &[b"counter".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(11));
    }

    #[tokio::test]
    async fn test_process_bytes_hash_commands() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // HSET
        let result = handler
            .process_bytes(
                b"HSET",
                &[b"h1".to_vec(), b"f1".to_vec(), b"v1".to_vec()],
                0,
            )
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // HGET
        let result = handler
            .process_bytes(b"hget", &[b"h1".to_vec(), b"f1".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"v1".to_vec())));
    }

    #[tokio::test]
    async fn test_process_bytes_set_sadd_srem() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // SADD
        let result = handler
            .process_bytes(b"SADD", &[b"myset".to_vec(), b"a".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // SREM
        let result = handler
            .process_bytes(b"srem", &[b"myset".to_vec(), b"a".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_process_bytes_zadd() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        let result = handler
            .process_bytes(
                b"ZADD",
                &[b"zs".to_vec(), b"1.5".to_vec(), b"mem".to_vec()],
                0,
            )
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_process_bytes_ttl_type_expire() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        handler
            .process_bytes(b"SET", &[b"k".to_vec(), b"v".to_vec()], 0)
            .await
            .unwrap();

        // TYPE
        let result = handler
            .process_bytes(b"TYPE", &[b"k".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("string".to_string()));

        // EXPIRE
        let result = handler
            .process_bytes(b"EXPIRE", &[b"k".to_vec(), b"100".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // TTL
        let result = handler
            .process_bytes(b"TTL", &[b"k".to_vec()], 0)
            .await
            .unwrap();
        if let RespValue::Integer(ttl) = result {
            assert!(ttl > 0 && ttl <= 100);
        } else {
            panic!("Expected integer from TTL");
        }

        // PEXPIRE
        let result = handler
            .process_bytes(b"PEXPIRE", &[b"k".to_vec(), b"50000".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_process_bytes_mget_mset() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // MSET
        let result = handler
            .process_bytes(
                b"MSET",
                &[
                    b"a".to_vec(),
                    b"1".to_vec(),
                    b"b".to_vec(),
                    b"2".to_vec(),
                ],
                0,
            )
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // MGET
        let result = handler
            .process_bytes(b"MGET", &[b"a".to_vec(), b"b".to_vec()], 0)
            .await
            .unwrap();
        match result {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 2);
                assert_eq!(arr[0], RespValue::BulkString(Some(b"1".to_vec())));
                assert_eq!(arr[1], RespValue::BulkString(Some(b"2".to_vec())));
            }
            _ => panic!("Expected array from MGET"),
        }
    }

    #[tokio::test]
    async fn test_process_bytes_lpush_rpush() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // LPUSH
        let result = handler
            .process_bytes(b"LPUSH", &[b"list".to_vec(), b"a".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // RPUSH
        let result = handler
            .process_bytes(b"RPUSH", &[b"list".to_vec(), b"b".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_process_bytes_info() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        let result = handler.process_bytes(b"INFO", &[], 0).await.unwrap();
        match result {
            RespValue::BulkString(Some(data)) => {
                let info_str = String::from_utf8_lossy(&data);
                assert!(info_str.contains("redis_version"));
            }
            _ => panic!("Expected bulk string from INFO"),
        }
    }

    #[tokio::test]
    async fn test_process_bytes_slow_path_fallback() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // APPEND goes through slow path (not in top-20)
        handler
            .process_bytes(b"SET", &[b"key".to_vec(), b"hello".to_vec()], 0)
            .await
            .unwrap();
        let result = handler
            .process_bytes(b"APPEND", &[b"key".to_vec(), b" world".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(11));

        // STRLEN goes through slow path
        let result = handler
            .process_bytes(b"STRLEN", &[b"key".to_vec()], 0)
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(11));

        // HGETALL goes through slow path
        handler
            .process_bytes(
                b"HSET",
                &[b"h".to_vec(), b"f".to_vec(), b"v".to_vec()],
                0,
            )
            .await
            .unwrap();
        let result = handler
            .process_bytes(b"HGETALL", &[b"h".to_vec()], 0)
            .await
            .unwrap();
        match result {
            RespValue::Array(Some(arr)) => assert_eq!(arr.len(), 2),
            _ => panic!("Expected array from HGETALL"),
        }
    }

    #[tokio::test]
    async fn test_dispatch_case_insensitive_all_cases() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        // Test various case combinations for SET
        for variant in &[&b"set"[..], b"Set", b"sEt", b"SET"] {
            let result = handler
                .process_bytes(variant, &[b"k".to_vec(), b"v".to_vec()], 0)
                .await;
            assert!(
                result.is_ok(),
                "SET variant {:?} should dispatch correctly",
                std::str::from_utf8(variant)
            );
        }

        // Test various case combinations for GET
        for variant in &[&b"get"[..], b"Get", b"gEt", b"GET"] {
            let result = handler
                .process_bytes(variant, &[b"k".to_vec()], 0)
                .await;
            assert!(
                result.is_ok(),
                "GET variant {:?} should dispatch correctly",
                std::str::from_utf8(variant)
            );
        }

        // Test various case combinations for DEL
        for variant in &[&b"del"[..], b"Del", b"dEl", b"DEL"] {
            // Re-set key before each DEL
            handler
                .process_bytes(b"SET", &[b"k".to_vec(), b"v".to_vec()], 0)
                .await
                .unwrap();
            let result = handler
                .process_bytes(variant, &[b"k".to_vec()], 0)
                .await;
            assert!(
                result.is_ok(),
                "DEL variant {:?} should dispatch correctly",
                std::str::from_utf8(variant)
            );
        }

        // Test case insensitivity for slow-path commands too
        for variant in &[&b"append"[..], b"APPEND", b"Append"] {
            let result = handler
                .process_bytes(variant, &[b"k2".to_vec(), b"x".to_vec()], 0)
                .await;
            assert!(
                result.is_ok(),
                "APPEND variant {:?} should dispatch correctly",
                std::str::from_utf8(variant)
            );
        }
    }

    #[tokio::test]
    async fn test_unknown_command_via_process_bytes() {
        let config = StorageConfig::default();
        let storage = StorageEngine::new(config);
        let handler = CommandHandler::new(Arc::clone(&storage));

        let result = handler
            .process_bytes(b"NOTACOMMAND", &[], 0)
            .await;
        assert!(result.is_err());
    }
}
