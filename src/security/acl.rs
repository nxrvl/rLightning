use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use sha2::{Sha256, Digest};
use tracing::{debug, info, warn};

use crate::command::error::CommandResult;
use crate::networking::resp::RespValue;

/// Maximum number of ACL log entries to keep
const ACL_LOG_MAX_ENTRIES: usize = 128;

/// Redis command categories for ACL
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandCategory {
    Keyspace,
    Read,
    Write,
    Set,
    SortedSet,
    List,
    Hash,
    String,
    Bitmap,
    HyperLogLog,
    Geo,
    Stream,
    PubSub,
    Admin,
    Fast,
    Slow,
    Blocking,
    Dangerous,
    Connection,
    Transaction,
    Scripting,
    Server,
    Generic,
}

impl CommandCategory {
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "keyspace" => Some(Self::Keyspace),
            "read" => Some(Self::Read),
            "write" => Some(Self::Write),
            "set" => Some(Self::Set),
            "sortedset" => Some(Self::SortedSet),
            "list" => Some(Self::List),
            "hash" => Some(Self::Hash),
            "string" => Some(Self::String),
            "bitmap" => Some(Self::Bitmap),
            "hyperloglog" => Some(Self::HyperLogLog),
            "geo" => Some(Self::Geo),
            "stream" => Some(Self::Stream),
            "pubsub" => Some(Self::PubSub),
            "admin" => Some(Self::Admin),
            "fast" => Some(Self::Fast),
            "slow" => Some(Self::Slow),
            "blocking" => Some(Self::Blocking),
            "dangerous" => Some(Self::Dangerous),
            "connection" => Some(Self::Connection),
            "transaction" => Some(Self::Transaction),
            "scripting" => Some(Self::Scripting),
            "server" => Some(Self::Server),
            "generic" => Some(Self::Generic),
            _ => None,
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::Keyspace => "keyspace",
            Self::Read => "read",
            Self::Write => "write",
            Self::Set => "set",
            Self::SortedSet => "sortedset",
            Self::List => "list",
            Self::Hash => "hash",
            Self::String => "string",
            Self::Bitmap => "bitmap",
            Self::HyperLogLog => "hyperloglog",
            Self::Geo => "geo",
            Self::Stream => "stream",
            Self::PubSub => "pubsub",
            Self::Admin => "admin",
            Self::Fast => "fast",
            Self::Slow => "slow",
            Self::Blocking => "blocking",
            Self::Dangerous => "dangerous",
            Self::Connection => "connection",
            Self::Transaction => "transaction",
            Self::Scripting => "scripting",
            Self::Server => "server",
            Self::Generic => "generic",
        }
    }

    pub fn all() -> &'static [CommandCategory] {
        &[
            Self::Keyspace,
            Self::Read,
            Self::Write,
            Self::Set,
            Self::SortedSet,
            Self::List,
            Self::Hash,
            Self::String,
            Self::Bitmap,
            Self::HyperLogLog,
            Self::Geo,
            Self::Stream,
            Self::PubSub,
            Self::Admin,
            Self::Fast,
            Self::Slow,
            Self::Blocking,
            Self::Dangerous,
            Self::Connection,
            Self::Transaction,
            Self::Scripting,
            Self::Server,
            Self::Generic,
        ]
    }
}

/// Get categories for a command
pub fn get_command_categories(cmd: &str) -> Vec<CommandCategory> {
    match cmd.to_lowercase().as_str() {
        // String commands
        "set" | "setnx" | "setex" | "psetex" | "mset" | "msetnx" | "setrange" | "getset" => {
            vec![CommandCategory::Write, CommandCategory::String]
        }
        "get" | "mget" | "getrange" | "strlen" | "substr" => {
            vec![CommandCategory::Read, CommandCategory::String]
        }
        "getex" | "getdel" => {
            vec![CommandCategory::Write, CommandCategory::String]
        }
        "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" | "append" => {
            vec![CommandCategory::Write, CommandCategory::String]
        }
        "lcs" => vec![CommandCategory::Read, CommandCategory::String],

        // Key commands
        "del" | "unlink" => vec![CommandCategory::Keyspace, CommandCategory::Write],
        "exists" | "type" | "object" | "dump" | "touch" | "randomkey" => {
            vec![CommandCategory::Keyspace, CommandCategory::Read]
        }
        "expire" | "pexpire" | "expireat" | "pexpireat" | "persist" => {
            vec![CommandCategory::Keyspace, CommandCategory::Write]
        }
        "ttl" | "pttl" | "expiretime" | "pexpiretime" => {
            vec![CommandCategory::Keyspace, CommandCategory::Read]
        }
        "rename" | "copy" | "move" | "restore" | "sort" | "sort_ro" => {
            vec![CommandCategory::Keyspace, CommandCategory::Write]
        }
        "keys" | "scan" | "dbsize" => {
            vec![CommandCategory::Keyspace, CommandCategory::Read]
        }
        "select" => vec![CommandCategory::Keyspace, CommandCategory::Connection],
        "wait" | "waitaof" => vec![CommandCategory::Keyspace, CommandCategory::Slow],

        // List commands
        "lpush" | "rpush" | "lpushx" | "rpushx" | "linsert" | "lset" | "ltrim" | "lmove"
        | "lmpop" => vec![CommandCategory::Write, CommandCategory::List],
        "lpop" | "rpop" => vec![CommandCategory::Write, CommandCategory::List],
        "lrange" | "lindex" | "llen" | "lpos" => {
            vec![CommandCategory::Read, CommandCategory::List]
        }
        "blpop" | "brpop" | "blmove" | "blmpop" => {
            vec![
                CommandCategory::Write,
                CommandCategory::List,
                CommandCategory::Blocking,
            ]
        }

        // Hash commands
        "hset" | "hmset" | "hsetnx" | "hdel" | "hincrby" | "hincrbyfloat" => {
            vec![CommandCategory::Write, CommandCategory::Hash]
        }
        "hget" | "hgetall" | "hexists" | "hkeys" | "hvals" | "hlen" | "hmget" | "hstrlen"
        | "hrandfield" | "hscan" => vec![CommandCategory::Read, CommandCategory::Hash],

        // Set commands
        "sadd" | "srem" | "spop" | "smove" => {
            vec![CommandCategory::Write, CommandCategory::Set]
        }
        "smembers" | "sismember" | "scard" | "srandmember" | "sinter" | "sunion" | "sdiff"
        | "sintercard" | "smismember" | "sscan" => {
            vec![CommandCategory::Read, CommandCategory::Set]
        }
        "sinterstore" | "sunionstore" | "sdiffstore" => {
            vec![CommandCategory::Write, CommandCategory::Set]
        }

        // Sorted set commands
        "zadd" | "zrem" | "zincrby" | "zpopmin" | "zpopmax" | "zrangestore"
        | "zremrangebyrank" | "zremrangebyscore" | "zremrangebylex" | "zinterstore"
        | "zunionstore" | "zdiffstore" | "zmpop" => {
            vec![CommandCategory::Write, CommandCategory::SortedSet]
        }
        "zrange" | "zrevrange" | "zscore" | "zcard" | "zcount" | "zrank" | "zrevrank"
        | "zrangebyscore" | "zrevrangebyscore" | "zrangebylex" | "zrevrangebylex"
        | "zlexcount" | "zinter" | "zunion" | "zdiff" | "zrandmember" | "zmscore" | "zscan" => {
            vec![CommandCategory::Read, CommandCategory::SortedSet]
        }
        "bzpopmin" | "bzpopmax" | "bzmpop" => {
            vec![
                CommandCategory::Write,
                CommandCategory::SortedSet,
                CommandCategory::Blocking,
            ]
        }

        // Bitmap commands
        "setbit" | "bitop" | "bitfield" => {
            vec![CommandCategory::Write, CommandCategory::Bitmap]
        }
        "getbit" | "bitcount" | "bitpos" | "bitfield_ro" => {
            vec![CommandCategory::Read, CommandCategory::Bitmap]
        }

        // HyperLogLog commands
        "pfadd" | "pfmerge" => {
            vec![CommandCategory::Write, CommandCategory::HyperLogLog]
        }
        "pfcount" => vec![CommandCategory::Read, CommandCategory::HyperLogLog],

        // Geo commands
        "geoadd" | "geosearchstore" => vec![CommandCategory::Write, CommandCategory::Geo],
        "geodist" | "geohash" | "geopos" | "geosearch" | "georadius" | "georadiusbymember" => {
            vec![CommandCategory::Read, CommandCategory::Geo]
        }

        // Stream commands
        "xadd" | "xtrim" | "xdel" | "xgroup" | "xack" | "xclaim" | "xautoclaim" => {
            vec![CommandCategory::Write, CommandCategory::Stream]
        }
        "xlen" | "xrange" | "xrevrange" | "xinfo" | "xpending" => {
            vec![CommandCategory::Read, CommandCategory::Stream]
        }
        "xread" | "xreadgroup" => {
            vec![
                CommandCategory::Read,
                CommandCategory::Stream,
                CommandCategory::Blocking,
            ]
        }

        // PubSub commands
        "subscribe" | "unsubscribe" | "psubscribe" | "punsubscribe" | "publish" | "pubsub" => {
            vec![CommandCategory::PubSub]
        }

        // Transaction commands
        "multi" | "exec" | "discard" | "watch" | "unwatch" => {
            vec![CommandCategory::Transaction]
        }

        // Scripting commands
        "eval" | "evalsha" | "eval_ro" | "evalsha_ro" | "script" | "function" | "fcall"
        | "fcall_ro" => vec![CommandCategory::Scripting],

        // Server/Connection commands
        "ping" | "echo" | "quit" | "reset" | "hello" | "auth" => {
            vec![CommandCategory::Connection]
        }
        "info" | "config" | "monitor" | "flushall" | "flushdb" | "acl" | "command" | "client"
        | "save" | "bgsave" | "bgrewriteaof" | "lastsave" | "shutdown" | "slowlog"
        | "latency" | "memory" | "debug" | "swapdb" | "time" | "lolwut" => {
            vec![CommandCategory::Admin, CommandCategory::Server]
        }

        // JSON commands
        cmd if cmd.starts_with("json.") || cmd.starts_with("json") => {
            vec![CommandCategory::Read, CommandCategory::Write]
        }

        _ => vec![CommandCategory::Generic],
    }
}

/// Get commands for a given category
pub fn get_commands_in_category(category: &CommandCategory) -> Vec<&'static str> {
    let all_commands = [
        "set", "setnx", "setex", "psetex", "mset", "msetnx", "setrange", "getset", "get",
        "mget", "getrange", "strlen", "substr", "getex", "getdel", "incr", "decr", "incrby",
        "decrby", "incrbyfloat", "append", "lcs", "del", "unlink", "exists", "type", "object",
        "dump", "touch", "randomkey", "expire", "pexpire", "expireat", "pexpireat", "persist",
        "ttl", "pttl", "expiretime", "pexpiretime", "rename", "copy", "move", "restore", "sort",
        "sort_ro", "keys", "scan", "dbsize", "select", "wait", "waitaof", "lpush", "rpush",
        "lpushx", "rpushx", "linsert", "lset", "ltrim", "lmove", "lmpop", "lpop", "rpop",
        "lrange", "lindex", "llen", "lpos", "blpop", "brpop", "blmove", "blmpop", "hset",
        "hmset", "hsetnx", "hdel", "hincrby", "hincrbyfloat", "hget", "hgetall", "hexists",
        "hkeys", "hvals", "hlen", "hmget", "hstrlen", "hrandfield", "hscan", "sadd", "srem",
        "spop", "smove", "smembers", "sismember", "scard", "srandmember", "sinter", "sunion",
        "sdiff", "sintercard", "smismember", "sscan", "sinterstore", "sunionstore", "sdiffstore",
        "zadd", "zrem", "zincrby", "zpopmin", "zpopmax", "zrangestore", "zremrangebyrank",
        "zremrangebyscore", "zremrangebylex", "zinterstore", "zunionstore", "zdiffstore", "zmpop",
        "zrange", "zrevrange", "zscore", "zcard", "zcount", "zrank", "zrevrank", "zrangebyscore",
        "zrevrangebyscore", "zrangebylex", "zrevrangebylex", "zlexcount", "zinter", "zunion",
        "zdiff", "zrandmember", "zmscore", "zscan", "bzpopmin", "bzpopmax", "bzmpop", "setbit",
        "bitop", "bitfield", "getbit", "bitcount", "bitpos", "bitfield_ro", "pfadd", "pfmerge",
        "pfcount", "geoadd", "geosearchstore", "geodist", "geohash", "geopos", "geosearch",
        "georadius", "georadiusbymember", "xadd", "xtrim", "xdel", "xgroup", "xack", "xclaim",
        "xautoclaim", "xlen", "xrange", "xrevrange", "xinfo", "xpending", "xread", "xreadgroup",
        "subscribe", "unsubscribe", "psubscribe", "punsubscribe", "publish", "pubsub", "multi",
        "exec", "discard", "watch", "unwatch", "eval", "evalsha", "eval_ro", "evalsha_ro",
        "script", "function", "fcall", "fcall_ro", "ping", "echo", "quit", "reset", "hello",
        "auth", "info", "config", "monitor", "flushall", "flushdb", "acl", "command", "client",
        "save", "bgsave", "bgrewriteaof", "lastsave", "shutdown", "slowlog", "latency", "memory",
        "debug", "swapdb", "time", "lolwut",
    ];

    all_commands
        .iter()
        .filter(|cmd| {
            let categories = get_command_categories(cmd);
            categories.contains(category)
        })
        .copied()
        .collect()
}

/// An ACL command rule - either allow or deny a specific command or category
#[derive(Debug, Clone)]
pub enum AclCommandRule {
    /// Allow a specific command
    AllowCommand(String),
    /// Deny a specific command
    DenyCommand(String),
    /// Allow all commands in a category
    AllowCategory(CommandCategory),
    /// Deny all commands in a category
    DenyCategory(CommandCategory),
    /// Allow all commands
    AllowAll,
    /// Deny all commands
    DenyAll,
}

/// An ACL user definition
#[derive(Debug, Clone)]
pub struct AclUser {
    pub username: String,
    pub enabled: bool,
    pub passwords: Vec<String>, // SHA256 hex-encoded hashed passwords
    pub nopass: bool,
    pub command_rules: Vec<AclCommandRule>,
    pub key_patterns: Vec<String>,
    pub channel_patterns: Vec<String>,
}

impl AclUser {
    /// Create a new default user with full permissions
    pub fn new_default(password: Option<&str>) -> Self {
        let mut user = AclUser {
            username: "default".to_string(),
            enabled: true,
            passwords: Vec::new(),
            nopass: true,
            command_rules: vec![AclCommandRule::AllowAll],
            key_patterns: vec!["*".to_string()],
            channel_patterns: vec!["*".to_string()],
        };
        if let Some(pass) = password {
            if !pass.is_empty() {
                user.passwords.push(hash_password(pass));
                user.nopass = false;
            }
        }
        user
    }

    /// Create a new empty user (disabled, no permissions)
    pub fn new_empty(username: &str) -> Self {
        AclUser {
            username: username.to_string(),
            enabled: false,
            passwords: Vec::new(),
            nopass: false,
            command_rules: vec![AclCommandRule::DenyAll],
            key_patterns: Vec::new(),
            channel_patterns: Vec::new(),
        }
    }

    /// Check if a given password matches this user's passwords
    pub fn check_password(&self, password: &str) -> bool {
        if self.nopass {
            return true;
        }
        let hashed = hash_password(password);
        self.passwords.contains(&hashed)
    }

    /// Check if this user can execute a given command
    pub fn can_execute_command(&self, cmd: &str) -> bool {
        if !self.enabled {
            return false;
        }

        let cmd_lower = cmd.to_lowercase();
        let cmd_categories = get_command_categories(&cmd_lower);

        // Process rules in order - later rules override earlier ones
        let mut allowed = false;
        for rule in &self.command_rules {
            match rule {
                AclCommandRule::AllowAll => allowed = true,
                AclCommandRule::DenyAll => allowed = false,
                AclCommandRule::AllowCommand(c) => {
                    if c.to_lowercase() == cmd_lower {
                        allowed = true;
                    }
                }
                AclCommandRule::DenyCommand(c) => {
                    if c.to_lowercase() == cmd_lower {
                        allowed = false;
                    }
                }
                AclCommandRule::AllowCategory(cat) => {
                    if cmd_categories.contains(cat) {
                        allowed = true;
                    }
                }
                AclCommandRule::DenyCategory(cat) => {
                    if cmd_categories.contains(cat) {
                        allowed = false;
                    }
                }
            }
        }
        allowed
    }

    /// Check if this user can access a given key
    pub fn can_access_key(&self, key: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        if self.key_patterns.is_empty() {
            return false;
        }
        let key_str = String::from_utf8_lossy(key);
        for pattern in &self.key_patterns {
            if pattern == "*" || glob_match(pattern, &key_str) {
                return true;
            }
        }
        false
    }

    /// Check if this user can access a given pub/sub channel
    pub fn can_access_channel(&self, channel: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        if self.channel_patterns.is_empty() {
            return false;
        }
        let channel_str = String::from_utf8_lossy(channel);
        for pattern in &self.channel_patterns {
            if pattern == "*" || glob_match(pattern, &channel_str) {
                return true;
            }
        }
        false
    }

    /// Apply a rule string to this user (e.g. "+@read", "-set", "~prefix:*", ">password")
    pub fn apply_rule(&mut self, rule: &str) -> Result<(), String> {
        match rule {
            "on" => {
                self.enabled = true;
                Ok(())
            }
            "off" => {
                self.enabled = false;
                Ok(())
            }
            "nopass" => {
                self.nopass = true;
                Ok(())
            }
            "resetpass" => {
                self.passwords.clear();
                self.nopass = false;
                Ok(())
            }
            "allcommands" => {
                self.command_rules.push(AclCommandRule::AllowAll);
                Ok(())
            }
            "nocommands" => {
                self.command_rules.push(AclCommandRule::DenyAll);
                Ok(())
            }
            "allkeys" => {
                self.key_patterns = vec!["*".to_string()];
                Ok(())
            }
            "resetkeys" => {
                self.key_patterns.clear();
                Ok(())
            }
            "allchannels" => {
                self.channel_patterns = vec!["*".to_string()];
                Ok(())
            }
            "resetchannels" => {
                self.channel_patterns.clear();
                Ok(())
            }
            "reset" => {
                self.enabled = false;
                self.passwords.clear();
                self.nopass = false;
                self.command_rules = vec![AclCommandRule::DenyAll];
                self.key_patterns.clear();
                self.channel_patterns.clear();
                Ok(())
            }
            _ => {
                if let Some(password) = rule.strip_prefix('>') {
                    // Add password
                    self.passwords.push(hash_password(password));
                    self.nopass = false;
                    Ok(())
                } else if let Some(password) = rule.strip_prefix('<') {
                    // Remove password
                    let hashed = hash_password(password);
                    self.passwords.retain(|p| p != &hashed);
                    Ok(())
                } else if let Some(pattern) = rule.strip_prefix('~') {
                    // Key pattern
                    self.key_patterns.push(pattern.to_string());
                    Ok(())
                } else if let Some(pattern) = rule.strip_prefix('&') {
                    // Channel pattern
                    self.channel_patterns.push(pattern.to_string());
                    Ok(())
                } else if let Some(rest) = rule.strip_prefix("+@") {
                    // Allow category
                    if rest == "all" {
                        self.command_rules.push(AclCommandRule::AllowAll);
                        Ok(())
                    } else if let Some(cat) = CommandCategory::from_str(rest) {
                        self.command_rules.push(AclCommandRule::AllowCategory(cat));
                        Ok(())
                    } else {
                        Err(format!("Unknown command category '{}'", rest))
                    }
                } else if let Some(rest) = rule.strip_prefix("-@") {
                    // Deny category
                    if rest == "all" {
                        self.command_rules.push(AclCommandRule::DenyAll);
                        Ok(())
                    } else if let Some(cat) = CommandCategory::from_str(rest) {
                        self.command_rules.push(AclCommandRule::DenyCategory(cat));
                        Ok(())
                    } else {
                        Err(format!("Unknown command category '{}'", rest))
                    }
                } else if let Some(cmd) = rule.strip_prefix('+') {
                    // Allow specific command
                    self.command_rules
                        .push(AclCommandRule::AllowCommand(cmd.to_string()));
                    Ok(())
                } else if let Some(cmd) = rule.strip_prefix('-') {
                    // Deny specific command
                    self.command_rules
                        .push(AclCommandRule::DenyCommand(cmd.to_string()));
                    Ok(())
                } else if rule.starts_with('#') {
                    // Hashed password (already SHA256)
                    let hash = rule[1..].to_string();
                    self.passwords.push(hash);
                    self.nopass = false;
                    Ok(())
                } else {
                    Err(format!("Unrecognized ACL rule: '{}'", rule))
                }
            }
        }
    }

    /// Serialize user to Redis ACL rule string
    pub fn to_acl_string(&self) -> String {
        let mut parts = vec![format!("user {}", self.username)];

        if self.enabled {
            parts.push("on".to_string());
        } else {
            parts.push("off".to_string());
        }

        if self.nopass {
            parts.push("nopass".to_string());
        } else {
            for hash in &self.passwords {
                parts.push(format!("#{}", hash));
            }
        }

        // Command rules
        for rule in &self.command_rules {
            match rule {
                AclCommandRule::AllowAll => parts.push("+@all".to_string()),
                AclCommandRule::DenyAll => parts.push("-@all".to_string()),
                AclCommandRule::AllowCommand(c) => parts.push(format!("+{}", c)),
                AclCommandRule::DenyCommand(c) => parts.push(format!("-{}", c)),
                AclCommandRule::AllowCategory(cat) => parts.push(format!("+@{}", cat.name())),
                AclCommandRule::DenyCategory(cat) => parts.push(format!("-@{}", cat.name())),
            }
        }

        // Key patterns
        if self.key_patterns.len() == 1 && self.key_patterns[0] == "*" {
            parts.push("~*".to_string());
        } else {
            for pattern in &self.key_patterns {
                parts.push(format!("~{}", pattern));
            }
        }

        // Channel patterns
        if self.channel_patterns.len() == 1 && self.channel_patterns[0] == "*" {
            parts.push("&*".to_string());
        } else {
            for pattern in &self.channel_patterns {
                parts.push(format!("&{}", pattern));
            }
        }

        parts.join(" ")
    }
}

/// An ACL log entry for denied commands
#[derive(Debug, Clone)]
pub struct AclLogEntry {
    pub count: u64,
    pub reason: String,
    pub context: String,
    pub object: String,
    pub username: String,
    pub age_seconds: f64,
    pub client_info: String,
    pub entry_id: u64,
    pub timestamp_ms: u64,
}

/// The ACL Manager handles user management and permission checking
pub struct AclManager {
    /// Map of username -> AclUser
    users: Arc<RwLock<HashMap<String, AclUser>>>,
    /// Map of client_addr -> username (active sessions)
    sessions: Arc<RwLock<HashMap<String, String>>>,
    /// ACL denial log
    log: Arc<RwLock<VecDeque<AclLogEntry>>>,
    /// Log entry counter
    log_counter: Arc<std::sync::atomic::AtomicU64>,
    /// Whether authentication is required (controls default user behavior)
    require_auth: bool,
}

impl AclManager {
    /// Create a new ACL manager
    pub fn new(require_auth: bool, default_password: Option<&str>) -> Self {
        let mut users = HashMap::new();

        // Always create the "default" user
        let default_user = if require_auth {
            AclUser::new_default(default_password)
        } else {
            // No auth required - default user has nopass and full access
            AclUser::new_default(None)
        };

        info!(
            username = "default",
            enabled = default_user.enabled,
            nopass = default_user.nopass,
            "Created default ACL user"
        );
        users.insert("default".to_string(), default_user);

        AclManager {
            users: Arc::new(RwLock::new(users)),
            sessions: Arc::new(RwLock::new(HashMap::new())),
            log: Arc::new(RwLock::new(VecDeque::new())),
            log_counter: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            require_auth,
        }
    }

    /// Whether authentication is required
    pub fn require_auth(&self) -> bool {
        self.require_auth
    }

    /// Authenticate a client with username and password
    /// Returns Ok(username) on success, Err(message) on failure
    pub fn authenticate(
        &self,
        client_addr: &str,
        username: &str,
        password: &str,
    ) -> Result<String, String> {
        let users = self.users.read().map_err(|_| "Internal error".to_string())?;

        let user = match users.get(username) {
            Some(u) => u,
            None => return Err("WRONGPASS invalid username-password pair or user is disabled.".to_string()),
        };

        if !user.enabled {
            return Err(
                "WRONGPASS invalid username-password pair or user is disabled.".to_string(),
            );
        }

        if !user.check_password(password) {
            return Err(
                "WRONGPASS invalid username-password pair or user is disabled.".to_string(),
            );
        }

        drop(users);

        // Register session
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.insert(client_addr.to_string(), username.to_string());
            debug!(
                client_addr = %client_addr,
                username = %username,
                "Client authenticated"
            );
        }

        Ok(username.to_string())
    }

    /// Check if a client is authenticated
    pub fn is_authenticated(&self, client_addr: &str) -> bool {
        if !self.require_auth {
            return true;
        }
        if let Ok(sessions) = self.sessions.read() {
            sessions.contains_key(client_addr)
        } else {
            false
        }
    }

    /// Get the username for a client session
    pub fn get_username(&self, client_addr: &str) -> Option<String> {
        if let Ok(sessions) = self.sessions.read() {
            sessions.get(client_addr).cloned()
        } else {
            None
        }
    }

    /// Remove a client session
    pub fn remove_client(&self, client_addr: &str) {
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.remove(client_addr);
            debug!(client_addr = %client_addr, "Client removed from ACL sessions");
        }
    }

    /// Check if a command is allowed for the given client
    pub fn check_command_permission(&self, client_addr: &str, cmd: &str) -> bool {
        let username = if let Ok(sessions) = self.sessions.read() {
            sessions.get(client_addr).cloned()
        } else {
            return false;
        };

        let username = match username {
            Some(u) => u,
            None => {
                // Not authenticated - if no auth required, use default user
                if !self.require_auth {
                    "default".to_string()
                } else {
                    return false;
                }
            }
        };

        if let Ok(users) = self.users.read() {
            if let Some(user) = users.get(&username) {
                return user.can_execute_command(cmd);
            }
        }
        false
    }

    /// Check if a key access is allowed for the given client
    pub fn check_key_permission(&self, client_addr: &str, key: &[u8]) -> bool {
        let username = if let Ok(sessions) = self.sessions.read() {
            sessions.get(client_addr).cloned()
        } else {
            return false;
        };

        let username = match username {
            Some(u) => u,
            None => {
                if !self.require_auth {
                    "default".to_string()
                } else {
                    return false;
                }
            }
        };

        if let Ok(users) = self.users.read() {
            if let Some(user) = users.get(&username) {
                return user.can_access_key(key);
            }
        }
        false
    }

    /// Log a denied command
    pub fn log_denial(&self, client_addr: &str, username: &str, cmd: &str, reason: &str) {
        let entry_id = self
            .log_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        let entry = AclLogEntry {
            count: 1,
            reason: reason.to_string(),
            context: "toplevel".to_string(),
            object: cmd.to_string(),
            username: username.to_string(),
            age_seconds: 0.0,
            client_info: client_addr.to_string(),
            entry_id,
            timestamp_ms: now.as_millis() as u64,
        };

        warn!(
            client_addr = %client_addr,
            username = %username,
            command = %cmd,
            reason = %reason,
            "ACL denied command"
        );

        if let Ok(mut log) = self.log.write() {
            log.push_front(entry);
            while log.len() > ACL_LOG_MAX_ENTRIES {
                log.pop_back();
            }
        }
    }

    // ==========================================
    // ACL Command Handlers
    // ==========================================

    /// Handle ACL SETUSER command
    pub fn handle_setuser(&self, args: &[Vec<u8>]) -> CommandResult {
        if args.is_empty() {
            return Err(crate::command::CommandError::WrongNumberOfArguments);
        }

        let username = String::from_utf8_lossy(&args[0]).to_string();

        let mut users = self
            .users
            .write()
            .map_err(|_| crate::command::CommandError::InternalError("Lock error".to_string()))?;

        // Get or create user
        let user = users
            .entry(username.clone())
            .or_insert_with(|| AclUser::new_empty(&username));

        // Apply rules
        for rule_bytes in &args[1..] {
            let rule = String::from_utf8_lossy(rule_bytes).to_string();
            if let Err(e) = user.apply_rule(&rule) {
                return Err(crate::command::CommandError::InvalidArgument(e));
            }
        }

        info!(username = %username, "ACL user updated");
        Ok(RespValue::SimpleString("OK".to_string()))
    }

    /// Handle ACL GETUSER command
    pub fn handle_getuser(&self, args: &[Vec<u8>]) -> CommandResult {
        if args.len() != 1 {
            return Err(crate::command::CommandError::WrongNumberOfArguments);
        }

        let username = String::from_utf8_lossy(&args[0]).to_string();

        let users = self
            .users
            .read()
            .map_err(|_| crate::command::CommandError::InternalError("Lock error".to_string()))?;

        let user = match users.get(&username) {
            Some(u) => u,
            None => return Ok(RespValue::BulkString(None)),
        };

        // Return user info as an array of key-value pairs (like Redis)
        let mut result = Vec::new();

        // flags
        result.push(RespValue::BulkString(Some(b"flags".to_vec())));
        let mut flags = Vec::new();
        if user.enabled {
            flags.push(RespValue::BulkString(Some(b"on".to_vec())));
        } else {
            flags.push(RespValue::BulkString(Some(b"off".to_vec())));
        }
        if user.nopass {
            flags.push(RespValue::BulkString(Some(b"nopass".to_vec())));
        }
        result.push(RespValue::Array(Some(flags)));

        // passwords (hashed)
        result.push(RespValue::BulkString(Some(b"passwords".to_vec())));
        let passwords: Vec<RespValue> = user
            .passwords
            .iter()
            .map(|p| RespValue::BulkString(Some(p.as_bytes().to_vec())))
            .collect();
        result.push(RespValue::Array(Some(passwords)));

        // commands
        result.push(RespValue::BulkString(Some(b"commands".to_vec())));
        let mut cmd_str = String::new();
        for rule in &user.command_rules {
            if !cmd_str.is_empty() {
                cmd_str.push(' ');
            }
            match rule {
                AclCommandRule::AllowAll => cmd_str.push_str("+@all"),
                AclCommandRule::DenyAll => cmd_str.push_str("-@all"),
                AclCommandRule::AllowCommand(c) => {
                    cmd_str.push('+');
                    cmd_str.push_str(c);
                }
                AclCommandRule::DenyCommand(c) => {
                    cmd_str.push('-');
                    cmd_str.push_str(c);
                }
                AclCommandRule::AllowCategory(cat) => {
                    cmd_str.push_str("+@");
                    cmd_str.push_str(cat.name());
                }
                AclCommandRule::DenyCategory(cat) => {
                    cmd_str.push_str("-@");
                    cmd_str.push_str(cat.name());
                }
            }
        }
        result.push(RespValue::BulkString(Some(cmd_str.into_bytes())));

        // keys
        result.push(RespValue::BulkString(Some(b"keys".to_vec())));
        let key_str = user
            .key_patterns
            .iter()
            .map(|p| format!("~{}", p))
            .collect::<Vec<_>>()
            .join(" ");
        result.push(RespValue::BulkString(Some(key_str.into_bytes())));

        // channels
        result.push(RespValue::BulkString(Some(b"channels".to_vec())));
        let channel_str = user
            .channel_patterns
            .iter()
            .map(|p| format!("&{}", p))
            .collect::<Vec<_>>()
            .join(" ");
        result.push(RespValue::BulkString(Some(channel_str.into_bytes())));

        Ok(RespValue::Array(Some(result)))
    }

    /// Handle ACL DELUSER command
    pub fn handle_deluser(&self, args: &[Vec<u8>]) -> CommandResult {
        if args.is_empty() {
            return Err(crate::command::CommandError::WrongNumberOfArguments);
        }

        let mut users = self
            .users
            .write()
            .map_err(|_| crate::command::CommandError::InternalError("Lock error".to_string()))?;

        let mut count = 0i64;
        for arg in args {
            let username = String::from_utf8_lossy(arg).to_string();
            if username == "default" {
                return Err(crate::command::CommandError::InvalidArgument(
                    "The 'default' user cannot be removed".to_string(),
                ));
            }
            if users.remove(&username).is_some() {
                count += 1;
                info!(username = %username, "ACL user deleted");
            }
        }

        // Also remove any sessions for deleted users
        if let Ok(mut sessions) = self.sessions.write() {
            sessions.retain(|_, v| users.contains_key(v));
        }

        Ok(RespValue::Integer(count))
    }

    /// Handle ACL LIST command
    pub fn handle_list(&self) -> CommandResult {
        let users = self
            .users
            .read()
            .map_err(|_| crate::command::CommandError::InternalError("Lock error".to_string()))?;

        let mut result: Vec<RespValue> = users
            .values()
            .map(|u| RespValue::BulkString(Some(u.to_acl_string().into_bytes())))
            .collect();
        result.sort_by(|a, b| {
            let a_str = match a {
                RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                _ => String::new(),
            };
            let b_str = match b {
                RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                _ => String::new(),
            };
            a_str.cmp(&b_str)
        });

        Ok(RespValue::Array(Some(result)))
    }

    /// Handle ACL USERS command
    pub fn handle_users(&self) -> CommandResult {
        let users = self
            .users
            .read()
            .map_err(|_| crate::command::CommandError::InternalError("Lock error".to_string()))?;

        let mut result: Vec<RespValue> = users
            .keys()
            .map(|k| RespValue::BulkString(Some(k.as_bytes().to_vec())))
            .collect();
        result.sort_by(|a, b| {
            let a_str = match a {
                RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                _ => String::new(),
            };
            let b_str = match b {
                RespValue::BulkString(Some(s)) => String::from_utf8_lossy(s).to_string(),
                _ => String::new(),
            };
            a_str.cmp(&b_str)
        });

        Ok(RespValue::Array(Some(result)))
    }

    /// Handle ACL WHOAMI command - returns the username of the current connection
    pub fn handle_whoami(&self, client_addr: &str) -> CommandResult {
        let username = self
            .get_username(client_addr)
            .unwrap_or_else(|| "default".to_string());
        Ok(RespValue::BulkString(Some(username.into_bytes())))
    }

    /// Handle ACL CAT command - list categories or commands in a category
    pub fn handle_cat(&self, args: &[Vec<u8>]) -> CommandResult {
        if args.is_empty() {
            // List all categories
            let categories: Vec<RespValue> = CommandCategory::all()
                .iter()
                .map(|c| RespValue::BulkString(Some(c.name().as_bytes().to_vec())))
                .collect();
            return Ok(RespValue::Array(Some(categories)));
        }

        if args.len() == 1 {
            let cat_name = String::from_utf8_lossy(&args[0]).to_string();
            let cat = CommandCategory::from_str(&cat_name).ok_or_else(|| {
                crate::command::CommandError::InvalidArgument(format!(
                    "Unknown ACL category '{}'",
                    cat_name
                ))
            })?;

            let commands = get_commands_in_category(&cat);
            let result: Vec<RespValue> = commands
                .iter()
                .map(|c| RespValue::BulkString(Some(c.as_bytes().to_vec())))
                .collect();
            return Ok(RespValue::Array(Some(result)));
        }

        Err(crate::command::CommandError::WrongNumberOfArguments)
    }

    /// Handle ACL GENPASS command - generate a random password
    pub fn handle_genpass(&self, args: &[Vec<u8>]) -> CommandResult {
        let bits = if args.is_empty() {
            256
        } else {
            let bits_str = String::from_utf8_lossy(&args[0]);
            bits_str.parse::<usize>().map_err(|_| {
                crate::command::CommandError::InvalidArgument(
                    "ERR ACL GENPASS argument must be a valid integer".to_string(),
                )
            })?
        };

        // Generate random bytes and hex-encode
        let num_bytes = (bits + 7) / 8;
        let bytes: Vec<u8> = (0..num_bytes).map(|_| fastrand::u8(..)).collect();

        let hex_str: String = bytes.iter().map(|b| format!("{:02x}", b)).collect();

        // Truncate to the right number of hex chars
        let hex_chars = (bits + 3) / 4;
        let result = if hex_str.len() > hex_chars {
            hex_str[..hex_chars].to_string()
        } else {
            hex_str
        };

        Ok(RespValue::BulkString(Some(result.into_bytes())))
    }

    /// Handle ACL LOG command
    pub fn handle_log(&self, args: &[Vec<u8>]) -> CommandResult {
        if !args.is_empty() {
            let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
            if subcmd == "RESET" {
                if let Ok(mut log) = self.log.write() {
                    log.clear();
                }
                return Ok(RespValue::SimpleString("OK".to_string()));
            }
            // COUNT argument
            let count = subcmd.parse::<usize>().map_err(|_| {
                crate::command::CommandError::InvalidArgument(
                    "ERR ACL LOG requires either RESET or a count argument".to_string(),
                )
            })?;
            return self.get_log_entries(count);
        }

        // Default: return up to 10 entries
        self.get_log_entries(10)
    }

    fn get_log_entries(&self, count: usize) -> CommandResult {
        let log = self
            .log
            .read()
            .map_err(|_| crate::command::CommandError::InternalError("Lock error".to_string()))?;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let entries: Vec<RespValue> = log
            .iter()
            .take(count)
            .map(|entry| {
                let age = (now.saturating_sub(entry.timestamp_ms)) as f64 / 1000.0;
                let fields = vec![
                    RespValue::BulkString(Some(b"count".to_vec())),
                    RespValue::Integer(entry.count as i64),
                    RespValue::BulkString(Some(b"reason".to_vec())),
                    RespValue::BulkString(Some(entry.reason.as_bytes().to_vec())),
                    RespValue::BulkString(Some(b"context".to_vec())),
                    RespValue::BulkString(Some(entry.context.as_bytes().to_vec())),
                    RespValue::BulkString(Some(b"object".to_vec())),
                    RespValue::BulkString(Some(entry.object.as_bytes().to_vec())),
                    RespValue::BulkString(Some(b"username".to_vec())),
                    RespValue::BulkString(Some(entry.username.as_bytes().to_vec())),
                    RespValue::BulkString(Some(b"age-seconds".to_vec())),
                    RespValue::BulkString(Some(format!("{:.3}", age).into_bytes())),
                    RespValue::BulkString(Some(b"client-info".to_vec())),
                    RespValue::BulkString(Some(entry.client_info.as_bytes().to_vec())),
                    RespValue::BulkString(Some(b"entry-id".to_vec())),
                    RespValue::Integer(entry.entry_id as i64),
                    RespValue::BulkString(Some(b"timestamp-created".to_vec())),
                    RespValue::Integer(entry.timestamp_ms as i64),
                ];
                RespValue::Array(Some(fields))
            })
            .collect();

        Ok(RespValue::Array(Some(entries)))
    }

    /// Handle ACL SAVE command (stub - returns OK)
    pub fn handle_save(&self) -> CommandResult {
        // In a full implementation, this would save ACL rules to a file
        Ok(RespValue::SimpleString("OK".to_string()))
    }

    /// Handle ACL LOAD command (stub - returns OK)
    pub fn handle_load(&self) -> CommandResult {
        // In a full implementation, this would load ACL rules from a file
        Ok(RespValue::SimpleString("OK".to_string()))
    }

    /// Dispatch ACL subcommands
    pub fn handle_acl_command(&self, args: &[Vec<u8>], client_addr: &str) -> CommandResult {
        if args.is_empty() {
            return Err(crate::command::CommandError::WrongNumberOfArguments);
        }

        let subcmd = String::from_utf8_lossy(&args[0]).to_uppercase();
        match subcmd.as_str() {
            "SETUSER" => self.handle_setuser(&args[1..]),
            "GETUSER" => self.handle_getuser(&args[1..]),
            "DELUSER" => self.handle_deluser(&args[1..]),
            "LIST" => self.handle_list(),
            "USERS" => self.handle_users(),
            "WHOAMI" => self.handle_whoami(client_addr),
            "CAT" => self.handle_cat(&args[1..]),
            "GENPASS" => self.handle_genpass(&args[1..]),
            "LOG" => self.handle_log(&args[1..]),
            "SAVE" => self.handle_save(),
            "LOAD" => self.handle_load(),
            "HELP" => {
                let help = vec![
                    "ACL <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
                    "CAT [<category>]",
                    "    List all commands that belong to <category>, or all command categories",
                    "    when no category is specified.",
                    "DELUSER <username> [<username> ...]",
                    "    Delete a list of users.",
                    "GENPASS [<bits>]",
                    "    Generate a secure password. The optional `bits` argument can be used to",
                    "    specify the size of the random string (by default 256 bits are used).",
                    "GETUSER <username>",
                    "    Get the user's details.",
                    "LIST",
                    "    List users and their ACL rules in config file format.",
                    "LOAD",
                    "    Reload users from the ACL file.",
                    "LOG [<count> | RESET]",
                    "    List latest events denied because of ACLs.",
                    "SAVE",
                    "    Save the current ACL rules in the configured ACL file.",
                    "SETUSER <username> <property> [<property> ...]",
                    "    Create or modify a user with the specified properties.",
                    "USERS",
                    "    List all configured usernames.",
                    "WHOAMI",
                    "    Return the current connection username.",
                    "HELP",
                    "    Print this help.",
                ];
                let result: Vec<RespValue> = help
                    .iter()
                    .map(|s| RespValue::BulkString(Some(s.as_bytes().to_vec())))
                    .collect();
                Ok(RespValue::Array(Some(result)))
            }
            _ => Err(crate::command::CommandError::InvalidArgument(format!(
                "Unknown ACL subcommand '{}'",
                subcmd
            ))),
        }
    }
}

/// Simple glob pattern matching (supports * and ?) using iterative algorithm.
/// Uses a two-pointer approach with O(n*m) worst-case instead of exponential backtracking.
fn glob_match(pattern: &str, string: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = string.chars().collect();
    let (plen, slen) = (p.len(), s.len());

    let mut pi = 0;
    let mut si = 0;
    let mut star_pi: Option<usize> = None;
    let mut star_si = 0;

    while si < slen {
        if pi < plen && (p[pi] == '?' || p[pi] == s[si]) {
            pi += 1;
            si += 1;
        } else if pi < plen && p[pi] == '*' {
            // Record star position for backtracking
            star_pi = Some(pi);
            star_si = si;
            pi += 1;
        } else if let Some(sp) = star_pi {
            // Backtrack: advance star match by one character
            pi = sp + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    // Consume trailing * patterns
    while pi < plen && p[pi] == '*' {
        pi += 1;
    }

    pi == plen
}

/// Hash a password using SHA256 for ACL password storage (Redis-compatible).
pub fn hash_password(password: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// Extract key arguments from a command for ACL key pattern checking
/// Returns the indices of arguments that are keys
pub fn get_key_indices(cmd: &str, args: &[Vec<u8>]) -> Vec<usize> {
    match cmd.to_lowercase().as_str() {
        // Commands with key at index 0
        "get" | "set" | "setnx" | "setex" | "psetex" | "getex" | "getdel" | "getset"
        | "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" | "append" | "strlen"
        | "getrange" | "setrange" | "substr" | "del" | "unlink" | "exists" | "type"
        | "expire" | "pexpire" | "expireat" | "pexpireat" | "persist" | "ttl" | "pttl"
        | "expiretime" | "pexpiretime" | "touch" | "object" | "dump" | "restore" | "sort"
        | "sort_ro" | "lpush" | "rpush" | "lpushx" | "rpushx" | "linsert" | "lset"
        | "ltrim" | "lpop" | "rpop" | "lrange" | "lindex" | "llen" | "lpos" | "hset"
        | "hget" | "hgetall" | "hdel" | "hexists" | "hmset" | "hkeys" | "hvals" | "hlen"
        | "hmget" | "hincrby" | "hincrbyfloat" | "hsetnx" | "hstrlen" | "hrandfield"
        | "hscan" | "sadd" | "srem" | "smembers" | "sismember" | "scard" | "spop"
        | "srandmember" | "smismember" | "sscan" | "zadd" | "zrem" | "zscore" | "zcard"
        | "zcount" | "zrank" | "zrevrank" | "zincrby" | "zpopmin" | "zpopmax" | "zrandmember"
        | "zmscore" | "zrange" | "zrevrange" | "zrangebyscore" | "zrevrangebyscore"
        | "zrangebylex" | "zrevrangebylex" | "zremrangebyrank" | "zremrangebyscore"
        | "zremrangebylex" | "zlexcount" | "zscan" | "xadd" | "xlen" | "xrange"
        | "xrevrange" | "xtrim" | "xdel" | "xinfo" | "xgroup" | "xack" | "xpending"
        | "xclaim" | "xautoclaim" | "setbit" | "getbit" | "bitcount" | "bitpos"
        | "bitfield" | "bitfield_ro" | "pfadd" | "pfcount" | "geoadd" | "geodist"
        | "geohash" | "geopos" | "geosearch" | "georadius" | "georadiusbymember" | "wait"
        | "waitaof" => vec![0],

        // Commands with key at index 0 and 1 (source, dest)
        "rename" | "copy" | "lmove" | "blmove" | "smove" | "geosearchstore"
        | "zrangestore" => {
            let mut keys = vec![0];
            if args.len() > 1 {
                keys.push(1);
            }
            keys
        }

        // MGET/MSET - alternating keys
        "mget" => (0..args.len()).collect(),
        "mset" | "msetnx" => (0..args.len()).step_by(2).collect(),

        // BLPOP/BRPOP/BZPOPMIN/BZPOPMAX - all args except last (timeout)
        "blpop" | "brpop" | "bzpopmin" | "bzpopmax" => {
            if args.len() > 1 {
                (0..args.len() - 1).collect()
            } else {
                vec![]
            }
        }

        // RPOPLPUSH - source and destination
        "rpoplpush" => {
            let mut keys = vec![0];
            if args.len() > 1 {
                keys.push(1);
            }
            keys
        }

        // LMPOP/BLMPOP - numkeys then keys (BLMPOP has timeout first)
        "lmpop" => {
            if let Some(first) = args.first() {
                if let Ok(numkeys) = String::from_utf8_lossy(first).parse::<usize>() {
                    (1..=numkeys).filter(|&i| i < args.len()).collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "blmpop" => {
            // BLMPOP timeout numkeys key [key ...] LEFT|RIGHT
            if args.len() >= 2 {
                if let Ok(numkeys) = String::from_utf8_lossy(&args[1]).parse::<usize>() {
                    (2..2 + numkeys).filter(|&i| i < args.len()).collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }

        // ZMPOP/BZMPOP - numkeys then keys (BZMPOP has timeout first)
        "zmpop" => {
            if let Some(first) = args.first() {
                if let Ok(numkeys) = String::from_utf8_lossy(first).parse::<usize>() {
                    (1..=numkeys).filter(|&i| i < args.len()).collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        "bzmpop" => {
            if args.len() >= 2 {
                if let Ok(numkeys) = String::from_utf8_lossy(&args[1]).parse::<usize>() {
                    (2..2 + numkeys).filter(|&i| i < args.len()).collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }

        // SINTERCARD/ZINTER/ZUNION/ZDIFF - numkeys then keys
        "sintercard" | "zinter" | "zunion" | "zdiff" => {
            if let Some(first) = args.first() {
                if let Ok(numkeys) = String::from_utf8_lossy(first).parse::<usize>() {
                    (1..=numkeys).filter(|&i| i < args.len()).collect()
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }

        // XREAD/XREADGROUP - keys follow STREAMS keyword
        "xread" | "xreadgroup" => {
            let streams_idx = args.iter().position(|a| {
                String::from_utf8_lossy(a).to_uppercase() == "STREAMS"
            });
            if let Some(idx) = streams_idx {
                let remaining = args.len() - idx - 1;
                let num_keys = remaining / 2;
                (idx + 1..idx + 1 + num_keys).collect()
            } else {
                vec![]
            }
        }

        // Intersection/union/diff operations (numkeys, then keys)
        "sinter" | "sunion" | "sdiff" | "pfmerge" => (0..args.len()).collect(),
        "sinterstore" | "sunionstore" | "sdiffstore" => (0..args.len()).collect(),

        // ZINTERSTORE/ZUNIONSTORE - dest numkeys key [key ...]
        "zinterstore" | "zunionstore" | "zdiffstore" => {
            let mut keys = vec![0]; // dest
            if args.len() >= 2 {
                if let Ok(numkeys) =
                    String::from_utf8_lossy(&args[1]).parse::<usize>()
                {
                    for i in 0..numkeys {
                        if 2 + i < args.len() {
                            keys.push(2 + i);
                        }
                    }
                }
            }
            keys
        }

        // BITOP - dest src1 [src2 ...]
        "bitop" => {
            if args.len() >= 2 {
                (1..args.len()).collect()
            } else {
                vec![]
            }
        }

        // LCS - key1 key2
        "lcs" => {
            let mut keys = vec![0];
            if args.len() > 1 {
                keys.push(1);
            }
            keys
        }

        // SELECT, PING, AUTH, ACL, etc. - no keys
        "select" | "ping" | "auth" | "acl" | "hello" | "info" | "config" | "monitor"
        | "flushall" | "flushdb" | "scan" | "dbsize" | "randomkey" | "eval" | "evalsha"
        | "eval_ro" | "evalsha_ro" | "script" | "function" | "fcall" | "fcall_ro"
        | "multi" | "exec" | "discard" | "watch" | "unwatch" | "subscribe"
        | "unsubscribe" | "psubscribe" | "punsubscribe" | "publish" | "pubsub"
        | "command" | "client" | "quit" | "reset" | "echo" => vec![],

        // WATCH has keys as all args
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("*", ""));
        assert!(glob_match("hello*", "hello world"));
        assert!(glob_match("hello*", "hello"));
        assert!(!glob_match("hello*", "hell"));
        assert!(glob_match("h?llo", "hello"));
        assert!(!glob_match("h?llo", "hllo"));
        assert!(glob_match("user:*", "user:123"));
        assert!(!glob_match("user:*", "admin:123"));
        assert!(glob_match("cache:*:data", "cache:user:data"));
        assert!(glob_match("prefix:*", "prefix:"));
    }

    #[test]
    fn test_hash_password() {
        let hash1 = hash_password("test");
        let hash2 = hash_password("test");
        assert_eq!(hash1, hash2);
        assert_eq!(hash1.len(), 64); // 4 * 16 hex chars

        let hash3 = hash_password("different");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_acl_user_default() {
        let user = AclUser::new_default(None);
        assert!(user.enabled);
        assert!(user.nopass);
        assert!(user.can_execute_command("get"));
        assert!(user.can_execute_command("set"));
        assert!(user.can_execute_command("acl"));
        assert!(user.can_access_key(b"any_key"));
        assert!(user.check_password("anything"));
    }

    #[test]
    fn test_acl_user_default_with_password() {
        let user = AclUser::new_default(Some("secret"));
        assert!(user.enabled);
        assert!(!user.nopass);
        assert!(user.check_password("secret"));
        assert!(!user.check_password("wrong"));
    }

    #[test]
    fn test_acl_user_empty() {
        let user = AclUser::new_empty("testuser");
        assert!(!user.enabled);
        assert!(!user.nopass);
        assert!(!user.can_execute_command("get"));
        assert!(!user.can_access_key(b"any_key"));
    }

    #[test]
    fn test_acl_user_apply_rules() {
        let mut user = AclUser::new_empty("testuser");

        // Enable user
        user.apply_rule("on").unwrap();
        assert!(user.enabled);

        // Add password
        user.apply_rule(">mypassword").unwrap();
        assert!(user.check_password("mypassword"));
        assert!(!user.check_password("wrong"));

        // Allow read commands
        user.apply_rule("+@read").unwrap();
        assert!(user.can_execute_command("get"));
        assert!(!user.can_execute_command("set"));

        // Add key pattern
        user.apply_rule("~user:*").unwrap();
        assert!(user.can_access_key(b"user:123"));
        assert!(!user.can_access_key(b"admin:123"));

        // Add channel pattern
        user.apply_rule("&notifications:*").unwrap();
        assert!(user.can_access_channel(b"notifications:user1"));
        assert!(!user.can_access_channel(b"system:alerts"));
    }

    #[test]
    fn test_acl_user_command_rules_override() {
        let mut user = AclUser::new_empty("testuser");
        user.apply_rule("on").unwrap();
        user.apply_rule("~*").unwrap();

        // Allow all, then deny dangerous
        user.apply_rule("+@all").unwrap();
        assert!(user.can_execute_command("get"));
        assert!(user.can_execute_command("flushall"));

        user.apply_rule("-@dangerous").unwrap();
        assert!(user.can_execute_command("get"));
        // flushall is admin+server, not dangerous per se
        // Let's test with a command we know is in the category

        // Allow all then deny specific command
        let mut user2 = AclUser::new_empty("testuser2");
        user2.apply_rule("on").unwrap();
        user2.apply_rule("~*").unwrap();
        user2.apply_rule("+@all").unwrap();
        user2.apply_rule("-set").unwrap();
        assert!(user2.can_execute_command("get"));
        assert!(!user2.can_execute_command("set"));
    }

    #[test]
    fn test_acl_user_nopass() {
        let mut user = AclUser::new_empty("testuser");
        user.apply_rule("on").unwrap();
        user.apply_rule("nopass").unwrap();
        assert!(user.check_password("anything"));
        assert!(user.check_password(""));
    }

    #[test]
    fn test_acl_user_reset() {
        let mut user = AclUser::new_default(Some("pass"));
        user.apply_rule("reset").unwrap();
        assert!(!user.enabled);
        assert!(user.passwords.is_empty());
        assert!(!user.nopass);
        assert!(user.key_patterns.is_empty());
    }

    #[test]
    fn test_acl_user_to_string() {
        let mut user = AclUser::new_empty("alice");
        user.apply_rule("on").unwrap();
        user.apply_rule(">secret").unwrap();
        user.apply_rule("+@read").unwrap();
        user.apply_rule("~cache:*").unwrap();
        user.apply_rule("&*").unwrap();

        let s = user.to_acl_string();
        assert!(s.contains("user alice"));
        assert!(s.contains("on"));
        assert!(s.contains("+@read"));
        assert!(s.contains("~cache:*"));
        assert!(s.contains("&*"));
    }

    #[test]
    fn test_acl_manager_authenticate() {
        let mgr = AclManager::new(true, Some("password123"));

        // Authenticate default user with correct password
        let result = mgr.authenticate("client:1", "default", "password123");
        assert!(result.is_ok());
        assert!(mgr.is_authenticated("client:1"));

        // Wrong password
        let result = mgr.authenticate("client:2", "default", "wrong");
        assert!(result.is_err());
        assert!(!mgr.is_authenticated("client:2"));

        // Non-existent user
        let result = mgr.authenticate("client:3", "nobody", "password123");
        assert!(result.is_err());
    }

    #[test]
    fn test_acl_manager_no_auth_required() {
        let mgr = AclManager::new(false, None);

        // Everyone is authenticated when no auth required
        assert!(mgr.is_authenticated("client:1"));
        assert!(mgr.check_command_permission("client:1", "get"));
        assert!(mgr.check_key_permission("client:1", b"any_key"));
    }

    #[test]
    fn test_acl_manager_command_permission() {
        let mgr = AclManager::new(true, Some("pass"));

        // Create a restricted user
        mgr.handle_setuser(&[
            b"reader".to_vec(),
            b"on".to_vec(),
            b">readerpass".to_vec(),
            b"+@read".to_vec(),
            b"~*".to_vec(),
        ])
        .unwrap();

        // Authenticate as reader
        mgr.authenticate("client:1", "reader", "readerpass")
            .unwrap();

        // Can read
        assert!(mgr.check_command_permission("client:1", "get"));
        assert!(mgr.check_command_permission("client:1", "hget"));

        // Cannot write
        assert!(!mgr.check_command_permission("client:1", "set"));
        assert!(!mgr.check_command_permission("client:1", "del"));
    }

    #[test]
    fn test_acl_manager_key_permission() {
        let mgr = AclManager::new(true, Some("pass"));

        // Create user with limited key access
        mgr.handle_setuser(&[
            b"app".to_vec(),
            b"on".to_vec(),
            b">apppass".to_vec(),
            b"+@all".to_vec(),
            b"~app:*".to_vec(),
        ])
        .unwrap();

        mgr.authenticate("client:1", "app", "apppass").unwrap();

        assert!(mgr.check_key_permission("client:1", b"app:data"));
        assert!(!mgr.check_key_permission("client:1", b"system:config"));
    }

    #[test]
    fn test_acl_setuser_getuser() {
        let mgr = AclManager::new(false, None);

        // Create a user
        let result = mgr.handle_setuser(&[
            b"alice".to_vec(),
            b"on".to_vec(),
            b">alicepass".to_vec(),
            b"+@read".to_vec(),
            b"+@write".to_vec(),
            b"~user:*".to_vec(),
            b"&*".to_vec(),
        ]);
        assert!(result.is_ok());

        // Get user details
        let result = mgr.handle_getuser(&[b"alice".to_vec()]).unwrap();
        if let RespValue::Array(Some(fields)) = result {
            assert!(!fields.is_empty());
            // Should have flags, passwords, commands, keys, channels
        } else {
            panic!("Expected array response from GETUSER");
        }

        // Get non-existent user
        let result = mgr.handle_getuser(&[b"nobody".to_vec()]).unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[test]
    fn test_acl_deluser() {
        let mgr = AclManager::new(false, None);

        // Create a user
        mgr.handle_setuser(&[b"temp".to_vec(), b"on".to_vec()])
            .unwrap();

        // Delete user
        let result = mgr.handle_deluser(&[b"temp".to_vec()]).unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Verify deleted
        let result = mgr.handle_getuser(&[b"temp".to_vec()]).unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Cannot delete default user
        let result = mgr.handle_deluser(&[b"default".to_vec()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_acl_list() {
        let mgr = AclManager::new(false, None);

        mgr.handle_setuser(&[b"alice".to_vec(), b"on".to_vec(), b"+@all".to_vec(), b"~*".to_vec()])
            .unwrap();

        let result = mgr.handle_list().unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(items.len() >= 2); // default + alice
        } else {
            panic!("Expected array from ACL LIST");
        }
    }

    #[test]
    fn test_acl_users() {
        let mgr = AclManager::new(false, None);

        mgr.handle_setuser(&[b"bob".to_vec(), b"on".to_vec()])
            .unwrap();

        let result = mgr.handle_users().unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(items.len() >= 2); // default + bob
        } else {
            panic!("Expected array from ACL USERS");
        }
    }

    #[test]
    fn test_acl_whoami() {
        let mgr = AclManager::new(true, Some("pass"));

        // Not authenticated - should return "default"
        let result = mgr.handle_whoami("client:1").unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"default".to_vec()))
        );

        // Authenticate as default
        mgr.authenticate("client:2", "default", "pass").unwrap();
        let result = mgr.handle_whoami("client:2").unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"default".to_vec()))
        );
    }

    #[test]
    fn test_acl_cat() {
        let mgr = AclManager::new(false, None);

        // List all categories
        let result = mgr.handle_cat(&[]).unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
        } else {
            panic!("Expected array from ACL CAT");
        }

        // List commands in "string" category
        let result = mgr.handle_cat(&[b"string".to_vec()]).unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(!items.is_empty());
            // Should include "get", "set", etc.
        } else {
            panic!("Expected array from ACL CAT string");
        }

        // Invalid category
        let result = mgr.handle_cat(&[b"nonexistent".to_vec()]);
        assert!(result.is_err());
    }

    #[test]
    fn test_acl_genpass() {
        let mgr = AclManager::new(false, None);

        // Default 256 bits
        let result = mgr.handle_genpass(&[]).unwrap();
        if let RespValue::BulkString(Some(pass)) = result {
            assert!(!pass.is_empty());
            // 256 bits = 64 hex chars
            assert_eq!(pass.len(), 64);
        } else {
            panic!("Expected bulk string from ACL GENPASS");
        }

        // Custom bits
        let result = mgr.handle_genpass(&[b"128".to_vec()]).unwrap();
        if let RespValue::BulkString(Some(pass)) = result {
            assert_eq!(pass.len(), 32); // 128 bits = 32 hex chars
        } else {
            panic!("Expected bulk string from ACL GENPASS 128");
        }
    }

    #[test]
    fn test_acl_log() {
        let mgr = AclManager::new(false, None);

        // Empty log
        let result = mgr.handle_log(&[]).unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(items.is_empty());
        } else {
            panic!("Expected array from ACL LOG");
        }

        // Add a log entry
        mgr.log_denial("client:1", "testuser", "SET", "command");

        // Check log has entry
        let result = mgr.handle_log(&[]).unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 1);
        } else {
            panic!("Expected array from ACL LOG");
        }

        // Reset log
        mgr.handle_log(&[b"RESET".to_vec()]).unwrap();
        let result = mgr.handle_log(&[]).unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert!(items.is_empty());
        } else {
            panic!("Expected empty array after ACL LOG RESET");
        }
    }

    #[test]
    fn test_acl_manager_remove_client() {
        let mgr = AclManager::new(true, Some("pass"));

        mgr.authenticate("client:1", "default", "pass").unwrap();
        assert!(mgr.is_authenticated("client:1"));

        mgr.remove_client("client:1");
        assert!(!mgr.is_authenticated("client:1"));
    }

    #[test]
    fn test_acl_manager_disabled_user() {
        let mgr = AclManager::new(true, Some("pass"));

        // Create a disabled user
        mgr.handle_setuser(&[
            b"disabled_user".to_vec(),
            b"off".to_vec(),
            b">pass".to_vec(),
            b"+@all".to_vec(),
            b"~*".to_vec(),
        ])
        .unwrap();

        // Can't authenticate as disabled user
        let result = mgr.authenticate("client:1", "disabled_user", "pass");
        assert!(result.is_err());
    }

    #[test]
    fn test_acl_command_dispatch() {
        let mgr = AclManager::new(false, None);

        // SETUSER
        let result = mgr.handle_acl_command(
            &[
                b"SETUSER".to_vec(),
                b"testuser".to_vec(),
                b"on".to_vec(),
                b"+@all".to_vec(),
                b"~*".to_vec(),
            ],
            "client:1",
        );
        assert!(result.is_ok());

        // USERS
        let result = mgr.handle_acl_command(&[b"USERS".to_vec()], "client:1");
        assert!(result.is_ok());

        // WHOAMI
        let result = mgr.handle_acl_command(&[b"WHOAMI".to_vec()], "client:1");
        assert!(result.is_ok());

        // HELP
        let result = mgr.handle_acl_command(&[b"HELP".to_vec()], "client:1");
        assert!(result.is_ok());

        // Unknown subcommand
        let result = mgr.handle_acl_command(&[b"UNKNOWN".to_vec()], "client:1");
        assert!(result.is_err());
    }

    #[test]
    fn test_command_categories() {
        // Test that common commands are categorized correctly
        let cats = get_command_categories("get");
        assert!(cats.contains(&CommandCategory::Read));
        assert!(cats.contains(&CommandCategory::String));

        let cats = get_command_categories("set");
        assert!(cats.contains(&CommandCategory::Write));
        assert!(cats.contains(&CommandCategory::String));

        let cats = get_command_categories("lpush");
        assert!(cats.contains(&CommandCategory::Write));
        assert!(cats.contains(&CommandCategory::List));

        let cats = get_command_categories("subscribe");
        assert!(cats.contains(&CommandCategory::PubSub));

        let cats = get_command_categories("multi");
        assert!(cats.contains(&CommandCategory::Transaction));

        let cats = get_command_categories("eval");
        assert!(cats.contains(&CommandCategory::Scripting));
    }

    #[test]
    fn test_get_commands_in_category() {
        let string_cmds = get_commands_in_category(&CommandCategory::String);
        assert!(string_cmds.contains(&"get"));
        assert!(string_cmds.contains(&"set"));
        assert!(!string_cmds.contains(&"lpush"));

        let list_cmds = get_commands_in_category(&CommandCategory::List);
        assert!(list_cmds.contains(&"lpush"));
        assert!(list_cmds.contains(&"rpop"));
        assert!(!list_cmds.contains(&"get"));
    }

    #[test]
    fn test_get_key_indices() {
        assert_eq!(get_key_indices("get", &[b"key".to_vec()]), vec![0]);
        assert_eq!(get_key_indices("set", &[b"key".to_vec(), b"val".to_vec()]), vec![0]);
        assert_eq!(
            get_key_indices("mget", &[b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec()]),
            vec![0, 1, 2]
        );
        assert_eq!(
            get_key_indices("mset", &[b"k1".to_vec(), b"v1".to_vec(), b"k2".to_vec(), b"v2".to_vec()]),
            vec![0, 2]
        );
        assert_eq!(get_key_indices("ping", &[]), Vec::<usize>::new());
        assert_eq!(get_key_indices("auth", &[b"pass".to_vec()]), Vec::<usize>::new());
    }

    #[test]
    fn test_acl_user_channel_patterns() {
        let mut user = AclUser::new_empty("testuser");
        user.apply_rule("on").unwrap();
        user.apply_rule("allcommands").unwrap();
        user.apply_rule("allkeys").unwrap();
        user.apply_rule("&user:*").unwrap();

        assert!(user.can_access_channel(b"user:123"));
        assert!(!user.can_access_channel(b"system:alerts"));

        // allchannels
        user.apply_rule("allchannels").unwrap();
        assert!(user.can_access_channel(b"system:alerts"));
    }

    #[test]
    fn test_acl_resetpass() {
        let mut user = AclUser::new_default(Some("pass1"));
        assert!(user.check_password("pass1"));

        user.apply_rule("resetpass").unwrap();
        assert!(!user.nopass);
        assert!(user.passwords.is_empty());
        assert!(!user.check_password("pass1"));
        assert!(!user.check_password(""));
    }

    #[test]
    fn test_acl_remove_password() {
        let mut user = AclUser::new_empty("test");
        user.apply_rule("on").unwrap();
        user.apply_rule(">pass1").unwrap();
        user.apply_rule(">pass2").unwrap();
        assert!(user.check_password("pass1"));
        assert!(user.check_password("pass2"));

        user.apply_rule("<pass1").unwrap();
        assert!(!user.check_password("pass1"));
        assert!(user.check_password("pass2"));
    }

    #[test]
    fn test_acl_save_load_stubs() {
        let mgr = AclManager::new(false, None);
        assert!(mgr.handle_save().is_ok());
        assert!(mgr.handle_load().is_ok());
    }

    #[test]
    fn test_auth_with_username_password() {
        let mgr = AclManager::new(true, Some("defaultpass"));

        // Create a non-default user
        mgr.handle_setuser(&[
            b"alice".to_vec(),
            b"on".to_vec(),
            b">alicepass".to_vec(),
            b"+@all".to_vec(),
            b"~*".to_vec(),
            b"&*".to_vec(),
        ])
        .unwrap();

        // Auth as default
        let result = mgr.authenticate("c1", "default", "defaultpass");
        assert!(result.is_ok());
        assert_eq!(mgr.get_username("c1"), Some("default".to_string()));

        // Auth as alice
        let result = mgr.authenticate("c2", "alice", "alicepass");
        assert!(result.is_ok());
        assert_eq!(mgr.get_username("c2"), Some("alice".to_string()));

        // Wrong password for alice
        let result = mgr.authenticate("c3", "alice", "wrong");
        assert!(result.is_err());
    }
}
