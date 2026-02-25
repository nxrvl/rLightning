use mlua::{Lua, MultiValue, Value, IntoLua};

use crate::command::{Command, CommandError, CommandResult};
use crate::command::handler::CommandHandler;
use crate::networking::resp::RespValue;

/// Apply sandbox restrictions to a Lua state, matching Redis behavior.
/// Disables dangerous globals and libraries that could allow sandbox escape.
fn sandbox_lua(lua: &Lua) {
    let globals_to_disable = [
        "io", "os", "loadfile", "dofile", "debug", "require", "package",
        "load", "loadstring", "rawset", "rawget", "rawequal",
        "collectgarbage", "setfenv", "getfenv", "newproxy", "print",
    ];
    for name in &globals_to_disable {
        let _ = lua.globals().set(*name, Value::Nil);
    }
}

/// List of Redis write commands for read-only script enforcement
const WRITE_COMMANDS: &[&str] = &[
    "set", "setnx", "setex", "psetex", "mset", "msetnx", "append", "incr", "incrby",
    "incrbyfloat", "decr", "decrby", "getset", "getdel", "getex", "del", "unlink",
    "expire", "expireat", "pexpire", "pexpireat", "persist", "rename", "renamenx",
    "copy", "move", "lpush", "rpush", "lpushx", "rpushx", "lpop", "rpop", "linsert", "lset", "ltrim",
    "lmove", "lmpop", "rpoplpush", "sadd", "srem", "smove", "spop", "sinterstore",
    "sunionstore", "sdiffstore", "zadd", "zrem", "zincrby", "zpopmin", "zpopmax",
    "zrangestore", "zinterstore", "zunionstore", "zdiffstore", "zremrangebyrank",
    "zremrangebyscore", "zremrangebylex", "hset", "hsetnx", "hmset",
    "hdel", "hincrby", "hincrbyfloat", "xadd", "xdel", "xtrim", "xgroup",
    "xack", "xclaim", "xautoclaim", "pfadd", "pfmerge", "geoadd", "geosearchstore",
    "setbit", "setrange", "bitop", "bitfield", "flushdb", "flushall", "swapdb", "sort",
    "restore",
];

/// Execute a Lua script with the given keys and args.
/// This runs inside a spawn_blocking context.
pub fn execute_in_lua(
    script: &str,
    keys: Vec<Vec<u8>>,
    args: Vec<Vec<u8>>,
    handler: &CommandHandler,
    handle: &tokio::runtime::Handle,
    read_only: bool,
) -> CommandResult {
    let lua = Lua::new();
    sandbox_lua(&lua);

    setup_keys_argv(&lua, &keys, &args)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    setup_redis_api(&lua, handler, handle, read_only)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    // Execute the script
    let result: Value = lua
        .load(script)
        .eval()
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    Ok(lua_to_resp(&result))
}

/// Execute a named function from a library.
/// This runs inside a spawn_blocking context.
pub fn execute_function_in_lua(
    library_code: &str,
    func_name: &str,
    keys: Vec<Vec<u8>>,
    args: Vec<Vec<u8>>,
    handler: &CommandHandler,
    handle: &tokio::runtime::Handle,
    read_only: bool,
) -> CommandResult {
    let lua = Lua::new();
    sandbox_lua(&lua);

    setup_redis_api(&lua, handler, handle, read_only)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    // Create a table to store registered functions
    let registered_functions = lua
        .create_table()
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;
    lua.globals()
        .set("__registered_functions", registered_functions)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    // Override redis.register_function to store callbacks
    lua.load(
        r#"
        redis.register_function = function(name_or_table, callback)
            if type(name_or_table) == 'table' then
                __registered_functions[name_or_table.function_name] = name_or_table.callback
            else
                __registered_functions[name_or_table] = callback
            end
        end
    "#,
    )
    .exec()
    .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    // Load the library (skip shebang line)
    let code_without_shebang: String = library_code
        .lines()
        .skip(1)
        .collect::<Vec<_>>()
        .join("\n");

    lua.load(&code_without_shebang)
        .exec()
        .map_err(|e| CommandError::InternalError(format!("ERR Error loading library: {}", e)))?;

    // Set up KEYS and ARGV
    setup_keys_argv(&lua, &keys, &args)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    // Call the named function safely (avoid string interpolation to prevent injection)
    lua.globals()
        .set("__target_func_name", lua.create_string(func_name.as_bytes()).map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    let call_script = "local func = __registered_functions[__target_func_name]\n\
         if not func then\n\
             error('Function not found')\n\
         end\n\
         return func(KEYS, ARGV)";

    let result: Value = lua
        .load(call_script)
        .eval()
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    Ok(lua_to_resp(&result))
}

fn setup_keys_argv(lua: &Lua, keys: &[Vec<u8>], args: &[Vec<u8>]) -> mlua::Result<()> {
    // KEYS table (1-indexed, as per Lua convention)
    let keys_table = lua.create_table()?;
    for (i, key) in keys.iter().enumerate() {
        keys_table.set(i + 1, lua.create_string(key)?)?;
    }
    lua.globals().set("KEYS", keys_table)?;

    // ARGV table (1-indexed)
    let argv_table = lua.create_table()?;
    for (i, arg) in args.iter().enumerate() {
        argv_table.set(i + 1, lua.create_string(arg)?)?;
    }
    lua.globals().set("ARGV", argv_table)?;

    Ok(())
}

fn setup_redis_api(
    lua: &Lua,
    handler: &CommandHandler,
    handle: &tokio::runtime::Handle,
    read_only: bool,
) -> mlua::Result<()> {
    let redis_table = lua.create_table()?;

    // redis.call - raises error on failure
    let handler_call = handler.clone();
    let handle_call = handle.clone();
    let call_fn = lua.create_function(move |lua, args: MultiValue| {
        redis_call_impl(lua, args, &handler_call, &handle_call, false, read_only)
    })?;
    redis_table.set("call", call_fn)?;

    // redis.pcall - returns error table on failure
    let handler_pcall = handler.clone();
    let handle_pcall = handle.clone();
    let pcall_fn = lua.create_function(move |lua, args: MultiValue| {
        redis_call_impl(lua, args, &handler_pcall, &handle_pcall, true, read_only)
    })?;
    redis_table.set("pcall", pcall_fn)?;

    // redis.status_reply
    let status_fn = lua.create_function(|lua, msg: mlua::String| {
        let table = lua.create_table()?;
        table.set("ok", msg)?;
        Ok(Value::Table(table))
    })?;
    redis_table.set("status_reply", status_fn)?;

    // redis.error_reply
    let error_fn = lua.create_function(|lua, msg: mlua::String| {
        let table = lua.create_table()?;
        table.set("err", msg)?;
        Ok(Value::Table(table))
    })?;
    redis_table.set("error_reply", error_fn)?;

    // redis.log
    let log_fn = lua.create_function(|_, args: MultiValue| {
        let parts: Vec<String> = args
            .iter()
            .map(|v| match v {
                Value::String(s) => s.to_str().map(|b| b.to_string()).unwrap_or_else(|_| "<binary>".to_string()),
                Value::Integer(n) => n.to_string(),
                Value::Number(n) => n.to_string(),
                Value::Boolean(b) => b.to_string(),
                Value::Nil => "nil".to_string(),
                _ => format!("{:?}", v),
            })
            .collect();
        tracing::info!(target: "lua_script", "{}", parts.join(" "));
        Ok(())
    })?;
    redis_table.set("log", log_fn)?;

    // redis.LOG_* constants
    redis_table.set("LOG_DEBUG", 0)?;
    redis_table.set("LOG_VERBOSE", 1)?;
    redis_table.set("LOG_NOTICE", 2)?;
    redis_table.set("LOG_WARNING", 3)?;

    // redis.sha1hex
    let sha1_fn = lua.create_function(|_, input: mlua::String| {
        let hash = sha1_smol::Sha1::from(input.as_bytes()).digest().to_string();
        Ok(hash)
    })?;
    redis_table.set("sha1hex", sha1_fn)?;

    // redis.register_function (no-op by default, overridden in function mode)
    let register_fn = lua.create_function(|_, _args: MultiValue| Ok(()))?;
    redis_table.set("register_function", register_fn)?;

    // redis.replicate_commands (no-op for compatibility)
    let replicate_fn = lua.create_function(|_, _args: MultiValue| Ok(true))?;
    redis_table.set("replicate_commands", replicate_fn)?;

    // redis.set_repl (no-op for compatibility)
    let set_repl_fn = lua.create_function(|_, _args: MultiValue| Ok(()))?;
    redis_table.set("set_repl", set_repl_fn)?;

    // Replication constants
    redis_table.set("REPL_NONE", 0)?;
    redis_table.set("REPL_SLAVE", 1)?;
    redis_table.set("REPL_REPLICA", 1)?;
    redis_table.set("REPL_AOF", 2)?;
    redis_table.set("REPL_ALL", 3)?;

    lua.globals().set("redis", redis_table)?;

    Ok(())
}

fn redis_call_impl(
    lua: &Lua,
    args: MultiValue,
    handler: &CommandHandler,
    handle: &tokio::runtime::Handle,
    protected: bool,
    read_only: bool,
) -> mlua::Result<Value> {
    if args.is_empty() {
        let msg = "ERR wrong number of arguments for 'redis.call'";
        if protected {
            let table = lua.create_table()?;
            table.set("err", msg)?;
            return Ok(Value::Table(table));
        }
        return Err(mlua::Error::external(msg));
    }

    // First argument is the command name
    let cmd_name = match args.get(0) {
        Some(Value::String(s)) => s
            .to_str()
            .map_err(|e| mlua::Error::external(format!("ERR invalid command name: {}", e)))?
            .to_string(),
        _ => {
            let msg = "ERR first argument must be a command string";
            if protected {
                let table = lua.create_table()?;
                table.set("err", msg)?;
                return Ok(Value::Table(table));
            }
            return Err(mlua::Error::external(msg));
        }
    };

    // Enforce read-only mode: block write commands in EVAL_RO/FCALL_RO
    if read_only {
        let cmd_lower = cmd_name.to_lowercase();
        if WRITE_COMMANDS.contains(&cmd_lower.as_str()) {
            let msg = format!("ERR Write commands are not allowed from read-only scripts. Command '{}' is a write command.", cmd_name);
            if protected {
                let table = lua.create_table()?;
                table.set("err", msg.as_str())?;
                return Ok(Value::Table(table));
            }
            return Err(mlua::Error::external(msg));
        }
    }

    // Convert remaining arguments to byte vectors
    let cmd_args: Vec<Vec<u8>> = args
        .iter()
        .skip(1)
        .map(|v| match v {
            Value::String(s) => s.as_bytes().to_vec(),
            Value::Integer(n) => n.to_string().into_bytes(),
            Value::Number(n) => {
                if *n == (*n as i64) as f64 {
                    (*n as i64).to_string().into_bytes()
                } else {
                    n.to_string().into_bytes()
                }
            }
            Value::Boolean(b) => {
                if *b {
                    b"1".to_vec()
                } else {
                    b"0".to_vec()
                }
            }
            _ => b"".to_vec(),
        })
        .collect();

    let command = Command {
        name: cmd_name.to_uppercase(),
        args: cmd_args,
    };

    // Execute the command using the handler (blocking bridge to async)
    let result = handle.block_on(handler.process(command));

    match result {
        Ok(resp) => resp_to_lua(lua, &resp),
        Err(e) => {
            let err_msg = e.to_string();
            if protected {
                let table = lua.create_table()?;
                table.set("err", err_msg.as_str())?;
                Ok(Value::Table(table))
            } else {
                Err(mlua::Error::external(err_msg))
            }
        }
    }
}

/// Convert a RespValue to a Lua value (RESP -> Lua conversion)
/// Following Redis conversion rules:
/// - Redis integer -> Lua number
/// - Redis bulk string -> Lua string
/// - Redis nil -> Lua false
/// - Redis array -> Lua table
/// - Redis status -> Lua table {ok = status}
/// - Redis error -> Lua table {err = message}
pub fn resp_to_lua(lua: &Lua, resp: &RespValue) -> mlua::Result<Value> {
    match resp {
        RespValue::Integer(n) => Ok(Value::Integer(*n)),
        RespValue::BulkString(Some(bytes)) => {
            Ok(Value::String(lua.create_string(bytes)?))
        }
        RespValue::BulkString(None) => Ok(Value::Boolean(false)),
        RespValue::SimpleString(s) => {
            let table = lua.create_table()?;
            table.set("ok", s.as_str())?;
            Ok(Value::Table(table))
        }
        RespValue::Error(msg) => {
            let table = lua.create_table()?;
            table.set("err", msg.as_str())?;
            Ok(Value::Table(table))
        }
        RespValue::Array(Some(items)) => {
            let table = lua.create_table()?;
            for (i, item) in items.iter().enumerate() {
                let lua_val = resp_to_lua(lua, item)?;
                table.set(i + 1, lua_val)?;
            }
            Ok(Value::Table(table))
        }
        RespValue::Array(None) => Ok(Value::Boolean(false)),
        RespValue::Null => Ok(Value::Boolean(false)),
        RespValue::Boolean(b) => {
            if *b {
                Ok(Value::Integer(1))
            } else {
                Ok(Value::Boolean(false))
            }
        }
        RespValue::Double(d) => {
            let s = lua.create_string(d.to_string().as_bytes())?;
            Ok(Value::String(s))
        }
        _ => Ok(Value::Boolean(false)),
    }
}

/// Convert a Lua value to a RespValue (Lua -> RESP conversion)
/// Following Redis conversion rules:
/// - Lua number -> Redis integer (truncated)
/// - Lua string -> Redis bulk string
/// - Lua boolean true -> Redis integer 1
/// - Lua boolean false -> Redis nil
/// - Lua nil -> Redis nil
/// - Lua table {err = msg} -> Redis error
/// - Lua table {ok = msg} -> Redis status
/// - Lua table (array) -> Redis array
pub fn lua_to_resp(value: &Value) -> RespValue {
    match value {
        Value::Integer(n) => RespValue::Integer(*n),
        Value::Number(n) => RespValue::Integer(*n as i64),
        Value::String(s) => RespValue::BulkString(Some(s.as_bytes().to_vec())),
        Value::Boolean(true) => RespValue::Integer(1),
        Value::Boolean(false) => RespValue::BulkString(None),
        Value::Nil => RespValue::BulkString(None),
        Value::Table(t) => {
            // Check for error table: {err = "message"}
            if let Ok(err) = t.get::<mlua::String>("err") {
                if let Ok(s) = err.to_str() {
                    return RespValue::Error(s.to_string());
                }
            }
            // Check for status table: {ok = "message"}
            if let Ok(ok) = t.get::<mlua::String>("ok") {
                if let Ok(s) = ok.to_str() {
                    return RespValue::SimpleString(s.to_string());
                }
            }

            // Regular array table
            let len = t.raw_len();
            let mut items = Vec::with_capacity(len);
            for i in 1..=len {
                if let Ok(val) = t.get::<Value>(i) {
                    items.push(lua_to_resp(&val));
                }
            }
            RespValue::Array(Some(items))
        }
        _ => RespValue::BulkString(None),
    }
}

/// Discover function names from a library's Lua code.
/// Creates a temporary Lua state and evaluates the library to find registered functions.
pub fn discover_functions(code: &str) -> Result<Vec<(String, Option<String>, Vec<String>)>, CommandError> {
    let lua = Lua::new();
    sandbox_lua(&lua);
    let functions = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    let funcs_clone = functions.clone();
    let register_fn = lua
        .create_function(move |_, args: MultiValue| {
            let mut funcs = funcs_clone.lock().unwrap();
            let args_vec: Vec<Value> = args.into_iter().collect();

            if args_vec.len() >= 2 {
                // Simple form: redis.register_function('name', callback)
                if let Value::String(ref s) = args_vec[0] {
                    if let Ok(name) = s.to_str() {
                        funcs.push((name.to_string(), None, vec![]));
                    }
                }
            } else if let Some(Value::Table(t)) = args_vec.first() {
                // Table form: redis.register_function{function_name=..., callback=...}
                if let Ok(name) = t.get::<String>("function_name") {
                    let description: Option<String> = t.get("description").ok();
                    let flags: Vec<String> = t.get::<Vec<String>>("flags").unwrap_or_default();
                    funcs.push((name, description, flags));
                }
            }
            Ok(())
        })
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    let redis_table = lua
        .create_table()
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;
    redis_table
        .set("register_function", register_fn)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;
    // Add no-op stubs for other redis functions that libraries might reference
    let noop = lua
        .create_function(|_, _: MultiValue| Ok(()))
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;
    redis_table
        .set("log", noop)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;
    lua.globals()
        .set("redis", redis_table)
        .map_err(|e| CommandError::InternalError(format!("ERR {}", e)))?;

    // Evaluate the library code (skip shebang line)
    let code_without_shebang: String = code.lines().skip(1).collect::<Vec<_>>().join("\n");

    lua.load(&code_without_shebang)
        .exec()
        .map_err(|e| CommandError::InternalError(format!("ERR Error compiling function: {}", e)))?;

    let funcs = functions.lock().unwrap();
    if funcs.is_empty() {
        return Err(CommandError::InternalError(
            "ERR No functions registered".to_string(),
        ));
    }

    Ok(funcs.clone())
}

// Make mlua Error convertible to CommandError for the ? operator within this module
impl From<mlua::Error> for CommandError {
    fn from(e: mlua::Error) -> Self {
        CommandError::InternalError(format!("ERR {}", e))
    }
}

// Implement IntoLua for RespValue to allow direct conversion in Lua contexts
impl IntoLua for RespValue {
    fn into_lua(self, lua: &Lua) -> mlua::Result<Value> {
        resp_to_lua(lua, &self)
    }
}
