pub mod redis_api;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use crate::command::handler::CommandHandler;
use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;

/// Information about a registered function within a library
#[derive(Debug, Clone)]
pub struct FunctionInfo {
    pub name: String,
    pub description: Option<String>,
    pub flags: Vec<String>,
}

/// A loaded function library
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FunctionLibrary {
    pub name: String,
    pub code: String,
    pub engine: String,
    pub functions: Vec<FunctionInfo>,
}

/// Maximum number of cached scripts to prevent unbounded memory growth.
/// Matches a reasonable default; SCRIPT FLUSH can be used to clear manually.
const MAX_SCRIPT_CACHE_SIZE: usize = 10_000;

/// Lua scripting engine providing EVAL, EVALSHA, SCRIPT, FUNCTION, and FCALL support.
/// Manages script cache and function registry.
pub struct ScriptingEngine {
    /// SHA1 -> script source code
    script_cache: RwLock<HashMap<String, String>>,
    /// Library name -> FunctionLibrary
    function_libraries: RwLock<HashMap<String, FunctionLibrary>>,
    /// Function name -> library name (quick lookup index)
    function_index: RwLock<HashMap<String, String>>,
    /// Flag to request script termination (Arc for sharing with spawn_blocking Lua hooks)
    kill_requested: Arc<AtomicBool>,
    /// Counter of currently running scripts (supports concurrent execution)
    scripts_running: AtomicUsize,
}

impl Default for ScriptingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl ScriptingEngine {
    pub fn new() -> Self {
        Self {
            script_cache: RwLock::new(HashMap::new()),
            function_libraries: RwLock::new(HashMap::new()),
            function_index: RwLock::new(HashMap::new()),
            kill_requested: Arc::new(AtomicBool::new(false)),
            scripts_running: AtomicUsize::new(0),
        }
    }

    /// Compute the SHA1 hex digest of a script
    pub fn compute_sha1(script: &str) -> String {
        sha1_smol::Sha1::from(script).digest().to_string()
    }

    /// Load a script into the cache, returning its SHA1 hash.
    /// Evicts oldest entries when cache exceeds MAX_SCRIPT_CACHE_SIZE.
    pub fn load_script(&self, script: &str) -> String {
        let sha1 = Self::compute_sha1(script);
        let mut cache = self.script_cache.write().unwrap_or_else(|e| e.into_inner());
        // Evict entries if cache is at capacity (simple eviction: clear half)
        if cache.len() >= MAX_SCRIPT_CACHE_SIZE && !cache.contains_key(&sha1) {
            let keys_to_remove: Vec<String> = cache.keys().take(cache.len() / 2).cloned().collect();
            for key in keys_to_remove {
                cache.remove(&key);
            }
        }
        cache.insert(sha1.clone(), script.to_string());
        sha1
    }

    /// Get a cached script by its SHA1 hash
    pub fn get_script(&self, sha1: &str) -> Option<String> {
        let cache = self.script_cache.read().unwrap_or_else(|e| e.into_inner());
        cache.get(sha1).cloned()
    }

    /// Check which SHA1 hashes exist in the script cache
    pub fn script_exists(&self, sha1s: &[String]) -> Vec<bool> {
        let cache = self.script_cache.read().unwrap_or_else(|e| e.into_inner());
        sha1s
            .iter()
            .map(|s| cache.contains_key(s.as_str()))
            .collect()
    }

    /// Clear all cached scripts
    pub fn flush_scripts(&self) {
        let mut cache = self.script_cache.write().unwrap_or_else(|e| e.into_inner());
        cache.clear();
    }

    /// Request termination of the currently running script
    pub fn request_kill(&self) {
        self.kill_requested.store(true, Ordering::SeqCst);
    }

    /// Check if any script is currently running
    pub fn is_script_running(&self) -> bool {
        self.scripts_running.load(Ordering::SeqCst) > 0
    }

    /// Execute a Lua script with the given keys and args
    pub async fn execute_script(
        &self,
        script: &str,
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
        handler: &CommandHandler,
        read_only: bool,
    ) -> CommandResult {
        self.scripts_running.fetch_add(1, Ordering::SeqCst);

        let handler = handler.clone();
        let script = script.to_string();
        let handle = tokio::runtime::Handle::current();
        let kill_flag = Arc::clone(&self.kill_requested);

        let result = tokio::task::spawn_blocking(move || {
            redis_api::execute_in_lua(
                &script,
                keys,
                args,
                &handler,
                &handle,
                read_only,
                Some(kill_flag),
            )
        })
        .await;

        let prev = self.scripts_running.fetch_sub(1, Ordering::SeqCst);
        // Clear kill flag only when no more scripts are running
        if prev == 1 {
            self.kill_requested.store(false, Ordering::SeqCst);
        }

        match result {
            Ok(r) => r,
            Err(e) => Err(CommandError::InternalError(format!(
                "ERR Script execution failed: {}",
                e
            ))),
        }
    }

    /// Execute a named function from a loaded library
    pub async fn execute_function(
        &self,
        func_name: &str,
        keys: Vec<Vec<u8>>,
        args: Vec<Vec<u8>>,
        handler: &CommandHandler,
        read_only: bool,
    ) -> CommandResult {
        // Find the library containing this function
        let library_code = {
            let index = self
                .function_index
                .read()
                .unwrap_or_else(|e| e.into_inner());
            let lib_name = index
                .get(func_name)
                .ok_or_else(|| CommandError::InternalError("ERR Function not found".to_string()))?;
            let libs = self
                .function_libraries
                .read()
                .unwrap_or_else(|e| e.into_inner());
            let lib = libs.get(lib_name.as_str()).ok_or_else(|| {
                CommandError::InternalError("ERR Function library not found".to_string())
            })?;
            lib.code.clone()
        };

        self.scripts_running.fetch_add(1, Ordering::SeqCst);

        let handler = handler.clone();
        let func_name = func_name.to_string();
        let handle = tokio::runtime::Handle::current();
        let kill_flag = Arc::clone(&self.kill_requested);

        let result = tokio::task::spawn_blocking(move || {
            redis_api::execute_function_in_lua(
                &library_code,
                &func_name,
                keys,
                args,
                &handler,
                &handle,
                read_only,
                Some(kill_flag),
            )
        })
        .await;

        let prev = self.scripts_running.fetch_sub(1, Ordering::SeqCst);
        if prev == 1 {
            self.kill_requested.store(false, Ordering::SeqCst);
        }

        match result {
            Ok(r) => r,
            Err(e) => Err(CommandError::InternalError(format!(
                "ERR Function execution failed: {}",
                e
            ))),
        }
    }

    /// Parse and load a function library
    pub fn function_load(&self, code: &str, replace: bool) -> Result<String, CommandError> {
        // Parse the shebang line: #!lua name=<libname>
        let first_line = code.lines().next().unwrap_or("");
        if !first_line.starts_with("#!lua") {
            return Err(CommandError::InternalError(
                "ERR Missing library metadata".to_string(),
            ));
        }

        let lib_name = first_line
            .split("name=")
            .nth(1)
            .map(|s| s.trim().to_string())
            .ok_or_else(|| {
                CommandError::InternalError("ERR Library name was not given".to_string())
            })?;

        if lib_name.is_empty() {
            return Err(CommandError::InternalError(
                "ERR Library name was not given".to_string(),
            ));
        }

        // Check if library already exists
        {
            let libs = self
                .function_libraries
                .read()
                .unwrap_or_else(|e| e.into_inner());
            if libs.contains_key(&lib_name) && !replace {
                return Err(CommandError::InternalError(format!(
                    "ERR Library '{}' already exists",
                    lib_name
                )));
            }
        }

        // Discover functions by evaluating in a sandbox
        let discovered = redis_api::discover_functions(code)?;

        let functions: Vec<FunctionInfo> = discovered
            .into_iter()
            .map(|(name, description, flags)| FunctionInfo {
                name,
                description,
                flags,
            })
            .collect();

        // Update the function index and store the library atomically
        // Acquire both locks in the same scope (index first, then libs) to match
        // function_delete/function_flush ordering and prevent TOCTOU races
        {
            let mut index = self
                .function_index
                .write()
                .unwrap_or_else(|e| e.into_inner());
            let mut libs = self
                .function_libraries
                .write()
                .unwrap_or_else(|e| e.into_inner());

            // Remove old library's functions from index if replacing
            if let Some(old_lib) = libs.get(&lib_name) {
                for func in &old_lib.functions {
                    index.remove(&func.name);
                }
            }

            // Check for conflicts with other libraries
            for func in &functions {
                if let Some(existing_lib) = index.get(&func.name)
                    && *existing_lib != lib_name
                {
                    return Err(CommandError::InternalError(format!(
                        "ERR Function {} already exists in library {}",
                        func.name, existing_lib
                    )));
                }
            }

            // Register functions
            for func in &functions {
                index.insert(func.name.clone(), lib_name.clone());
            }

            // Store the library in the same lock scope
            libs.insert(
                lib_name.clone(),
                FunctionLibrary {
                    name: lib_name.clone(),
                    code: code.to_string(),
                    engine: "LUA".to_string(),
                    functions,
                },
            );
        }

        Ok(lib_name)
    }

    /// Delete a function library
    pub fn function_delete(&self, library_name: &str) -> Result<(), CommandError> {
        // Acquire locks in the same order as function_load (index first, then libs)
        // to prevent AB-BA deadlock
        let mut index = self
            .function_index
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let mut libs = self
            .function_libraries
            .write()
            .unwrap_or_else(|e| e.into_inner());

        if let Some(lib) = libs.remove(library_name) {
            for func in &lib.functions {
                index.remove(&func.name);
            }
            Ok(())
        } else {
            Err(CommandError::InternalError(
                "ERR Library not found".to_string(),
            ))
        }
    }

    /// Flush all function libraries
    pub fn function_flush(&self) {
        // Acquire locks in the same order as function_load/function_delete (index first, then libs)
        // to prevent AB-BA deadlock
        let mut index = self
            .function_index
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let mut libs = self
            .function_libraries
            .write()
            .unwrap_or_else(|e| e.into_inner());
        index.clear();
        libs.clear();
    }

    /// List function libraries, optionally filtered by pattern
    pub fn function_list(
        &self,
        library_pattern: Option<&str>,
    ) -> Vec<(String, String, Vec<FunctionInfo>)> {
        let libs = self
            .function_libraries
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let mut result = Vec::new();

        for (name, lib) in libs.iter() {
            if let Some(pat) = library_pattern
                && !name.contains(pat)
            {
                continue;
            }
            result.push((name.clone(), lib.engine.clone(), lib.functions.clone()));
        }

        result
    }

    /// Dump all function libraries as serialized data
    pub fn function_dump(&self) -> Vec<u8> {
        let libs = self
            .function_libraries
            .read()
            .unwrap_or_else(|e| e.into_inner());
        let serializable: Vec<(&str, &str)> = libs
            .iter()
            .map(|(name, lib)| (name.as_str(), lib.code.as_str()))
            .collect();
        serde_json::to_vec(&serializable).unwrap_or_default()
    }

    /// Restore function libraries from serialized data
    pub fn function_restore(&self, data: &[u8], policy: &str) -> Result<(), CommandError> {
        let entries: Vec<(String, String)> = serde_json::from_slice(data).map_err(|e| {
            CommandError::InternalError(format!("ERR Can't restore function library: {}", e))
        })?;

        let replace = match policy.to_uppercase().as_str() {
            "REPLACE" => true,
            "APPEND" => false,
            "FLUSH" => {
                self.function_flush();
                false
            }
            _ => false,
        };

        for (_, code) in entries {
            self.function_load(&code, replace)?;
        }

        Ok(())
    }

    // ========== Command Handlers ==========

    /// Handle EVAL / EVAL_RO command
    /// Syntax: EVAL script numkeys [key ...] [arg ...]
    pub async fn handle_eval(
        &self,
        handler: &CommandHandler,
        args: &[Vec<u8>],
        read_only: bool,
    ) -> CommandResult {
        if args.len() < 2 {
            return Err(CommandError::WrongNumberOfArguments);
        }

        let script = std::str::from_utf8(&args[0])
            .map_err(|_| CommandError::InvalidArgument("invalid script encoding".to_string()))?;

        let numkeys: usize = String::from_utf8_lossy(&args[1]).parse().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

        if args.len() < 2 + numkeys {
            return Err(CommandError::InvalidArgument(
                "Number of keys can't be greater than number of args".to_string(),
            ));
        }

        let keys = args[2..2 + numkeys].to_vec();
        let argv = args[2 + numkeys..].to_vec();

        // Cache the script for future EVALSHA calls
        self.load_script(script);

        self.execute_script(script, keys, argv, handler, read_only)
            .await
    }

    /// Handle EVALSHA / EVALSHA_RO command
    /// Syntax: EVALSHA sha1 numkeys [key ...] [arg ...]
    pub async fn handle_evalsha(
        &self,
        handler: &CommandHandler,
        args: &[Vec<u8>],
        read_only: bool,
    ) -> CommandResult {
        if args.len() < 2 {
            return Err(CommandError::WrongNumberOfArguments);
        }

        let sha1 = String::from_utf8_lossy(&args[0]).to_lowercase();

        let script = self.get_script(&sha1).ok_or_else(|| {
            CommandError::InternalError("NOSCRIPT No matching script. Please use EVAL.".to_string())
        })?;

        let numkeys: usize = String::from_utf8_lossy(&args[1]).parse().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

        if args.len() < 2 + numkeys {
            return Err(CommandError::InvalidArgument(
                "Number of keys can't be greater than number of args".to_string(),
            ));
        }

        let keys = args[2..2 + numkeys].to_vec();
        let argv = args[2 + numkeys..].to_vec();

        self.execute_script(&script, keys, argv, handler, read_only)
            .await
    }

    /// Handle SCRIPT subcommands
    /// Syntax: SCRIPT LOAD|EXISTS|FLUSH|KILL [args...]
    pub fn handle_script(&self, args: &[Vec<u8>]) -> CommandResult {
        if args.is_empty() {
            return Err(CommandError::WrongNumberOfArguments);
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

        match subcommand.as_str() {
            "LOAD" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let script = std::str::from_utf8(&args[1]).map_err(|_| {
                    CommandError::InvalidArgument("invalid script encoding".to_string())
                })?;
                let sha1 = self.load_script(script);
                Ok(RespValue::BulkString(Some(sha1.into_bytes())))
            }
            "EXISTS" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let sha1s: Vec<String> = args[1..]
                    .iter()
                    .map(|a| String::from_utf8_lossy(a).to_lowercase())
                    .collect();
                let results = self.script_exists(&sha1s);
                let resp: Vec<RespValue> = results
                    .into_iter()
                    .map(|exists| RespValue::Integer(if exists { 1 } else { 0 }))
                    .collect();
                Ok(RespValue::Array(Some(resp)))
            }
            "FLUSH" => {
                self.flush_scripts();
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "KILL" => {
                if self.is_script_running() {
                    self.request_kill();
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Err(CommandError::InternalError(
                        "NOTBUSY No scripts in execution right now.".to_string(),
                    ))
                }
            }
            _ => Err(CommandError::InvalidArgument(format!(
                "Unknown SCRIPT subcommand '{}'",
                subcommand
            ))),
        }
    }

    /// Handle FUNCTION subcommands
    /// Syntax: FUNCTION LOAD|DELETE|FLUSH|KILL|LIST|DUMP|RESTORE [args...]
    pub fn handle_function(&self, args: &[Vec<u8>]) -> CommandResult {
        if args.is_empty() {
            return Err(CommandError::WrongNumberOfArguments);
        }

        let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

        match subcommand.as_str() {
            "LOAD" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                // Check for REPLACE flag
                let mut replace = false;
                let mut code_idx = 1;
                if args.len() >= 3 {
                    let flag = String::from_utf8_lossy(&args[1]).to_uppercase();
                    if flag == "REPLACE" {
                        replace = true;
                        code_idx = 2;
                    }
                }
                if code_idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let code = std::str::from_utf8(&args[code_idx]).map_err(|_| {
                    CommandError::InvalidArgument("invalid library encoding".to_string())
                })?;
                let lib_name = self.function_load(code, replace)?;
                Ok(RespValue::BulkString(Some(lib_name.into_bytes())))
            }
            "DELETE" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let lib_name = String::from_utf8_lossy(&args[1]).to_string();
                self.function_delete(&lib_name)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "FLUSH" => {
                self.function_flush();
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            "KILL" => {
                if self.is_script_running() {
                    self.request_kill();
                    Ok(RespValue::SimpleString("OK".to_string()))
                } else {
                    Err(CommandError::InternalError(
                        "NOTBUSY No scripts in execution right now.".to_string(),
                    ))
                }
            }
            "LIST" => {
                // FUNCTION LIST [LIBRARYNAME pattern]
                let pattern = if args.len() >= 3 {
                    let flag = String::from_utf8_lossy(&args[1]).to_uppercase();
                    if flag == "LIBRARYNAME" {
                        Some(String::from_utf8_lossy(&args[2]).to_string())
                    } else {
                        None
                    }
                } else {
                    None
                };

                let libraries = self.function_list(pattern.as_deref());
                let mut result = Vec::new();

                for (name, engine, functions) in libraries {
                    let mut func_list = Vec::new();
                    for func in functions {
                        let flags: Vec<RespValue> = func
                            .flags
                            .iter()
                            .map(|f| RespValue::BulkString(Some(f.clone().into_bytes())))
                            .collect();
                        let func_info = vec![
                            RespValue::BulkString(Some(b"name".to_vec())),
                            RespValue::BulkString(Some(func.name.into_bytes())),
                            RespValue::BulkString(Some(b"description".to_vec())),
                            RespValue::BulkString(Some(
                                func.description.unwrap_or_default().into_bytes(),
                            )),
                            RespValue::BulkString(Some(b"flags".to_vec())),
                            RespValue::Array(Some(flags)),
                        ];
                        func_list.push(RespValue::Array(Some(func_info)));
                    }

                    let lib_info = vec![
                        RespValue::BulkString(Some(b"library_name".to_vec())),
                        RespValue::BulkString(Some(name.into_bytes())),
                        RespValue::BulkString(Some(b"engine".to_vec())),
                        RespValue::BulkString(Some(engine.into_bytes())),
                        RespValue::BulkString(Some(b"functions".to_vec())),
                        RespValue::Array(Some(func_list)),
                    ];
                    result.push(RespValue::Array(Some(lib_info)));
                }

                Ok(RespValue::Array(Some(result)))
            }
            "DUMP" => {
                let data = self.function_dump();
                Ok(RespValue::BulkString(Some(data)))
            }
            "RESTORE" => {
                if args.len() < 2 {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let data = &args[1];
                let policy = if args.len() >= 3 {
                    String::from_utf8_lossy(&args[2]).to_string()
                } else {
                    "APPEND".to_string()
                };
                self.function_restore(data, &policy)?;
                Ok(RespValue::SimpleString("OK".to_string()))
            }
            _ => Err(CommandError::InvalidArgument(format!(
                "Unknown FUNCTION subcommand '{}'",
                subcommand
            ))),
        }
    }

    /// Handle FCALL / FCALL_RO command
    /// Syntax: FCALL function numkeys [key ...] [arg ...]
    pub async fn handle_fcall(
        &self,
        handler: &CommandHandler,
        args: &[Vec<u8>],
        read_only: bool,
    ) -> CommandResult {
        if args.len() < 2 {
            return Err(CommandError::WrongNumberOfArguments);
        }

        let func_name = String::from_utf8_lossy(&args[0]).to_string();

        let numkeys: usize = String::from_utf8_lossy(&args[1]).parse().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

        if args.len() < 2 + numkeys {
            return Err(CommandError::InvalidArgument(
                "Number of keys can't be greater than number of args".to_string(),
            ));
        }

        let keys = args[2..2 + numkeys].to_vec();
        let argv = args[2 + numkeys..].to_vec();

        self.execute_function(&func_name, keys, argv, handler, read_only)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::{StorageConfig, StorageEngine};
    use std::sync::Arc;

    fn create_engine() -> Arc<StorageEngine> {
        StorageEngine::new(StorageConfig::default())
    }

    // ========== SHA1 and Script Cache Tests ==========

    #[test]
    fn test_compute_sha1() {
        let hash = ScriptingEngine::compute_sha1("return 1");
        assert_eq!(hash.len(), 40);
        // SHA1 should be consistent
        assert_eq!(hash, ScriptingEngine::compute_sha1("return 1"));
        // Different scripts produce different hashes
        assert_ne!(hash, ScriptingEngine::compute_sha1("return 2"));
    }

    #[test]
    fn test_load_and_get_script() {
        let engine = ScriptingEngine::new();
        let sha1 = engine.load_script("return 'hello'");
        assert_eq!(sha1.len(), 40);

        let script = engine.get_script(&sha1);
        assert_eq!(script, Some("return 'hello'".to_string()));

        // Non-existent hash
        assert_eq!(engine.get_script("nonexistent"), None);
    }

    #[test]
    fn test_script_exists() {
        let engine = ScriptingEngine::new();
        let sha1 = engine.load_script("return 1");

        let results = engine.script_exists(&[sha1.clone(), "nonexistent".to_string()]);
        assert_eq!(results, vec![true, false]);
    }

    #[test]
    fn test_flush_scripts() {
        let engine = ScriptingEngine::new();
        let sha1 = engine.load_script("return 1");
        assert!(engine.get_script(&sha1).is_some());

        engine.flush_scripts();
        assert!(engine.get_script(&sha1).is_none());
    }

    // ========== EVAL Tests ==========

    #[tokio::test]
    async fn test_eval_return_integer() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(&handler, &[b"return 42".to_vec(), b"0".to_vec()], false)
            .await
            .unwrap();

        assert_eq!(result, RespValue::Integer(42));
    }

    #[tokio::test]
    async fn test_eval_return_string() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[b"return 'hello world'".to_vec(), b"0".to_vec()],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"hello world".to_vec())));
    }

    #[tokio::test]
    async fn test_eval_return_table() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[b"return {1, 2, 3}".to_vec(), b"0".to_vec()],
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::Integer(1),
                RespValue::Integer(2),
                RespValue::Integer(3),
            ]))
        );
    }

    #[tokio::test]
    async fn test_eval_return_nil() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(&handler, &[b"return nil".to_vec(), b"0".to_vec()], false)
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_eval_return_boolean() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(&handler, &[b"return true".to_vec(), b"0".to_vec()], false)
            .await
            .unwrap();

        assert_eq!(result, RespValue::Integer(1));

        let result = scripting
            .handle_eval(&handler, &[b"return false".to_vec(), b"0".to_vec()], false)
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_eval_return_status() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[b"return redis.status_reply('OK')".to_vec(), b"0".to_vec()],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::SimpleString("OK".to_string()));
    }

    #[tokio::test]
    async fn test_eval_return_error() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"return redis.error_reply('custom error')".to_vec(),
                    b"0".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::Error("custom error".to_string()));
    }

    // ========== KEYS and ARGV Tests ==========

    #[tokio::test]
    async fn test_eval_keys_argv() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"return {KEYS[1], KEYS[2], ARGV[1]}".to_vec(),
                    b"2".to_vec(),
                    b"key1".to_vec(),
                    b"key2".to_vec(),
                    b"arg1".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"key1".to_vec())),
                RespValue::BulkString(Some(b"key2".to_vec())),
                RespValue::BulkString(Some(b"arg1".to_vec())),
            ]))
        );
    }

    #[tokio::test]
    async fn test_eval_numkeys_zero() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[b"return ARGV[1]".to_vec(), b"0".to_vec(), b"myarg".to_vec()],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"myarg".to_vec())));
    }

    // ========== redis.call Tests ==========

    #[tokio::test]
    async fn test_eval_redis_call_set_get() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"redis.call('SET', KEYS[1], ARGV[1])\nreturn redis.call('GET', KEYS[1])"
                        .to_vec(),
                    b"1".to_vec(),
                    b"mykey".to_vec(),
                    b"myvalue".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));
    }

    #[tokio::test]
    async fn test_eval_redis_call_incr() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"redis.call('SET', 'counter', '10')\nreturn redis.call('INCR', 'counter')"
                        .to_vec(),
                    b"0".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::Integer(11));
    }

    #[tokio::test]
    async fn test_eval_redis_call_list_operations() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"redis.call('RPUSH', 'mylist', 'a', 'b', 'c')\nreturn redis.call('LRANGE', 'mylist', '0', '-1')"
                        .to_vec(),
                    b"0".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"a".to_vec())),
                RespValue::BulkString(Some(b"b".to_vec())),
                RespValue::BulkString(Some(b"c".to_vec())),
            ]))
        );
    }

    // ========== redis.pcall Tests ==========

    #[tokio::test]
    async fn test_eval_redis_pcall_error() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        // pcall should return error table instead of raising
        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"local ok, err = pcall(function() return redis.call('INVALID_CMD') end)\nif not ok then return 'error caught' else return 'no error' end"
                        .to_vec(),
                    b"0".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(
            result,
            RespValue::BulkString(Some(b"error caught".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_eval_redis_pcall_returns_error_table() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"local result = redis.pcall('INVALID_CMD')\nif result.err then return result.err else return 'ok' end"
                        .to_vec(),
                    b"0".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        // Should contain the error message
        match result {
            RespValue::BulkString(Some(msg)) => {
                let msg_str = String::from_utf8_lossy(&msg);
                assert!(
                    msg_str.contains("unknown command") || msg_str.contains("Unknown"),
                    "Expected error about unknown command, got: {}",
                    msg_str
                );
            }
            _ => panic!("Expected bulk string with error message, got: {:?}", result),
        }
    }

    // ========== Error Handling Tests ==========

    #[tokio::test]
    async fn test_eval_syntax_error() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[b"this is not valid lua!!!".to_vec(), b"0".to_vec()],
                false,
            )
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_eval_runtime_error() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[b"error('my custom error')".to_vec(), b"0".to_vec()],
                false,
            )
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("my custom error"));
    }

    #[tokio::test]
    async fn test_eval_wrong_numkeys() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        // numkeys > available args
        let result = scripting
            .handle_eval(&handler, &[b"return 1".to_vec(), b"5".to_vec()], false)
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_eval_missing_args() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        // No args at all
        let result = scripting.handle_eval(&handler, &[], false).await;
        assert!(result.is_err());

        // Only script, no numkeys
        let result = scripting
            .handle_eval(&handler, &[b"return 1".to_vec()], false)
            .await;
        assert!(result.is_err());
    }

    // ========== EVALSHA Tests ==========

    #[tokio::test]
    async fn test_evalsha_after_eval() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        // First EVAL to cache the script
        let script = "return 'cached'";
        scripting
            .handle_eval(
                &handler,
                &[script.as_bytes().to_vec(), b"0".to_vec()],
                false,
            )
            .await
            .unwrap();

        // Now use EVALSHA
        let sha1 = ScriptingEngine::compute_sha1(script);
        let result = scripting
            .handle_evalsha(&handler, &[sha1.as_bytes().to_vec(), b"0".to_vec()], false)
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"cached".to_vec())));
    }

    #[tokio::test]
    async fn test_evalsha_after_script_load() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        // SCRIPT LOAD
        let sha1 = scripting.load_script("return 'loaded'");

        // EVALSHA
        let result = scripting
            .handle_evalsha(&handler, &[sha1.as_bytes().to_vec(), b"0".to_vec()], false)
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"loaded".to_vec())));
    }

    #[tokio::test]
    async fn test_evalsha_noscript_error() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_evalsha(
                &handler,
                &[b"nonexistent_sha1".to_vec(), b"0".to_vec()],
                false,
            )
            .await;

        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("NOSCRIPT"));
    }

    // ========== SCRIPT Subcommand Tests ==========

    #[test]
    fn test_script_load() {
        let scripting = ScriptingEngine::new();
        let result = scripting
            .handle_script(&[b"LOAD".to_vec(), b"return 1".to_vec()])
            .unwrap();

        if let RespValue::BulkString(Some(sha1)) = result {
            let sha1_str = String::from_utf8(sha1).unwrap();
            assert_eq!(sha1_str.len(), 40);
        } else {
            panic!("Expected BulkString response");
        }
    }

    #[test]
    fn test_script_exists_subcommand() {
        let scripting = ScriptingEngine::new();
        let sha1 = scripting.load_script("return 1");

        let result = scripting
            .handle_script(&[
                b"EXISTS".to_vec(),
                sha1.as_bytes().to_vec(),
                b"nonexistent".to_vec(),
            ])
            .unwrap();

        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::Integer(1), RespValue::Integer(0),]))
        );
    }

    #[test]
    fn test_script_flush() {
        let scripting = ScriptingEngine::new();
        let sha1 = scripting.load_script("return 1");
        assert!(scripting.get_script(&sha1).is_some());

        scripting.handle_script(&[b"FLUSH".to_vec()]).unwrap();

        assert!(scripting.get_script(&sha1).is_none());
    }

    // ========== FUNCTION Tests ==========

    #[test]
    fn test_function_load() {
        let scripting = ScriptingEngine::new();
        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";

        let result = scripting.function_load(code, false);
        assert_eq!(result.unwrap(), "mylib");
    }

    #[test]
    fn test_function_load_duplicate_error() {
        let scripting = ScriptingEngine::new();
        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";

        scripting.function_load(code, false).unwrap();
        let result = scripting.function_load(code, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_function_load_replace() {
        let scripting = ScriptingEngine::new();
        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";

        scripting.function_load(code, false).unwrap();
        let result = scripting.function_load(code, true);
        assert!(result.is_ok());
    }

    #[test]
    fn test_function_delete() {
        let scripting = ScriptingEngine::new();
        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";

        scripting.function_load(code, false).unwrap();
        let result = scripting.function_delete("mylib");
        assert!(result.is_ok());

        // Deleting again should fail
        let result = scripting.function_delete("mylib");
        assert!(result.is_err());
    }

    #[test]
    fn test_function_flush() {
        let scripting = ScriptingEngine::new();
        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";

        scripting.function_load(code, false).unwrap();
        scripting.function_flush();

        let list = scripting.function_list(None);
        assert!(list.is_empty());
    }

    #[test]
    fn test_function_list() {
        let scripting = ScriptingEngine::new();
        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";

        scripting.function_load(code, false).unwrap();
        let list = scripting.function_list(None);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "mylib");
        assert_eq!(list[0].1, "LUA");
        assert_eq!(list[0].2.len(), 1);
        assert_eq!(list[0].2[0].name, "myfunc");
    }

    #[tokio::test]
    async fn test_fcall_basic() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello from function' end)";
        scripting.function_load(code, false).unwrap();

        let result = scripting
            .handle_fcall(&handler, &[b"myfunc".to_vec(), b"0".to_vec()], false)
            .await
            .unwrap();

        assert_eq!(
            result,
            RespValue::BulkString(Some(b"hello from function".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_fcall_with_keys_args() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let code = "#!lua name=mylib\nredis.register_function('setfunc', function(keys, args)\n  redis.call('SET', keys[1], args[1])\n  return redis.call('GET', keys[1])\nend)";
        scripting.function_load(code, false).unwrap();

        let result = scripting
            .handle_fcall(
                &handler,
                &[
                    b"setfunc".to_vec(),
                    b"1".to_vec(),
                    b"mykey".to_vec(),
                    b"myvalue".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"myvalue".to_vec())));
    }

    #[tokio::test]
    async fn test_fcall_nonexistent_function() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_fcall(&handler, &[b"nonexistent".to_vec(), b"0".to_vec()], false)
            .await;

        assert!(result.is_err());
    }

    // ========== redis.sha1hex Test ==========

    #[tokio::test]
    async fn test_eval_redis_sha1hex() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"return redis.sha1hex('Hello World')".to_vec(),
                    b"0".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        // SHA1 of "Hello World" = "0a4d55a8d778e5022fab701977c5d840bbc486d0"
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"0a4d55a8d778e5022fab701977c5d840bbc486d0".to_vec()))
        );
    }

    // ========== FUNCTION DUMP/RESTORE Test ==========

    #[test]
    fn test_function_dump_restore() {
        let scripting = ScriptingEngine::new();
        let code = "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return 'hello' end)";

        scripting.function_load(code, false).unwrap();

        let dump = scripting.function_dump();
        assert!(!dump.is_empty());

        // Flush and restore
        scripting.function_flush();
        assert!(scripting.function_list(None).is_empty());

        scripting.function_restore(&dump, "APPEND").unwrap();
        let list = scripting.function_list(None);
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].0, "mylib");
    }

    // ========== Script Caching via EVAL ==========

    #[tokio::test]
    async fn test_eval_caches_script() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let script = "return 'auto-cached'";
        let sha1 = ScriptingEngine::compute_sha1(script);

        // Script not cached yet
        assert!(scripting.get_script(&sha1).is_none());

        // EVAL should cache it
        scripting
            .handle_eval(
                &handler,
                &[script.as_bytes().to_vec(), b"0".to_vec()],
                false,
            )
            .await
            .unwrap();

        // Now it should be cached
        assert!(scripting.get_script(&sha1).is_some());
    }

    // ========== Handler Integration Tests ==========

    #[tokio::test]
    async fn test_handler_eval_dispatch() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);

        let cmd = crate::command::Command {
            name: "EVAL".to_string(),
            args: vec![b"return 42".to_vec(), b"0".to_vec()],
        };

        let result = handler.process(cmd, 0).await.unwrap();
        assert_eq!(result, RespValue::Integer(42));
    }

    #[tokio::test]
    async fn test_handler_evalsha_dispatch() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);

        // First load a script
        let script = "return 'from evalsha'";
        let sha1 = handler.scripting().load_script(script);

        let cmd = crate::command::Command {
            name: "EVALSHA".to_string(),
            args: vec![sha1.into_bytes(), b"0".to_vec()],
        };

        let result = handler.process(cmd, 0).await.unwrap();
        assert_eq!(
            result,
            RespValue::BulkString(Some(b"from evalsha".to_vec()))
        );
    }

    #[tokio::test]
    async fn test_handler_script_load_dispatch() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);

        let cmd = crate::command::Command {
            name: "SCRIPT".to_string(),
            args: vec![b"LOAD".to_vec(), b"return 1".to_vec()],
        };

        let result = handler.process(cmd, 0).await.unwrap();
        match result {
            RespValue::BulkString(Some(sha1)) => {
                assert_eq!(String::from_utf8(sha1).unwrap().len(), 40);
            }
            _ => panic!("Expected BulkString with SHA1"),
        }
    }

    // ========== Complex Script Tests ==========

    #[tokio::test]
    async fn test_eval_complex_script_conditional() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let script = r#"
            redis.call('SET', 'x', '5')
            local val = tonumber(redis.call('GET', 'x'))
            if val > 3 then
                return 'greater'
            else
                return 'less'
            end
        "#;

        let result = scripting
            .handle_eval(
                &handler,
                &[script.as_bytes().to_vec(), b"0".to_vec()],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"greater".to_vec())));
    }

    #[tokio::test]
    async fn test_eval_complex_script_loop() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        let script = r#"
            local sum = 0
            for i = 1, 10 do
                sum = sum + i
            end
            return sum
        "#;

        let result = scripting
            .handle_eval(
                &handler,
                &[script.as_bytes().to_vec(), b"0".to_vec()],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::Integer(55));
    }

    // ========== Script Replication (no-op verification) ==========

    #[tokio::test]
    async fn test_eval_replicate_commands_noop() {
        let storage = create_engine();
        let handler = CommandHandler::new(storage);
        let scripting = ScriptingEngine::new();

        // redis.replicate_commands() should be a no-op that returns true
        let result = scripting
            .handle_eval(
                &handler,
                &[
                    b"redis.replicate_commands()\nreturn 'ok'".to_vec(),
                    b"0".to_vec(),
                ],
                false,
            )
            .await
            .unwrap();

        assert_eq!(result, RespValue::BulkString(Some(b"ok".to_vec())));
    }
}
