use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Redis MODULE command - Manage loadable modules
///
/// rLightning does not support Redis C-extension modules.
/// These commands are implemented as stubs for protocol compatibility:
/// - MODULE LIST returns an empty array
/// - MODULE LOAD/LOADEX/UNLOAD return appropriate errors
pub async fn module_command(_engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcmd = bytes_to_string(&args[0])?.to_uppercase();
    match subcmd.as_str() {
        "LIST" => module_list(&args[1..]).await,
        "LOAD" => module_load(&args[1..]).await,
        "LOADEX" => module_loadex(&args[1..]).await,
        "UNLOAD" => module_unload(&args[1..]).await,
        _ => Err(CommandError::InvalidArgument("Unknown subcommand or wrong number of arguments for 'module' command".to_string())),
    }
}

/// MODULE LIST - Return list of loaded modules.
/// Always returns an empty array since rLightning does not support modules.
async fn module_list(_args: &[Vec<u8>]) -> CommandResult {
    Ok(RespValue::Array(Some(vec![])))
}

/// MODULE LOAD - Load a module from a shared object file.
/// Returns an error since rLightning does not support C-extension modules.
async fn module_load(args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    Err(CommandError::InternalError(
        "ERR Module loading is not supported by rLightning. rLightning is not compatible with Redis C-extension modules.".to_string()
    ))
}

/// MODULE LOADEX - Load a module with extended options.
/// Returns an error since rLightning does not support C-extension modules.
async fn module_loadex(args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    Err(CommandError::InternalError(
        "ERR Module loading is not supported by rLightning. rLightning is not compatible with Redis C-extension modules.".to_string()
    ))
}

/// MODULE UNLOAD - Unload a module.
/// Returns an error since rLightning does not support modules.
async fn module_unload(args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let name = bytes_to_string(&args[0])?;
    Err(CommandError::InternalError(
        format!("ERR Error unloading module: no such module with that name '{}'. rLightning does not support Redis C-extension modules.", name)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::storage::engine::StorageConfig;

    fn create_test_engine() -> Arc<StorageEngine> {
        let config = StorageConfig::default();
        StorageEngine::new(config)
    }

    #[tokio::test]
    async fn test_module_list_returns_empty_array() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"LIST".to_vec()];
        let result = module_command(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_module_list_case_insensitive() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"list".to_vec()];
        let result = module_command(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_module_load_returns_error() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"LOAD".to_vec(), b"/path/to/module.so".to_vec()];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            CommandError::InternalError(msg) => {
                assert!(msg.contains("not supported"), "Error should mention not supported: {}", msg);
            }
            other => panic!("Expected InternalError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_module_load_missing_path() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"LOAD".to_vec()];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CommandError::WrongNumberOfArguments);
    }

    #[tokio::test]
    async fn test_module_loadex_returns_error() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![
            b"LOADEX".to_vec(),
            b"/path/to/module.so".to_vec(),
            b"CONFIG".to_vec(),
            b"name".to_vec(),
            b"value".to_vec(),
        ];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            CommandError::InternalError(msg) => {
                assert!(msg.contains("not supported"), "Error should mention not supported: {}", msg);
            }
            other => panic!("Expected InternalError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_module_loadex_missing_path() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"LOADEX".to_vec()];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CommandError::WrongNumberOfArguments);
    }

    #[tokio::test]
    async fn test_module_unload_returns_error() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"UNLOAD".to_vec(), b"mymodule".to_vec()];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            CommandError::InternalError(msg) => {
                assert!(msg.contains("mymodule"), "Error should mention module name: {}", msg);
                assert!(msg.contains("not support"), "Error should mention not supported: {}", msg);
            }
            other => panic!("Expected InternalError, got: {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_module_unload_missing_name() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"UNLOAD".to_vec()];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CommandError::WrongNumberOfArguments);
    }

    #[tokio::test]
    async fn test_module_no_subcommand() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), CommandError::WrongNumberOfArguments);
    }

    #[tokio::test]
    async fn test_module_unknown_subcommand() {
        let engine = create_test_engine();
        let args: Vec<Vec<u8>> = vec![b"BADCMD".to_vec()];
        let result = module_command(&engine, &args).await;
        assert!(result.is_err());
        match result.unwrap_err() {
            CommandError::InvalidArgument(msg) => {
                assert!(msg.contains("Unknown subcommand"), "Error should mention unknown subcommand: {}", msg);
            }
            other => panic!("Expected InvalidArgument, got: {:?}", other),
        }
    }
}
