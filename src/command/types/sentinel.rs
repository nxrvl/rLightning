use std::sync::Arc;

use crate::command::error::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::sentinel::SentinelManager;
use crate::storage::engine::StorageEngine;

/// Handle SENTINEL command - dispatches to SentinelManager
/// This is called from the command handler when sentinel mode is not active
/// (returns error). The actual handling happens in server.rs.
#[allow(dead_code)]
pub async fn sentinel_command(
    _engine: &StorageEngine,
    args: &[Vec<u8>],
    sentinel_mgr: Option<&Arc<SentinelManager>>,
) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    match sentinel_mgr {
        Some(mgr) => mgr
            .handle_sentinel_command(args)
            .await
            .map_err(CommandError::InternalError),
        None => {
            let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();
            match subcommand.as_str() {
                "MYID" => Ok(RespValue::BulkString(Some(
                    b"0000000000000000000000000000000000000000".to_vec(),
                ))),
                "HELP" => Ok(RespValue::SimpleString(
                    "SENTINEL commands are not available - sentinel mode is not enabled"
                        .to_string(),
                )),
                _ => Ok(RespValue::Error(
                    "ERR This instance has sentinel support disabled".to_string(),
                )),
            }
        }
    }
}
