use crate::command::Command;
use crate::command::error::CommandError;
use crate::networking::resp::RespValue;

/// Parse a RESP value into a command
pub fn parse_command(value: RespValue) -> Result<Command, CommandError> {
    match value {
        RespValue::Array(Some(parts)) if !parts.is_empty() => {
            let mut command_parts = Vec::with_capacity(parts.len());
            
            for part in parts {
                match part {
                    RespValue::BulkString(Some(bytes)) => command_parts.push(bytes),
                    _ => return Err(CommandError::InvalidArgument("Command parts must be bulk strings".to_string())),
                }
            }
            
            let name = match String::from_utf8(command_parts[0].clone()) {
                Ok(s) => s.to_lowercase(),
                Err(_) => return Err(CommandError::InvalidArgument("Command name must be valid UTF-8".to_string())),
            };
            
            Ok(Command {
                name,
                args: command_parts[1..].to_vec(),
            })
        },
        _ => Err(CommandError::InvalidArgument("Commands must be encoded as RESP arrays".to_string())),
    }
} 