use bytes::BytesMut;

/// Parser state to distinguish between command mode and data mode
#[derive(Debug, Clone, PartialEq, Default)]
pub enum ParserState {
    /// Reading command headers (RESP protocol commands)
    #[default]
    ReadingCommand,
    /// Reading data content (do not parse as commands)
    #[allow(dead_code)]
    ReadingData { remaining_bytes: usize },
    /// Reading length prefix
    #[allow(dead_code)]
    ReadingLength,
}

/// State-aware RESP parser that prevents data/command confusion
pub struct StatefulRespParser {
    state: ParserState,
}

impl Default for StatefulRespParser {
    fn default() -> Self {
        Self::new()
    }
}

impl StatefulRespParser {
    pub fn new() -> Self {
        Self {
            state: ParserState::ReadingCommand,
        }
    }

    /// Parse buffer with state awareness to prevent treating data as commands
    #[allow(dead_code)]
    pub fn parse_with_state(&mut self, buffer: &mut BytesMut) -> Result<Option<crate::networking::resp::RespValue>, crate::networking::resp::RespError> {
        use crate::networking::resp::{RespValue, RespError};
        
        if buffer.is_empty() {
            return Ok(None);
        }

        match self.state {
            ParserState::ReadingCommand => {
                // Only parse as RESP commands when in command mode
                // This prevents stored data from being interpreted as commands
                match buffer[0] as char {
                    '+' | '-' | ':' | '$' | '*' => {
                        // Valid RESP command start
                        RespValue::parse(buffer)
                    }
                    _ => {
                        // Invalid RESP command start - this should not happen in command mode
                        // Reset state and return error
                        self.state = ParserState::ReadingCommand;
                        let preview = if buffer.len() >= 20 {
                            String::from_utf8_lossy(&buffer[0..20])
                        } else {
                            String::from_utf8_lossy(&buffer[0..buffer.len()])
                        };
                        Err(RespError::InvalidFormatDetails(format!(
                            "Invalid command start in command mode: {}", preview
                        )))
                    }
                }
            }
            ParserState::ReadingData { remaining_bytes } => {
                // In data mode - consume bytes without parsing as commands
                if buffer.len() >= remaining_bytes {
                    // We have all the data, consume it and return to command mode
                    let data = buffer.split_to(remaining_bytes).to_vec();
                    self.state = ParserState::ReadingCommand;
                    Ok(Some(RespValue::BulkString(Some(data))))
                } else {
                    // Still waiting for more data
                    self.state = ParserState::ReadingData { 
                        remaining_bytes: remaining_bytes - buffer.len() 
                    };
                    let _data = buffer.split_to(buffer.len()).to_vec();
                    Err(RespError::Incomplete)
                }
            }
            ParserState::ReadingLength => {
                // Reading length prefix
                RespValue::parse(buffer)
            }
        }
    }

    /// Reset parser state to command mode
    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.state = ParserState::ReadingCommand;
    }

    /// Get current parser state
    #[allow(dead_code)]
    pub fn state(&self) -> &ParserState {
        &self.state
    }

    /// Set parser state manually (for testing)
    #[cfg(test)]
    pub fn set_state(&mut self, state: ParserState) {
        self.state = state;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use crate::networking::resp::RespValue;

    #[test]
    fn test_state_parser_commands() {
        let mut parser = StatefulRespParser::new();
        
        // Valid command
        let mut buffer = BytesMut::from("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");
        let result = parser.parse_with_state(&mut buffer);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), Some(RespValue::Array(_))));
    }

    #[test]
    fn test_state_parser_rejects_data_in_command_mode() {
        let mut parser = StatefulRespParser::new();
        
        // Invalid data that looks like stored content
        let mut buffer = BytesMut::from("B64JSON:W3...");
        let result = parser.parse_with_state(&mut buffer);
        assert!(result.is_err());
        
        // Should contain error about invalid command start
        let error = result.unwrap_err();
        assert!(error.to_string().contains("Invalid command start"));
    }
}