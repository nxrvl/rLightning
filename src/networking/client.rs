use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::networking::resp::RespValue;

const LOG_PREVIEW_LIMIT: usize = 256;

fn preview_ascii(bytes: &[u8]) -> (String, bool) {
    let mut preview = String::new();
    for b in bytes.iter().copied().take(LOG_PREVIEW_LIMIT) {
        for escaped in std::ascii::escape_default(b) {
            preview.push(escaped as char);
        }
    }
    (preview, bytes.len() > LOG_PREVIEW_LIMIT)
}

fn describe_bulk(bytes: &[u8]) -> String {
    let (preview, truncated) = preview_ascii(bytes);
    if truncated {
        format!("{} bytes (preview: \"{}\"…)", bytes.len(), preview)
    } else {
        format!("{} bytes (value: \"{}\")", bytes.len(), preview)
    }
}

fn describe_resp_value(value: &RespValue) -> String {
    match value {
        RespValue::BulkString(Some(bytes)) => {
            format!("BulkString {}", describe_bulk(bytes))
        }
        RespValue::Array(Some(items)) => {
            let mut previews = Vec::new();
            for (idx, item) in items.iter().enumerate() {
                if idx == 3 {
                    previews.push("…".to_string());
                    break;
                }
                previews.push(describe_resp_value(item));
            }
            format!("Array(len={}, [{}])", items.len(), previews.join(", "))
        }
        _ => format!("{:?}", value),
    }
}

/// A simple Redis client for integration testing
#[allow(dead_code)]
pub struct Client {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Client {
    /// Connect to a Redis server
    #[allow(dead_code)]
    pub async fn connect(
        addr: SocketAddr,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;

        Ok(Self {
            stream,
            buffer: BytesMut::with_capacity(1024 * 1024), // 1MB buffer
        })
    }

    /// Send a command to the server
    /// Helper method to easily send a command with string arguments
    #[allow(dead_code)]
    pub async fn send_command_str(
        &mut self,
        command: &str,
        args: &[&str],
    ) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>> {
        let mut cmd_args = Vec::with_capacity(1 + args.len());
        cmd_args.push(command.as_bytes());
        for arg in args {
            cmd_args.push(arg.as_bytes());
        }
        self.send_command_raw(cmd_args).await
    }

    /// Send a command - backward compatibility method that routes to send_command_raw
    #[allow(dead_code)]
    pub async fn send_command<B>(
        &mut self,
        args: Vec<B>,
    ) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>>
    where
        B: AsRef<[u8]>,
    {
        self.send_command_raw(args).await
    }

    /// Send a command with raw byte arguments
    /// Takes a vector of command arguments, each convertible to byte slices
    #[allow(dead_code)]
    pub async fn send_command_raw<B>(
        &mut self,
        args: Vec<B>,
    ) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>>
    where
        B: AsRef<[u8]>,
    {
        // Convert the command to a RESP array
        let mut items = Vec::with_capacity(args.len());

        println!("Client preparing to send command ({} args):", args.len());
        for (i, arg) in args.iter().enumerate() {
            let arg_bytes = arg.as_ref();
            let (preview, truncated) = preview_ascii(arg_bytes);
            if truncated {
                println!(
                    "  Arg {}: {} bytes (preview: \"{}\"…)",
                    i,
                    arg_bytes.len(),
                    preview
                );
            } else {
                println!(
                    "  Arg {}: {} bytes (value: \"{}\")",
                    i,
                    arg_bytes.len(),
                    preview
                );
            }
            items.push(RespValue::BulkString(Some(arg_bytes.to_vec())));
        }

        let cmd = RespValue::Array(Some(items));
        println!("Command as RESP array with {} element(s)", args.len());
        let cmd_bytes = cmd.serialize()?;
        let (preview, truncated) = preview_ascii(&cmd_bytes);
        if truncated {
            println!(
                "Serialized command: {} bytes (preview: \"{}\"…)",
                cmd_bytes.len(),
                preview
            );
        } else {
            println!(
                "Serialized command: {} bytes (value: \"{}\")",
                cmd_bytes.len(),
                preview
            );
        }

        // Send the command
        self.stream.write_all(&cmd_bytes).await?;

        // Read the response with timeout and retries
        for attempt in 1..=3 {
            if attempt > 1 {
                println!("Retry attempt {} for response", attempt);
            }

            match timeout(Duration::from_secs(3), self.read_response()).await {
                Ok(result) => return result,
                Err(e) => {
                    if attempt == 3 {
                        return Err(format!("Timeout waiting for response: {}", e).into());
                    }
                    println!("Timeout waiting for response, retrying...");
                    // Continue to next attempt
                }
            }
        }

        Err("Failed to get response after retries".into())
    }

    /// Helper method to read a response
    async fn read_response(
        &mut self,
    ) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                // For connection closed errors during tests, let's try to handle them better
                // For client-side tests, we'll assume this means the expected error response
                // was received but the server closed the connection
                if !self.buffer.is_empty() {
                    // Try to parse what we have
                    let (preview, truncated) = preview_ascii(self.buffer.as_ref());
                    if truncated {
                        println!(
                            "Connection closed but buffer has data ({} bytes, preview: \"{}\"…)",
                            self.buffer.len(),
                            preview
                        );
                    } else {
                        println!(
                            "Connection closed but buffer has data ({} bytes, value: \"{}\")",
                            self.buffer.len(),
                            preview
                        );
                    }
                    if let Some(value) = RespValue::parse(&mut self.buffer)? {
                        return Ok(value);
                    }
                }
                return Err("Connection closed by server".into());
            }

            println!("Received {} bytes from server", n);
            let buffer_slice = self.buffer.as_ref();
            let (preview, truncated) = preview_ascii(buffer_slice);
            if truncated {
                println!(
                    "Buffer contents: {} bytes (preview: \"{}\"…)",
                    buffer_slice.len(),
                    preview
                );
            } else {
                println!(
                    "Buffer contents: {} bytes (value: \"{}\")",
                    buffer_slice.len(),
                    preview
                );
            }

            // Try to parse a complete response
            if let Some(value) = RespValue::parse(&mut self.buffer)? {
                println!("Parsed response: {}", describe_resp_value(&value));
                return Ok(value);
            }
        }
    }
}
