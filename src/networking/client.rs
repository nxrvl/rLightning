use std::net::SocketAddr;
use std::time::Duration;

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

use crate::networking::resp::RespValue;

/// A simple Redis client for integration testing
#[allow(dead_code)]
pub struct Client {
    stream: TcpStream,
    buffer: BytesMut,
}

impl Client {
    /// Connect to a Redis server
    #[allow(dead_code)]
    pub async fn connect(addr: SocketAddr) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let stream = TcpStream::connect(addr).await?;
        
        Ok(Self {
            stream,
            buffer: BytesMut::with_capacity(1024 * 1024), // 1MB buffer
        })
    }
    
    /// Send a command to the server
    /// Helper method to easily send a command with string arguments
    #[allow(dead_code)]
    pub async fn send_command_str(&mut self, command: &str, args: &[&str]) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>> {
        let mut cmd_args = Vec::with_capacity(1 + args.len());
        cmd_args.push(command.as_bytes());
        for arg in args {
            cmd_args.push(arg.as_bytes());
        }
        self.send_command_raw(cmd_args).await
    }
    
    /// Send a command - backward compatibility method that routes to send_command_raw
    #[allow(dead_code)]
    pub async fn send_command<B>(&mut self, args: Vec<B>) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>> 
    where B: AsRef<[u8]> {
        self.send_command_raw(args).await
    }
    
    /// Send a command with raw byte arguments
    /// Takes a vector of command arguments, each convertible to byte slices
    #[allow(dead_code)]
    pub async fn send_command_raw<B>(&mut self, args: Vec<B>) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>> 
    where B: AsRef<[u8]> {
        // Convert the command to a RESP array
        let mut items = Vec::with_capacity(args.len());
        
        println!("Client preparing to send command:");
        for (i, arg) in args.iter().enumerate() {
            let arg_bytes = arg.as_ref();
            let arg_str = String::from_utf8_lossy(arg_bytes);
            println!("  Arg {}: \"{}\" (bytes: {:?})", i, arg_str, arg_bytes);
            items.push(RespValue::BulkString(Some(arg_bytes.to_vec())));
        }
        
        let cmd = RespValue::Array(Some(items));
        println!("Command as RESP: {:?}", cmd);
        let cmd_bytes = cmd.serialize()?;
        println!("Serialized command bytes: {:?}", cmd_bytes);
        
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
    async fn read_response(&mut self) -> Result<RespValue, Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let n = self.stream.read_buf(&mut self.buffer).await?;
            if n == 0 {
                // For connection closed errors during tests, let's try to handle them better
                // For client-side tests, we'll assume this means the expected error response
                // was received but the server closed the connection
                if !self.buffer.is_empty() {
                    // Try to parse what we have
                    println!("Connection closed but buffer has data, trying to parse: {:?}", self.buffer);
                    if let Some(value) = RespValue::parse(&mut self.buffer)? {
                        return Ok(value);
                    }
                }
                return Err("Connection closed by server".into());
            }
            
            println!("Received {} bytes from server", n);
            println!("Buffer contents: {:?}", self.buffer);
            
            // Try to parse a complete response
            if let Some(value) = RespValue::parse(&mut self.buffer)? {
                println!("Parsed response: {:?}", value);
                return Ok(value);
            }
        }
    }
}