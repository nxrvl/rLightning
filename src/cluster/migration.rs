use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// MIGRATE command implementation
/// Transfers a key from this instance to another Redis/rLightning instance
#[allow(clippy::too_many_arguments)]
pub async fn migrate_key(
    engine: &StorageEngine,
    host: &str,
    port: u16,
    key: &[u8],
    db: u16,
    timeout_ms: u64,
    copy: bool,
    replace: bool,
    auth: Option<&str>,
    auth2: Option<(&str, &str)>,
) -> Result<RespValue, String> {
    let addr_str = format!("{}:{}", host, port);
    let addr: SocketAddr = addr_str
        .parse()
        .map_err(|e| format!("ERR Invalid address: {}", e))?;

    let timeout = Duration::from_millis(timeout_ms);

    // Get the key data from local storage using DUMP format: [type_byte][serialized_value]
    let item = engine
        .get_item(key)
        .await
        .map_err(|e| format!("ERR {}", e))?
        .ok_or_else(|| "ERR no such key".to_string())?;
    let type_byte: u8 = match item.data_type() {
        crate::storage::item::RedisDataType::String => 0,
        crate::storage::item::RedisDataType::List => 1,
        crate::storage::item::RedisDataType::Set => 2,
        crate::storage::item::RedisDataType::Hash => 3,
        crate::storage::item::RedisDataType::ZSet => 4,
        crate::storage::item::RedisDataType::Stream => 5,
    };
    let value_bytes: Vec<u8> = match &item.value {
        crate::storage::value::StoreValue::Str(v) => v.clone(),
        crate::storage::value::StoreValue::Hash(m) => {
            let std_map: std::collections::HashMap<Vec<u8>, Vec<u8>> = m.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
            bincode::serialize(&std_map).map_err(|e| format!("ERR serialization: {}", e))?
        }
        crate::storage::value::StoreValue::Set(s) => {
            let std_set: std::collections::HashSet<Vec<u8>> = s.iter().cloned().collect();
            bincode::serialize(&std_set).map_err(|e| format!("ERR serialization: {}", e))?
        }
        crate::storage::value::StoreValue::ZSet(ss) => {
            bincode::serialize(ss).map_err(|e| format!("ERR serialization: {}", e))?
        }
        crate::storage::value::StoreValue::List(deque) => {
            let vec: Vec<Vec<u8>> = deque.iter().cloned().collect();
            bincode::serialize(&vec).map_err(|e| format!("ERR serialization: {}", e))?
        }
        crate::storage::value::StoreValue::Stream(s) => {
            bincode::serialize(s).map_err(|e| format!("ERR serialization: {}", e))?
        }
    };
    let mut dump_data = Vec::with_capacity(1 + value_bytes.len());
    dump_data.push(type_byte);
    dump_data.extend_from_slice(&value_bytes);

    let ttl_ms = engine
        .ttl(key)
        .await
        .map_err(|e| format!("ERR {}", e))?
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);

    // Connect to the target
    let mut stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
        .await
        .map_err(|_| "IOERR error or timeout connecting to the specified instance".to_string())?
        .map_err(|e| {
            format!(
                "IOERR error or timeout connecting to the specified instance: {}",
                e
            )
        })?;

    // Authenticate if needed
    if let Some((user, pass)) = auth2 {
        send_command(&mut stream, &["AUTH", user, pass], timeout).await?;
    } else if let Some(pass) = auth {
        send_command(&mut stream, &["AUTH", pass], timeout).await?;
    }

    // Select the target database
    if db != 0 {
        send_command(&mut stream, &["SELECT", &db.to_string()], timeout).await?;
    }

    // Send RESTORE command to the target
    let key_str = String::from_utf8_lossy(key);
    let ttl_str = ttl_ms.to_string();
    let mut restore_args: Vec<&str> = vec!["RESTORE", &key_str, &ttl_str];

    // The dump data needs to be sent as binary
    // For simplicity in the RESP protocol, we'll encode it
    let dump_hex = hex_encode(&dump_data);
    restore_args.push(&dump_hex);

    if replace {
        restore_args.push("REPLACE");
    }

    let response = send_command(&mut stream, &restore_args, timeout).await?;

    match &response {
        RespValue::SimpleString(s) if s == "OK" => {
            // Remove key from local storage if not COPY
            if !copy {
                let _ = engine.del(key).await;
            }
            Ok(RespValue::SimpleString("OK".to_string()))
        }
        RespValue::Error(e) => Err(e.clone()),
        _ => Ok(RespValue::SimpleString("OK".to_string())),
    }
}

/// MIGRATE command for multiple keys
#[allow(clippy::too_many_arguments)]
pub async fn migrate_keys(
    engine: &StorageEngine,
    host: &str,
    port: u16,
    keys: &[Vec<u8>],
    db: u16,
    timeout_ms: u64,
    copy: bool,
    replace: bool,
    auth: Option<&str>,
    auth2: Option<(&str, &str)>,
) -> Result<RespValue, String> {
    if keys.is_empty() {
        return Ok(RespValue::SimpleString("NOKEY".to_string()));
    }

    if keys.len() == 1 {
        return migrate_key(
            engine, host, port, &keys[0], db, timeout_ms, copy, replace, auth, auth2,
        )
        .await;
    }

    // For multiple keys, migrate each one
    // In a real implementation, we'd batch these in a single connection
    let mut errors = Vec::new();
    let mut success_count = 0;

    for key in keys {
        match migrate_key(
            engine, host, port, key, db, timeout_ms, copy, replace, auth, auth2,
        )
        .await
        {
            Ok(_) => success_count += 1,
            Err(e) => errors.push(e),
        }
    }

    if errors.is_empty() {
        Ok(RespValue::SimpleString("OK".to_string()))
    } else if success_count > 0 {
        // Partial success
        Ok(RespValue::SimpleString("OK".to_string()))
    } else {
        Err(errors.join("; "))
    }
}

/// Send a RESP command to a stream and read the response
async fn send_command(
    stream: &mut TcpStream,
    args: &[&str],
    timeout: Duration,
) -> Result<RespValue, String> {
    // Build RESP array
    let mut cmd = format!("*{}\r\n", args.len());
    for arg in args {
        cmd.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }

    tokio::time::timeout(timeout, stream.write_all(cmd.as_bytes()))
        .await
        .map_err(|_| "IOERR timeout sending command".to_string())?
        .map_err(|e| format!("IOERR {}", e))?;

    // Read response (simplified - just read the first line)
    let mut buf = vec![0u8; 4096];
    let n = tokio::time::timeout(timeout, stream.read(&mut buf))
        .await
        .map_err(|_| "IOERR timeout reading response".to_string())?
        .map_err(|e| format!("IOERR {}", e))?;

    if n == 0 {
        return Err("IOERR connection closed".to_string());
    }

    let response = String::from_utf8_lossy(&buf[..n]);

    if response.starts_with('+') {
        let s = response
            .trim_start_matches('+')
            .trim_end_matches("\r\n")
            .to_string();
        Ok(RespValue::SimpleString(s))
    } else if response.starts_with('-') {
        let s = response
            .trim_start_matches('-')
            .trim_end_matches("\r\n")
            .to_string();
        Ok(RespValue::Error(s))
    } else if response.starts_with(':') {
        let s = response
            .trim_start_matches(':')
            .trim_end_matches("\r\n")
            .to_string();
        let n: i64 = s.parse().unwrap_or(0);
        Ok(RespValue::Integer(n))
    } else {
        Ok(RespValue::SimpleString(response.to_string()))
    }
}

/// Hex-encode bytes
fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0x00, 0xff, 0x42]), "00ff42");
        assert_eq!(hex_encode(&[]), "");
        assert_eq!(hex_encode(b"AB"), "4142");
    }
}
