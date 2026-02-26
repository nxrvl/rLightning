use std::time::{Duration, Instant};

use crate::command::error::{CommandError, CommandResult};
use crate::command::types::blocking::BlockingManager;
use crate::command::utils::bytes_to_string;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::error::StorageError;
use crate::storage::item::RedisDataType;
use crate::storage::stream::{ConsumerGroup, PendingEntry, StreamData, StreamEntryId};

/// Helper: get stream data for multi-key read operations (XREAD, XREADGROUP).
/// Uses engine.get() since atomic_modify is per-key and we need cross-key reads.
async fn get_stream_data(engine: &StorageEngine, key: &[u8]) -> Result<Option<StreamData>, CommandError> {
    match engine.get(key).await.map_err(|e| CommandError::StorageError(e.to_string()))? {
        Some(data) => {
            let key_type = engine.get_type(key).await.map_err(|e| CommandError::StorageError(e.to_string()))?;
            if key_type != "stream" {
                return Err(CommandError::WrongType);
            }
            let stream: StreamData = bincode::deserialize(&data)
                .map_err(|_| CommandError::WrongType)?;
            Ok(Some(stream))
        }
        None => Ok(None),
    }
}

/// Helper: format a stream entry as a RESP array [id, [field, value, ...]]
fn entry_to_resp(entry: &crate::storage::stream::StreamEntry) -> RespValue {
    let id = RespValue::BulkString(Some(entry.id.to_string().into_bytes()));
    let mut fields = Vec::new();
    for (field, value) in &entry.fields {
        fields.push(RespValue::BulkString(Some(field.clone())));
        fields.push(RespValue::BulkString(Some(value.clone())));
    }
    RespValue::Array(Some(vec![id, RespValue::Array(Some(fields))]))
}

/// Helper: get current time in ms
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ============================================================
// XADD
// ============================================================

/// Redis XADD command - Appends an entry to a stream
/// XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
pub async fn xadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let mut idx = 1;
    let mut nomkstream = false;
    let mut maxlen: Option<(usize, bool)> = None; // (threshold, approximate)
    let mut minid: Option<(StreamEntryId, bool)> = None;

    // Parse options
    loop {
        if idx >= args.len() {
            return Err(CommandError::WrongNumberOfArguments);
        }
        let arg = bytes_to_string(&args[idx])?.to_uppercase();
        match arg.as_str() {
            "NOMKSTREAM" => {
                nomkstream = true;
                idx += 1;
            }
            "MAXLEN" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let next = bytes_to_string(&args[idx])?;
                let (approximate, threshold_str) = if next == "~" || next == "=" {
                    let approx = next == "~";
                    idx += 1;
                    if idx >= args.len() {
                        return Err(CommandError::WrongNumberOfArguments);
                    }
                    (approx, bytes_to_string(&args[idx])?)
                } else {
                    (false, next)
                };
                let threshold = threshold_str.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                maxlen = Some((threshold, approximate));
                idx += 1;
            }
            "MINID" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let next = bytes_to_string(&args[idx])?;
                let (approximate, id_str) = if next == "~" || next == "=" {
                    let approx = next == "~";
                    idx += 1;
                    if idx >= args.len() {
                        return Err(CommandError::WrongNumberOfArguments);
                    }
                    (approx, bytes_to_string(&args[idx])?)
                } else {
                    (false, next)
                };
                let min_entry_id = StreamEntryId::parse_range_start(&id_str)
                    .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID for MINID".to_string()))?;
                minid = Some((min_entry_id, approximate));
                idx += 1;
            }
            _ => break, // This should be the ID
        }
    }

    // Parse the entry ID string (resolve against stream state inside closure)
    if idx >= args.len() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let id_str = bytes_to_string(&args[idx])?;
    idx += 1;

    // Parse field-value pairs
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let mut fields = Vec::new();
    for chunk in remaining.chunks(2) {
        fields.push((chunk[0].clone(), chunk[1].clone()));
    }

    // Pre-parse the ID string to extract ms/seq components (but resolve against stream inside closure)
    let parsed_id = if id_str == "*" {
        (None, None) // fully auto-generated
    } else {
        let parts: Vec<&str> = id_str.splitn(2, '-').collect();
        let ms = parts[0].parse::<u64>().map_err(|_| {
            CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string())
        })?;
        if parts.len() > 1 && parts[1] == "*" {
            (Some(ms), None) // explicit ms, auto seq
        } else {
            let seq = if parts.len() > 1 {
                parts[1].parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string())
                })?
            } else {
                0
            };
            (Some(ms), Some(seq))
        }
    };

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => {
                if nomkstream {
                    return Ok((None, RespValue::BulkString(None)));
                }
                StreamData::new()
            }
        };

        // Generate/resolve the entry ID using stream state
        let entry_id = stream.generate_id(parsed_id.0, parsed_id.1)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;

        // Add the entry
        let added_id = stream.add_entry(entry_id, fields.clone());

        // Apply trimming
        if let Some((threshold, approximate)) = maxlen {
            stream.trim_maxlen(threshold, approximate);
        }
        if let Some((ref min_entry_id, approximate)) = minid {
            stream.trim_minid(min_entry_id, approximate);
        }

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::BulkString(Some(added_id.to_string().into_bytes()))))
    })?;

    Ok(result)
}

// ============================================================
// XLEN
// ============================================================

/// Redis XLEN command - Returns the number of entries in a stream
pub async fn xlen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Ok((None, RespValue::Integer(0))),
        };
        let len = stream.len() as i64;
        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::Integer(len)))
    })?;

    Ok(result)
}

// ============================================================
// XRANGE / XREVRANGE
// ============================================================

/// Redis XRANGE command - Return entries in a range
/// XRANGE key start end [COUNT count]
pub async fn xrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let start_str = bytes_to_string(&args[1])?;
    let end_str = bytes_to_string(&args[2])?;

    let start = StreamEntryId::parse_range_start(&start_str)
        .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;
    let end = StreamEntryId::parse_range_end(&end_str)
        .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;

    let mut count = None;
    if args.len() >= 5 {
        let count_kw = bytes_to_string(&args[3])?.to_uppercase();
        if count_kw == "COUNT" {
            count = Some(bytes_to_string(&args[4])?.parse::<usize>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?);
        }
    }

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Ok((None, RespValue::Array(Some(vec![])))),
        };

        let entries = stream.range(&start, &end, count);
        let resp: Vec<RespValue> = entries.iter().map(|e| entry_to_resp(e)).collect();
        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::Array(Some(resp))))
    })?;

    Ok(result)
}

/// Redis XREVRANGE command - Return entries in reverse order
/// XREVRANGE key end start [COUNT count]
pub async fn xrevrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let end_str = bytes_to_string(&args[1])?;
    let start_str = bytes_to_string(&args[2])?;

    let end = StreamEntryId::parse_range_end(&end_str)
        .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;
    let start = StreamEntryId::parse_range_start(&start_str)
        .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;

    let mut count = None;
    if args.len() >= 5 {
        let count_kw = bytes_to_string(&args[3])?.to_uppercase();
        if count_kw == "COUNT" {
            count = Some(bytes_to_string(&args[4])?.parse::<usize>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?);
        }
    }

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Ok((None, RespValue::Array(Some(vec![])))),
        };

        let entries = stream.rev_range(&end, &start, count);
        let resp: Vec<RespValue> = entries.iter().map(|e| entry_to_resp(e)).collect();
        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::Array(Some(resp))))
    })?;

    Ok(result)
}

// ============================================================
// XREAD
// ============================================================

/// Redis XREAD command - Read entries from one or more streams
/// XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
pub async fn xread(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let mut idx = 0;
    let mut count: Option<usize> = None;
    let mut block: Option<f64> = None;

    // Parse options
    loop {
        if idx >= args.len() {
            return Err(CommandError::WrongNumberOfArguments);
        }
        let arg = bytes_to_string(&args[idx])?.to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                count = Some(bytes_to_string(&args[idx])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
                idx += 1;
            }
            "BLOCK" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let ms = bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("timeout is not an integer or out of range".to_string())
                })?;
                block = Some(ms as f64 / 1000.0);
                idx += 1;
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unrecognized XREAD option '{}'", arg
                )));
            }
        }
    }

    // After STREAMS keyword, we have keys followed by IDs
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let half = remaining.len() / 2;
    let keys = &remaining[..half];
    let ids = &remaining[half..];

    // Parse stream IDs
    let mut parsed_ids: Vec<StreamEntryId> = Vec::new();
    for (i, id_bytes) in ids.iter().enumerate() {
        let id_str = bytes_to_string(id_bytes)?;
        if id_str == "$" {
            // $ means "only new entries from now"
            let stream = get_stream_data(engine, &keys[i]).await?;
            let last_id = stream.map(|s| s.last_id.clone()).unwrap_or_else(|| StreamEntryId::new(0, 0));
            parsed_ids.push(last_id);
        } else {
            let id = StreamEntryId::parse_range_start(&id_str)
                .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;
            parsed_ids.push(id);
        }
    }

    // Try reading
    let result = do_xread(engine, keys, &parsed_ids, count).await?;
    if result.is_some() || block.is_none() {
        return Ok(result.unwrap_or(RespValue::BulkString(None)));
    }

    // Blocking mode
    let timeout_secs = block.unwrap();
    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 means block forever
    };

    loop {
        let receivers: Vec<_> = keys.iter()
            .map(|k| blocking_mgr.subscribe(k))
            .collect();

        // Try again
        let result = do_xread(engine, keys, &parsed_ids, count).await?;
        if result.is_some() {
            return Ok(result.unwrap());
        }

        // Wait for notification or timeout
        let futs: Vec<_> = receivers.into_iter()
            .map(|mut rx| Box::pin(async move { let _ = rx.changed().await; }))
            .collect();

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::BulkString(None));
            }
            if tokio::time::timeout(remaining, futures::future::select_all(futs)).await.is_err() {
                return Ok(RespValue::BulkString(None));
            }
        } else {
            let _ = futures::future::select_all(futs).await;
        }
    }
}

/// Internal helper to do a non-blocking XREAD (multi-key, uses get_stream_data)
async fn do_xread(
    engine: &StorageEngine,
    keys: &[Vec<u8>],
    after_ids: &[StreamEntryId],
    count: Option<usize>,
) -> Result<Option<RespValue>, CommandError> {
    let mut results = Vec::new();
    let mut has_data = false;

    for (i, key) in keys.iter().enumerate() {
        let stream = get_stream_data(engine, key).await?;
        let entries = match &stream {
            Some(s) => s.read_after(&after_ids[i], count),
            None => vec![],
        };
        if !entries.is_empty() {
            has_data = true;
        }
        let key_name = RespValue::BulkString(Some(key.clone()));
        let entry_list: Vec<RespValue> = entries.iter().map(|e| entry_to_resp(e)).collect();
        results.push(RespValue::Array(Some(vec![
            key_name,
            RespValue::Array(Some(entry_list)),
        ])));
    }

    if has_data {
        Ok(Some(RespValue::Array(Some(results))))
    } else {
        Ok(None)
    }
}

// ============================================================
// XTRIM
// ============================================================

/// Redis XTRIM command - Trim a stream to a given number of items
/// XTRIM key MAXLEN|MINID [=|~] threshold
pub async fn xtrim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let strategy = bytes_to_string(&args[1])?.to_uppercase();
    let mut idx = 2;

    let approximate = if idx < args.len() {
        let next = bytes_to_string(&args[idx])?;
        if next == "~" || next == "=" {
            idx += 1;
            next == "~"
        } else {
            false
        }
    } else {
        false
    };

    if idx >= args.len() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let threshold_str = bytes_to_string(&args[idx])?;

    // Pre-parse strategy-specific arguments outside the closure
    enum TrimStrategy {
        MaxLen(usize, bool),
        MinId(StreamEntryId, bool),
    }

    let trim_strategy = match strategy.as_str() {
        "MAXLEN" => {
            let maxlen = threshold_str.parse::<usize>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            TrimStrategy::MaxLen(maxlen, approximate)
        }
        "MINID" => {
            let min_id = StreamEntryId::parse_range_start(&threshold_str)
                .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;
            TrimStrategy::MinId(min_id, approximate)
        }
        _ => {
            return Err(CommandError::InvalidArgument(
                "syntax error, MAXLEN or MINID must be specified".to_string(),
            ));
        }
    };

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Ok((None, RespValue::Integer(0))),
        };

        let removed = match &trim_strategy {
            TrimStrategy::MaxLen(maxlen, approx) => stream.trim_maxlen(*maxlen, *approx),
            TrimStrategy::MinId(min_id, approx) => stream.trim_minid(min_id, *approx),
        };

        if removed > 0 {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(removed as i64)))
        } else {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(0)))
        }
    })?;

    Ok(result)
}

// ============================================================
// XDEL
// ============================================================

/// Redis XDEL command - Remove entries from a stream by ID
/// XDEL key id [id ...]
pub async fn xdel(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Parse IDs outside the closure
    let mut ids = Vec::new();
    for id_bytes in &args[1..] {
        let id_str = bytes_to_string(id_bytes)?;
        let id = StreamEntryId::parse_range_start(&id_str)
            .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;
        ids.push(id);
    }

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Ok((None, RespValue::Integer(0))),
        };

        let deleted = stream.delete_entries(&ids);
        if deleted > 0 {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(deleted as i64)))
        } else {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(0)))
        }
    })?;

    Ok(result)
}

// ============================================================
// XINFO
// ============================================================

/// Redis XINFO command - Get info about a stream, consumer group, or consumer
/// XINFO STREAM key [FULL [COUNT count]]
/// XINFO GROUPS key
/// XINFO CONSUMERS key groupname
pub async fn xinfo(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcommand = bytes_to_string(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "STREAM" => xinfo_stream(engine, &args[1..]).await,
        "GROUPS" => xinfo_groups(engine, &args[1..]).await,
        "CONSUMERS" => xinfo_consumers(engine, &args[1..]).await,
        "HELP" => {
            Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"XINFO STREAM <key> [FULL [COUNT <count>]]".to_vec())),
                RespValue::BulkString(Some(b"XINFO GROUPS <key>".to_vec())),
                RespValue::BulkString(Some(b"XINFO CONSUMERS <key> <groupname>".to_vec())),
            ])))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown XINFO subcommand '{}'", subcommand
        ))),
    }
}

async fn xinfo_stream(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let first_entry = stream.entries.values().next().map(|e| entry_to_resp(e))
            .unwrap_or(RespValue::BulkString(None));
        let last_entry = stream.entries.values().next_back().map(|e| entry_to_resp(e))
            .unwrap_or(RespValue::BulkString(None));

        let resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"length".to_vec())),
            RespValue::Integer(stream.len() as i64),
            RespValue::BulkString(Some(b"radix-tree-keys".to_vec())),
            RespValue::Integer(stream.len() as i64),
            RespValue::BulkString(Some(b"radix-tree-nodes".to_vec())),
            RespValue::Integer((stream.len() + 1) as i64),
            RespValue::BulkString(Some(b"last-generated-id".to_vec())),
            RespValue::BulkString(Some(stream.last_id.to_string().into_bytes())),
            RespValue::BulkString(Some(b"max-deleted-entry-id".to_vec())),
            RespValue::BulkString(Some(
                stream.max_deleted_entry_id.as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "0-0".to_string())
                    .into_bytes()
            )),
            RespValue::BulkString(Some(b"entries-added".to_vec())),
            RespValue::Integer(stream.entries_added as i64),
            RespValue::BulkString(Some(b"recorded-first-entry-id".to_vec())),
            RespValue::BulkString(Some(
                stream.first_entry_id.as_ref()
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "0-0".to_string())
                    .into_bytes()
            )),
            RespValue::BulkString(Some(b"groups".to_vec())),
            RespValue::Integer(stream.groups.len() as i64),
            RespValue::BulkString(Some(b"first-entry".to_vec())),
            first_entry,
            RespValue::BulkString(Some(b"last-entry".to_vec())),
            last_entry,
        ]));

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), resp))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(e) => Err(e.into()),
    }
}

async fn xinfo_groups(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let mut groups = Vec::new();
        for group in stream.groups.values() {
            groups.push(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"name".to_vec())),
                RespValue::BulkString(Some(group.name.as_bytes().to_vec())),
                RespValue::BulkString(Some(b"consumers".to_vec())),
                RespValue::Integer(group.consumers.len() as i64),
                RespValue::BulkString(Some(b"pending".to_vec())),
                RespValue::Integer(group.pel.len() as i64),
                RespValue::BulkString(Some(b"last-delivered-id".to_vec())),
                RespValue::BulkString(Some(group.last_delivered_id.to_string().into_bytes())),
                RespValue::BulkString(Some(b"entries-read".to_vec())),
                group.entries_read.map(|n| RespValue::Integer(n as i64)).unwrap_or(RespValue::BulkString(None)),
                RespValue::BulkString(Some(b"lag".to_vec())),
                {
                    let lag = group.entries_read.map(|er| {
                        (stream.entries_added as i64 - er as i64).max(0)
                    });
                    lag.map(RespValue::Integer).unwrap_or(RespValue::BulkString(None))
                },
            ])));
        }

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::Array(Some(groups))))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(e) => Err(e.into()),
    }
}

async fn xinfo_consumers(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let key_str = String::from_utf8_lossy(key).to_string();

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let group = stream.groups.get(&group_name)
            .ok_or_else(|| StorageError::InternalError(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                group_name, key_str
            )))?;

        let mut consumers = Vec::new();
        for consumer in group.consumers.values() {
            let idle = now_ms().saturating_sub(consumer.seen_time);
            consumers.push(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"name".to_vec())),
                RespValue::BulkString(Some(consumer.name.as_bytes().to_vec())),
                RespValue::BulkString(Some(b"pending".to_vec())),
                RespValue::Integer(consumer.pending.len() as i64),
                RespValue::BulkString(Some(b"idle".to_vec())),
                RespValue::Integer(idle as i64),
            ])));
        }

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::Array(Some(consumers))))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

// ============================================================
// XGROUP
// ============================================================

/// Redis XGROUP command - Manage consumer groups
/// XGROUP CREATE key groupname id|$ [MKSTREAM] [ENTRIESREAD entries-read]
/// XGROUP SETID key groupname id|$ [ENTRIESREAD entries-read]
/// XGROUP DESTROY key groupname
/// XGROUP DELCONSUMER key groupname consumername
/// XGROUP CREATECONSUMER key groupname consumername
pub async fn xgroup(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcommand = bytes_to_string(&args[0])?.to_uppercase();

    match subcommand.as_str() {
        "CREATE" => xgroup_create(engine, &args[1..]).await,
        "SETID" => xgroup_setid(engine, &args[1..]).await,
        "DESTROY" => xgroup_destroy(engine, &args[1..]).await,
        "DELCONSUMER" => xgroup_delconsumer(engine, &args[1..]).await,
        "CREATECONSUMER" => xgroup_createconsumer(engine, &args[1..]).await,
        "HELP" => {
            Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"XGROUP CREATE <key> <groupname> <id|$> [MKSTREAM] [ENTRIESREAD <n>]".to_vec())),
                RespValue::BulkString(Some(b"XGROUP SETID <key> <groupname> <id|$> [ENTRIESREAD <n>]".to_vec())),
                RespValue::BulkString(Some(b"XGROUP DESTROY <key> <groupname>".to_vec())),
                RespValue::BulkString(Some(b"XGROUP DELCONSUMER <key> <groupname> <consumername>".to_vec())),
                RespValue::BulkString(Some(b"XGROUP CREATECONSUMER <key> <groupname> <consumername>".to_vec())),
            ])))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown XGROUP subcommand '{}'. Try XGROUP HELP.", subcommand
        ))),
    }
}

async fn xgroup_create(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let id_str = bytes_to_string(&args[2])?;

    let mut mkstream = false;
    let mut entries_read: Option<u64> = None;
    let mut idx = 3;
    while idx < args.len() {
        let opt = bytes_to_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "MKSTREAM" => {
                mkstream = true;
                idx += 1;
            }
            "ENTRIESREAD" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                entries_read = Some(bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
                idx += 1;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unknown option '{}'", opt
                )));
            }
        }
    }

    // Pre-parse the ID (resolve $ inside closure where we have stream state)
    let parsed_id = if id_str == "$" {
        None // resolve inside closure
    } else if id_str == "0" || id_str == "0-0" {
        Some(StreamEntryId::new(0, 0))
    } else {
        Some(StreamEntryId::parse_range_start(&id_str)
            .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?)
    };

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => {
                if mkstream {
                    StreamData::new()
                } else {
                    return Err(StorageError::InternalError(
                        "The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.".to_string()
                    ));
                }
            }
        };

        if stream.groups.contains_key(&group_name) {
            return Err(StorageError::InternalError(
                "BUSYGROUP Consumer Group name already exists".to_string()
            ));
        }

        let last_id = match &parsed_id {
            Some(id) => id.clone(),
            None => stream.last_id.clone(), // $ resolves to current last_id
        };

        let mut group = ConsumerGroup::new(group_name.clone(), last_id);
        group.entries_read = entries_read;
        stream.groups.insert(group_name.clone(), group);

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::SimpleString("OK".to_string())))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("The XGROUP subcommand") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("BUSYGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

async fn xgroup_setid(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let id_str = bytes_to_string(&args[2])?;
    let key_str = String::from_utf8_lossy(key).to_string();

    let mut entries_read: Option<u64> = None;
    let mut idx = 3;
    while idx < args.len() {
        let opt = bytes_to_string(&args[idx])?.to_uppercase();
        if opt == "ENTRIESREAD" {
            idx += 1;
            if idx >= args.len() {
                return Err(CommandError::WrongNumberOfArguments);
            }
            entries_read = Some(bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?);
        }
        idx += 1;
    }

    // Pre-parse the ID (resolve $ inside closure)
    let parsed_id = if id_str == "$" {
        None
    } else {
        Some(StreamEntryId::parse_range_start(&id_str)
            .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?)
    };

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let group = stream.groups.get_mut(&group_name)
            .ok_or_else(|| StorageError::InternalError(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                group_name, key_str
            )))?;

        group.last_delivered_id = match &parsed_id {
            Some(id) => id.clone(),
            None => stream.last_id.clone(), // $ resolves to current last_id
        };
        if let Some(er) = entries_read {
            group.entries_read = Some(er);
        }

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::SimpleString("OK".to_string())))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

async fn xgroup_destroy(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Ok((None, RespValue::Integer(0))),
        };

        let removed = stream.groups.remove(&group_name).is_some();
        if removed {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(1)))
        } else {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(0)))
        }
    })?;

    Ok(result)
}

async fn xgroup_delconsumer(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let consumer_name = bytes_to_string(&args[2])?;
    let key_str = String::from_utf8_lossy(key).to_string();

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let group = stream.groups.get_mut(&group_name)
            .ok_or_else(|| StorageError::InternalError(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                group_name, key_str
            )))?;

        let pending_count = match group.consumers.remove(&consumer_name) {
            Some(consumer) => {
                let count = consumer.pending.len();
                for id in &consumer.pending {
                    group.pel.remove(id);
                }
                count as i64
            }
            None => 0,
        };

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::Integer(pending_count)))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

async fn xgroup_createconsumer(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let consumer_name = bytes_to_string(&args[2])?;
    let key_str = String::from_utf8_lossy(key).to_string();

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let group = stream.groups.get_mut(&group_name)
            .ok_or_else(|| StorageError::InternalError(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                group_name, key_str
            )))?;

        let created = if group.consumers.contains_key(&consumer_name) {
            0
        } else {
            group.get_or_create_consumer(&consumer_name);
            1
        };

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), RespValue::Integer(created)))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

// ============================================================
// XREADGROUP
// ============================================================

/// Redis XREADGROUP command - Read entries from a stream via a consumer group
/// XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
pub async fn xreadgroup(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 6 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let mut idx = 0;

    // Parse GROUP keyword
    let group_kw = bytes_to_string(&args[idx])?.to_uppercase();
    if group_kw != "GROUP" {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }
    idx += 1;

    let group_name = bytes_to_string(&args[idx])?;
    idx += 1;
    let consumer_name = bytes_to_string(&args[idx])?;
    idx += 1;

    let mut count: Option<usize> = None;
    let mut block: Option<f64> = None;
    let mut noack = false;

    // Parse options
    loop {
        if idx >= args.len() {
            return Err(CommandError::WrongNumberOfArguments);
        }
        let arg = bytes_to_string(&args[idx])?.to_uppercase();
        match arg.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                count = Some(bytes_to_string(&args[idx])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
                idx += 1;
            }
            "BLOCK" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let ms = bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("timeout is not an integer or out of range".to_string())
                })?;
                block = Some(ms as f64 / 1000.0);
                idx += 1;
            }
            "NOACK" => {
                noack = true;
                idx += 1;
            }
            "STREAMS" => {
                idx += 1;
                break;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unrecognized XREADGROUP option '{}'", arg
                )));
            }
        }
    }

    // Parse keys and IDs
    let remaining = &args[idx..];
    if remaining.is_empty() || remaining.len() % 2 != 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let half = remaining.len() / 2;
    let keys = &remaining[..half];
    let ids = &remaining[half..];

    // Try reading
    let result = do_xreadgroup(engine, keys, ids, &group_name, &consumer_name, count, noack).await?;
    if result.is_some() || block.is_none() {
        return Ok(result.unwrap_or(RespValue::BulkString(None)));
    }

    // Blocking mode
    let timeout_secs = block.unwrap();
    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };

    loop {
        let receivers: Vec<_> = keys.iter()
            .map(|k| blocking_mgr.subscribe(k))
            .collect();

        let result = do_xreadgroup(engine, keys, ids, &group_name, &consumer_name, count, noack).await?;
        if result.is_some() {
            return Ok(result.unwrap());
        }

        let futs: Vec<_> = receivers.into_iter()
            .map(|mut rx| Box::pin(async move { let _ = rx.changed().await; }))
            .collect();

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::BulkString(None));
            }
            if tokio::time::timeout(remaining, futures::future::select_all(futs)).await.is_err() {
                return Ok(RespValue::BulkString(None));
            }
        } else {
            let _ = futures::future::select_all(futs).await;
        }
    }
}

/// Internal helper for XREADGROUP non-blocking read.
/// Uses atomic_modify per-key since XREADGROUP modifies stream state (PEL, consumer tracking).
async fn do_xreadgroup(
    engine: &StorageEngine,
    keys: &[Vec<u8>],
    ids: &[Vec<u8>],
    group_name: &str,
    consumer_name: &str,
    count: Option<usize>,
    noack: bool,
) -> Result<Option<RespValue>, CommandError> {
    let mut results = Vec::new();
    let mut has_data = false;

    for (i, key) in keys.iter().enumerate() {
        let id_str = bytes_to_string(&ids[i])?;
        let key_str = String::from_utf8_lossy(key).to_string();
        let group_name_owned = group_name.to_string();
        let consumer_name_owned = consumer_name.to_string();

        let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
            let mut stream = match existing {
                Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                    .map_err(|_| StorageError::WrongType)?,
                None => {
                    return Ok((None, (false, RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::Array(Some(vec![])),
                    ])))));
                }
            };

            let (stream_has_data, entry_resp) = if id_str == ">" {
                // Read new entries not yet delivered to this group
                let group = stream.groups.get_mut(&group_name_owned)
                    .ok_or_else(|| StorageError::InternalError(format!(
                        "NOGROUP No such consumer group '{}' for key name '{}'",
                        group_name_owned, key_str
                    )))?;

                let entries: Vec<_> = {
                    let iter = stream.entries.range(
                        (std::ops::Bound::Excluded(group.last_delivered_id.clone()),
                         std::ops::Bound::Unbounded)
                    );
                    let mut collected = Vec::new();
                    for (_, entry) in iter {
                        collected.push(entry.clone());
                        if let Some(max) = count {
                            if collected.len() >= max {
                                break;
                            }
                        }
                    }
                    collected
                };

                if !entries.is_empty() {
                    let now = now_ms();

                    // Insert PEL entries and collect pending IDs
                    let mut pending_ids = Vec::new();
                    if !noack {
                        for entry in &entries {
                            pending_ids.push(entry.id.clone());
                            group.pel.insert(entry.id.clone(), PendingEntry {
                                id: entry.id.clone(),
                                consumer: consumer_name_owned.clone(),
                                delivery_time: now,
                                delivery_count: 1,
                            });
                        }
                    }

                    // Update consumer
                    let consumer = group.get_or_create_consumer(&consumer_name_owned);
                    consumer.seen_time = now;
                    for pid in pending_ids {
                        consumer.pending.push(pid);
                    }

                    // Update last delivered ID
                    if let Some(last) = entries.last() {
                        group.last_delivered_id = last.id.clone();
                    }
                    if let Some(er) = group.entries_read.as_mut() {
                        *er += entries.len() as u64;
                    }

                    let entry_list: Vec<RespValue> = entries.iter().map(|e| entry_to_resp(e)).collect();
                    (true, RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::Array(Some(entry_list)),
                    ])))
                } else {
                    let entry_list: Vec<RespValue> = Vec::new();
                    (false, RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::Array(Some(entry_list)),
                    ])))
                }
            } else {
                // Read pending entries for this consumer
                let group = stream.groups.get_mut(&group_name_owned)
                    .ok_or_else(|| StorageError::InternalError(format!(
                        "NOGROUP No such consumer group '{}' for key name '{}'",
                        group_name_owned, key_str
                    )))?;

                let start = if id_str == "0" || id_str == "0-0" {
                    StreamEntryId::min()
                } else {
                    StreamEntryId::parse_range_start(&id_str)
                        .ok_or_else(|| StorageError::InternalError("Invalid stream ID".to_string()))?
                };

                let consumer = group.get_or_create_consumer(&consumer_name_owned);
                consumer.seen_time = now_ms();

                let mut pending_entries = Vec::new();
                for pending_id in &consumer.pending {
                    if *pending_id >= start {
                        if let Some(entry) = stream.entries.get(pending_id) {
                            pending_entries.push(entry.clone());
                        }
                        if let Some(max) = count {
                            if pending_entries.len() >= max {
                                break;
                            }
                        }
                    }
                }

                let has = !pending_entries.is_empty();
                let entry_list: Vec<RespValue> = pending_entries.iter().map(|e| entry_to_resp(e)).collect();
                (has, RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::Array(Some(entry_list)),
                ])))
            };

            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), (stream_has_data, entry_resp)))
        });

        match result {
            Ok((stream_has_data, entry_resp)) => {
                if stream_has_data {
                    has_data = true;
                }
                results.push(entry_resp);
            }
            Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
                return Err(CommandError::InvalidArgument(msg));
            }
            Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "Invalid stream ID" => {
                return Err(CommandError::InvalidArgument(msg));
            }
            Err(e) => return Err(e.into()),
        }
    }

    if has_data {
        Ok(Some(RespValue::Array(Some(results))))
    } else {
        Ok(None)
    }
}

// ============================================================
// XACK
// ============================================================

/// Redis XACK command - Acknowledge one or more messages as processed
/// XACK key group id [id ...]
pub async fn xack(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;

    // Parse IDs outside the closure
    let mut ids = Vec::new();
    for id_bytes in &args[2..] {
        let id_str = bytes_to_string(id_bytes)?;
        let id = StreamEntryId::parse_range_start(&id_str)
            .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;
        ids.push(id);
    }

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Ok((None, RespValue::Integer(0))),
        };

        let group = match stream.groups.get_mut(&group_name) {
            Some(g) => g,
            None => {
                let serialized = bincode::serialize(&stream)
                    .map_err(|e| StorageError::InternalError(e.to_string()))?;
                return Ok((Some(serialized), RespValue::Integer(0)));
            }
        };

        let mut acked = 0i64;
        for id in &ids {
            if group.pel.remove(id).is_some() {
                acked += 1;
                for consumer in group.consumers.values_mut() {
                    consumer.pending.retain(|pid| pid != id);
                }
            }
        }

        if acked > 0 {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(acked)))
        } else {
            let serialized = bincode::serialize(&stream)
                .map_err(|e| StorageError::InternalError(e.to_string()))?;
            Ok((Some(serialized), RespValue::Integer(0)))
        }
    })?;

    Ok(result)
}

// ============================================================
// XPENDING
// ============================================================

/// Redis XPENDING command - Get info about pending messages
/// XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
pub async fn xpending(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let key_str = String::from_utf8_lossy(key).to_string();

    // Parse extended form arguments outside the closure
    let extended_args = if args.len() > 2 {
        let mut idx = 2;
        let mut min_idle: Option<u64> = None;

        if idx < args.len() {
            let maybe_idle = bytes_to_string(&args[idx])?.to_uppercase();
            if maybe_idle == "IDLE" {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                min_idle = Some(bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
                idx += 1;
            }
        }

        if idx + 2 >= args.len() {
            return Err(CommandError::WrongNumberOfArguments);
        }

        let start_str = bytes_to_string(&args[idx])?;
        let end_str = bytes_to_string(&args[idx + 1])?;
        let count_str = bytes_to_string(&args[idx + 2])?;
        idx += 3;

        let start = StreamEntryId::parse_range_start(&start_str)
            .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID".to_string()))?;
        let end = StreamEntryId::parse_range_end(&end_str)
            .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID".to_string()))?;
        let count = count_str.parse::<usize>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

        let consumer_filter = if idx < args.len() {
            Some(bytes_to_string(&args[idx])?)
        } else {
            None
        };

        Some((min_idle, start, end, count, consumer_filter))
    } else {
        None
    };

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let group = stream.groups.get(&group_name)
            .ok_or_else(|| StorageError::InternalError(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                group_name, key_str
            )))?;

        let resp = if let Some((min_idle, ref start, ref end, count, ref consumer_filter)) = extended_args {
            // Extended form
            let now = now_ms();
            let mut result = Vec::new();
            for (id, pe) in group.pel.range(start.clone()..=end.clone()) {
                if let Some(cf) = consumer_filter {
                    if pe.consumer != *cf {
                        continue;
                    }
                }
                if let Some(mi) = min_idle {
                    let idle = now.saturating_sub(pe.delivery_time);
                    if idle < mi {
                        continue;
                    }
                }
                result.push(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(id.to_string().into_bytes())),
                    RespValue::BulkString(Some(pe.consumer.as_bytes().to_vec())),
                    RespValue::Integer(now.saturating_sub(pe.delivery_time) as i64),
                    RespValue::Integer(pe.delivery_count as i64),
                ])));
                if result.len() >= count {
                    break;
                }
            }
            RespValue::Array(Some(result))
        } else {
            // Summary form
            let pel_count = group.pel.len() as i64;
            if pel_count == 0 {
                RespValue::Array(Some(vec![
                    RespValue::Integer(0),
                    RespValue::BulkString(None),
                    RespValue::BulkString(None),
                    RespValue::BulkString(None),
                ]))
            } else {
                let min_id = group.pel.keys().next().unwrap();
                let max_id = group.pel.keys().next_back().unwrap();

                let mut consumer_counts: std::collections::HashMap<&str, i64> = std::collections::HashMap::new();
                for pe in group.pel.values() {
                    *consumer_counts.entry(&pe.consumer).or_insert(0) += 1;
                }
                let consumer_list: Vec<RespValue> = consumer_counts.iter().map(|(name, count)| {
                    RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(name.as_bytes().to_vec())),
                        RespValue::BulkString(Some(count.to_string().into_bytes())),
                    ]))
                }).collect();

                RespValue::Array(Some(vec![
                    RespValue::Integer(pel_count),
                    RespValue::BulkString(Some(min_id.to_string().into_bytes())),
                    RespValue::BulkString(Some(max_id.to_string().into_bytes())),
                    RespValue::Array(Some(consumer_list)),
                ]))
            }
        };

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), resp))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

// ============================================================
// XCLAIM
// ============================================================

/// Redis XCLAIM command - Claim ownership of pending messages
/// XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID]
pub async fn xclaim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let new_consumer = bytes_to_string(&args[2])?;
    let min_idle_time = bytes_to_string(&args[3])?.parse::<u64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    let mut idx = 4;
    let mut entry_ids = Vec::new();
    let mut idle_ms: Option<u64> = None;
    let mut time_ms: Option<u64> = None;
    let mut retry_count: Option<u64> = None;
    let mut force = false;
    let mut justid = false;

    // Parse IDs and options
    while idx < args.len() {
        let arg_str = bytes_to_string(&args[idx])?;
        let arg_upper = arg_str.to_uppercase();
        match arg_upper.as_str() {
            "IDLE" => {
                idx += 1;
                idle_ms = Some(bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
            }
            "TIME" => {
                idx += 1;
                time_ms = Some(bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
            }
            "RETRYCOUNT" => {
                idx += 1;
                retry_count = Some(bytes_to_string(&args[idx])?.parse::<u64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?);
            }
            "FORCE" => {
                force = true;
            }
            "JUSTID" => {
                justid = true;
            }
            _ => {
                // Try to parse as an entry ID
                let id = StreamEntryId::parse_range_start(&arg_str)
                    .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;
                entry_ids.push(id);
            }
        }
        idx += 1;
    }

    if entry_ids.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key_str = String::from_utf8_lossy(key).to_string();

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let group = stream.groups.get_mut(&group_name)
            .ok_or_else(|| StorageError::InternalError(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                group_name, key_str
            )))?;

        let now = now_ms();
        let mut claimed = Vec::new();

        for id in &entry_ids {
            let should_claim = if let Some(pe) = group.pel.get(id) {
                let idle = now.saturating_sub(pe.delivery_time);
                idle >= min_idle_time
            } else {
                force
            };

            if should_claim {
                // Remove from old consumer's pending list
                if let Some(pe) = group.pel.get(id) {
                    let old_consumer = pe.consumer.clone();
                    if let Some(consumer) = group.consumers.get_mut(&old_consumer) {
                        consumer.pending.retain(|pid| pid != id);
                    }
                }

                let delivery_time = if let Some(t) = time_ms {
                    t
                } else if let Some(idle) = idle_ms {
                    now.saturating_sub(idle)
                } else {
                    now
                };

                let new_count = if let Some(rc) = retry_count {
                    rc
                } else {
                    group.pel.get(id).map(|pe| pe.delivery_count + 1).unwrap_or(1)
                };

                // Update or insert PEL entry
                group.pel.insert(id.clone(), PendingEntry {
                    id: id.clone(),
                    consumer: new_consumer.clone(),
                    delivery_time,
                    delivery_count: new_count,
                });

                // Add to new consumer's pending list
                let consumer = group.get_or_create_consumer(&new_consumer);
                consumer.seen_time = now;
                if !consumer.pending.contains(id) {
                    consumer.pending.push(id.clone());
                }

                if let Some(entry) = stream.entries.get(id) {
                    claimed.push(entry.clone());
                }
            }
        }

        let resp = if justid {
            let result: Vec<RespValue> = claimed.iter().map(|e| {
                RespValue::BulkString(Some(e.id.to_string().into_bytes()))
            }).collect();
            RespValue::Array(Some(result))
        } else {
            let result: Vec<RespValue> = claimed.iter().map(|e| entry_to_resp(e)).collect();
            RespValue::Array(Some(result))
        };

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), resp))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

// ============================================================
// XAUTOCLAIM
// ============================================================

/// Redis XAUTOCLAIM command - Auto-claim pending messages that have been idle
/// XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
pub async fn xautoclaim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let group_name = bytes_to_string(&args[1])?;
    let new_consumer = bytes_to_string(&args[2])?;
    let min_idle_time = bytes_to_string(&args[3])?.parse::<u64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let start_str = bytes_to_string(&args[4])?;
    let start_id = StreamEntryId::parse_range_start(&start_str)
        .ok_or_else(|| CommandError::InvalidArgument("Invalid stream ID specified as stream command argument".to_string()))?;

    let mut count: usize = 100; // default
    let mut justid = false;
    let mut idx = 5;
    while idx < args.len() {
        let opt = bytes_to_string(&args[idx])?.to_uppercase();
        match opt.as_str() {
            "COUNT" => {
                idx += 1;
                if idx >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                count = bytes_to_string(&args[idx])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
            }
            "JUSTID" => {
                justid = true;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unknown option '{}'", opt
                )));
            }
        }
        idx += 1;
    }

    let key_str = String::from_utf8_lossy(key).to_string();

    let result = engine.atomic_modify(key, RedisDataType::Stream, |existing| {
        let mut stream = match existing {
            Some(bytes) => bincode::deserialize::<StreamData>(&bytes[..])
                .map_err(|_| StorageError::WrongType)?,
            None => return Err(StorageError::InternalError("no such key".to_string())),
        };

        let group = stream.groups.get_mut(&group_name)
            .ok_or_else(|| StorageError::InternalError(format!(
                "NOGROUP No such consumer group '{}' for key name '{}'",
                group_name, key_str
            )))?;

        let now = now_ms();
        let mut claimed = Vec::new();
        let mut deleted_ids = Vec::new();
        let mut next_start = StreamEntryId::new(0, 0);
        let mut found = 0;

        let pel_entries: Vec<(StreamEntryId, PendingEntry)> = group.pel.range(start_id..)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();

        for (id, pe) in &pel_entries {
            if found >= count {
                next_start = id.clone();
                break;
            }

            let idle = now.saturating_sub(pe.delivery_time);
            if idle >= min_idle_time {
                found += 1;

                // Check if entry still exists
                if stream.entries.contains_key(id) {
                    // Remove from old consumer
                    if let Some(consumer) = group.consumers.get_mut(&pe.consumer) {
                        consumer.pending.retain(|pid| pid != id);
                    }

                    // Claim for new consumer
                    group.pel.insert(id.clone(), PendingEntry {
                        id: id.clone(),
                        consumer: new_consumer.clone(),
                        delivery_time: now,
                        delivery_count: pe.delivery_count + 1,
                    });

                    let consumer = group.get_or_create_consumer(&new_consumer);
                    consumer.seen_time = now;
                    if !consumer.pending.contains(id) {
                        consumer.pending.push(id.clone());
                    }

                    if let Some(entry) = stream.entries.get(id) {
                        claimed.push(entry.clone());
                    }
                } else {
                    // Entry was deleted - remove from PEL
                    deleted_ids.push(id.clone());
                }
            }
        }

        // Clean up deleted entries from PEL
        for id in &deleted_ids {
            group.pel.remove(id);
            for consumer in group.consumers.values_mut() {
                consumer.pending.retain(|pid| pid != id);
            }
        }

        let next_start_str = if found < count {
            "0-0".to_string()
        } else {
            next_start.to_string()
        };

        let claimed_resp = if justid {
            claimed.iter().map(|e| {
                RespValue::BulkString(Some(e.id.to_string().into_bytes()))
            }).collect::<Vec<_>>()
        } else {
            claimed.iter().map(|e| entry_to_resp(e)).collect::<Vec<_>>()
        };

        let deleted_resp: Vec<RespValue> = deleted_ids.iter().map(|id| {
            RespValue::BulkString(Some(id.to_string().into_bytes()))
        }).collect();

        let resp = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(next_start_str.into_bytes())),
            RespValue::Array(Some(claimed_resp)),
            RespValue::Array(Some(deleted_resp)),
        ]));

        let serialized = bincode::serialize(&stream)
            .map_err(|e| StorageError::InternalError(e.to_string()))?;
        Ok((Some(serialized), resp))
    });

    match result {
        Ok(resp) => Ok(resp),
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg == "no such key" => {
            Err(CommandError::InvalidArgument("no such key".to_string()))
        }
        Err(crate::storage::error::StorageError::InternalError(msg)) if msg.starts_with("NOGROUP") => {
            Err(CommandError::InvalidArgument(msg))
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;

    fn make_engine() -> std::sync::Arc<StorageEngine> {
        StorageEngine::new(StorageConfig::default())
    }

    fn b(s: &str) -> Vec<u8> {
        s.as_bytes().to_vec()
    }

    #[tokio::test]
    async fn test_xadd_and_xlen() {
        let engine = make_engine();
        // XADD mystream * field1 value1
        let result = xadd(&engine, &[b("mystream"), b("*"), b("field1"), b("value1")]).await.unwrap();
        if let RespValue::BulkString(Some(id_bytes)) = &result {
            let id_str = String::from_utf8(id_bytes.clone()).unwrap();
            assert!(id_str.contains('-'), "ID should contain '-': {}", id_str);
        } else {
            panic!("Expected BulkString, got {:?}", result);
        }

        // XLEN
        let result = xlen(&engine, &[b("mystream")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Add more entries
        xadd(&engine, &[b("mystream"), b("*"), b("field2"), b("value2")]).await.unwrap();
        xadd(&engine, &[b("mystream"), b("*"), b("field3"), b("value3")]).await.unwrap();

        let result = xlen(&engine, &[b("mystream")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
    }

    #[tokio::test]
    async fn test_xadd_with_explicit_id() {
        let engine = make_engine();
        let result = xadd(&engine, &[b("s"), b("1-1"), b("k"), b("v")]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"1-1".to_vec())));

        let result = xadd(&engine, &[b("s"), b("2-0"), b("k"), b("v")]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2-0".to_vec())));

        // Should fail - ID too small
        let result = xadd(&engine, &[b("s"), b("1-0"), b("k"), b("v")]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_xadd_nomkstream() {
        let engine = make_engine();
        // NOMKSTREAM on nonexistent key should return nil
        let result = xadd(&engine, &[b("nonexistent"), b("NOMKSTREAM"), b("*"), b("k"), b("v")]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // XLEN should be 0
        let result = xlen(&engine, &[b("nonexistent")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_xadd_with_maxlen() {
        let engine = make_engine();
        for i in 1..=10 {
            xadd(&engine, &[b("s"), b("MAXLEN"), b("5"), b(&format!("{}-0", i)), b("k"), b("v")]).await.unwrap();
        }
        let result = xlen(&engine, &[b("s")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[tokio::test]
    async fn test_xrange() {
        let engine = make_engine();
        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();
        xadd(&engine, &[b("s"), b("3-0"), b("c"), b("3")]).await.unwrap();

        // Full range
        let result = xrange(&engine, &[b("s"), b("-"), b("+")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 3);
        } else {
            panic!("Expected array");
        }

        // Partial range
        let result = xrange(&engine, &[b("s"), b("2-0"), b("3-0")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 2);
        } else {
            panic!("Expected array");
        }

        // With count
        let result = xrange(&engine, &[b("s"), b("-"), b("+"), b("COUNT"), b("2")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_xrevrange() {
        let engine = make_engine();
        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();
        xadd(&engine, &[b("s"), b("3-0"), b("c"), b("3")]).await.unwrap();

        let result = xrevrange(&engine, &[b("s"), b("+"), b("-")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 3);
            // First entry should be 3-0 (reverse order)
            if let RespValue::Array(Some(ref parts)) = entries[0] {
                if let RespValue::BulkString(Some(ref id_bytes)) = parts[0] {
                    assert_eq!(String::from_utf8(id_bytes.clone()).unwrap(), "3-0");
                }
            }
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_xtrim_maxlen() {
        let engine = make_engine();
        for i in 1..=10 {
            xadd(&engine, &[b("s"), b(&format!("{}-0", i)), b("k"), b("v")]).await.unwrap();
        }

        let result = xtrim(&engine, &[b("s"), b("MAXLEN"), b("5")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let result = xlen(&engine, &[b("s")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[tokio::test]
    async fn test_xtrim_minid() {
        let engine = make_engine();
        for i in 1..=10 {
            xadd(&engine, &[b("s"), b(&format!("{}-0", i * 1000)), b("k"), b("v")]).await.unwrap();
        }

        let result = xtrim(&engine, &[b("s"), b("MINID"), b("5000")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(4)); // IDs 1000-4000 removed

        let result = xlen(&engine, &[b("s")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(6));
    }

    #[tokio::test]
    async fn test_xdel() {
        let engine = make_engine();
        xadd(&engine, &[b("s"), b("1-0"), b("k"), b("v")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("k"), b("v")]).await.unwrap();
        xadd(&engine, &[b("s"), b("3-0"), b("k"), b("v")]).await.unwrap();

        let result = xdel(&engine, &[b("s"), b("2-0")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        let result = xlen(&engine, &[b("s")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // Delete non-existent
        let result = xdel(&engine, &[b("s"), b("99-0")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_xread_non_blocking() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        xadd(&engine, &[b("s1"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s1"), b("2-0"), b("b"), b("2")]).await.unwrap();
        xadd(&engine, &[b("s2"), b("1-0"), b("c"), b("3")]).await.unwrap();

        // XREAD STREAMS s1 s2 0 0
        let result = xread(&engine, &[b("STREAMS"), b("s1"), b("s2"), b("0-0"), b("0-0")], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(streams)) = result {
            assert_eq!(streams.len(), 2);
        } else {
            panic!("Expected array result from XREAD");
        }

        // XREAD COUNT 1 STREAMS s1 0
        let result = xread(&engine, &[b("COUNT"), b("1"), b("STREAMS"), b("s1"), b("0-0")], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(streams)) = result {
            assert_eq!(streams.len(), 1);
            if let RespValue::Array(Some(ref stream_data)) = streams[0] {
                if let RespValue::Array(Some(ref entries)) = stream_data[1] {
                    assert_eq!(entries.len(), 1);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_xread_blocking_timeout() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        // XREAD BLOCK 100 STREAMS s1 $ (should timeout and return nil)
        let result = xread(&engine, &[b("BLOCK"), b("100"), b("STREAMS"), b("s1"), b("$")], &blocking_mgr).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_xgroup_create_and_readgroup() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        // Create stream with entries
        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();
        xadd(&engine, &[b("s"), b("3-0"), b("c"), b("3")]).await.unwrap();

        // Create consumer group starting from 0
        let result = xgroup(&engine, &[b("CREATE"), b("s"), b("mygroup"), b("0")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // XREADGROUP GROUP mygroup consumer1 COUNT 2 STREAMS s >
        let result = xreadgroup(&engine, &[
            b("GROUP"), b("mygroup"), b("consumer1"),
            b("COUNT"), b("2"),
            b("STREAMS"), b("s"), b(">")
        ], &blocking_mgr).await.unwrap();

        if let RespValue::Array(Some(streams)) = &result {
            assert_eq!(streams.len(), 1);
            if let RespValue::Array(Some(ref stream_data)) = streams[0] {
                if let RespValue::Array(Some(ref entries)) = stream_data[1] {
                    assert_eq!(entries.len(), 2);
                }
            }
        } else {
            panic!("Expected array result from XREADGROUP: {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_xack() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();

        xgroup(&engine, &[b("CREATE"), b("s"), b("g"), b("0")]).await.unwrap();

        // Read entries
        xreadgroup(&engine, &[
            b("GROUP"), b("g"), b("c1"),
            b("STREAMS"), b("s"), b(">")
        ], &blocking_mgr).await.unwrap();

        // Check pending
        let result = xpending(&engine, &[b("s"), b("g")]).await.unwrap();
        if let RespValue::Array(Some(ref parts)) = result {
            assert_eq!(parts[0], RespValue::Integer(2));
        }

        // Ack one
        let result = xack(&engine, &[b("s"), b("g"), b("1-0")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Check pending again
        let result = xpending(&engine, &[b("s"), b("g")]).await.unwrap();
        if let RespValue::Array(Some(ref parts)) = result {
            assert_eq!(parts[0], RespValue::Integer(1));
        }
    }

    #[tokio::test]
    async fn test_xpending_extended() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();

        xgroup(&engine, &[b("CREATE"), b("s"), b("g"), b("0")]).await.unwrap();

        xreadgroup(&engine, &[
            b("GROUP"), b("g"), b("c1"),
            b("STREAMS"), b("s"), b(">")
        ], &blocking_mgr).await.unwrap();

        // Extended form
        let result = xpending(&engine, &[b("s"), b("g"), b("-"), b("+"), b("10")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 2);
        } else {
            panic!("Expected array");
        }

        // Filter by consumer
        let result = xpending(&engine, &[b("s"), b("g"), b("-"), b("+"), b("10"), b("c1")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 2);
        }

        // Filter by non-existent consumer
        let result = xpending(&engine, &[b("s"), b("g"), b("-"), b("+"), b("10"), b("c2")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 0);
        }
    }

    #[tokio::test]
    async fn test_xclaim() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();

        xgroup(&engine, &[b("CREATE"), b("s"), b("g"), b("0")]).await.unwrap();
        xreadgroup(&engine, &[
            b("GROUP"), b("g"), b("c1"),
            b("STREAMS"), b("s"), b(">")
        ], &blocking_mgr).await.unwrap();

        // Claim with 0 min-idle-time (claim immediately)
        let result = xclaim(&engine, &[b("s"), b("g"), b("c2"), b("0"), b("1-0")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 1);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_xautoclaim() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();

        xgroup(&engine, &[b("CREATE"), b("s"), b("g"), b("0")]).await.unwrap();
        xreadgroup(&engine, &[
            b("GROUP"), b("g"), b("c1"),
            b("STREAMS"), b("s"), b(">")
        ], &blocking_mgr).await.unwrap();

        // Auto-claim with 0 min-idle-time
        let result = xautoclaim(&engine, &[b("s"), b("g"), b("c2"), b("0"), b("0-0")]).await.unwrap();
        if let RespValue::Array(Some(parts)) = result {
            assert_eq!(parts.len(), 3); // [next-start, claimed-entries, deleted-entries]
            if let RespValue::Array(Some(ref claimed)) = parts[1] {
                assert_eq!(claimed.len(), 2);
            }
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_xinfo_stream() {
        let engine = make_engine();
        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xadd(&engine, &[b("s"), b("2-0"), b("b"), b("2")]).await.unwrap();

        let result = xinfo(&engine, &[b("STREAM"), b("s")]).await.unwrap();
        if let RespValue::Array(Some(parts)) = result {
            // Should have key-value pairs
            assert!(parts.len() >= 4);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_xinfo_groups() {
        let engine = make_engine();
        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xgroup(&engine, &[b("CREATE"), b("s"), b("g1"), b("0")]).await.unwrap();
        xgroup(&engine, &[b("CREATE"), b("s"), b("g2"), b("0")]).await.unwrap();

        let result = xinfo(&engine, &[b("GROUPS"), b("s")]).await.unwrap();
        if let RespValue::Array(Some(groups)) = result {
            assert_eq!(groups.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_xgroup_destroy() {
        let engine = make_engine();
        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xgroup(&engine, &[b("CREATE"), b("s"), b("g1"), b("0")]).await.unwrap();

        let result = xgroup(&engine, &[b("DESTROY"), b("s"), b("g1")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Destroy non-existent
        let result = xgroup(&engine, &[b("DESTROY"), b("s"), b("g1")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_xgroup_createconsumer() {
        let engine = make_engine();
        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xgroup(&engine, &[b("CREATE"), b("s"), b("g"), b("0")]).await.unwrap();

        let result = xgroup(&engine, &[b("CREATECONSUMER"), b("s"), b("g"), b("consumer1")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Creating again returns 0
        let result = xgroup(&engine, &[b("CREATECONSUMER"), b("s"), b("g"), b("consumer1")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_xgroup_delconsumer() {
        let engine = make_engine();
        let blocking_mgr = BlockingManager::new();

        xadd(&engine, &[b("s"), b("1-0"), b("a"), b("1")]).await.unwrap();
        xgroup(&engine, &[b("CREATE"), b("s"), b("g"), b("0")]).await.unwrap();

        // Read with a consumer to create it with pending entries
        xreadgroup(&engine, &[
            b("GROUP"), b("g"), b("c1"),
            b("STREAMS"), b("s"), b(">")
        ], &blocking_mgr).await.unwrap();

        // Delete consumer
        let result = xgroup(&engine, &[b("DELCONSUMER"), b("s"), b("g"), b("c1")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // had 1 pending entry
    }

    #[tokio::test]
    async fn test_xgroup_mkstream() {
        let engine = make_engine();

        // Without MKSTREAM, should fail on nonexistent key
        let result = xgroup(&engine, &[b("CREATE"), b("nonexistent"), b("g"), b("0")]).await;
        assert!(result.is_err());

        // With MKSTREAM, should succeed
        let result = xgroup(&engine, &[b("CREATE"), b("newstream"), b("g"), b("0"), b("MKSTREAM")]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Stream should exist but be empty
        let result = xlen(&engine, &[b("newstream")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_xadd_multiple_fields() {
        let engine = make_engine();
        let result = xadd(&engine, &[
            b("s"), b("1-0"),
            b("name"), b("John"),
            b("age"), b("30"),
            b("city"), b("NYC"),
        ]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"1-0".to_vec())));

        let result = xrange(&engine, &[b("s"), b("-"), b("+")]).await.unwrap();
        if let RespValue::Array(Some(entries)) = result {
            assert_eq!(entries.len(), 1);
            if let RespValue::Array(Some(ref parts)) = entries[0] {
                if let RespValue::Array(Some(ref fields)) = parts[1] {
                    assert_eq!(fields.len(), 6); // 3 field-value pairs
                }
            }
        }
    }

    #[tokio::test]
    async fn test_xlen_nonexistent() {
        let engine = make_engine();
        let result = xlen(&engine, &[b("nonexistent")]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_xrange_empty_stream() {
        let engine = make_engine();
        // Use MKSTREAM to create empty stream
        xgroup(&engine, &[b("CREATE"), b("empty"), b("g"), b("0"), b("MKSTREAM")]).await.unwrap();
        let result = xrange(&engine, &[b("empty"), b("-"), b("+")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_xrange_nonexistent() {
        let engine = make_engine();
        let result = xrange(&engine, &[b("nonexistent"), b("-"), b("+")]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }
}
