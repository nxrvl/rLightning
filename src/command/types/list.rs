use std::time::{Duration, Instant};

use futures::future::select_all;

use crate::command::types::blocking::BlockingManager;
use crate::command::utils::bytes_to_string;
use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;
use crate::storage::list_bytes;

/// Internal helper for LPOP/RPOP return types
enum PopResult {
    Single(Vec<u8>),
    Multi(Vec<Vec<u8>>),
    Nil(bool), // bool = whether count was specified
}

/// Redis LPUSH command - Push a value onto the beginning of a list
/// Uses byte-level operations for O(N) memmove instead of O(N) deserialize+splice+serialize
pub async fn lpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let elements: Vec<Vec<u8>> = args[1..].iter().rev().cloned().collect();

    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            list_bytes::lpush(data, &elements)?;
            let new_len = list_bytes::len(data)? as i64;
            Ok((Some(std::mem::take(data)), new_len))
        }
        None => {
            let new_data = list_bytes::new_from_elements(&elements);
            let new_len = elements.len() as i64;
            Ok((Some(new_data), new_len))
        }
    })?;

    Ok(RespValue::Integer(length))
}

/// Redis RPUSH command - Push a value onto the end of a list
/// Uses byte-level operations for O(k) append instead of O(N) deserialize+extend+serialize
pub async fn rpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let elements: Vec<Vec<u8>> = args[1..].to_vec();

    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let new_len = list_bytes::rpush(data, &elements)? as i64;
            Ok((Some(std::mem::take(data)), new_len))
        }
        None => {
            let new_data = list_bytes::new_from_elements(&elements);
            let new_len = elements.len() as i64;
            Ok((Some(new_data), new_len))
        }
    })?;

    Ok(RespValue::Integer(length))
}

/// Redis LPOP command - Remove and return the first element(s) of a list
/// Uses byte-level operations for efficient element extraction
pub async fn lpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Parse optional count parameter (Redis 6.2+)
    let count = if args.len() == 2 {
        let count_str = bytes_to_string(&args[1])?;
        let c = count_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        if c < 0 {
            return Err(CommandError::InvalidArgument(
                "value is not an integer or out of range".to_string(),
            ));
        }
        Some(c as usize)
    } else {
        None
    };

    let result = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let total = list_bytes::len(data)?;
            if total == 0 {
                return Ok((Some(std::mem::take(data)), PopResult::Nil(count.is_some())));
            }

            let pop_count = count.unwrap_or(1);
            let elements = list_bytes::lpop(data, pop_count)?;

            let pop_result = if count.is_some() {
                PopResult::Multi(elements)
            } else {
                match elements.into_iter().next() {
                    Some(e) => PopResult::Single(e),
                    None => PopResult::Nil(false),
                }
            };

            let remaining = list_bytes::len(data)?;
            if remaining == 0 {
                Ok((None, pop_result))
            } else {
                Ok((Some(std::mem::take(data)), pop_result))
            }
        }
        None => Ok((None, PopResult::Nil(count.is_some()))),
    })?;

    match result {
        PopResult::Single(element) => Ok(RespValue::BulkString(Some(element))),
        PopResult::Multi(elements) => {
            let arr: Vec<RespValue> = elements
                .into_iter()
                .map(|e| RespValue::BulkString(Some(e)))
                .collect();
            Ok(RespValue::Array(Some(arr)))
        }
        PopResult::Nil(has_count) => {
            if has_count {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            }
        }
    }
}

/// Redis RPOP command - Remove and return the last element(s) of a list
/// Uses byte-level operations for efficient element extraction
pub async fn rpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    // Parse optional count parameter (Redis 6.2+)
    let count = if args.len() == 2 {
        let count_str = bytes_to_string(&args[1])?;
        let c = count_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        if c < 0 {
            return Err(CommandError::InvalidArgument(
                "value is not an integer or out of range".to_string(),
            ));
        }
        Some(c as usize)
    } else {
        None
    };

    let result = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let total = list_bytes::len(data)?;
            if total == 0 {
                return Ok((Some(std::mem::take(data)), PopResult::Nil(count.is_some())));
            }

            let pop_count = count.unwrap_or(1);
            let elements = list_bytes::rpop(data, pop_count)?;

            let pop_result = if count.is_some() {
                PopResult::Multi(elements)
            } else {
                match elements.into_iter().next() {
                    Some(e) => PopResult::Single(e),
                    None => PopResult::Nil(false),
                }
            };

            let remaining = list_bytes::len(data)?;
            if remaining == 0 {
                Ok((None, pop_result))
            } else {
                Ok((Some(std::mem::take(data)), pop_result))
            }
        }
        None => Ok((None, PopResult::Nil(count.is_some()))),
    })?;

    match result {
        PopResult::Single(element) => Ok(RespValue::BulkString(Some(element))),
        PopResult::Multi(elements) => {
            let arr: Vec<RespValue> = elements
                .into_iter()
                .map(|e| RespValue::BulkString(Some(e)))
                .collect();
            Ok(RespValue::Array(Some(arr)))
        }
        PopResult::Nil(has_count) => {
            if has_count {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            }
        }
    }
}

/// Redis LRANGE command - Get a range of elements from a list
/// Uses byte-level range scan for O(start + k) instead of full deserialization
pub async fn lrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let start = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;

    let stop = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;

    let elements = engine.atomic_read(key, RedisDataType::List, |data| match data {
        Some(d) => list_bytes::range(d, start, stop),
        None => Ok(vec![]),
    })?;

    let result: Vec<RespValue> = elements
        .into_iter()
        .map(|e| RespValue::BulkString(Some(e)))
        .collect();
    Ok(RespValue::Array(Some(result)))
}

/// Redis LLEN command - Return the length of a list
/// Uses byte-level O(1) length read instead of full deserialization
pub async fn llen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let count = engine.atomic_read(key, RedisDataType::List, |data| match data {
        Some(d) => Ok(list_bytes::len(d)? as i64),
        None => Ok(0i64),
    })?;

    Ok(RespValue::Integer(count))
}

/// Redis LINDEX command - Get an element from a list by index
/// Uses byte-level index scan for O(index) instead of full deserialization
pub async fn lindex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let idx = bytes_to_string(&args[1])?
        .parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument("Invalid index, not an integer".to_string()))?;

    let elem = engine.atomic_read(key, RedisDataType::List, |data| match data {
        Some(d) => list_bytes::index(d, idx),
        None => Ok(None),
    })?;

    Ok(RespValue::BulkString(elem))
}

/// Redis LTRIM command - Trim a list to the specified range
/// Uses byte-level trim for efficient in-place range extraction
pub async fn ltrim(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let start = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;

    let stop = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;

    engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let should_delete = list_bytes::trim(data, start, stop)?;
            if should_delete {
                Ok((None, ()))
            } else {
                Ok((Some(std::mem::take(data)), ()))
            }
        }
        None => Ok((None, ())),
    })?;

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis LPUSHX command - Push elements to the head of a list only if the key exists
pub async fn lpushx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let elements: Vec<Vec<u8>> = args[1..].iter().rev().cloned().collect();

    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            list_bytes::lpush(data, &elements)?;
            let new_len = list_bytes::len(data)? as i64;
            Ok((Some(std::mem::take(data)), new_len))
        }
        None => Ok((None, 0i64)),
    })?;

    Ok(RespValue::Integer(length))
}

/// Redis RPUSHX command - Push elements to the tail of a list only if the key exists
pub async fn rpushx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let elements: Vec<Vec<u8>> = args[1..].to_vec();

    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let new_len = list_bytes::rpush(data, &elements)? as i64;
            Ok((Some(std::mem::take(data)), new_len))
        }
        None => Ok((None, 0i64)),
    })?;

    Ok(RespValue::Integer(length))
}

/// Redis LINSERT command - Insert an element before or after a pivot element
pub async fn linsert(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let position = bytes_to_string(&args[1])?.to_uppercase();
    let pivot = args[2].clone();
    let element = args[3].clone();

    if position != "BEFORE" && position != "AFTER" {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    let before = position == "BEFORE";
    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let result = list_bytes::insert_pivot(data, &pivot, element.clone(), before)?;
            if result == -1 {
                Ok((Some(std::mem::take(data)), -1i64))
            } else {
                Ok((Some(std::mem::take(data)), result))
            }
        }
        None => Ok((None, 0i64)),
    })?;

    Ok(RespValue::Integer(length))
}

/// Redis LSET command - Set the value of an element at an index
pub async fn lset(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let index = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let element = args[2].clone();

    engine.check_write_memory(0).await?;
    engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            list_bytes::set_element(data, index, element.clone())?;
            Ok((Some(std::mem::take(data)), ()))
        }
        None => Err(crate::storage::error::StorageError::InternalError(
            "no such key".to_string(),
        )),
    })?;

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis LREM command - Remove elements from a list
/// LREM key count element
pub async fn lrem(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let count = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let element = args[2].clone();

    let removed = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let removed = list_bytes::remove(data, count, &element)?;
            let remaining = list_bytes::len(data)?;
            if remaining == 0 {
                Ok((None, removed))
            } else {
                Ok((Some(std::mem::take(data)), removed))
            }
        }
        None => Ok((None, 0i64)),
    })?;

    Ok(RespValue::Integer(removed))
}

/// Redis LPOS command - Return the index of matching elements in a list
pub async fn lpos(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let element = &args[1];

    let mut rank = 1i64;
    let mut count: Option<i64> = None;
    let mut maxlen = 0usize;

    let mut i = 2;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "RANK" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument("syntax error".to_string()));
                }
                i += 1;
                rank = bytes_to_string(&args[i])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if rank == 0 {
                    return Err(CommandError::InvalidArgument(
                        "RANK can't be zero: use a positive or negative rank".to_string(),
                    ));
                }
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument("syntax error".to_string()));
                }
                i += 1;
                count = Some(bytes_to_string(&args[i])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?);
                if count.unwrap() < 0 {
                    return Err(CommandError::InvalidArgument(
                        "COUNT can't be negative".to_string(),
                    ));
                }
            }
            "MAXLEN" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::InvalidArgument("syntax error".to_string()));
                }
                i += 1;
                let ml = bytes_to_string(&args[i])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if ml < 0 {
                    return Err(CommandError::InvalidArgument(
                        "MAXLEN can't be negative".to_string(),
                    ));
                }
                maxlen = ml as usize;
            }
            _ => return Err(CommandError::InvalidArgument("syntax error".to_string())),
        }
        i += 1;
    }

    let matches = engine.atomic_read(key, RedisDataType::List, |data| match data {
        Some(d) => list_bytes::pos(d, element, rank, count, maxlen),
        None => Ok(vec![]),
    })?;

    if matches.is_empty() && !engine.exists(key).await? {
        return Ok(if count.is_some() {
            RespValue::Array(Some(vec![]))
        } else {
            RespValue::BulkString(None)
        });
    }

    if count.is_some() {
        Ok(RespValue::Array(Some(
            matches.into_iter().map(RespValue::Integer).collect(),
        )))
    } else {
        match matches.first() {
            Some(&idx) => Ok(RespValue::Integer(idx)),
            None => Ok(RespValue::BulkString(None)),
        }
    }
}

/// Redis LMOVE command - Atomically pop from one list and push to another
pub async fn lmove(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let source = &args[0];
    let destination = &args[1];
    let wherefrom = bytes_to_string(&args[2])?.to_uppercase();
    let whereto = bytes_to_string(&args[3])?.to_uppercase();

    if !matches!(wherefrom.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }
    if !matches!(whereto.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    let pop_left = wherefrom == "LEFT";
    let push_left = whereto == "LEFT";

    if source == destination {
        // Same-key: use atomic_modify for single-key atomicity
        let result =
            engine.atomic_modify(source, RedisDataType::List, |current| match current {
                Some(data) => {
                    let total = list_bytes::len(data)?;
                    if total == 0 {
                        return Ok((Some(std::mem::take(data)), None));
                    }

                    let elements = if pop_left {
                        list_bytes::lpop(data, 1)?
                    } else {
                        list_bytes::rpop(data, 1)?
                    };
                    let element = elements.into_iter().next().unwrap();

                    if push_left {
                        list_bytes::lpush(data, std::slice::from_ref(&element))?;
                    } else {
                        list_bytes::rpush(data, std::slice::from_ref(&element))?;
                    }

                    Ok((Some(std::mem::take(data)), Some(element)))
                }
                None => Ok((None, None)),
            })?;

        return match result {
            Some(element) => Ok(RespValue::BulkString(Some(element))),
            None => Ok(RespValue::BulkString(None)),
        };
    }

    // Different keys: use lock_keys for cross-key atomicity
    let _guard = engine
        .lock_keys(&[source.clone(), destination.clone()])
        .await;

    // Pop from source
    let element = engine.atomic_modify(source, RedisDataType::List, |current| match current {
        Some(data) => {
            let total = list_bytes::len(data)?;
            if total == 0 {
                return Ok((Some(std::mem::take(data)), None));
            }

            let elements = if pop_left {
                list_bytes::lpop(data, 1)?
            } else {
                list_bytes::rpop(data, 1)?
            };
            let element = elements.into_iter().next().unwrap();

            let remaining = list_bytes::len(data)?;
            if remaining == 0 {
                Ok((None, Some(element)))
            } else {
                Ok((Some(std::mem::take(data)), Some(element)))
            }
        }
        None => Ok((None, None)),
    })?;

    let element = match element {
        Some(e) => e,
        None => return Ok(RespValue::BulkString(None)),
    };

    // Push to destination
    engine.check_write_memory(0).await?;
    let elem_for_push = element.clone();
    engine.atomic_modify(destination, RedisDataType::List, |current| match current {
        Some(data) => {
            if push_left {
                list_bytes::lpush(data, std::slice::from_ref(&elem_for_push))?;
            } else {
                list_bytes::rpush(data, std::slice::from_ref(&elem_for_push))?;
            }
            Ok((Some(std::mem::take(data)), ()))
        }
        None => {
            let new_data = list_bytes::new_from_elements(std::slice::from_ref(&elem_for_push));
            Ok((Some(new_data), ()))
        }
    })?;

    Ok(RespValue::BulkString(Some(element)))
}

/// Redis RPOPLPUSH command (deprecated) - Pop from tail of source, push to head of destination
/// This is an alias for LMOVE source destination RIGHT LEFT
pub async fn rpoplpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    // Translate to LMOVE args: source, destination, RIGHT, LEFT
    let lmove_args = vec![
        args[0].clone(),
        args[1].clone(),
        b"RIGHT".to_vec(),
        b"LEFT".to_vec(),
    ];
    lmove(engine, &lmove_args).await
}

/// Redis LMPOP command - Pop elements from the first non-empty list
/// Uses atomic_modify for thread-safe read-modify-write
pub async fn lmpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let numkeys_str = bytes_to_string(&args[0])?;
    let numkeys = numkeys_str.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "numkeys can't be non-positive".to_string(),
        ));
    }

    if args.len() < 1 + numkeys + 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let keys = &args[1..1 + numkeys];
    let direction = bytes_to_string(&args[1 + numkeys])?.to_uppercase();

    if direction != "LEFT" && direction != "RIGHT" {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    let mut count = 1usize;
    let mut opt_idx = 2 + numkeys;
    if opt_idx < args.len() {
        let count_opt = bytes_to_string(&args[opt_idx])?.to_uppercase();
        if count_opt == "COUNT" {
            opt_idx += 1;
            if opt_idx >= args.len() {
                return Err(CommandError::WrongNumberOfArguments);
            }
            count = bytes_to_string(&args[opt_idx])?
                .parse::<usize>()
                .map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
            if count == 0 {
                return Err(CommandError::InvalidArgument(
                    "COUNT value of 0 is not allowed".to_string(),
                ));
            }
        }
    }

    let left = direction == "LEFT";

    for key in keys {
        let result = engine.atomic_modify(key, RedisDataType::List, |current| match current {
            Some(data) => {
                let total = list_bytes::len(data)?;
                if total == 0 {
                    return Ok((Some(std::mem::take(data)), None));
                }

                let popped = if left {
                    list_bytes::lpop(data, count)?
                } else {
                    list_bytes::rpop(data, count)?
                };

                let remaining = list_bytes::len(data)?;
                if remaining == 0 {
                    Ok((None, Some(popped)))
                } else {
                    Ok((Some(std::mem::take(data)), Some(popped)))
                }
            }
            None => Ok((None, None)),
        });

        let result = result?;

        if let Some(popped) = result {
            let elements: Vec<RespValue> = popped
                .into_iter()
                .map(|e| RespValue::BulkString(Some(e)))
                .collect();

            return Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::Array(Some(elements)),
            ])));
        }
    }

    Ok(RespValue::Array(None))
}

// --- Blocking Commands ---

/// Helper: try to pop an element from a list
async fn try_pop(
    engine: &StorageEngine,
    key: &[u8],
    left: bool,
) -> Result<Option<Vec<u8>>, CommandError> {
    let result = engine.atomic_modify(key, RedisDataType::List, |current| match current {
        Some(data) => {
            let total = list_bytes::len(data)?;
            if total == 0 {
                return Ok((Some(std::mem::take(data)), None));
            }

            let elements = if left {
                list_bytes::lpop(data, 1)?
            } else {
                list_bytes::rpop(data, 1)?
            };
            let element = elements.into_iter().next().unwrap();

            let remaining = list_bytes::len(data)?;
            if remaining == 0 {
                Ok((None, Some(element)))
            } else {
                Ok((Some(std::mem::take(data)), Some(element)))
            }
        }
        None => Ok((None, None)),
    })?;

    Ok(result)
}

/// Core blocking pop implementation shared by BLPOP and BRPOP
async fn blocking_pop(
    engine: &StorageEngine,
    keys: &[Vec<u8>],
    timeout_secs: f64,
    left: bool,
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    // Try immediate pop first
    for key in keys {
        match try_pop(engine, key, left).await? {
            Some(element) => {
                return Ok(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(key.clone())),
                    RespValue::BulkString(Some(element)),
                ])));
            }
            None => continue,
        }
    }

    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None // 0 means wait indefinitely
    };

    loop {
        // Subscribe to all keys before checking (avoids race condition)
        let receivers: Vec<_> = keys.iter().map(|k| blocking_mgr.subscribe(k)).collect();

        // Try to pop from each key
        for key in keys {
            match try_pop(engine, key, left).await? {
                Some(element) => {
                    return Ok(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        RespValue::BulkString(Some(element)),
                    ])));
                }
                None => continue,
            }
        }

        // Wait for any key to get data or timeout
        let futs: Vec<_> = receivers
            .into_iter()
            .map(|mut rx| {
                Box::pin(async move {
                    let _ = rx.changed().await;
                })
            })
            .collect();

        if futs.is_empty() {
            return Ok(RespValue::Array(None));
        }

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::Array(None));
            }
            if tokio::time::timeout(remaining, select_all(futs))
                .await
                .is_err()
            {
                return Ok(RespValue::Array(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
}

/// Redis BLPOP command - Blocking left pop with timeout
pub async fn blpop(
    engine: &StorageEngine,
    args: &[Vec<u8>],
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let timeout_str = bytes_to_string(args.last().unwrap())?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }

    let keys = &args[..args.len() - 1];
    blocking_pop(engine, keys, timeout_secs, true, blocking_mgr).await
}

/// Redis BRPOP command - Blocking right pop with timeout
pub async fn brpop(
    engine: &StorageEngine,
    args: &[Vec<u8>],
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let timeout_str = bytes_to_string(args.last().unwrap())?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }

    let keys = &args[..args.len() - 1];
    blocking_pop(engine, keys, timeout_secs, false, blocking_mgr).await
}

/// Redis BLMOVE command - Blocking LMOVE with timeout
pub async fn blmove(
    engine: &StorageEngine,
    args: &[Vec<u8>],
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    if args.len() != 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let source = &args[0];
    let destination = &args[1];
    let wherefrom = bytes_to_string(&args[2])?.to_uppercase();
    let whereto = bytes_to_string(&args[3])?.to_uppercase();
    let timeout_str = bytes_to_string(&args[4])?;

    if !matches!(wherefrom.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }
    if !matches!(whereto.as_str(), "LEFT" | "RIGHT") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }

    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }

    // Try immediately first
    let lmove_args = vec![
        source.clone(),
        destination.clone(),
        args[2].clone(),
        args[3].clone(),
    ];
    let result = lmove(engine, &lmove_args).await?;
    if result != RespValue::BulkString(None) {
        return Ok(result);
    }

    // Block until data is available
    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };

    loop {
        let receivers: Vec<_> = vec![blocking_mgr.subscribe(source)];

        // Try again
        let result = lmove(engine, &lmove_args).await?;
        if result != RespValue::BulkString(None) {
            return Ok(result);
        }

        let futs: Vec<_> = receivers
            .into_iter()
            .map(|mut rx| {
                Box::pin(async move {
                    let _ = rx.changed().await;
                })
            })
            .collect();

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::BulkString(None));
            }
            if tokio::time::timeout(remaining, select_all(futs))
                .await
                .is_err()
            {
                return Ok(RespValue::BulkString(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
}

/// Redis BLMPOP command - Blocking LMPOP with timeout
pub async fn blmpop(
    engine: &StorageEngine,
    args: &[Vec<u8>],
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let timeout_str = bytes_to_string(&args[0])?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }

    // Parse remaining args as LMPOP args (numkeys key... direction [COUNT count])
    let lmpop_args = &args[1..];

    // Try immediately
    let result = lmpop(engine, lmpop_args).await?;
    if result != RespValue::Array(None) {
        return Ok(result);
    }

    // Parse keys for subscription
    let numkeys_str = bytes_to_string(&lmpop_args[0])?;
    let numkeys = numkeys_str.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    if numkeys == 0 || lmpop_args.len() < 1 + numkeys + 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let keys = &lmpop_args[1..1 + numkeys];

    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };

    loop {
        let receivers: Vec<_> = keys.iter().map(|k| blocking_mgr.subscribe(k)).collect();

        let result = lmpop(engine, lmpop_args).await?;
        if result != RespValue::Array(None) {
            return Ok(result);
        }

        let futs: Vec<_> = receivers
            .into_iter()
            .map(|mut rx| {
                Box::pin(async move {
                    let _ = rx.changed().await;
                })
            })
            .collect();

        if futs.is_empty() {
            return Ok(RespValue::Array(None));
        }

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::Array(None));
            }
            if tokio::time::timeout(remaining, select_all(futs))
                .await
                .is_err()
            {
                return Ok(RespValue::Array(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::commands;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    // Test LPUSH and RPUSH
    #[tokio::test]
    async fn test_lpush_rpush() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Test LPUSH - First push creates the list
        let args = vec![b"mylist".to_vec(), b"world".to_vec()];
        let result = lpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Test LPUSH - Second push prepends
        let args = vec![b"mylist".to_vec(), b"hello".to_vec()];
        let result = lpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // Check the list with LRANGE
        let args = vec![b"mylist".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::BulkString(Some(b"hello".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"world".to_vec())));
        } else {
            panic!("Unexpected result type");
        }

        // Test RPUSH - Appends to the end
        let args = vec![b"mylist".to_vec(), b"!".to_vec()];
        let result = rpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Check the updated list
        let args = vec![b"mylist".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"hello".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"world".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"!".to_vec())));
        } else {
            panic!("Unexpected result type");
        }
    }

    // Test LPOP and RPOP
    #[tokio::test]
    async fn test_lpop_rpop() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Use a key with "list" in the name to ensure it's recognized as a list type
        let key = b"test_list_poplist".to_vec();

        // Force delete the key to ensure it doesn't exist
        engine.del(&key).await.unwrap();

        // Create a list directly first using set_with_type to properly track type
        let serialized = list_bytes::new_empty();
        engine
            .set_with_type(
                key.clone(),
                serialized,
                crate::storage::item::RedisDataType::List,
                None,
            )
            .await
            .unwrap();

        // DEBUG: Check the key type
        let key_type = engine.get_type(&key).await.unwrap();
        println!("DEBUG: Key type after setting empty list: {}", key_type);
        assert_eq!(key_type, "list"); // Assert that the key is recognized as a list

        // Now push elements using rpush
        let mut args = vec![key.clone()];
        args.push(b"one".to_vec());
        args.push(b"two".to_vec());
        args.push(b"three".to_vec());

        rpush(&engine, &args).await.unwrap();

        // DEBUG: Check key type again after rpush
        let key_type = engine.get_type(&key).await.unwrap();
        println!("DEBUG: Key type after rpush: {}", key_type);

        // Test LPOP - removes and returns first element
        let args = vec![key.clone()];
        let result = lpop(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"one".to_vec())));

        // Test RPOP - removes and returns last element
        let args = vec![key.clone()];
        let result = rpop(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"three".to_vec())));

        // Check the remaining list
        let args = vec![key.clone(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], RespValue::BulkString(Some(b"two".to_vec())));
        } else {
            panic!("Unexpected result type");
        }

        // Empty the list
        lpop(&engine, &[key.clone()]).await.unwrap();

        // LPOP and RPOP on empty list should return nil
        let result = lpop(&engine, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        let result = rpop(&engine, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    // Test LRANGE with different indices
    #[tokio::test]
    async fn test_lrange() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Setup a list with values
        let args = vec![
            b"rangelist".to_vec(),
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec(),
            b"five".to_vec(),
        ];
        rpush(&engine, &args).await.unwrap();

        // Test positive indices
        let args = vec![b"rangelist".to_vec(), b"0".to_vec(), b"2".to_vec()];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"two".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"three".to_vec())));
        } else {
            panic!("Unexpected result type");
        }

        // Test negative indices
        let args = vec![b"rangelist".to_vec(), b"-3".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"three".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"four".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"five".to_vec())));
        } else {
            panic!("Unexpected result type");
        }

        // Test out of bounds indices
        let args = vec![b"rangelist".to_vec(), b"10".to_vec(), b"20".to_vec()];
        let result = lrange(&engine, &args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 0); // Should return an empty array
        } else {
            panic!("Unexpected result type");
        }
    }

    // Test LRANGE with large lists to verify chunked processing works
    #[tokio::test]
    async fn test_lrange_large_list() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create a large list
        let key = b"largelist".to_vec();
        let mut items = Vec::new();

        // Add 5000 items to make it large enough to test chunking
        for i in 0..5000 {
            items.push(format!("item-{}", i).into_bytes());
        }

        // Push all items at once
        let mut args = vec![key.clone()];
        args.extend(items.clone());
        rpush(&engine, &args).await.unwrap();

        // Test retrieving the entire list
        let result = lrange(&engine, &[key.clone(), b"0".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 5000);

            // Check a few sample values
            assert_eq!(values[0], RespValue::BulkString(Some(b"item-0".to_vec())));
            assert_eq!(
                values[1000],
                RespValue::BulkString(Some(b"item-1000".to_vec()))
            );
            assert_eq!(
                values[4999],
                RespValue::BulkString(Some(b"item-4999".to_vec()))
            );
        } else {
            panic!("Expected array response from LRANGE");
        }

        // Test retrieving a subset that spans multiple chunks
        let result = lrange(&engine, &[key.clone(), b"950".to_vec(), b"1050".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 101);
            assert_eq!(values[0], RespValue::BulkString(Some(b"item-950".to_vec())));
            assert_eq!(
                values[100],
                RespValue::BulkString(Some(b"item-1050".to_vec()))
            );
        } else {
            panic!("Expected array response from LRANGE");
        }
    }

    #[tokio::test]
    async fn test_llen() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Use a key with "list" in the name to ensure it's recognized as a list type
        let key = b"test_list_lenlist".to_vec();

        // Force delete the key to ensure it doesn't exist
        engine.del(&key).await.unwrap();

        // Create a list directly first using set_with_type to properly track type
        let serialized = list_bytes::new_empty();
        engine
            .set_with_type(
                key.clone(),
                serialized,
                crate::storage::item::RedisDataType::List,
                None,
            )
            .await
            .unwrap();

        // DEBUG: Check the key type
        let key_type = engine.get_type(&key).await.unwrap();
        println!("DEBUG: Key type after setting empty list: {}", key_type);
        assert_eq!(key_type, "list"); // Assert that the key is recognized as a list

        // Empty list (key doesn't exist)
        let args = vec![b"nonexistent".to_vec()];
        let result = llen(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Setup a list with some values
        let mut args = vec![key.clone()];
        args.push(b"one".to_vec());
        args.push(b"two".to_vec());
        args.push(b"three".to_vec());

        rpush(&engine, &args).await.unwrap();

        // Test LLEN
        let args = vec![key.clone()];
        let result = llen(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Add more elements
        let mut args = vec![key.clone()];
        args.push(b"four".to_vec());
        args.push(b"five".to_vec());

        rpush(&engine, &args).await.unwrap();

        // Check LLEN again
        let args = vec![key.clone()];
        let result = llen(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Remove an element and check again
        lpop(&engine, &[key.clone()]).await.unwrap();
        let result = llen(&engine, &[key.clone()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(4));
    }

    #[tokio::test]
    async fn test_ltrim() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Setup a test list
        let lpush_args = vec![
            b"trim_list".to_vec(),
            b"e".to_vec(),
            b"d".to_vec(),
            b"c".to_vec(),
            b"b".to_vec(),
            b"a".to_vec(),
        ];
        let _ = lpush(&engine, &lpush_args).await.unwrap();

        // Test LTRIM to keep only elements 1-3 (b, c, d)
        let ltrim_args = vec![b"trim_list".to_vec(), b"1".to_vec(), b"3".to_vec()];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check that the list was trimmed correctly
        let lrange_args = vec![b"trim_list".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &lrange_args).await.unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"d".to_vec())));
        } else {
            panic!("Expected array response");
        }

        // Test LTRIM with negative indices
        let ltrim_args = vec![
            b"trim_list".to_vec(),
            b"0".to_vec(),
            b"-2".to_vec(), // Keep first element to second-to-last
        ];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check that the list was trimmed correctly (should be "b" and "c")
        let lrange_args = vec![b"trim_list".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &lrange_args).await.unwrap();

        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"c".to_vec())));
        } else {
            panic!("Expected array response");
        }

        // Test LTRIM on non-existent key
        let ltrim_args = vec![b"nonexistent_list".to_vec(), b"0".to_vec(), b"1".to_vec()];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Test LTRIM that results in an empty list (should remove the key)
        let ltrim_args = vec![
            b"trim_list".to_vec(),
            b"10".to_vec(),
            b"20".to_vec(), // Indices beyond the list length
        ];
        let result = ltrim(&engine, &ltrim_args).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Check that the key no longer exists
        let exists_args = vec![b"trim_list".to_vec()];
        let result = commands::exists(&engine, &exists_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_lindex() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create a list with values
        let args = vec![
            b"indexlist".to_vec(),
            b"zero".to_vec(),
            b"one".to_vec(),
            b"two".to_vec(),
            b"three".to_vec(),
            b"four".to_vec(),
        ];
        rpush(&engine, &args).await.unwrap();

        // Test positive indices
        let args = vec![b"indexlist".to_vec(), b"0".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"zero".to_vec())));

        let args = vec![b"indexlist".to_vec(), b"2".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"two".to_vec())));

        let args = vec![b"indexlist".to_vec(), b"4".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"four".to_vec())));

        // Test negative indices
        let args = vec![b"indexlist".to_vec(), b"-1".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"four".to_vec())));

        let args = vec![b"indexlist".to_vec(), b"-2".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"three".to_vec())));

        let args = vec![b"indexlist".to_vec(), b"-5".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"zero".to_vec())));

        // Test out of bounds indices
        let args = vec![b"indexlist".to_vec(), b"10".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        let args = vec![b"indexlist".to_vec(), b"-10".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Test on non-existent key
        let args = vec![b"nonexistent".to_vec(), b"0".to_vec()];
        let result = lindex(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_lpushx_rpushx() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // LPUSHX on non-existent key returns 0
        let args = vec![b"mylist".to_vec(), b"a".to_vec()];
        let result = lpushx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Create the list first
        rpush(&engine, &[b"mylist".to_vec(), b"x".to_vec()])
            .await
            .unwrap();

        // LPUSHX on existing list works
        let result = lpushx(&engine, &[b"mylist".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // RPUSHX on non-existent key returns 0
        let result = rpushx(&engine, &[b"nolist".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // RPUSHX on existing list works
        let result = rpushx(&engine, &[b"mylist".to_vec(), b"z".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Verify order: [a, x, z]
        let result = lrange(
            &engine,
            &[b"mylist".to_vec(), b"0".to_vec(), b"-1".to_vec()],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"x".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"z".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_linsert() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create list [a, b, d]
        rpush(
            &engine,
            &[
                b"mylist".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"d".to_vec(),
            ],
        )
        .await
        .unwrap();

        // Insert "c" BEFORE "d"
        let result = linsert(
            &engine,
            &[
                b"mylist".to_vec(),
                b"BEFORE".to_vec(),
                b"d".to_vec(),
                b"c".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(4));

        // Insert "e" AFTER "d"
        let result = linsert(
            &engine,
            &[
                b"mylist".to_vec(),
                b"AFTER".to_vec(),
                b"d".to_vec(),
                b"e".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(5));

        // Verify: [a, b, c, d, e]
        let result = lrange(
            &engine,
            &[b"mylist".to_vec(), b"0".to_vec(), b"-1".to_vec()],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 5);
            assert_eq!(values[2], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(values[4], RespValue::BulkString(Some(b"e".to_vec())));
        } else {
            panic!("Expected array");
        }

        // Pivot not found returns -1
        let result = linsert(
            &engine,
            &[
                b"mylist".to_vec(),
                b"BEFORE".to_vec(),
                b"z".to_vec(),
                b"x".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(-1));

        // Non-existent key returns 0
        let result = linsert(
            &engine,
            &[
                b"nolist".to_vec(),
                b"BEFORE".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_lset() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create list [a, b, c]
        rpush(
            &engine,
            &[
                b"mylist".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ],
        )
        .await
        .unwrap();

        // Set index 1 to "B"
        let result = lset(&engine, &[b"mylist".to_vec(), b"1".to_vec(), b"B".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Verify
        let result = lindex(&engine, &[b"mylist".to_vec(), b"1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"B".to_vec())));

        // Negative index
        let result = lset(
            &engine,
            &[b"mylist".to_vec(), b"-1".to_vec(), b"C".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = lindex(&engine, &[b"mylist".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"C".to_vec())));

        // Out of range index
        let result = lset(
            &engine,
            &[b"mylist".to_vec(), b"10".to_vec(), b"x".to_vec()],
        )
        .await;
        assert!(result.is_err());

        // Non-existent key
        let result = lset(&engine, &[b"nolist".to_vec(), b"0".to_vec(), b"x".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_lpos() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create list [a, b, c, b, a]
        rpush(
            &engine,
            &[
                b"mylist".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"b".to_vec(),
                b"a".to_vec(),
            ],
        )
        .await
        .unwrap();

        // Find first "b"
        let result = lpos(&engine, &[b"mylist".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Find with COUNT 0 (all occurrences)
        let result = lpos(
            &engine,
            &[
                b"mylist".to_vec(),
                b"b".to_vec(),
                b"COUNT".to_vec(),
                b"0".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values, vec![RespValue::Integer(1), RespValue::Integer(3)]);
        } else {
            panic!("Expected array");
        }

        // Find with RANK 2 (second occurrence)
        let result = lpos(
            &engine,
            &[
                b"mylist".to_vec(),
                b"b".to_vec(),
                b"RANK".to_vec(),
                b"2".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));

        // Find with negative RANK (from end)
        let result = lpos(
            &engine,
            &[
                b"mylist".to_vec(),
                b"a".to_vec(),
                b"RANK".to_vec(),
                b"-1".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(4));

        // Not found returns nil
        let result = lpos(&engine, &[b"mylist".to_vec(), b"z".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Non-existent key returns nil
        let result = lpos(&engine, &[b"nolist".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // With MAXLEN
        let result = lpos(
            &engine,
            &[
                b"mylist".to_vec(),
                b"a".to_vec(),
                b"COUNT".to_vec(),
                b"0".to_vec(),
                b"MAXLEN".to_vec(),
                b"3".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values, vec![RespValue::Integer(0)]);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_lmove() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create source [a, b, c]
        rpush(
            &engine,
            &[b"src".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();

        // LMOVE src dst LEFT RIGHT -> moves "a" to end of dst
        let result = lmove(
            &engine,
            &[
                b"src".to_vec(),
                b"dst".to_vec(),
                b"LEFT".to_vec(),
                b"RIGHT".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"a".to_vec())));

        // src should be [b, c]
        let result = lrange(&engine, &[b"src".to_vec(), b"0".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }

        // dst should be [a]
        let result = lrange(&engine, &[b"dst".to_vec(), b"0".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected array");
        }

        // Non-existent source returns nil
        let result = lmove(
            &engine,
            &[
                b"nolist".to_vec(),
                b"dst".to_vec(),
                b"LEFT".to_vec(),
                b"RIGHT".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::BulkString(None));

        // Same key rotation: src [b, c] -> LMOVE src src LEFT RIGHT -> src [c, b]
        let result = lmove(
            &engine,
            &[
                b"src".to_vec(),
                b"src".to_vec(),
                b"LEFT".to_vec(),
                b"RIGHT".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"b".to_vec())));

        let result = lrange(&engine, &[b"src".to_vec(), b"0".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 2);
            assert_eq!(values[0], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_lmpop() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // Create lists
        rpush(
            &engine,
            &[
                b"list1".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ],
        )
        .await
        .unwrap();
        rpush(&engine, &[b"list2".to_vec(), b"x".to_vec(), b"y".to_vec()])
            .await
            .unwrap();

        // Pop 1 from left of first non-empty key
        let result = lmpop(
            &engine,
            &[
                b"2".to_vec(),
                b"list1".to_vec(),
                b"list2".to_vec(),
                b"LEFT".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list1".to_vec())));
            if let RespValue::Array(Some(elements)) = &values[1] {
                assert_eq!(elements.len(), 1);
                assert_eq!(elements[0], RespValue::BulkString(Some(b"a".to_vec())));
            } else {
                panic!("Expected elements array");
            }
        } else {
            panic!("Expected array");
        }

        // Pop 2 from right
        let result = lmpop(
            &engine,
            &[
                b"2".to_vec(),
                b"list1".to_vec(),
                b"list2".to_vec(),
                b"RIGHT".to_vec(),
                b"COUNT".to_vec(),
                b"2".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list1".to_vec())));
            if let RespValue::Array(Some(elements)) = &values[1] {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], RespValue::BulkString(Some(b"c".to_vec())));
                assert_eq!(elements[1], RespValue::BulkString(Some(b"b".to_vec())));
            } else {
                panic!("Expected elements array");
            }
        } else {
            panic!("Expected array");
        }

        // list1 is now empty, so next pop should come from list2
        let result = lmpop(
            &engine,
            &[
                b"2".to_vec(),
                b"list1".to_vec(),
                b"list2".to_vec(),
                b"LEFT".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list2".to_vec())));
        } else {
            panic!("Expected array");
        }

        // All empty returns nil
        let result = lmpop(
            &engine,
            &[b"1".to_vec(), b"emptylist".to_vec(), b"LEFT".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_blpop_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        // Create list with data
        rpush(&engine, &[b"mylist".to_vec(), b"hello".to_vec()])
            .await
            .unwrap();

        // BLPOP should return immediately
        let result = blpop(&engine, &[b"mylist".to_vec(), b"1".to_vec()], &blocking_mgr)
            .await
            .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"mylist".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"hello".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blpop_timeout() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        // BLPOP on empty list should timeout
        let start = Instant::now();
        let result = blpop(
            &engine,
            &[b"emptylist".to_vec(), b"0.1".to_vec()],
            &blocking_mgr,
        )
        .await
        .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result, RespValue::Array(None));
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_blpop_wakeup() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        let blocking_mgr = Arc::new(BlockingManager::new());

        let engine_clone = engine.clone();
        let mgr_clone = blocking_mgr.clone();

        // Spawn BLPOP in background
        let handle = tokio::spawn(async move {
            blpop(
                &engine_clone,
                &[b"wakekey".to_vec(), b"5".to_vec()],
                &mgr_clone,
            )
            .await
        });

        // Give it time to start waiting
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Push data to wake it up
        rpush(&engine, &[b"wakekey".to_vec(), b"wakedata".to_vec()])
            .await
            .unwrap();
        blocking_mgr.notify_key(b"wakekey");

        let result = handle.await.unwrap().unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"wakekey".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"wakedata".to_vec())));
        } else {
            panic!("Expected array, got {:?}", result);
        }
    }

    #[tokio::test]
    async fn test_brpop_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        rpush(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        let result = brpop(&engine, &[b"mylist".to_vec(), b"1".to_vec()], &blocking_mgr)
            .await
            .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"mylist".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blmove_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        rpush(&engine, &[b"src".to_vec(), b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        let result = blmove(
            &engine,
            &[
                b"src".to_vec(),
                b"dst".to_vec(),
                b"LEFT".to_vec(),
                b"RIGHT".to_vec(),
                b"1".to_vec(),
            ],
            &blocking_mgr,
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"a".to_vec())));

        // Verify dst has the element
        let result = lrange(&engine, &[b"dst".to_vec(), b"0".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 1);
            assert_eq!(values[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blmove_timeout() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        let start = Instant::now();
        let result = blmove(
            &engine,
            &[
                b"empty".to_vec(),
                b"dst".to_vec(),
                b"LEFT".to_vec(),
                b"RIGHT".to_vec(),
                b"0.1".to_vec(),
            ],
            &blocking_mgr,
        )
        .await
        .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result, RespValue::BulkString(None));
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_blmpop_immediate() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        rpush(&engine, &[b"mylist".to_vec(), b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        // BLMPOP timeout numkeys key LEFT
        let result = blmpop(
            &engine,
            &[
                b"1".to_vec(),
                b"1".to_vec(),
                b"mylist".to_vec(),
                b"LEFT".to_vec(),
            ],
            &blocking_mgr,
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"mylist".to_vec())));
            if let RespValue::Array(Some(elements)) = &values[1] {
                assert_eq!(elements[0], RespValue::BulkString(Some(b"a".to_vec())));
            } else {
                panic!("Expected elements array");
            }
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_blmpop_timeout() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        let start = Instant::now();
        let result = blmpop(
            &engine,
            &[
                b"0.1".to_vec(),
                b"1".to_vec(),
                b"emptylist".to_vec(),
                b"LEFT".to_vec(),
            ],
            &blocking_mgr,
        )
        .await
        .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(result, RespValue::Array(None));
        assert!(elapsed >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_blpop_multi_key() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);
        let blocking_mgr = BlockingManager::new();

        // Only second key has data
        rpush(&engine, &[b"list2".to_vec(), b"hello".to_vec()])
            .await
            .unwrap();

        let result = blpop(
            &engine,
            &[b"list1".to_vec(), b"list2".to_vec(), b"1".to_vec()],
            &blocking_mgr,
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"list2".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"hello".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_concurrent_rpush_same_list() {
        // Multiple tasks racing RPUSH on the same list - no elements should be lost
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..10 {
                    let args = vec![
                        b"rpush_race".to_vec(),
                        format!("val_{}_{}", i, j).into_bytes(),
                    ];
                    rpush(&eng, &args).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 100 elements should be present
        let result = llen(&engine, &[b"rpush_race".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(100));
    }

    #[tokio::test]
    async fn test_concurrent_lpush_rpush_mixed() {
        // LPUSH and RPUSH racing on same list - total length must equal sum of all pushes
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let mut handles = Vec::new();
        // 5 LPUSH tasks
        for i in 0..5 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..20 {
                    let args = vec![
                        b"mixed_race".to_vec(),
                        format!("left_{}_{}", i, j).into_bytes(),
                    ];
                    lpush(&eng, &args).await.unwrap();
                }
            }));
        }
        // 5 RPUSH tasks
        for i in 0..5 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..20 {
                    let args = vec![
                        b"mixed_race".to_vec(),
                        format!("right_{}_{}", i, j).into_bytes(),
                    ];
                    rpush(&eng, &args).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // 5*20 + 5*20 = 200 elements
        let result = llen(&engine, &[b"mixed_race".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(200));
    }

    #[tokio::test]
    async fn test_concurrent_lpush_lpop() {
        // Concurrent LPUSH and LPOP - no elements should be lost or duplicated
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Pre-populate with 100 elements
        for i in 0..100 {
            let args = vec![b"pop_race".to_vec(), format!("init_{}", i).into_bytes()];
            rpush(&engine, &args).await.unwrap();
        }

        let push_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let pop_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let mut handles = Vec::new();

        // 5 push tasks, each pushes 20 elements
        for i in 0..5 {
            let eng = engine.clone();
            let pc = push_count.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..20 {
                    let args = vec![
                        b"pop_race".to_vec(),
                        format!("new_{}_{}", i, j).into_bytes(),
                    ];
                    lpush(&eng, &args).await.unwrap();
                    pc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }));
        }

        // 5 pop tasks, each pops 20 elements
        for _ in 0..5 {
            let eng = engine.clone();
            let pc = pop_count.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..20 {
                    let args = vec![b"pop_race".to_vec()];
                    if let Ok(RespValue::BulkString(Some(_))) = lpop(&eng, &args).await {
                        pc.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        let pushes = push_count.load(std::sync::atomic::Ordering::Relaxed);
        let pops = pop_count.load(std::sync::atomic::Ordering::Relaxed);
        let result = llen(&engine, &[b"pop_race".to_vec()]).await.unwrap();
        if let RespValue::Integer(len) = result {
            // initial(100) + pushes - pops = remaining length
            assert_eq!(len, (100 + pushes - pops) as i64);
        } else {
            panic!("Expected Integer response from LLEN");
        }
    }

    #[tokio::test]
    async fn test_concurrent_lpush_same_list() {
        // Two clients racing LPUSH on the same list - no elements should be lost
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..10 {
                    let args = vec![
                        b"race_list".to_vec(),
                        format!("val_{}_{}", i, j).into_bytes(),
                    ];
                    lpush(&eng, &args).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 100 elements should be present
        let result = llen(&engine, &[b"race_list".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(100));
    }
}
