use std::collections::VecDeque;
use std::time::{Duration, Instant};

use futures::future::select_all;

use crate::command::types::blocking::BlockingManager;
use crate::command::utils::bytes_to_string;
use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;
use crate::storage::value::{ModifyResult, StoreValue};

/// Internal helper for LPOP/RPOP return types
enum PopResult {
    Single(Vec<u8>),
    Multi(Vec<Vec<u8>>),
    Nil(bool), // bool = whether count was specified
}

/// Helper: resolve a possibly-negative index into a positive index, or None if out of range.
fn resolve_index(idx: i64, len: usize) -> Option<usize> {
    let len_i64 = len as i64;
    let i = if idx < 0 { len_i64 + idx } else { idx };
    if i < 0 || i >= len_i64 {
        None
    } else {
        Some(i as usize)
    }
}

/// Helper: list result for atomic_modify - delete if empty, keep otherwise.
/// The delta parameter is the byte size change from the mutation.
fn list_result(deque: &VecDeque<Vec<u8>>, delta: i64) -> ModifyResult {
    if deque.is_empty() {
        ModifyResult::Delete
    } else {
        ModifyResult::Keep(delta)
    }
}

/// Redis LPUSH command - Push a value onto the beginning of a list
pub async fn lpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    // Redis LPUSH pushes each element left-to-right onto the head.
    // e.g. LPUSH key a b c → push a (head=a), push b (head=b,a), push c (head=c,b,a)
    let elements: Vec<Vec<u8>> = args[1..].to_vec();

    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, move |current| match current {
        Some(StoreValue::List(deque)) => {
            let mut delta: i64 = 0;
            for e in elements {
                delta += e.len() as i64 + 24;
                deque.push_front(e);
            }
            let new_len = deque.len() as i64;
            Ok((ModifyResult::Keep(delta), new_len))
        }
        None => {
            let mut deque = VecDeque::with_capacity(elements.len());
            for e in elements {
                deque.push_front(e);
            }
            let new_len = deque.len() as i64;
            Ok((ModifyResult::Set(StoreValue::List(deque)), new_len))
        }
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::Integer(length))
}

/// Redis RPUSH command - Push a value onto the end of a list
pub async fn rpush(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let elements: Vec<Vec<u8>> = args[1..].to_vec();

    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, move |current| match current {
        Some(StoreValue::List(deque)) => {
            let mut delta: i64 = 0;
            for e in elements {
                delta += e.len() as i64 + 24;
                deque.push_back(e);
            }
            let new_len = deque.len() as i64;
            Ok((ModifyResult::Keep(delta), new_len))
        }
        None => {
            let mut deque = VecDeque::with_capacity(elements.len());
            for e in elements {
                deque.push_back(e);
            }
            let new_len = deque.len() as i64;
            Ok((ModifyResult::Set(StoreValue::List(deque)), new_len))
        }
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::Integer(length))
}

/// Redis LPOP command - Remove and return the first element(s) of a list
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
        Some(StoreValue::List(deque)) => {
            if deque.is_empty() {
                return Ok((ModifyResult::Keep(0), PopResult::Nil(count.is_some())));
            }

            let pop_count = count.unwrap_or(1);
            let actual = pop_count.min(deque.len());
            let mut elements = Vec::with_capacity(actual);
            let mut delta: i64 = 0;
            for _ in 0..actual {
                if let Some(e) = deque.pop_front() {
                    delta -= e.len() as i64 + 24;
                    elements.push(e);
                }
            }

            let pop_result = if count.is_some() {
                PopResult::Multi(elements)
            } else {
                match elements.into_iter().next() {
                    Some(e) => PopResult::Single(e),
                    None => PopResult::Nil(false),
                }
            };

            Ok((list_result(deque, delta), pop_result))
        }
        None => Ok((ModifyResult::Keep(0), PopResult::Nil(count.is_some()))),
        _ => Err(crate::storage::error::StorageError::WrongType),
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
        Some(StoreValue::List(deque)) => {
            if deque.is_empty() {
                return Ok((ModifyResult::Keep(0), PopResult::Nil(count.is_some())));
            }

            let pop_count = count.unwrap_or(1);
            let actual = pop_count.min(deque.len());
            let mut elements = Vec::with_capacity(actual);
            let mut delta: i64 = 0;
            for _ in 0..actual {
                if let Some(e) = deque.pop_back() {
                    delta -= e.len() as i64 + 24;
                    elements.push(e);
                }
            }

            let pop_result = if count.is_some() {
                PopResult::Multi(elements)
            } else {
                match elements.into_iter().next() {
                    Some(e) => PopResult::Single(e),
                    None => PopResult::Nil(false),
                }
            };

            Ok((list_result(deque, delta), pop_result))
        }
        None => Ok((ModifyResult::Keep(0), PopResult::Nil(count.is_some()))),
        _ => Err(crate::storage::error::StorageError::WrongType),
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
        Some(StoreValue::List(deque)) => {
            let len = deque.len();
            if len == 0 {
                return Ok(vec![]);
            }
            let len_i64 = len as i64;
            let si = if start < 0 {
                (len_i64 + start).max(0) as usize
            } else {
                start.min(len_i64) as usize
            };
            let ei = if stop < 0 {
                (len_i64 + stop).max(-1) as usize
            } else {
                stop.min(len_i64 - 1) as usize
            };
            if si > ei || si >= len {
                return Ok(vec![]);
            }
            Ok(deque.iter().skip(si).take(ei - si + 1).cloned().collect())
        }
        None => Ok(vec![]),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    let result: Vec<RespValue> = elements
        .into_iter()
        .map(|e| RespValue::BulkString(Some(e)))
        .collect();
    Ok(RespValue::Array(Some(result)))
}

/// Redis LLEN command - Return the length of a list
pub async fn llen(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let count = engine.atomic_read(key, RedisDataType::List, |data| match data {
        Some(StoreValue::List(deque)) => Ok(deque.len() as i64),
        None => Ok(0i64),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::Integer(count))
}

/// Redis LINDEX command - Get an element from a list by index
pub async fn lindex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let idx = bytes_to_string(&args[1])?
        .parse::<i64>()
        .map_err(|_| CommandError::InvalidArgument("Invalid index, not an integer".to_string()))?;

    let elem = engine.atomic_read(key, RedisDataType::List, |data| match data {
        Some(StoreValue::List(deque)) => {
            let resolved = resolve_index(idx, deque.len());
            Ok(resolved.and_then(|i| deque.get(i).cloned()))
        }
        None => Ok(None),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::BulkString(elem))
}

/// Redis LTRIM command - Trim a list to the specified range
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
        Some(StoreValue::List(deque)) => {
            let len = deque.len();
            if len == 0 {
                return Ok((ModifyResult::Delete, ()));
            }
            let len_i64 = len as i64;
            let si = if start < 0 {
                (len_i64 + start).max(0) as usize
            } else {
                start.min(len_i64) as usize
            };
            let ei = if stop < 0 {
                (len_i64 + stop).max(-1) as usize
            } else {
                stop.min(len_i64 - 1) as usize
            };
            if si > ei || si >= len {
                // Empty result
                return Ok((ModifyResult::Delete, ()));
            }
            // Compute delta: size of elements being removed (before si and after ei)
            let mut removed_size: i64 = 0;
            for (i, e) in deque.iter().enumerate() {
                if i < si || i > ei {
                    removed_size += e.len() as i64 + 24;
                }
            }
            let new_deque: VecDeque<Vec<u8>> =
                deque.iter().skip(si).take(ei - si + 1).cloned().collect();
            *deque = new_deque;
            Ok((list_result(deque, -removed_size), ()))
        }
        None => Ok((ModifyResult::Keep(0), ())),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::SimpleString("OK".to_string()))
}

/// Redis LPUSHX command - Push elements to the head of a list only if the key exists
pub async fn lpushx(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let elements: Vec<Vec<u8>> = args[1..].to_vec();

    engine.check_write_memory(0).await?;
    let length = engine.atomic_modify(key, RedisDataType::List, move |current| match current {
        Some(StoreValue::List(deque)) => {
            let mut delta: i64 = 0;
            for e in elements {
                delta += e.len() as i64 + 24;
                deque.push_front(e);
            }
            let new_len = deque.len() as i64;
            Ok((ModifyResult::Keep(delta), new_len))
        }
        None => Ok((ModifyResult::Keep(0), 0i64)),
        _ => Err(crate::storage::error::StorageError::WrongType),
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
    let length = engine.atomic_modify(key, RedisDataType::List, move |current| match current {
        Some(StoreValue::List(deque)) => {
            let mut delta: i64 = 0;
            for e in elements {
                delta += e.len() as i64 + 24;
                deque.push_back(e);
            }
            let new_len = deque.len() as i64;
            Ok((ModifyResult::Keep(delta), new_len))
        }
        None => Ok((ModifyResult::Keep(0), 0i64)),
        _ => Err(crate::storage::error::StorageError::WrongType),
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
        Some(StoreValue::List(deque)) => {
            // Find the pivot element
            let pivot_pos = deque.iter().position(|e| e == &pivot);
            match pivot_pos {
                Some(pos) => {
                    let insert_pos = if before { pos } else { pos + 1 };
                    // VecDeque doesn't have insert, so we convert to Vec, insert, convert back
                    let delta = element.len() as i64 + 24;
                    let mut vec: Vec<Vec<u8>> = deque.drain(..).collect();
                    vec.insert(insert_pos, element.clone());
                    *deque = vec.into();
                    Ok((ModifyResult::Keep(delta), deque.len() as i64))
                }
                None => {
                    // Pivot not found
                    Ok((ModifyResult::Keep(0), -1i64))
                }
            }
        }
        None => Ok((ModifyResult::Keep(0), 0i64)),
        _ => Err(crate::storage::error::StorageError::WrongType),
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
        Some(StoreValue::List(deque)) => {
            let resolved = resolve_index(index, deque.len()).ok_or_else(|| {
                crate::storage::error::StorageError::InternalError("index out of range".to_string())
            })?;
            let delta = element.len() as i64 - deque[resolved].len() as i64;
            deque[resolved] = element.clone();
            Ok((ModifyResult::Keep(delta), ()))
        }
        None => Err(crate::storage::error::StorageError::InternalError(
            "no such key".to_string(),
        )),
        _ => Err(crate::storage::error::StorageError::WrongType),
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
        Some(StoreValue::List(deque)) => {
            let mut removed_count = 0i64;
            if count > 0 {
                // Remove first N occurrences from head to tail
                let max_remove = count as usize;
                let mut i = 0;
                while i < deque.len() && removed_count < max_remove as i64 {
                    if deque[i] == element {
                        deque.remove(i);
                        removed_count += 1;
                    } else {
                        i += 1;
                    }
                }
            } else if count < 0 {
                // Remove first N occurrences from tail to head
                let max_remove = (-count) as usize;
                let mut i = deque.len();
                while i > 0 && removed_count < max_remove as i64 {
                    i -= 1;
                    if deque[i] == element {
                        deque.remove(i);
                        removed_count += 1;
                    }
                }
            } else {
                // count == 0: remove all occurrences
                deque.retain(|e| {
                    if e == &element {
                        removed_count += 1;
                        false
                    } else {
                        true
                    }
                });
            }

            // All removed elements have the same content, so delta = count * per-element size
            let delta = -(removed_count * (element.len() as i64 + 24));
            Ok((list_result(deque, delta), removed_count))
        }
        None => Ok((ModifyResult::Keep(0), 0i64)),
        _ => Err(crate::storage::error::StorageError::WrongType),
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

    let element_clone = element.clone();
    let matches = engine.atomic_read(key, RedisDataType::List, |data| match data {
        Some(StoreValue::List(deque)) => {
            let len = deque.len();
            if len == 0 {
                return Ok(vec![]);
            }
            let scan_len = if maxlen > 0 { maxlen.min(len) } else { len };
            let max_matches = count.unwrap_or(1);
            let find_all = max_matches == 0;
            let mut result = Vec::new();
            let mut match_count = 0i64;

            if rank > 0 {
                // Forward scan
                let mut skip = (rank - 1) as usize;
                for (idx, item) in deque.iter().enumerate().take(scan_len) {
                    if *item == element_clone {
                        if skip > 0 {
                            skip -= 1;
                        } else {
                            result.push(idx as i64);
                            match_count += 1;
                            if !find_all && match_count >= max_matches {
                                break;
                            }
                        }
                    }
                }
            } else {
                // Backward scan
                let mut skip = ((-rank) - 1) as usize;
                let start_from = if maxlen > 0 && maxlen < len {
                    len - maxlen
                } else {
                    0
                };
                for idx in (start_from..len).rev() {
                    if deque[idx] == element_clone {
                        if skip > 0 {
                            skip -= 1;
                        } else {
                            result.push(idx as i64);
                            match_count += 1;
                            if !find_all && match_count >= max_matches {
                                break;
                            }
                        }
                    }
                }
            }
            Ok(result)
        }
        None => Ok(vec![]),
        _ => Err(crate::storage::error::StorageError::WrongType),
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
                Some(StoreValue::List(deque)) => {
                    if deque.is_empty() {
                        return Ok((ModifyResult::Keep(0), None));
                    }

                    let element = if pop_left {
                        deque.pop_front().unwrap()
                    } else {
                        deque.pop_back().unwrap()
                    };

                    if push_left {
                        deque.push_front(element.clone());
                    } else {
                        deque.push_back(element.clone());
                    }

                    Ok((ModifyResult::Keep(0), Some(element)))
                }
                None => Ok((ModifyResult::Keep(0), None)),
                _ => Err(crate::storage::error::StorageError::WrongType),
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
        Some(StoreValue::List(deque)) => {
            if deque.is_empty() {
                return Ok((ModifyResult::Keep(0), None));
            }

            let element = if pop_left {
                deque.pop_front().unwrap()
            } else {
                deque.pop_back().unwrap()
            };
            let delta = -(element.len() as i64 + 24);

            Ok((list_result(deque, delta), Some(element)))
        }
        None => Ok((ModifyResult::Keep(0), None)),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    let element = match element {
        Some(e) => e,
        None => return Ok(RespValue::BulkString(None)),
    };

    // Push to destination
    engine.check_write_memory(0).await?;
    let elem_for_push = element.clone();
    engine.atomic_modify(destination, RedisDataType::List, |current| match current {
        Some(StoreValue::List(deque)) => {
            if push_left {
                deque.push_front(elem_for_push.clone());
            } else {
                deque.push_back(elem_for_push.clone());
            }
            Ok((ModifyResult::Keep(0), ()))
        }
        None => {
            let mut deque = VecDeque::new();
            deque.push_back(elem_for_push.clone());
            Ok((ModifyResult::Set(StoreValue::List(deque)), ()))
        }
        _ => Err(crate::storage::error::StorageError::WrongType),
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
            Some(StoreValue::List(deque)) => {
                if deque.is_empty() {
                    return Ok((ModifyResult::Keep(0), None));
                }

                let actual = count.min(deque.len());
                let mut popped = Vec::with_capacity(actual);
                let mut delta: i64 = 0;
                for _ in 0..actual {
                    if left {
                        if let Some(e) = deque.pop_front() {
                            delta -= e.len() as i64 + 24;
                            popped.push(e);
                        }
                    } else if let Some(e) = deque.pop_back() {
                        delta -= e.len() as i64 + 24;
                        popped.push(e);
                    }
                }

                Ok((list_result(deque, delta), Some(popped)))
            }
            None => Ok((ModifyResult::Keep(0), None)),
            _ => Err(crate::storage::error::StorageError::WrongType),
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
        Some(StoreValue::List(deque)) => {
            if deque.is_empty() {
                return Ok((ModifyResult::Keep(0), None));
            }

            let element = if left {
                deque.pop_front().unwrap()
            } else {
                deque.pop_back().unwrap()
            };
            let delta = -(element.len() as i64 + 24);

            Ok((list_result(deque, delta), Some(element)))
        }
        None => Ok((ModifyResult::Keep(0), None)),
        _ => Err(crate::storage::error::StorageError::WrongType),
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

        let key = b"test_list_poplist".to_vec();

        // Push elements using rpush
        let mut args = vec![key.clone()];
        args.push(b"one".to_vec());
        args.push(b"two".to_vec());
        args.push(b"three".to_vec());

        rpush(&engine, &args).await.unwrap();

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

        let key = b"test_list_lenlist".to_vec();

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

    // Test LPUSH/RPUSH with multiple elements consume by value (no double-clone)
    #[tokio::test]
    async fn test_lpush_rpush_multi_elements() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // LPUSH with multiple elements: LPUSH key a b c -> list is [c, b, a]
        let args = vec![
            b"mlist".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
        ];
        let result = lpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let range_args = vec![b"mlist".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &range_args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected array from LRANGE");
        }

        // RPUSH with multiple elements: RPUSH key2 x y z -> list is [x, y, z]
        let args = vec![
            b"mlist2".to_vec(),
            b"x".to_vec(),
            b"y".to_vec(),
            b"z".to_vec(),
        ];
        let result = rpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let range_args = vec![b"mlist2".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &range_args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            assert_eq!(values[0], RespValue::BulkString(Some(b"x".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"y".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"z".to_vec())));
        } else {
            panic!("Expected array from LRANGE");
        }

        // LPUSH onto existing list: LPUSH mlist2 w -> list is [w, x, y, z]
        let args = vec![b"mlist2".to_vec(), b"w".to_vec()];
        let result = lpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(4));

        let range_args = vec![b"mlist2".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &range_args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values[0], RespValue::BulkString(Some(b"w".to_vec())));
            assert_eq!(values[3], RespValue::BulkString(Some(b"z".to_vec())));
        } else {
            panic!("Expected array from LRANGE");
        }

        // RPUSH onto existing list: RPUSH mlist2 end -> list is [w, x, y, z, end]
        let args = vec![b"mlist2".to_vec(), b"end".to_vec()];
        let result = rpush(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let range_args = vec![b"mlist2".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &range_args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 5);
            assert_eq!(values[4], RespValue::BulkString(Some(b"end".to_vec())));
        } else {
            panic!("Expected array from LRANGE");
        }
    }

    // Test LPUSHX/RPUSHX with multiple elements consume by value
    #[tokio::test]
    async fn test_lpushx_rpushx_multi_elements() {
        let config = StorageConfig::default();
        let engine = StorageEngine::new(config);

        // LPUSHX on non-existent key returns 0
        let args = vec![b"nxlist".to_vec(), b"a".to_vec(), b"b".to_vec()];
        let result = lpushx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // RPUSHX on non-existent key returns 0
        let args = vec![b"nxlist".to_vec(), b"a".to_vec(), b"b".to_vec()];
        let result = rpushx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Create the list first
        let args = vec![b"nxlist".to_vec(), b"init".to_vec()];
        rpush(&engine, &args).await.unwrap();

        // LPUSHX with multiple elements on existing key
        let args = vec![b"nxlist".to_vec(), b"a".to_vec(), b"b".to_vec()];
        let result = lpushx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let range_args = vec![b"nxlist".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &range_args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 3);
            // LPUSHX pushes a then b to front: [b, a, init]
            assert_eq!(values[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(values[1], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(values[2], RespValue::BulkString(Some(b"init".to_vec())));
        } else {
            panic!("Expected array from LRANGE");
        }

        // RPUSHX with multiple elements on existing key
        let args = vec![b"nxlist".to_vec(), b"x".to_vec(), b"y".to_vec()];
        let result = rpushx(&engine, &args).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));

        let range_args = vec![b"nxlist".to_vec(), b"0".to_vec(), b"-1".to_vec()];
        let result = lrange(&engine, &range_args).await.unwrap();
        if let RespValue::Array(Some(values)) = result {
            assert_eq!(values.len(), 5);
            assert_eq!(values[3], RespValue::BulkString(Some(b"x".to_vec())));
            assert_eq!(values[4], RespValue::BulkString(Some(b"y".to_vec())));
        } else {
            panic!("Expected array from LRANGE");
        }
    }
}
