use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;
use crate::storage::value::{ModifyResult, NativeHashSet, StoreValue};

use crate::utils::glob::glob_match;

/// Helper enum for SPOP atomic result
enum SpopResult {
    Empty(bool), // bool = has_count argument
    Single(Vec<u8>),
    Multiple(Vec<Vec<u8>>),
}

/// Redis SADD command - Add members to a set
/// Uses atomic_modify for thread-safe read-modify-write
pub async fn sadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let members = &args[1..];

    // Deduplicate input members
    let mut unique_members = NativeHashSet::default();
    for member in members {
        unique_members.insert(member.clone());
    }

    engine.check_write_memory(0).await?;
    let new_count = engine.atomic_modify(key, RedisDataType::Set, |current| {
        match current {
            Some(StoreValue::Set(set)) => {
                let mut added = 0i64;
                let mut delta: i64 = 0;
                for member in &unique_members {
                    if set.insert(member.clone()) {
                        added += 1;
                        delta += member.len() as i64 + 32;
                    }
                }
                Ok((ModifyResult::Keep(delta), added))
            }
            None => {
                let mut set = NativeHashSet::default();
                let mut added = 0i64;
                for member in &unique_members {
                    if set.insert(member.clone()) {
                        added += 1;
                    }
                }
                Ok((ModifyResult::Set(StoreValue::Set(set)), added))
            }
            _ => Err(crate::storage::error::StorageError::WrongType),
        }
    })?;

    Ok(RespValue::Integer(new_count))
}

/// Redis SREM command - Remove members from a set
/// Uses atomic_modify for thread-safe read-modify-write
pub async fn srem(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let members = &args[1..];
    let members_owned: Vec<Vec<u8>> = members.to_vec();

    let removed = engine.atomic_modify(key, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => {
            let mut count = 0i64;
            let mut delta: i64 = 0;
            for member in &members_owned {
                if set.remove(member) {
                    count += 1;
                    delta -= member.len() as i64 + 32;
                }
            }

            if set.is_empty() && count > 0 {
                Ok((ModifyResult::Delete, count))
            } else if count > 0 {
                Ok((ModifyResult::Keep(delta), count))
            } else {
                Ok((ModifyResult::KeepUnchanged, 0))
            }
        }
        None => Ok((ModifyResult::KeepUnchanged, 0)),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::Integer(removed))
}

/// Redis SMEMBERS command - Get all members of a set
pub async fn smembers(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let members = engine.atomic_read(key, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => {
            Ok(set.iter().cloned().collect::<Vec<_>>())
        }
        None => Ok(vec![]),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    let result: Vec<RespValue> = members
        .into_iter()
        .map(|member| RespValue::BulkString(Some(member)))
        .collect();

    Ok(RespValue::Array(Some(result)))
}

/// Redis SISMEMBER command - Check if a value is a member of a set
pub async fn sismember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let member = &args[1];

    let exists = engine.atomic_read(key, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => Ok(set.contains(member.as_slice())),
        None => Ok(false),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::Integer(if exists { 1 } else { 0 }))
}

/// Redis SCARD command - Get the cardinality (number of members) of a set
pub async fn scard(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];

    let count = engine.atomic_read(key, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => Ok(set.len() as i64),
        None => Ok(0),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::Integer(count))
}

/// Redis SPOP command - Remove and return random members from a set
/// Uses atomic_modify for thread-safe read-modify-write
pub async fn spop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let count = if args.len() == 2 {
        match String::from_utf8_lossy(&args[1]).parse::<usize>() {
            Ok(n) => Some(n),
            Err(_) => return Err(CommandError::NotANumber),
        }
    } else {
        None
    };

    let result = engine.atomic_modify(key, RedisDataType::Set, |current| {
        match current {
            Some(StoreValue::Set(set)) => {
                if set.is_empty() {
                    return Ok((ModifyResult::Keep(0), SpopResult::Empty(count.is_some())));
                }

                if let Some(count) = count {
                    let actual_count = std::cmp::min(count, set.len());

                    if actual_count >= set.len() {
                        // Pop all elements - just drain (Delete handles full entry removal)
                        let popped: Vec<Vec<u8>> = set.drain().collect();
                        Ok((ModifyResult::Delete, SpopResult::Multiple(popped)))
                    } else if actual_count == 1 {
                        // Fast path for SPOP key 1: O(N/2) avg instead of O(N) drain+rebuild
                        let idx = fastrand::usize(0..set.len());
                        let member = set.iter().nth(idx).cloned().unwrap();
                        set.remove(&member);
                        let delta = -(member.len() as i64 + 32);
                        if set.is_empty() {
                            Ok((ModifyResult::Delete, SpopResult::Multiple(vec![member])))
                        } else {
                            Ok((ModifyResult::Keep(delta), SpopResult::Multiple(vec![member])))
                        }
                    } else {
                        // Fisher-Yates partial shuffle with ownership transfer (no clone)
                        let mut all: Vec<_> = set.drain().collect();
                        for i in 0..actual_count {
                            let j = fastrand::usize(i..all.len());
                            all.swap(i, j);
                        }
                        let remaining_vec = all.split_off(actual_count);
                        let popped = all;
                        let delta: i64 = -(popped.iter().map(|m| m.len() as i64 + 32).sum::<i64>());
                        *set = remaining_vec.into_iter().collect();
                        Ok((ModifyResult::Keep(delta), SpopResult::Multiple(popped)))
                    }
                } else {
                    // Return single random element - pick random via iterator, remove in place
                    let idx = fastrand::usize(0..set.len());
                    let member = set.iter().nth(idx).cloned().unwrap();
                    set.remove(&member);
                    let delta = -(member.len() as i64 + 32);

                    if set.is_empty() {
                        Ok((ModifyResult::Delete, SpopResult::Single(member)))
                    } else {
                        Ok((ModifyResult::Keep(delta), SpopResult::Single(member)))
                    }
                }
            }
            None => Ok((ModifyResult::Keep(0), SpopResult::Empty(count.is_some()))),
            _ => Err(crate::storage::error::StorageError::WrongType),
        }
    })?;

    match result {
        SpopResult::Empty(has_count) => {
            if has_count {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            }
        }
        SpopResult::Single(member) => Ok(RespValue::BulkString(Some(member))),
        SpopResult::Multiple(members) => {
            let result: Vec<_> = members
                .into_iter()
                .map(|m| RespValue::BulkString(Some(m)))
                .collect();
            Ok(RespValue::Array(Some(result)))
        }
    }
}

/// Helper function to get a set from storage using atomic_read
async fn get_set(engine: &StorageEngine, key: &[u8]) -> Result<NativeHashSet, CommandError> {
    engine
        .atomic_read(key, RedisDataType::Set, |current| match current {
            Some(StoreValue::Set(set)) => Ok(set.clone()),
            None => Ok(NativeHashSet::default()),
            _ => Err(crate::storage::error::StorageError::WrongType),
        })
        .map_err(CommandError::from)
}

/// Redis SINTER command - Return the intersection of multiple sets
pub async fn sinter(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Get the first set
    let mut result = get_set(engine, &args[0]).await?;

    // Intersect with remaining sets
    for key in &args[1..] {
        let other_set = get_set(engine, key).await?;
        result = result.intersection(&other_set).cloned().collect();
    }

    // Convert to array of bulk strings
    let members: Vec<RespValue> = result
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();

    Ok(RespValue::Array(Some(members)))
}

/// Redis SINTERSTORE command - Store intersection of sets in destination key
pub async fn sinterstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let destination = &args[0];
    let keys = &args[1..];

    // Lock all involved keys (sources + destination) for cross-key atomicity
    let all_keys: Vec<Vec<u8>> = std::iter::once(destination.clone())
        .chain(keys.iter().cloned())
        .collect();
    let _guard = engine.lock_keys(&all_keys).await;

    // Get the first set
    let mut result = get_set(engine, &keys[0]).await?;

    // Intersect with remaining sets
    for key in &keys[1..] {
        let other_set = get_set(engine, key).await?;
        result = result.intersection(&other_set).cloned().collect();
    }

    let count = result.len() as i64;

    // Store the result (or delete if empty)
    if result.is_empty() {
        let _ = engine.del(destination).await;
    } else {
        engine
            .set_with_type(destination.to_vec(), StoreValue::Set(result), None)
            .await?;
    }

    Ok(RespValue::Integer(count))
}

/// Redis SUNION command - Return the union of multiple sets
pub async fn sunion(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let mut result = NativeHashSet::default();

    // Union all sets
    for key in args {
        let set = get_set(engine, key).await?;
        for member in set {
            result.insert(member);
        }
    }

    // Convert to array of bulk strings
    let members: Vec<RespValue> = result
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();

    Ok(RespValue::Array(Some(members)))
}

/// Redis SUNIONSTORE command - Store union of sets in destination key
pub async fn sunionstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let destination = &args[0];
    let keys = &args[1..];

    // Lock all involved keys (sources + destination) for cross-key atomicity
    let all_keys: Vec<Vec<u8>> = std::iter::once(destination.clone())
        .chain(keys.iter().cloned())
        .collect();
    let _guard = engine.lock_keys(&all_keys).await;

    let mut result = NativeHashSet::default();

    // Union all sets
    for key in keys {
        let set = get_set(engine, key).await?;
        for member in set {
            result.insert(member);
        }
    }

    let count = result.len() as i64;

    // Store the result (or delete if empty)
    if result.is_empty() {
        let _ = engine.del(destination).await;
    } else {
        engine
            .set_with_type(destination.to_vec(), StoreValue::Set(result), None)
            .await?;
    }

    Ok(RespValue::Integer(count))
}

/// Redis SDIFF command - Return the difference between the first set and all subsequent sets
pub async fn sdiff(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // Get the first set
    let mut result = get_set(engine, &args[0]).await?;

    // Subtract remaining sets
    for key in &args[1..] {
        let other_set = get_set(engine, key).await?;
        for member in &other_set {
            result.remove(member);
        }
    }

    // Convert to array of bulk strings
    let members: Vec<RespValue> = result
        .into_iter()
        .map(|m| RespValue::BulkString(Some(m)))
        .collect();

    Ok(RespValue::Array(Some(members)))
}

/// Redis SDIFFSTORE command - Store difference of sets in destination key
pub async fn sdiffstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let destination = &args[0];
    let keys = &args[1..];

    // Lock all involved keys (sources + destination) for cross-key atomicity
    let all_keys: Vec<Vec<u8>> = std::iter::once(destination.clone())
        .chain(keys.iter().cloned())
        .collect();
    let _guard = engine.lock_keys(&all_keys).await;

    // Get the first set
    let mut result = get_set(engine, &keys[0]).await?;

    // Subtract remaining sets
    for key in &keys[1..] {
        let other_set = get_set(engine, key).await?;
        for member in &other_set {
            result.remove(member);
        }
    }

    let count = result.len() as i64;

    // Store the result (or delete if empty)
    if result.is_empty() {
        let _ = engine.del(destination).await;
    } else {
        engine
            .set_with_type(destination.to_vec(), StoreValue::Set(result), None)
            .await?;
    }

    Ok(RespValue::Integer(count))
}

/// Redis SRANDMEMBER command - Get random member(s) from a set without removing them
pub async fn srandmember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let count = if args.len() == 2 {
        match String::from_utf8_lossy(&args[1]).parse::<i64>() {
            Ok(n) => Some(n),
            Err(_) => return Err(CommandError::NotANumber),
        }
    } else {
        None
    };

    // Get the set members
    let members_vec = engine.atomic_read(key, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => Ok(Some(set.iter().cloned().collect::<Vec<_>>())),
        None => Ok(None),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    let set_vec = match members_vec {
        Some(v) => v,
        None => {
            return if count.is_some() {
                Ok(RespValue::Array(Some(vec![])))
            } else {
                Ok(RespValue::BulkString(None))
            };
        }
    };

    if set_vec.is_empty() {
        return if count.is_some() {
            Ok(RespValue::Array(Some(vec![])))
        } else {
            Ok(RespValue::BulkString(None))
        };
    }

    if let Some(count) = count {
        // Return multiple elements (with possible repetition if count > set size)
        let mut result = Vec::new();
        let mut set_vec = set_vec;

        if count >= 0 {
            // Positive count: return unique random elements
            let actual_count = std::cmp::min(count as usize, set_vec.len());
            // Fisher-Yates partial shuffle for random selection
            for i in 0..actual_count {
                let j = fastrand::usize(i..set_vec.len());
                set_vec.swap(i, j);
            }
            for item in set_vec.iter().take(actual_count) {
                result.push(RespValue::BulkString(Some(item.clone())));
            }
        } else {
            // Negative count: return with possible repetition
            let actual_count = (-count) as usize;
            for _ in 0..actual_count {
                let index = fastrand::usize(0..set_vec.len());
                result.push(RespValue::BulkString(Some(set_vec[index].clone())));
            }
        }

        Ok(RespValue::Array(Some(result)))
    } else {
        // Return single element
        let index = fastrand::usize(0..set_vec.len());
        Ok(RespValue::BulkString(Some(set_vec[index].clone())))
    }
}

/// Redis SMOVE command - Move a member from one set to another
/// Uses lock_keys for cross-key atomicity, atomic_modify for single-key operations
pub async fn smove(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let source = &args[0];
    let destination = &args[1];
    let member = &args[2];

    if source == destination {
        // Same key: just check membership, no-op — use KeepUnchanged to avoid
        // triggering WATCH invalidation and write counters for a true no-op
        let exists = engine.atomic_modify(source, RedisDataType::Set, |current| match current {
            Some(StoreValue::Set(set)) => {
                let has_member = set.contains(member.as_slice());
                Ok((ModifyResult::KeepUnchanged, has_member))
            }
            None => Ok((ModifyResult::KeepUnchanged, false)),
            _ => Err(crate::storage::error::StorageError::WrongType),
        })?;
        return Ok(RespValue::Integer(if exists { 1 } else { 0 }));
    }

    // Different keys: use lock_keys for cross-key atomicity
    let _guard = engine
        .lock_keys(&[source.clone(), destination.clone()])
        .await;

    // Remove from source using atomic_modify
    let member_clone = member.clone();
    let removed = engine.atomic_modify(source, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => {
            if !set.remove(&member_clone) {
                return Ok((ModifyResult::KeepUnchanged, false));
            }
            let delta = -(member_clone.len() as i64 + 32);

            if set.is_empty() {
                Ok((ModifyResult::Delete, true))
            } else {
                Ok((ModifyResult::Keep(delta), true))
            }
        }
        None => Ok((ModifyResult::KeepUnchanged, false)),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    if !removed {
        return Ok(RespValue::Integer(0));
    }

    // Add to destination using atomic_modify
    engine.check_write_memory(0).await?;
    let member_clone = member.clone();
    engine.atomic_modify(destination, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => {
            let was_new = set.insert(member_clone.clone());
            if was_new {
                Ok((ModifyResult::Keep(member_clone.len() as i64 + 32), ()))
            } else {
                Ok((ModifyResult::KeepUnchanged, ()))
            }
        }
        None => {
            let mut set = NativeHashSet::default();
            set.insert(member_clone.clone());
            Ok((ModifyResult::Set(StoreValue::Set(set)), ()))
        }
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    Ok(RespValue::Integer(1))
}

/// Redis SINTERCARD command - Return the cardinality of the intersection with optional LIMIT
pub async fn sintercard(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    // First arg is numkeys
    let numkeys = String::from_utf8_lossy(&args[0])
        .parse::<usize>()
        .map_err(|_| {
            CommandError::InvalidArgument("numkeys must be a positive integer".to_string())
        })?;

    if numkeys == 0 {
        return Err(CommandError::InvalidArgument(
            "ERR numkeys can't be zero".to_string(),
        ));
    }

    if args.len() < 1 + numkeys {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let keys = &args[1..1 + numkeys];

    // Parse optional LIMIT
    let mut limit: usize = 0; // 0 means no limit
    let mut i = 1 + numkeys;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        if opt == "LIMIT" {
            if i + 1 >= args.len() {
                return Err(CommandError::WrongNumberOfArguments);
            }
            limit = String::from_utf8_lossy(&args[i + 1])
                .parse::<usize>()
                .map_err(|_| {
                    CommandError::InvalidArgument(
                        "LIMIT must be a non-negative integer".to_string(),
                    )
                })?;
            i += 2;
        } else {
            return Err(CommandError::InvalidArgument(format!(
                "ERR syntax error, unexpected: {}",
                opt
            )));
        }
    }

    // Get the first set
    let mut result = get_set(engine, &keys[0]).await?;

    // Intersect with remaining sets
    for key in &keys[1..] {
        let other_set = get_set(engine, key).await?;
        result = result.intersection(&other_set).cloned().collect();
        // Early exit if intersection becomes empty
        if result.is_empty() {
            return Ok(RespValue::Integer(0));
        }
    }

    let count = result.len();
    if limit > 0 && count > limit {
        Ok(RespValue::Integer(limit as i64))
    } else {
        Ok(RespValue::Integer(count as i64))
    }
}

/// Redis SMISMEMBER command - Check multiple members for set membership
pub async fn smismember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let members = &args[1..];

    let members_owned: Vec<Vec<u8>> = members.to_vec();
    let results = engine.atomic_read(key, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => {
            Ok(members_owned
                .iter()
                .map(|m| if set.contains(m.as_slice()) { 1i64 } else { 0 })
                .collect::<Vec<_>>())
        }
        None => Ok(vec![0i64; members_owned.len()]),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    let resp_results: Vec<RespValue> = results
        .into_iter()
        .map(RespValue::Integer)
        .collect();

    Ok(RespValue::Array(Some(resp_results)))
}

/// Redis SSCAN command - Incrementally iterate set members
pub async fn sscan(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let cursor_str = String::from_utf8_lossy(&args[1]);
    let cursor = cursor_str.parse::<u64>().map_err(|_| {
        CommandError::InvalidArgument("Cursor value must be an integer".to_string())
    })?;

    // Parse optional MATCH and COUNT
    let mut pattern = String::from("*");
    let mut count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = String::from_utf8_lossy(&args[i]).to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                pattern = String::from_utf8_lossy(&args[i + 1]).to_string();
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                count = String::from_utf8_lossy(&args[i + 1])
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "COUNT must be a positive integer".to_string(),
                        )
                    })?;
                i += 2;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "Unsupported SSCAN option: {}",
                    opt
                )));
            }
        }
    }

    // Get the set members
    let set_members = engine.atomic_read(key, RedisDataType::Set, |current| match current {
        Some(StoreValue::Set(set)) => Ok(Some(set.iter().cloned().collect::<Vec<_>>())),
        None => Ok(None),
        _ => Err(crate::storage::error::StorageError::WrongType),
    })?;

    let set_members = match set_members {
        Some(m) => m,
        None => {
            // Key doesn't exist, return cursor 0 with empty array
            return Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"0".to_vec())),
                RespValue::Array(Some(vec![])),
            ])));
        }
    };

    // Collect and filter members
    let mut members = set_members;
    members.sort(); // Sort for deterministic cursor behavior

    // Apply MATCH filter
    if pattern != "*" {
        members.retain(|m| {
            let s = String::from_utf8_lossy(m);
            glob_match(&pattern, &s)
        });
    }

    let total = members.len();
    let start = if cursor == 0 || cursor as usize >= total {
        0
    } else {
        cursor as usize
    };

    let end = (start + count).min(total);
    let next_cursor = if end >= total { 0 } else { end as u64 };

    let result_members: Vec<RespValue> = members[start..end]
        .iter()
        .map(|m| RespValue::BulkString(Some(m.clone())))
        .collect();

    Ok(RespValue::Array(Some(vec![
        RespValue::BulkString(Some(next_cursor.to_string().into_bytes())),
        RespValue::Array(Some(result_members)),
    ])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::collections::HashSet;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_set_commands() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Test SADD
        let sadd_args = vec![b"set1".to_vec(), b"member1".to_vec(), b"member2".to_vec()];
        let result = sadd(&engine, &sadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(2)); // Added 2 members

        // Add a duplicate and a new member
        let sadd_args = vec![b"set1".to_vec(), b"member2".to_vec(), b"member3".to_vec()];
        let result = sadd(&engine, &sadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Added 1 new member

        // Test SISMEMBER
        let sismember_args = vec![b"set1".to_vec(), b"member1".to_vec()];
        let result = sismember(&engine, &sismember_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Is a member

        let sismember_args = vec![b"set1".to_vec(), b"nonexistent".to_vec()];
        let result = sismember(&engine, &sismember_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // Not a member

        // Test SMEMBERS
        let smembers_args = vec![b"set1".to_vec()];
        let result = smembers(&engine, &smembers_args).await.unwrap();
        if let RespValue::Array(Some(members)) = result {
            assert_eq!(members.len(), 3);

            // Verify that all expected members are present
            let mut found_member1 = false;
            let mut found_member2 = false;
            let mut found_member3 = false;

            for member in members {
                if let RespValue::BulkString(Some(data)) = member {
                    if data == b"member1" {
                        found_member1 = true;
                    } else if data == b"member2" {
                        found_member2 = true;
                    } else if data == b"member3" {
                        found_member3 = true;
                    }
                }
            }

            assert!(found_member1, "member1 not found in SMEMBERS results");
            assert!(found_member2, "member2 not found in SMEMBERS results");
            assert!(found_member3, "member3 not found in SMEMBERS results");
        } else {
            panic!("Expected array response from SMEMBERS");
        }

        // Test SREM
        let srem_args = vec![
            b"set1".to_vec(),
            b"member1".to_vec(),
            b"nonexistent".to_vec(),
        ];
        let result = srem(&engine, &srem_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // Removed 1 member

        // Verify that the member was removed
        let sismember_args = vec![b"set1".to_vec(), b"member1".to_vec()];
        let result = sismember(&engine, &sismember_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // No longer a member
    }

    #[tokio::test]
    async fn test_smove_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Create source set {a, b, c}
        sadd(
            &engine,
            &[b"src".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
        // Create destination set {d}
        sadd(&engine, &[b"dst".to_vec(), b"d".to_vec()])
            .await
            .unwrap();

        // Move "b" from src to dst
        let result = smove(&engine, &[b"src".to_vec(), b"dst".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Verify "b" is no longer in src
        let result = sismember(&engine, &[b"src".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Verify "b" is in dst
        let result = sismember(&engine, &[b"dst".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // src should still have a and c
        let result = scard(&engine, &[b"src".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_smove_nonexistent_member() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(&engine, &[b"src".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        sadd(&engine, &[b"dst".to_vec(), b"d".to_vec()])
            .await
            .unwrap();

        // Try to move nonexistent member
        let result = smove(&engine, &[b"src".to_vec(), b"dst".to_vec(), b"x".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_smove_nonexistent_source() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Source doesn't exist
        let result = smove(
            &engine,
            &[b"nosrc".to_vec(), b"dst".to_vec(), b"a".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_smove_creates_destination() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(&engine, &[b"src".to_vec(), b"a".to_vec()])
            .await
            .unwrap();

        // dst doesn't exist yet - smove should create it
        let result = smove(
            &engine,
            &[b"src".to_vec(), b"newdst".to_vec(), b"a".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // src should be deleted (empty after removing only member)
        let result = scard(&engine, &[b"src".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // dst should have the member
        let result = sismember(&engine, &[b"newdst".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_sintercard_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(
            &engine,
            &[b"s1".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
        sadd(
            &engine,
            &[b"s2".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()],
        )
        .await
        .unwrap();

        // Intersection of s1 and s2 has {b, c} => cardinality 2
        let result = sintercard(&engine, &[b"2".to_vec(), b"s1".to_vec(), b"s2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_sintercard_with_limit() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(
            &engine,
            &[b"s1".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
        sadd(
            &engine,
            &[
                b"s2".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
                b"d".to_vec(),
            ],
        )
        .await
        .unwrap();

        // Intersection has {a, b, c} = 3, but LIMIT 1
        let result = sintercard(
            &engine,
            &[
                b"2".to_vec(),
                b"s1".to_vec(),
                b"s2".to_vec(),
                b"LIMIT".to_vec(),
                b"1".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_sintercard_empty_intersection() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(&engine, &[b"s1".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        sadd(&engine, &[b"s2".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        let result = sintercard(&engine, &[b"2".to_vec(), b"s1".to_vec(), b"s2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_smismember_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(
            &engine,
            &[
                b"myset".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ],
        )
        .await
        .unwrap();

        let result = smismember(
            &engine,
            &[
                b"myset".to_vec(),
                b"a".to_vec(),
                b"x".to_vec(),
                b"b".to_vec(),
                b"y".to_vec(),
            ],
        )
        .await
        .unwrap();

        assert_eq!(
            result,
            RespValue::Array(Some(vec![
                RespValue::Integer(1), // a exists
                RespValue::Integer(0), // x not
                RespValue::Integer(1), // b exists
                RespValue::Integer(0), // y not
            ]))
        );
    }

    #[tokio::test]
    async fn test_smismember_nonexistent_key() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let result = smismember(&engine, &[b"nokey".to_vec(), b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        assert_eq!(
            result,
            RespValue::Array(Some(vec![RespValue::Integer(0), RespValue::Integer(0),]))
        );
    }

    #[tokio::test]
    async fn test_sscan_basic() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(
            &engine,
            &[
                b"scanset".to_vec(),
                b"a".to_vec(),
                b"b".to_vec(),
                b"c".to_vec(),
            ],
        )
        .await
        .unwrap();

        // SSCAN with cursor 0
        let result = sscan(&engine, &[b"scanset".to_vec(), b"0".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            // First element is cursor
            if let RespValue::BulkString(Some(cursor)) = &items[0] {
                // Cursor should be "0" since we have only 3 elements and default count is 10
                assert_eq!(cursor, b"0");
            }
            // Second element is array of members
            if let RespValue::Array(Some(members)) = &items[1] {
                assert_eq!(members.len(), 3);
            } else {
                panic!("Expected array of members");
            }
        } else {
            panic!("Expected array response");
        }
    }

    #[tokio::test]
    async fn test_sscan_with_match() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        sadd(
            &engine,
            &[
                b"scanset2".to_vec(),
                b"alpha".to_vec(),
                b"beta".to_vec(),
                b"gamma".to_vec(),
                b"delta".to_vec(),
            ],
        )
        .await
        .unwrap();

        // SSCAN with MATCH *a*
        let result = sscan(
            &engine,
            &[
                b"scanset2".to_vec(),
                b"0".to_vec(),
                b"MATCH".to_vec(),
                b"*a*".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result
            && let RespValue::Array(Some(members)) = &items[1]
        {
            // Should match: alpha, beta, gamma, delta (all contain 'a')
            assert!(members.len() >= 3); // alpha, gamma, delta at minimum
            for m in members {
                if let RespValue::BulkString(Some(data)) = m {
                    let s = String::from_utf8_lossy(data);
                    assert!(s.contains('a'), "Member '{}' should match pattern *a*", s);
                }
            }
        }
    }

    #[tokio::test]
    async fn test_sscan_nonexistent_key() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let result = sscan(&engine, &[b"nokey".to_vec(), b"0".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            if let RespValue::BulkString(Some(cursor)) = &items[0] {
                assert_eq!(cursor, b"0");
            }
            if let RespValue::Array(Some(members)) = &items[1] {
                assert!(members.is_empty());
            }
        }
    }

    #[tokio::test]
    async fn test_sscan_with_count() {
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Add many members
        for i in 0..20 {
            sadd(
                &engine,
                &[b"bigset".to_vec(), format!("member{}", i).into_bytes()],
            )
            .await
            .unwrap();
        }

        // SSCAN with COUNT 5
        let result = sscan(
            &engine,
            &[
                b"bigset".to_vec(),
                b"0".to_vec(),
                b"COUNT".to_vec(),
                b"5".to_vec(),
            ],
        )
        .await
        .unwrap();

        if let RespValue::Array(Some(items)) = result {
            if let RespValue::BulkString(Some(cursor)) = &items[0] {
                // Cursor should be non-zero since we have 20 elements but COUNT is 5
                let cursor_val: u64 = String::from_utf8_lossy(cursor).parse().unwrap();
                assert!(cursor_val > 0, "Cursor should be non-zero for partial scan");
            }
            if let RespValue::Array(Some(members)) = &items[1] {
                assert_eq!(members.len(), 5);
            }
        }
    }

    #[tokio::test]
    async fn test_concurrent_sadd_same_set() {
        // Two clients racing SADD on the same set - no members should be lost
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..10 {
                    let args = vec![
                        b"race_set".to_vec(),
                        format!("member_{}_{}", i, j).into_bytes(),
                    ];
                    sadd(&eng, &args).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 100 unique members should be present
        let result = scard(&engine, &[b"race_set".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(100));
    }

    #[tokio::test]
    async fn test_concurrent_srem_same_set() {
        // Pre-populate a set, then have multiple tasks remove different members concurrently
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Add 100 members
        for i in 0..100 {
            let args = vec![b"srem_race".to_vec(), format!("member_{}", i).into_bytes()];
            sadd(&engine, &args).await.unwrap();
        }

        let result = scard(&engine, &[b"srem_race".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(100));

        // 10 tasks each remove 10 distinct members
        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let mut removed = 0i64;
                for j in 0..10 {
                    let member_idx = i * 10 + j;
                    let args = vec![
                        b"srem_race".to_vec(),
                        format!("member_{}", member_idx).into_bytes(),
                    ];
                    if let Ok(RespValue::Integer(n)) = srem(&eng, &args).await {
                        removed += n;
                    }
                }
                removed
            }));
        }

        let mut total_removed = 0i64;
        for h in handles {
            total_removed += h.await.unwrap();
        }

        // All 100 members should have been removed (each exactly once)
        assert_eq!(total_removed, 100);
        let result = scard(&engine, &[b"srem_race".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_concurrent_spop_no_duplicates() {
        // Pre-populate a set, then have multiple tasks SPOP concurrently
        // Verify no member is popped twice
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Add 50 members
        for i in 0..50 {
            let args = vec![b"spop_race".to_vec(), format!("member_{}", i).into_bytes()];
            sadd(&engine, &args).await.unwrap();
        }

        // 10 tasks each pop 5 members
        let mut handles = Vec::new();
        for _i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let mut popped = Vec::new();
                for _j in 0..5 {
                    let args = vec![b"spop_race".to_vec()];
                    match spop(&eng, &args).await.unwrap() {
                        RespValue::BulkString(Some(m)) => popped.push(m),
                        RespValue::BulkString(None) => {} // set became empty
                        _ => panic!("unexpected response"),
                    }
                }
                popped
            }));
        }

        let mut all_popped = Vec::new();
        for h in handles {
            all_popped.extend(h.await.unwrap());
        }

        // Verify no duplicates
        let unique: HashSet<Vec<u8>> = all_popped.iter().cloned().collect();
        assert_eq!(
            unique.len(),
            all_popped.len(),
            "SPOP returned duplicate members"
        );
        // All 50 members should have been popped
        assert_eq!(all_popped.len(), 50);
    }

    #[tokio::test]
    async fn test_concurrent_smove_preserves_total() {
        // Multiple tasks SMOVE from source to dest concurrently
        // Total members across both sets should be conserved
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        // Add 100 members to source
        for i in 0..100 {
            let args = vec![b"smove_src".to_vec(), format!("member_{}", i).into_bytes()];
            sadd(&engine, &args).await.unwrap();
        }

        // 10 tasks each move 10 distinct members
        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                let mut moved = 0i64;
                for j in 0..10 {
                    let member_idx = i * 10 + j;
                    let args = vec![
                        b"smove_src".to_vec(),
                        b"smove_dst".to_vec(),
                        format!("member_{}", member_idx).into_bytes(),
                    ];
                    if let Ok(RespValue::Integer(n)) = smove(&eng, &args).await {
                        moved += n;
                    }
                }
                moved
            }));
        }

        let mut total_moved = 0i64;
        for h in handles {
            total_moved += h.await.unwrap();
        }

        // All 100 should have been moved
        assert_eq!(total_moved, 100);
        // Source should be empty
        let src_card = scard(&engine, &[b"smove_src".to_vec()]).await.unwrap();
        assert_eq!(src_card, RespValue::Integer(0));
        // Destination should have all 100
        let dst_card = scard(&engine, &[b"smove_dst".to_vec()]).await.unwrap();
        assert_eq!(dst_card, RespValue::Integer(100));
    }
}
