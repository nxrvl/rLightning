use crate::command::types::blocking::BlockingManager;
use crate::command::utils::bytes_to_string;
use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::error::StorageError;
use crate::storage::item::RedisDataType;
use crate::storage::value::{ModifyResult, StoreValue};
use futures::future::select_all;
use ordered_float::OrderedFloat;
use std::collections::HashMap;
use std::time::{Duration, Instant};

// SortedSetData is now defined in crate::storage::value.
// Re-export for backward compatibility with code that imports from this module.
pub use crate::storage::value::SortedSetData;

/// Extract a SortedSetData for read-only access from a StoreValue reference.
fn read_zset(existing: Option<&StoreValue>) -> Result<SortedSetData, StorageError> {
    match existing {
        Some(StoreValue::ZSet(ss)) => Ok(ss.clone()),
        Some(_) => Err(StorageError::WrongType),
        None => Ok(SortedSetData::new()),
    }
}

/// Extract a mutable SortedSetData from a StoreValue reference for modification.
/// Takes ownership of the data via std::mem::take, caller must put it back or return ModifyResult::Set.
fn take_zset_mut(existing: Option<&mut StoreValue>) -> Result<SortedSetData, StorageError> {
    match existing {
        Some(StoreValue::ZSet(ss)) => Ok(std::mem::take(ss)),
        Some(_) => Err(StorageError::WrongType),
        None => Ok(SortedSetData::new()),
    }
}

/// Convert a SortedSetData back to a ModifyResult.
fn zset_result(ss: SortedSetData) -> ModifyResult {
    if ss.is_empty() {
        ModifyResult::Delete
    } else {
        ModifyResult::Set(StoreValue::ZSet(ss))
    }
}

fn load_sorted_set_readonly(
    engine: &StorageEngine,
    key: &[u8],
) -> Result<Option<SortedSetData>, CommandError> {
    engine
        .atomic_read(key, RedisDataType::ZSet, |data| match data {
            Some(StoreValue::ZSet(ss)) => Ok(Some(ss.clone())),
            Some(_) => Err(StorageError::WrongType),
            None => Ok(None),
        })
        .map_err(|e| match e {
            StorageError::WrongType => CommandError::WrongType,
            other => CommandError::StorageError(other.to_string()),
        })
}

fn format_score(score: f64) -> String {
    if score == f64::INFINITY {
        "inf".to_string()
    } else if score == f64::NEG_INFINITY {
        "-inf".to_string()
    } else if score.fract() == 0.0 && score.abs() < (1_i64 << 53) as f64 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

fn parse_score_bound(s: &str) -> Result<(f64, bool), CommandError> {
    if s == "-inf" {
        return Ok((f64::NEG_INFINITY, false));
    }
    if s == "+inf" || s == "inf" {
        return Ok((f64::INFINITY, false));
    }
    let (val_str, exclusive) = if let Some(stripped) = s.strip_prefix('(') {
        (stripped, true)
    } else {
        (s, false)
    };
    let val = val_str
        .parse::<f64>()
        .map_err(|_| CommandError::InvalidArgument("min or max is not a float".to_string()))?;
    Ok((val, exclusive))
}

fn parse_lex_bound(s: &str) -> Result<(Vec<u8>, bool, bool, bool), CommandError> {
    if s == "-" {
        return Ok((vec![], false, true, false));
    }
    if s == "+" {
        return Ok((vec![], false, false, true));
    }
    if let Some(stripped) = s.strip_prefix('[') {
        return Ok((stripped.as_bytes().to_vec(), false, false, false));
    }
    if let Some(stripped) = s.strip_prefix('(') {
        return Ok((stripped.as_bytes().to_vec(), true, false, false));
    }
    Err(CommandError::InvalidArgument(
        "min or max not valid string range item".to_string(),
    ))
}

fn in_lex_range(
    member: &[u8],
    min: &(Vec<u8>, bool, bool, bool),
    max: &(Vec<u8>, bool, bool, bool),
) -> bool {
    if !min.2 {
        if min.1 {
            if member <= min.0.as_slice() {
                return false;
            }
        } else if member < min.0.as_slice() {
            return false;
        }
    }
    if !max.3 {
        if max.1 {
            if member >= max.0.as_slice() {
                return false;
            }
        } else if member > max.0.as_slice() {
            return false;
        }
    }
    true
}

fn score_in_range(
    score: f64,
    min_score: f64,
    min_exclusive: bool,
    max_score: f64,
    max_exclusive: bool,
) -> bool {
    let above_min = if min_exclusive {
        score > min_score
    } else {
        score >= min_score
    };
    let below_max = if max_exclusive {
        score < max_score
    } else {
        score <= max_score
    };
    above_min && below_max
}

use crate::utils::glob::glob_match;

fn normalize_indices(start: i64, stop: i64, len: usize) -> Option<(usize, usize)> {
    let len_i64 = len as i64;
    let start_idx = if start < 0 {
        (len_i64 + start).max(0) as usize
    } else {
        start as usize
    };
    let stop_idx = if stop < 0 {
        (len_i64 + stop).max(0) as usize
    } else {
        stop.min(len_i64 - 1) as usize
    };
    if start_idx <= stop_idx && start_idx < len {
        Some((start_idx, stop_idx.min(len - 1)))
    } else {
        None
    }
}

fn aggregate_scores(existing: f64, new: f64, aggregate: &str) -> f64 {
    match aggregate {
        "SUM" => existing + new,
        "MIN" => existing.min(new),
        "MAX" => existing.max(new),
        _ => existing + new,
    }
}

fn parse_weights_aggregate(
    args: &[Vec<u8>],
    start: usize,
    numkeys: usize,
) -> Result<(Vec<f64>, String, bool, usize), CommandError> {
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = "SUM".to_string();
    let mut with_scores = false;
    let mut i = start;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                if i + numkeys >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                for j in 0..numkeys {
                    let w = bytes_to_string(&args[i + 1 + j])?
                        .parse::<f64>()
                        .map_err(|_| {
                            CommandError::InvalidArgument("weight value is not a float".to_string())
                        })?;
                    if w.is_nan() {
                        return Err(CommandError::InvalidArgument(
                            "weight value is not a float".to_string(),
                        ));
                    }
                    weights[j] = w;
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                aggregate = bytes_to_string(&args[i + 1])?.to_uppercase();
                if !matches!(aggregate.as_str(), "SUM" | "MIN" | "MAX") {
                    return Err(CommandError::InvalidArgument(
                        "aggregate must be SUM, MIN, or MAX".to_string(),
                    ));
                }
                i += 2;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option: {}",
                    opt
                )));
            }
        }
    }
    Ok((weights, aggregate, with_scores, i))
}

/// Redis ZADD command
pub async fn zadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let mut only_if_new = false;
    let mut only_if_exists = false;
    let mut only_if_greater = false;
    let mut only_if_less = false;
    let mut return_changed = false;
    let mut i = 1;
    while i < args.len() {
        let option = bytes_to_string(&args[i])?;
        match option.to_uppercase().as_str() {
            "NX" => {
                only_if_new = true;
                i += 1;
            }
            "XX" => {
                only_if_exists = true;
                i += 1;
            }
            "GT" => {
                only_if_greater = true;
                i += 1;
            }
            "LT" => {
                only_if_less = true;
                i += 1;
            }
            "CH" => {
                return_changed = true;
                i += 1;
            }
            _ => break,
        }
    }
    if only_if_new && only_if_exists {
        return Err(CommandError::InvalidArgument(
            "NX and XX options cannot be used together".to_string(),
        ));
    }
    if only_if_new && (only_if_greater || only_if_less) {
        return Err(CommandError::InvalidArgument(
            "GT/LT options cannot be used with NX".to_string(),
        ));
    }
    if (args.len() - i) < 2 || !(args.len() - i).is_multiple_of(2) {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let mut pairs: Vec<(f64, Vec<u8>)> = Vec::new();
    while i < args.len() {
        if i + 1 >= args.len() {
            return Err(CommandError::WrongNumberOfArguments);
        }
        let score_str = bytes_to_string(&args[i])?;
        let score = score_str
            .parse::<f64>()
            .map_err(|_| CommandError::InvalidArgument("Score is not a valid float".to_string()))?;
        if score.is_nan() {
            return Err(CommandError::InvalidArgument(
                "Score is not a valid float".to_string(),
            ));
        }
        pairs.push((score, args[i + 1].clone()));
        i += 2;
    }

    let result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        let mut ss = take_zset_mut(existing)?;
        let mut added = 0i64;
        let mut changed = 0i64;
        for (score, member) in &pairs {
            let existing_score = ss.get_score(member);
            if let Some(old_score) = existing_score {
                if !only_if_new {
                    let mut should_update = true;
                    if only_if_greater && *score <= old_score {
                        should_update = false;
                    }
                    if only_if_less && *score >= old_score {
                        should_update = false;
                    }
                    if should_update && old_score != *score {
                        ss.insert(*score, member.clone());
                        changed += 1;
                    }
                }
            } else if !only_if_exists {
                ss.insert(*score, member.clone());
                added += 1;
            }
        }
        let ret = if return_changed {
            added + changed
        } else {
            added
        };
        Ok((zset_result(ss), ret))
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZRANGE command
pub async fn zrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let start = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;
    let stop = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;
    let with_scores = args.len() == 4
        && bytes_to_string(&args[3])
            .map(|s| s.to_uppercase() == "WITHSCORES")
            .unwrap_or(false);

    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        if ss.is_empty() {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        if let Some((si, ei)) = normalize_indices(start, stop, ss.len()) {
            let rlen = ei - si + 1;
            output = Vec::with_capacity(if with_scores { rlen * 2 } else { rlen });
            for (score, member) in ss.entries.iter().skip(si).take(rlen) {
                output.push(RespValue::BulkString(Some(member.clone())));
                if with_scores {
                    output.push(RespValue::BulkString(Some(
                        format_score(score.0).into_bytes(),
                    )));
                }
            }
        }
        Ok(output)
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREM command
pub async fn zrem(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let members = args[1..].to_vec();
    let result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        match existing {
            Some(StoreValue::ZSet(ss)) => {
                let mut removed = 0i64;
                for member in &members {
                    if ss.remove(member) {
                        removed += 1;
                    }
                }
                if removed == 0 {
                    Ok((ModifyResult::KeepUnchanged, 0i64))
                } else if ss.is_empty() {
                    Ok((ModifyResult::Delete, removed))
                } else {
                    Ok((ModifyResult::Keep, removed))
                }
            }
            None => Ok((ModifyResult::KeepUnchanged, 0i64)),
            _ => unreachable!(),
        }
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZSCORE command
pub async fn zscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let member = args[1].clone();
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        Ok(ss.get_score(&member))
    })?;
    match result {
        Some(score) => Ok(RespValue::BulkString(Some(
            format_score(score).into_bytes(),
        ))),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis ZCARD command
pub async fn zcard(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        Ok(ss.len() as i64)
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZCOUNT command
pub async fn zcount(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let (min_score, min_exclusive) = parse_score_bound(&bytes_to_string(&args[1])?)?;
    let (max_score, max_exclusive) = parse_score_bound(&bytes_to_string(&args[2])?)?;
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let count = ss
            .entries
            .iter()
            .filter(|(s, _)| {
                score_in_range(s.0, min_score, min_exclusive, max_score, max_exclusive)
            })
            .count();
        Ok(count as i64)
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZRANK command
pub async fn zrank(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let member = args[1].clone();
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        if existing.is_none() {
            return Ok(None);
        }
        let ss = read_zset(existing)?;
        Ok(ss.rank(&member))
    })?;
    match result {
        Some(rank) => Ok(RespValue::Integer(rank as i64)),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis ZREVRANK command
pub async fn zrevrank(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let member = args[1].clone();
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        if existing.is_none() {
            return Ok(None);
        }
        let ss = read_zset(existing)?;
        Ok(ss.rev_rank(&member))
    })?;
    match result {
        Some(rank) => Ok(RespValue::Integer(rank as i64)),
        None => Ok(RespValue::BulkString(None)),
    }
}

/// Redis ZREVRANGE command
pub async fn zrevrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let start = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;
    let stop = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;
    let with_scores = args.len() == 4
        && bytes_to_string(&args[3])
            .map(|s| s.to_uppercase() == "WITHSCORES")
            .unwrap_or(false);
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        if ss.is_empty() {
            return Ok(Vec::new());
        }
        let mut output = Vec::new();
        if let Some((si, ei)) = normalize_indices(start, stop, ss.len()) {
            let rlen = ei - si + 1;
            output = Vec::with_capacity(if with_scores { rlen * 2 } else { rlen });
            let entries_vec: Vec<_> = ss.entries.iter().rev().skip(si).take(rlen).collect();
            for (score, member) in entries_vec {
                output.push(RespValue::BulkString(Some(member.clone())));
                if with_scores {
                    output.push(RespValue::BulkString(Some(
                        format_score(score.0).into_bytes(),
                    )));
                }
            }
        }
        Ok(output)
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZINCRBY command
pub async fn zincrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let increment = bytes_to_string(&args[1])?
        .parse::<f64>()
        .map_err(|_| CommandError::InvalidArgument("Increment is not a valid float".to_string()))?;
    if increment.is_nan() {
        return Err(CommandError::InvalidArgument(
            "Increment is not a valid float".to_string(),
        ));
    }
    let member = args[2].clone();
    let new_score = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        let mut ss = take_zset_mut(existing)?;
        let old_score = ss.get_score(&member).unwrap_or(0.0);
        let new_score = old_score + increment;
        ss.insert(new_score, member.clone());
        Ok((zset_result(ss), new_score))
    })?;
    Ok(RespValue::BulkString(Some(
        format_score(new_score).into_bytes(),
    )))
}

/// Redis ZRANGEBYSCORE command
pub async fn zrangebyscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let (min_score, min_exclusive) = parse_score_bound(&bytes_to_string(&args[1])?)?;
    let (max_score, max_exclusive) = parse_score_bound(&bytes_to_string(&args[2])?)?;
    let mut with_scores = false;
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                offset = bytes_to_string(&args[i + 1])?
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "value is not an integer or out of range".to_string(),
                        )
                    })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if cnt >= 0 {
                    count = Some(cnt as usize);
                }
                i += 3;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option: {}",
                    opt
                )));
            }
        }
    }
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let mut output = Vec::new();
        for (score, member) in ss
            .entries
            .iter()
            .filter(|(s, _)| {
                score_in_range(s.0, min_score, min_exclusive, max_score, max_exclusive)
            })
            .skip(offset)
            .take(count.unwrap_or(usize::MAX))
        {
            output.push(RespValue::BulkString(Some(member.clone())));
            if with_scores {
                output.push(RespValue::BulkString(Some(
                    format_score(score.0).into_bytes(),
                )));
            }
        }
        Ok(output)
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREVRANGEBYSCORE command
pub async fn zrevrangebyscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let (max_score, max_exclusive) = parse_score_bound(&bytes_to_string(&args[1])?)?;
    let (min_score, min_exclusive) = parse_score_bound(&bytes_to_string(&args[2])?)?;
    let mut with_scores = false;
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                offset = bytes_to_string(&args[i + 1])?
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "value is not an integer or out of range".to_string(),
                        )
                    })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if cnt >= 0 {
                    count = Some(cnt as usize);
                }
                i += 3;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option: {}",
                    opt
                )));
            }
        }
    }
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let mut output = Vec::new();
        for (score, member) in ss
            .entries
            .iter()
            .rev()
            .filter(|(s, _)| {
                score_in_range(s.0, min_score, min_exclusive, max_score, max_exclusive)
            })
            .skip(offset)
            .take(count.unwrap_or(usize::MAX))
        {
            output.push(RespValue::BulkString(Some(member.clone())));
            if with_scores {
                output.push(RespValue::BulkString(Some(
                    format_score(score.0).into_bytes(),
                )));
            }
        }
        Ok(output)
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZRANGEBYLEX command
pub async fn zrangebylex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min = parse_lex_bound(&bytes_to_string(&args[1])?)?;
    let max = parse_lex_bound(&bytes_to_string(&args[2])?)?;
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        if opt == "LIMIT" {
            if i + 2 >= args.len() {
                return Err(CommandError::WrongNumberOfArguments);
            }
            offset = bytes_to_string(&args[i + 1])?
                .parse::<usize>()
                .map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
            let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            if cnt >= 0 {
                count = Some(cnt as usize);
            }
            i += 3;
        } else {
            return Err(CommandError::InvalidArgument(format!(
                "unsupported option: {}",
                opt
            )));
        }
    }
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let output: Vec<RespValue> = ss
            .entries
            .iter()
            .filter(|(_, member)| in_lex_range(member, &min, &max))
            .skip(offset)
            .take(count.unwrap_or(usize::MAX))
            .map(|(_, member)| RespValue::BulkString(Some(member.clone())))
            .collect();
        Ok(output)
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREVRANGEBYLEX command
pub async fn zrevrangebylex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let max = parse_lex_bound(&bytes_to_string(&args[1])?)?;
    let min = parse_lex_bound(&bytes_to_string(&args[2])?)?;
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        if opt == "LIMIT" {
            if i + 2 >= args.len() {
                return Err(CommandError::WrongNumberOfArguments);
            }
            offset = bytes_to_string(&args[i + 1])?
                .parse::<usize>()
                .map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
            let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            if cnt >= 0 {
                count = Some(cnt as usize);
            }
            i += 3;
        } else {
            return Err(CommandError::InvalidArgument(format!(
                "unsupported option: {}",
                opt
            )));
        }
    }
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let output: Vec<RespValue> = ss
            .entries
            .iter()
            .rev()
            .filter(|(_, member)| in_lex_range(member, &min, &max))
            .skip(offset)
            .take(count.unwrap_or(usize::MAX))
            .map(|(_, member)| RespValue::BulkString(Some(member.clone())))
            .collect();
        Ok(output)
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREMRANGEBYRANK command
pub async fn zremrangebyrank(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let start = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let stop = bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        if existing.is_none() {
            return Ok((ModifyResult::Delete, 0i64));
        }
        let mut ss = take_zset_mut(existing)?;
        if ss.is_empty() {
            return Ok((ModifyResult::Delete, 0i64));
        }
        match normalize_indices(start, stop, ss.len()) {
            None => {
                Ok((zset_result(ss), 0i64))
            }
            Some((si, ei)) => {
                let to_remove: Vec<_> = ss
                    .entries
                    .iter()
                    .skip(si)
                    .take(ei - si + 1)
                    .cloned()
                    .collect();
                let removed = to_remove.len() as i64;
                for (score, member) in to_remove {
                    ss.entries.remove(&(score, member.clone()));
                    ss.scores.remove(&member);
                }
                Ok((zset_result(ss), removed))
            }
        }
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZREMRANGEBYSCORE command
pub async fn zremrangebyscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let (min_score, min_exclusive) = parse_score_bound(&bytes_to_string(&args[1])?)?;
    let (max_score, max_exclusive) = parse_score_bound(&bytes_to_string(&args[2])?)?;
    let result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        if existing.is_none() {
            return Ok((ModifyResult::Delete, 0i64));
        }
        let mut ss = take_zset_mut(existing)?;
        let to_remove: Vec<_> = ss
            .entries
            .iter()
            .filter(|(s, _)| {
                score_in_range(s.0, min_score, min_exclusive, max_score, max_exclusive)
            })
            .cloned()
            .collect();
        let removed = to_remove.len() as i64;
        for (score, member) in to_remove {
            ss.entries.remove(&(score, member.clone()));
            ss.scores.remove(&member);
        }
        Ok((zset_result(ss), removed))
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZREMRANGEBYLEX command
pub async fn zremrangebylex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min = parse_lex_bound(&bytes_to_string(&args[1])?)?;
    let max = parse_lex_bound(&bytes_to_string(&args[2])?)?;
    let result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        if existing.is_none() {
            return Ok((ModifyResult::Delete, 0i64));
        }
        let mut ss = take_zset_mut(existing)?;
        let to_remove: Vec<_> = ss
            .entries
            .iter()
            .filter(|(_, member)| in_lex_range(member, &min, &max))
            .cloned()
            .collect();
        let removed = to_remove.len() as i64;
        for (score, member) in to_remove {
            ss.entries.remove(&(score, member.clone()));
            ss.scores.remove(&member);
        }
        Ok((zset_result(ss), removed))
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZLEXCOUNT command
pub async fn zlexcount(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min = parse_lex_bound(&bytes_to_string(&args[1])?)?;
    let max = parse_lex_bound(&bytes_to_string(&args[2])?)?;
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let count = ss
            .entries
            .iter()
            .filter(|(_, member)| in_lex_range(member, &min, &max))
            .count();
        Ok(count as i64)
    })?;
    Ok(RespValue::Integer(result))
}

/// Redis ZINTERSTORE command
pub async fn zinterstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let dest = &args[0];
    let numkeys = bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    if numkeys == 0 || args.len() < 2 + numkeys {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys: Vec<Vec<u8>> = args[2..2 + numkeys].to_vec();
    let (weights, aggregate, _, _) = parse_weights_aggregate(args, 2 + numkeys, numkeys)?;

    // Lock all involved keys (sources + destination) for cross-key atomicity
    let all_lock_keys: Vec<Vec<u8>> = std::iter::once(dest.clone())
        .chain(keys.iter().cloned())
        .collect();
    let _guard = engine.lock_keys(&all_lock_keys).await;

    let mut sets: Vec<SortedSetData> = Vec::with_capacity(numkeys);
    for key in &keys {
        match load_sorted_set_readonly(engine, key)? {
            Some(ss) => sets.push(ss),
            None => sets.push(SortedSetData::new()),
        }
    }
    if sets.is_empty() || sets.iter().any(|s| s.is_empty()) {
        engine.atomic_modify(dest, RedisDataType::ZSet, |_| Ok((ModifyResult::Delete, ())))?;
        return Ok(RespValue::Integer(0));
    }
    let mut result_ss = SortedSetData::new();
    for (score, member) in &sets[0].entries {
        let mut in_all = true;
        let mut agg_score = score.0 * weights[0];
        for (idx, set) in sets.iter().enumerate().skip(1) {
            if let Some(s) = set.get_score(member) {
                agg_score = aggregate_scores(agg_score, s * weights[idx], &aggregate);
            } else {
                in_all = false;
                break;
            }
        }
        if in_all {
            result_ss.insert(agg_score, member.clone());
        }
    }
    let count = result_ss.len() as i64;
    engine.atomic_modify(dest, RedisDataType::ZSet, |_| {
        Ok((zset_result(result_ss), ()))
    })?;
    Ok(RespValue::Integer(count))
}

/// Redis ZUNIONSTORE command
pub async fn zunionstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let dest = &args[0];
    let numkeys = bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    if numkeys == 0 || args.len() < 2 + numkeys {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys: Vec<Vec<u8>> = args[2..2 + numkeys].to_vec();
    let (weights, aggregate, _, _) = parse_weights_aggregate(args, 2 + numkeys, numkeys)?;

    // Lock all involved keys (sources + destination) for cross-key atomicity
    let all_lock_keys: Vec<Vec<u8>> = std::iter::once(dest.clone())
        .chain(keys.iter().cloned())
        .collect();
    let _guard = engine.lock_keys(&all_lock_keys).await;

    let mut member_scores: HashMap<Vec<u8>, f64> = HashMap::new();
    for (idx, key) in keys.iter().enumerate() {
        if let Some(set) = load_sorted_set_readonly(engine, key)? {
            for (score, member) in &set.entries {
                let weighted = score.0 * weights[idx];
                member_scores
                    .entry(member.clone())
                    .and_modify(|e| {
                        *e = aggregate_scores(*e, weighted, &aggregate);
                    })
                    .or_insert(weighted);
            }
        }
    }
    let mut result_ss = SortedSetData::new();
    for (member, score) in member_scores {
        result_ss.insert(score, member);
    }
    let count = result_ss.len() as i64;
    engine.atomic_modify(dest, RedisDataType::ZSet, |_| {
        Ok((zset_result(result_ss), ()))
    })?;
    Ok(RespValue::Integer(count))
}

/// Redis ZINTER command
pub async fn zinter(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let numkeys = bytes_to_string(&args[0])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    if numkeys == 0 || args.len() < 1 + numkeys {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys: Vec<Vec<u8>> = args[1..1 + numkeys].to_vec();
    let (weights, aggregate, with_scores, _) = parse_weights_aggregate(args, 1 + numkeys, numkeys)?;

    let mut sets: Vec<SortedSetData> = Vec::new();
    for key in &keys {
        match load_sorted_set_readonly(engine, key)? {
            Some(ss) => sets.push(ss),
            None => return Ok(RespValue::Array(Some(vec![]))),
        }
    }
    let mut result_set = SortedSetData::new();
    for (score, member) in &sets[0].entries {
        let mut in_all = true;
        let mut agg_score = score.0 * weights[0];
        for (idx, set) in sets.iter().enumerate().skip(1) {
            if let Some(s) = set.get_score(member) {
                agg_score = aggregate_scores(agg_score, s * weights[idx], &aggregate);
            } else {
                in_all = false;
                break;
            }
        }
        if in_all {
            result_set.insert(agg_score, member.clone());
        }
    }
    let mut result = Vec::new();
    for (score, member) in &result_set.entries {
        result.push(RespValue::BulkString(Some(member.clone())));
        if with_scores {
            result.push(RespValue::BulkString(Some(
                format_score(score.0).into_bytes(),
            )));
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZUNION command
pub async fn zunion(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let numkeys = bytes_to_string(&args[0])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    if numkeys == 0 || args.len() < 1 + numkeys {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys: Vec<Vec<u8>> = args[1..1 + numkeys].to_vec();
    let (weights, aggregate, with_scores, _) = parse_weights_aggregate(args, 1 + numkeys, numkeys)?;

    let mut member_scores: HashMap<Vec<u8>, f64> = HashMap::new();
    for (idx, key) in keys.iter().enumerate() {
        if let Some(set) = load_sorted_set_readonly(engine, key)? {
            for (score, member) in &set.entries {
                let weighted = score.0 * weights[idx];
                member_scores
                    .entry(member.clone())
                    .and_modify(|e| {
                        *e = aggregate_scores(*e, weighted, &aggregate);
                    })
                    .or_insert(weighted);
            }
        }
    }
    let mut result_set = SortedSetData::new();
    for (member, score) in member_scores {
        result_set.insert(score, member);
    }
    let mut result = Vec::new();
    for (score, member) in &result_set.entries {
        result.push(RespValue::BulkString(Some(member.clone())));
        if with_scores {
            result.push(RespValue::BulkString(Some(
                format_score(score.0).into_bytes(),
            )));
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZDIFF command
pub async fn zdiff(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let numkeys = bytes_to_string(&args[0])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    if numkeys == 0 || args.len() < 1 + numkeys {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys: Vec<Vec<u8>> = args[1..1 + numkeys].to_vec();
    let with_scores = args.len() > 1 + numkeys
        && bytes_to_string(&args[1 + numkeys])?.to_uppercase() == "WITHSCORES";

    let first_set = match load_sorted_set_readonly(engine, &keys[0])? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };
    let mut other_members: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for key in keys.iter().skip(1) {
        if let Some(set) = load_sorted_set_readonly(engine, key)? {
            for (_, member) in &set.entries {
                other_members.insert(member.clone());
            }
        }
    }
    let mut result = Vec::new();
    for (score, member) in &first_set.entries {
        if !other_members.contains(member) {
            result.push(RespValue::BulkString(Some(member.clone())));
            if with_scores {
                result.push(RespValue::BulkString(Some(
                    format_score(score.0).into_bytes(),
                )));
            }
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZDIFFSTORE command
pub async fn zdiffstore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let dest = &args[0];
    let numkeys = bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    if numkeys == 0 || args.len() < 2 + numkeys {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys: Vec<Vec<u8>> = args[2..2 + numkeys].to_vec();

    // Lock all involved keys (sources + destination) for cross-key atomicity
    let all_lock_keys: Vec<Vec<u8>> = std::iter::once(dest.clone())
        .chain(keys.iter().cloned())
        .collect();
    let _guard = engine.lock_keys(&all_lock_keys).await;

    let first_set = match load_sorted_set_readonly(engine, &keys[0])? {
        Some(ss) => ss,
        None => {
            engine.atomic_modify(dest, RedisDataType::ZSet, |_| Ok((ModifyResult::Delete, ())))?;
            return Ok(RespValue::Integer(0));
        }
    };
    let mut other_members: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for key in keys.iter().skip(1) {
        if let Some(set) = load_sorted_set_readonly(engine, key)? {
            for (_, member) in &set.entries {
                other_members.insert(member.clone());
            }
        }
    }
    let mut result_ss = SortedSetData::new();
    for (score, member) in &first_set.entries {
        if !other_members.contains(member) {
            result_ss.insert(score.0, member.clone());
        }
    }
    let count = result_ss.len() as i64;
    engine.atomic_modify(dest, RedisDataType::ZSet, |_| {
        Ok((zset_result(result_ss), ()))
    })?;
    Ok(RespValue::Integer(count))
}

/// Redis ZPOPMIN command
pub async fn zpopmin(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let count = if args.len() == 2 {
        bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?
    } else {
        1
    };

    let result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        if existing.is_none() {
            return Ok((ModifyResult::Delete, Vec::new()));
        }
        let mut ss = take_zset_mut(existing)?;
        if ss.is_empty() {
            return Ok((ModifyResult::Delete, Vec::new()));
        }
        let take = count.min(ss.len());
        let mut popped = Vec::with_capacity(take * 2);
        for _ in 0..take {
            if let Some((score, member)) = ss.entries.iter().next().cloned() {
                ss.entries.remove(&(score, member.clone()));
                ss.scores.remove(&member);
                popped.push(RespValue::BulkString(Some(member)));
                popped.push(RespValue::BulkString(Some(
                    format_score(score.0).into_bytes(),
                )));
            }
        }
        Ok((zset_result(ss), popped))
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZPOPMAX command
pub async fn zpopmax(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let count = if args.len() == 2 {
        bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?
    } else {
        1
    };

    let result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
        if existing.is_none() {
            return Ok((ModifyResult::Delete, Vec::new()));
        }
        let mut ss = take_zset_mut(existing)?;
        if ss.is_empty() {
            return Ok((ModifyResult::Delete, Vec::new()));
        }
        let take = count.min(ss.len());
        let mut popped = Vec::with_capacity(take * 2);
        for _ in 0..take {
            if let Some((score, member)) = ss.entries.iter().next_back().cloned() {
                ss.entries.remove(&(score, member.clone()));
                ss.scores.remove(&member);
                popped.push(RespValue::BulkString(Some(member)));
                popped.push(RespValue::BulkString(Some(
                    format_score(score.0).into_bytes(),
                )));
            }
        }
        Ok((zset_result(ss), popped))
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Helper for blocking sorted set pop
async fn blocking_zpop(
    engine: &StorageEngine,
    keys: &[Vec<u8>],
    timeout_secs: f64,
    pop_min: bool,
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    for key in keys {
        if let Some(ss) = load_sorted_set_readonly(engine, key)?
            && !ss.is_empty()
        {
            let pop_args = vec![key.clone()];
            let result = if pop_min {
                zpopmin(engine, &pop_args).await?
            } else {
                zpopmax(engine, &pop_args).await?
            };
            if let RespValue::Array(Some(ref items)) = result
                && items.len() >= 2
            {
                return Ok(RespValue::Array(Some(vec![
                    RespValue::BulkString(Some(key.clone())),
                    items[0].clone(),
                    items[1].clone(),
                ])));
            }
        }
    }
    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };
    loop {
        let receivers: Vec<_> = keys.iter().map(|k| blocking_mgr.subscribe(k)).collect();
        for key in keys {
            if let Some(ss) = load_sorted_set_readonly(engine, key)?
                && !ss.is_empty()
            {
                let pop_args = vec![key.clone()];
                let result = if pop_min {
                    zpopmin(engine, &pop_args).await?
                } else {
                    zpopmax(engine, &pop_args).await?
                };
                if let RespValue::Array(Some(ref items)) = result
                    && items.len() >= 2
                {
                    return Ok(RespValue::Array(Some(vec![
                        RespValue::BulkString(Some(key.clone())),
                        items[0].clone(),
                        items[1].clone(),
                    ])));
                }
            }
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

/// Redis BZPOPMIN command
pub async fn bzpopmin(
    engine: &StorageEngine,
    args: &[Vec<u8>],
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let timeout_secs = bytes_to_string(args.last().unwrap())?
        .parse::<f64>()
        .map_err(|_| {
            CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
        })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }
    blocking_zpop(
        engine,
        &args[..args.len() - 1],
        timeout_secs,
        true,
        blocking_mgr,
    )
    .await
}

/// Redis BZPOPMAX command
pub async fn bzpopmax(
    engine: &StorageEngine,
    args: &[Vec<u8>],
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let timeout_secs = bytes_to_string(args.last().unwrap())?
        .parse::<f64>()
        .map_err(|_| {
            CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
        })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }
    blocking_zpop(
        engine,
        &args[..args.len() - 1],
        timeout_secs,
        false,
        blocking_mgr,
    )
    .await
}

/// Redis ZRANDMEMBER command
pub async fn zrandmember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];

    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let entries: Vec<(f64, Vec<u8>)> =
            ss.entries.iter().map(|(s, m)| (s.0, m.clone())).collect();
        Ok(entries)
    })?;

    let entries = result;
    if entries.is_empty() {
        if args.len() == 1 {
            return Ok(RespValue::BulkString(None));
        }
        return Ok(RespValue::Array(Some(vec![])));
    }

    if args.len() == 1 {
        let idx = fastrand::usize(0..entries.len());
        return Ok(RespValue::BulkString(Some(entries[idx].1.clone())));
    }

    let count_val = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let with_scores = args.len() == 3 && bytes_to_string(&args[2])?.to_uppercase() == "WITHSCORES";

    let mut output = Vec::new();
    if count_val > 0 {
        let count = (count_val as usize).min(entries.len());
        let mut indices: Vec<usize> = (0..entries.len()).collect();
        for i in (1..indices.len()).rev() {
            let j = fastrand::usize(0..=i);
            indices.swap(i, j);
        }
        for &idx in indices.iter().take(count) {
            output.push(RespValue::BulkString(Some(entries[idx].1.clone())));
            if with_scores {
                output.push(RespValue::BulkString(Some(
                    format_score(entries[idx].0).into_bytes(),
                )));
            }
        }
    } else if count_val < 0 {
        let count = (-count_val) as usize;
        for _ in 0..count {
            let idx = fastrand::usize(0..entries.len());
            output.push(RespValue::BulkString(Some(entries[idx].1.clone())));
            if with_scores {
                output.push(RespValue::BulkString(Some(
                    format_score(entries[idx].0).into_bytes(),
                )));
            }
        }
    }
    Ok(RespValue::Array(Some(output)))
}

/// Redis ZMSCORE command
pub async fn zmscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let members: Vec<Vec<u8>> = args[1..].to_vec();
    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        let scores: Vec<RespValue> = members
            .iter()
            .map(|member| match ss.get_score(member) {
                Some(score) => RespValue::BulkString(Some(format_score(score).into_bytes())),
                None => RespValue::BulkString(None),
            })
            .collect();
        Ok(scores)
    })?;
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZMPOP command
pub async fn zmpop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let numkeys = bytes_to_string(&args[0])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    if numkeys == 0 || args.len() < 1 + numkeys + 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys = &args[1..1 + numkeys];
    let direction = bytes_to_string(&args[1 + numkeys])?.to_uppercase();
    if !matches!(direction.as_str(), "MIN" | "MAX") {
        return Err(CommandError::InvalidArgument("syntax error".to_string()));
    }
    let count = if args.len() > 2 + numkeys {
        let count_opt = bytes_to_string(&args[2 + numkeys])?.to_uppercase();
        if count_opt == "COUNT" {
            if args.len() <= 3 + numkeys {
                return Err(CommandError::WrongNumberOfArguments);
            }
            bytes_to_string(&args[3 + numkeys])?
                .parse::<usize>()
                .map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?
        } else {
            return Err(CommandError::InvalidArgument("syntax error".to_string()));
        }
    } else {
        1
    };

    for key in keys {
        let pop_result = engine.atomic_modify(key, RedisDataType::ZSet, |existing| {
            if existing.is_none() {
                return Ok((ModifyResult::Delete, None));
            }
            let mut ss = take_zset_mut(existing)?;
            if ss.is_empty() {
                return Ok((ModifyResult::Delete, None));
            }
            let take = count.min(ss.len());
            let mut elements = Vec::with_capacity(take * 2);
            for _ in 0..take {
                let entry = if direction == "MIN" {
                    ss.entries.iter().next().cloned()
                } else {
                    ss.entries.iter().next_back().cloned()
                };
                if let Some((score, member)) = entry {
                    ss.entries.remove(&(score, member.clone()));
                    ss.scores.remove(&member);
                    elements.push(RespValue::BulkString(Some(member)));
                    elements.push(RespValue::BulkString(Some(
                        format_score(score.0).into_bytes(),
                    )));
                }
            }
            Ok((zset_result(ss), Some(elements)))
        })?;

        if let Some(elements) = pop_result {
            return Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(key.clone())),
                RespValue::Array(Some(elements)),
            ])));
        }
    }
    Ok(RespValue::Array(None))
}

/// Redis BZMPOP command
pub async fn bzmpop(
    engine: &StorageEngine,
    args: &[Vec<u8>],
    blocking_mgr: &BlockingManager,
) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let timeout_secs = bytes_to_string(&args[0])?.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument(
            "timeout is negative".to_string(),
        ));
    }
    let zmpop_args = &args[1..];

    let result = zmpop(engine, zmpop_args).await?;
    if result != RespValue::Array(None) {
        return Ok(result);
    }

    let numkeys = bytes_to_string(&zmpop_args[0])?
        .parse::<usize>()
        .map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
    if numkeys == 0 || zmpop_args.len() < 1 + numkeys + 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let keys = &zmpop_args[1..1 + numkeys];

    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };
    loop {
        let receivers: Vec<_> = keys.iter().map(|k| blocking_mgr.subscribe(k)).collect();
        let result = zmpop(engine, zmpop_args).await?;
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

/// Redis ZRANGESTORE command
pub async fn zrangestore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let dst = &args[0];
    let src = &args[1];
    let min_str = bytes_to_string(&args[2])?;
    let max_str = bytes_to_string(&args[3])?;

    // Lock source + destination for cross-key atomicity
    let all_lock_keys: Vec<Vec<u8>> = vec![dst.clone(), src.clone()];
    let _guard = engine.lock_keys(&all_lock_keys).await;

    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 4;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "BYSCORE" => {
                by_score = true;
                i += 1;
            }
            "BYLEX" => {
                by_lex = true;
                i += 1;
            }
            "REV" => {
                rev = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                limit_offset = bytes_to_string(&args[i + 1])?
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "value is not an integer or out of range".to_string(),
                        )
                    })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if cnt >= 0 {
                    limit_count = Some(cnt as usize);
                }
                i += 3;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option: {}",
                    opt
                )));
            }
        }
    }

    let src_ss = match load_sorted_set_readonly(engine, src)? {
        Some(ss) => ss,
        None => {
            engine.atomic_modify(dst, RedisDataType::ZSet, |_| Ok((ModifyResult::Delete, ())))?;
            return Ok(RespValue::Integer(0));
        }
    };

    let selected: Vec<(f64, Vec<u8>)> = if by_score {
        let (min_score, min_excl) = if rev {
            parse_score_bound(&max_str)?
        } else {
            parse_score_bound(&min_str)?
        };
        let (max_score, max_excl) = if rev {
            parse_score_bound(&min_str)?
        } else {
            parse_score_bound(&max_str)?
        };
        let iter: Box<dyn Iterator<Item = &(OrderedFloat<f64>, Vec<u8>)>> = if rev {
            Box::new(src_ss.entries.iter().rev())
        } else {
            Box::new(src_ss.entries.iter())
        };
        iter.filter(|(s, _)| score_in_range(s.0, min_score, min_excl, max_score, max_excl))
            .skip(limit_offset)
            .take(limit_count.unwrap_or(usize::MAX))
            .map(|(s, m)| (s.0, m.clone()))
            .collect()
    } else if by_lex {
        let min_bound = if rev {
            parse_lex_bound(&max_str)?
        } else {
            parse_lex_bound(&min_str)?
        };
        let max_bound = if rev {
            parse_lex_bound(&min_str)?
        } else {
            parse_lex_bound(&max_str)?
        };
        let iter: Box<dyn Iterator<Item = &(OrderedFloat<f64>, Vec<u8>)>> = if rev {
            Box::new(src_ss.entries.iter().rev())
        } else {
            Box::new(src_ss.entries.iter())
        };
        iter.filter(|(_, member)| in_lex_range(member, &min_bound, &max_bound))
            .skip(limit_offset)
            .take(limit_count.unwrap_or(usize::MAX))
            .map(|(s, m)| (s.0, m.clone()))
            .collect()
    } else {
        let len = src_ss.len();
        let start = min_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        let stop = max_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        match normalize_indices(start, stop, len) {
            None => Vec::new(),
            Some((si, ei)) => {
                let rlen = ei - si + 1;
                if rev {
                    src_ss
                        .entries
                        .iter()
                        .rev()
                        .skip(si)
                        .take(rlen)
                        .map(|(s, m)| (s.0, m.clone()))
                        .collect()
                } else {
                    src_ss
                        .entries
                        .iter()
                        .skip(si)
                        .take(rlen)
                        .map(|(s, m)| (s.0, m.clone()))
                        .collect()
                }
            }
        }
    };

    let mut result_ss = SortedSetData::new();
    for (score, member) in &selected {
        result_ss.insert(*score, member.clone());
    }
    let count = result_ss.len() as i64;
    engine.atomic_modify(dst, RedisDataType::ZSet, |_| {
        Ok((zset_result(result_ss), ()))
    })?;
    Ok(RespValue::Integer(count))
}

/// Redis ZSCAN command
pub async fn zscan(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let cursor = bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let mut pattern: Option<String> = None;
    let mut scan_count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                pattern = Some(bytes_to_string(&args[i + 1])?);
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                scan_count = bytes_to_string(&args[i + 1])?
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "value is not an integer or out of range".to_string(),
                        )
                    })?;
                i += 2;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option: {}",
                    opt
                )));
            }
        }
    }

    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        if ss.is_empty() {
            return Ok((0usize, Vec::new()));
        }
        let mut elements = Vec::new();
        let mut new_cursor = 0usize;
        let mut scanned = 0;
        for (idx, (score, member)) in ss.entries.iter().enumerate().skip(cursor) {
            let member_str = String::from_utf8_lossy(member);
            let matches = match &pattern {
                Some(pat) => glob_match(pat, &member_str),
                None => true,
            };
            if matches {
                elements.push(RespValue::BulkString(Some(member.clone())));
                elements.push(RespValue::BulkString(Some(
                    format_score(score.0).into_bytes(),
                )));
            }
            scanned += 1;
            if scanned >= scan_count {
                new_cursor = idx + 1;
                if new_cursor >= ss.len() {
                    new_cursor = 0;
                }
                break;
            }
        }
        if scanned < scan_count {
            new_cursor = 0;
        }
        Ok((new_cursor, elements))
    })?;

    let (new_cursor, elements) = result;
    Ok(RespValue::Array(Some(vec![
        RespValue::BulkString(Some(new_cursor.to_string().into_bytes())),
        RespValue::Array(Some(elements)),
    ])))
}

/// Redis unified ZRANGE command (Redis 6.2+)
pub async fn zrange_unified(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;

    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut with_scores = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "BYSCORE" => {
                by_score = true;
                i += 1;
            }
            "BYLEX" => {
                by_lex = true;
                i += 1;
            }
            "REV" => {
                rev = true;
                i += 1;
            }
            "WITHSCORES" => {
                with_scores = true;
                i += 1;
            }
            "LIMIT" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                limit_offset = bytes_to_string(&args[i + 1])?
                    .parse::<usize>()
                    .map_err(|_| {
                        CommandError::InvalidArgument(
                            "value is not an integer or out of range".to_string(),
                        )
                    })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument(
                        "value is not an integer or out of range".to_string(),
                    )
                })?;
                if cnt >= 0 {
                    limit_count = Some(cnt as usize);
                }
                i += 3;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "unsupported option: {}",
                    opt
                )));
            }
        }
    }

    // Fall back to original ZRANGE for plain index-based
    if !by_score && !by_lex && !rev && limit_count.is_none() {
        let mut orig_args = vec![args[0].clone(), args[1].clone(), args[2].clone()];
        if with_scores {
            orig_args.push(b"WITHSCORES".to_vec());
        }
        return zrange(engine, &orig_args).await;
    }

    let result = engine.atomic_read(key, RedisDataType::ZSet, |existing| {
        let ss = read_zset(existing)?;
        if ss.is_empty() {
            return Ok(Vec::new());
        }

        let selected: Vec<(f64, Vec<u8>)> = if by_score {
            let (min_score, min_excl) = if rev {
                parse_score_bound(&max_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            } else {
                parse_score_bound(&min_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            };
            let (max_score, max_excl) = if rev {
                parse_score_bound(&min_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            } else {
                parse_score_bound(&max_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            };
            let iter: Box<dyn Iterator<Item = &(OrderedFloat<f64>, Vec<u8>)>> = if rev {
                Box::new(ss.entries.iter().rev())
            } else {
                Box::new(ss.entries.iter())
            };
            iter.filter(|(s, _)| score_in_range(s.0, min_score, min_excl, max_score, max_excl))
                .skip(limit_offset)
                .take(limit_count.unwrap_or(usize::MAX))
                .map(|(s, m)| (s.0, m.clone()))
                .collect()
        } else if by_lex {
            let min_bound = if rev {
                parse_lex_bound(&max_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            } else {
                parse_lex_bound(&min_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            };
            let max_bound = if rev {
                parse_lex_bound(&min_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            } else {
                parse_lex_bound(&max_str)
                    .map_err(|e| StorageError::InternalError(format!("{}", e)))?
            };
            let iter: Box<dyn Iterator<Item = &(OrderedFloat<f64>, Vec<u8>)>> = if rev {
                Box::new(ss.entries.iter().rev())
            } else {
                Box::new(ss.entries.iter())
            };
            iter.filter(|(_, member)| in_lex_range(member, &min_bound, &max_bound))
                .skip(limit_offset)
                .take(limit_count.unwrap_or(usize::MAX))
                .map(|(s, m)| (s.0, m.clone()))
                .collect()
        } else {
            // REV with index-based
            let len = ss.len();
            let start = min_str
                .parse::<i64>()
                .map_err(|_| StorageError::InternalError("not an integer".to_string()))?;
            let stop = max_str
                .parse::<i64>()
                .map_err(|_| StorageError::InternalError("not an integer".to_string()))?;
            match normalize_indices(start, stop, len) {
                None => Vec::new(),
                Some((si, ei)) => {
                    let rlen = ei - si + 1;
                    if rev {
                        ss.entries
                            .iter()
                            .rev()
                            .skip(si)
                            .take(rlen)
                            .map(|(s, m)| (s.0, m.clone()))
                            .collect()
                    } else {
                        ss.entries
                            .iter()
                            .skip(si)
                            .take(rlen)
                            .map(|(s, m)| (s.0, m.clone()))
                            .collect()
                    }
                }
            }
        };

        let mut output = Vec::new();
        for (score, member) in &selected {
            output.push(RespValue::BulkString(Some(member.clone())));
            if with_scores {
                output.push(RespValue::BulkString(Some(
                    format_score(*score).into_bytes(),
                )));
            }
        }
        Ok(output)
    })?;
    Ok(RespValue::Array(Some(result)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    async fn setup_engine() -> Arc<StorageEngine> {
        StorageEngine::new(StorageConfig::default())
    }

    async fn add_members(engine: &StorageEngine, key: &[u8], members: &[(f64, &[u8])]) {
        let mut args: Vec<Vec<u8>> = vec![key.to_vec()];
        for (score, member) in members {
            args.push(score.to_string().into_bytes());
            args.push(member.to_vec());
        }
        zadd(engine, &args).await.unwrap();
    }

    #[tokio::test]
    async fn test_sorted_set_commands() {
        let engine = setup_engine().await;
        let zadd_args = vec![
            b"zset1".to_vec(),
            b"1.5".to_vec(),
            b"member1".to_vec(),
            b"2.5".to_vec(),
            b"member2".to_vec(),
            b"3.5".to_vec(),
            b"member3".to_vec(),
        ];
        let result = zadd(&engine, &zadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = zscore(&engine, &[b"zset1".to_vec(), b"member2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2.5".to_vec())));
        let result = zrange(&engine, &[b"zset1".to_vec(), b"0".to_vec(), b"1".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(elements)) = result {
            assert_eq!(elements.len(), 2);
            assert_eq!(
                elements[0],
                RespValue::BulkString(Some(b"member1".to_vec()))
            );
        } else {
            panic!("Expected array");
        }
        let result = zrem(
            &engine,
            &[
                b"zset1".to_vec(),
                b"member1".to_vec(),
                b"nonexistent".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_zrangebyscore() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[
                (1.0, b"a"),
                (2.0, b"b"),
                (3.0, b"c"),
                (4.0, b"d"),
                (5.0, b"e"),
            ],
        )
        .await;
        let result = zrangebyscore(&engine, &[b"zs".to_vec(), b"2".to_vec(), b"4".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"d".to_vec())));
        } else {
            panic!("Expected array");
        }
        let result = zrangebyscore(&engine, &[b"zs".to_vec(), b"(1".to_vec(), b"(5".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected array");
        }
        let result = zrangebyscore(
            &engine,
            &[
                b"zs".to_vec(),
                b"-inf".to_vec(),
                b"+inf".to_vec(),
                b"LIMIT".to_vec(),
                b"1".to_vec(),
                b"2".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
        let result = zrangebyscore(
            &engine,
            &[
                b"zs".to_vec(),
                b"1".to_vec(),
                b"2".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected array");
        }
        let result = zrangebyscore(
            &engine,
            &[b"nokey".to_vec(), b"-inf".to_vec(), b"+inf".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_zrevrangebyscore() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")],
        )
        .await;
        let result = zrevrangebyscore(&engine, &[b"zs".to_vec(), b"4".to_vec(), b"2".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"d".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zrangebylex() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[
                (0.0, b"a"),
                (0.0, b"b"),
                (0.0, b"c"),
                (0.0, b"d"),
                (0.0, b"e"),
            ],
        )
        .await;
        let result = zrangebylex(&engine, &[b"zs".to_vec(), b"[b".to_vec(), b"[d".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
        let result = zrangebylex(&engine, &[b"zs".to_vec(), b"(a".to_vec(), b"(d".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected array");
        }
        let result = zrangebylex(&engine, &[b"zs".to_vec(), b"-".to_vec(), b"+".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 5);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zrevrangebylex() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d")],
        )
        .await;
        let result = zrevrangebylex(&engine, &[b"zs".to_vec(), b"[d".to_vec(), b"[b".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"d".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zremrangebyrank() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")],
        )
        .await;
        let result = zremrangebyrank(&engine, &[b"zs".to_vec(), b"1".to_vec(), b"2".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = zrange(&engine, &[b"zs".to_vec(), b"0".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"d".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zremrangebyscore() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")],
        )
        .await;
        let result = zremrangebyscore(&engine, &[b"zs".to_vec(), b"2".to_vec(), b"3".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let card = zcard(&engine, &[b"zs".to_vec()]).await.unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zremrangebylex() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d")],
        )
        .await;
        let result = zremrangebylex(&engine, &[b"zs".to_vec(), b"[b".to_vec(), b"[c".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let card = zcard(&engine, &[b"zs".to_vec()]).await.unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zlexcount() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[
                (0.0, b"a"),
                (0.0, b"b"),
                (0.0, b"c"),
                (0.0, b"d"),
                (0.0, b"e"),
            ],
        )
        .await;
        let result = zlexcount(&engine, &[b"zs".to_vec(), b"[b".to_vec(), b"[d".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = zlexcount(&engine, &[b"zs".to_vec(), b"-".to_vec(), b"+".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[tokio::test]
    async fn test_zrevrank() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zrevrank(&engine, &[b"zs".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = zrevrank(&engine, &[b"zs".to_vec(), b"c".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(0));
        let result = zrevrank(&engine, &[b"zs".to_vec(), b"x".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_zinterstore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c"), (30.0, b"d")]).await;
        let result = zinterstore(
            &engine,
            &[
                b"out".to_vec(),
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = zrange(
            &engine,
            &[
                b"out".to_vec(),
                b"0".to_vec(),
                b"-1".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"12".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zinterstore_with_weights() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"a"), (20.0, b"b")]).await;
        let result = zinterstore(
            &engine,
            &[
                b"out".to_vec(),
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
                b"WEIGHTS".to_vec(),
                b"2".to_vec(),
                b"3".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = zscore(&engine, &[b"out".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"32".to_vec())));
    }

    #[tokio::test]
    async fn test_zunionstore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c")]).await;
        let result = zunionstore(
            &engine,
            &[
                b"out".to_vec(),
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = zscore(&engine, &[b"out".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"12".to_vec())));
    }

    #[tokio::test]
    async fn test_zinter() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c"), (30.0, b"d")]).await;
        let result = zinter(
            &engine,
            &[
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zunion() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c")]).await;
        let result = zunion(
            &engine,
            &[
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 6);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zdiff() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"d")]).await;
        let result = zdiff(&engine, &[b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"c".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zdiffstore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b")]).await;
        let result = zdiffstore(
            &engine,
            &[
                b"out".to_vec(),
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zpopmin() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zpopmin(&engine, &[b"zs".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"1".to_vec())));
        } else {
            panic!("Expected array");
        }
        let result = zpopmin(&engine, &[b"zs".to_vec(), b"2".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected array");
        }
        let result = zpopmin(&engine, &[b"zs".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_zpopmax() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zpopmax(&engine, &[b"zs".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"3".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_bzpopmin_immediate() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b")]).await;
        let result = bzpopmin(&engine, &[b"zs".to_vec(), b"1".to_vec()], &blocking_mgr)
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"zs".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"1".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_bzpopmin_timeout() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        let result = bzpopmin(
            &engine,
            &[b"emptykey".to_vec(), b"0.1".to_vec()],
            &blocking_mgr,
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_bzpopmax_immediate() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b")]).await;
        let result = bzpopmax(&engine, &[b"zs".to_vec(), b"1".to_vec()], &blocking_mgr)
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[1], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"2".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zrandmember() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zrandmember(&engine, &[b"zs".to_vec()]).await.unwrap();
        if let RespValue::BulkString(Some(_)) = result {
        } else {
            panic!("Expected bulk string");
        }
        let result = zrandmember(&engine, &[b"zs".to_vec(), b"2".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected array");
        }
        let result = zrandmember(&engine, &[b"zs".to_vec(), b"-5".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 5);
        } else {
            panic!("Expected array");
        }
        let result = zrandmember(&engine, &[b"zs".to_vec(), b"10".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected array");
        }
        let result = zrandmember(&engine, &[b"nokey".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_zmscore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zmscore(
            &engine,
            &[b"zs".to_vec(), b"a".to_vec(), b"x".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"1".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(None));
            assert_eq!(items[2], RespValue::BulkString(Some(b"3".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zmpop() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zmpop(&engine, &[b"1".to_vec(), b"zs1".to_vec(), b"MIN".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"zs1".to_vec())));
        } else {
            panic!("Expected array");
        }
        let result = zmpop(
            &engine,
            &[
                b"1".to_vec(),
                b"zs1".to_vec(),
                b"MAX".to_vec(),
                b"COUNT".to_vec(),
                b"2".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected array");
        }
        let result = zmpop(
            &engine,
            &[b"1".to_vec(), b"emptykey".to_vec(), b"MIN".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_bzmpop_immediate() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;
        let result = bzmpop(
            &engine,
            &[
                b"1".to_vec(),
                b"1".to_vec(),
                b"zs1".to_vec(),
                b"MIN".to_vec(),
            ],
            &blocking_mgr,
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_bzmpop_timeout() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        let result = bzmpop(
            &engine,
            &[
                b"0.1".to_vec(),
                b"1".to_vec(),
                b"emptykey".to_vec(),
                b"MIN".to_vec(),
            ],
            &blocking_mgr,
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_zrangestore() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")],
        )
        .await;
        let result = zrangestore(
            &engine,
            &[
                b"dst".to_vec(),
                b"zs".to_vec(),
                b"1".to_vec(),
                b"2".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = zrange(
            &engine,
            &[
                b"dst".to_vec(),
                b"0".to_vec(),
                b"-1".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else {
            panic!("Expected array");
        }
        let result = zrangestore(
            &engine,
            &[
                b"dst2".to_vec(),
                b"zs".to_vec(),
                b"2".to_vec(),
                b"3".to_vec(),
                b"BYSCORE".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zscan() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.0, b"alpha"), (2.0, b"beta"), (3.0, b"gamma")],
        )
        .await;
        let result = zscan(&engine, &[b"zs".to_vec(), b"0".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            if let RespValue::Array(Some(elements)) = &items[1] {
                assert_eq!(elements.len(), 6);
            } else {
                panic!("Expected inner array");
            }
        } else {
            panic!("Expected array");
        }
        let result = zscan(
            &engine,
            &[
                b"zs".to_vec(),
                b"0".to_vec(),
                b"MATCH".to_vec(),
                b"*eta".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            if let RespValue::Array(Some(elements)) = &items[1] {
                assert_eq!(elements.len(), 2);
            } else {
                panic!("Expected inner array");
            }
        } else {
            panic!("Expected array");
        }
        let result = zscan(&engine, &[b"nokey".to_vec(), b"0".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items[0], RespValue::BulkString(Some(b"0".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zrange_unified_byscore() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")],
        )
        .await;
        let result = zrange_unified(
            &engine,
            &[
                b"zs".to_vec(),
                b"1".to_vec(),
                b"3".to_vec(),
                b"BYSCORE".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected array");
        }
        let result = zrange_unified(
            &engine,
            &[
                b"zs".to_vec(),
                b"3".to_vec(),
                b"1".to_vec(),
                b"BYSCORE".to_vec(),
                b"REV".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"c".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zrange_unified_bylex() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d")],
        )
        .await;
        let result = zrange_unified(
            &engine,
            &[
                b"zs".to_vec(),
                b"[b".to_vec(),
                b"[d".to_vec(),
                b"BYLEX".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zrange_unified_rev() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zrange_unified(
            &engine,
            &[
                b"zs".to_vec(),
                b"0".to_vec(),
                b"-1".to_vec(),
                b"REV".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"a".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zrange_withscores_format() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.5, b"one"), (2.5, b"two"), (3.5, b"three")],
        )
        .await;
        let result = zrange(
            &engine,
            &[
                b"zs".to_vec(),
                b"0".to_vec(),
                b"-1".to_vec(),
                b"WITHSCORES".to_vec(),
            ],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 6);
            assert_eq!(items[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"1.5".to_vec())));
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zadd_multiple_pairs() {
        let engine = setup_engine().await;
        let zadd_args = vec![
            b"multi_zset".to_vec(),
            b"1.0".to_vec(),
            b"member1".to_vec(),
            b"2.0".to_vec(),
            b"member2".to_vec(),
            b"3.0".to_vec(),
            b"member3".to_vec(),
        ];
        let result = zadd(&engine, &zadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = zscore(&engine, &[b"multi_zset".to_vec(), b"member1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"1".to_vec())));
    }

    #[tokio::test]
    async fn test_glob_match_fn() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("a*", "abc"));
        assert!(!glob_match("a*", "bac"));
        assert!(glob_match("?bc", "abc"));
        assert!(!glob_match("?bc", "abcd"));
        assert!(glob_match("*eta", "beta"));
        assert!(!glob_match("*eta", "gamma"));
    }

    #[tokio::test]
    async fn test_zinterstore_aggregate_min_max() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (5.0, b"b")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"a"), (2.0, b"b")]).await;
        zinterstore(
            &engine,
            &[
                b"min_out".to_vec(),
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
                b"AGGREGATE".to_vec(),
                b"MIN".to_vec(),
            ],
        )
        .await
        .unwrap();
        let result = zscore(&engine, &[b"min_out".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"1".to_vec())));
        let result = zscore(&engine, &[b"min_out".to_vec(), b"b".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2".to_vec())));
        zinterstore(
            &engine,
            &[
                b"max_out".to_vec(),
                b"2".to_vec(),
                b"zs1".to_vec(),
                b"zs2".to_vec(),
                b"AGGREGATE".to_vec(),
                b"MAX".to_vec(),
            ],
        )
        .await
        .unwrap();
        let result = zscore(&engine, &[b"max_out".to_vec(), b"a".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"10".to_vec())));
    }

    #[tokio::test]
    async fn test_zpopmin_empty_and_nonexistent() {
        let engine = setup_engine().await;
        let result = zpopmin(&engine, &[b"nokey".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_zpopmax_count_larger_than_set() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b")]).await;
        let result = zpopmax(&engine, &[b"zs".to_vec(), b"10".to_vec()])
            .await
            .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_zremrangebyrank_negative_indices() {
        let engine = setup_engine().await;
        add_members(
            &engine,
            b"zs",
            &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")],
        )
        .await;
        let result = zremrangebyrank(&engine, &[b"zs".to_vec(), b"-2".to_vec(), b"-1".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let card = zcard(&engine, &[b"zs".to_vec()]).await.unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zrangebyscore_exclusive_inf() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        let result = zrangebyscore(
            &engine,
            &[b"zs".to_vec(), b"-inf".to_vec(), b"+inf".to_vec()],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        } else {
            panic!("Expected array");
        }
    }

    #[tokio::test]
    async fn test_concurrent_zadd_same_sorted_set() {
        // Two clients racing ZADD on the same sorted set - no members should be lost
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));

        let mut handles = Vec::new();
        for i in 0..10 {
            let eng = engine.clone();
            handles.push(tokio::spawn(async move {
                for j in 0..10 {
                    let score = format!("{}", i * 10 + j);
                    let member = format!("member_{}_{}", i, j);
                    let args = vec![
                        b"race_zset".to_vec(),
                        score.into_bytes(),
                        member.into_bytes(),
                    ];
                    zadd(&eng, &args).await.unwrap();
                }
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        // All 100 unique members should be present
        let result = zcard(&engine, &[b"race_zset".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(100));
    }

    #[tokio::test]
    async fn test_sorted_set_performance_large() {
        // Performance test: ZADD/ZRANGE on sorted set with 1000 members
        // Verifies BTreeSet O(log N) data structure works correctly at scale.
        let config = StorageConfig::default();
        let engine = Arc::new(StorageEngine::new(config));
        let n: i64 = 1000;

        for i in 0..n {
            let score = format!("{}", i);
            let member = format!("member_{:05}", i);
            let args = vec![
                b"perf_zset".to_vec(),
                score.into_bytes(),
                member.into_bytes(),
            ];
            zadd(&engine, &args).await.unwrap();
        }

        // Verify cardinality
        let result = zcard(&engine, &[b"perf_zset".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(n));

        // Test ZRANGE on large set
        let result = zrange(
            &engine,
            &[b"perf_zset".to_vec(), b"0".to_vec(), b"9".to_vec()],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 10);
        } else {
            panic!("Expected array");
        }

        // Test ZRANGEBYSCORE on large set
        let result = zrangebyscore(
            &engine,
            &[b"perf_zset".to_vec(), b"500".to_vec(), b"510".to_vec()],
        )
        .await
        .unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 11); // 500..=510
        } else {
            panic!("Expected array");
        }

        // Test ZSCORE lookup on large set
        let result = zscore(&engine, &[b"perf_zset".to_vec(), b"member_00500".to_vec()])
            .await
            .unwrap();
        if let RespValue::BulkString(Some(score)) = result {
            assert_eq!(String::from_utf8_lossy(&score), "500");
        } else {
            panic!("Expected bulk string");
        }

        // Test ZRANK on large set
        let result = zrank(&engine, &[b"perf_zset".to_vec(), b"member_00500".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::Integer(500));
    }
}
