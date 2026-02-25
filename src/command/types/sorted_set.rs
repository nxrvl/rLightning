use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::command::types::blocking::BlockingManager;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;
use futures::future::select_all;
use std::time::{Duration, Instant};

// We'll use a vector to store score-member pairs, sorted by score
type SortedSet = Vec<(f64, Vec<u8>)>;

/// Helper: load and sort a sorted set from the engine
async fn load_sorted_set(engine: &StorageEngine, key: &[u8]) -> Result<Option<SortedSet>, CommandError> {
    match engine.get(key).await? {
        Some(data) => {
            let mut ss = bincode::deserialize::<SortedSet>(&data)
                .map_err(|_| CommandError::WrongType)?;
            ss.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.1.cmp(&b.1)));
            Ok(Some(ss))
        }
        None => Ok(None),
    }
}

/// Helper: save a sorted set back to the engine (deletes key if empty)
async fn save_sorted_set(engine: &StorageEngine, key: &[u8], ss: &SortedSet) -> Result<(), CommandError> {
    if ss.is_empty() {
        engine.del(key).await?;
    } else {
        let serialized = bincode::serialize(ss)
            .map_err(|e| CommandError::InternalError(format!("Serialization error: {}", e)))?;
        engine.set_with_type(key.to_vec(), serialized, RedisDataType::ZSet, None).await?;
    }
    Ok(())
}

/// Helper: format a score as a string matching Redis behavior
fn format_score(score: f64) -> String {
    if score == f64::INFINITY {
        "inf".to_string()
    } else if score == f64::NEG_INFINITY {
        "-inf".to_string()
    } else if score.fract() == 0.0 && score.abs() < 1e15 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

/// Parse a score boundary string like "-inf", "+inf", "(3.5", "3.5"
/// Returns (score, exclusive)
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
    let val = val_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("min or max is not a float".to_string())
    })?;
    Ok((val, exclusive))
}

/// Parse a lex boundary string like "-", "+", "[a", "(a"
/// Returns (bound_bytes, exclusive, is_unbounded_min, is_unbounded_max)
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
    Err(CommandError::InvalidArgument("min or max not valid string range item".to_string()))
}

/// Check if a member is within lex bounds
fn in_lex_range(member: &[u8], min: &(Vec<u8>, bool, bool, bool), max: &(Vec<u8>, bool, bool, bool)) -> bool {
    // Check min bound
    if !min.2 { // not unbounded min
        if min.1 { // exclusive
            if member <= min.0.as_slice() { return false; }
        } else {
            if member < min.0.as_slice() { return false; }
        }
    }
    // Check max bound
    if !max.3 { // not unbounded max
        if max.1 { // exclusive
            if member >= max.0.as_slice() { return false; }
        } else {
            if member > max.0.as_slice() { return false; }
        }
    }
    true
}

/// Redis ZADD command - Add members to a sorted set with scores
pub async fn zadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Parse options - check for NX, XX, etc.
    let mut only_if_new = false; // NX option
    let mut only_if_exists = false; // XX option
    let mut i = 1;
    
    // Process options
    while i < args.len() {
        let option = bytes_to_string(&args[i])?;
        match option.to_uppercase().as_str() {
            "NX" => {
                only_if_new = true;
                i += 1;
            },
            "XX" => {
                only_if_exists = true;
                i += 1;
            },
            _ => {
                // Not an option, continue to score-member pairs
                break;
            }
        }
    }
    
    // Check for conflicting options
    if only_if_new && only_if_exists {
        return Err(CommandError::InvalidArgument("NX and XX options cannot be used together".to_string()));
    }
    
    // Now check that we have score-member pairs
    if (args.len() - i) < 2 || (args.len() - i) % 2 != 0 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    // Get the current sorted set or create a new one
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            bincode::deserialize::<SortedSet>(&data).unwrap_or_default()
        },
        None => Vec::new(),
    };
    
    // Track how many new members were added
    let mut added = 0;
    
    // Process score-member pairs
    while i < args.len() {
        if i + 1 >= args.len() {
            return Err(CommandError::WrongNumberOfArguments);
        }
        
        let score_bytes = &args[i];
        let member = args[i + 1].clone();
        i += 2;
        
        // Parse the score
        let score_str = bytes_to_string(score_bytes)?;
        let score = score_str.parse::<f64>().map_err(|_| {
            CommandError::InvalidArgument("Score is not a valid float".to_string())
        })?;
        
        // Check if member already exists 
        let existing_index = sorted_set.iter().position(|(_, m)| m == &member);
        
        if let Some(index) = existing_index {
            // Member exists already
            if !only_if_new {
                // Update the existing member if NX is not set
                sorted_set[index].0 = score;
            }
            // With NX, we skip updating existing members
        } else {
            // Member doesn't exist
            if !only_if_exists {
                // Add new member if XX is not set
                sorted_set.push((score, member));
                added += 1;
            }
            // With XX, we skip adding new members
        }
    }
    
    // Sort the set by score
    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Serialize and save the sorted set
    let serialized = bincode::serialize(&sorted_set).map_err(|e| {
        CommandError::InternalError(format!("Serialization error: {}", e))
    })?;
    
    engine.set_with_type(key.clone(), serialized, RedisDataType::ZSet, None).await?;
    
    Ok(RespValue::Integer(added))
}

/// Redis ZRANGE command - Return a range of members in a sorted set by index
pub async fn zrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Parse start and stop indices
    let start_str = bytes_to_string(&args[1])?;
    let stop_str = bytes_to_string(&args[2])?;
    
    let start = start_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;
    
    let stop = stop_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;
    
    // Check for WITHSCORES option
    let with_scores = args.len() == 4 && 
        bytes_to_string(&args[3]).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(mut sorted_set) => {
                    // Sort by score for consistency with Redis
                    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                    sorted_set
                },
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return empty array (Redis behavior)
            return Ok(RespValue::Array(Some(vec![])));
        },
    };
    
    // Get the total number of elements
    let len = sorted_set.len() as i64;
    
    // Convert indices to zero-based array indices
    let start_idx = if start < 0 {
        // Negative index means count from the end
        (len + start).max(0) as usize
    } else {
        start as usize
    };
    
    let stop_idx = if stop < 0 {
        // Negative index means count from the end
        (len + stop).min(len - 1) as usize
    } else {
        stop.min(len - 1) as usize
    };
    
    // Get the elements in the range
    let mut result = Vec::new();
    if start_idx <= stop_idx && start_idx < sorted_set.len() {
        // Calculate the actual range length
        let actual_stop_idx = stop_idx.min(sorted_set.len() - 1);
        
        // Pre-allocate vector with appropriate capacity
        // If with_scores is true, we'll have twice as many items
        let range_len = actual_stop_idx - start_idx + 1;
        let capacity = if with_scores { range_len * 2 } else { range_len };
        result = Vec::with_capacity(capacity);
        
        for (score, member) in sorted_set.iter().take(actual_stop_idx + 1).skip(start_idx) {
            // Always add the member
            result.push(RespValue::BulkString(Some(member.clone())));
            
            // If WITHSCORES option is present, add the score after each member as a separate array item
            if with_scores {
                // Format score as string with proper format
                let score_str = format!("{}", score);
                result.push(RespValue::BulkString(Some(score_str.into_bytes())));
            }
        }
    }
    
    // ZRANGE always returns an array (Redis behavior)
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREM command - Remove members from a sorted set
pub async fn zrem(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let members = &args[1..];
    
    // Get the current sorted set
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, nothing to remove
            return Ok(RespValue::Integer(0));
        },
    };
    
    // Track how many members were removed
    let mut removed = 0;
    
    // Remove each member
    for member in members {
        let old_len = sorted_set.len();
        sorted_set.retain(|(_, m)| m != member);
        if sorted_set.len() < old_len {
            removed += 1;
        }
    }
    
    if removed > 0 {
        // If the sorted set is now empty, remove the key
        if sorted_set.is_empty() {
            engine.del(key).await?;
        } else {
            // Otherwise, update the sorted set
            let serialized = bincode::serialize(&sorted_set).map_err(|e| {
                CommandError::InternalError(format!("Serialization error: {}", e))
            })?;
            
            engine.set_with_type(key.clone(), serialized, RedisDataType::ZSet, None).await?;
        }
    }
    
    Ok(RespValue::Integer(removed))
}

/// Redis ZSCORE command - Get the score of a member in a sorted set
pub async fn zscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let member = &args[1];
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist
            return Ok(RespValue::BulkString(None));
        },
    };
    
    // Find the score for this member
    for (score, m) in sorted_set.iter() {
        if m == member {
            // Convert score to string with proper formatting
            let score_str = format!("{}", score);
            return Ok(RespValue::BulkString(Some(score_str.into_bytes())));
        }
    }
    
    // Member not found
    Ok(RespValue::BulkString(None))
}

/// Redis ZCARD command - Get the cardinality (number of members) of a sorted set
pub async fn zcard(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 1 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return 0
            return Ok(RespValue::Integer(0));
        },
    };
    
    Ok(RespValue::Integer(sorted_set.len() as i64))
}

/// Redis ZCOUNT command - Count members in a sorted set within a score range
pub async fn zcount(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;
    
    // Parse min and max scores (supporting -inf and +inf)
    let min_score = if min_str == "-inf" {
        f64::NEG_INFINITY
    } else if min_str == "+inf" {
        f64::INFINITY
    } else {
        min_str.parse::<f64>().map_err(|_| {
            CommandError::InvalidArgument("Min score is not a valid float".to_string())
        })?
    };
    
    let max_score = if max_str == "-inf" {
        f64::NEG_INFINITY
    } else if max_str == "+inf" {
        f64::INFINITY
    } else {
        max_str.parse::<f64>().map_err(|_| {
            CommandError::InvalidArgument("Max score is not a valid float".to_string())
        })?
    };
    
    // Get the sorted set
    let sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return 0
            return Ok(RespValue::Integer(0));
        },
    };
    
    // Count members within the score range
    let count = sorted_set.iter()
        .filter(|(score, _)| *score >= min_score && *score <= max_score)
        .count();
    
    Ok(RespValue::Integer(count as i64))
}

/// Redis ZRANK command - Get the rank (index) of a member in a sorted set
pub async fn zrank(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let member = &args[1];
    
    // Get the sorted set
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist
            return Ok(RespValue::BulkString(None));
        },
    };
    
    // Sort by score (ascending order)
    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Find the rank (0-based index) of the member
    for (index, (_, m)) in sorted_set.iter().enumerate() {
        if m == member {
            return Ok(RespValue::Integer(index as i64));
        }
    }
    
    // Member not found
    Ok(RespValue::BulkString(None))
}

/// Redis ZREVRANGE command - Return a range of members in a sorted set by index (in reverse order)
pub async fn zrevrange(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 || args.len() > 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    
    // Parse start and stop indices
    let start_str = bytes_to_string(&args[1])?;
    let stop_str = bytes_to_string(&args[2])?;
    
    let start = start_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid start index, not an integer".to_string())
    })?;
    
    let stop = stop_str.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("Invalid stop index, not an integer".to_string())
    })?;
    
    // Check for WITHSCORES option
    let with_scores = args.len() == 4 && 
        bytes_to_string(&args[3]).map(|s| s.to_uppercase() == "WITHSCORES").unwrap_or(false);
    
    // Get the sorted set
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(mut sorted_set) => {
                    // Sort by score for consistency with Redis
                    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
                    sorted_set
                },
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => {
            // Sorted set doesn't exist, return empty array (Redis behavior)
            return Ok(RespValue::Array(Some(vec![])));
        },
    };
    
    // Reverse the sorted set for descending order
    sorted_set.reverse();
    
    // Get the total number of elements
    let len = sorted_set.len() as i64;
    
    // Convert indices to zero-based array indices
    let start_idx = if start < 0 {
        // Negative index means count from the end
        (len + start).max(0) as usize
    } else {
        start as usize
    };
    
    let stop_idx = if stop < 0 {
        // Negative index means count from the end
        (len + stop).min(len - 1) as usize
    } else {
        stop.min(len - 1) as usize
    };
    
    // Get the elements in the range
    let mut result = Vec::new();
    if start_idx <= stop_idx && start_idx < sorted_set.len() {
        // Calculate the actual range length
        let actual_stop_idx = stop_idx.min(sorted_set.len() - 1);
        
        // Pre-allocate vector with appropriate capacity
        // If with_scores is true, we'll have twice as many items
        let range_len = actual_stop_idx - start_idx + 1;
        let capacity = if with_scores { range_len * 2 } else { range_len };
        result = Vec::with_capacity(capacity);
        
        for (score, member) in sorted_set.iter().take(actual_stop_idx + 1).skip(start_idx) {
            // Always add the member
            result.push(RespValue::BulkString(Some(member.clone())));
            
            // If WITHSCORES option is present, add the score after each member as a separate array item
            if with_scores {
                // Format score as string with proper format
                let score_str = format!("{}", score);
                result.push(RespValue::BulkString(Some(score_str.into_bytes())));
            }
        }
    }
    
    // ZREVRANGE always returns an array (Redis behavior)
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZINCRBY command - Increment the score of a member in a sorted set
pub async fn zincrby(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    
    let key = &args[0];
    let increment_str = bytes_to_string(&args[1])?;
    let member = &args[2];
    
    // Parse the increment value
    let increment = increment_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("Increment is not a valid float".to_string())
    })?;
    
    // Get the current sorted set or create a new one
    let mut sorted_set = match engine.get(key).await? {
        Some(data) => {
            match bincode::deserialize::<SortedSet>(&data) {
                Ok(sorted_set) => sorted_set,
                Err(_) => {
                    return Err(CommandError::WrongType);
                }
            }
        },
        None => Vec::new(),
    };
    
    // Find if member already exists and update its score
    let mut found = false;
    let mut new_score = increment;
    
    for (score, m) in sorted_set.iter_mut() {
        if m == member {
            *score += increment;
            new_score = *score;
            found = true;
            break;
        }
    }
    
    // If member doesn't exist, add it with the increment as its score
    if !found {
        sorted_set.push((increment, member.clone()));
        new_score = increment;
    }
    
    // Sort the set by score
    sorted_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    
    // Serialize and save the sorted set
    let serialized = bincode::serialize(&sorted_set).map_err(|e| {
        CommandError::InternalError(format!("Serialization error: {}", e))
    })?;
    
    engine.set_with_type(key.clone(), serialized, RedisDataType::ZSet, None).await?;
    
    // Return the new score
    let score_str = format_score(new_score);
    Ok(RespValue::BulkString(Some(score_str.into_bytes())))
}

/// Redis ZRANGEBYSCORE command - Return members with scores in range
pub async fn zrangebyscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;
    let (min_score, min_exclusive) = parse_score_bound(&min_str)?;
    let (max_score, max_exclusive) = parse_score_bound(&max_str)?;

    let mut with_scores = false;
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => { with_scores = true; i += 1; }
            "LIMIT" => {
                if i + 2 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                offset = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if cnt >= 0 { count = Some(cnt as usize); }
                i += 3;
            }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let filtered: Vec<&(f64, Vec<u8>)> = sorted_set.iter()
        .filter(|(score, _)| {
            let above_min = if min_exclusive { *score > min_score } else { *score >= min_score };
            let below_max = if max_exclusive { *score < max_score } else { *score <= max_score };
            above_min && below_max
        })
        .skip(offset)
        .take(count.unwrap_or(usize::MAX))
        .collect();

    let mut result = Vec::new();
    for (score, member) in filtered {
        result.push(RespValue::BulkString(Some(member.clone())));
        if with_scores {
            result.push(RespValue::BulkString(Some(format_score(*score).into_bytes())));
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREVRANGEBYSCORE command - Return members with scores in range (high to low)
pub async fn zrevrangebyscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let max_str = bytes_to_string(&args[1])?;
    let min_str = bytes_to_string(&args[2])?;
    let (max_score, max_exclusive) = parse_score_bound(&max_str)?;
    let (min_score, min_exclusive) = parse_score_bound(&min_str)?;

    let mut with_scores = false;
    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WITHSCORES" => { with_scores = true; i += 1; }
            "LIMIT" => {
                if i + 2 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                offset = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if cnt >= 0 { count = Some(cnt as usize); }
                i += 3;
            }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let filtered: Vec<&(f64, Vec<u8>)> = sorted_set.iter().rev()
        .filter(|(score, _)| {
            let above_min = if min_exclusive { *score > min_score } else { *score >= min_score };
            let below_max = if max_exclusive { *score < max_score } else { *score <= max_score };
            above_min && below_max
        })
        .skip(offset)
        .take(count.unwrap_or(usize::MAX))
        .collect();

    let mut result = Vec::new();
    for (score, member) in filtered {
        result.push(RespValue::BulkString(Some(member.clone())));
        if with_scores {
            result.push(RespValue::BulkString(Some(format_score(*score).into_bytes())));
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZRANGEBYLEX command - Return members in lex range (all same score assumed)
pub async fn zrangebylex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;
    let min = parse_lex_bound(&min_str)?;
    let max = parse_lex_bound(&max_str)?;

    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        if opt == "LIMIT" {
            if i + 2 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
            offset = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            if cnt >= 0 { count = Some(cnt as usize); }
            i += 3;
        } else {
            return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt)));
        }
    }

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let result: Vec<RespValue> = sorted_set.iter()
        .filter(|(_, member)| in_lex_range(member, &min, &max))
        .skip(offset)
        .take(count.unwrap_or(usize::MAX))
        .map(|(_, member)| RespValue::BulkString(Some(member.clone())))
        .collect();

    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREVRANGEBYLEX command - Return members in reverse lex range
pub async fn zrevrangebylex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let max_str = bytes_to_string(&args[1])?;
    let min_str = bytes_to_string(&args[2])?;
    let max = parse_lex_bound(&max_str)?;
    let min = parse_lex_bound(&min_str)?;

    let mut offset: usize = 0;
    let mut count: Option<usize> = None;
    let mut i = 3;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        if opt == "LIMIT" {
            if i + 2 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
            offset = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?;
            if cnt >= 0 { count = Some(cnt as usize); }
            i += 3;
        } else {
            return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt)));
        }
    }

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let result: Vec<RespValue> = sorted_set.iter().rev()
        .filter(|(_, member)| in_lex_range(member, &min, &max))
        .skip(offset)
        .take(count.unwrap_or(usize::MAX))
        .map(|(_, member)| RespValue::BulkString(Some(member.clone())))
        .collect();

    Ok(RespValue::Array(Some(result)))
}

/// Redis ZREMRANGEBYRANK command - Remove members by rank range
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

    let mut sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Integer(0)),
    };

    let len = sorted_set.len() as i64;
    let start_idx = if start < 0 { (len + start).max(0) as usize } else { start as usize };
    let stop_idx = if stop < 0 { (len + stop).max(0) as usize } else { stop.min(len - 1) as usize };

    if start_idx > stop_idx || start_idx >= sorted_set.len() {
        return Ok(RespValue::Integer(0));
    }

    let actual_stop = stop_idx.min(sorted_set.len() - 1);
    let removed = actual_stop - start_idx + 1;
    sorted_set.drain(start_idx..=actual_stop);
    save_sorted_set(engine, key, &sorted_set).await?;
    Ok(RespValue::Integer(removed as i64))
}

/// Redis ZREMRANGEBYSCORE command - Remove members by score range
pub async fn zremrangebyscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;
    let (min_score, min_exclusive) = parse_score_bound(&min_str)?;
    let (max_score, max_exclusive) = parse_score_bound(&max_str)?;

    let mut sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Integer(0)),
    };

    let old_len = sorted_set.len();
    sorted_set.retain(|(score, _)| {
        let above_min = if min_exclusive { *score > min_score } else { *score >= min_score };
        let below_max = if max_exclusive { *score < max_score } else { *score <= max_score };
        !(above_min && below_max)
    });
    let removed = old_len - sorted_set.len();
    save_sorted_set(engine, key, &sorted_set).await?;
    Ok(RespValue::Integer(removed as i64))
}

/// Redis ZREMRANGEBYLEX command - Remove members by lex range
pub async fn zremrangebylex(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;
    let min = parse_lex_bound(&min_str)?;
    let max = parse_lex_bound(&max_str)?;

    let mut sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Integer(0)),
    };

    let old_len = sorted_set.len();
    sorted_set.retain(|(_, member)| !in_lex_range(member, &min, &max));
    let removed = old_len - sorted_set.len();
    save_sorted_set(engine, key, &sorted_set).await?;
    Ok(RespValue::Integer(removed as i64))
}

/// Redis ZLEXCOUNT command - Count members in lex range
pub async fn zlexcount(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let min_str = bytes_to_string(&args[1])?;
    let max_str = bytes_to_string(&args[2])?;
    let min = parse_lex_bound(&min_str)?;
    let max = parse_lex_bound(&max_str)?;

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Integer(0)),
    };

    let count = sorted_set.iter()
        .filter(|(_, member)| in_lex_range(member, &min, &max))
        .count();
    Ok(RespValue::Integer(count as i64))
}

/// Redis ZREVRANK command - Get the reverse rank of a member
pub async fn zrevrank(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let member = &args[1];

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::BulkString(None)),
    };

    let len = sorted_set.len();
    for (index, (_, m)) in sorted_set.iter().enumerate() {
        if m == member {
            return Ok(RespValue::Integer((len - 1 - index) as i64));
        }
    }
    Ok(RespValue::BulkString(None))
}

/// Redis ZINTERSTORE command - Store intersection of sorted sets
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

    let keys = &args[2..2 + numkeys];
    let mut i = 2 + numkeys;
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = "SUM".to_string();

    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                if i + numkeys >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                for j in 0..numkeys {
                    weights[j] = bytes_to_string(&args[i + 1 + j])?.parse::<f64>().map_err(|_| {
                        CommandError::InvalidArgument("weight value is not a float".to_string())
                    })?;
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                aggregate = bytes_to_string(&args[i + 1])?.to_uppercase();
                if !matches!(aggregate.as_str(), "SUM" | "MIN" | "MAX") {
                    return Err(CommandError::InvalidArgument("aggregate must be SUM, MIN, or MAX".to_string()));
                }
                i += 2;
            }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    // Load all sets
    let mut sets: Vec<SortedSet> = Vec::with_capacity(numkeys);
    for key in keys {
        match load_sorted_set(engine, key).await? {
            Some(ss) => sets.push(ss),
            None => sets.push(Vec::new()),
        }
    }

    // Intersection: members present in all sets
    if sets.is_empty() || sets.iter().any(|s| s.is_empty()) {
        save_sorted_set(engine, dest, &Vec::new()).await?;
        return Ok(RespValue::Integer(0));
    }

    let mut result: SortedSet = Vec::new();
    for (score, member) in &sets[0] {
        let mut in_all = true;
        let mut agg_score = *score * weights[0];
        for (idx, set) in sets.iter().enumerate().skip(1) {
            if let Some((s, _)) = set.iter().find(|(_, m)| m == member) {
                let weighted = *s * weights[idx];
                agg_score = match aggregate.as_str() {
                    "SUM" => agg_score + weighted,
                    "MIN" => agg_score.min(weighted),
                    "MAX" => agg_score.max(weighted),
                    _ => agg_score + weighted,
                };
            } else {
                in_all = false;
                break;
            }
        }
        if in_all {
            result.push((agg_score, member.clone()));
        }
    }

    result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.1.cmp(&b.1)));
    let count = result.len() as i64;
    save_sorted_set(engine, dest, &result).await?;
    Ok(RespValue::Integer(count))
}

/// Redis ZUNIONSTORE command - Store union of sorted sets
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

    let keys = &args[2..2 + numkeys];
    let mut i = 2 + numkeys;
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = "SUM".to_string();

    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                if i + numkeys >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                for j in 0..numkeys {
                    weights[j] = bytes_to_string(&args[i + 1 + j])?.parse::<f64>().map_err(|_| {
                        CommandError::InvalidArgument("weight value is not a float".to_string())
                    })?;
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                aggregate = bytes_to_string(&args[i + 1])?.to_uppercase();
                if !matches!(aggregate.as_str(), "SUM" | "MIN" | "MAX") {
                    return Err(CommandError::InvalidArgument("aggregate must be SUM, MIN, or MAX".to_string()));
                }
                i += 2;
            }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    // Union: collect all members, aggregate scores
    let mut member_scores: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
    let mut member_seen: std::collections::HashMap<Vec<u8>, bool> = std::collections::HashMap::new();

    for (idx, key) in keys.iter().enumerate() {
        let set = match load_sorted_set(engine, key).await? {
            Some(ss) => ss,
            None => continue,
        };
        for (score, member) in &set {
            let weighted = *score * weights[idx];
            if let Some(existing) = member_scores.get_mut(member) {
                *existing = match aggregate.as_str() {
                    "SUM" => *existing + weighted,
                    "MIN" => existing.min(weighted),
                    "MAX" => existing.max(weighted),
                    _ => *existing + weighted,
                };
            } else {
                member_scores.insert(member.clone(), weighted);
            }
            member_seen.insert(member.clone(), true);
        }
    }

    let mut result: SortedSet = member_scores.into_iter()
        .map(|(member, score)| (score, member))
        .collect();
    result.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.1.cmp(&b.1)));
    let count = result.len() as i64;
    save_sorted_set(engine, dest, &result).await?;
    Ok(RespValue::Integer(count))
}

/// Redis ZINTER command - Return intersection without storing
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

    let keys = &args[1..1 + numkeys];
    let mut i = 1 + numkeys;
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = "SUM".to_string();
    let mut with_scores = false;

    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                if i + numkeys >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                for j in 0..numkeys {
                    weights[j] = bytes_to_string(&args[i + 1 + j])?.parse::<f64>().map_err(|_| {
                        CommandError::InvalidArgument("weight value is not a float".to_string())
                    })?;
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                aggregate = bytes_to_string(&args[i + 1])?.to_uppercase();
                i += 2;
            }
            "WITHSCORES" => { with_scores = true; i += 1; }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    let mut sets: Vec<SortedSet> = Vec::new();
    for key in keys {
        match load_sorted_set(engine, key).await? {
            Some(ss) => sets.push(ss),
            None => return Ok(RespValue::Array(Some(vec![]))),
        }
    }

    let mut result_set: SortedSet = Vec::new();
    for (score, member) in &sets[0] {
        let mut in_all = true;
        let mut agg_score = *score * weights[0];
        for (idx, set) in sets.iter().enumerate().skip(1) {
            if let Some((s, _)) = set.iter().find(|(_, m)| m == member) {
                let weighted = *s * weights[idx];
                agg_score = match aggregate.as_str() {
                    "SUM" => agg_score + weighted,
                    "MIN" => agg_score.min(weighted),
                    "MAX" => agg_score.max(weighted),
                    _ => agg_score + weighted,
                };
            } else {
                in_all = false;
                break;
            }
        }
        if in_all {
            result_set.push((agg_score, member.clone()));
        }
    }
    result_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.1.cmp(&b.1)));

    let mut result = Vec::new();
    for (score, member) in &result_set {
        result.push(RespValue::BulkString(Some(member.clone())));
        if with_scores {
            result.push(RespValue::BulkString(Some(format_score(*score).into_bytes())));
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZUNION command - Return union without storing
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

    let keys = &args[1..1 + numkeys];
    let mut i = 1 + numkeys;
    let mut weights: Vec<f64> = vec![1.0; numkeys];
    let mut aggregate = "SUM".to_string();
    let mut with_scores = false;

    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "WEIGHTS" => {
                if i + numkeys >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                for j in 0..numkeys {
                    weights[j] = bytes_to_string(&args[i + 1 + j])?.parse::<f64>().map_err(|_| {
                        CommandError::InvalidArgument("weight value is not a float".to_string())
                    })?;
                }
                i += 1 + numkeys;
            }
            "AGGREGATE" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                aggregate = bytes_to_string(&args[i + 1])?.to_uppercase();
                i += 2;
            }
            "WITHSCORES" => { with_scores = true; i += 1; }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    let mut member_scores: std::collections::HashMap<Vec<u8>, f64> = std::collections::HashMap::new();
    for (idx, key) in keys.iter().enumerate() {
        if let Some(set) = load_sorted_set(engine, key).await? {
            for (score, member) in &set {
                let weighted = *score * weights[idx];
                member_scores.entry(member.clone())
                    .and_modify(|e| {
                        *e = match aggregate.as_str() {
                            "SUM" => *e + weighted,
                            "MIN" => e.min(weighted),
                            "MAX" => e.max(weighted),
                            _ => *e + weighted,
                        };
                    })
                    .or_insert(weighted);
            }
        }
    }

    let mut result_set: SortedSet = member_scores.into_iter()
        .map(|(member, score)| (score, member))
        .collect();
    result_set.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal).then_with(|| a.1.cmp(&b.1)));

    let mut result = Vec::new();
    for (score, member) in &result_set {
        result.push(RespValue::BulkString(Some(member.clone())));
        if with_scores {
            result.push(RespValue::BulkString(Some(format_score(*score).into_bytes())));
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZDIFF command - Return difference without storing
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

    let keys = &args[1..1 + numkeys];
    let with_scores = args.len() > 1 + numkeys && bytes_to_string(&args[1 + numkeys])?.to_uppercase() == "WITHSCORES";

    let first_set = match load_sorted_set(engine, &keys[0]).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    // Collect members from other sets
    let mut other_members: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for key in keys.iter().skip(1) {
        if let Some(set) = load_sorted_set(engine, key).await? {
            for (_, member) in &set {
                other_members.insert(member.clone());
            }
        }
    }

    let mut result = Vec::new();
    for (score, member) in &first_set {
        if !other_members.contains(member) {
            result.push(RespValue::BulkString(Some(member.clone())));
            if with_scores {
                result.push(RespValue::BulkString(Some(format_score(*score).into_bytes())));
            }
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZDIFFSTORE command - Store difference of sorted sets
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

    let keys = &args[2..2 + numkeys];
    let first_set = match load_sorted_set(engine, &keys[0]).await? {
        Some(ss) => ss,
        None => {
            save_sorted_set(engine, dest, &Vec::new()).await?;
            return Ok(RespValue::Integer(0));
        }
    };

    let mut other_members: std::collections::HashSet<Vec<u8>> = std::collections::HashSet::new();
    for key in keys.iter().skip(1) {
        if let Some(set) = load_sorted_set(engine, key).await? {
            for (_, member) in &set {
                other_members.insert(member.clone());
            }
        }
    }

    let result: SortedSet = first_set.into_iter()
        .filter(|(_, member)| !other_members.contains(member))
        .collect();
    let count = result.len() as i64;
    save_sorted_set(engine, dest, &result).await?;
    Ok(RespValue::Integer(count))
}

/// Redis ZPOPMIN command - Remove and return members with lowest scores
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

    let mut sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let take = count.min(sorted_set.len());
    let popped: Vec<(f64, Vec<u8>)> = sorted_set.drain(..take).collect();
    save_sorted_set(engine, key, &sorted_set).await?;

    let mut result = Vec::with_capacity(take * 2);
    for (score, member) in popped {
        result.push(RespValue::BulkString(Some(member)));
        result.push(RespValue::BulkString(Some(format_score(score).into_bytes())));
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZPOPMAX command - Remove and return members with highest scores
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

    let mut sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
    };

    let take = count.min(sorted_set.len());
    let start = sorted_set.len() - take;
    let popped: Vec<(f64, Vec<u8>)> = sorted_set.drain(start..).collect();
    save_sorted_set(engine, key, &sorted_set).await?;

    let mut result = Vec::with_capacity(take * 2);
    for (score, member) in popped.into_iter().rev() {
        result.push(RespValue::BulkString(Some(member)));
        result.push(RespValue::BulkString(Some(format_score(score).into_bytes())));
    }
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
    // Try immediate pop first
    for key in keys {
        if let Some(ss) = load_sorted_set(engine, key).await? {
            if !ss.is_empty() {
                let pop_args = vec![key.clone()];
                let result = if pop_min {
                    zpopmin(engine, &pop_args).await?
                } else {
                    zpopmax(engine, &pop_args).await?
                };
                if let RespValue::Array(Some(ref items)) = result {
                    if items.len() >= 2 {
                        return Ok(RespValue::Array(Some(vec![
                            RespValue::BulkString(Some(key.clone())),
                            items[0].clone(),
                            items[1].clone(),
                        ])));
                    }
                }
            }
        }
    }

    let deadline = if timeout_secs > 0.0 {
        Some(Instant::now() + Duration::from_secs_f64(timeout_secs))
    } else {
        None
    };

    loop {
        let receivers: Vec<_> = keys.iter()
            .map(|k| blocking_mgr.subscribe(k))
            .collect();

        for key in keys {
            if let Some(ss) = load_sorted_set(engine, key).await? {
                if !ss.is_empty() {
                    let pop_args = vec![key.clone()];
                    let result = if pop_min {
                        zpopmin(engine, &pop_args).await?
                    } else {
                        zpopmax(engine, &pop_args).await?
                    };
                    if let RespValue::Array(Some(ref items)) = result {
                        if items.len() >= 2 {
                            return Ok(RespValue::Array(Some(vec![
                                RespValue::BulkString(Some(key.clone())),
                                items[0].clone(),
                                items[1].clone(),
                            ])));
                        }
                    }
                }
            }
        }

        let futs: Vec<_> = receivers.into_iter()
            .map(|mut rx| Box::pin(async move { let _ = rx.changed().await; }))
            .collect();

        if futs.is_empty() {
            return Ok(RespValue::Array(None));
        }

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::Array(None));
            }
            if tokio::time::timeout(remaining, select_all(futs)).await.is_err() {
                return Ok(RespValue::Array(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
}

/// Redis BZPOPMIN command - Blocking pop of member with lowest score
pub async fn bzpopmin(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let timeout_str = bytes_to_string(args.last().unwrap())?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument("timeout is negative".to_string()));
    }
    let keys = &args[..args.len() - 1];
    blocking_zpop(engine, keys, timeout_secs, true, blocking_mgr).await
}

/// Redis BZPOPMAX command - Blocking pop of member with highest score
pub async fn bzpopmax(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let timeout_str = bytes_to_string(args.last().unwrap())?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument("timeout is negative".to_string()));
    }
    let keys = &args[..args.len() - 1];
    blocking_zpop(engine, keys, timeout_secs, false, blocking_mgr).await
}

/// Redis ZRANDMEMBER command - Return random members
pub async fn zrandmember(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => {
            if args.len() == 1 {
                return Ok(RespValue::BulkString(None));
            }
            return Ok(RespValue::Array(Some(vec![])));
        }
    };

    if args.len() == 1 {
        // Return single random member
        let idx = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as usize) % sorted_set.len();
        return Ok(RespValue::BulkString(Some(sorted_set[idx].1.clone())));
    }

    let count_val = bytes_to_string(&args[1])?.parse::<i64>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;
    let with_scores = args.len() == 3 && bytes_to_string(&args[2])?.to_uppercase() == "WITHSCORES";

    let mut result = Vec::new();
    if count_val > 0 {
        // Unique random members, up to count or set size
        let count = (count_val as usize).min(sorted_set.len());
        // Simple selection: take first `count` after a pseudo-random shuffle
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as usize;
        let mut indices: Vec<usize> = (0..sorted_set.len()).collect();
        // Fisher-Yates with simple PRNG
        let mut rng = seed;
        for i in (1..indices.len()).rev() {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = rng % (i + 1);
            indices.swap(i, j);
        }
        for &idx in indices.iter().take(count) {
            result.push(RespValue::BulkString(Some(sorted_set[idx].1.clone())));
            if with_scores {
                result.push(RespValue::BulkString(Some(format_score(sorted_set[idx].0).into_bytes())));
            }
        }
    } else if count_val < 0 {
        // Allow duplicates
        let count = (-count_val) as usize;
        let seed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .subsec_nanos() as usize;
        let mut rng = seed;
        for _ in 0..count {
            rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
            let idx = rng % sorted_set.len();
            result.push(RespValue::BulkString(Some(sorted_set[idx].1.clone())));
            if with_scores {
                result.push(RespValue::BulkString(Some(format_score(sorted_set[idx].0).into_bytes())));
            }
        }
    }
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZMSCORE command - Get scores of multiple members
pub async fn zmscore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let members = &args[1..];

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => {
            let result: Vec<RespValue> = members.iter().map(|_| RespValue::BulkString(None)).collect();
            return Ok(RespValue::Array(Some(result)));
        }
    };

    let result: Vec<RespValue> = members.iter().map(|member| {
        match sorted_set.iter().find(|(_, m)| m == member) {
            Some((score, _)) => RespValue::BulkString(Some(format_score(*score).into_bytes())),
            None => RespValue::BulkString(None),
        }
    }).collect();
    Ok(RespValue::Array(Some(result)))
}

/// Redis ZMPOP command - Pop from multiple sorted sets
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
            bytes_to_string(&args[3 + numkeys])?.parse::<usize>().map_err(|_| {
                CommandError::InvalidArgument("value is not an integer or out of range".to_string())
            })?
        } else {
            1
        }
    } else {
        1
    };

    for key in keys {
        let mut sorted_set = match load_sorted_set(engine, key).await? {
            Some(ss) if !ss.is_empty() => ss,
            _ => continue,
        };

        let take = count.min(sorted_set.len());
        let popped: Vec<(f64, Vec<u8>)> = if direction == "MIN" {
            sorted_set.drain(..take).collect()
        } else {
            let start = sorted_set.len() - take;
            let mut p: Vec<(f64, Vec<u8>)> = sorted_set.drain(start..).collect();
            p.reverse();
            p
        };
        save_sorted_set(engine, key, &sorted_set).await?;

        let mut elements = Vec::with_capacity(take * 2);
        for (score, member) in popped {
            elements.push(RespValue::BulkString(Some(member)));
            elements.push(RespValue::BulkString(Some(format_score(score).into_bytes())));
        }
        return Ok(RespValue::Array(Some(vec![
            RespValue::BulkString(Some(key.clone())),
            RespValue::Array(Some(elements)),
        ])));
    }

    Ok(RespValue::Array(None))
}

/// Redis BZMPOP command - Blocking ZMPOP
pub async fn bzmpop(engine: &StorageEngine, args: &[Vec<u8>], blocking_mgr: &BlockingManager) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let timeout_str = bytes_to_string(&args[0])?;
    let timeout_secs = timeout_str.parse::<f64>().map_err(|_| {
        CommandError::InvalidArgument("timeout is not a float or out of range".to_string())
    })?;
    if timeout_secs < 0.0 {
        return Err(CommandError::InvalidArgument("timeout is negative".to_string()));
    }

    let zmpop_args = &args[1..];

    // Try immediately
    let result = zmpop(engine, zmpop_args).await?;
    if result != RespValue::Array(None) {
        return Ok(result);
    }

    // Parse keys for subscription
    let numkeys = bytes_to_string(&zmpop_args[0])?.parse::<usize>().map_err(|_| {
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
        let receivers: Vec<_> = keys.iter()
            .map(|k| blocking_mgr.subscribe(k))
            .collect();

        let result = zmpop(engine, zmpop_args).await?;
        if result != RespValue::Array(None) {
            return Ok(result);
        }

        let futs: Vec<_> = receivers.into_iter()
            .map(|mut rx| Box::pin(async move { let _ = rx.changed().await; }))
            .collect();

        if futs.is_empty() {
            return Ok(RespValue::Array(None));
        }

        if let Some(deadline) = deadline {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Ok(RespValue::Array(None));
            }
            if tokio::time::timeout(remaining, select_all(futs)).await.is_err() {
                return Ok(RespValue::Array(None));
            }
        } else {
            let _ = select_all(futs).await;
        }
    }
}

/// Redis ZRANGESTORE command - Store the result of ZRANGE into a destination key
pub async fn zrangestore(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let dst = &args[0];
    let src = &args[1];
    let min_str = bytes_to_string(&args[2])?;
    let max_str = bytes_to_string(&args[3])?;

    let mut by_score = false;
    let mut by_lex = false;
    let mut rev = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;
    let mut i = 4;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "BYSCORE" => { by_score = true; i += 1; }
            "BYLEX" => { by_lex = true; i += 1; }
            "REV" => { rev = true; i += 1; }
            "LIMIT" => {
                if i + 2 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                limit_offset = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if cnt >= 0 { limit_count = Some(cnt as usize); }
                i += 3;
            }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    let sorted_set = match load_sorted_set(engine, src).await? {
        Some(ss) => ss,
        None => {
            save_sorted_set(engine, dst, &Vec::new()).await?;
            return Ok(RespValue::Integer(0));
        }
    };

    let result: SortedSet = if by_score {
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
        let iter: Box<dyn Iterator<Item = &(f64, Vec<u8>)>> = if rev {
            Box::new(sorted_set.iter().rev())
        } else {
            Box::new(sorted_set.iter())
        };
        iter.filter(|(score, _)| {
            let above = if min_excl { *score > min_score } else { *score >= min_score };
            let below = if max_excl { *score < max_score } else { *score <= max_score };
            above && below
        })
        .skip(limit_offset)
        .take(limit_count.unwrap_or(usize::MAX))
        .cloned()
        .collect()
    } else if by_lex {
        let min_bound = if rev { parse_lex_bound(&max_str)? } else { parse_lex_bound(&min_str)? };
        let max_bound = if rev { parse_lex_bound(&min_str)? } else { parse_lex_bound(&max_str)? };
        let iter: Box<dyn Iterator<Item = &(f64, Vec<u8>)>> = if rev {
            Box::new(sorted_set.iter().rev())
        } else {
            Box::new(sorted_set.iter())
        };
        iter.filter(|(_, member)| in_lex_range(member, &min_bound, &max_bound))
            .skip(limit_offset)
            .take(limit_count.unwrap_or(usize::MAX))
            .cloned()
            .collect()
    } else {
        // By rank (index)
        let len = sorted_set.len() as i64;
        let start = min_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        let stop = max_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

        if rev {
            let start_idx = if start < 0 { (len + start).max(0) as usize } else { start as usize };
            let stop_idx = if stop < 0 { (len + stop).max(0) as usize } else { stop.min(len - 1) as usize };
            if start_idx <= stop_idx && start_idx < sorted_set.len() {
                let actual_stop = stop_idx.min(sorted_set.len() - 1);
                sorted_set[start_idx..=actual_stop].iter().rev().cloned().collect()
            } else {
                Vec::new()
            }
        } else {
            let start_idx = if start < 0 { (len + start).max(0) as usize } else { start as usize };
            let stop_idx = if stop < 0 { (len + stop).max(0) as usize } else { stop.min(len - 1) as usize };
            if start_idx <= stop_idx && start_idx < sorted_set.len() {
                let actual_stop = stop_idx.min(sorted_set.len() - 1);
                sorted_set[start_idx..=actual_stop].to_vec()
            } else {
                Vec::new()
            }
        }
    };

    let count = result.len() as i64;
    save_sorted_set(engine, dst, &result).await?;
    Ok(RespValue::Integer(count))
}

/// Redis ZSCAN command - Incrementally iterate sorted set members
pub async fn zscan(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }
    let key = &args[0];
    let cursor = bytes_to_string(&args[1])?.parse::<usize>().map_err(|_| {
        CommandError::InvalidArgument("value is not an integer or out of range".to_string())
    })?;

    let mut pattern: Option<String> = None;
    let mut count: usize = 10;
    let mut i = 2;
    while i < args.len() {
        let opt = bytes_to_string(&args[i])?.to_uppercase();
        match opt.as_str() {
            "MATCH" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                pattern = Some(bytes_to_string(&args[i + 1])?);
                i += 2;
            }
            "COUNT" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                count = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                i += 2;
            }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => {
            return Ok(RespValue::Array(Some(vec![
                RespValue::BulkString(Some(b"0".to_vec())),
                RespValue::Array(Some(vec![])),
            ])));
        }
    };

    let mut elements = Vec::new();
    let mut new_cursor = 0usize;
    let mut scanned = 0;

    for (idx, (score, member)) in sorted_set.iter().enumerate().skip(cursor) {
        let member_str = String::from_utf8_lossy(member);
        let matches = match &pattern {
            Some(pat) => glob_match(pat, &member_str),
            None => true,
        };
        if matches {
            elements.push(RespValue::BulkString(Some(member.clone())));
            elements.push(RespValue::BulkString(Some(format_score(*score).into_bytes())));
        }
        scanned += 1;
        if scanned >= count {
            new_cursor = idx + 1;
            if new_cursor >= sorted_set.len() {
                new_cursor = 0;
            }
            break;
        }
    }

    if scanned < count {
        new_cursor = 0; // Scan complete
    }

    Ok(RespValue::Array(Some(vec![
        RespValue::BulkString(Some(new_cursor.to_string().into_bytes())),
        RespValue::Array(Some(elements)),
    ])))
}

/// Simple glob pattern matching (supports * and ?) using O(1) memory iterative algorithm
fn glob_match(pattern: &str, s: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let s: Vec<char> = s.chars().collect();
    let (plen, slen) = (p.len(), s.len());
    let mut pi = 0;
    let mut si = 0;
    let mut star_pi: Option<usize> = None;
    let mut star_si: usize = 0;

    while si < slen {
        if pi < plen && (p[pi] == '?' || p[pi] == s[si]) {
            pi += 1;
            si += 1;
        } else if pi < plen && p[pi] == '*' {
            star_pi = Some(pi);
            star_si = si;
            pi += 1;
        } else if let Some(sp) = star_pi {
            pi = sp + 1;
            star_si += 1;
            si = star_si;
        } else {
            return false;
        }
    }

    while pi < plen && p[pi] == '*' {
        pi += 1;
    }

    pi == plen
}

/// Redis unified ZRANGE command (Redis 6.2+) with BYSCORE, BYLEX, REV, LIMIT options
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
            "BYSCORE" => { by_score = true; i += 1; }
            "BYLEX" => { by_lex = true; i += 1; }
            "REV" => { rev = true; i += 1; }
            "WITHSCORES" => { with_scores = true; i += 1; }
            "LIMIT" => {
                if i + 2 >= args.len() { return Err(CommandError::WrongNumberOfArguments); }
                limit_offset = bytes_to_string(&args[i + 1])?.parse::<usize>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                let cnt = bytes_to_string(&args[i + 2])?.parse::<i64>().map_err(|_| {
                    CommandError::InvalidArgument("value is not an integer or out of range".to_string())
                })?;
                if cnt >= 0 { limit_count = Some(cnt as usize); }
                i += 3;
            }
            _ => return Err(CommandError::InvalidArgument(format!("unsupported option: {}", opt))),
        }
    }

    // Detect if unified options are used; if not, fall back to original ZRANGE behavior
    if !by_score && !by_lex && !rev && limit_count.is_none() {
        // Original ZRANGE behavior (index-based)
        let mut orig_args = vec![args[0].clone(), args[1].clone(), args[2].clone()];
        if with_scores {
            orig_args.push(b"WITHSCORES".to_vec());
        }
        return zrange(engine, &orig_args).await;
    }

    let sorted_set = match load_sorted_set(engine, key).await? {
        Some(ss) => ss,
        None => return Ok(RespValue::Array(Some(vec![]))),
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
        let iter: Box<dyn Iterator<Item = &(f64, Vec<u8>)>> = if rev {
            Box::new(sorted_set.iter().rev())
        } else {
            Box::new(sorted_set.iter())
        };
        iter.filter(|(score, _)| {
            let above = if min_excl { *score > min_score } else { *score >= min_score };
            let below = if max_excl { *score < max_score } else { *score <= max_score };
            above && below
        })
        .skip(limit_offset)
        .take(limit_count.unwrap_or(usize::MAX))
        .cloned()
        .collect()
    } else if by_lex {
        let min_bound = if rev { parse_lex_bound(&max_str)? } else { parse_lex_bound(&min_str)? };
        let max_bound = if rev { parse_lex_bound(&min_str)? } else { parse_lex_bound(&max_str)? };
        let iter: Box<dyn Iterator<Item = &(f64, Vec<u8>)>> = if rev {
            Box::new(sorted_set.iter().rev())
        } else {
            Box::new(sorted_set.iter())
        };
        iter.filter(|(_, member)| in_lex_range(member, &min_bound, &max_bound))
            .skip(limit_offset)
            .take(limit_count.unwrap_or(usize::MAX))
            .cloned()
            .collect()
    } else {
        // REV with index-based
        let len = sorted_set.len() as i64;
        let start = min_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        let stop = max_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        let start_idx = if start < 0 { (len + start).max(0) as usize } else { start as usize };
        let stop_idx = if stop < 0 { (len + stop).max(0) as usize } else { stop.min(len - 1) as usize };
        if start_idx <= stop_idx && start_idx < sorted_set.len() {
            let actual_stop = stop_idx.min(sorted_set.len() - 1);
            if rev {
                sorted_set[start_idx..=actual_stop].iter().rev().cloned().collect()
            } else {
                sorted_set[start_idx..=actual_stop].to_vec()
            }
        } else {
            Vec::new()
        }
    };

    let mut result = Vec::new();
    for (score, member) in &selected {
        result.push(RespValue::BulkString(Some(member.clone())));
        if with_scores {
            result.push(RespValue::BulkString(Some(format_score(*score).into_bytes())));
        }
    }
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
            b"zset1".to_vec(), b"1.5".to_vec(), b"member1".to_vec(),
            b"2.5".to_vec(), b"member2".to_vec(), b"3.5".to_vec(), b"member3".to_vec(),
        ];
        let result = zadd(&engine, &zadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = zscore(&engine, &[b"zset1".to_vec(), b"member2".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2.5".to_vec())));
        let result = zrange(&engine, &[b"zset1".to_vec(), b"0".to_vec(), b"1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(elements)) = result {
            assert_eq!(elements.len(), 2);
            assert_eq!(elements[0], RespValue::BulkString(Some(b"member1".to_vec())));
        } else { panic!("Expected array"); }
        let result = zrem(&engine, &[b"zset1".to_vec(), b"member1".to_vec(), b"nonexistent".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_zrangebyscore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d"), (5.0, b"e")]).await;

        // Basic range
        let result = zrangebyscore(&engine, &[b"zs".to_vec(), b"2".to_vec(), b"4".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"d".to_vec())));
        } else { panic!("Expected array"); }

        // Exclusive bounds
        let result = zrangebyscore(&engine, &[b"zs".to_vec(), b"(1".to_vec(), b"(5".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3); // b, c, d
        } else { panic!("Expected array"); }

        // With LIMIT
        let result = zrangebyscore(&engine, &[
            b"zs".to_vec(), b"-inf".to_vec(), b"+inf".to_vec(),
            b"LIMIT".to_vec(), b"1".to_vec(), b"2".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else { panic!("Expected array"); }

        // WITHSCORES
        let result = zrangebyscore(&engine, &[
            b"zs".to_vec(), b"1".to_vec(), b"2".to_vec(), b"WITHSCORES".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
        } else { panic!("Expected array"); }

        // Nonexistent key
        let result = zrangebyscore(&engine, &[b"nokey".to_vec(), b"-inf".to_vec(), b"+inf".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![])));
    }

    #[tokio::test]
    async fn test_zrevrangebyscore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")]).await;

        let result = zrevrangebyscore(&engine, &[b"zs".to_vec(), b"4".to_vec(), b"2".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"d".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"b".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zrangebylex() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d"), (0.0, b"e")]).await;

        let result = zrangebylex(&engine, &[b"zs".to_vec(), b"[b".to_vec(), b"[d".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else { panic!("Expected array"); }

        // Exclusive
        let result = zrangebylex(&engine, &[b"zs".to_vec(), b"(a".to_vec(), b"(d".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2); // b, c
        } else { panic!("Expected array"); }

        // Unbounded
        let result = zrangebylex(&engine, &[b"zs".to_vec(), b"-".to_vec(), b"+".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 5);
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zrevrangebylex() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d")]).await;

        let result = zrevrangebylex(&engine, &[b"zs".to_vec(), b"[d".to_vec(), b"[b".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"d".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zremrangebyrank() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")]).await;

        let result = zremrangebyrank(&engine, &[b"zs".to_vec(), b"1".to_vec(), b"2".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let result = zrange(&engine, &[b"zs".to_vec(), b"0".to_vec(), b"-1".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"d".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zremrangebyscore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")]).await;

        let result = zremrangebyscore(&engine, &[b"zs".to_vec(), b"2".to_vec(), b"3".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let card = zcard(&engine, &[b"zs".to_vec()]).await.unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zremrangebylex() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d")]).await;

        let result = zremrangebylex(&engine, &[b"zs".to_vec(), b"[b".to_vec(), b"[c".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let card = zcard(&engine, &[b"zs".to_vec()]).await.unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zlexcount() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d"), (0.0, b"e")]).await;

        let result = zlexcount(&engine, &[b"zs".to_vec(), b"[b".to_vec(), b"[d".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let result = zlexcount(&engine, &[b"zs".to_vec(), b"-".to_vec(), b"+".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(5));
    }

    #[tokio::test]
    async fn test_zrevrank() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;

        let result = zrevrank(&engine, &[b"zs".to_vec(), b"a".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let result = zrevrank(&engine, &[b"zs".to_vec(), b"c".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
        let result = zrevrank(&engine, &[b"zs".to_vec(), b"x".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_zinterstore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c"), (30.0, b"d")]).await;

        let result = zinterstore(&engine, &[
            b"out".to_vec(), b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec(),
        ]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let result = zrange(&engine, &[b"out".to_vec(), b"0".to_vec(), b"-1".to_vec(), b"WITHSCORES".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"12".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zinterstore_with_weights() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"a"), (20.0, b"b")]).await;

        let result = zinterstore(&engine, &[
            b"out".to_vec(), b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec(),
            b"WEIGHTS".to_vec(), b"2".to_vec(), b"3".to_vec(),
        ]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        // a: 1*2 + 10*3 = 32, b: 2*2 + 20*3 = 64
        let result = zscore(&engine, &[b"out".to_vec(), b"a".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"32".to_vec())));
    }

    #[tokio::test]
    async fn test_zunionstore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c")]).await;

        let result = zunionstore(&engine, &[
            b"out".to_vec(), b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec(),
        ]).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));

        let result = zscore(&engine, &[b"out".to_vec(), b"b".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"12".to_vec())));
    }

    #[tokio::test]
    async fn test_zinter() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c"), (30.0, b"d")]).await;

        let result = zinter(&engine, &[
            b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec(), b"WITHSCORES".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4); // b+score, c+score
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zunion() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"c")]).await;

        let result = zunion(&engine, &[
            b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec(), b"WITHSCORES".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 6); // a, b, c with scores
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zdiff() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b"), (20.0, b"d")]).await;

        let result = zdiff(&engine, &[b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2); // a, c
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"c".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zdiffstore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;
        add_members(&engine, b"zs2", &[(10.0, b"b")]).await;

        let result = zdiffstore(&engine, &[b"out".to_vec(), b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec()]).await.unwrap();
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
        } else { panic!("Expected array"); }

        // Pop 2
        let result = zpopmin(&engine, &[b"zs".to_vec(), b"2".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
        } else { panic!("Expected array"); }

        // Empty
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
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_bzpopmin_immediate() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b")]).await;

        let result = bzpopmin(&engine, &[b"zs".to_vec(), b"1".to_vec()], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3); // key, member, score
            assert_eq!(items[0], RespValue::BulkString(Some(b"zs".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"a".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"1".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_bzpopmin_timeout() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();

        let result = bzpopmin(&engine, &[b"emptykey".to_vec(), b"0.1".to_vec()], &blocking_mgr).await.unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_bzpopmax_immediate() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b")]).await;

        let result = bzpopmax(&engine, &[b"zs".to_vec(), b"1".to_vec()], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[1], RespValue::BulkString(Some(b"b".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"2".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zrandmember() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;

        // Single random member
        let result = zrandmember(&engine, &[b"zs".to_vec()]).await.unwrap();
        if let RespValue::BulkString(Some(_)) = result {
            // OK, got some member
        } else { panic!("Expected bulk string"); }

        // Positive count (unique)
        let result = zrandmember(&engine, &[b"zs".to_vec(), b"2".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else { panic!("Expected array"); }

        // Negative count (with duplicates)
        let result = zrandmember(&engine, &[b"zs".to_vec(), b"-5".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 5);
        } else { panic!("Expected array"); }

        // Count larger than set - returns all unique
        let result = zrandmember(&engine, &[b"zs".to_vec(), b"10".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        } else { panic!("Expected array"); }

        // Nonexistent key
        let result = zrandmember(&engine, &[b"nokey".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(None));
    }

    #[tokio::test]
    async fn test_zmscore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;

        let result = zmscore(&engine, &[b"zs".to_vec(), b"a".to_vec(), b"x".to_vec(), b"c".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"1".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(None));
            assert_eq!(items[2], RespValue::BulkString(Some(b"3".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zmpop() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;

        let result = zmpop(&engine, &[b"1".to_vec(), b"zs1".to_vec(), b"MIN".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2); // key, array of elements
            assert_eq!(items[0], RespValue::BulkString(Some(b"zs1".to_vec())));
        } else { panic!("Expected array"); }

        // With COUNT
        let result = zmpop(&engine, &[
            b"1".to_vec(), b"zs1".to_vec(), b"MAX".to_vec(), b"COUNT".to_vec(), b"2".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else { panic!("Expected array"); }

        // Empty key
        let result = zmpop(&engine, &[b"1".to_vec(), b"emptykey".to_vec(), b"MIN".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_bzmpop_immediate() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();
        add_members(&engine, b"zs1", &[(1.0, b"a"), (2.0, b"b")]).await;

        let result = bzmpop(&engine, &[
            b"1".to_vec(), b"1".to_vec(), b"zs1".to_vec(), b"MIN".to_vec(),
        ], &blocking_mgr).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2);
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_bzmpop_timeout() {
        let engine = setup_engine().await;
        let blocking_mgr = BlockingManager::new();

        let result = bzmpop(&engine, &[
            b"0.1".to_vec(), b"1".to_vec(), b"emptykey".to_vec(), b"MIN".to_vec(),
        ], &blocking_mgr).await.unwrap();
        assert_eq!(result, RespValue::Array(None));
    }

    #[tokio::test]
    async fn test_zrangestore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")]).await;

        // By rank
        let result = zrangestore(&engine, &[b"dst".to_vec(), b"zs".to_vec(), b"1".to_vec(), b"2".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let result = zrange(&engine, &[b"dst".to_vec(), b"0".to_vec(), b"-1".to_vec(), b"WITHSCORES".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4);
            assert_eq!(items[0], RespValue::BulkString(Some(b"b".to_vec())));
        } else { panic!("Expected array"); }

        // By score
        let result = zrangestore(&engine, &[
            b"dst2".to_vec(), b"zs".to_vec(), b"2".to_vec(), b"3".to_vec(), b"BYSCORE".to_vec(),
        ]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zscan() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"alpha"), (2.0, b"beta"), (3.0, b"gamma")]).await;

        // Scan all
        let result = zscan(&engine, &[b"zs".to_vec(), b"0".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 2); // cursor + array
            if let RespValue::Array(Some(elements)) = &items[1] {
                assert_eq!(elements.len(), 6); // 3 members * 2 (member+score)
            } else { panic!("Expected inner array"); }
        } else { panic!("Expected array"); }

        // Scan with MATCH
        let result = zscan(&engine, &[b"zs".to_vec(), b"0".to_vec(), b"MATCH".to_vec(), b"*eta".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            if let RespValue::Array(Some(elements)) = &items[1] {
                assert_eq!(elements.len(), 2); // beta + score
            } else { panic!("Expected inner array"); }
        } else { panic!("Expected array"); }

        // Nonexistent key
        let result = zscan(&engine, &[b"nokey".to_vec(), b"0".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items[0], RespValue::BulkString(Some(b"0".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zrange_unified_byscore() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")]).await;

        let result = zrange_unified(&engine, &[
            b"zs".to_vec(), b"1".to_vec(), b"3".to_vec(), b"BYSCORE".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"a".to_vec())));
        } else { panic!("Expected array"); }

        // REV + BYSCORE
        let result = zrange_unified(&engine, &[
            b"zs".to_vec(), b"3".to_vec(), b"1".to_vec(), b"BYSCORE".to_vec(), b"REV".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"c".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zrange_unified_bylex() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(0.0, b"a"), (0.0, b"b"), (0.0, b"c"), (0.0, b"d")]).await;

        let result = zrange_unified(&engine, &[
            b"zs".to_vec(), b"[b".to_vec(), b"[d".to_vec(), b"BYLEX".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zrange_unified_rev() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;

        let result = zrange_unified(&engine, &[
            b"zs".to_vec(), b"0".to_vec(), b"-1".to_vec(), b"REV".to_vec(),
        ]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
            assert_eq!(items[0], RespValue::BulkString(Some(b"c".to_vec())));
            assert_eq!(items[2], RespValue::BulkString(Some(b"a".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zrange_withscores_format() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.5, b"one"), (2.5, b"two"), (3.5, b"three")]).await;

        let result = zrange(&engine, &[b"zs".to_vec(), b"0".to_vec(), b"-1".to_vec(), b"WITHSCORES".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 6);
            assert_eq!(items[0], RespValue::BulkString(Some(b"one".to_vec())));
            assert_eq!(items[1], RespValue::BulkString(Some(b"1.5".to_vec())));
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zadd_multiple_pairs() {
        let engine = setup_engine().await;
        let zadd_args = vec![
            b"multi_zset".to_vec(),
            b"1.0".to_vec(), b"member1".to_vec(),
            b"2.0".to_vec(), b"member2".to_vec(),
            b"3.0".to_vec(), b"member3".to_vec(),
        ];
        let result = zadd(&engine, &zadd_args).await.unwrap();
        assert_eq!(result, RespValue::Integer(3));
        let result = zscore(&engine, &[b"multi_zset".to_vec(), b"member1".to_vec()]).await.unwrap();
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

        // MIN
        zinterstore(&engine, &[
            b"min_out".to_vec(), b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec(),
            b"AGGREGATE".to_vec(), b"MIN".to_vec(),
        ]).await.unwrap();
        let result = zscore(&engine, &[b"min_out".to_vec(), b"a".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"1".to_vec())));
        let result = zscore(&engine, &[b"min_out".to_vec(), b"b".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::BulkString(Some(b"2".to_vec())));

        // MAX
        zinterstore(&engine, &[
            b"max_out".to_vec(), b"2".to_vec(), b"zs1".to_vec(), b"zs2".to_vec(),
            b"AGGREGATE".to_vec(), b"MAX".to_vec(),
        ]).await.unwrap();
        let result = zscore(&engine, &[b"max_out".to_vec(), b"a".to_vec()]).await.unwrap();
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

        let result = zpopmax(&engine, &[b"zs".to_vec(), b"10".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 4); // 2 members * 2
        } else { panic!("Expected array"); }
    }

    #[tokio::test]
    async fn test_zremrangebyrank_negative_indices() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c"), (4.0, b"d")]).await;

        // Remove last two: -2 to -1
        let result = zremrangebyrank(&engine, &[b"zs".to_vec(), b"-2".to_vec(), b"-1".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));

        let card = zcard(&engine, &[b"zs".to_vec()]).await.unwrap();
        assert_eq!(card, RespValue::Integer(2));
    }

    #[tokio::test]
    async fn test_zrangebyscore_exclusive_inf() {
        let engine = setup_engine().await;
        add_members(&engine, b"zs", &[(1.0, b"a"), (2.0, b"b"), (3.0, b"c")]).await;

        let result = zrangebyscore(&engine, &[b"zs".to_vec(), b"-inf".to_vec(), b"+inf".to_vec()]).await.unwrap();
        if let RespValue::Array(Some(items)) = result {
            assert_eq!(items.len(), 3);
        } else { panic!("Expected array"); }
    }
}