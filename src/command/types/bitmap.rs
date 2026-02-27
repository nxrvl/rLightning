use crate::command::{CommandError, CommandResult};
use crate::command::utils::bytes_to_string;
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;

/// Check if a key has a non-string type (WRONGTYPE error).
/// Uses get_raw_data_type to avoid false positives from legacy heuristic detection
/// that can misidentify binary bitmap data as other types.
async fn check_string_type(engine: &StorageEngine, key: &[u8]) -> Result<(), CommandError> {
    let key_type = engine.get_raw_data_type(key).await?;
    match key_type.as_str() {
        "none" | "string" => Ok(()),
        _ => Err(CommandError::WrongType),
    }
}

/// Redis SETBIT command - Sets or clears the bit at offset in the string value stored at key
pub async fn setbit(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let offset_str = bytes_to_string(&args[1])?;
    let bit_str = bytes_to_string(&args[2])?;

    let offset: u64 = offset_str.parse().map_err(|_| {
        CommandError::InvalidArgument("bit offset is not an integer or out of range".to_string())
    })?;

    // Redis limits offset to 2^32 - 1
    if offset >= 4_294_967_296 {
        return Err(CommandError::InvalidArgument(
            "ERR bit offset is not an integer or out of range".to_string(),
        ));
    }

    let bit_value: u8 = bit_str.parse().map_err(|_| {
        CommandError::InvalidArgument("bit is not an integer or out of range".to_string())
    })?;

    if bit_value > 1 {
        return Err(CommandError::InvalidArgument(
            "ERR bit is not an integer or out of range".to_string(),
        ));
    }

    check_string_type(engine, key).await?;

    let byte_offset = (offset / 8) as usize;
    let bit_offset = 7 - (offset % 8) as u8; // Redis uses big-endian bit ordering

    let mut bitmap = engine.get(key).await?.unwrap_or_default();

    // Expand bitmap if needed
    if bitmap.len() <= byte_offset {
        bitmap.resize(byte_offset + 1, 0);
    }

    // Get old bit value
    let old_bit = (bitmap[byte_offset] >> bit_offset) & 1;

    // Set new bit value
    if bit_value == 1 {
        bitmap[byte_offset] |= 1 << bit_offset;
    } else {
        bitmap[byte_offset] &= !(1 << bit_offset);
    }

    // Preserve TTL
    let ttl = engine.ttl(key).await?;
    engine.set(key.clone(), bitmap, ttl).await?;

    Ok(RespValue::Integer(old_bit as i64))
}

/// Redis GETBIT command - Returns the bit value at offset in the string value stored at key
pub async fn getbit(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let offset_str = bytes_to_string(&args[1])?;

    let offset: u64 = offset_str.parse().map_err(|_| {
        CommandError::InvalidArgument("bit offset is not an integer or out of range".to_string())
    })?;

    check_string_type(engine, key).await?;

    let byte_offset = (offset / 8) as usize;
    let bit_offset = 7 - (offset % 8) as u8;

    let bitmap = engine.get(key).await?.unwrap_or_default();

    if byte_offset >= bitmap.len() {
        return Ok(RespValue::Integer(0));
    }

    let bit = (bitmap[byte_offset] >> bit_offset) & 1;
    Ok(RespValue::Integer(bit as i64))
}

/// Redis BITCOUNT command - Count the number of set bits in a string
pub async fn bitcount(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() || args.len() > 4 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    check_string_type(engine, key).await?;

    let bitmap = engine.get(key).await?.unwrap_or_default();

    if bitmap.is_empty() {
        return Ok(RespValue::Integer(0));
    }

    // Parse optional range [start end [BYTE|BIT]]
    if args.len() >= 3 {
        let start_str = bytes_to_string(&args[1])?;
        let end_str = bytes_to_string(&args[2])?;

        let start: i64 = start_str.parse().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        let end: i64 = end_str.parse().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;

        // Check for BIT or BYTE mode (Redis 7.0+)
        let bit_mode = if args.len() == 4 {
            let mode = bytes_to_string(&args[3])?.to_uppercase();
            match mode.as_str() {
                "BIT" => true,
                "BYTE" => false,
                _ => {
                    return Err(CommandError::InvalidArgument(
                        "ERR syntax error".to_string(),
                    ))
                }
            }
        } else {
            false // default is BYTE mode
        };

        if bit_mode {
            let bit_len = bitmap.len() as i64 * 8;
            let start = normalize_index(start, bit_len);
            let end = normalize_index(end, bit_len);

            if start > end || start >= bit_len as usize {
                return Ok(RespValue::Integer(0));
            }

            let end = end.min(bit_len as usize - 1);
            let mut count: i64 = 0;
            for bit_pos in start..=end {
                let byte_idx = bit_pos / 8;
                let bit_idx = 7 - (bit_pos % 8);
                if byte_idx < bitmap.len() && (bitmap[byte_idx] >> bit_idx) & 1 == 1 {
                    count += 1;
                }
            }
            Ok(RespValue::Integer(count))
        } else {
            let byte_len = bitmap.len() as i64;
            let start = normalize_index(start, byte_len);
            let end = normalize_index(end, byte_len);

            if start > end || start >= bitmap.len() {
                return Ok(RespValue::Integer(0));
            }

            let end = end.min(bitmap.len() - 1);
            let count: i64 = bitmap[start..=end]
                .iter()
                .map(|b| b.count_ones() as i64)
                .sum();
            Ok(RespValue::Integer(count))
        }
    } else {
        // No range, count all bits
        let count: i64 = bitmap.iter().map(|b| b.count_ones() as i64).sum();
        Ok(RespValue::Integer(count))
    }
}

/// Redis BITPOS command - Find first bit set or clear in a string
pub async fn bitpos(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 2 || args.len() > 5 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let bit_str = bytes_to_string(&args[1])?;
    let target_bit: u8 = bit_str.parse().map_err(|_| {
        CommandError::InvalidArgument("bit is not an integer or out of range".to_string())
    })?;

    if target_bit > 1 {
        return Err(CommandError::InvalidArgument(
            "ERR bit is not an integer or out of range".to_string(),
        ));
    }

    check_string_type(engine, key).await?;

    let bitmap = engine.get(key).await?.unwrap_or_default();

    if bitmap.is_empty() {
        // For empty string: looking for 0 returns 0 (since all bits are conceptually 0)
        // looking for 1 returns -1
        if target_bit == 0 {
            // If explicit range was provided, return 0; otherwise return -1 per Redis behavior
            // Actually Redis returns 0 for BITPOS on empty key looking for 0 with start
            // and -1 for BITPOS on empty key looking for 1
            return Ok(RespValue::Integer(-1));
        } else {
            return Ok(RespValue::Integer(-1));
        }
    }

    // Parse optional start, end, [BYTE|BIT]
    let has_start = args.len() >= 3;
    let _has_end = args.len() >= 4;

    let bit_mode = if args.len() == 5 {
        let mode = bytes_to_string(&args[4])?.to_uppercase();
        match mode.as_str() {
            "BIT" => true,
            "BYTE" => false,
            _ => {
                return Err(CommandError::InvalidArgument(
                    "ERR syntax error".to_string(),
                ))
            }
        }
    } else if args.len() == 4 {
        // Could be start end BYTE/BIT or start end (no mode = BYTE)
        let maybe_mode = bytes_to_string(&args[3])?.to_uppercase();
        match maybe_mode.as_str() {
            "BIT" => true,
            "BYTE" => false,
            _ => false, // it's the end parameter, not a mode
        }
    } else {
        false
    };

    // Re-parse: we need to figure out which args are start, end, and mode
    let (start_val, end_val, explicit_end) = parse_bitpos_range(args, bit_mode)?;

    if bit_mode {
        let bit_len = bitmap.len() as i64 * 8;
        let start = if has_start {
            normalize_index(start_val, bit_len)
        } else {
            0
        };
        let end = if explicit_end {
            normalize_index(end_val, bit_len).min(bit_len as usize - 1)
        } else {
            bit_len as usize - 1
        };

        if start > end || start >= bit_len as usize {
            return Ok(RespValue::Integer(-1));
        }

        for bit_pos in start..=end {
            let byte_idx = bit_pos / 8;
            let bit_idx = 7 - (bit_pos % 8);
            let bit = if byte_idx < bitmap.len() {
                (bitmap[byte_idx] >> bit_idx) & 1
            } else {
                0
            };
            if bit == target_bit {
                return Ok(RespValue::Integer(bit_pos as i64));
            }
        }
        Ok(RespValue::Integer(-1))
    } else {
        let byte_len = bitmap.len() as i64;
        let start_byte = if has_start {
            normalize_index(start_val, byte_len)
        } else {
            0
        };
        let end_byte = if explicit_end {
            normalize_index(end_val, byte_len).min(bitmap.len() - 1)
        } else {
            bitmap.len() - 1
        };

        if start_byte > end_byte || start_byte >= bitmap.len() {
            return Ok(RespValue::Integer(-1));
        }

        for (i, &byte_val) in bitmap[start_byte..=end_byte].iter().enumerate() {
            let byte_idx = start_byte + i;
            for bit_idx in (0..8).rev() {
                let bit = (byte_val >> bit_idx) & 1;
                if bit == target_bit {
                    let pos = byte_idx * 8 + (7 - bit_idx as usize);
                    return Ok(RespValue::Integer(pos as i64));
                }
            }
        }

        // If looking for 0 and no end was specified, the bit after the string is conceptually 0
        if target_bit == 0 && !explicit_end {
            return Ok(RespValue::Integer((bitmap.len() * 8) as i64));
        }

        Ok(RespValue::Integer(-1))
    }
}

/// Parse the start/end/mode arguments for BITPOS
fn parse_bitpos_range(args: &[Vec<u8>], bit_mode: bool) -> Result<(i64, i64, bool), CommandError> {
    let start = if args.len() >= 3 {
        bytes_to_string(&args[2])?.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?
    } else {
        0
    };

    let explicit_end;
    let end = if args.len() >= 4 {
        let val_str = bytes_to_string(&args[3])?.to_uppercase();
        if val_str == "BIT" || val_str == "BYTE" {
            explicit_end = false;
            -1
        } else {
            explicit_end = true;
            val_str.parse::<i64>().map_err(|_| {
                CommandError::InvalidArgument(
                    "ERR value is not an integer or out of range".to_string(),
                )
            })?
        }
    } else {
        explicit_end = false;
        -1
    };

    // If there's a 5th arg, the 4th was definitely end and 5th is mode
    let (end, explicit_end) = if args.len() == 5 {
        let end_str = bytes_to_string(&args[3])?;
        let end_val = end_str.parse::<i64>().map_err(|_| {
            CommandError::InvalidArgument("value is not an integer or out of range".to_string())
        })?;
        (end_val, true)
    } else {
        (end, explicit_end)
    };

    let _ = bit_mode; // used by caller
    Ok((start, end, explicit_end))
}

/// Redis BITOP command - Perform bitwise operations between strings
pub async fn bitop(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let operation = bytes_to_string(&args[0])?.to_uppercase();
    let dest_key = &args[1];
    let source_keys = &args[2..];

    if operation == "NOT" && source_keys.len() != 1 {
        return Err(CommandError::InvalidArgument(
            "ERR BITOP NOT requires one and only one key".to_string(),
        ));
    }

    // Validate all source keys are string type
    for k in source_keys {
        check_string_type(engine, k).await?;
    }

    // Read all source bitmaps
    let mut bitmaps: Vec<Vec<u8>> = Vec::new();
    let mut max_len = 0;
    for k in source_keys {
        let bm = engine.get(k).await?.unwrap_or_default();
        if bm.len() > max_len {
            max_len = bm.len();
        }
        bitmaps.push(bm);
    }

    if max_len == 0 {
        // All source keys are empty, store empty and return 0
        engine.set(dest_key.clone(), vec![], None).await?;
        return Ok(RespValue::Integer(0));
    }

    // Pad all bitmaps to max_len
    for bm in &mut bitmaps {
        bm.resize(max_len, 0);
    }

    let result = match operation.as_str() {
        "AND" => {
            let mut res = bitmaps[0].clone();
            for bm in &bitmaps[1..] {
                for (i, b) in bm.iter().enumerate() {
                    res[i] &= b;
                }
            }
            res
        }
        "OR" => {
            let mut res = bitmaps[0].clone();
            for bm in &bitmaps[1..] {
                for (i, b) in bm.iter().enumerate() {
                    res[i] |= b;
                }
            }
            res
        }
        "XOR" => {
            let mut res = bitmaps[0].clone();
            for bm in &bitmaps[1..] {
                for (i, b) in bm.iter().enumerate() {
                    res[i] ^= b;
                }
            }
            res
        }
        "NOT" => bitmaps[0].iter().map(|b| !b).collect(),
        _ => {
            return Err(CommandError::InvalidArgument(
                "ERR syntax error".to_string(),
            ))
        }
    };

    let result_len = result.len() as i64;
    engine.set(dest_key.clone(), result, None).await?;

    Ok(RespValue::Integer(result_len))
}

/// Redis BITFIELD command - Perform arbitrary bitfield integer operations on strings
pub async fn bitfield(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    check_string_type(engine, key).await?;

    let ops = parse_bitfield_ops(&args[1..], false)?;

    if ops.is_empty() {
        return Ok(RespValue::Array(Some(vec![])));
    }

    let mut bitmap = engine.get(key).await?.unwrap_or_default();
    let mut results: Vec<RespValue> = Vec::new();
    let mut modified = false;

    for op in &ops {
        match op {
            BitfieldOp::Get { encoding, offset } => {
                let val = bitfield_get(&bitmap, encoding, *offset);
                results.push(RespValue::Integer(val));
            }
            BitfieldOp::Set {
                encoding,
                offset,
                value,
            } => {
                let old_val = bitfield_get(&bitmap, encoding, *offset);
                ensure_bitmap_size(&mut bitmap, *offset + encoding.bits as u64);
                bitfield_set(&mut bitmap, encoding, *offset, *value);
                results.push(RespValue::Integer(old_val));
                modified = true;
            }
            BitfieldOp::IncrBy {
                encoding,
                offset,
                increment,
                overflow,
            } => {
                let old_val = bitfield_get(&bitmap, encoding, *offset);
                let result = bitfield_incrby(old_val, *increment, encoding, *overflow);
                match result {
                    Some(new_val) => {
                        ensure_bitmap_size(&mut bitmap, *offset + encoding.bits as u64);
                        bitfield_set(&mut bitmap, encoding, *offset, new_val);
                        results.push(RespValue::Integer(new_val));
                        modified = true;
                    }
                    None => {
                        // OVERFLOW FAIL - don't change, push nil
                        results.push(RespValue::BulkString(None));
                    }
                }
            }
        }
    }

    if modified {
        let ttl = engine.ttl(key).await?;
        engine.set(key.clone(), bitmap, ttl).await?;
    }

    Ok(RespValue::Array(Some(results)))
}

/// Redis BITFIELD_RO command - Read-only variant of BITFIELD
pub async fn bitfield_ro(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    check_string_type(engine, key).await?;

    let ops = parse_bitfield_ops(&args[1..], true)?;

    let bitmap = engine.get(key).await?.unwrap_or_default();
    let mut results: Vec<RespValue> = Vec::new();

    for op in &ops {
        match op {
            BitfieldOp::Get { encoding, offset } => {
                let val = bitfield_get(&bitmap, encoding, *offset);
                results.push(RespValue::Integer(val));
            }
            _ => {
                return Err(CommandError::InvalidArgument(
                    "ERR BITFIELD_RO only supports the GET subcommand".to_string(),
                ));
            }
        }
    }

    Ok(RespValue::Array(Some(results)))
}

// --- BITFIELD internals ---

#[derive(Debug, Clone, Copy, PartialEq)]
enum OverflowBehavior {
    Wrap,
    Sat,
    Fail,
}

#[derive(Debug, Clone, Copy)]
struct BitfieldEncoding {
    signed: bool,
    bits: u8,
}

#[derive(Debug)]
enum BitfieldOp {
    Get {
        encoding: BitfieldEncoding,
        offset: u64,
    },
    Set {
        encoding: BitfieldEncoding,
        offset: u64,
        value: i64,
    },
    IncrBy {
        encoding: BitfieldEncoding,
        offset: u64,
        increment: i64,
        overflow: OverflowBehavior,
    },
}

fn parse_encoding(s: &str) -> Result<BitfieldEncoding, CommandError> {
    if s.is_empty() {
        return Err(CommandError::InvalidArgument(
            "ERR Invalid bitfield type. Use something like i8 u8 u16 i32".to_string(),
        ));
    }

    let signed = match s.chars().next().unwrap() {
        'i' => true,
        'u' => false,
        _ => {
            return Err(CommandError::InvalidArgument(
                "ERR Invalid bitfield type. Use something like i8 u8 u16 i32".to_string(),
            ))
        }
    };

    let bits: u8 = s[1..].parse().map_err(|_| {
        CommandError::InvalidArgument(
            "ERR Invalid bitfield type. Use something like i8 u8 u16 i32".to_string(),
        )
    })?;

    if bits == 0 || (signed && bits > 64) || (!signed && bits > 63) {
        return Err(CommandError::InvalidArgument(
            "ERR Invalid bitfield type. Use something like i8 u8 u16 i32".to_string(),
        ));
    }

    Ok(BitfieldEncoding { signed, bits })
}

fn parse_offset(s: &str, encoding: &BitfieldEncoding) -> Result<u64, CommandError> {
    if let Some(rest) = s.strip_prefix('#') {
        // Multiplied offset: #N means N * encoding.bits
        let n: u64 = rest.parse().map_err(|_| {
            CommandError::InvalidArgument(
                "ERR bit offset is not an integer or out of range".to_string(),
            )
        })?;
        Ok(n * encoding.bits as u64)
    } else {
        s.parse().map_err(|_| {
            CommandError::InvalidArgument(
                "ERR bit offset is not an integer or out of range".to_string(),
            )
        })
    }
}

fn parse_bitfield_ops(
    args: &[Vec<u8>],
    read_only: bool,
) -> Result<Vec<BitfieldOp>, CommandError> {
    let mut ops = Vec::new();
    let mut i = 0;
    let mut current_overflow = OverflowBehavior::Wrap;

    while i < args.len() {
        let subcmd = bytes_to_string(&args[i])?.to_uppercase();
        match subcmd.as_str() {
            "GET" => {
                if i + 2 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let encoding = parse_encoding(&bytes_to_string(&args[i + 1])?)?;
                let offset = parse_offset(&bytes_to_string(&args[i + 2])?, &encoding)?;
                ops.push(BitfieldOp::Get { encoding, offset });
                i += 3;
            }
            "SET" => {
                if read_only {
                    return Err(CommandError::InvalidArgument(
                        "ERR BITFIELD_RO only supports the GET subcommand".to_string(),
                    ));
                }
                if i + 3 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let encoding = parse_encoding(&bytes_to_string(&args[i + 1])?)?;
                let offset = parse_offset(&bytes_to_string(&args[i + 2])?, &encoding)?;
                let value: i64 = bytes_to_string(&args[i + 3])?.parse().map_err(|_| {
                    CommandError::InvalidArgument(
                        "ERR value is not an integer or out of range".to_string(),
                    )
                })?;
                ops.push(BitfieldOp::Set {
                    encoding,
                    offset,
                    value,
                });
                i += 4;
            }
            "INCRBY" => {
                if read_only {
                    return Err(CommandError::InvalidArgument(
                        "ERR BITFIELD_RO only supports the GET subcommand".to_string(),
                    ));
                }
                if i + 3 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let encoding = parse_encoding(&bytes_to_string(&args[i + 1])?)?;
                let offset = parse_offset(&bytes_to_string(&args[i + 2])?, &encoding)?;
                let increment: i64 =
                    bytes_to_string(&args[i + 3])?.parse().map_err(|_| {
                        CommandError::InvalidArgument(
                            "ERR value is not an integer or out of range".to_string(),
                        )
                    })?;
                ops.push(BitfieldOp::IncrBy {
                    encoding,
                    offset,
                    increment,
                    overflow: current_overflow,
                });
                i += 4;
            }
            "OVERFLOW" => {
                if i + 1 >= args.len() {
                    return Err(CommandError::WrongNumberOfArguments);
                }
                let mode = bytes_to_string(&args[i + 1])?.to_uppercase();
                current_overflow = match mode.as_str() {
                    "WRAP" => OverflowBehavior::Wrap,
                    "SAT" => OverflowBehavior::Sat,
                    "FAIL" => OverflowBehavior::Fail,
                    _ => {
                        return Err(CommandError::InvalidArgument(
                            "ERR Invalid OVERFLOW type (should be one of WRAP, SAT, FAIL)"
                                .to_string(),
                        ))
                    }
                };
                i += 2;
            }
            _ => {
                return Err(CommandError::InvalidArgument(format!(
                    "ERR Unknown BITFIELD subcommand '{}'",
                    subcmd
                )));
            }
        }
    }

    Ok(ops)
}

fn ensure_bitmap_size(bitmap: &mut Vec<u8>, bit_count: u64) {
    let needed_bytes = bit_count.div_ceil(8) as usize;
    if bitmap.len() < needed_bytes {
        bitmap.resize(needed_bytes, 0);
    }
}

/// Read an integer from the bitmap at a given bit offset with the specified encoding
fn bitfield_get(bitmap: &[u8], encoding: &BitfieldEncoding, offset: u64) -> i64 {
    let bits = encoding.bits as u64;
    let mut value: u64 = 0;

    for i in 0..bits {
        let bit_pos = offset + i;
        let byte_idx = (bit_pos / 8) as usize;
        let bit_idx = 7 - (bit_pos % 8);

        if byte_idx < bitmap.len() {
            let bit = ((bitmap[byte_idx] as u64) >> bit_idx) & 1;
            value = (value << 1) | bit;
        } else {
            value <<= 1; // bit is 0 for out-of-range
        }
    }

    if encoding.signed && bits > 0 {
        // Sign-extend: if the highest bit is set, this is negative
        let sign_bit = 1u64 << (bits - 1);
        if value & sign_bit != 0 {
            // Set all bits above 'bits' to 1 for sign extension
            let mask = !((1u64 << bits) - 1);
            value |= mask;
        }
    }

    value as i64
}

/// Write an integer to the bitmap at a given bit offset with the specified encoding
fn bitfield_set(bitmap: &mut [u8], encoding: &BitfieldEncoding, offset: u64, value: i64) {
    let bits = encoding.bits as u64;
    let value = value as u64;

    for i in 0..bits {
        let bit_pos = offset + i;
        let byte_idx = (bit_pos / 8) as usize;
        let bit_idx = 7 - (bit_pos % 8);

        if byte_idx < bitmap.len() {
            let bit = (value >> (bits - 1 - i)) & 1;
            if bit == 1 {
                bitmap[byte_idx] |= 1 << bit_idx;
            } else {
                bitmap[byte_idx] &= !(1 << bit_idx);
            }
        }
    }
}

/// Perform INCRBY with overflow handling, returns None if OVERFLOW FAIL and would overflow
fn bitfield_incrby(
    current: i64,
    increment: i64,
    encoding: &BitfieldEncoding,
    overflow: OverflowBehavior,
) -> Option<i64> {
    let bits = encoding.bits;

    if encoding.signed {
        let min = if bits == 64 {
            i64::MIN
        } else {
            -(1i64 << (bits - 1))
        };
        let max = if bits == 64 {
            i64::MAX
        } else {
            (1i64 << (bits - 1)) - 1
        };

        match overflow {
            OverflowBehavior::Wrap => {
                let result = current.wrapping_add(increment);
                // Truncate to bit width and sign-extend
                let mask = if bits == 64 {
                    u64::MAX
                } else {
                    (1u64 << bits) - 1
                };
                let truncated = (result as u64) & mask;
                let sign_bit = 1u64 << (bits - 1);
                let val = if truncated & sign_bit != 0 && bits < 64 {
                    (truncated | !mask) as i64
                } else {
                    truncated as i64
                };
                Some(val)
            }
            OverflowBehavior::Sat => {
                let result = current as i128 + increment as i128;
                if result > max as i128 {
                    Some(max)
                } else if result < min as i128 {
                    Some(min)
                } else {
                    Some(result as i64)
                }
            }
            OverflowBehavior::Fail => {
                let result = current as i128 + increment as i128;
                if result > max as i128 || result < min as i128 {
                    None
                } else {
                    Some(result as i64)
                }
            }
        }
    } else {
        let max = if bits >= 64 {
            u64::MAX
        } else {
            (1u64 << bits) - 1
        };

        let current_u = (current as u64) & max;

        match overflow {
            OverflowBehavior::Wrap => {
                let result = if increment >= 0 {
                    current_u.wrapping_add(increment as u64)
                } else {
                    current_u.wrapping_sub((-increment) as u64)
                };
                Some((result & max) as i64)
            }
            OverflowBehavior::Sat => {
                let result = current_u as i128 + increment as i128;
                if result > max as i128 {
                    Some(max as i64)
                } else if result < 0 {
                    Some(0)
                } else {
                    Some(result as i64)
                }
            }
            OverflowBehavior::Fail => {
                let result = current_u as i128 + increment as i128;
                if result > max as i128 || result < 0 {
                    None
                } else {
                    Some(result as i64)
                }
            }
        }
    }
}

/// Normalize a negative index to a positive one (Redis-style)
fn normalize_index(index: i64, len: i64) -> usize {
    if index < 0 {
        let normalized = len + index;
        if normalized < 0 {
            0
        } else {
            normalized as usize
        }
    } else {
        index as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use crate::storage::engine::StorageConfig;
    use crate::command::types::list::lpush;

    fn make_engine() -> Arc<StorageEngine> {
        StorageEngine::new(StorageConfig::default())
    }

    fn args(strs: &[&str]) -> Vec<Vec<u8>> {
        strs.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    // --- SETBIT / GETBIT tests ---

    #[tokio::test]
    async fn test_setbit_basic() {
        let engine = make_engine();
        // Set bit 7 (last bit of first byte) to 1
        let result = setbit(&engine, &args(&["mykey", "7", "1"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0)); // old value was 0

        // Get bit 7 back
        let result = getbit(&engine, &args(&["mykey", "7"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Set bit 7 to 0
        let result = setbit(&engine, &args(&["mykey", "7", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(1)); // old value was 1

        // Verify
        let result = getbit(&engine, &args(&["mykey", "7"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_setbit_expands_bitmap() {
        let engine = make_engine();
        // Set bit at offset 100 (requires 13 bytes)
        let result = setbit(&engine, &args(&["mykey", "100", "1"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        let result = getbit(&engine, &args(&["mykey", "100"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Bits before 100 should be 0
        let result = getbit(&engine, &args(&["mykey", "99"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_getbit_nonexistent_key() {
        let engine = make_engine();
        let result = getbit(&engine, &args(&["nosuchkey", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_getbit_out_of_range() {
        let engine = make_engine();
        engine.set(b"mykey".to_vec(), vec![0xFF], None).await.unwrap();
        // Bit 8 is beyond the 1-byte bitmap
        let result = getbit(&engine, &args(&["mykey", "8"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_setbit_invalid_bit() {
        let engine = make_engine();
        let result = setbit(&engine, &args(&["mykey", "0", "2"])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_setbit_wrong_type() {
        let engine = make_engine();
        // Create a list key via lpush command
        lpush(&engine, &args(&["listkey", "val"])).await.unwrap();
        let result = setbit(&engine, &args(&["listkey", "0", "1"])).await;
        assert!(matches!(result, Err(CommandError::WrongType)));
    }

    #[tokio::test]
    async fn test_setbit_preserves_ttl() {
        let engine = make_engine();
        use std::time::Duration;
        engine.set(b"mykey".to_vec(), vec![0x00], Some(Duration::from_secs(300))).await.unwrap();
        setbit(&engine, &args(&["mykey", "0", "1"])).await.unwrap();
        let ttl = engine.ttl(b"mykey").await.unwrap();
        assert!(ttl.is_some());
    }

    // --- BITCOUNT tests ---

    #[tokio::test]
    async fn test_bitcount_full() {
        let engine = make_engine();
        // "foobar" = 0x66 0x6f 0x6f 0x62 0x61 0x72
        engine.set(b"mykey".to_vec(), b"foobar".to_vec(), None).await.unwrap();
        let result = bitcount(&engine, &args(&["mykey"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(26));
    }

    #[tokio::test]
    async fn test_bitcount_byte_range() {
        let engine = make_engine();
        engine.set(b"mykey".to_vec(), b"foobar".to_vec(), None).await.unwrap();
        // Count bits in bytes 0 and 1
        let result = bitcount(&engine, &args(&["mykey", "0", "0"])).await.unwrap();
        // 'f' = 0x66 = 01100110 = 4 bits
        assert_eq!(result, RespValue::Integer(4));

        let result = bitcount(&engine, &args(&["mykey", "1", "1"])).await.unwrap();
        // 'o' = 0x6f = 01101111 = 6 bits
        assert_eq!(result, RespValue::Integer(6));
    }

    #[tokio::test]
    async fn test_bitcount_negative_indices() {
        let engine = make_engine();
        engine.set(b"mykey".to_vec(), b"foobar".to_vec(), None).await.unwrap();
        // Last byte: 'r' = 0x72 = 01110010 = 4 bits
        let result = bitcount(&engine, &args(&["mykey", "-1", "-1"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(4));
    }

    #[tokio::test]
    async fn test_bitcount_bit_mode() {
        let engine = make_engine();
        // 0xFF = 11111111
        engine.set(b"mykey".to_vec(), vec![0xFF, 0x00], None).await.unwrap();
        // Count bits 0-7 (first byte)
        let result = bitcount(&engine, &args(&["mykey", "0", "7", "BIT"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(8));
        // Count bits 8-15 (second byte, all zeros)
        let result = bitcount(&engine, &args(&["mykey", "8", "15", "BIT"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_bitcount_empty_key() {
        let engine = make_engine();
        let result = bitcount(&engine, &args(&["nosuchkey"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // --- BITPOS tests ---

    #[tokio::test]
    async fn test_bitpos_find_first_set() {
        let engine = make_engine();
        // 0x00 0xFF = first set bit at position 8
        engine.set(b"mykey".to_vec(), vec![0x00, 0xFF], None).await.unwrap();
        let result = bitpos(&engine, &args(&["mykey", "1"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(8));
    }

    #[tokio::test]
    async fn test_bitpos_find_first_clear() {
        let engine = make_engine();
        // 0xFF = all set, first clear bit is at position 8 (beyond string)
        engine.set(b"mykey".to_vec(), vec![0xFF], None).await.unwrap();
        let result = bitpos(&engine, &args(&["mykey", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(8));
    }

    #[tokio::test]
    async fn test_bitpos_with_range() {
        let engine = make_engine();
        // 0xFF 0x00 0xFF
        engine.set(b"mykey".to_vec(), vec![0xFF, 0x00, 0xFF], None).await.unwrap();
        // Find first 0 starting from byte 0
        let result = bitpos(&engine, &args(&["mykey", "0", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(8)); // first 0 is in byte 1
    }

    #[tokio::test]
    async fn test_bitpos_no_match() {
        let engine = make_engine();
        // All zeros, looking for 1
        engine.set(b"mykey".to_vec(), vec![0x00, 0x00], None).await.unwrap();
        let result = bitpos(&engine, &args(&["mykey", "1"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    #[tokio::test]
    async fn test_bitpos_empty_key() {
        let engine = make_engine();
        let result = bitpos(&engine, &args(&["nosuchkey", "1"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(-1));
    }

    // --- BITOP tests ---

    #[tokio::test]
    async fn test_bitop_and() {
        let engine = make_engine();
        engine.set(b"key1".to_vec(), vec![0xFF, 0x0F], None).await.unwrap();
        engine.set(b"key2".to_vec(), vec![0x0F, 0xFF], None).await.unwrap();
        let result = bitop(&engine, &args(&["AND", "dest", "key1", "key2"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let val = engine.get(b"dest").await.unwrap().unwrap();
        assert_eq!(val, vec![0x0F, 0x0F]);
    }

    #[tokio::test]
    async fn test_bitop_or() {
        let engine = make_engine();
        engine.set(b"key1".to_vec(), vec![0xF0, 0x00], None).await.unwrap();
        engine.set(b"key2".to_vec(), vec![0x0F, 0xFF], None).await.unwrap();
        let result = bitop(&engine, &args(&["OR", "dest", "key1", "key2"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let val = engine.get(b"dest").await.unwrap().unwrap();
        assert_eq!(val, vec![0xFF, 0xFF]);
    }

    #[tokio::test]
    async fn test_bitop_xor() {
        let engine = make_engine();
        engine.set(b"key1".to_vec(), vec![0xFF, 0x00], None).await.unwrap();
        engine.set(b"key2".to_vec(), vec![0xFF, 0xFF], None).await.unwrap();
        let result = bitop(&engine, &args(&["XOR", "dest", "key1", "key2"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let val = engine.get(b"dest").await.unwrap().unwrap();
        assert_eq!(val, vec![0x00, 0xFF]);
    }

    #[tokio::test]
    async fn test_bitop_not() {
        let engine = make_engine();
        engine.set(b"key1".to_vec(), vec![0x0F], None).await.unwrap();
        let result = bitop(&engine, &args(&["NOT", "dest", "key1"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));
        let val = engine.get(b"dest").await.unwrap().unwrap();
        assert_eq!(val, vec![0xF0]);
    }

    #[tokio::test]
    async fn test_bitop_not_requires_single_key() {
        let engine = make_engine();
        engine.set(b"key1".to_vec(), vec![0x0F], None).await.unwrap();
        engine.set(b"key2".to_vec(), vec![0xF0], None).await.unwrap();
        let result = bitop(&engine, &args(&["NOT", "dest", "key1", "key2"])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bitop_different_lengths() {
        let engine = make_engine();
        engine.set(b"key1".to_vec(), vec![0xFF], None).await.unwrap();
        engine.set(b"key2".to_vec(), vec![0xFF, 0xFF], None).await.unwrap();
        let result = bitop(&engine, &args(&["AND", "dest", "key1", "key2"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(2));
        let val = engine.get(b"dest").await.unwrap().unwrap();
        // key1 padded with 0x00, so AND gives [0xFF, 0x00]
        assert_eq!(val, vec![0xFF, 0x00]);
    }

    #[tokio::test]
    async fn test_bitop_empty_keys() {
        let engine = make_engine();
        let result = bitop(&engine, &args(&["AND", "dest", "nosuch1", "nosuch2"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    // --- BITFIELD tests ---

    #[tokio::test]
    async fn test_bitfield_get_set() {
        let engine = make_engine();
        // SET an unsigned 8-bit value at offset 0
        let result = bitfield(&engine, &args(&["mykey", "SET", "u8", "0", "200"])).await.unwrap();
        // Old value should be 0
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(0)])));

        // GET it back
        let result = bitfield(&engine, &args(&["mykey", "GET", "u8", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(200)])));
    }

    #[tokio::test]
    async fn test_bitfield_signed() {
        let engine = make_engine();
        // SET a signed 8-bit value to -1 at offset 0
        let result = bitfield(&engine, &args(&["mykey", "SET", "i8", "0", "-1"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(0)])));

        // GET it back
        let result = bitfield(&engine, &args(&["mykey", "GET", "i8", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(-1)])));
    }

    #[tokio::test]
    async fn test_bitfield_incrby_wrap() {
        let engine = make_engine();
        // Set u8 at offset 0 to 200
        bitfield(&engine, &args(&["mykey", "SET", "u8", "0", "200"])).await.unwrap();

        // Increment by 100 with WRAP (default) -> 200 + 100 = 300 -> wraps to 300 % 256 = 44
        let result = bitfield(&engine, &args(&["mykey", "INCRBY", "u8", "0", "100"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(44)])));
    }

    #[tokio::test]
    async fn test_bitfield_incrby_sat() {
        let engine = make_engine();
        bitfield(&engine, &args(&["mykey", "SET", "u8", "0", "200"])).await.unwrap();

        // Increment with SAT -> 200 + 100 = 300, clamped to 255
        let result = bitfield(&engine, &args(&["mykey", "OVERFLOW", "SAT", "INCRBY", "u8", "0", "100"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(255)])));
    }

    #[tokio::test]
    async fn test_bitfield_incrby_fail() {
        let engine = make_engine();
        bitfield(&engine, &args(&["mykey", "SET", "u8", "0", "200"])).await.unwrap();

        // Increment with FAIL -> returns nil, value unchanged
        let result = bitfield(&engine, &args(&["mykey", "OVERFLOW", "FAIL", "INCRBY", "u8", "0", "100"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::BulkString(None)])));

        // Value should be unchanged
        let result = bitfield(&engine, &args(&["mykey", "GET", "u8", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(200)])));
    }

    #[tokio::test]
    async fn test_bitfield_multiple_ops() {
        let engine = make_engine();
        let result = bitfield(&engine, &args(&[
            "mykey",
            "SET", "u8", "0", "100",
            "SET", "u8", "8", "200",
            "GET", "u8", "0",
            "GET", "u8", "8",
        ])).await.unwrap();

        assert_eq!(result, RespValue::Array(Some(vec![
            RespValue::Integer(0),   // old val of first SET
            RespValue::Integer(0),   // old val of second SET
            RespValue::Integer(100), // GET #1
            RespValue::Integer(200), // GET #2
        ])));
    }

    #[tokio::test]
    async fn test_bitfield_hash_offset() {
        let engine = make_engine();
        // #1 with u8 means offset = 1 * 8 = 8
        let result = bitfield(&engine, &args(&["mykey", "SET", "u8", "#1", "42"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(0)])));

        // Read back at bit offset 8
        let result = bitfield(&engine, &args(&["mykey", "GET", "u8", "8"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(42)])));
    }

    // --- BITFIELD_RO tests ---

    #[tokio::test]
    async fn test_bitfield_ro_get() {
        let engine = make_engine();
        engine.set(b"mykey".to_vec(), vec![0xAB], None).await.unwrap();
        let result = bitfield_ro(&engine, &args(&["mykey", "GET", "u8", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(0xAB)])));
    }

    #[tokio::test]
    async fn test_bitfield_ro_rejects_set() {
        let engine = make_engine();
        let result = bitfield_ro(&engine, &args(&["mykey", "SET", "u8", "0", "1"])).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_bitfield_ro_rejects_incrby() {
        let engine = make_engine();
        let result = bitfield_ro(&engine, &args(&["mykey", "INCRBY", "u8", "0", "1"])).await;
        assert!(result.is_err());
    }

    // --- Edge case tests ---

    #[tokio::test]
    async fn test_setbit_first_bit() {
        let engine = make_engine();
        // Bit 0 is the highest bit of byte 0
        setbit(&engine, &args(&["mykey", "0", "1"])).await.unwrap();
        let val = engine.get(b"mykey").await.unwrap().unwrap();
        assert_eq!(val, vec![0x80]); // 10000000
    }

    #[tokio::test]
    async fn test_bitcount_all_ones() {
        let engine = make_engine();
        engine.set(b"mykey".to_vec(), vec![0xFF, 0xFF], None).await.unwrap();
        let result = bitcount(&engine, &args(&["mykey"])).await.unwrap();
        assert_eq!(result, RespValue::Integer(16));
    }

    #[tokio::test]
    async fn test_bitfield_u16() {
        let engine = make_engine();
        bitfield(&engine, &args(&["mykey", "SET", "u16", "0", "1000"])).await.unwrap();
        let result = bitfield(&engine, &args(&["mykey", "GET", "u16", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(1000)])));
    }

    #[tokio::test]
    async fn test_bitfield_i16_negative() {
        let engine = make_engine();
        bitfield(&engine, &args(&["mykey", "SET", "i16", "0", "-500"])).await.unwrap();
        let result = bitfield(&engine, &args(&["mykey", "GET", "i16", "0"])).await.unwrap();
        assert_eq!(result, RespValue::Array(Some(vec![RespValue::Integer(-500)])));
    }
}
