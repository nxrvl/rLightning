use crate::command::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use crate::storage::engine::StorageEngine;
use crate::storage::item::RedisDataType;
use crate::storage::value::{ModifyResult, StoreValue};

#[allow(deprecated)]
use std::hash::SipHasher;
use std::hash::{Hash, Hasher};

/// Number of registers in a dense HyperLogLog (2^14 = 16384)
const HLL_P: u32 = 14;
const HLL_REGISTERS: usize = 1 << HLL_P; // 16384
/// Mask for extracting register index
const HLL_P_MASK: u64 = (HLL_REGISTERS as u64) - 1;
/// Number of bits used for the count (64 - 14 = 50)
const HLL_Q: u32 = 64 - HLL_P;

/// Magic header bytes for HyperLogLog (matching Redis format concept)
const HLL_MAGIC: [u8; 4] = [b'H', b'Y', b'L', b'L'];
/// Dense encoding marker
const HLL_DENSE: u8 = 0;
/// Size of a dense HLL: 4 byte magic + 1 byte encoding + 16384 registers
const HLL_DENSE_SIZE: usize = 4 + 1 + HLL_REGISTERS;

/// Create a new empty dense HLL representation
fn hll_new_dense() -> Vec<u8> {
    let mut data = Vec::with_capacity(HLL_DENSE_SIZE);
    data.extend_from_slice(&HLL_MAGIC);
    data.push(HLL_DENSE);
    data.resize(HLL_DENSE_SIZE, 0);
    data
}

/// Check if data is a valid HLL
fn hll_is_valid(data: &[u8]) -> bool {
    data.len() >= 5 && data[0..4] == HLL_MAGIC && data[4] == HLL_DENSE
}

/// Get the register value at the given index
fn hll_get_register(data: &[u8], index: usize) -> u8 {
    let offset = 5 + index; // skip header
    if offset < data.len() { data[offset] } else { 0 }
}

/// Set the register value at the given index, returns true if value changed
fn hll_set_register(data: &mut [u8], index: usize, value: u8) -> bool {
    let offset = 5 + index; // skip header
    if offset < data.len() {
        let old = data[offset];
        if value > old {
            data[offset] = value;
            return true;
        }
    }
    false
}

/// Hash an element and return (register_index, count_of_leading_zeros + 1)
/// Uses SipHasher with fixed keys (0, 0) for deterministic hashing across
/// restarts and replicas, which is required for correct HLL merge and persistence.
#[allow(deprecated)]
fn hll_hash_element(element: &[u8]) -> (usize, u8) {
    let mut hasher = SipHasher::new_with_keys(0, 0);
    element.hash(&mut hasher);
    let hash = hasher.finish();

    // Use lower HLL_P bits as register index
    let index = (hash & HLL_P_MASK) as usize;

    // Use remaining bits to compute the count of leading zeros + 1
    let remaining = hash >> HLL_P;
    // Count leading zeros in the remaining bits (out of HLL_Q bits)
    let count = if remaining == 0 {
        HLL_Q as u8 + 1
    } else {
        // Count leading zeros in 64-bit value, but we only care about the lower HLL_Q bits
        let shifted = remaining << HLL_P; // shift back to get leading zeros of the original remaining bits
        (shifted.leading_zeros() as u8) + 1
    };

    (index, count)
}

/// Estimate cardinality from HLL registers
fn hll_count(data: &[u8]) -> i64 {
    if !hll_is_valid(data) {
        return 0;
    }

    let mut sum: f64 = 0.0;
    let mut zero_count: usize = 0;

    for i in 0..HLL_REGISTERS {
        let val = hll_get_register(data, i);
        sum += 1.0 / (1u64 << val) as f64;
        if val == 0 {
            zero_count += 1;
        }
    }

    // Alpha constant for m = 16384
    let m = HLL_REGISTERS as f64;
    let alpha = 0.7213 / (1.0 + 1.079 / m);
    let mut estimate = alpha * m * m / sum;

    // Small range correction (linear counting)
    if estimate <= 5.0 * m / 2.0 && zero_count > 0 {
        estimate = m * (m / zero_count as f64).ln();
    }

    estimate.round() as i64
}

/// Merge source HLL registers into destination
fn hll_merge(dest: &mut Vec<u8>, src: &[u8]) {
    if !hll_is_valid(src) {
        return;
    }
    // Ensure dest is valid and large enough
    if dest.len() < HLL_DENSE_SIZE {
        dest.resize(HLL_DENSE_SIZE, 0);
    }
    for i in 0..HLL_REGISTERS {
        let src_val = hll_get_register(src, i);
        hll_set_register(dest, i, src_val);
    }
}

/// Check if key holds a non-HLL type (WRONGTYPE error)
async fn check_hll_type(engine: &StorageEngine, key: &[u8]) -> Result<(), CommandError> {
    let key_type = engine.get_raw_data_type(key).await?;
    match key_type.as_str() {
        "none" | "string" => {
            // If it exists as string, verify it's either empty-ish or valid HLL data
            if key_type == "string"
                && let Some(data) = engine.get(key).await?
                && !data.is_empty()
                && !hll_is_valid(&data)
            {
                return Err(CommandError::InvalidArgument(
                    "WRONGTYPE Key is not a valid HyperLogLog string value.".to_string(),
                ));
            }
            Ok(())
        }
        _ => Err(CommandError::WrongType),
    }
}

/// Redis PFADD command - Add elements to a HyperLogLog
/// Returns 1 if at least one internal register was altered, 0 otherwise
pub async fn pfadd(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let key = &args[0];
    let elements = &args[1..];

    // Atomic read-modify-write: type check, HLL register update, TTL preservation
    let changed = engine.atomic_modify(key, RedisDataType::String, |current| {
        let is_new = current.is_none();
        let mut hll_data = match current {
            Some(StoreValue::Str(data)) if hll_is_valid(data) => data.to_vec(),
            Some(StoreValue::Str(data)) if data.is_empty() => hll_new_dense(),
            Some(StoreValue::Str(_)) => {
                return Err(crate::storage::error::StorageError::WrongType);
            }
            None => hll_new_dense(),
            _ => return Err(crate::storage::error::StorageError::WrongType),
        };

        // If no elements provided, just ensure the key exists
        if elements.is_empty() {
            if is_new {
                return Ok((ModifyResult::Set(StoreValue::Str(hll_data.into())), 1i64));
            }
            return Ok((ModifyResult::Set(StoreValue::Str(hll_data.into())), 0i64));
        }

        let mut changed = false;
        for element in elements {
            let (index, count) = hll_hash_element(element);
            if hll_set_register(&mut hll_data, index, count) {
                changed = true;
            }
        }

        Ok((
            ModifyResult::Set(StoreValue::Str(hll_data.into())),
            if changed { 1 } else { 0 },
        ))
    })?;

    Ok(RespValue::Integer(changed))
}

/// Redis PFCOUNT command - Return the approximated cardinality of the set(s)
/// Single key: return cardinality
/// Multiple keys: return cardinality of the union
pub async fn pfcount(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    if args.len() == 1 {
        // Single key - atomic type check + read in one DashMap hold
        let key = &args[0];
        let count = engine.atomic_read(key, RedisDataType::String, |data| match data {
            Some(StoreValue::Str(d)) if hll_is_valid(d) => Ok(hll_count(d)),
            Some(StoreValue::Str(d)) if d.is_empty() => Ok(0),
            Some(StoreValue::Str(_)) => Err(crate::storage::error::StorageError::WrongType),
            None => Ok(0),
            _ => Err(crate::storage::error::StorageError::WrongType),
        })?;
        Ok(RespValue::Integer(count))
    } else {
        // Multiple keys - atomic type check + read per key
        let mut merged = hll_new_dense();

        for key in args {
            let data_opt: Option<Vec<u8>> =
                engine.atomic_read(key, RedisDataType::String, |data| match data {
                    Some(StoreValue::Str(d)) if hll_is_valid(d) => Ok(Some(d.to_vec())),
                    Some(StoreValue::Str(d)) if d.is_empty() => Ok(None),
                    Some(StoreValue::Str(_)) => Err(crate::storage::error::StorageError::WrongType),
                    None => Ok(None),
                    _ => Err(crate::storage::error::StorageError::WrongType),
                })?;
            if let Some(data) = data_opt {
                hll_merge(&mut merged, &data);
            }
        }

        Ok(RespValue::Integer(hll_count(&merged)))
    }
}

/// Redis PFMERGE command - Merge multiple HLLs into a destination key
/// PFMERGE destkey sourcekey [sourcekey ...]
pub async fn pfmerge(engine: &StorageEngine, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let dest_key = &args[0];

    // Lock all involved keys for cross-key atomicity
    let all_keys: Vec<Vec<u8>> = args.to_vec();
    let _lock = engine.lock_keys(&all_keys).await;

    // Check dest key type
    check_hll_type(engine, dest_key).await?;

    // Start with existing dest HLL or create new
    let mut merged = match engine.get(dest_key).await? {
        Some(data) if hll_is_valid(&data) => data,
        Some(data) if data.is_empty() => hll_new_dense(),
        Some(_) => {
            return Err(CommandError::InvalidArgument(
                "WRONGTYPE Key is not a valid HyperLogLog string value.".to_string(),
            ));
        }
        None => hll_new_dense(),
    };

    // Merge all source keys
    for source_key in &args[1..] {
        check_hll_type(engine, source_key).await?;

        if let Some(data) = engine.get(source_key).await? {
            if hll_is_valid(&data) {
                hll_merge(&mut merged, &data);
            } else if !data.is_empty() {
                return Err(CommandError::InvalidArgument(
                    "WRONGTYPE Key is not a valid HyperLogLog string value.".to_string(),
                ));
            }
        }
    }

    // Store merged result in dest key
    let ttl = engine.ttl(dest_key).await?;
    engine
        .set_with_type(dest_key.clone(), StoreValue::Str(merged.into()), ttl)
        .await?;

    Ok(RespValue::SimpleString("OK".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::engine::StorageConfig;
    use std::sync::Arc;

    async fn setup() -> Arc<StorageEngine> {
        let config = StorageConfig::default();
        StorageEngine::new(config)
    }

    #[tokio::test]
    async fn test_pfadd_basic() {
        let engine = setup().await;

        // PFADD with elements
        let result = pfadd(
            &engine,
            &[b"hll".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Adding same elements again should return 0
        let result = pfadd(
            &engine,
            &[b"hll".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_pfadd_no_elements() {
        let engine = setup().await;

        // PFADD with no elements on non-existing key - creates the key
        let result = pfadd(&engine, &[b"hll".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(1));

        // Calling again without elements should return 0 (key already exists)
        let result = pfadd(&engine, &[b"hll".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_pfadd_wrong_args() {
        let engine = setup().await;

        let result = pfadd(&engine, &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pfcount_basic() {
        let engine = setup().await;

        // PFCOUNT on non-existing key returns 0
        let result = pfcount(&engine, &[b"hll".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));

        // Add elements
        pfadd(
            &engine,
            &[b"hll".to_vec(), b"a".to_vec(), b"b".to_vec(), b"c".to_vec()],
        )
        .await
        .unwrap();

        // PFCOUNT should return approximately 3
        let result = pfcount(&engine, &[b"hll".to_vec()]).await.unwrap();
        if let RespValue::Integer(count) = result {
            // HLL is approximate, allow some error margin
            assert!((2..=4).contains(&count), "Expected ~3, got {}", count);
        } else {
            panic!("Expected Integer response");
        }
    }

    #[tokio::test]
    async fn test_pfcount_multiple_keys() {
        let engine = setup().await;

        // Add different elements to two HLLs
        pfadd(&engine, &[b"hll1".to_vec(), b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        pfadd(&engine, &[b"hll2".to_vec(), b"b".to_vec(), b"c".to_vec()])
            .await
            .unwrap();

        // Union count should be approximately 3 (a, b, c)
        let result = pfcount(&engine, &[b"hll1".to_vec(), b"hll2".to_vec()])
            .await
            .unwrap();
        if let RespValue::Integer(count) = result {
            assert!((2..=4).contains(&count), "Expected ~3, got {}", count);
        } else {
            panic!("Expected Integer response");
        }
    }

    #[tokio::test]
    async fn test_pfcount_wrong_args() {
        let engine = setup().await;

        let result = pfcount(&engine, &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pfmerge_basic() {
        let engine = setup().await;

        // Add elements to two HLLs
        pfadd(&engine, &[b"hll1".to_vec(), b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        pfadd(&engine, &[b"hll2".to_vec(), b"c".to_vec(), b"d".to_vec()])
            .await
            .unwrap();

        // Merge into destination
        let result = pfmerge(
            &engine,
            &[b"dest".to_vec(), b"hll1".to_vec(), b"hll2".to_vec()],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Dest should contain union of elements
        let result = pfcount(&engine, &[b"dest".to_vec()]).await.unwrap();
        if let RespValue::Integer(count) = result {
            assert!((3..=5).contains(&count), "Expected ~4, got {}", count);
        } else {
            panic!("Expected Integer response");
        }
    }

    #[tokio::test]
    async fn test_pfmerge_into_existing() {
        let engine = setup().await;

        // Create dest with some elements
        pfadd(&engine, &[b"dest".to_vec(), b"x".to_vec(), b"y".to_vec()])
            .await
            .unwrap();

        pfadd(&engine, &[b"src".to_vec(), b"a".to_vec(), b"b".to_vec()])
            .await
            .unwrap();

        // Merge - dest should retain its existing elements plus src
        let result = pfmerge(&engine, &[b"dest".to_vec(), b"src".to_vec()])
            .await
            .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = pfcount(&engine, &[b"dest".to_vec()]).await.unwrap();
        if let RespValue::Integer(count) = result {
            assert!((3..=5).contains(&count), "Expected ~4, got {}", count);
        } else {
            panic!("Expected Integer response");
        }
    }

    #[tokio::test]
    async fn test_pfmerge_no_sources() {
        let engine = setup().await;

        // PFMERGE with just dest key (no sources) should succeed
        let result = pfmerge(&engine, &[b"dest".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        // Dest should exist with count 0
        let result = pfcount(&engine, &[b"dest".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_pfmerge_wrong_args() {
        let engine = setup().await;

        let result = pfmerge(&engine, &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pfmerge_nonexistent_sources() {
        let engine = setup().await;

        // Merge with nonexistent source keys should succeed (treated as empty)
        let result = pfmerge(
            &engine,
            &[
                b"dest".to_vec(),
                b"nonexistent1".to_vec(),
                b"nonexistent2".to_vec(),
            ],
        )
        .await
        .unwrap();
        assert_eq!(result, RespValue::SimpleString("OK".to_string()));

        let result = pfcount(&engine, &[b"dest".to_vec()]).await.unwrap();
        assert_eq!(result, RespValue::Integer(0));
    }

    #[tokio::test]
    async fn test_hll_cardinality_accuracy() {
        let engine = setup().await;

        // Add 1000 unique elements
        let mut args = vec![b"hll_accuracy".to_vec()];
        for i in 0..1000 {
            args.push(format!("element_{}", i).into_bytes());
        }
        pfadd(&engine, &args).await.unwrap();

        let result = pfcount(&engine, &[b"hll_accuracy".to_vec()]).await.unwrap();
        if let RespValue::Integer(count) = result {
            // HLL should be within ~2% of the true count for 1000 elements
            // Allow generous margin for small register sets
            let error_margin = 0.05; // 5% error margin
            let lower = (1000.0 * (1.0 - error_margin)) as i64;
            let upper = (1000.0 * (1.0 + error_margin)) as i64;
            assert!(
                count >= lower && count <= upper,
                "Expected ~1000, got {} (allowed range {}-{})",
                count,
                lower,
                upper
            );
        } else {
            panic!("Expected Integer response");
        }
    }

    #[tokio::test]
    async fn test_hll_cardinality_larger_set() {
        let engine = setup().await;

        // Add 10000 unique elements in batches
        for batch in 0..100 {
            let mut args = vec![b"hll_large".to_vec()];
            for i in 0..100 {
                let elem = format!("elem_{}_{}", batch, i);
                args.push(elem.into_bytes());
            }
            pfadd(&engine, &args).await.unwrap();
        }

        let result = pfcount(&engine, &[b"hll_large".to_vec()]).await.unwrap();
        if let RespValue::Integer(count) = result {
            // Allow 5% error for 10000 elements
            let lower = 9500;
            let upper = 10500;
            assert!(
                count >= lower && count <= upper,
                "Expected ~10000, got {} (allowed range {}-{})",
                count,
                lower,
                upper
            );
        } else {
            panic!("Expected Integer response");
        }
    }

    #[tokio::test]
    async fn test_wrong_type_error() {
        let engine = setup().await;

        // Create a list key
        engine
            .set_with_type(
                b"mylist".to_vec(),
                crate::storage::value::StoreValue::List(std::collections::VecDeque::from(vec![
                    b"item".to_vec(),
                ])),
                None,
            )
            .await
            .unwrap();

        // PFADD on list should fail with WRONGTYPE
        let result = pfadd(&engine, &[b"mylist".to_vec(), b"a".to_vec()]).await;
        assert!(result.is_err());

        // PFCOUNT on list should fail with WRONGTYPE
        let result = pfcount(&engine, &[b"mylist".to_vec()]).await;
        assert!(result.is_err());

        // PFMERGE with list as source should fail with WRONGTYPE
        let result = pfmerge(&engine, &[b"dest".to_vec(), b"mylist".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_hll_internal_format() {
        // Test the internal HLL functions directly
        let mut hll = hll_new_dense();
        assert!(hll_is_valid(&hll));
        assert_eq!(hll.len(), HLL_DENSE_SIZE);

        // Empty HLL should count 0
        assert_eq!(hll_count(&hll), 0);

        // Test register operations
        assert_eq!(hll_get_register(&hll, 0), 0);
        assert!(hll_set_register(&mut hll, 0, 5));
        assert_eq!(hll_get_register(&hll, 0), 5);

        // Setting a lower value should not change register
        assert!(!hll_set_register(&mut hll, 0, 3));
        assert_eq!(hll_get_register(&hll, 0), 5);

        // Setting a higher value should work
        assert!(hll_set_register(&mut hll, 0, 7));
        assert_eq!(hll_get_register(&hll, 0), 7);
    }

    #[tokio::test]
    async fn test_hll_merge_function() {
        let mut hll1 = hll_new_dense();
        let mut hll2 = hll_new_dense();

        // Set different registers in each
        hll_set_register(&mut hll1, 0, 3);
        hll_set_register(&mut hll1, 1, 5);

        hll_set_register(&mut hll2, 0, 7); // higher than hll1
        hll_set_register(&mut hll2, 2, 4);

        hll_merge(&mut hll1, &hll2);

        // After merge, hll1 should have max of each register
        assert_eq!(hll_get_register(&hll1, 0), 7); // max(3, 7)
        assert_eq!(hll_get_register(&hll1, 1), 5); // max(5, 0)
        assert_eq!(hll_get_register(&hll1, 2), 4); // max(0, 4)
    }

    #[tokio::test]
    async fn test_hll_hash_distribution() {
        // Verify hash function produces reasonable distribution
        let mut registers_used = std::collections::HashSet::new();
        for i in 0..10000 {
            let elem = format!("test_elem_{}", i);
            let (index, count) = hll_hash_element(elem.as_bytes());
            assert!(index < HLL_REGISTERS);
            assert!(count > 0 && count <= (HLL_Q as u8 + 1));
            registers_used.insert(index);
        }
        // With 10000 elements and 16384 registers, we should hit a good portion
        assert!(
            registers_used.len() > 5000,
            "Expected good distribution, got {} unique registers",
            registers_used.len()
        );
    }

    #[tokio::test]
    async fn test_pfadd_preserves_ttl() {
        let engine = setup().await;

        // Set a key with TTL
        let hll_data = hll_new_dense();
        engine
            .set_with_type(
                b"hll_ttl".to_vec(),
                crate::storage::value::StoreValue::Str(hll_data.into()),
                Some(std::time::Duration::from_secs(3600)),
            )
            .await
            .unwrap();

        // PFADD should preserve TTL
        pfadd(&engine, &[b"hll_ttl".to_vec(), b"a".to_vec()])
            .await
            .unwrap();

        let ttl = engine.ttl(b"hll_ttl").await.unwrap();
        assert!(ttl.is_some(), "TTL should be preserved after PFADD");
    }
}
