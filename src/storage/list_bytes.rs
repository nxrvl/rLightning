//! Byte-level operations on bincode-serialized list data.
//!
//! Lists are stored as `Vec<u8>` in `StorageItem.value` using the bincode 1.3
//! default wire format for `Vec<Vec<u8>>`:
//!
//! ```text
//! [u64 LE: element count]
//! [u64 LE: elem0 byte length] [elem0 bytes...]
//! [u64 LE: elem1 byte length] [elem1 bytes...]
//! ...
//! ```
//!
//! This module manipulates that byte layout directly, avoiding full
//! deserialization/serialization on every operation.

use super::error::StorageError;

type Result<T> = std::result::Result<T, StorageError>;

const HEADER_SIZE: usize = 8; // u64 LE for counts/lengths

// ──────────────────────────── Internal helpers ────────────────────────────

#[inline]
fn read_u64(data: &[u8], offset: usize) -> Result<u64> {
    if offset + HEADER_SIZE > data.len() {
        return Err(StorageError::InternalError(
            "list data corrupted: unexpected end of data".into(),
        ));
    }
    Ok(u64::from_le_bytes(
        data[offset..offset + HEADER_SIZE].try_into().unwrap(),
    ))
}

#[inline]
fn write_u64(data: &mut [u8], offset: usize, val: u64) {
    data[offset..offset + HEADER_SIZE].copy_from_slice(&val.to_le_bytes());
}

/// Returns (element_bytes_slice, next_offset) for the element at `offset`.
#[inline]
fn read_element_at(data: &[u8], offset: usize) -> Result<(&[u8], usize)> {
    let elem_len = read_u64(data, offset)? as usize;
    let start = offset + HEADER_SIZE;
    let end = start + elem_len;
    if end > data.len() {
        return Err(StorageError::InternalError(
            "list data corrupted: element extends past end".into(),
        ));
    }
    Ok((&data[start..end], end))
}

/// Scan forward to find the byte offset of the `n`-th element (0-based).
/// Returns the offset where that element's length header starts.
fn offset_of_index(data: &[u8], n: u64) -> Result<usize> {
    let mut off = HEADER_SIZE; // skip count header
    for _ in 0..n {
        let elem_len = read_u64(data, off)? as usize;
        off = off + HEADER_SIZE + elem_len;
        if off > data.len() {
            return Err(StorageError::InternalError(
                "list data corrupted: offset past end while scanning".into(),
            ));
        }
    }
    Ok(off)
}

/// Serialize a single element in bincode wire format: [u64 LE len] [bytes].
#[inline]
fn element_wire(elem: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(HEADER_SIZE + elem.len());
    buf.extend_from_slice(&(elem.len() as u64).to_le_bytes());
    buf.extend_from_slice(elem);
    buf
}

/// Serialize multiple elements contiguously (no count prefix).
fn elements_wire(elements: &[Vec<u8>]) -> Vec<u8> {
    let total: usize = elements.iter().map(|e| HEADER_SIZE + e.len()).sum();
    let mut buf = Vec::with_capacity(total);
    for e in elements {
        buf.extend_from_slice(&(e.len() as u64).to_le_bytes());
        buf.extend_from_slice(e);
    }
    buf
}

/// Resolve a possibly-negative Redis index to a concrete 0-based index,
/// clamped to `[0, len]`. Returns `None` if out of range for read operations.
fn resolve_index(idx: i64, len: u64) -> Option<u64> {
    let len_i = len as i64;
    let resolved = if idx < 0 { len_i + idx } else { idx };
    if resolved < 0 || resolved >= len_i {
        None
    } else {
        Some(resolved as u64)
    }
}

// ──────────────────────────── Public API ──────────────────────────────────

/// Create an empty list in wire format.
#[allow(dead_code)]
pub fn new_empty() -> Vec<u8> {
    0u64.to_le_bytes().to_vec()
}

/// Create a list from the given elements.
pub fn new_from_elements(elements: &[Vec<u8>]) -> Vec<u8> {
    let body = elements_wire(elements);
    let mut buf = Vec::with_capacity(HEADER_SIZE + body.len());
    buf.extend_from_slice(&(elements.len() as u64).to_le_bytes());
    buf.extend_from_slice(&body);
    buf
}

/// Read element count. O(1).
pub fn len(data: &[u8]) -> Result<u64> {
    if data.len() < HEADER_SIZE {
        return Err(StorageError::InternalError(
            "list data corrupted: too short for count header".into(),
        ));
    }
    read_u64(data, 0)
}

/// Append elements at the end. O(k) for k new elements.
pub fn rpush(data: &mut Vec<u8>, elements: &[Vec<u8>]) -> Result<u64> {
    let count = len(data)?;
    let new_count = count + elements.len() as u64;
    write_u64(data, 0, new_count);
    let wire = elements_wire(elements);
    data.extend_from_slice(&wire);
    Ok(new_count)
}

/// Prepend elements at the front. O(N) memmove for existing data, but no
/// heap allocation for deserialization.
pub fn lpush(data: &mut Vec<u8>, elements: &[Vec<u8>]) -> Result<u64> {
    let count = len(data)?;
    let new_count = count + elements.len() as u64;

    let wire = elements_wire(elements);
    let wire_len = wire.len();

    // Make room: grow buffer, shift existing element bytes right
    let old_len = data.len();
    data.resize(old_len + wire_len, 0);
    data.copy_within(HEADER_SIZE..old_len, HEADER_SIZE + wire_len);

    // Write new elements after the count header
    data[HEADER_SIZE..HEADER_SIZE + wire_len].copy_from_slice(&wire);

    // Update count
    write_u64(data, 0, new_count);
    Ok(new_count)
}

/// Pop `count` elements from the left. Returns popped elements.
pub fn lpop(data: &mut Vec<u8>, count: usize) -> Result<Vec<Vec<u8>>> {
    let total = len(data)? as usize;
    if total == 0 || count == 0 {
        return Ok(vec![]);
    }
    let actual = count.min(total);

    // Scan to collect elements and find the byte offset after them
    let mut result = Vec::with_capacity(actual);
    let mut off = HEADER_SIZE;
    for _ in 0..actual {
        let (elem, next) = read_element_at(data, off)?;
        result.push(elem.to_vec());
        off = next;
    }

    // Shift remaining data left
    let remaining = data.len() - off;
    data.copy_within(off.., HEADER_SIZE);
    data.truncate(HEADER_SIZE + remaining);

    // Update count
    write_u64(data, 0, (total - actual) as u64);
    Ok(result)
}

/// Pop `count` elements from the right. Returns popped elements (in pop order,
/// i.e. rightmost first).
pub fn rpop(data: &mut Vec<u8>, count: usize) -> Result<Vec<Vec<u8>>> {
    let total = len(data)? as usize;
    if total == 0 || count == 0 {
        return Ok(vec![]);
    }
    let actual = count.min(total);
    let start_idx = total - actual;

    // Find the byte offset of element at `start_idx`
    let cut_offset = offset_of_index(data, start_idx as u64)?;

    // Read elements from cut_offset to end
    let mut result = Vec::with_capacity(actual);
    let mut off = cut_offset;
    for _ in 0..actual {
        let (elem, next) = read_element_at(data, off)?;
        result.push(elem.to_vec());
        off = next;
    }
    // Reverse so rightmost is first (Redis RPOP order)
    result.reverse();

    // Truncate buffer
    data.truncate(cut_offset);

    // Update count
    write_u64(data, 0, start_idx as u64);
    Ok(result)
}

/// Read elements in [start, stop] range (inclusive, supports negative indices).
pub fn range(data: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
    let total = len(data)?;
    if total == 0 {
        return Ok(vec![]);
    }

    let len_i = total as i64;
    let raw_start = if start < 0 { len_i + start } else { start };
    let raw_stop = if stop < 0 { len_i + stop } else { stop };

    // If start is past the end, result is empty
    if raw_start >= len_i || raw_start > raw_stop || raw_stop < 0 {
        return Ok(vec![]);
    }

    let start_idx = raw_start.max(0) as u64;
    let stop_idx = raw_stop.min(len_i - 1) as u64;

    // Scan to start_idx
    let mut off = offset_of_index(data, start_idx)?;

    let count = (stop_idx - start_idx + 1) as usize;
    let mut result = Vec::with_capacity(count);
    for _ in 0..count {
        if off >= data.len() {
            break;
        }
        let (elem, next) = read_element_at(data, off)?;
        result.push(elem.to_vec());
        off = next;
    }
    Ok(result)
}

/// Read a single element by index (supports negative indices).
pub fn index(data: &[u8], idx: i64) -> Result<Option<Vec<u8>>> {
    let total = len(data)?;
    if total == 0 {
        return Ok(None);
    }
    match resolve_index(idx, total) {
        None => Ok(None),
        Some(i) => {
            let off = offset_of_index(data, i)?;
            let (elem, _) = read_element_at(data, off)?;
            Ok(Some(elem.to_vec()))
        }
    }
}

/// Set element at index. Replaces in-place with possible resize.
pub fn set_element(data: &mut Vec<u8>, idx: i64, value: Vec<u8>) -> Result<()> {
    let total = len(data)?;
    let i = resolve_index(idx, total)
        .ok_or_else(|| StorageError::InternalError("index out of range".into()))?;

    let off = offset_of_index(data, i)?;
    let old_elem_len = read_u64(data, off)? as usize;
    let old_total = HEADER_SIZE + old_elem_len; // old element wire size
    let new_wire = element_wire(&value);

    if new_wire.len() == old_total {
        // Same size: overwrite in place
        data[off..off + new_wire.len()].copy_from_slice(&new_wire);
    } else {
        // Different size: splice
        let tail_start = off + old_total;
        let tail_len = data.len() - tail_start;
        let new_len = off + new_wire.len() + tail_len;

        if new_wire.len() > old_total {
            // Growing: expand first, then shift tail right
            let diff = new_wire.len() - old_total;
            data.resize(data.len() + diff, 0);
            data.copy_within(tail_start..tail_start + tail_len, off + new_wire.len());
        } else {
            // Shrinking: shift tail left, then truncate
            data.copy_within(tail_start..tail_start + tail_len, off + new_wire.len());
            data.truncate(new_len);
        }
        data[off..off + new_wire.len()].copy_from_slice(&new_wire);
    }
    Ok(())
}

/// Trim list to [start, stop] range. Deletes elements outside the range.
pub fn trim(data: &mut Vec<u8>, start: i64, stop: i64) -> Result<bool> {
    let total = len(data)?;
    if total == 0 {
        return Ok(true); // empty list trimmed = delete key
    }

    let len_i = total as i64;
    let raw_start = if start < 0 { len_i + start } else { start };
    let raw_stop = if stop < 0 { len_i + stop } else { stop };

    // If start is past the end, or stop is before the start, delete everything
    if raw_start >= len_i || raw_start > raw_stop || raw_stop < 0 {
        return Ok(true);
    }

    let start_idx = raw_start.max(0) as u64;
    let stop_idx = raw_stop.min(len_i - 1) as u64;
    let new_count = stop_idx - start_idx + 1;

    let range_start_off = offset_of_index(data, start_idx)?;
    let range_end_off = offset_of_index(data, stop_idx + 1).unwrap_or(data.len());

    let range_bytes = range_end_off - range_start_off;

    // Move kept range to right after count header
    data.copy_within(range_start_off..range_end_off, HEADER_SIZE);
    data.truncate(HEADER_SIZE + range_bytes);
    write_u64(data, 0, new_count);

    Ok(false)
}

/// Remove elements equal to `element`. Returns count removed.
/// count > 0: from head, count < 0: from tail, count == 0: all.
pub fn remove(data: &mut Vec<u8>, count: i64, element: &[u8]) -> Result<i64> {
    let total = len(data)? as usize;
    if total == 0 {
        return Ok(0);
    }

    let max_removals = if count == 0 {
        total
    } else {
        count.unsigned_abs() as usize
    };

    if count >= 0 {
        // Forward scan: remove from head
        remove_forward(data, total, max_removals, element)
    } else {
        // Reverse removal: we need to find which indices to remove by scanning
        // backward, then remove them
        remove_backward(data, total, max_removals, element)
    }
}

fn remove_forward(
    data: &mut Vec<u8>,
    total: usize,
    max_removals: usize,
    element: &[u8],
) -> Result<i64> {
    let mut removed = 0usize;
    let mut read_off = HEADER_SIZE;
    let mut write_off = HEADER_SIZE;

    for _ in 0..total {
        let (elem, next_off) = read_element_at(data, read_off)?;
        let elem_wire_size = next_off - read_off;

        if removed < max_removals && elem == element {
            removed += 1;
            // Skip this element (don't copy)
        } else {
            if write_off != read_off {
                data.copy_within(read_off..next_off, write_off);
            }
            write_off += elem_wire_size;
        }
        read_off = next_off;
    }

    data.truncate(write_off);
    write_u64(data, 0, (total - removed) as u64);
    Ok(removed as i64)
}

fn remove_backward(
    data: &mut Vec<u8>,
    total: usize,
    max_removals: usize,
    element: &[u8],
) -> Result<i64> {
    // First pass: collect offsets and identify which elements to remove
    let mut offsets = Vec::with_capacity(total);
    let mut off = HEADER_SIZE;
    for _ in 0..total {
        let (_, next) = read_element_at(data, off)?;
        offsets.push(off);
        off = next;
    }

    // Scan backward to find indices to remove
    let mut to_remove = Vec::new();
    for i in (0..total).rev() {
        if to_remove.len() >= max_removals {
            break;
        }
        let (elem, _) = read_element_at(data, offsets[i])?;
        if elem == element {
            to_remove.push(i);
        }
    }

    if to_remove.is_empty() {
        return Ok(0);
    }

    // Sort removal indices ascending for efficient compaction
    to_remove.sort_unstable();

    // Compact: copy non-removed elements
    let removed = to_remove.len();
    let mut remove_idx = 0;
    let mut write_off = HEADER_SIZE;
    let mut read_off = HEADER_SIZE;

    for i in 0..total {
        let (_, next_off) = read_element_at(data, read_off)?;
        let elem_wire_size = next_off - read_off;

        if remove_idx < removed && to_remove[remove_idx] == i {
            remove_idx += 1;
            // Skip
        } else {
            if write_off != read_off {
                data.copy_within(read_off..next_off, write_off);
            }
            write_off += elem_wire_size;
        }
        read_off = next_off;
    }

    data.truncate(write_off);
    write_u64(data, 0, (total - removed) as u64);
    Ok(removed as i64)
}

/// Insert `element` before or after the first occurrence of `pivot`.
/// Returns new list length, or -1 if pivot not found.
pub fn insert_pivot(
    data: &mut Vec<u8>,
    pivot: &[u8],
    element: Vec<u8>,
    before: bool,
) -> Result<i64> {
    let total = len(data)?;
    if total == 0 {
        return Ok(-1);
    }

    // Find pivot
    let mut off = HEADER_SIZE;
    for _ in 0..total {
        let (elem, next) = read_element_at(data, off)?;
        if elem == pivot {
            let insert_off = if before { off } else { next };
            let wire = element_wire(&element);
            let wire_len = wire.len();

            // Make room
            let old_len = data.len();
            data.resize(old_len + wire_len, 0);
            data.copy_within(insert_off..old_len, insert_off + wire_len);
            data[insert_off..insert_off + wire_len].copy_from_slice(&wire);

            let new_count = total + 1;
            write_u64(data, 0, new_count);
            return Ok(new_count as i64);
        }
        off = next;
    }
    Ok(-1) // pivot not found
}

/// Find positions of `element` in the list. Supports RANK, COUNT, MAXLEN.
pub fn pos(
    data: &[u8],
    element: &[u8],
    rank: i64,
    count: Option<i64>,
    maxlen: usize,
) -> Result<Vec<i64>> {
    let total = len(data)? as usize;
    if total == 0 {
        return Ok(vec![]);
    }

    let search_len = if maxlen > 0 { maxlen.min(total) } else { total };

    let mut matches = Vec::new();

    if rank > 0 {
        // Forward scan
        let mut found = 0i64;
        let mut off = HEADER_SIZE;
        for idx in 0..search_len {
            let (elem, next) = read_element_at(data, off)?;
            if elem == element {
                found += 1;
                if found >= rank {
                    matches.push(idx as i64);
                    if let Some(c) = count {
                        if c > 0 && matches.len() >= c as usize {
                            break;
                        }
                    } else {
                        // No COUNT: return first match only
                        break;
                    }
                }
            }
            off = next;
        }
    } else {
        // Backward scan: collect all element offsets first
        let scan_len = if maxlen > 0 { maxlen.min(total) } else { total };
        let scan_start = total - scan_len;

        // Skip to scan_start
        let mut off = if scan_start > 0 {
            offset_of_index(data, scan_start as u64)?
        } else {
            HEADER_SIZE
        };

        let mut candidates = Vec::new();
        for idx in scan_start..total {
            let (elem, next) = read_element_at(data, off)?;
            if elem == element {
                candidates.push(idx as i64);
            }
            off = next;
        }

        // Process from the end (negative rank)
        let abs_rank = rank.unsigned_abs() as usize;
        let mut found = 0usize;
        for &idx in candidates.iter().rev() {
            found += 1;
            if found >= abs_rank {
                matches.push(idx);
                if let Some(c) = count {
                    if c > 0 && matches.len() >= c as usize {
                        break;
                    }
                } else {
                    break;
                }
            }
        }
        // For backward rank with COUNT, results should be in ascending order
        matches.reverse();
    }

    Ok(matches)
}

/// Fully deserialize to `Vec<Vec<u8>>`. Fallback for complex operations
/// and compatibility (persistence, SORT, OBJECT ENCODING).
pub fn deserialize_all(data: &[u8]) -> Result<Vec<Vec<u8>>> {
    let total = len(data)? as usize;
    let mut result = Vec::with_capacity(total);
    let mut off = HEADER_SIZE;
    for _ in 0..total {
        let (elem, next) = read_element_at(data, off)?;
        result.push(elem.to_vec());
        off = next;
    }
    Ok(result)
}

// ──────────────────────────── Tests ──────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify our wire format is compatible with bincode::serialize
    #[test]
    fn bincode_round_trip_compatibility() {
        let elements: Vec<Vec<u8>> = vec![b"hello".to_vec(), b"world".to_vec(), b"!".to_vec()];
        let bincode_data = bincode::serialize(&elements).unwrap();
        let our_data = new_from_elements(&elements);
        assert_eq!(bincode_data, our_data);

        // Verify our deserialize matches bincode's
        let our_result = deserialize_all(&our_data).unwrap();
        let bincode_result: Vec<Vec<u8>> = bincode::deserialize(&bincode_data).unwrap();
        assert_eq!(our_result, bincode_result);
    }

    #[test]
    fn empty_list() {
        let data = new_empty();
        assert_eq!(len(&data).unwrap(), 0);
        assert_eq!(deserialize_all(&data).unwrap(), Vec::<Vec<u8>>::new());

        // Verify empty list is bincode-compatible
        let bincode_empty = bincode::serialize(&Vec::<Vec<u8>>::new()).unwrap();
        assert_eq!(data, bincode_empty);
    }

    #[test]
    fn rpush_basic() {
        let mut data = new_empty();
        let count = rpush(&mut data, &[b"a".to_vec(), b"b".to_vec()]).unwrap();
        assert_eq!(count, 2);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );

        let count = rpush(&mut data, &[b"c".to_vec()]).unwrap();
        assert_eq!(count, 3);

        // Verify bincode can read our format
        let bincode_result: Vec<Vec<u8>> = bincode::deserialize(&data).unwrap();
        assert_eq!(
            bincode_result,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn lpush_basic() {
        let mut data = new_empty();
        let count = lpush(&mut data, &[b"a".to_vec()]).unwrap();
        assert_eq!(count, 1);

        let count = lpush(&mut data, &[b"b".to_vec()]).unwrap();
        assert_eq!(count, 2);

        // LPUSH prepends, so order is [b, a]
        let result = deserialize_all(&data).unwrap();
        assert_eq!(result, vec![b"b".to_vec(), b"a".to_vec()]);

        // Verify bincode compatibility
        let bincode_result: Vec<Vec<u8>> = bincode::deserialize(&data).unwrap();
        assert_eq!(bincode_result, result);
    }

    #[test]
    fn lpush_multiple() {
        let mut data = new_from_elements(&[b"x".to_vec()]);
        // LPUSH multiple elements: they are inserted at head in order
        let count = lpush(&mut data, &[b"a".to_vec(), b"b".to_vec()]).unwrap();
        assert_eq!(count, 3);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec(), b"x".to_vec()]
        );
    }

    #[test]
    fn lpop_basic() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let popped = lpop(&mut data, 1).unwrap();
        assert_eq!(popped, vec![b"a".to_vec()]);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn lpop_multiple() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let popped = lpop(&mut data, 2).unwrap();
        assert_eq!(popped, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(deserialize_all(&data).unwrap(), vec![b"c".to_vec()]);
    }

    #[test]
    fn lpop_more_than_available() {
        let mut data = new_from_elements(&[b"a".to_vec()]);
        let popped = lpop(&mut data, 5).unwrap();
        assert_eq!(popped, vec![b"a".to_vec()]);
        assert_eq!(len(&data).unwrap(), 0);
    }

    #[test]
    fn rpop_basic() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let popped = rpop(&mut data, 1).unwrap();
        assert_eq!(popped, vec![b"c".to_vec()]);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[test]
    fn rpop_multiple() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        let popped = rpop(&mut data, 2).unwrap();
        // RPOP returns rightmost first
        assert_eq!(popped, vec![b"c".to_vec(), b"b".to_vec()]);
        assert_eq!(deserialize_all(&data).unwrap(), vec![b"a".to_vec()]);
    }

    #[test]
    fn range_basic() {
        let data = new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
        assert_eq!(
            range(&data, 1, 2).unwrap(),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            range(&data, 0, -1).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
        assert_eq!(
            range(&data, -2, -1).unwrap(),
            vec![b"c".to_vec(), b"d".to_vec()]
        );
    }

    #[test]
    fn index_basic() {
        let data = new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        assert_eq!(index(&data, 0).unwrap(), Some(b"a".to_vec()));
        assert_eq!(index(&data, 2).unwrap(), Some(b"c".to_vec()));
        assert_eq!(index(&data, -1).unwrap(), Some(b"c".to_vec()));
        assert_eq!(index(&data, 3).unwrap(), None);
    }

    #[test]
    fn set_element_basic() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        set_element(&mut data, 1, b"BB".to_vec()).unwrap();
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"a".to_vec(), b"BB".to_vec(), b"c".to_vec()]
        );

        // Verify bincode compatibility after mutation
        let bincode_result: Vec<Vec<u8>> = bincode::deserialize(&data).unwrap();
        assert_eq!(
            bincode_result,
            vec![b"a".to_vec(), b"BB".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn trim_basic() {
        let mut data =
            new_from_elements(&[b"a".to_vec(), b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]);
        let deleted = trim(&mut data, 1, 2).unwrap();
        assert!(!deleted);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn trim_delete_all() {
        let mut data = new_from_elements(&[b"a".to_vec()]);
        let deleted = trim(&mut data, 5, 10).unwrap();
        assert!(deleted);
    }

    #[test]
    fn remove_forward() {
        let mut data = new_from_elements(&[
            b"a".to_vec(),
            b"b".to_vec(),
            b"a".to_vec(),
            b"c".to_vec(),
            b"a".to_vec(),
        ]);
        let removed = remove(&mut data, 2, b"a").unwrap();
        assert_eq!(removed, 2);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"b".to_vec(), b"c".to_vec(), b"a".to_vec()]
        );
    }

    #[test]
    fn remove_backward() {
        let mut data = new_from_elements(&[
            b"a".to_vec(),
            b"b".to_vec(),
            b"a".to_vec(),
            b"c".to_vec(),
            b"a".to_vec(),
        ]);
        let removed = remove(&mut data, -2, b"a").unwrap();
        assert_eq!(removed, 2);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn remove_all() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"a".to_vec(), b"b".to_vec()]);
        let removed = remove(&mut data, 0, b"a").unwrap();
        assert_eq!(removed, 2);
        assert_eq!(deserialize_all(&data).unwrap(), vec![b"b".to_vec()]);
    }

    #[test]
    fn insert_pivot_before() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"c".to_vec()]);
        let new_len = insert_pivot(&mut data, b"c", b"b".to_vec(), true).unwrap();
        assert_eq!(new_len, 3);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn insert_pivot_after() {
        let mut data = new_from_elements(&[b"a".to_vec(), b"c".to_vec()]);
        let new_len = insert_pivot(&mut data, b"a", b"b".to_vec(), false).unwrap();
        assert_eq!(new_len, 3);
        assert_eq!(
            deserialize_all(&data).unwrap(),
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn insert_pivot_not_found() {
        let mut data = new_from_elements(&[b"a".to_vec()]);
        let result = insert_pivot(&mut data, b"z", b"b".to_vec(), true).unwrap();
        assert_eq!(result, -1);
    }

    #[test]
    fn pos_forward() {
        let data = new_from_elements(&[
            b"a".to_vec(),
            b"b".to_vec(),
            b"a".to_vec(),
            b"c".to_vec(),
            b"a".to_vec(),
        ]);
        // First occurrence
        assert_eq!(pos(&data, b"a", 1, None, 0).unwrap(), vec![0]);
        // Second occurrence
        assert_eq!(pos(&data, b"a", 2, None, 0).unwrap(), vec![2]);
        // All occurrences with COUNT 0
        assert_eq!(pos(&data, b"a", 1, Some(0), 0).unwrap(), vec![0, 2, 4]);
    }

    #[test]
    fn operations_on_bincode_data() {
        // Start with data created by bincode (simulating existing stored data)
        let elements: Vec<Vec<u8>> = vec![b"one".to_vec(), b"two".to_vec(), b"three".to_vec()];
        let mut data = bincode::serialize(&elements).unwrap();

        // Our operations should work on bincode-created data
        assert_eq!(len(&data).unwrap(), 3);
        rpush(&mut data, &[b"four".to_vec()]).unwrap();
        assert_eq!(len(&data).unwrap(), 4);

        let result = deserialize_all(&data).unwrap();
        assert_eq!(
            result,
            vec![
                b"one".to_vec(),
                b"two".to_vec(),
                b"three".to_vec(),
                b"four".to_vec()
            ]
        );

        // And bincode can still read the result
        let bincode_result: Vec<Vec<u8>> = bincode::deserialize(&data).unwrap();
        assert_eq!(bincode_result, result);
    }
}
