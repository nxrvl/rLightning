//! Native data types for the storage engine.
//! Eliminates bincode serialization from the data access hot path.

use std::collections::{BTreeSet, VecDeque};
use std::fmt;
use std::ops::Deref;

use hashbrown::HashMap as HBHashMap;
use hashbrown::HashSet as HBHashSet;
use ordered_float::OrderedFloat;
use rustc_hash::FxBuildHasher;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::storage::item::RedisDataType;
use crate::storage::stream::StreamData;

/// Native HashMap with FxHash for fast key lookups.
pub type NativeHashMap = HBHashMap<Vec<u8>, Vec<u8>, FxBuildHasher>;

/// Native HashSet with FxHash for fast member lookups.
pub type NativeHashSet = HBHashSet<Vec<u8>, FxBuildHasher>;

/// Small String Optimization: values <= 23 bytes stored inline (no heap allocation),
/// larger values stored on the heap. Saves one allocation per small string value.
#[derive(Clone)]
pub enum CompactValue {
    /// Inline storage for values 0-23 bytes (no heap allocation).
    Inline { len: u8, data: [u8; 23] },
    /// Heap storage for values > 23 bytes.
    Heap(Vec<u8>),
}

/// Maximum number of bytes that fit in the inline variant.
const COMPACT_INLINE_MAX: usize = 23;

impl CompactValue {
    /// Create an empty CompactValue.
    pub fn new() -> Self {
        CompactValue::Inline {
            len: 0,
            data: [0u8; 23],
        }
    }

    /// Get the stored bytes as a slice.
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            CompactValue::Inline { len, data } => &data[..*len as usize],
            CompactValue::Heap(v) => v,
        }
    }

    /// Get the length of the stored value.
    #[inline]
    pub fn len(&self) -> usize {
        match self {
            CompactValue::Inline { len, .. } => *len as usize,
            CompactValue::Heap(v) => v.len(),
        }
    }

    /// Check if the stored value is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Memory size of this CompactValue (including heap allocation if any).
    #[inline]
    pub fn mem_size(&self) -> usize {
        match self {
            CompactValue::Inline { .. } => std::mem::size_of::<CompactValue>(),
            CompactValue::Heap(h) => std::mem::size_of::<CompactValue>() + h.capacity(),
        }
    }

    /// Predict memory size for a value of the given byte length.
    /// Used by batch operations to avoid constructing a CompactValue just
    /// to measure its size.
    #[inline]
    pub fn mem_for_data_len(len: usize) -> usize {
        if len <= COMPACT_INLINE_MAX {
            std::mem::size_of::<CompactValue>()
        } else {
            std::mem::size_of::<CompactValue>() + len
        }
    }

    /// Convert to an owned Vec<u8>.
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    /// Consume self and return the bytes as a Vec<u8>.
    pub fn into_vec(self) -> Vec<u8> {
        match self {
            CompactValue::Inline { len, data } => data[..len as usize].to_vec(),
            CompactValue::Heap(v) => v,
        }
    }

    /// Append bytes to this value (used by APPEND command).
    pub fn append(&mut self, extra: &[u8]) {
        let new_len = self.len() + extra.len();
        if new_len <= COMPACT_INLINE_MAX {
            // Result still fits inline
            match self {
                CompactValue::Inline { len, data } => {
                    let start = *len as usize;
                    data[start..start + extra.len()].copy_from_slice(extra);
                    *len = new_len as u8;
                }
                CompactValue::Heap(v) => {
                    v.extend_from_slice(extra);
                }
            }
        } else {
            // Must go to heap
            let mut v = self.to_vec();
            v.extend_from_slice(extra);
            *self = CompactValue::Heap(v);
        }
    }

    /// Replace all bytes with new content (used by SETRANGE and similar).
    #[allow(dead_code)]
    pub fn set_bytes(&mut self, bytes: Vec<u8>) {
        *self = CompactValue::from(bytes);
    }
}

impl Default for CompactValue {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for CompactValue {
    type Target = [u8];

    #[inline]
    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl AsRef<[u8]> for CompactValue {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<Vec<u8>> for CompactValue {
    fn from(v: Vec<u8>) -> Self {
        if v.len() <= COMPACT_INLINE_MAX {
            let mut data = [0u8; 23];
            data[..v.len()].copy_from_slice(&v);
            CompactValue::Inline {
                len: v.len() as u8,
                data,
            }
        } else {
            CompactValue::Heap(v)
        }
    }
}

impl From<&[u8]> for CompactValue {
    fn from(s: &[u8]) -> Self {
        if s.len() <= COMPACT_INLINE_MAX {
            let mut data = [0u8; 23];
            data[..s.len()].copy_from_slice(s);
            CompactValue::Inline {
                len: s.len() as u8,
                data,
            }
        } else {
            CompactValue::Heap(s.to_vec())
        }
    }
}

impl fmt::Debug for CompactValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompactValue::Inline { len, .. } => {
                write!(f, "CompactValue::Inline({} bytes)", len)
            }
            CompactValue::Heap(v) => {
                write!(f, "CompactValue::Heap({} bytes)", v.len())
            }
        }
    }
}

impl PartialEq for CompactValue {
    fn eq(&self, other: &Self) -> bool {
        self.as_bytes() == other.as_bytes()
    }
}

impl Eq for CompactValue {}

impl PartialEq<[u8]> for CompactValue {
    fn eq(&self, other: &[u8]) -> bool {
        self.as_bytes() == other
    }
}

impl PartialEq<Vec<u8>> for CompactValue {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.as_bytes() == other.as_slice()
    }
}

// Serde: serialize as raw bytes (same wire format as Vec<u8>)
impl Serialize for CompactValue {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.as_bytes())
    }
}

impl<'de> Deserialize<'de> for CompactValue {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct CompactVisitor;

        impl<'de> Visitor<'de> for CompactVisitor {
            type Value = CompactValue;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a byte sequence")
            }

            fn visit_bytes<E: de::Error>(self, v: &[u8]) -> Result<CompactValue, E> {
                Ok(CompactValue::from(v))
            }

            fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> Result<CompactValue, E> {
                Ok(CompactValue::from(v))
            }

            fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<CompactValue, A::Error> {
                let mut bytes = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(b) = seq.next_element()? {
                    bytes.push(b);
                }
                Ok(CompactValue::from(bytes))
            }
        }

        deserializer.deserialize_byte_buf(CompactVisitor)
    }
}

/// Sorted set data structure using BTreeSet for ordered iteration and FxHashMap for O(1) lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortedSetData {
    pub entries: BTreeSet<(OrderedFloat<f64>, Vec<u8>)>,
    pub scores: HBHashMap<Vec<u8>, f64, FxBuildHasher>,
}

impl Default for SortedSetData {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedSetData {
    pub fn new() -> Self {
        SortedSetData {
            entries: BTreeSet::new(),
            scores: HBHashMap::with_hasher(FxBuildHasher),
        }
    }

    pub fn len(&self) -> usize {
        self.scores.len()
    }

    pub fn is_empty(&self) -> bool {
        self.scores.is_empty()
    }

    pub fn insert(&mut self, score: f64, member: Vec<u8>) -> bool {
        if let Some(&old_score) = self.scores.get(&member) {
            if old_score != score {
                self.entries
                    .remove(&(OrderedFloat(old_score), member.clone()));
                self.entries.insert((OrderedFloat(score), member.clone()));
                self.scores.insert(member, score);
            }
            false
        } else {
            self.entries.insert((OrderedFloat(score), member.clone()));
            self.scores.insert(member, score);
            true
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(score) = self.scores.remove(member) {
            self.entries
                .remove(&(OrderedFloat(score), member.to_vec()));
            true
        } else {
            false
        }
    }

    pub fn get_score(&self, member: &[u8]) -> Option<f64> {
        self.scores.get(member).copied()
    }

    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let target = (OrderedFloat(*score), member.to_vec());
        Some(self.entries.range(..&target).count())
    }

    pub fn rev_rank(&self, member: &[u8]) -> Option<usize> {
        let score = self.scores.get(member)?;
        let target = (OrderedFloat(*score), member.to_vec());
        Some(self.entries.len() - 1 - self.entries.range(..&target).count())
    }
}

/// Native value types stored directly in the storage engine.
/// Eliminates bincode serialization/deserialization on every access.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StoreValue {
    /// String value (also used for bitmaps and HyperLogLog).
    /// Uses CompactValue for SSO: values <= 23 bytes stored inline.
    Str(CompactValue),
    /// Hash: field -> value mapping with FxHash
    Hash(NativeHashMap),
    /// Set: unique member collection with FxHash
    Set(NativeHashSet),
    /// Sorted set: dual-index with BTreeSet (score-ordered) + HashMap (member lookup)
    ZSet(SortedSetData),
    /// List: double-ended queue for O(1) push/pop at both ends
    List(VecDeque<Vec<u8>>),
    /// Stream: ordered log with consumer groups
    Stream(StreamData),
}

/// Result of an atomic_modify closure indicating what to do with the entry.
#[derive(Debug)]
pub enum ModifyResult {
    /// Value was modified in-place via the &mut reference, keep it
    Keep,
    /// Value was NOT modified — true no-op. Unlike `Keep`, this skips
    /// WATCH version bump and LRU touch. Use for commands like HDEL/SREM/ZREM
    /// that examined an existing key but removed nothing (matching Redis
    /// semantics where signalModifiedKey is only called on actual mutation).
    KeepUnchanged,
    /// Replace/create with a new value
    Set(StoreValue),
    /// Delete the key
    Delete,
}

impl StoreValue {
    /// Get the Redis data type for this value.
    pub fn data_type(&self) -> RedisDataType {
        match self {
            StoreValue::Str(_) => RedisDataType::String,
            StoreValue::Hash(_) => RedisDataType::Hash,
            StoreValue::Set(_) => RedisDataType::Set,
            StoreValue::ZSet(_) => RedisDataType::ZSet,
            StoreValue::List(_) => RedisDataType::List,
            StoreValue::Stream(_) => RedisDataType::Stream,
        }
    }

    /// Estimate the memory usage of this value in bytes.
    pub fn mem_size(&self) -> usize {
        match self {
            StoreValue::Str(v) => v.mem_size(),
            StoreValue::Hash(map) => map
                .iter()
                .map(|(k, v)| k.len() + v.len() + 64)
                .sum::<usize>(),
            StoreValue::Set(set) => set.iter().map(|k| k.len() + 32).sum::<usize>(),
            StoreValue::ZSet(ss) => {
                let entries_size: usize =
                    ss.entries.iter().map(|(_, member)| member.len() + 24).sum();
                let scores_size: usize =
                    ss.scores.iter().map(|(member, _)| member.len() + 16).sum();
                entries_size + scores_size
            }
            StoreValue::List(deque) => {
                deque.iter().map(|elem| elem.len() + 24).sum::<usize>()
            }
            StoreValue::Stream(s) => {
                let entries_size: usize = s
                    .entries
                    .values()
                    .map(|entry| {
                        entry
                            .fields
                            .iter()
                            .map(|(k, v)| k.len() + v.len() + 16)
                            .sum::<usize>()
                            + 32
                    })
                    .sum();
                entries_size + 256
            }
        }
    }

    /// Create a default/empty value for a given data type.
    #[allow(dead_code)]
    pub fn default_for_type(data_type: RedisDataType) -> Self {
        match data_type {
            RedisDataType::String => StoreValue::Str(CompactValue::new()),
            RedisDataType::Hash => StoreValue::Hash(NativeHashMap::default()),
            RedisDataType::Set => StoreValue::Set(NativeHashSet::default()),
            RedisDataType::ZSet => StoreValue::ZSet(SortedSetData::new()),
            RedisDataType::List => StoreValue::List(VecDeque::new()),
            RedisDataType::Stream => StoreValue::Stream(StreamData::new()),
        }
    }

    /// Get reference to string bytes as a slice, or None if not a string.
    pub fn as_str(&self) -> Option<&[u8]> {
        match self {
            StoreValue::Str(v) => Some(v.as_bytes()),
            _ => None,
        }
    }

    /// Get mutable reference to the CompactValue, or None if not a string.
    /// Use this for in-place mutation (e.g., APPEND).
    pub fn as_compact_str_mut(&mut self) -> Option<&mut CompactValue> {
        match self {
            StoreValue::Str(v) => Some(v),
            _ => None,
        }
    }

    /// Get reference to hash map, or None if not a hash.
    #[allow(dead_code)]
    pub fn as_hash(&self) -> Option<&NativeHashMap> {
        match self {
            StoreValue::Hash(m) => Some(m),
            _ => None,
        }
    }

    /// Get mutable reference to hash map, or None if not a hash.
    #[allow(dead_code)]
    pub fn as_hash_mut(&mut self) -> Option<&mut NativeHashMap> {
        match self {
            StoreValue::Hash(m) => Some(m),
            _ => None,
        }
    }

    /// Get reference to set, or None if not a set.
    #[allow(dead_code)]
    pub fn as_set(&self) -> Option<&NativeHashSet> {
        match self {
            StoreValue::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Get mutable reference to set, or None if not a set.
    #[allow(dead_code)]
    pub fn as_set_mut(&mut self) -> Option<&mut NativeHashSet> {
        match self {
            StoreValue::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Get reference to sorted set, or None if not a sorted set.
    #[allow(dead_code)]
    pub fn as_zset(&self) -> Option<&SortedSetData> {
        match self {
            StoreValue::ZSet(ss) => Some(ss),
            _ => None,
        }
    }

    /// Get mutable reference to sorted set, or None if not a sorted set.
    #[allow(dead_code)]
    pub fn as_zset_mut(&mut self) -> Option<&mut SortedSetData> {
        match self {
            StoreValue::ZSet(ss) => Some(ss),
            _ => None,
        }
    }

    /// Get reference to list, or None if not a list.
    #[allow(dead_code)]
    pub fn as_list(&self) -> Option<&VecDeque<Vec<u8>>> {
        match self {
            StoreValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Get mutable reference to list, or None if not a list.
    #[allow(dead_code)]
    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<Vec<u8>>> {
        match self {
            StoreValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Get reference to stream, or None if not a stream.
    #[allow(dead_code)]
    pub fn as_stream(&self) -> Option<&StreamData> {
        match self {
            StoreValue::Stream(s) => Some(s),
            _ => None,
        }
    }

    /// Get mutable reference to stream, or None if not a stream.
    #[allow(dead_code)]
    pub fn as_stream_mut(&mut self) -> Option<&mut StreamData> {
        match self {
            StoreValue::Stream(s) => Some(s),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── CompactValue SSO tests ───────────────────────────────────────────

    #[test]
    fn test_compact_value_empty() {
        let cv = CompactValue::new();
        assert_eq!(cv.len(), 0);
        assert!(cv.is_empty());
        assert_eq!(cv.as_bytes(), b"");
        assert!(matches!(cv, CompactValue::Inline { len: 0, .. }));
    }

    #[test]
    fn test_compact_value_inline_boundary_23_bytes() {
        // Exactly 23 bytes should be inline
        let data = vec![b'x'; 23];
        let cv = CompactValue::from(data.clone());
        assert_eq!(cv.len(), 23);
        assert_eq!(cv.as_bytes(), &data[..]);
        assert!(matches!(cv, CompactValue::Inline { len: 23, .. }));
    }

    #[test]
    fn test_compact_value_heap_24_bytes() {
        // 24 bytes should go to heap
        let data = vec![b'y'; 24];
        let cv = CompactValue::from(data.clone());
        assert_eq!(cv.len(), 24);
        assert_eq!(cv.as_bytes(), &data[..]);
        assert!(matches!(cv, CompactValue::Heap(_)));
    }

    #[test]
    fn test_compact_value_from_slice() {
        let small: &[u8] = b"hello";
        let cv = CompactValue::from(small);
        assert_eq!(cv.as_bytes(), b"hello");
        assert!(matches!(cv, CompactValue::Inline { len: 5, .. }));

        let large: &[u8] = &[b'z'; 100];
        let cv = CompactValue::from(large);
        assert_eq!(cv.len(), 100);
        assert!(matches!(cv, CompactValue::Heap(_)));
    }

    #[test]
    fn test_compact_value_to_vec() {
        let cv = CompactValue::from(b"test".as_slice());
        assert_eq!(cv.to_vec(), b"test".to_vec());

        let cv = CompactValue::from(vec![1u8; 50]);
        assert_eq!(cv.to_vec(), vec![1u8; 50]);
    }

    #[test]
    fn test_compact_value_into_vec() {
        let cv = CompactValue::from(b"inline".as_slice());
        assert_eq!(cv.into_vec(), b"inline".to_vec());

        let data = vec![2u8; 100];
        let cv = CompactValue::from(data.clone());
        assert_eq!(cv.into_vec(), data);
    }

    #[test]
    fn test_compact_value_append_stays_inline() {
        let mut cv = CompactValue::from(b"hello".as_slice());
        cv.append(b" world");
        assert_eq!(cv.as_bytes(), b"hello world");
        assert!(matches!(cv, CompactValue::Inline { len: 11, .. }));
    }

    #[test]
    fn test_compact_value_append_promotes_to_heap() {
        let mut cv = CompactValue::from(vec![b'a'; 20]);
        assert!(matches!(cv, CompactValue::Inline { len: 20, .. }));
        cv.append(b"xxxx"); // 24 bytes total
        assert_eq!(cv.len(), 24);
        assert_eq!(&cv.as_bytes()[..20], &[b'a'; 20]);
        assert_eq!(&cv.as_bytes()[20..], b"xxxx");
        assert!(matches!(cv, CompactValue::Heap(_)));
    }

    #[test]
    fn test_compact_value_append_to_heap() {
        let mut cv = CompactValue::from(vec![b'b'; 50]);
        cv.append(b"more");
        assert_eq!(cv.len(), 54);
        assert!(matches!(cv, CompactValue::Heap(_)));
    }

    #[test]
    fn test_compact_value_deref() {
        let cv = CompactValue::from(b"slice_test".as_slice());
        // Test Deref to [u8]
        let slice: &[u8] = &cv;
        assert_eq!(slice, b"slice_test");
        // Test len through Deref
        assert_eq!(cv.len(), 10);
    }

    #[test]
    fn test_compact_value_partial_eq() {
        let cv1 = CompactValue::from(b"same".as_slice());
        let cv2 = CompactValue::from(b"same".to_vec());
        assert_eq!(cv1, cv2);

        let cv3 = CompactValue::from(b"different".as_slice());
        assert_ne!(cv1, cv3);

        // PartialEq with &[u8]
        assert!(cv1 == *b"same".as_slice());
        // PartialEq with Vec<u8>
        assert!(cv1 == b"same".to_vec());
    }

    #[test]
    fn test_compact_value_clone() {
        let cv = CompactValue::from(b"clone_me".as_slice());
        let cloned = cv.clone();
        assert_eq!(cv, cloned);

        let cv_heap = CompactValue::from(vec![0u8; 100]);
        let cloned_heap = cv_heap.clone();
        assert_eq!(cv_heap, cloned_heap);
    }

    #[test]
    fn test_compact_value_binary_data() {
        // Test with binary data including null bytes
        let data = vec![0u8, 1, 2, 255, 254, 0, 128];
        let cv = CompactValue::from(data.clone());
        assert_eq!(cv.as_bytes(), &data[..]);
    }

    // ── StoreValue with CompactValue integration ────────────────────────

    #[test]
    fn test_store_value_str_with_compact_inline() {
        let sv = StoreValue::Str(b"small".to_vec().into());
        assert_eq!(sv.as_str(), Some(b"small".as_slice()));
        assert_eq!(sv.data_type(), RedisDataType::String);
    }

    #[test]
    fn test_store_value_str_with_compact_heap() {
        let big = vec![b'x'; 100];
        let sv = StoreValue::Str(big.clone().into());
        assert_eq!(sv.as_str(), Some(big.as_slice()));
    }

    #[test]
    fn test_store_value_compact_str_mut() {
        let mut sv = StoreValue::Str(b"base".to_vec().into());
        if let Some(cv) = sv.as_compact_str_mut() {
            cv.append(b"_ext");
        }
        assert_eq!(sv.as_str(), Some(b"base_ext".as_slice()));
    }
}
