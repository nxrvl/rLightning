//! Native data types for the storage engine.
//! Eliminates bincode serialization from the data access hot path.

use std::collections::{BTreeSet, HashMap, VecDeque};

use hashbrown::HashMap as HBHashMap;
use hashbrown::HashSet as HBHashSet;
use ordered_float::OrderedFloat;
use rustc_hash::FxBuildHasher;
use serde::{Deserialize, Serialize};

use crate::storage::item::RedisDataType;
use crate::storage::stream::StreamData;

/// Native HashMap with FxHash for fast key lookups.
pub type NativeHashMap = HBHashMap<Vec<u8>, Vec<u8>, FxBuildHasher>;

/// Native HashSet with FxHash for fast member lookups.
pub type NativeHashSet = HBHashSet<Vec<u8>, FxBuildHasher>;

/// Sorted set data structure using BTreeSet for ordered iteration and HashMap for O(1) lookups.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SortedSetData {
    pub entries: BTreeSet<(OrderedFloat<f64>, Vec<u8>)>,
    pub scores: HashMap<Vec<u8>, f64>,
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
            scores: HashMap::new(),
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
    /// String value (also used for bitmaps and HyperLogLog)
    Str(Vec<u8>),
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
            StoreValue::Str(v) => v.len(),
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
                    .iter()
                    .map(|(_, entry)| {
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
    pub fn default_for_type(data_type: RedisDataType) -> Self {
        match data_type {
            RedisDataType::String => StoreValue::Str(Vec::new()),
            RedisDataType::Hash => StoreValue::Hash(NativeHashMap::default()),
            RedisDataType::Set => StoreValue::Set(NativeHashSet::default()),
            RedisDataType::ZSet => StoreValue::ZSet(SortedSetData::new()),
            RedisDataType::List => StoreValue::List(VecDeque::new()),
            RedisDataType::Stream => StoreValue::Stream(StreamData::new()),
        }
    }

    /// Get reference to string bytes, or None if not a string.
    pub fn as_str(&self) -> Option<&Vec<u8>> {
        match self {
            StoreValue::Str(v) => Some(v),
            _ => None,
        }
    }

    /// Get mutable reference to string bytes, or None if not a string.
    pub fn as_str_mut(&mut self) -> Option<&mut Vec<u8>> {
        match self {
            StoreValue::Str(v) => Some(v),
            _ => None,
        }
    }

    /// Get reference to hash map, or None if not a hash.
    pub fn as_hash(&self) -> Option<&NativeHashMap> {
        match self {
            StoreValue::Hash(m) => Some(m),
            _ => None,
        }
    }

    /// Get mutable reference to hash map, or None if not a hash.
    pub fn as_hash_mut(&mut self) -> Option<&mut NativeHashMap> {
        match self {
            StoreValue::Hash(m) => Some(m),
            _ => None,
        }
    }

    /// Get reference to set, or None if not a set.
    pub fn as_set(&self) -> Option<&NativeHashSet> {
        match self {
            StoreValue::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Get mutable reference to set, or None if not a set.
    pub fn as_set_mut(&mut self) -> Option<&mut NativeHashSet> {
        match self {
            StoreValue::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Get reference to sorted set, or None if not a sorted set.
    pub fn as_zset(&self) -> Option<&SortedSetData> {
        match self {
            StoreValue::ZSet(ss) => Some(ss),
            _ => None,
        }
    }

    /// Get mutable reference to sorted set, or None if not a sorted set.
    pub fn as_zset_mut(&mut self) -> Option<&mut SortedSetData> {
        match self {
            StoreValue::ZSet(ss) => Some(ss),
            _ => None,
        }
    }

    /// Get reference to list, or None if not a list.
    pub fn as_list(&self) -> Option<&VecDeque<Vec<u8>>> {
        match self {
            StoreValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Get mutable reference to list, or None if not a list.
    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<Vec<u8>>> {
        match self {
            StoreValue::List(l) => Some(l),
            _ => None,
        }
    }

    /// Get reference to stream, or None if not a stream.
    pub fn as_stream(&self) -> Option<&StreamData> {
        match self {
            StoreValue::Stream(s) => Some(s),
            _ => None,
        }
    }

    /// Get mutable reference to stream, or None if not a stream.
    pub fn as_stream_mut(&mut self) -> Option<&mut StreamData> {
        match self {
            StoreValue::Stream(s) => Some(s),
            _ => None,
        }
    }
}
