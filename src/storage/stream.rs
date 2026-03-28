use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

/// A stream entry ID in the format "timestamp-sequence"
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamEntryId {
    pub ms: u64,
    pub seq: u64,
}

impl StreamEntryId {
    pub fn new(ms: u64, seq: u64) -> Self {
        StreamEntryId { ms, seq }
    }

    pub fn min() -> Self {
        StreamEntryId { ms: 0, seq: 0 }
    }

    pub fn max() -> Self {
        StreamEntryId {
            ms: u64::MAX,
            seq: u64::MAX,
        }
    }

    /// Parse a stream entry ID from a string like "1526919030474-55"
    /// Supports special values: "*", "-", "+", and incomplete IDs like "1526919030474"
    #[allow(dead_code)]
    pub fn parse(s: &str) -> Option<Self> {
        if s == "-" {
            return Some(Self::min());
        }
        if s == "+" {
            return Some(Self::max());
        }
        if s == "*" {
            return None; // auto-generate
        }

        let parts: Vec<&str> = s.splitn(2, '-').collect();
        let ms = parts[0].parse::<u64>().ok()?;
        let seq = if parts.len() > 1 {
            if parts[1] == "*" {
                return Some(StreamEntryId { ms, seq: 0 }); // will be resolved later
            }
            parts[1].parse::<u64>().ok()?
        } else {
            0
        };
        Some(StreamEntryId { ms, seq })
    }

    /// Parse for range queries where incomplete IDs get min seq
    pub fn parse_range_start(s: &str) -> Option<Self> {
        if s == "-" {
            return Some(Self::min());
        }
        if s == "+" {
            return Some(Self::max());
        }
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        let ms = parts[0].parse::<u64>().ok()?;
        let seq = if parts.len() > 1 {
            parts[1].parse::<u64>().ok()?
        } else {
            0
        };
        Some(StreamEntryId { ms, seq })
    }

    /// Parse for range queries where incomplete IDs get max seq
    pub fn parse_range_end(s: &str) -> Option<Self> {
        if s == "-" {
            return Some(Self::min());
        }
        if s == "+" {
            return Some(Self::max());
        }
        let parts: Vec<&str> = s.splitn(2, '-').collect();
        let ms = parts[0].parse::<u64>().ok()?;
        let seq = if parts.len() > 1 {
            parts[1].parse::<u64>().ok()?
        } else {
            u64::MAX
        };
        Some(StreamEntryId { ms, seq })
    }
}

impl std::fmt::Display for StreamEntryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.ms, self.seq)
    }
}

/// A single stream entry containing field-value pairs
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamEntry {
    pub id: StreamEntryId,
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Compute the in-memory byte size of a stream entry for accurate memory accounting.
/// Uses the same formula as `estimate_entry_size` in command/types/stream.rs.
fn entry_mem_size(entry: &StreamEntry) -> usize {
    // StreamEntryId: 16 bytes, StreamEntry struct overhead: ~24 bytes (Vec ptr+len+cap)
    // BTreeMap node overhead: ~64 bytes
    let mut size: usize = 16 + 24 + 64;
    for (k, v) in &entry.fields {
        // Each field pair: 2x Vec overhead (24 bytes each) + actual data
        size += 24 + k.len() + 24 + v.len();
    }
    size
}

/// A pending entry in a consumer group's PEL (Pending Entries List)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEntry {
    pub id: StreamEntryId,
    pub consumer: String,
    pub delivery_time: u64, // ms timestamp
    pub delivery_count: u64,
}

/// A consumer within a consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConsumer {
    pub name: String,
    pub seen_time: u64,              // last time this consumer was active (ms)
    pub pending: Vec<StreamEntryId>, // IDs pending for this consumer
}

/// A consumer group
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroup {
    pub name: String,
    pub last_delivered_id: StreamEntryId,
    pub consumers: HashMap<String, StreamConsumer>,
    pub pel: BTreeMap<StreamEntryId, PendingEntry>,
    pub entries_read: Option<u64>,
}

impl ConsumerGroup {
    pub fn new(name: String, last_delivered_id: StreamEntryId) -> Self {
        ConsumerGroup {
            name,
            last_delivered_id,
            consumers: HashMap::new(),
            pel: BTreeMap::new(),
            entries_read: Some(0),
        }
    }

    /// Get or create a consumer by name
    pub fn get_or_create_consumer(&mut self, name: &str) -> &mut StreamConsumer {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.consumers
            .entry(name.to_string())
            .or_insert_with(|| StreamConsumer {
                name: name.to_string(),
                seen_time: now_ms,
                pending: Vec::new(),
            })
    }
}

/// The main stream data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamData {
    /// Entries stored in order (BTreeMap for efficient range queries)
    pub entries: BTreeMap<StreamEntryId, StreamEntry>,
    /// The last generated entry ID
    pub last_id: StreamEntryId,
    /// Consumer groups attached to this stream
    pub groups: HashMap<String, ConsumerGroup>,
    /// Total number of entries ever added (includes deleted ones)
    pub entries_added: u64,
    /// The first entry ID ever added to the stream
    pub first_entry_id: Option<StreamEntryId>,
    /// The maximum entry ID that was deleted
    pub max_deleted_entry_id: Option<StreamEntryId>,
}

impl StreamData {
    /// Compute the total in-memory byte size of this stream.
    /// Uses `entry_mem_size()` per entry so that the baseline matches
    /// the deltas applied by `estimate_entry_size` / `entry_mem_size` on
    /// XADD/XDEL/XTRIM, preventing `cached_mem_size` drift.
    pub fn mem_size(&self) -> usize {
        let entries_size: usize = self.entries.values().map(entry_mem_size).sum();
        // Base overhead: StreamData struct fields (BTreeMap, HashMap, metadata)
        entries_size + 256
    }

    pub fn new() -> Self {
        StreamData {
            entries: BTreeMap::new(),
            last_id: StreamEntryId::new(0, 0),
            groups: HashMap::new(),
            entries_added: 0,
            first_entry_id: None,
            max_deleted_entry_id: None,
        }
    }

    /// Generate the next entry ID based on auto-generation rules
    #[allow(dead_code)]
    pub fn generate_id(
        &self,
        explicit_ms: Option<u64>,
        explicit_seq: Option<u64>,
    ) -> Result<StreamEntryId, &'static str> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        match (explicit_ms, explicit_seq) {
            // Fully auto-generated: "*"
            (None, None) => {
                let ms = now_ms.max(self.last_id.ms);
                let seq = if ms == self.last_id.ms {
                    self.last_id.seq + 1
                } else {
                    0
                };
                Ok(StreamEntryId::new(ms, seq))
            }
            // Explicit ms, auto seq: "12345-*"
            (Some(ms), None) => {
                let seq = if ms == self.last_id.ms {
                    self.last_id.seq + 1
                } else if ms > self.last_id.ms {
                    0
                } else {
                    return Err(
                        "The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                };
                Ok(StreamEntryId::new(ms, seq))
            }
            // Fully explicit: "12345-67"
            (Some(ms), Some(seq)) => {
                let new_id = StreamEntryId::new(ms, seq);
                if new_id <= self.last_id && !(self.last_id.ms == 0 && self.last_id.seq == 0) {
                    return Err(
                        "The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                // Special case: 0-0 is not allowed if stream is not empty
                if ms == 0 && seq == 0 && !self.entries.is_empty() {
                    return Err(
                        "The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                Ok(new_id)
            }
            (None, Some(_)) => Err("Invalid stream ID format"),
        }
    }

    /// Generate the next entry ID using the cached wall-clock time.
    ///
    /// Same logic as `generate_id` but avoids the `SystemTime::now()` syscall
    /// on the hot path by reading from the cached clock updated every 1ms.
    pub fn generate_id_cached(
        &self,
        explicit_ms: Option<u64>,
        explicit_seq: Option<u64>,
    ) -> Result<StreamEntryId, &'static str> {
        let now_ms = crate::storage::clock::cached_now_ms();

        match (explicit_ms, explicit_seq) {
            // Fully auto-generated: "*"
            (None, None) => {
                let ms = now_ms.max(self.last_id.ms);
                let seq = if ms == self.last_id.ms {
                    self.last_id.seq + 1
                } else {
                    0
                };
                Ok(StreamEntryId::new(ms, seq))
            }
            // Explicit ms, auto seq: "12345-*"
            (Some(ms), None) => {
                let seq = if ms == self.last_id.ms {
                    self.last_id.seq + 1
                } else if ms > self.last_id.ms {
                    0
                } else {
                    return Err(
                        "The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                };
                Ok(StreamEntryId::new(ms, seq))
            }
            // Fully explicit: "12345-67"
            (Some(ms), Some(seq)) => {
                let new_id = StreamEntryId::new(ms, seq);
                if new_id <= self.last_id && !(self.last_id.ms == 0 && self.last_id.seq == 0) {
                    return Err(
                        "The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                // Special case: 0-0 is not allowed if stream is not empty
                if ms == 0 && seq == 0 && !self.entries.is_empty() {
                    return Err(
                        "The ID specified in XADD is equal or smaller than the target stream top item",
                    );
                }
                Ok(new_id)
            }
            (None, Some(_)) => Err("Invalid stream ID format"),
        }
    }

    /// Add an entry to the stream, returns the entry ID
    pub fn add_entry(
        &mut self,
        id: StreamEntryId,
        fields: Vec<(Vec<u8>, Vec<u8>)>,
    ) -> StreamEntryId {
        if self.first_entry_id.is_none() {
            self.first_entry_id = Some(id);
        }
        self.last_id = id;
        self.entries_added += 1;
        let entry = StreamEntry { id, fields };
        self.entries.insert(id, entry);
        id
    }

    /// Get entries in a range (inclusive)
    pub fn range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        for (_, entry) in self.entries.range(*start..=*end) {
            result.push(entry);
            if let Some(max) = count
                && result.len() >= max
            {
                break;
            }
        }
        result
    }

    /// Get entries in reverse range (inclusive)
    pub fn rev_range(
        &self,
        start: &StreamEntryId,
        end: &StreamEntryId,
        count: Option<usize>,
    ) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        for (_, entry) in self.entries.range(*end..=*start).rev() {
            result.push(entry);
            if let Some(max) = count
                && result.len() >= max
            {
                break;
            }
        }
        result
    }

    /// Trim the stream to a maximum length, removing oldest entries.
    /// Returns (count_removed, bytes_freed) for accurate memory accounting.
    pub fn trim_maxlen(&mut self, maxlen: usize, approximate: bool) -> (u64, i64) {
        if self.entries.len() <= maxlen {
            return (0, 0);
        }
        let to_remove = self.entries.len() - maxlen;
        // For approximate trimming, we trim at least ~10% less aggressively
        let actual_remove = if approximate {
            // Approximate: remove a bit less, but at least something
            to_remove
                .saturating_sub(to_remove / 10)
                .max(1)
                .min(to_remove)
        } else {
            to_remove
        };
        let mut removed = 0u64;
        let mut bytes_freed: i64 = 0;
        let ids_to_remove: Vec<StreamEntryId> =
            self.entries.keys().take(actual_remove).copied().collect();
        for id in ids_to_remove {
            if let Some(entry) = self.entries.remove(&id) {
                bytes_freed += entry_mem_size(&entry) as i64;
                if self
                    .max_deleted_entry_id
                    .as_ref()
                    .is_none_or(|max| id > *max)
                {
                    self.max_deleted_entry_id = Some(id);
                }
                removed += 1;
            }
        }
        (removed, -bytes_freed)
    }

    /// Trim entries with IDs less than the given minid.
    /// Returns (count_removed, bytes_freed) for accurate memory accounting.
    pub fn trim_minid(&mut self, minid: &StreamEntryId, approximate: bool) -> (u64, i64) {
        let ids_to_remove: Vec<StreamEntryId> = self
            .entries
            .range(..*minid)
            .map(|(k, _)| *k)
            .collect();
        if ids_to_remove.is_empty() {
            return (0, 0);
        }
        let to_remove = if approximate {
            ids_to_remove
                .len()
                .saturating_sub(ids_to_remove.len() / 10)
                .max(1)
        } else {
            ids_to_remove.len()
        };
        let mut removed = 0u64;
        let mut bytes_freed: i64 = 0;
        for id in ids_to_remove.into_iter().take(to_remove) {
            if let Some(entry) = self.entries.remove(&id) {
                bytes_freed += entry_mem_size(&entry) as i64;
                if self
                    .max_deleted_entry_id
                    .as_ref()
                    .is_none_or(|max| id > *max)
                {
                    self.max_deleted_entry_id = Some(id);
                }
                removed += 1;
            }
        }
        (removed, -bytes_freed)
    }

    /// Delete specific entries by ID.
    /// Returns (count_deleted, bytes_freed) for accurate memory accounting.
    pub fn delete_entries(&mut self, ids: &[StreamEntryId]) -> (u64, i64) {
        let mut deleted = 0u64;
        let mut bytes_freed: i64 = 0;
        for id in ids {
            if let Some(entry) = self.entries.remove(id) {
                bytes_freed += entry_mem_size(&entry) as i64;
                if self
                    .max_deleted_entry_id
                    .as_ref()
                    .is_none_or(|max| id > max)
                {
                    self.max_deleted_entry_id = Some(*id);
                }
                deleted += 1;
            }
        }
        (deleted, -bytes_freed)
    }

    /// Get entries after a given ID (exclusive)
    pub fn read_after(&self, after: &StreamEntryId, count: Option<usize>) -> Vec<&StreamEntry> {
        let mut result = Vec::new();
        // Use a range that starts just after `after`
        let start = if after.seq == u64::MAX {
            if after.ms == u64::MAX {
                return result; // No entries can exist after the maximum possible ID
            }
            StreamEntryId::new(after.ms + 1, 0)
        } else {
            StreamEntryId::new(after.ms, after.seq + 1)
        };

        for (_, entry) in self.entries.range(start..) {
            result.push(entry);
            if let Some(max) = count
                && result.len() >= max
            {
                break;
            }
        }
        result
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

impl Default for StreamData {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_entry_id_parse() {
        let id = StreamEntryId::parse("1526919030474-55").unwrap();
        assert_eq!(id.ms, 1526919030474);
        assert_eq!(id.seq, 55);

        let id = StreamEntryId::parse("1526919030474").unwrap();
        assert_eq!(id.ms, 1526919030474);
        assert_eq!(id.seq, 0);

        assert_eq!(StreamEntryId::parse("-").unwrap(), StreamEntryId::min());
        assert_eq!(StreamEntryId::parse("+").unwrap(), StreamEntryId::max());
        assert!(StreamEntryId::parse("*").is_none());
    }

    #[test]
    fn test_stream_entry_id_ordering() {
        let id1 = StreamEntryId::new(100, 0);
        let id2 = StreamEntryId::new(100, 1);
        let id3 = StreamEntryId::new(200, 0);
        assert!(id1 < id2);
        assert!(id2 < id3);
        assert!(id1 < id3);
    }

    #[test]
    fn test_stream_add_and_range() {
        let mut stream = StreamData::new();
        let id1 = StreamEntryId::new(1000, 0);
        let id2 = StreamEntryId::new(1000, 1);
        let id3 = StreamEntryId::new(2000, 0);

        stream.add_entry(id1, vec![(b"field1".to_vec(), b"val1".to_vec())]);
        stream.add_entry(id2, vec![(b"field2".to_vec(), b"val2".to_vec())]);
        stream.add_entry(id3, vec![(b"field3".to_vec(), b"val3".to_vec())]);

        assert_eq!(stream.len(), 3);
        assert_eq!(stream.entries_added, 3);

        let results = stream.range(&StreamEntryId::min(), &StreamEntryId::max(), None);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id, id1);
        assert_eq!(results[2].id, id3);

        // Range with count
        let results = stream.range(&StreamEntryId::min(), &StreamEntryId::max(), Some(2));
        assert_eq!(results.len(), 2);
    }

    #[test]
    fn test_stream_rev_range() {
        let mut stream = StreamData::new();
        stream.add_entry(
            StreamEntryId::new(1000, 0),
            vec![(b"a".to_vec(), b"1".to_vec())],
        );
        stream.add_entry(
            StreamEntryId::new(2000, 0),
            vec![(b"b".to_vec(), b"2".to_vec())],
        );
        stream.add_entry(
            StreamEntryId::new(3000, 0),
            vec![(b"c".to_vec(), b"3".to_vec())],
        );

        let results = stream.rev_range(&StreamEntryId::max(), &StreamEntryId::min(), None);
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].id.ms, 3000);
        assert_eq!(results[2].id.ms, 1000);
    }

    #[test]
    fn test_stream_trim_maxlen() {
        let mut stream = StreamData::new();
        for i in 0..10 {
            stream.add_entry(
                StreamEntryId::new(i * 1000, 0),
                vec![(b"k".to_vec(), b"v".to_vec())],
            );
        }
        assert_eq!(stream.len(), 10);

        let (removed, bytes_delta) = stream.trim_maxlen(5, false);
        assert_eq!(removed, 5);
        assert!(bytes_delta < 0, "trim should report negative byte delta");
        assert_eq!(stream.len(), 5);
    }

    #[test]
    fn test_stream_trim_minid() {
        let mut stream = StreamData::new();
        for i in 1..=10 {
            stream.add_entry(
                StreamEntryId::new(i * 1000, 0),
                vec![(b"k".to_vec(), b"v".to_vec())],
            );
        }

        let (removed, bytes_delta) = stream.trim_minid(&StreamEntryId::new(5000, 0), false);
        assert_eq!(removed, 4); // IDs 1000-4000 removed
        assert!(bytes_delta < 0, "trim should report negative byte delta");
        assert_eq!(stream.len(), 6);
    }

    #[test]
    fn test_stream_delete_entries() {
        let mut stream = StreamData::new();
        let id1 = StreamEntryId::new(1000, 0);
        let id2 = StreamEntryId::new(2000, 0);
        let id3 = StreamEntryId::new(3000, 0);
        stream.add_entry(id1, vec![]);
        stream.add_entry(id2, vec![]);
        stream.add_entry(id3, vec![]);

        let (deleted, bytes_delta) = stream.delete_entries(&[id2]);
        assert_eq!(deleted, 1);
        assert!(bytes_delta < 0 || deleted == 0, "delete should report non-positive byte delta");
        assert_eq!(stream.len(), 2);

        // Deleting non-existent ID
        let (deleted, bytes_delta) = stream.delete_entries(&[StreamEntryId::new(9999, 0)]);
        assert_eq!(deleted, 0);
        assert_eq!(bytes_delta, 0);
    }

    #[test]
    fn test_stream_read_after() {
        let mut stream = StreamData::new();
        let id1 = StreamEntryId::new(1000, 0);
        let id2 = StreamEntryId::new(2000, 0);
        let id3 = StreamEntryId::new(3000, 0);
        stream.add_entry(id1, vec![(b"a".to_vec(), b"1".to_vec())]);
        stream.add_entry(id2, vec![(b"b".to_vec(), b"2".to_vec())]);
        stream.add_entry(id3, vec![(b"c".to_vec(), b"3".to_vec())]);

        let results = stream.read_after(&id1, None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].id, id2);

        let results = stream.read_after(&StreamEntryId::new(0, 0), Some(1));
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, id1);
    }

    #[test]
    fn test_stream_generate_id_explicit() {
        let mut stream = StreamData::new();
        stream.add_entry(StreamEntryId::new(1000, 0), vec![]);

        // ID greater than last
        let id = stream.generate_id(Some(2000), Some(0)).unwrap();
        assert_eq!(id, StreamEntryId::new(2000, 0));

        // ID equal to last should fail
        let result = stream.generate_id(Some(1000), Some(0));
        assert!(result.is_err());
    }

    #[test]
    fn test_stream_generate_id_cached_explicit() {
        // generate_id_cached with explicit IDs should behave identically to generate_id
        let mut stream = StreamData::new();
        stream.add_entry(StreamEntryId::new(1000, 0), vec![]);

        // Explicit ms + seq greater than last
        let id = stream.generate_id_cached(Some(2000), Some(0)).unwrap();
        assert_eq!(id, StreamEntryId::new(2000, 0));

        // Equal to last should fail
        let result = stream.generate_id_cached(Some(1000), Some(0));
        assert!(result.is_err());

        // Explicit ms, auto seq
        let id = stream.generate_id_cached(Some(1000), None).unwrap();
        assert_eq!(id, StreamEntryId::new(1000, 1));

        // Invalid format
        let result = stream.generate_id_cached(None, Some(5));
        assert!(result.is_err());
    }

    #[test]
    fn test_stream_generate_id_cached_auto() {
        // Fully auto-generated IDs should produce valid IDs
        // Note: cached_now_ms() may return 0 without the clock updater running,
        // but the logic still works — it uses max(now_ms, last_id.ms)
        let mut stream = StreamData::new();

        // First auto-ID on empty stream
        let id = stream.generate_id_cached(None, None).unwrap();

        // Add this entry and generate another — should increment seq or advance ms
        stream.add_entry(id, vec![]);
        let id2 = stream.generate_id_cached(None, None).unwrap();
        assert!(id2 > id, "second auto-ID should be greater than first");

        // Third auto-ID should also be strictly greater
        stream.add_entry(id2, vec![]);
        let id3 = stream.generate_id_cached(None, None).unwrap();
        assert!(id3 > id2, "third auto-ID should be greater than second");
    }

    #[test]
    fn test_consumer_group() {
        let mut group = ConsumerGroup::new("mygroup".to_string(), StreamEntryId::new(0, 0));
        let consumer = group.get_or_create_consumer("consumer1");
        assert_eq!(consumer.name, "consumer1");
        assert!(consumer.pending.is_empty());

        // Getting again should return the same consumer
        let consumer = group.get_or_create_consumer("consumer1");
        assert_eq!(consumer.name, "consumer1");
    }

    #[test]
    fn test_stream_entry_id_is_copy() {
        // Verify that StreamEntryId implements Copy — this is a compile-time check
        let id = StreamEntryId::new(42, 7);
        let id2 = id; // Copy, not move
        assert_eq!(id, id2); // id is still usable
    }
}
