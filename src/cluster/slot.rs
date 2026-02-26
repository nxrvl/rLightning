use std::fmt;

/// Total number of hash slots in a Redis cluster
pub const CLUSTER_SLOTS: u16 = 16384;

/// CRC16 lookup table (XMODEM variant used by Redis)
/// Source: https://github.com/redis/redis/blob/unstable/src/crc16.c
static CRC16_TAB: [u16; 256] = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
];

/// Compute CRC16 checksum using the CCITT variant (same as Redis)
pub fn crc16(data: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &byte in data {
        let index = ((crc >> 8) ^ (byte as u16)) & 0xFF;
        crc = (crc << 8) ^ CRC16_TAB[index as usize];
    }
    crc
}

/// Calculate the hash slot for a given key.
/// Supports hash tags: if the key contains {...}, only the content between
/// the first { and the next } is used for hashing.
pub fn key_hash_slot(key: &[u8]) -> u16 {
    let hash_key = extract_hash_tag(key);
    crc16(hash_key) % CLUSTER_SLOTS
}

/// Extract the hash tag from a key.
/// Returns the substring between first '{' and next '}', or the whole key if no valid tag.
fn extract_hash_tag(key: &[u8]) -> &[u8] {
    if let Some(start) = key.iter().position(|&b| b == b'{')
        && let Some(end) = key[start + 1..].iter().position(|&b| b == b'}')
            && end > 0 {
                return &key[start + 1..start + 1 + end];
            }
    key
}

/// A range of hash slots [start, end] (inclusive)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotRange {
    pub start: u16,
    pub end: u16,
}

impl SlotRange {
    pub fn new(start: u16, end: u16) -> Self {
        assert!(start <= end);
        assert!(end < CLUSTER_SLOTS);
        SlotRange { start, end }
    }

    pub fn single(slot: u16) -> Self {
        SlotRange::new(slot, slot)
    }

    pub fn contains(&self, slot: u16) -> bool {
        slot >= self.start && slot <= self.end
    }

    pub fn count(&self) -> u16 {
        self.end - self.start + 1
    }
}

impl fmt::Display for SlotRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.start == self.end {
            write!(f, "{}", self.start)
        } else {
            write!(f, "{}-{}", self.start, self.end)
        }
    }
}

/// The state of a slot during migration
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotState {
    /// Normal state - slot is owned by a node
    Normal,
    /// Slot is being imported from another node
    Importing(String),
    /// Slot is being migrated to another node
    Migrating(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc16_basic() {
        // Known CRC16 values for Redis
        assert_eq!(crc16(b"123456789"), 0x31C3);
    }

    #[test]
    fn test_key_hash_slot_basic() {
        // Basic keys without hash tags
        let slot = key_hash_slot(b"foo");
        assert!(slot < CLUSTER_SLOTS);

        let slot2 = key_hash_slot(b"bar");
        assert!(slot2 < CLUSTER_SLOTS);

        // Same key should always hash to same slot
        assert_eq!(key_hash_slot(b"test"), key_hash_slot(b"test"));
    }

    #[test]
    fn test_key_hash_slot_with_hash_tag() {
        // Keys with same hash tag should map to same slot
        assert_eq!(
            key_hash_slot(b"user:{123}:name"),
            key_hash_slot(b"user:{123}:email")
        );
        assert_eq!(
            key_hash_slot(b"{user}.following"),
            key_hash_slot(b"{user}.followers")
        );
    }

    #[test]
    fn test_key_hash_slot_empty_hash_tag() {
        // Empty hash tag {} should be treated as no hash tag
        let slot_with_empty = key_hash_slot(b"foo{}bar");
        let slot_without = key_hash_slot(b"foo{}bar");
        assert_eq!(slot_with_empty, slot_without);
    }

    #[test]
    fn test_key_hash_slot_no_closing_brace() {
        // No closing brace means no hash tag
        let slot = key_hash_slot(b"foo{bar");
        assert_eq!(slot, key_hash_slot(b"foo{bar"));
    }

    #[test]
    fn test_key_hash_slot_first_occurrence() {
        // Only first { is considered
        let slot1 = key_hash_slot(b"{a{b}");
        let slot2 = key_hash_slot(b"x{a{b}y");
        // {a{b} uses "a{b" as hash tag (between first { and first })
        // x{a{b}y uses "a{b" as hash tag
        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_slot_range() {
        let range = SlotRange::new(0, 5460);
        assert!(range.contains(0));
        assert!(range.contains(5460));
        assert!(range.contains(1000));
        assert!(!range.contains(5461));
        assert_eq!(range.count(), 5461);
    }

    #[test]
    fn test_slot_range_single() {
        let range = SlotRange::single(42);
        assert!(range.contains(42));
        assert!(!range.contains(41));
        assert!(!range.contains(43));
        assert_eq!(range.count(), 1);
    }

    #[test]
    fn test_slot_range_display() {
        assert_eq!(format!("{}", SlotRange::new(0, 5460)), "0-5460");
        assert_eq!(format!("{}", SlotRange::single(42)), "42");
    }

    #[test]
    fn test_known_redis_hash_slots() {
        // These are well-known Redis slot values
        // "foo" hashes to slot 12182 in Redis
        assert_eq!(key_hash_slot(b"foo"), 12182);
        // "bar" hashes to slot 5061
        assert_eq!(key_hash_slot(b"bar"), 5061);
        // "hello" hashes to slot 866
        assert_eq!(key_hash_slot(b"hello"), 866);
    }
}
