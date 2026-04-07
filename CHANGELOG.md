# Changelog

All notable changes to rLightning will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.2.0] - 2026-04-07

### Added

- **Zero-copy RESP parser** — `RawCommand` parses commands as byte slices directly from the read buffer, avoiding heap allocations on the hot path
- **Sharded storage engine** — cache-line aligned shards with `parking_lot::RwLock`, replacing DashMap; shard count auto-tuned to CPU count
- **Pipeline batching** — consecutive same-shard commands grouped under single lock acquisition per shard, with batched AOF logging, replication propagation, and prefix index updates
- **Two-level byte dispatch** — first-byte + length routing eliminates `to_lowercase()` allocation; fast path for top-20 commands (SET, GET, DEL, INCR, HSET, etc.)
- **Small String Optimization** — `CompactValue` with inline storage for values ≤ 23 bytes, avoiding heap allocation for short strings
- **O(1) memory tracking** — `cached_mem_size` field on entries with byte deltas from mutation closures, eliminating O(n) size recalculation on writes
- **Per-shard expiration heaps** — round-robin background expiration with cached monotonic clock (~1ms resolution)
- **Probabilistic eviction** — per-shard `ArrayVec<EvictionCandidate, 16>` candidate buffer for memory-efficient sampling
- **COW-based RDB snapshots** — per-shard read-lock, clone HashMap, release lock, serialize in background (no global write lock)
- **Lock-free AOF appending** — MPSC channel to dedicated AOF writer thread with batch-drain and `fsync()` on everysec schedule
- **MSET single-shard fast path** — avoids cross-shard locking when all keys hash to the same shard
- **XADD/XDEL/XTRIM in-place mutation** — `ModifyResult::Keep(delta)` for stream operations without deep-clone
- **Conditional key versioning** — `active_watch_count` tracks WATCH sessions; version bumps skipped when no clients are watching
- **Static response interning** — OK, PONG, NIL, 0, 1, and empty array responses use `Bytes::from_static`
- **Buffer memory management** — response and partial command buffers shrink when capacity exceeds 64KB, preventing unbounded growth

### Changed

- **Default `maxmemory` changed to 0 (unlimited)** — matches Redis default behavior; previously defaulted to 128MB which caused silent evictions
- Storage engine replaced DashMap with sharded `parking_lot::RwLock` for better cache-line alignment and read concurrency
- Read-optimized locking: GET, EXISTS, TTL, TYPE use `RwLock::read()` for maximum concurrent reader throughput
- Write coalescing: small responses buffered and flushed when read buffer is empty or write buffer is full
- TCP_NODELAY enabled on all client connections immediately after accept
- `Entry::expire()` now returns the computed `Instant` to eliminate double `cached_now()` calls in SET paths
- New LFU entries initialized with `access_count = 5` (warm-up) to prevent immediate eviction of freshly-inserted keys

### Fixed

- **`is_expired()` consistency** — changed from strict `>` to `>=` to match `expire_shard`/`lazy_expire` which use `<=`, closing a 1μs window where background expiry could remove a key that `is_expired()` considered alive
- **EXAT/PXAT with past timestamps** — now returns `"invalid expire time in 'set' command"` error instead of silently setting a 1ms TTL (matches Redis behavior)
- **SET with EX 0 or negative** — now returns error instead of silently creating a persistent key (matches Redis behavior)
- **LFU eviction of new keys** — new entries no longer start at `access_count = 0` (highest eviction priority); initialized to 5 to survive initial sampling
- **Double `cached_now()` in TTL paths** — eliminated 1ms inconsistency between item and expiration heap timestamps
- **RESP3 pipeline batch response conversion** — added missing RESP3 conversion in pipeline batch path
- **Memory leaks** — fixed unbounded growth in `key_versions` map and network buffers
- **Cluster routing** — MOVED/ASK redirections in command dispatch, gossip protocol activation on startup

### Performance

- **20-48% throughput improvement** across all benchmarks vs v2.1.0
- Zero-allocation hot path for top-20 commands
- Lock-free reads for GET/EXISTS/TTL/TYPE
- Batch processing reduces per-command lock overhead in pipelines

## [2.1.0] - 2026-03-11

### Added

- **Gap buffer for list operations** — V1 format with left-gap buffer for lists exceeding 128 elements, enabling O(1) amortized LPUSH and O(1) LPOP on large lists
- 19 new unit tests for V1 gap buffer: migration, compaction, mixed operations, edge cases

### Changed

- **Bincode cleanup** — all raw `bincode::serialize`/`deserialize` for lists replaced with `list_bytes` API (production code in `engine.rs` RPUSH and `key.rs` SORT STORE)
- List operations now use version-aware format: V0 (flat sequential) for small lists, V1 (gap buffer) for lists > 128 elements
- RPOP uses cached `tail_offset` for O(1) single-element pop after RPUSH

### Performance

- **LPUSH**: 0.51x → 1.13x Redis 7 (122% improvement, now beats Redis)
- **LPOP**: 0.32x → 1.01x Redis 7 (216% improvement, matches Redis)
- **RPOP**: 0.28x → 0.95x Redis 7 (239% improvement, matches Redis)
- No regressions on RPUSH or LRANGE

## [2.0.0] - 2026-03-10

### Added

- **RESP3 protocol support** with HELLO command negotiation and per-connection protocol version tracking
- **400+ Redis 7.x commands** across all data types:
  - Bitmap: SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD
  - HyperLogLog: PFADD, PFCOUNT, PFMERGE
  - Geospatial: GEOADD, GEODIST, GEOSEARCH, GEOSEARCHSTORE, GEOPOS, GEOHASH
  - Streams: XADD, XREAD, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO, XDEL, XTRIM
  - Missing string commands: GETEX, GETDEL, LCS, PSETEX, SETRANGE, SUBSTR
  - Missing list commands: LMOVE, LMPOP, LPOS, LINSERT, BLMOVE, BLMPOP
  - Missing set/hash commands: SINTERCARD, SMISMEMBER, HRANDFIELD
  - Missing sorted set commands: ZRANGESTORE, ZMPOP, BZMPOP, ZUNION, ZDIFF, ZRANDMEMBER
  - Key management: COPY, OBJECT ENCODING, DUMP, RESTORE, SORT, EXPIRETIME, PEXPIRETIME
- **Transaction support**: MULTI/EXEC/DISCARD/WATCH/UNWATCH with multi-key locking and sorted-order deadlock prevention
- **Lua scripting**: EVAL/EVALSHA with redis.call()/pcall() bindings, SCRIPT LOAD/EXISTS/FLUSH, Redis 7.0 Functions (FUNCTION LOAD/FCALL)
- **ACL security system**: per-user command/key/channel permissions, AUTH with username, ACL LOG, ACL GENPASS
- **Cluster mode**: CRC16 hash slot sharding, gossip protocol, MOVED/ASK redirections, slot migration, automatic failover
- **Sentinel HA**: master monitoring with quorum-based SDOWN/ODOWN detection, automatic failover, leader election
- **Sharded pub/sub**: SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH (Redis 7.0+)
- **Multi-database support**: SELECT command with 16 databases, task-local routing
- **Atomic storage engine**: lock-free DashMap with atomic read-modify-write primitives, cross-key atomicity via sorted-order locking
- **Full RDB + AOF persistence**: all data types across all 16 databases, AOF rewrite with stream consumer groups, hybrid mode
- **Replication improvements**: SELECT-aware command propagation, MULTI/EXEC transaction atomicity on replicas
- **RESP3 response types**: Map for HGETALL/CONFIG/XINFO/HSCAN/ZSCAN/SSCAN, Set for SMEMBERS, Double for float commands
- **Pipeline error isolation**: per-command errors captured as RespValue::Error instead of killing entire pipeline
- **CONFIG SET/GET**: runtime configuration store with glob pattern matching
- **Connection tracking**: CLIENT LIST/INFO/ID/SETNAME with real connection data
- **Module system stubs**: MODULE LIST/LOAD/UNLOAD for client compatibility
- **Comprehensive benchmark suite**: storage, protocol, throughput, replication, auth, cluster, advanced features
- **Multi-language compatibility tests**: Go (go-redis/v9), JavaScript (ioredis), Python (redis-py) — ~231 tests across 25 categories each
- **Documentation site**: Astro-based static site with full command reference, architecture docs, benchmarks

### Changed

- Hash storage redesigned from flat composite keys to single-key HashMap (bincode-serialized)
- String commands migrated to atomic storage primitives
- Collection commands (list, set, geo) migrated to atomic_modify for thread safety
- AOF replay now routes through full CommandHandler instead of a subset
- GEOHASH accuracy fixed by using standard latitude range for string encoding
- Periodic cleanup of stale key_locks/key_versions entries (every 60 seconds)
- CI/CD: all workflows manual-only (workflow_dispatch), release triggered by tag push

### Fixed

- TOCTOU race in set_with_type_preserve_ttl resolved with atomic DashMap entry operation
- AOF partial batch write corruption fixed with atomic memory staging
- GEORADIUS STORE destination key extraction for transaction locking and cluster slots
- Replication client no longer hardcoded to db_index=0
- CLIENT LIST sub/psub counts now reflect real subscription state
- OBJECT ENCODING returns size-based encoding strings matching Redis 7.x
- HELLO SETNAME wired to connection tracker

## [1.1.0] - 2026-01-15

### Added

- **Pub/Sub messaging**: SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PSUBSCRIBE, PUNSUBSCRIBE
- Pattern-based subscriptions with glob matching
- Real-time message broadcasting to all subscribers

## [1.0.6] - 2025-12-20

### Added

- Redis-compatible JSON path extraction for JSON.GET
- Improved Set operations compatibility

### Fixed

- Docker image name corrected to nxrvl/rlightning
- Docker push and Telegram notification improvements

## [1.0.5] - 2025-12-18

### Fixed

- RESP parser for large JSON values
- Improved Telegram CI notifications

## [1.0.4] - 2025-12-14

### Fixed

- **Critical**: Fixed RESP protocol buffer corruption bug that caused SET operations to fail with values larger than ~9-20KB
  - Root cause: `parse_bulk_string()` was modifying the buffer before verifying all data was available
  - Fix ensures buffer is only modified after verifying complete command is available
- Type mismatch errors on GET operations after failed large SET commands
- Base64/JSON encoded data now handled correctly regardless of size

### Added

- Comprehensive test suite for large values (up to 512MB)
- Tests for exact boundary conditions and chunked TCP packet handling

## [1.0.3] - 2025-10-10

### Fixed

- Base64 data protocol handling
- Prevent infinite error loops and add B64 data acceptance tests
- Clear buffers after protocol errors and improve error messaging

## [1.0.2] - 2025-10-09

### Fixed

- Protocol error handling to avoid infinite loops
- Handle malformed RESP data by clearing buffer and breaking connection

## [1.0.1] - 2025-10-05

### Added

- Initial release with Redis compatibility
- Support for strings, hashes, lists, sets, sorted sets
- TTL and expiration support
- RDB and AOF persistence
- Master-replica replication
- Authentication support
- Configurable eviction policies (LRU, Random, NoEviction)

[2.2.0]: https://github.com/nxrvl/rLightning/compare/v2.1.0...v2.2.0
[2.1.0]: https://github.com/nxrvl/rLightning/compare/v2.0.0...v2.1.0
[2.0.0]: https://github.com/nxrvl/rLightning/compare/v1.1.0...v2.0.0
[1.1.0]: https://github.com/nxrvl/rLightning/compare/v1.0.6...v1.1.0
[1.0.6]: https://github.com/nxrvl/rLightning/compare/v1.0.5...v1.0.6
[1.0.5]: https://github.com/nxrvl/rLightning/compare/v1.0.4...v1.0.5
[1.0.4]: https://github.com/nxrvl/rLightning/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/nxrvl/rLightning/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/nxrvl/rLightning/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/nxrvl/rLightning/releases/tag/v1.0.1
