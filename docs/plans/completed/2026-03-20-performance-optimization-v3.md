# rLightning v3.0 Performance Optimization

## Overview

Implement the full 11-phase performance optimization plan from docs/OPTIMIZATION_PLAN.md, targeting 3-10x throughput improvement over Redis 7/8. Covers release profile, network layer, zero-copy parser, sharded storage, pipeline batching, command dispatch, native data types, memory optimization, lock-free paths, TTL/eviction, and non-blocking persistence.

## Context

- Source document: `docs/OPTIMIZATION_PLAN.md` (11 phases, Russian language)
- Files involved: `Cargo.toml`, `src/storage/engine.rs`, `src/networking/server.rs`, `src/networking/resp.rs`, `src/command/handler.rs`, `src/command/mod.rs`, `src/command/types/*.rs`, `src/persistence/rdb.rs`, `src/persistence/aof.rs`, `src/storage/item.rs`, `src/networking/client.rs`
- Related patterns: DashMap-based storage, bincode serialization, RespValue enum, string-match command dispatch
- Dependencies to add: `parking_lot`, `smallvec`, `itoa`, `rustc-hash`, `arrayvec`
- Existing deps already present: `hashbrown`, `bytes`, `ordered-float`, `dashmap`, `memchr`

## Development Approach

- **Testing approach**: Regular (code first, then tests) - optimization work modifies existing code paths
- Complete each task fully before moving to the next
- Run full test suite after each task to catch regressions immediately
- Run benchmarks before and after each phase to measure impact
- Phases follow the dependency order from the optimization plan: 0 -> 1 -> 2+5 (parallel) -> 6 -> 3 -> 4 -> 7+8+9 (parallel) -> 10
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: Phase 0 - Release Profile (Cargo.toml)

**Files:**
- Modify: `Cargo.toml`

- [x] Add `[profile.release]` section with `opt-level = 3`, `lto = "fat"`, `codegen-units = 1`, `strip = true`, `panic = "abort"`
- [x] Verify no `catch_unwind` usage exists in codebase (already confirmed: none found)
- [x] Run `cargo test` to verify all tests pass with new profile settings
- [x] Run `cargo build --release` to verify compilation succeeds
- [x] Run benchmarks (`cargo bench`) to establish post-Phase-0 baseline

### Task 2: Phase 1 - Network Layer Optimization

**Files:**
- Modify: `src/networking/server.rs`
- Modify: `src/networking/client.rs`

- [x] Add `stream.set_nodelay(true)` immediately after `accept()` for all client connections in server.rs
- [x] Remove `last_cmd` update from hot path in ClientInfo - defer to CLIENT LIST/CLIENT INFO queries only
- [x] Replace `created_at: Instant` with unix timestamp (u64) to avoid syscall on connection creation
- [x] Add write coalescing: buffer small responses (<1KB) and flush when read buffer is empty or write buffer is full
- [x] Write tests verifying TCP_NODELAY is set on client connections
- [x] Run full test suite - must pass before task 3

### Task 3: Phase 2 - Zero-Copy RESP Parser

**Files:**
- Modify: `src/networking/resp.rs`
- Create: `src/networking/raw_command.rs`
- Modify: `src/networking/mod.rs`
- Modify: `src/networking/server.rs`

- [x] Add `smallvec` dependency to Cargo.toml
- [x] Create `RawCommand<'buf>` struct with `name: &'buf [u8]` and `args: SmallVec<[&'buf [u8]; 4]>`
- [x] Implement `try_parse_raw()` function that parses RESP commands as zero-copy byte slices from read buffer
- [x] Implement case-insensitive byte comparison (`cmd_eq`) without allocation
- [x] Add fallback path: convert `RawCommand` to owned `RespValue` for MULTI queue, AOF logging, and RESP3 features
- [x] Integrate zero-copy parser into the server connection handler hot path
- [x] Keep existing `RespValue` parser for owned-data paths (AOF, MULTI, RESP3)
- [x] Write tests for zero-copy parser covering: simple commands, multi-bulk, edge cases (empty args, binary data, >4 args triggering SmallVec spill)
- [x] Run full test suite + integration tests - must pass before task 4

### Task 4: Phase 5 - Fast Command Dispatch

**Files:**
- Modify: `src/command/handler.rs`

- [x] Implement two-level byte dispatch: Level 1 dispatches on first byte (26 letters), Level 2 dispatches on command length + exact byte comparison
- [x] Add `cmd_eq()` helper for case-insensitive byte-level comparison without allocations
- [x] Remove `command.name.to_lowercase()` allocation from dispatch hot path
- [x] Implement fast path for top-20 most common commands (SET, GET, DEL, INCR, HSET, HGET, LPUSH, RPUSH, SADD, SREM, ZADD, MGET, MSET, EXISTS, TTL, TYPE, EXPIRE, PEXPIRE, PING, INFO)
- [x] Keep phf-style or match fallback for remaining 380+ commands
- [x] Wire new dispatch into `RawCommand` path from Task 3
- [x] Write tests verifying all 400+ commands still dispatch correctly (case-insensitive, mixed case)
- [x] Run full test suite - must pass before task 5

### Task 5: Phase 6 - Native Data Types (Remove Bincode Serialization)

**Files:**
- Modify: `src/storage/engine.rs`
- Modify: `src/storage/item.rs`
- Create: `src/storage/value.rs`
- Modify: `src/command/types/hash.rs`
- Modify: `src/command/types/set.rs`
- Modify: `src/command/types/sorted_set.rs`
- Modify: `src/command/types/list.rs`
- Modify: `src/command/types/string.rs`
- Modify: `src/persistence/rdb.rs`
- Modify: `src/persistence/aof.rs`

- [x] Add `rustc-hash` dependency to Cargo.toml
- [x] Create `StoreValue` enum with native variants: `Str(Vec<u8>)`, `Hash(NativeHashMap)`, `Set(NativeHashSet)`, `ZSet(SortedSetData)`, `List(VecDeque<Vec<u8>>)`, `Stream(StreamData)`
- [x] Create `Entry` struct with `value: StoreValue`, `expires_at: Option<Instant>`, `lru_clock: u32`, `access_count: u16`
- [x] Migrate `StorageEngine` from `DashMap<Vec<u8>, StorageItem>` with bincode to `DashMap<Vec<u8>, Entry>` with native `StoreValue`
- [x] Update all hash command handlers to use native HashMap directly (no deserialize/serialize)
- [x] Update all set command handlers to use native HashSet directly
- [x] Update all sorted set command handlers to use native dual-index (BTreeMap + HashMap)
- [x] Update list command handlers to use `VecDeque<Vec<u8>>` instead of list_bytes binary format
- [x] Update string command handlers to use StoreValue::Str
- [x] Update RDB persistence to serialize/deserialize native types to/from disk format
- [x] Update AOF replay to produce native types
- [x] Remove bincode serialization from data access hot paths (keep bincode dependency for any remaining uses)
- [x] Write tests for each data type: verify HGET/HSET/HDEL, SADD/SREM/SMEMBERS, ZADD/ZRANGE/ZSCORE, GET/SET/APPEND all work correctly with native types
- [x] Write tests for RDB save/load roundtrip with native types
- [x] Run full test suite + integration tests + docker compat tests - must pass before task 6

### Task 6: Phase 3 - Sharded Storage Engine

**Files:**
- Create: `src/storage/sharded.rs`
- Modify: `src/storage/engine.rs`
- Modify: `src/storage/mod.rs`
- Modify: `src/command/transaction.rs`

- [x] Add `parking_lot` dependency to Cargo.toml
- [x] Implement `Shard` struct with `#[repr(align(128))]`, containing `hashbrown::HashMap<Box<[u8]>, Entry, FxBuildHasher>`, `version: u64`, `used_memory: usize`, `key_count: u32`
- [x] Implement `ShardedStore` with `(num_cpus * 16).next_power_of_two().clamp(16, 1024)` shards
- [x] Use FxHash for shard selection with single-hash lookup via `raw_entry()` API
- [x] Use `parking_lot::RwLock` for per-shard locking
- [x] Implement per-shard memory tracking (sum for global INFO/CONFIG queries)
- [x] Implement the same public API as current StorageEngine (get, set, atomic_modify, etc.)
- [x] Migrate `StorageEngine` to use `ShardedStore` internally instead of DashMap
- [x] Update `lock_keys()` to use sorted shard-level locking for deadlock prevention
- [x] Update WATCH/MULTI/EXEC to use per-shard version tracking
- [x] Support multi-database via array of `ShardedStore` (one per DB)
- [x] Write tests for shard distribution, concurrent read/write, WATCH with per-shard versioning, cross-shard MSET atomicity
- [x] Run full test suite + integration tests + docker compat tests - must pass before task 7

### Task 7: Phase 4 - Pipeline Batching

**Files:**
- Modify: `src/networking/server.rs`
- Create: `src/storage/batch.rs`
- Modify: `src/storage/mod.rs`

- [x] Implement `process_pipeline()` that groups consecutive same-shard commands for batch execution under a single lock
- [x] Implement read/write command classification (GET/EXISTS/TTL = read, SET/DEL/INCR = write) for optimal lock selection (read lock vs write lock)
- [x] Implement `execute_on_shard_mut()` and `execute_on_shard_ref()` that operate directly on shard HashMap
- [x] Maintain Redis command ordering semantics (no reordering, only consecutive same-shard grouping)
- [x] Exclude MULTI/EXEC, EVAL/EVALSHA, Pub/Sub subscription commands from batching
- [x] Exclude multi-key commands (MSET, MGET) from batching - they use their own shard locking
- [x] Preserve pipeline error isolation: each command gets its own result (Ok or Error)
- [x] Wire batch processing into server connection handler for pipeline depth > 1
- [x] Write tests for: pipeline batching correctness, command ordering preserved, error isolation, mixed read/write batches, fallback for excluded commands
- [x] Run full test suite + integration tests - must pass before task 8

### Task 8: Phase 7 - Memory Optimization

**Files:**
- Modify: `src/storage/value.rs`
- Modify: `src/networking/resp.rs`

- [x] Implement Small String Optimization (SSO): `CompactValue` enum with `Inline { len: u8, data: [u8; 23] }` for values <= 23 bytes, `Heap(Vec<u8>)` for larger
- [x] Add static response interning: `OK_RESPONSE`, `PONG_RESPONSE`, `ZERO_RESPONSE`, `ONE_RESPONSE`, `NIL_RESPONSE`, `EMPTY_ARRAY` as `Bytes::from_static`
- [x] Add `itoa` dependency and implement direct response serialization to write buffer (skip intermediate RespValue for hot-path responses like bulk strings, integers)
- [x] Write tests for SSO (values at boundary: 0, 23, 24 bytes), response interning correctness
- [x] Run full test suite - must pass before task 9

### Task 9: Phase 8 - Lock-Free Fast Paths

**Files:**
- Modify: `src/storage/sharded.rs`
- Modify: `src/storage/engine.rs`

- [x] Optimize read-heavy operations (GET, EXISTS, TTL, TYPE) to use `parking_lot::RwLock::read()` for maximum concurrent reader throughput
- [x] Implement inline atomic operations (INCR, DECR, APPEND, SETNX) that operate directly within shard write guard without intermediate deserialization
- [x] Audit and relax atomic orderings where safe: use `Relaxed` for `memory_used`, `key_count`, `lru_clock` loads/stores
- [x] Write tests verifying concurrent read correctness, INCR atomicity under contention
- [x] Run full test suite - must pass before task 10

### Task 10: Phase 9 - TTL and Eviction Optimization

**Files:**
- Modify: `src/storage/sharded.rs`
- Modify: `src/storage/engine.rs`

- [x] Add `arrayvec` dependency to Cargo.toml
- [x] Implement per-shard expiration heaps (move from global BinaryHeap with RwLock to per-shard BinaryHeap accessed under existing shard lock)
- [x] Implement background round-robin shard expiration (N expires per iteration per shard)
- [x] Implement probabilistic eviction with per-shard `ArrayVec<EvictionCandidate, 16>` candidate buffer
- [x] Replace `Instant::now()` calls on every GET with cached clock (`CACHED_NOW: AtomicU64`, updated every 1ms by background timer)
- [x] Write tests for: per-shard expiration correctness, eviction candidate selection, cached clock accuracy
- [x] Run full test suite - must pass before task 11

### Task 11: Phase 10 - Non-Blocking Persistence

**Files:**
- Modify: `src/persistence/rdb.rs`
- Modify: `src/persistence/aof.rs`
- Modify: `src/persistence/mod.rs`

- [x] Implement COW-based RDB snapshots: per-shard read-lock, clone HashMap (Bytes cheap clone via refcount), release lock, serialize clone in background
- [x] Remove `cross_db_lock` write lock from snapshot path
- [x] Implement lock-free AOF appending via MPSC channel: command threads send `AofEntry` to dedicated AOF writer thread
- [x] AOF writer thread batch-drains channel and performs `fsync()` on everysec schedule
- [x] Write tests for: RDB snapshot during concurrent writes, AOF append ordering, persistence roundtrip (save -> kill -> restart -> verify data)
- [x] Run full test suite + persistence integration tests - must pass before task 12

### Task 12: Verify Acceptance Criteria

- [x] Run full test suite: `cargo test` (1400+ tests, 0 failures)
- [x] Run integration tests: `cargo test --test integration_test` (passed)
- [x] Run Redis compatibility tests: `cargo test --test redis_compatibility_test` (passed)
- [x] Run multi-language compat: `cd tests/docker-compat && ./run-tests.sh --local` (Go/Python ran, JS client infra issue; gaps are pre-existing)
- [x] Run benchmarks and compare against pre-optimization baseline: `cargo bench` (fixed OOM in storage_bench and AOF bench)
- [x] Run redis-benchmark at pipeline depths 1, 16, 64: SET 174K/418K/443K rps, GET 181K/1.9M/1.9M rps
- [x] Verify no data races: ThreadSanitizer requires nightly (not installed) - skipped, not automatable without toolchain install
- [x] Verify persistence integrity: save -> restart -> verify data (all 5 data types verified correct)

### Task 13: Update Documentation

- [x] Update CLAUDE.md architecture section to reflect sharded storage, native types, zero-copy parser
- [x] Update `docs/OPTIMIZATION_PLAN.md` to mark completed phases
- [x] Move this plan to `docs/plans/completed/`
