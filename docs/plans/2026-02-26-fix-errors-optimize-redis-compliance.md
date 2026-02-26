# Fix All Errors and Optimize rLightning for Full Redis Protocol Compliance

## Overview
Systematically fix all identified correctness bugs, race conditions, incomplete features, and code quality issues in rLightning while preserving full Redis protocol compatibility with 3rd party libraries. Issues span the storage engine, command handlers, protocol layer, persistence, security, and code organization.

## Context
- Files involved: src/storage/engine.rs, src/command/types/*.rs, src/command/handler.rs, src/command/transaction.rs, src/networking/server.rs, src/networking/resp.rs, src/persistence/aof.rs, src/persistence/rdb.rs, src/security/acl.rs, src/pubsub/manager.rs, src/sentinel/mod.rs, src/cluster/mod.rs, src/command/types/connection.rs, src/main.rs
- Related patterns: DashMap entry() API for atomic operations, bincode serialization for collections, RESP2/RESP3 wire formats
- Dependencies: dashmap, bincode, tokio, mlua
- 61 compiler warnings confirmed (mostly dead_code from unwired modules)

## Development Approach
- **Testing approach**: TDD where feasible (write test for bug, then fix), regular for refactors
- Complete each task fully before moving to the next
- Tasks are ordered by dependency: foundation first, then consumers
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: Storage Engine Atomic Primitives

**Files:**
- Modify: `src/storage/engine.rs`

This is the foundation that enables fixing race conditions across all command types. Add atomic read-modify-write methods to StorageEngine using DashMap's entry() API.

- [x] Add `set_nx(key, value, ttl) -> bool` method that atomically sets only if key doesn't exist (uses `entry().or_insert()`)
- [x] Add `set_xx(key, value, ttl) -> bool` method that atomically sets only if key exists (uses `entry().and_modify()`)
- [x] Add `set_with_options(key, value, ttl, nx, xx, get_old) -> SetResult` unified method for SET with all flag combinations
- [x] Add `atomic_incr(key, delta: i64) -> Result<i64>` that reads, parses, increments, writes within single DashMap lock
- [x] Add `atomic_incr_float(key, delta: f64) -> Result<f64>` for INCRBYFLOAT
- [x] Add `atomic_append(key, value) -> Result<usize>` for APPEND
- [x] Add `atomic_modify<F>(key, data_type, f: F)` generic closure-based method for collection read-modify-write within DashMap entry lock (closure receives Option<&mut Vec<u8>> and returns new value)
- [x] Add `atomic_getdel(key) -> Option<Vec<u8>>` and `atomic_getset(key, value) -> Option<Vec<u8>>`
- [x] Fix memory counter atomic ordering: use `Ordering::AcqRel` for fetch_add/fetch_sub and `Ordering::Acquire` for loads (replace Relaxed where memory accounting accuracy matters)
- [x] Write unit tests for each atomic method verifying correctness under concurrent access (use tokio::spawn with multiple tasks racing)
- [x] Run project test suite - must pass before task 2

### Task 2: Hash Storage Redesign

**Files:**
- Modify: `src/storage/engine.rs` (add hash-specific atomic methods)
- Modify: `src/command/types/hash.rs` (rewrite all hash commands)
- Modify: `src/command/types/key.rs` (fix DEL, RENAME, TYPE, KEYS, SCAN for hashes)
- Modify: `src/storage/types/hash.rs` (repurpose or remove unused HashItem)

Migrate from flat composite `key:field` keys to single-key HashMap storage, fixing namespace collisions and O(N) HGETALL.

- [ ] Define hash storage format: bincode-serialized `HashMap<Vec<u8>, Vec<u8>>` stored as single key with `RedisDataType::Hash`
- [ ] Add hash-specific storage methods: `hash_set(key, fields)`, `hash_get(key, field)`, `hash_del(key, fields)`, `hash_getall(key)`, `hash_exists(key, field)`, `hash_len(key)` - all using `atomic_modify` for thread safety
- [ ] Rewrite HSET/HSETNX/HMSET to use new hash storage
- [ ] Rewrite HGET/HMGET/HGETALL/HKEYS/HVALS/HLEN to use new hash storage
- [ ] Rewrite HDEL/HEXISTS/HRANDFIELD/HSCAN to use new hash storage
- [ ] Rewrite HINCRBY/HINCRBYFLOAT to use atomic hash modification
- [ ] Verify DEL, EXPIRE, TTL, RENAME, TYPE, OBJECT ENCODING now work correctly for hash keys (single key instead of scattered composite keys)
- [ ] Verify KEYS and SCAN patterns correctly match hash keys (no false positives from old composite keys)
- [ ] Remove or repurpose the unused HashItem struct in src/storage/types/hash.rs
- [ ] Write tests: namespace isolation (SET key:field value + HSET key field value2 must not collide), HGETALL correctness, concurrent HSET/HGET, DEL removes entire hash
- [ ] Run project test suite - must pass before task 3

### Task 3: String Command Atomicity

**Files:**
- Modify: `src/command/types/string.rs`

Use the atomic storage primitives from Task 1 to fix all string command race conditions.

- [ ] Rewrite SET handler to use `set_with_options()` for NX/XX/GET flags (single atomic operation instead of exists-check-then-set)
- [ ] Rewrite SETNX to use `set_nx()`
- [ ] Rewrite INCR/INCRBY/DECR/DECRBY to use `atomic_incr()`
- [ ] Rewrite INCRBYFLOAT to use `atomic_incr_float()`
- [ ] Rewrite APPEND to use `atomic_append()`
- [ ] Rewrite GETSET/GETDEL/GETEX to use atomic variants
- [ ] Rewrite MSET/MSETNX to use atomic batch operation (all-or-nothing for MSETNX)
- [ ] Write concurrency tests: two clients racing SET NX on same key, two clients racing INCR on same key, MSETNX atomicity
- [ ] Run project test suite - must pass before task 4

### Task 4: Collection Command Atomicity and Sorted Set Upgrade

**Files:**
- Modify: `src/command/types/list.rs`
- Modify: `src/command/types/set.rs`
- Modify: `src/command/types/sorted_set.rs`
- Modify: `src/command/types/stream.rs`

Use `atomic_modify` for all collection operations and upgrade sorted set data structure.

- [ ] Rewrite all list commands (LPUSH, RPUSH, LPOP, RPOP, LSET, LREM, LINSERT, LTRIM, LRANGE, LINDEX, LLEN, LPOS, LMPOP) to use `atomic_modify` for thread-safe read-modify-write
- [ ] Rewrite all set commands (SADD, SREM, SPOP, SRANDMEMBER, SMEMBERS, SCARD, SISMEMBER, SMISMEMBER, SINTER, SUNION, SDIFF and their STORE variants, SSCAN) to use `atomic_modify`
- [ ] Upgrade sorted set internal data structure from `Vec<(f64, Vec<u8>)>` to `BTreeMap<OrderedFloat<f64>, Vec<u8>>` + `HashMap<Vec<u8>, f64>` for O(log N) operations
- [ ] Rewrite all sorted set commands (ZADD, ZREM, ZSCORE, ZRANK, ZRANGE, ZRANGEBYSCORE, ZRANGEBYLEX, ZCARD, ZCOUNT, ZINCRBY, ZPOPMIN, ZPOPMAX, ZRANDMEMBER, ZLEXCOUNT, ZMSCORE, ZSCAN, ZUNION, ZINTER, ZDIFF and STORE variants) to use `atomic_modify` with new data structure
- [ ] Rewrite all stream commands (XADD, XLEN, XRANGE, XREVRANGE, XREAD, XTRIM, XACK, XDEL, XINFO, XCLAIM, XAUTOCLAIM, XPENDING, XGROUP) to use `atomic_modify`
- [ ] Write concurrency tests: two clients racing LPUSH on same list, SADD on same set, ZADD on same sorted set
- [ ] Write performance test: ZADD/ZRANGE on sorted set with 10K+ members to verify O(log N) improvement
- [ ] Run project test suite - must pass before task 5

### Task 5: MULTI/EXEC Transaction Atomicity

**Files:**
- Modify: `src/command/transaction.rs`
- Modify: `src/networking/server.rs` (EXEC dispatch section)
- Modify: `src/storage/engine.rs` (add multi-key locking support)

Implement proper transaction isolation so queued commands execute atomically.

- [ ] Add `lock_keys(keys: &[Vec<u8>]) -> KeyLockGuard` to StorageEngine that locks multiple keys in sorted order (prevents deadlocks)
- [ ] Before EXEC execution, collect all keys from queued commands
- [ ] Lock all keys atomically using sorted-order locking
- [ ] Execute all queued commands while holding locks
- [ ] Release locks after EXEC completes (via Drop on guard)
- [ ] Verify WATCH correctly detects modifications between WATCH and EXEC (key version checking)
- [ ] Verify DISCARD properly clears queued commands and releases WATCH
- [ ] Write tests: two transactions racing on overlapping keys, WATCH detecting concurrent modification, DISCARD cleanup
- [ ] Run project test suite - must pass before task 6

### Task 6: SELECT Multi-Database Support

**Files:**
- Modify: `src/networking/server.rs` (add per-connection db_index state)
- Modify: `src/command/handler.rs` (accept db_index parameter)
- Modify: `src/command/types/key.rs` (SELECT handler)
- Modify: `src/storage/engine.rs` (route operations to correct database)

Implement real database switching so SELECT actually changes the active database.

- [ ] Add `db_index: usize` field to per-connection state in server.rs (initialized to 0)
- [ ] Modify SELECT handler to update the per-connection db_index and return it to the server loop
- [ ] Add `get_db(&self, db_index: usize) -> &DashMap<...>` method to StorageEngine that returns the correct database (self.data for 0, self.extra_dbs[n-1] for 1-15)
- [ ] Modify CommandHandler::process() to accept db_index and pass it through to storage operations
- [ ] Update all storage engine methods that access self.data to use get_db(db_index) instead
- [ ] Ensure MOVE command correctly moves between databases using db_index
- [ ] Ensure SWAPDB works correctly
- [ ] Ensure DBSIZE, FLUSHDB, RANDOMKEY respect db_index
- [ ] Write tests: SELECT 1 then SET/GET should isolate from database 0, MOVE between databases, FLUSHDB only flushes selected db
- [ ] Run project test suite - must pass before task 7

### Task 7: RESP3 Response Serialization

**Files:**
- Modify: `src/networking/server.rs` (track protocol_version properly, pass to serializer)
- Modify: `src/networking/resp.rs` (add RESP3-aware serialization)
- Modify: `src/command/handler.rs` (pass protocol version context)

Implement RESP3-specific response encoding so clients that negotiate RESP3 via HELLO receive correct wire format.

- [ ] Remove underscore prefix from `_protocol_version` variable, make it a proper tracked state
- [ ] Pass protocol_version through to response serialization (add parameter to serialize method or use per-connection context)
- [ ] Implement RESP3 map type encoding (`%<count>\r\n` followed by key-value pairs) for HGETALL, CONFIG GET, etc.
- [ ] Implement RESP3 set type encoding (`~<count>\r\n` followed by members) for SMEMBERS, etc.
- [ ] Implement RESP3 double/big number types where applicable
- [ ] Implement RESP3 null type (`_\r\n`) instead of RESP2 null bulk string (`$-1\r\n`)
- [ ] Implement RESP3 boolean type (`#t\r\n`/`#f\r\n`) where applicable
- [ ] Implement RESP3 push type (`><count>\r\n`) for pub/sub and client tracking
- [ ] Ensure RESP2 clients continue receiving RESP2 format (backward compatibility)
- [ ] Write tests: HELLO 3 negotiation then verify HGETALL returns map type, SMEMBERS returns set type, null returns RESP3 null
- [ ] Run project test suite - must pass before task 8

### Task 8: Full Persistence Support (RDB + AOF)

**Files:**
- Modify: `src/persistence/rdb.rs` (add all data type save/load)
- Modify: `src/persistence/aof.rs` (verify rewrite generates correct commands for all types)

Ensure all data types survive restart via both RDB and AOF persistence.

- [ ] Implement RDB save for lists (TYPE_LIST): serialize Vec<Vec<u8>> with length-prefixed entries
- [ ] Implement RDB save for sets (TYPE_SET): serialize HashSet<Vec<u8>> with length-prefixed entries
- [ ] Implement RDB save for sorted sets (TYPE_ZSET): serialize score-member pairs
- [ ] Implement RDB save for hashes (TYPE_HASH): serialize field-value pairs (using new single-key format from Task 2)
- [ ] Implement RDB save for streams (TYPE_STREAM): serialize StreamData structure
- [ ] Implement corresponding RDB load for each data type
- [ ] Verify AOF rewrite generates correct reconstruction commands for all types (RPUSH for lists, SADD for sets, ZADD for sorted sets, HSET for hashes, XADD for streams)
- [ ] Add data type indicator to RDB entries so load can route to correct deserializer
- [ ] Write tests: save database with all types, restart, verify all data intact via RDB
- [ ] Write tests: replay AOF with all command types, verify data integrity
- [ ] Run project test suite - must pass before task 9

### Task 9: ACL Lock Poisoning Fix and Security Hardening

**Files:**
- Modify: `src/security/acl.rs`

Replace panic-on-poison pattern with graceful recovery across all 11 .expect() call sites.

- [ ] Replace all 11 `.expect("...")` on RwLock read()/write() with `.unwrap_or_else(|e| e.into_inner())` (consistent with scripting module pattern)
- [ ] Audit all lock acquisitions for potential deadlock patterns (nested locks, lock ordering)
- [ ] Consider migrating from std::sync::RwLock to tokio::sync::RwLock for async compatibility (evaluate if this is needed based on current usage patterns)
- [ ] Write test: verify ACL operations don't panic on poisoned lock scenario
- [ ] Run project test suite - must pass before task 10

### Task 10: Server Code Deduplication

**Files:**
- Modify: `src/networking/server.rs` (unify fast/slow path dispatch)
- Create: `src/utils/glob.rs` or `src/utils/mod.rs` (shared glob_match)
- Modify: `src/command/types/hash.rs`, `src/command/types/set.rs`, `src/command/types/sorted_set.rs`, `src/security/acl.rs`, `src/sentinel/mod.rs`, `src/pubsub/manager.rs` (use shared glob_match)

Eliminate massive code duplication in server dispatch and 6x duplicated glob_match.

- [ ] Extract shared glob_match into `src/utils/glob.rs` with generic implementation supporting both `&str` and `&[u8]` inputs
- [ ] Replace all 6 duplicated glob_match implementations with calls to shared module
- [ ] Refactor server.rs: extract common dispatch logic (auth check, ACL check, pub/sub routing, sentinel routing, replication routing, transaction handling) into shared functions
- [ ] Make fast path parse into same Command struct as slow path, then call unified dispatch
- [ ] Verify no behavior changes in dispatch after refactor
- [ ] Write test: glob_match edge cases (wildcards, character classes, escape sequences)
- [ ] Run project test suite - must pass before task 11

### Task 11: Compiler Warnings, CLIENT LIST, and Minor Fixes

**Files:**
- Modify: Various files with dead_code warnings
- Modify: `src/command/types/connection.rs` (CLIENT LIST/INFO/ID)
- Modify: `src/networking/server.rs` (connection tracking)

Fix all 61 compiler warnings and implement real CLIENT LIST.

- [ ] Run `cargo fix --bin rlightning` for automatic fixes (unused imports, etc.)
- [ ] For dead_code warnings from unwired modules (cluster, sentinel): add appropriate `#[allow(dead_code)]` only where the code is genuinely planned for future use, remove truly dead code
- [ ] Implement basic connection tracking in server.rs: maintain Arc<DashMap<u64, ClientInfo>> with client ID, address, db, subscription count, last command
- [ ] Update CLIENT LIST to return real data from connection tracker
- [ ] Update CLIENT INFO to return current connection's real data
- [ ] Update CLIENT ID to return real connection ID
- [ ] Update CLIENT SETNAME/GETNAME to store/retrieve per-connection name
- [ ] Verify zero compiler warnings after fixes
- [ ] Run project test suite - must pass before task 12

### Task 12: Cluster and Sentinel Basic Wiring

**Files:**
- Modify: `src/main.rs` (initialize cluster/sentinel)
- Modify: `src/networking/server.rs` (route cluster/sentinel commands)
- Modify: `src/sentinel/mod.rs` (start monitoring loops in init())
- Modify: `src/cluster/mod.rs` (basic initialization)

Wire existing cluster and sentinel modules into the server so they're functional when enabled.

- [ ] Initialize ClusterManager in main.rs when cluster mode is enabled in config
- [ ] Wire cluster command dispatch in server.rs (CLUSTER INFO, CLUSTER NODES, CLUSTER MEET, etc.)
- [ ] Add slot validation for multi-key commands when in cluster mode (CROSSSLOT error)
- [ ] Start sentinel monitoring background tasks in init() when sentinel is enabled
- [ ] Start PING/health check loops for monitored masters
- [ ] Wire SDOWN/ODOWN detection based on ping failures
- [ ] Write tests: CLUSTER INFO returns valid response when enabled, sentinel detects master down (mock)
- [ ] Run project test suite - must pass before task 13

### Task 13: Verify Acceptance Criteria

- [ ] Run full test suite: `cargo test`
- [ ] Run clippy: `cargo clippy -- -D warnings` (zero warnings)
- [ ] Build release: `cargo build --release` (zero warnings)
- [ ] Verify Redis protocol compliance: test with redis-cli connecting and running basic commands across all data types
- [ ] Verify SET NX/XX atomicity: concurrent SET NX test shows correct single-winner behavior
- [ ] Verify INCR atomicity: concurrent INCR test shows no lost updates
- [ ] Verify SELECT works: SELECT 1, SET key val, SELECT 0, GET key returns nil
- [ ] Verify hash isolation: HSET myhash field val + SET myhash:field val2 are independent keys
- [ ] Verify MULTI/EXEC atomicity: concurrent transactions on overlapping keys don't interleave
- [ ] Verify RESP3: HELLO 3, HGETALL returns map format
- [ ] Verify persistence: save RDB with all types, restart, verify data intact
- [ ] Run benchmarks: `cargo bench` to verify no performance regressions
- [ ] Verify test coverage meets 80%+

### Task 14: Update Documentation

- [ ] Update CLAUDE.md if internal patterns changed (new storage engine API, utils module, etc.)
- [ ] Move this plan to `docs/plans/completed/`
