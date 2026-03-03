# rLightning v2.0 — Full Redis Compatibility & Release Readiness Plan

## Overview

Achieve 100% Redis 7.x protocol compatibility, fix all known concurrency/persistence/replication bugs, add comprehensive Docker-based comparison tests covering persistence, clustering, replication, ACL, memory management, and blocking commands. Every change must pass `cargo clippy -- -D warnings` with zero warnings.

## Context

- Current state: v1.1.0, 400+ commands, 97.9% compatibility (142/145 multi-language tests)
- 3 compatibility gaps: GEOHASH accuracy, pipeline error isolation, CONFIG SET/GET stub
- ~15 known bugs: TOCTOU races, non-atomic mutations, unbounded memory, AOF gaps, replication hardcodes
- Existing test infrastructure: 145 tests x 3 languages (Go/JS/Python) x 2 servers (Redis 7/rLightning) in Docker
- Missing test categories: persistence, clustering, replication, sentinel, ACL, memory management, blocking commands
- Key patterns: `atomic_modify()` at `src/storage/engine.rs:2163` is the concurrency fix template
- Related patterns: Command handlers in `src/command/types/`, RESP protocol in `src/networking/server.rs`, storage engine in `src/storage/engine.rs`

## Development Approach

- **Testing approach**: TDD where practical; each task must include tests
- **Dev containers**: Integration tests and compatibility tests use Docker (rLightning + real Redis side-by-side)
- Complete each task fully before moving to the next
- Every new command/fix must have unit tests + integration tests
- Docker comparison tests compare rLightning vs Redis for each feature area
- **No ignored tests** (`#[ignore]` is not allowed; all tests must run)
- **Zero warnings**: `cargo clippy -- -D warnings` must pass after every task
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

---

### Task 1: Fix pipeline error isolation

**Files:**
- Modify: `src/networking/server.rs`

**Problem:** `command_handler.process(cmd, db_index).await?` at ~line 1394 uses `?` which propagates `CommandError` (like `WRONGTYPE`) as `Err`, killing the entire pipeline. In Redis, each command's error is isolated — other commands in the pipeline still return correct results.

- [x] Change error handling in `process_with_transaction()` to catch `CommandError` and convert to `Ok(RespValue::Error(err.to_string()))` instead of propagating with `?`
- [x] Verify `dispatch_command` serializes error responses into the pipeline response buffer (not directly to socket)
- [x] Write Rust integration test: pipeline [SET k "hello", LPUSH k "bad", GET k] — verify LPUSH returns WRONGTYPE error, GET returns "hello"
- [x] Run multi-language compat test `pipeline_with_errors` — must pass all 3 clients
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 2: Fix GEOHASH accuracy

**Files:**
- Modify: `src/command/types/geo.rs`

**Problem:** Paris (2.3522, 48.8566) produces geohash "u11wje8ffnh" instead of starting with "u0". The `interleave()` function (lines 50-58) places longitude bits at odd positions (`2*i+1`) and latitude at even positions (`2*i`), but the standard geohash convention is longitude=even, latitude=odd.

- [x] Fix `interleave()`: swap bit positions — longitude to `2*i` (even), latitude to `2*i+1` (odd)
- [x] Fix `deinterleave()`: extract longitude from even positions, latitude from odd positions
- [x] Verify `geohash_to_string()` BASE32_ALPHABET matches Redis convention
- [x] Verify `geohash_decode` still works correctly (used by GEOPOS, GEODIST) — run existing geo tests
- [x] Add unit tests with known reference values: Paris→starts with "u09t", Rome (12.4964, 41.9028)→starts with "sr2y", New York (-74.006, 40.7128)→starts with "dr5r"
- [x] Run multi-language compat test `GEOHASH_accuracy` — must pass all 3 clients
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 3: Implement CONFIG SET/GET runtime config store

**Files:**
- Modify: `src/storage/engine.rs`
- Modify: `src/command/types/server.rs`
- Modify: `src/command/types/connection.rs`
- Modify: `src/main.rs`

**Problem:** CONFIG SET is a stub that returns OK but stores nothing (connection.rs:287-295). CONFIG GET has a hardcoded 4-parameter match (server.rs:54-90), missing `maxmemory-policy` and most other parameters.

- [x] Add `runtime_config: DashMap<String, String>` field to `StorageEngine`
- [x] Seed from config at startup: `maxmemory`, `maxmemory-policy`, `port`, `timeout`, `databases`, `bind`, `requirepass`, `hz`, `appendonly`, `appendfsync`, `save`, `tcp-backlog`, `tcp-keepalive`
- [x] Rewrite CONFIG GET to read from `runtime_config` with glob pattern matching (`CONFIG GET *`, `CONFIG GET max*`)
- [x] Rewrite CONFIG SET to write to `runtime_config`; for `maxmemory-policy`, also update actual eviction policy in engine
- [x] Write tests: CONFIG SET→GET roundtrip, glob patterns, behavior change on `maxmemory-policy` SET
- [x] Run multi-language compat test `CONFIG_SET_and_GET` — must pass all 3 clients
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 4: Migrate string commands to atomic operations

**Files:**
- Modify: `src/command/types/string.rs`
- Modify: `src/storage/engine.rs`

**Problem:** SETRANGE, GETEX, GETDEL, GET have TOCTOU race conditions — separate `engine.get()` then `engine.set()` allows concurrent modification between read and write.

- [x] **SETRANGE** (lines 373-422): Replace get→modify→set with `atomic_modify()` that checks type, pads, overwrites range, returns new length
- [x] **GETEX** (lines 659-752): Create `atomic_get_set_expiry()` to read value and set/clear TTL in one lock hold
- [x] **GETDEL**: Verified it uses existing `atomic_getdel()` primitive — already correct
- [x] **GET**: Replace separate type-check + value-read with single `atomic_read()` DashMap lookup
- [x] Write concurrent tests: multiple tokio tasks doing SETRANGE on overlapping ranges simultaneously
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 5: Make MSET/MSETNX atomic

**Files:**
- Modify: `src/command/types/string.rs`
- Modify: `src/storage/engine.rs`

**Problem:** MSET (lines 222-232) sets keys one-by-one in a loop. Redis guarantees MSET is atomic.

- [x] MSET: Use `lock_keys()` (sorted order for deadlock prevention) → set all keys → release locks
- [x] MSETNX: Review existing lock-based implementation (lines 442-493), fix remaining TOCTOU gaps in rollback logic
- [x] Write concurrent tests: multiple MSET operations from parallel tasks, verify no partial states
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 6: Migrate list commands to atomic_modify

**Files:**
- Modify: `src/command/types/list.rs`

**Problem:** All 11 list commands use get→deserialize→modify→serialize→set pattern with separate `engine.get()` and `engine.set_with_type_preserve_ttl()`. Race condition under concurrent access.

- [x] **Push operations**: LPUSH, RPUSH, LPUSHX, RPUSHX — `atomic_modify()` with `Vec<Vec<u8>>` deserialization
- [x] **Pop operations**: LPOP, RPOP — `atomic_modify()`, remove from front/back, handle count argument
- [x] **Modify operations**: LTRIM, LSET, LREM, LINSERT, LMOVE — `atomic_modify()` for each
- [x] Write concurrent tests: parallel LPUSH/RPUSH from multiple tasks, verify final list length = sum of all pushes
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 7: Migrate set commands to atomic_modify

**Files:**
- Modify: `src/command/types/set.rs`

**Problem:** SADD, SREM, SPOP, SMOVE use same load-modify-save pattern as lists.

- [x] SADD, SREM: `atomic_modify()` with `HashSet<Vec<u8>>` deserialization
- [x] SPOP: `atomic_modify()` with random element removal
- [x] SMOVE: Use `lock_keys()` for both source and dest keys (cross-key atomic operation)
- [x] Write concurrent tests: parallel SADD from multiple tasks, verify final set cardinality
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 8: Migrate geo commands to atomic_modify

**Files:**
- Modify: `src/command/types/geo.rs`

**Problem:** GEOADD and GEOSEARCHSTORE use load-modify-save pattern for sorted set data.

- [x] GEOADD: `atomic_modify()` with sorted set data deserialization
- [x] GEOSEARCHSTORE: `atomic_modify()` on destination key with `lock_keys()` for cross-key atomicity
- [x] Write concurrent GEOADD tests (same-key parallel adds + concurrent updates)
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 9: Fix set_with_type_preserve_ttl TOCTOU race

**Files:**
- Modify: `src/storage/engine.rs`

**Problem:** `set_with_type_preserve_ttl()` (~line 653) reads TTL in one DashMap lookup, then calls `set_with_type()` in a separate operation. Between them, the key could be modified or deleted.

- [x] Rewrite to use single DashMap entry operation (read TTL + write value atomically)
- [x] Audit remaining callers after Tasks 6-8 (most should be eliminated by atomic_modify migration)
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 10: Add cleanup for unbounded key_locks/key_versions

**Files:**
- Modify: `src/storage/engine.rs`

**Problem:** `key_versions` (line 149) and `key_locks` (line 154) DashMaps grow forever. Every WATCH'd or transacted key gets an entry that is never removed. Memory leak over time.

- [x] Add `cleanup_stale_metadata()` method that removes entries for keys not in the main data store
- [x] Call from existing periodic background task (alongside expiration sweep), every 60 seconds
- [x] Guard against race: only remove if no active WATCH or pending transaction references the key
- [x] Write test: create 10K keys, WATCH them, delete all, verify maps shrink after cleanup cycle
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 11: Fix AOF partial batch write corruption

**Files:**
- Modify: `src/persistence/aof.rs`

**Problem:** Mid-batch I/O error (lines 118-163) leaves partial RESP commands in BufWriter. The `continue` on line 143 skips flush but partial data remains in BufWriter's internal buffer, producing corrupt AOF on next flush.

- [x] Stage entire batch in a `Vec<u8>` memory buffer first
- [x] Write the complete batch to BufWriter only after all commands are serialized successfully
- [x] On I/O error, discard the batch entirely (no partial writes reach the file)
- [x] Log the error with batch details for debugging
- [x] Write test: simulate I/O error mid-batch, verify AOF file remains valid RESP
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 12: Expand AOF replay to full command coverage

**Files:**
- Modify: `src/persistence/aof.rs`
- Modify: `src/storage/engine.rs`

**Problem:** `process_command()` (engine.rs:1039-1196) only handles ~9 command types. ~60 write commands are silently dropped during AOF replay, causing data loss on recovery.

- [x] Create a `CommandHandler` instance during AOF load and route replay through `cmd_handler.process()` instead of manual `process_command()` switch
- [x] Handle async properly (existing code already uses `block_on()` inside `spawn_blocking`)
- [x] Keep `process_command()` as bootstrap fallback if needed
- [x] Write tests: AOF replay for LPUSH, SREM, HDEL, ZINCRBY, GEOADD, PFADD, SETBIT, XGROUP, APPEND, INCR, RENAME
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 13: Persist stream consumer groups in AOF rewrite

**Files:**
- Modify: `src/persistence/aof.rs`
- Reference: `src/storage/stream.rs`

**Problem:** AOF rewrite emits XADD commands for stream entries but does not persist consumer groups, PEL (pending entry list), or consumer state. On reload, all consumer group state is lost.

- [x] After XADD commands in `aof_rewrite_commands_for_item()`, emit `XGROUP CREATE key groupname id` for each consumer group
- [x] Emit `XCLAIM key groupname consumer 0 id` for each pending entry in each consumer's PEL
- [x] Write test: create stream with consumer groups + pending entries, trigger AOF rewrite, reload, verify consumer group state survives
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 14: Ensure XREADGROUP mutations are persisted to AOF

**Files:**
- Modify: `src/persistence/aof.rs`
- Depends on: Task 12

**Problem:** XREADGROUP modifies consumer group state (moves entries to consumers' PEL) but these mutations may not be properly replayed. With Task 12 complete (full command handler for replay), verify this works end-to-end.

- [x] Verify XREADGROUP is classified as a write command in `is_read_only_command()` (not in the exclusion list)
- [x] With Task 12's full replay handler, verify AOF replay handles XREADGROUP correctly
- [x] Write test: XREADGROUP, AOF save, restart, verify consumer state preserved
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 15: Fix replication client hardcoded db_index=0

**Files:**
- Modify: `src/replication/client.rs`

**Problem:** Line 240 always passes `0` as db_index: `cmd_handler.process(cmd, 0)`. Commands from master targeting DB 1-15 are applied to wrong database.

- [x] Add `current_db: usize` variable in the command processing loop
- [x] On `SELECT` command, update `current_db` to the new database index
- [x] Pass `current_db` to `cmd_handler.process(cmd, current_db)` instead of hardcoded `0`
- [x] Write test: master uses SELECT 3 + SET, verify replica has key in DB 3
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 16: Handle MULTI/EXEC in replication stream

**Files:**
- Modify: `src/replication/client.rs`

**Problem:** Replication client processes commands individually (lines 205-265). MULTI/EXEC blocks from master are not preserved as transactions on the replica, breaking atomicity guarantees.

- [x] Detect MULTI in replication command stream, begin buffering commands
- [x] On EXEC, execute all buffered commands as a transaction on replica
- [x] Handle DISCARD to abort buffering
- [x] Write test: master executes MULTI/SET key1/SET key2/EXEC, verify replica state is consistent and atomic
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 17: Wire HELLO SETNAME to connection tracker

**Files:**
- Modify: `src/networking/server.rs`

**Problem:** HELLO command's SETNAME option (~line 1972) is acknowledged but the client name is discarded.

- [x] Pass `connections` DashMap and `conn_id` to `handle_hello_command()`
- [x] Update `ClientInfo.name` when SETNAME option is provided
- [x] Write test: HELLO 3 SETNAME "myconn" → CLIENT GETNAME returns "myconn"
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 18: Fix CLIENT LIST sub/psub counts

**Files:**
- Modify: `src/networking/server.rs`

**Problem:** CLIENT LIST always shows sub=0 psub=0 instead of actual subscription counts.

- [x] Add `sub_count` and `psub_count` fields to `ClientInfo` (or use AtomicUsize)
- [x] Increment on SUBSCRIBE/PSUBSCRIBE, decrement on UNSUBSCRIBE/PUNSUBSCRIBE
- [x] Update CLIENT LIST output to use real counts
- [x] Write test: SUBSCRIBE to 3 channels, verify CLIENT LIST shows sub=3
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 19: Multi-DB persistence support

**Files:**
- Modify: `src/persistence/aof.rs`
- Modify: `src/persistence/rdb.rs`

**Problem:** RDB/AOF persistence only operates on DB 0. Data in databases 1-15 is lost on restart.

- [x] AOF writer: prefix command batches with `SELECT db_index` when command targets non-zero database
- [x] AOF replay: handle SELECT commands to route replayed commands to correct database (Task 12 may partially solve this)
- [x] RDB save: iterate all 16 databases, write DB selector marker per database
- [x] RDB load: handle DB selector markers, restore to correct databases
- [x] Write test: SET keys in DB 0 and DB 3, save (RDB + AOF), restart, verify both databases restored
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 20: Fix OBJECT ENCODING responses

**Files:**
- Modify: `src/storage/engine.rs`

**Problem:** `get_encoding()` (~line 1669) returns "listpack" for all collection types regardless of size.

- [x] Add size-based heuristics matching Redis behavior:
  - String: integer value → "int", <=44 bytes → "embstr", else "raw"
  - List: <=128 elements && all <=64 bytes → "listpack", else "quicklist"
  - Set: <=128 elements && all <=64 bytes → "listpack", else "hashtable"
  - Hash: <=128 fields && all <=64 bytes → "listpack", else "hashtable"
  - ZSet: <=128 elements && all <=64 bytes → "listpack", else "skiplist"
- [x] Write tests: create small/large collections, verify OBJECT ENCODING returns correct values
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 21: Fix missing RESP3 Map conversions

**Files:**
- Modify: `src/networking/server.rs`

**Problem:** Some commands that should return RESP3 Map type in RESP3 mode still return plain arrays.

- [x] Audit all commands returning key-value pairs (XINFO STREAM, XINFO GROUPS, COMMAND INFO, CLIENT INFO, SLOWLOG GET, etc.)
- [x] Add `convert_for_resp3()` support for remaining commands
- [x] Write test: connect with HELLO 3, verify Map responses for applicable commands
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 22: Fix GEORADIUS STORE destination key extraction for tx locking

**Files:**
- Modify: `src/networking/server.rs`

**Problem:** GEORADIUS STORE and GEOSEARCHSTORE destination keys not extracted for transaction key locking (~line 1060).

- [x] Add destination key extraction logic for GEORADIUS STORE and GEOSEARCHSTORE in the cluster slot / tx key extraction code
- [x] Write test: WATCH + MULTI + GEOSEARCHSTORE with destination key, verify transaction integrity
- [x] Run `cargo clippy -- -D warnings` — zero warnings
- [x] Run project test suite — must pass before next task

---

### Task 23: Docker comparison tests — Persistence (RDB/AOF)

**Files:**
- Create: `tests/docker-compat/go-client/tests/persistence.go`
- Create: `tests/docker-compat/js-client/tests/persistence.js`
- Create: `tests/docker-compat/python-client/tests/persistence.py`
- Modify: `tests/docker-compat/docker-compose.yml` (enable persistence in test containers)

New Docker-based comparison test category testing persistence behavior against real Redis 7.

- [x] **RDB_SAVE_and_RESTORE**: BGSAVE, wait, verify LASTSAVE timestamp changes
- [x] **AOF_append_and_replay**: CONFIG SET appendonly yes, write data, verify data survives container restart
- [x] **PERSIST_across_restart**: SET keys with TTL, restart container, verify keys and TTLs survived
- [x] **RDB_background_save**: SET data, BGSAVE, verify DBSIZE after restart
- [x] **CONFIG_persistence_settings**: CONFIG SET/GET for `save`, `appendonly`, `appendfsync`
- [x] **DEBUG_SLEEP_during_save**: Verify server remains responsive during BGSAVE
- [x] **MULTI_DB_persistence**: SET keys in DB 0 and DB 3, restart, verify both databases restored
- [x] **LARGE_VALUE_persistence**: SET 1MB value, restart, verify value intact
- [x] Implement in all 3 languages (Go, JS, Python) following existing framework patterns
- [x] Run full comparison suite — must pass on both Redis and rLightning
- [x] Run `cargo clippy -- -D warnings` — zero warnings

---

### Task 24: Docker comparison tests — ACL & Security

**Files:**
- Create: `tests/docker-compat/go-client/tests/acl.go`
- Create: `tests/docker-compat/js-client/tests/acl.js`
- Create: `tests/docker-compat/python-client/tests/acl.py`

- [x] **ACL_SETUSER_basic**: Create user with specific permissions, verify access
- [x] **ACL_SETUSER_command_restrictions**: Create user with restricted commands, verify denied commands return NOPERM
- [x] **ACL_SETUSER_key_patterns**: Create user with key pattern restrictions (~key:*), verify access control
- [x] **ACL_DELUSER**: Create user, delete, verify deleted user cannot AUTH
- [x] **ACL_LIST**: Create multiple users, verify ACL LIST returns all
- [x] **ACL_WHOAMI**: AUTH as different users, verify ACL WHOAMI returns correct username
- [x] **ACL_LOG**: Execute denied command, verify ACL LOG records the denial
- [x] **ACL_CAT**: Verify ACL CAT returns command categories
- [x] **AUTH_username_password**: Test named user authentication (AUTH username password)
- [x] **AUTH_default_user**: Test default user password authentication
- [x] Implement in all 3 languages
- [x] Run full comparison suite

---

### Task 25: Docker comparison tests — Blocking Commands

**Files:**
- Create: `tests/docker-compat/go-client/tests/blocking.go`
- Create: `tests/docker-compat/js-client/tests/blocking.js`
- Create: `tests/docker-compat/python-client/tests/blocking.py`

- [x] **BLPOP_with_data**: LPUSH then BLPOP, verify immediate return
- [x] **BLPOP_timeout**: BLPOP on empty key with 1s timeout, verify timeout return
- [x] **BLPOP_multi_key**: BLPOP on multiple keys, LPUSH to second key, verify correct key returned
- [x] **BRPOP_basic**: Same as BLPOP but from right side
- [x] **BLMOVE_basic**: BLMOVE from source to dest list, verify move
- [x] **BLMPOP_basic**: BLMPOP with LEFT/RIGHT direction
- [x] **BZPOPMIN_basic**: BZPOPMIN with data and timeout variants
- [x] **BZPOPMAX_basic**: BZPOPMAX with data and timeout variants
- [x] **BLOCKING_concurrent**: Producer-consumer pattern — one goroutine/thread pushes after delay, another BLPOP waits
- [x] **BLOCKING_timeout_accuracy**: Verify timeout duration is within acceptable range (±100ms)
- [x] Implement in all 3 languages
- [x] Run full comparison suite

---

### Task 26: Docker comparison tests — Memory Management & Eviction

**Files:**
- Create: `tests/docker-compat/go-client/tests/memory.go`
- Create: `tests/docker-compat/js-client/tests/memory.js`
- Create: `tests/docker-compat/python-client/tests/memory.py`
- Modify: `tests/docker-compat/docker-compose.yml` (add memory-limited test container)

- [x] **MEMORY_USAGE_basic**: SET key, verify MEMORY USAGE returns reasonable byte count
- [x] **MEMORY_USAGE_types**: Test MEMORY USAGE for string, hash, list, set, zset
- [x] **MEMORY_DOCTOR**: Run MEMORY DOCTOR, verify response format
- [x] **INFO_memory_section**: Verify INFO memory section contains used_memory, used_memory_peak, maxmemory
- [x] **CONFIG_maxmemory**: CONFIG SET maxmemory 1mb, fill beyond limit, verify OOM error or eviction
- [x] **CONFIG_maxmemory_policy**: Test allkeys-lru, volatile-lru, allkeys-random, noeviction policies
- [x] **OBJECT_ENCODING**: Verify encoding changes based on data size (listpack→hashtable, etc.)
- [x] **OBJECT_REFCOUNT**: Verify OBJECT REFCOUNT returns integer
- [x] **OBJECT_IDLETIME**: SET key, wait, verify OBJECT IDLETIME reflects elapsed time
- [x] **OBJECT_FREQ**: Verify OBJECT FREQ returns LFU frequency counter
- [x] Implement in all 3 languages
- [x] Run full comparison suite

---

### Task 27: Docker comparison tests — Replication

**Files:**
- Create: `tests/docker-compat/go-client/tests/replication.go`
- Create: `tests/docker-compat/js-client/tests/replication.js`
- Create: `tests/docker-compat/python-client/tests/replication.py`
- Modify: `tests/docker-compat/docker-compose.yml` (add master + replica containers for both Redis and rLightning)

- [x] **INFO_replication**: Verify INFO replication section on master (role:master, connected_slaves)
- [x] **REPLICAOF_basic**: Configure replica, verify it connects to master
- [x] **REPLICATION_data_sync**: SET key on master, verify key appears on replica
- [x] **REPLICATION_multi_commands**: SET multiple keys on master, verify all replicated
- [x] **REPLICATION_expiry**: SET key with TTL on master, verify TTL replicated
- [x] **REPLICA_read_only**: Verify writes to replica return READONLY error
- [x] **REPLICATION_after_reconnect**: Disconnect replica, write to master, reconnect, verify partial sync
- [x] **WAIT_command**: SET key, WAIT 1 0 (wait for 1 replica, no timeout), verify acknowledged
- [x] **REPLICATION_SELECT**: Master uses SELECT 3 + SET, verify replica DB 3 has key
- [x] Implement in all 3 languages
- [x] Run full comparison suite

---

### Task 28: Docker comparison tests — Cluster Mode

**Files:**
- Create: `tests/docker-compat/go-client/tests/cluster.go`
- Create: `tests/docker-compat/js-client/tests/cluster.js`
- Create: `tests/docker-compat/python-client/tests/cluster.py`
- Modify: `tests/docker-compat/docker-compose.yml` (add 3-node cluster for both Redis and rLightning)

- [x] **CLUSTER_INFO**: Verify CLUSTER INFO returns cluster_state, cluster_slots_assigned, cluster_size
- [x] **CLUSTER_NODES**: Verify CLUSTER NODES returns node list with correct format
- [x] **CLUSTER_SLOTS**: Verify CLUSTER SLOTS returns slot ranges with master/replica info
- [x] **CLUSTER_SHARDS**: Verify CLUSTER SHARDS returns shard information (Redis 7.0+)
- [x] **CLUSTER_hash_tag**: Verify {tag} keys hash to same slot — SET {user}.name, SET {user}.email, verify same slot
- [x] **CLUSTER_MOVED_redirect**: Access key on wrong node, verify MOVED error with correct slot and target
- [x] **CLUSTER_KEYSLOT**: Verify CLUSTER KEYSLOT returns correct CRC16 mod 16384
- [x] **CLUSTER_COUNTKEYSINSLOT**: Verify key counting per slot
- [x] **CLUSTER_cross_slot_error**: Verify CROSSSLOT error for multi-key commands on different slots
- [x] **READONLY_mode**: READONLY on replica node, verify reads succeed
- [x] Implement in all 3 languages
- [x] Run full comparison suite

---

### Task 29: Docker comparison tests — Sentinel

**Files:**
- Create: `tests/docker-compat/go-client/tests/sentinel.go`
- Create: `tests/docker-compat/js-client/tests/sentinel.js`
- Create: `tests/docker-compat/python-client/tests/sentinel.py`
- Modify: `tests/docker-compat/docker-compose.yml` (add sentinel containers)

- [x] **SENTINEL_master_info**: SENTINEL MASTER mymaster — verify master host/port/flags
- [x] **SENTINEL_replicas**: SENTINEL REPLICAS mymaster — verify replica list
- [x] **SENTINEL_sentinels**: SENTINEL SENTINELS mymaster — verify sentinel peer list
- [x] **SENTINEL_get_master_addr**: SENTINEL GET-MASTER-ADDR-BY-NAME mymaster — verify address
- [x] **SENTINEL_ckquorum**: SENTINEL CKQUORUM mymaster — verify quorum check
- [x] **SENTINEL_failover**: Simulate master failure, verify failover completes, new master elected
- [x] **SENTINEL_client_discovery**: Connect via sentinel, verify automatic master discovery
- [x] Implement in all 3 languages
- [x] Run full comparison suite

---

### Task 30: Docker comparison tests — Streams Advanced

**Files:**
- Create: `tests/docker-compat/go-client/tests/streams_advanced.go`
- Create: `tests/docker-compat/js-client/tests/streams_advanced.js`
- Create: `tests/docker-compat/python-client/tests/streams_advanced.py`

- [ ] **XGROUP_CREATE**: Create consumer group, verify with XINFO GROUPS
- [ ] **XREADGROUP_basic**: Create group, XREADGROUP, verify message delivered to consumer
- [ ] **XREADGROUP_pending**: XREADGROUP without ACK, verify message in pending list (XPENDING)
- [ ] **XACK_basic**: Read message, XACK, verify removed from pending list
- [ ] **XCLAIM_basic**: Read message on consumer1, XCLAIM to consumer2, verify transfer
- [ ] **XAUTOCLAIM_basic**: Read message, wait, XAUTOCLAIM with min-idle, verify reclaim
- [ ] **XPENDING_summary**: Check pending summary (consumers, min-id, max-id, count)
- [ ] **XPENDING_detail**: Check per-consumer pending detail
- [ ] **XINFO_STREAM**: Verify full stream info including length, groups, first/last entry
- [ ] **XINFO_CONSUMERS**: Verify consumer info including name, pending count, idle time
- [ ] **XTRIM_MAXLEN**: Add entries, XTRIM MAXLEN 5, verify only 5 remain
- [ ] **XTRIM_MINID**: Add entries, XTRIM MINID, verify entries before ID removed
- [ ] Implement in all 3 languages
- [ ] Run full comparison suite

---

### Task 31: Docker comparison tests — Lua Scripting Advanced

**Files:**
- Create: `tests/docker-compat/go-client/tests/scripting_advanced.go`
- Create: `tests/docker-compat/js-client/tests/scripting_advanced.js`
- Create: `tests/docker-compat/python-client/tests/scripting_advanced.py`

- [ ] **EVAL_return_types**: Test Lua returning string, integer, table, nil, boolean, error
- [ ] **EVAL_redis_call**: Lua calling redis.call('SET', KEYS[1], ARGV[1])
- [ ] **EVAL_redis_pcall**: Lua calling redis.pcall() with error handling
- [ ] **EVALSHA_cached**: SCRIPT LOAD, then EVALSHA, verify execution
- [ ] **SCRIPT_EXISTS**: SCRIPT LOAD, SCRIPT EXISTS with SHA, verify true
- [ ] **SCRIPT_FLUSH**: Load scripts, SCRIPT FLUSH, verify NOSCRIPT on EVALSHA
- [ ] **FUNCTION_LOAD**: Load Lua function library (Redis 7.0 FUNCTION LOAD)
- [ ] **FCALL_basic**: FCALL loaded function, verify execution
- [ ] **EVAL_KEYS_ARGV**: Verify KEYS and ARGV arrays passed correctly
- [ ] **EVAL_error_handling**: Verify error propagation from Lua to client
- [ ] Implement in all 3 languages
- [ ] Run full comparison suite

---

### Task 32: Update test infrastructure and run full verification

**Files:**
- Modify: `tests/docker-compat/run-tests.sh` (register new test categories)
- Modify: `tests/docker-compat/report/generate-report.py` (update category list)
- Modify: `tests/docker-compat/shared/test-report.schema.json` (update if needed)
- Modify: `tests/docker-compat/KNOWN-INCOMPATIBILITIES.txt` (clear resolved items)

- [ ] Register all new test categories in each language's main entry point
- [ ] Update `run-tests.sh` for new container configurations (replication, cluster, sentinel)
- [ ] Update report generator to include new categories
- [ ] Run full suite end-to-end: `cd tests/docker-compat && ./run-tests.sh --local`
- [ ] Generate final comparison report: `python tests/docker-compat/report/generate-report.py --results-dir tests/docker-compat/results/`
- [ ] Verify 0 compatibility gaps across all categories
- [ ] Run `cargo test` — all Rust tests pass
- [ ] Run `cargo clippy -- -D warnings` — zero warnings
- [ ] Run `cargo fmt --check` — properly formatted
- [ ] Run `cargo bench` — benchmarks execute without errors
- [ ] Docker build: `./scripts/build-docker.sh` — image builds successfully
