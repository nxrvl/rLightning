# 100% Redis Compatibility Implementation Plan

## Overview

Implement full Redis 7.x protocol and command compatibility for rLightning, covering all missing commands, features, and protocols. Includes comprehensive benchmark tests comparing rLightning vs Redis performance, and full test coverage for every feature area.

## Context

- Current state: ~96 commands implemented out of ~400+ Redis 7.x commands
- Major missing areas: Transactions, Lua Scripting, Streams, Blocking Commands, Bitmap, HyperLogLog, Geo, ACL, Cluster, Sentinel, RESP3
- Files involved: src/command/, src/storage/, src/networking/, src/security/, src/replication/, src/pubsub/, benches/, tests/, site/
- Related patterns: Command handlers in src/command/types/, RESP protocol in src/networking/resp.rs, storage engine in src/storage/engine.rs

## Development Approach

- **Testing approach**: TDD where practical; each task must include tests
- **Dev containers**: Integration tests and compatibility tests use Docker dev containers (rLightning + real Redis side-by-side) for reproducible test environments
- Complete each task fully before moving to the next
- Every new command must have unit tests + integration tests
- Benchmark tests compare rLightning vs Redis for each command category
- **No ignored tests** (#[ignore] is not allowed; all tests must run)
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: RESP3 Protocol Support

**Files:**
- Modify: `src/networking/resp.rs`
- Modify: `src/networking/mod.rs`
- Modify: `src/command/handler.rs`

- [x] Implement RESP3 data types: Map (%), Set (~), Null (_), Boolean (#), Double (,), Big Number (, Verbatim String (=), Push (>)
- [x] Implement HELLO command for protocol version negotiation (RESP2/RESP3 switching)
- [x] Add per-connection protocol version tracking
- [x] Implement RESP3 attribute type for metadata
- [x] Support RESP3 inline and aggregate types in parser and serializer
- [x] Write tests for all RESP3 types parsing and serialization
- [x] Write integration test: client connects, issues HELLO 3, verifies RESP3 responses
- [x] Run project test suite - must pass before next task

### Task 2: Missing Key/Expiration Commands

**Files:**
- Modify: `src/command/handler.rs`
- Modify: `src/storage/engine.rs`
- Create: `src/command/types/key.rs` (consolidate key commands)

- [x] Implement COPY (copy key to destination)
- [x] Implement MOVE (move key between databases - requires multi-db support)
- [x] Implement UNLINK (async DELETE)
- [x] Implement TOUCH (update last access time)
- [x] Implement EXPIREAT / PEXPIREAT (absolute timestamp expiration)
- [x] Implement EXPIRETIME / PEXPIRETIME (get expiration as timestamp)
- [x] Implement OBJECT subcommands (REFCOUNT, ENCODING, IDLETIME, FREQ, HELP)
- [x] Implement DUMP / RESTORE (serialize/deserialize key values)
- [x] Implement SORT / SORT_RO (sort lists, sets, sorted sets with BY/GET/STORE)
- [x] Implement WAIT / WAITAOF (wait for replication/AOF acknowledgements)
- [x] Implement SELECT (database selection - requires multi-database support in engine)
- [x] Implement SCAN improvements (TYPE filter, MATCH pattern enhancements)
- [x] Write tests for each new command
- [x] Write Redis compatibility test comparing behavior with real Redis (using dev container)
- [x] Run project test suite - must pass before next task

### Task 3: Missing String Commands

**Files:**
- Modify: `src/command/types/string.rs`
- Modify: `src/command/handler.rs`

- [x] Implement GETEX (GET with expiration options: EX, PX, EXAT, PXAT, PERSIST)
- [x] Implement GETDEL (GET and DELETE atomically)
- [x] Implement PSETEX (SET with millisecond expiration)
- [x] Implement LCS (Longest Common Subsequence between two strings)
- [x] Implement SUBSTR (deprecated alias for GETRANGE)
- [x] Write tests for each new command
- [x] Write Redis compatibility test (using dev container)
- [x] Run project test suite - must pass before next task

### Task 4: Bitmap Commands

**Files:**
- Modify: `src/storage/engine.rs` (bitmap storage support)
- Create: `src/command/types/bitmap.rs`
- Modify: `src/command/handler.rs`

- [x] Add bitmap data representation in storage engine (use string values with bit operations)
- [x] Implement SETBIT (set bit at offset)
- [x] Implement GETBIT (get bit at offset)
- [x] Implement BITCOUNT (count set bits in range)
- [x] Implement BITPOS (find first set/unset bit)
- [x] Implement BITOP (AND, OR, XOR, NOT between keys)
- [x] Implement BITFIELD (atomic read/write/increment of bitfield values)
- [x] Implement BITFIELD_RO (read-only BITFIELD)
- [x] Write tests for all bitmap commands with edge cases
- [x] Write Redis compatibility test (using dev container)
- [x] Run project test suite - must pass before next task

### Task 5: Missing List Commands + Blocking Operations

**Files:**
- Modify: `src/command/types/list.rs`
- Modify: `src/command/handler.rs`
- Modify: `src/networking/mod.rs` (blocking connection support)

- [x] Implement LPUSHX / RPUSHX (push only if list exists)
- [x] Implement LINSERT (insert before/after pivot element)
- [x] Implement LSET (set element at index)
- [x] Implement LPOS (find element position)
- [x] Implement LMOVE (atomically move element between lists)
- [x] Implement LMPOP (pop from multiple lists)
- [x] Build blocking command infrastructure: per-key wait queues, timeout handling, client wakeup mechanism
- [x] Implement BLPOP (blocking left pop with timeout)
- [x] Implement BRPOP (blocking right pop with timeout)
- [x] Implement BLMOVE (blocking list move)
- [x] Implement BLMPOP (blocking pop from multiple lists)
- [x] Write tests for all non-blocking list commands
- [x] Write tests for blocking commands with timeout, multi-key, and concurrent client scenarios
- [x] Write Redis compatibility test (using dev container)
- [x] Run project test suite - must pass before next task

### Task 6: Missing Set & Hash Commands

**Files:**
- Modify: `src/command/types/set.rs`
- Modify: `src/command/types/hash.rs`
- Modify: `src/command/handler.rs`

- [x] Implement SMOVE (move member between sets)
- [x] Implement SINTERCARD (intersection cardinality with LIMIT)
- [x] Implement SMISMEMBER (check multiple members at once)
- [x] Implement SSCAN (incrementally iterate set members)
- [x] Implement HRANDFIELD (random field with optional count and WITHVALUES)
- [x] Implement HSCAN (incrementally iterate hash fields)
- [x] Write tests for each new command
- [x] Write Redis compatibility test (using dev container)
- [x] Run project test suite - must pass before next task

### Task 7: Missing Sorted Set Commands

**Files:**
- Modify: `src/command/types/sorted_set.rs`
- Modify: `src/command/handler.rs`

- [x] Implement ZRANGEBYSCORE / ZREVRANGEBYSCORE (range by score with LIMIT)
- [x] Implement ZRANGEBYLEX / ZREVRANGEBYLEX (range by lexicographic order)
- [x] Implement ZREMRANGEBYRANK (remove by rank range)
- [x] Implement ZREMRANGEBYSCORE (remove by score range)
- [x] Implement ZREMRANGEBYLEX (remove by lex range)
- [x] Implement ZLEXCOUNT (count by lex range)
- [x] Implement ZREVRANK (reverse rank)
- [x] Implement ZINTERSTORE / ZUNIONSTORE (store intersection/union with AGGREGATE and WEIGHTS)
- [x] Implement ZINTER / ZUNION / ZDIFF (without storing)
- [x] Implement ZDIFFSTORE (store difference)
- [x] Implement ZPOPMIN / ZPOPMAX (pop lowest/highest scored members)
- [x] Implement BZPOPMIN / BZPOPMAX (blocking pop - reuse blocking infra from Task 5)
- [x] Implement ZRANDMEMBER (random member)
- [x] Implement ZMSCORE (multiple member scores)
- [x] Implement ZMPOP / BZMPOP (pop from multiple sorted sets)
- [x] Implement ZRANGESTORE (store range result)
- [x] Implement ZSCAN (incrementally iterate sorted set members)
- [x] Implement unified ZRANGE with REV, BYSCORE, BYLEX, LIMIT options (Redis 6.2+)
- [x] Write tests for each new command including edge cases
- [x] Write Redis compatibility test (using dev container)
- [x] Run project test suite - must pass before next task

### Task 8: HyperLogLog Commands

**Files:**
- Create: `src/command/types/hyperloglog.rs`
- Modify: `src/storage/engine.rs` (HLL data structure support)
- Modify: `src/command/handler.rs`

- [ ] Implement HyperLogLog data structure (sparse and dense representations)
- [ ] Implement PFADD (add elements to HLL)
- [ ] Implement PFCOUNT (estimate cardinality, single and multi-key)
- [ ] Implement PFMERGE (merge multiple HLLs into one)
- [ ] Ensure storage engine handles HLL type correctly
- [ ] Write tests with known cardinality validation (error margin < 1%)
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 9: Geospatial Commands

**Files:**
- Create: `src/command/types/geo.rs`
- Modify: `src/storage/engine.rs` (geo uses sorted sets internally)
- Modify: `src/command/handler.rs`

- [ ] Implement geohash encoding/decoding utility
- [ ] Implement GEOADD (add geospatial members using sorted set with geohash scores)
- [ ] Implement GEODIST (distance between members with unit options: m, km, mi, ft)
- [ ] Implement GEOHASH (get geohash strings)
- [ ] Implement GEOPOS (get longitude/latitude)
- [ ] Implement GEOSEARCH (search by radius or box from member or coordinates, with COUNT and ASC/DESC)
- [ ] Implement GEOSEARCHSTORE (store GEOSEARCH results)
- [ ] Implement GEORADIUS / GEORADIUSBYMEMBER (deprecated but needed for compatibility)
- [ ] Write tests for all geo commands with real-world coordinate data
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 10: Stream Commands

**Files:**
- Create: `src/storage/stream.rs` (stream data structure)
- Create: `src/command/types/stream.rs`
- Modify: `src/storage/engine.rs`
- Modify: `src/command/handler.rs`

- [ ] Design and implement Stream data structure: entry IDs (timestamp-sequence), radix tree storage, consumer groups, pending entries list (PEL)
- [ ] Implement XADD (add entry with auto-generated or explicit ID, MAXLEN/MINID trimming, NOMKSTREAM)
- [ ] Implement XLEN (stream length)
- [ ] Implement XRANGE / XREVRANGE (get entries by ID range with COUNT)
- [ ] Implement XREAD (read from multiple streams with optional COUNT and BLOCK)
- [ ] Implement XTRIM (trim stream by MAXLEN or MINID with approximate ~)
- [ ] Implement XDEL (delete entries by ID)
- [ ] Implement XINFO (STREAM, GROUPS, CONSUMERS subcommands)
- [ ] Implement XGROUP CREATE / SETID / DESTROY / DELCONSUMER / CREATECONSUMER
- [ ] Implement XREADGROUP (read from consumer group with GROUP, COUNT, BLOCK, NOACK)
- [ ] Implement XACK (acknowledge messages)
- [ ] Implement XPENDING (get pending entries with filtering)
- [ ] Implement XCLAIM / XAUTOCLAIM (claim/auto-claim pending messages)
- [ ] Write tests for stream CRUD, consumer groups, blocking reads, trimming
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 11: Transaction Commands (MULTI/EXEC/WATCH)

**Files:**
- Create: `src/command/transaction.rs`
- Modify: `src/command/handler.rs`
- Modify: `src/networking/mod.rs` (per-connection transaction state)
- Modify: `src/storage/engine.rs` (WATCH key versioning)

- [ ] Add per-connection transaction state: command queue, WATCH list, dirty flag
- [ ] Implement MULTI (start queuing commands)
- [ ] Implement EXEC (execute queued commands atomically, return array of results)
- [ ] Implement DISCARD (discard queued commands)
- [ ] Implement WATCH (optimistic locking - track key versions, abort on change)
- [ ] Implement UNWATCH (clear all watched keys)
- [ ] Handle errors in queued commands (EXECABORT vs per-command errors)
- [ ] Ensure commands within MULTI/EXEC are properly isolated
- [ ] Write tests: basic transactions, WATCH with concurrent modification, nested errors, DISCARD
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 12: Lua Scripting (EVAL/EVALSHA/FUNCTION)

**Files:**
- Create: `src/scripting/mod.rs` (Lua VM integration)
- Create: `src/scripting/redis_api.rs` (redis.call/redis.pcall bindings)
- Modify: `src/command/handler.rs`
- Modify: `Cargo.toml` (add mlua or rlua dependency)

- [ ] Add Lua scripting engine dependency (mlua with Lua 5.1 compatibility)
- [ ] Implement script execution sandbox with redis.call() and redis.pcall() bindings
- [ ] Implement KEYS and ARGV passing to scripts
- [ ] Implement EVAL (execute Lua script with keys and args)
- [ ] Implement EVALSHA (execute cached script by SHA1)
- [ ] Implement EVALSHA_RO / EVAL_RO (read-only variants)
- [ ] Implement SCRIPT LOAD / EXISTS / FLUSH / KILL subcommands
- [ ] Implement Redis 7.0 Functions: FUNCTION LOAD / DELETE / FLUSH / KILL / LIST / DUMP / RESTORE
- [ ] Implement FCALL / FCALL_RO (call named functions)
- [ ] Implement script replication (propagate scripts or their effects)
- [ ] Ensure script atomicity (no other commands execute during script)
- [ ] Write tests: basic scripts, error handling, KEYS/ARGV, script caching, functions
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 13: ACL System

**Files:**
- Create: `src/security/acl.rs`
- Modify: `src/security/mod.rs`
- Modify: `src/command/handler.rs`
- Modify: `src/networking/mod.rs` (per-connection user context)

- [ ] Design ACL data model: users, passwords, command permissions, key patterns, channels
- [ ] Implement ACL SETUSER (create/modify users with complex rule syntax: +command, -command, ~keypattern, &channelpattern, >password, <password, on/off, allcommands, allkeys, nopass, reset)
- [ ] Implement ACL GETUSER (get user details)
- [ ] Implement ACL DELUSER (delete users)
- [ ] Implement ACL LIST (list all users and rules)
- [ ] Implement ACL USERS (list usernames)
- [ ] Implement ACL WHOAMI (current user)
- [ ] Implement ACL CAT (list command categories)
- [ ] Implement ACL GENPASS (generate password)
- [ ] Implement ACL LOG (log denied commands)
- [ ] Implement ACL SAVE / LOAD (persist ACL to file)
- [ ] Integrate ACL checks into command dispatch (check permissions before execution)
- [ ] Update AUTH command to support username + password (AUTH username password)
- [ ] Write tests for each ACL subcommand and permission enforcement
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 14: Connection & Server Commands

**Files:**
- Modify: `src/command/handler.rs`
- Modify: `src/networking/mod.rs`
- Create: `src/command/types/server.rs` (consolidate server commands)
- Create: `src/command/types/connection.rs`

- [ ] Implement QUIT / RESET (close/reset connection)
- [ ] Implement ECHO (echo message back)
- [ ] Implement CLIENT subcommands: LIST, INFO, SETNAME, GETNAME, ID, KILL, PAUSE, UNPAUSE, REPLY, NO-EVICT, NO-TOUCH
- [ ] Implement CLIENT TRACKING (server-assisted client-side caching with invalidation)
- [ ] Implement COMMAND subcommands: COUNT, INFO, DOCS, LIST, GETKEYS
- [ ] Implement CONFIG SET (currently only GET is implemented)
- [ ] Implement CONFIG REWRITE / RESETSTAT
- [ ] Implement SAVE / BGSAVE / BGREWRITEAOF / LASTSAVE (expose persistence commands)
- [ ] Implement SHUTDOWN (graceful server shutdown with SAVE/NOSAVE options)
- [ ] Implement SLOWLOG (slow query tracking and retrieval)
- [ ] Implement LATENCY subcommands (LATEST, HISTORY, RESET, GRAPH)
- [ ] Implement MEMORY subcommands (USAGE, STATS, DOCTOR, MALLOC-STATS, PURGE)
- [ ] Implement DEBUG subcommands (SLEEP, SET-ACTIVE-EXPIRE, JMAP, RELOAD, etc.)
- [ ] Implement SWAPDB (swap two databases)
- [ ] Implement TIME (server time)
- [ ] Implement LOLWUT (version art easter egg)
- [ ] Expand INFO command to include all standard Redis sections
- [ ] Write tests for each new command
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 15: Pub/Sub Enhancements (Sharded Pub/Sub)

**Files:**
- Modify: `src/pubsub/manager.rs`
- Modify: `src/command/handler.rs`

- [ ] Implement SSUBSCRIBE (sharded channel subscribe - Redis 7.0+)
- [ ] Implement SUNSUBSCRIBE (sharded channel unsubscribe)
- [ ] Implement SPUBLISH (publish to shard channel)
- [ ] Implement PUBSUB SHARDCHANNELS / SHARDNUMSUB
- [ ] Write tests for sharded pub/sub
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 16: Complete Replication System

**Files:**
- Modify: `src/replication/mod.rs`
- Modify: `src/command/handler.rs`
- Modify: `src/persistence/rdb.rs` (RDB transfer during sync)

- [ ] Complete PSYNC partial resynchronization (backlog buffer, offset-based sync)
- [ ] Implement full RDB transfer during initial sync
- [ ] Implement command stream propagation to replicas
- [ ] Implement REPLICAOF / SLAVEOF commands (dynamic replication configuration)
- [ ] Implement ROLE command (return replication role and state)
- [ ] Implement replication backlog for partial resync
- [ ] Implement replica read-only mode
- [ ] Implement WAIT command (wait for replica acknowledgement)
- [ ] Implement FAILOVER command (manual failover)
- [ ] Write tests for full sync, partial sync, failover scenarios (using dev containers for multi-instance testing)
- [ ] Write Redis compatibility test (using dev container)
- [ ] Run project test suite - must pass before next task

### Task 17: Redis Cluster Support

**Files:**
- Create: `src/cluster/mod.rs`
- Create: `src/cluster/slot.rs` (hash slot management)
- Create: `src/cluster/gossip.rs` (cluster bus protocol)
- Create: `src/cluster/migration.rs` (slot migration)
- Modify: `src/command/handler.rs`
- Modify: `src/networking/mod.rs`

- [ ] Implement hash slot calculation (CRC16 mod 16384) and hash tag support
- [ ] Implement cluster topology: node roles, slot assignment, epoch-based versioning
- [ ] Implement cluster bus (gossip protocol on port+10000 for inter-node communication)
- [ ] Implement CLUSTER subcommands: INFO, NODES, SLOTS, SHARDS, MYID, MEET, FORGET, REPLICATE, RESET, ADDSLOTS, DELSLOTS, FLUSHSLOTS, SETSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT, KEYSLOT, FAILOVER, SAVECONFIG, SET-CONFIG-EPOCH, COUNT-FAILURE-REPORTS, LINKS
- [ ] Implement MOVED and ASK redirections for cross-slot operations
- [ ] Implement ASKING command (redirect handling)
- [ ] Implement READONLY / READWRITE (replica reads in cluster mode)
- [ ] Implement slot migration (IMPORTING/MIGRATING states)
- [ ] Implement MIGRATE command (move keys between nodes)
- [ ] Implement cluster-aware key validation (reject cross-slot commands)
- [ ] Implement automatic failover detection and promotion
- [ ] Write tests for slot calculation, redirections, topology, migration (using dev containers for multi-node cluster testing)
- [ ] Write multi-node integration test (using dev containers to spin up 6-node cluster)
- [ ] Run project test suite - must pass before next task

### Task 18: Redis Sentinel Support

**Files:**
- Create: `src/sentinel/mod.rs`
- Create: `src/sentinel/monitor.rs`
- Create: `src/sentinel/failover.rs`
- Modify: `src/command/handler.rs`

- [ ] Implement Sentinel mode (separate binary mode or config flag)
- [ ] Implement master monitoring with configurable quorum
- [ ] Implement SENTINEL subcommands: MASTERS, MASTER, REPLICAS/SLAVES, SENTINELS, GET-MASTER-ADDR-BY-NAME, RESET, FAILOVER, CKQUORUM, FLUSHCONFIG, MONITOR, REMOVE, SET, IS-MASTER-DOWN-BY-ADDR, SIMULATE-FAILURE, PENDING-SCRIPTS, INFO-CACHE, MYID, CONFIG
- [ ] Implement SDOWN (subjective down) and ODOWN (objective down) detection
- [ ] Implement automatic failover: leader election, replica promotion, reconfiguration
- [ ] Implement Sentinel pub/sub channels (+switch-master, +sdown, etc.)
- [ ] Implement configuration provider mode (clients query Sentinel for master address)
- [ ] Write tests for monitoring, failover, multi-sentinel coordination (using dev containers for multi-sentinel topology)
- [ ] Run project test suite - must pass before next task

### Task 19: Module System (Minimal Compatibility)

**Files:**
- Create: `src/module/mod.rs`
- Modify: `src/command/handler.rs`

- [ ] Implement MODULE LIST (return empty list or loaded modules)
- [ ] Implement MODULE LOADEX / LOAD / UNLOAD stubs (return appropriate errors or no-ops)
- [ ] Document module system limitations (rLightning is not C-extension compatible)
- [ ] Write tests for module commands
- [ ] Run project test suite - must pass before next task

### Task 20: Comprehensive Benchmark Suite (rLightning vs Redis)

**Files:**
- Modify: `benches/redis_comparison_bench.rs` (expand existing)
- Create: `benches/command_category_bench.rs`
- Create: `benches/advanced_features_bench.rs`
- Create: `benches/cluster_bench.rs`
- Create: `scripts/benchmark_comparison.sh`

**Note: Benchmarks run locally only (not in CI/CD) due to hardware-dependent results.**

- [ ] String command benchmarks: SET/GET/MSET/MGET/INCR/APPEND with various value sizes (64B, 1KB, 10KB, 100KB)
- [ ] List command benchmarks: LPUSH/RPUSH/LPOP/RPOP/LRANGE with varying list sizes
- [ ] Hash command benchmarks: HSET/HGET/HGETALL with varying field counts
- [ ] Set command benchmarks: SADD/SINTER/SUNION with varying set sizes
- [ ] Sorted set command benchmarks: ZADD/ZRANGE/ZRANGEBYSCORE with varying sizes
- [ ] Stream command benchmarks: XADD/XREAD/XRANGE throughput
- [ ] Pub/Sub benchmarks: message throughput, fan-out performance
- [ ] Transaction benchmarks: MULTI/EXEC overhead vs non-transactional
- [ ] Pipeline benchmarks: batched command throughput
- [ ] Persistence benchmarks: RDB save/load times, AOF write throughput
- [ ] Memory efficiency benchmarks: memory usage per key for each data type
- [ ] Concurrent client benchmarks: throughput with 1/10/100/1000 clients
- [ ] Latency benchmarks: p50/p95/p99 latency for common operations
- [ ] Blocking command benchmarks: BLPOP/BRPOP wakeup latency
- [ ] Lua scripting benchmarks: script execution overhead
- [ ] Create automated comparison script that runs both Redis and rLightning, generates comparison report
- [ ] Write benchmark result validation tests (ensure benchmarks complete without errors)
- [ ] Run all benchmarks locally - verify they complete successfully

### Task 21: Full Redis Compatibility Test Suite (Dev Container-Based)

**Files:**
- Create: `tests/redis_full_compatibility_test.rs`
- Create: `tests/redis_cluster_test.rs`
- Create: `tests/redis_sentinel_test.rs`
- Create: `.devcontainer/devcontainer.json` (dev container config for test environment)
- Create: `.devcontainer/docker-compose.test.yml` (Redis + rLightning side-by-side for compatibility tests)
- Modify: existing test files as needed

- [ ] Set up dev container configuration: Docker Compose with Redis 7.x and rLightning containers for side-by-side compatibility testing
- [ ] Create comprehensive command compatibility test: every implemented command tested against real Redis for identical behavior (using dev container)
- [ ] Test error responses match Redis exactly (error messages, error types)
- [ ] Test edge cases: empty keys, binary data, large values, Unicode, special characters
- [ ] Test RESP3 protocol compatibility with Redis 7.x
- [ ] Test transaction isolation and error handling matches Redis behavior
- [ ] Test Lua scripting compatibility with Redis Lua API
- [ ] Test ACL behavior matches Redis ACL system
- [ ] Test cluster redirect behavior with redis-cli --cluster (using dev container with multi-node setup)
- [ ] Test sentinel failover behavior (using dev container with sentinel topology)
- [ ] Test persistence (RDB/AOF) format compatibility where applicable
- [ ] Test client library compatibility (test with redis-rs crate against both Redis and rLightning in dev container)
- [ ] Run full test suite - all tests must pass

### Task 22: Automated Acceptance Tests & CI/CD Pipeline

**Files:**
- Create: `.github/workflows/ci.yml` (main CI pipeline)
- Create: `.github/workflows/benchmarks.yml` (optional local-only benchmark reference)
- Create: `tests/acceptance_test.rs` (automated acceptance tests)
- Modify: `Cargo.toml` (add dev-dependencies for test tooling if needed)

**All acceptance criteria are automated tests. No manual tests. No ignored (#[ignore]) tests anywhere in the codebase.**

- [ ] Create automated acceptance test: connect with redis crate client, run commands from each category (strings, lists, sets, hashes, sorted sets, streams, bitmap, HLL, geo, pub/sub, transactions, scripting, ACL), verify correct behavior
- [ ] Create automated acceptance test: run application-level scenarios simulating real workloads (session store, cache, rate limiter, leaderboard, message queue)
- [ ] Create automated acceptance test: start cluster mode in dev containers, verify slot assignment and redirections work correctly
- [ ] Create automated acceptance test: start sentinel mode in dev containers, verify failover completes correctly
- [ ] Create automated test: RESP3 negotiation with modern Redis client libraries
- [ ] Create CI/CD pipeline (.github/workflows/ci.yml) with the following jobs:
- [ ] `build`: cargo build --release on Linux, macOS
- [ ] `test-unit`: cargo test --lib (unit tests)
- [ ] `test-integration`: cargo test --test '*' (all integration tests including acceptance, compatibility, cluster, sentinel) using Docker service containers
- [ ] `lint`: cargo clippy -- -D warnings
- [ ] `format`: cargo fmt -- --check
- [ ] `coverage`: run cargo-tarpaulin or llvm-cov, verify 80%+ coverage, upload report
- [ ] `docker-test`: build Docker image, run Redis compatibility tests against containerized rLightning
- [ ] `redis-compat`: start both real Redis and rLightning in Docker service containers, run compatibility test suite against both
- [ ] Audit entire codebase: remove all #[ignore] attributes from tests; fix or delete any tests that cannot run
- [ ] Verify all tests pass in CI: `cargo test` with no failures and no ignored tests
- [ ] Run linter: `cargo clippy -- -D warnings` with zero warnings
- [ ] Run formatter: `cargo fmt -- --check` with no diffs
- [ ] Verify test coverage meets 80%+
- [ ] Run benchmarks locally: `cargo bench` completes successfully (benchmarks are local-only, not in CI)
- [ ] Run project test suite - must pass before next task

### Task 23: Redesign Documentation Site (spotctl.com-style)

**Files:**
- Modify: `site/mkdocs.yml`
- Modify: `site/docs/stylesheets/extra.css` (complete rewrite)
- Modify: `site/docs/index.md` (redesign as modern landing page)
- Create: `site/docs/javascripts/extra.js` (animations and scroll effects)
- Modify: `site/docs/getting-started.md`
- Modify: `site/docs/quick-start.md`
- Modify: `site/docs/configuration.md`
- Modify: `site/docs/architecture.md`
- Modify: `site/docs/use-cases.md`
- Modify: `site/docs/commands/index.md`
- Create: `site/docs/commands/` pages for each new command category (bitmap, hyperloglog, geo, streams, transactions, scripting, acl, cluster, sentinel)

- [ ] Redesign site theme with dark color scheme: deep navy background (#0a0e14), coral accent color (#E8553E), modern typography (IBM Plex Sans body, Fira Code for code)
- [ ] Build hero section on index.md: large headline with gradient glow effects, tagline, CTA buttons (Get Started, GitHub)
- [ ] Create "Why rLightning" section: 4-column responsive grid with key selling points (performance, Redis compatibility, Rust safety, modern architecture)
- [ ] Create "Features" section: card-based layout showcasing capabilities (each data type, clustering, persistence, pub/sub, scripting)
- [ ] Create "How It Works" section: step-by-step walkthrough with code examples showing Redis CLI usage
- [ ] Create "Benchmark Results" section: visual comparison charts (rLightning vs Redis) with p50/p95/p99 latency and throughput numbers
- [ ] Create "Installation" section: multi-method install (cargo, docker, binary download) with tabbed code blocks
- [ ] Add glassmorphism navigation bar with backdrop blur, logo, and section links
- [ ] Implement CSS animations: fade-in on scroll (intersection observer), hover transitions on cards, subtle animated dot-grid background pattern
- [ ] Configure MkDocs Material theme overrides for dark palette, custom fonts, and extended navigation
- [ ] Update command reference pages: add new command categories (bitmap, hyperloglog, geo, streams, transactions, scripting, ACL, cluster, sentinel) with usage examples and compatibility notes
- [ ] Update architecture.md with cluster and sentinel architecture diagrams
- [ ] Add benchmark results page with detailed comparison data and methodology
- [ ] Ensure responsive design: grid layouts adapt from multi-column to single-column on mobile
- [ ] Update CLAUDE.md with new commands, features, and architecture notes
- [ ] Update README.md with new feature list
- [ ] Move this plan to `docs/plans/completed/`
