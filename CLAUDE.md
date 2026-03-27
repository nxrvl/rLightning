# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

rLightning is a high-performance, Redis 7.x compatible in-memory data store built in Rust. It implements the full Redis Serialization Protocol (RESP2 and RESP3) and supports 400+ Redis commands across all data types: strings, hashes, lists, sets, sorted sets, streams, bitmaps, HyperLogLog, and geospatial indexes. Features include transactions (MULTI/EXEC/WATCH), Lua scripting (EVAL/Functions), ACL security, cluster mode, Sentinel HA, pub/sub (including sharded), and full persistence (RDB/AOF/hybrid).

## Key Commands

### Building and Running

```bash
# Build the project
cargo build --release

# Run the server with default settings
cargo run --release

# Run with custom configuration
cargo run --release -- --config path/to/config.toml

# Run with command-line options
cargo run --release -- --port 6380 --max-memory-mb 512
```

### Testing

```bash
# Run all tests
cargo test

# Run a specific test
cargo test <test_name>

# Run integration tests
cargo test --test integration_test

# Run Redis compatibility tests (includes Docker testing)
cargo test --test redis_compatibility_test

# Run tests with verbose output
cargo test -- --nocapture

# Run authentication tests
cargo test --test auth_integration_test
```

### Docker Testing

The Redis compatibility test includes comprehensive Docker testing with automatic image building and retry logic for network issues:

```bash
# Build Docker image with retry logic
./scripts/build-docker.sh

# Run Redis compatibility test with Docker
cargo test --test redis_compatibility_test

# Start container with docker-compose
docker-compose up -d

# Test against running container
docker run -d -p 6379:6379 rlightning:latest
```

**Docker Test Features:**
- **Automatic image building** with 3 retry attempts
- **Base image pulling** with network failure retry logic
- **Container health checks** and connectivity testing
- **Fallback to local testing** if Docker is unavailable
- **Comprehensive Redis protocol testing** in containerized environment
- **Automatic cleanup** of test containers

### Multi-Language Compatibility Tests

The `tests/docker-compat/` directory contains a comprehensive multi-language compatibility test suite that tests rLightning against real Redis 7 using Go (go-redis/v9), JavaScript (ioredis), and Python (redis-py) client libraries. Each language implements ~231 tests across 25 categories.

```bash
# Run full suite end-to-end (local mode - uses local Go/Node/Python, Docker for servers)
cd tests/docker-compat && ./run-tests.sh --local

# Run full suite with Docker clients (requires Docker networking for package downloads)
cd tests/docker-compat && ./run-tests.sh

# Run individual client against a server
REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=test_password \
  go run ./tests/docker-compat/go-client/...
REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=test_password \
  node tests/docker-compat/js-client/index.js
REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=test_password \
  python tests/docker-compat/python-client/test_compat.py

# Generate comparison report from collected results
python tests/docker-compat/report/generate-report.py --results-dir tests/docker-compat/results/
```

**Test Categories:** Connection & Auth, Strings, Hashes, Lists, Sets, Sorted Sets, Key Management, Transactions, Pub/Sub, Pipelining, Lua Scripting, Lua Scripting Advanced, Streams, Streams Advanced, Advanced Types (Bitmap/HLL/Geo), Edge Cases, Server Commands, ACL & Security, Blocking Commands, Memory Management & Eviction, Persistence (RDB/AOF), Replication, Cluster Mode, Sentinel, Language-Specific (Go/JS/Python)

**Known Incompatibilities:** See `tests/docker-compat/KNOWN-INCOMPATIBILITIES.txt` for the current list of known behavioral differences between rLightning and Redis 7.

### Benchmarking

```bash
# Run all benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench storage_bench
cargo bench --bench protocol_bench
cargo bench --bench throughput_bench

# Run a specific benchmark function
cargo bench --bench storage_bench -- set_get
```

## Architecture

### Core Components

1. **Networking (`src/networking/`)**:
   - RESP2 and RESP3 protocol support with HELLO negotiation and per-connection protocol version tracking
   - Zero-copy RESP parser (`src/networking/raw_command.rs`): `RawCommand<'buf>` parses commands as byte slices directly from the read buffer, avoiding heap allocations on the hot path. Falls back to owned `RespValue` for MULTI queue, AOF logging, and RESP3 features
   - RESP3 response conversion: Map type for HGETALL/CONFIG/XINFO/HSCAN/ZSCAN/SSCAN/XRANGE/XREAD/COMMAND DOCS, Set type for SMEMBERS, Double for float commands, native Null/Boolean/Push types
   - Pipeline error isolation: per-command errors in a pipeline are captured as RespValue::Error instead of killing the entire pipeline
   - Pipeline batching (`src/storage/batch.rs`): sort-then-group strategy annotates each command with `(original_index, shard_index, is_read)`, sorts by shard, forms batches under a single lock acquisition per shard, then reorders responses back to original command order. Works with RESP2/RESP3 and security-enabled connections (ACL checks per-command within batch loop)
   - Batched pipeline post-processing: prefix index updates, AOF logging, and replication propagation are collected during the batch loop and flushed in single bulk calls after completion, avoiding per-command overhead
   - Static response interning: OK, PONG, NIL, 0, 1, and empty array responses use `Bytes::from_static` to avoid allocation
   - TCP_NODELAY enabled on all client connections immediately after accept
   - Connection tracking via `Arc<DashMap<u64, ClientInfo>>` for real CLIENT LIST/INFO/ID/SETNAME
   - Async TCP server with Tokio, per-connection state (db_index, protocol_version, client name, subscriptions)
   - Write coalescing: small responses buffered and flushed when read buffer is empty or write buffer is full
   - Buffer memory management: response and partial command buffers shrink when capacity exceeds 64KB after clearing, preventing unbounded memory growth from large commands

2. **Storage Engine (`src/storage/`)**:
   - Sharded storage (`src/storage/sharded.rs`): cache-line aligned shards (`#[repr(align(128))]`) with `parking_lot::RwLock` per shard, replacing DashMap. Shard count is `(num_cpus * 16).next_power_of_two().clamp(16, 1024)` with FxHash-based shard selection
   - Native data types (`src/storage/value.rs`): `StoreValue` enum with `Str`, `Hash(FxHashMap)`, `Set(FxHashSet)`, `ZSet(BTreeMap + FxHashMap)`, `List(VecDeque)`, `Stream` — no bincode serialization on hot paths
   - Small String Optimization: `CompactValue` with inline storage for values <= 23 bytes, avoiding heap allocation for short strings
   - Read-optimized locking: GET, EXISTS, TTL, TYPE use `RwLock::read()` for maximum concurrent reader throughput
   - Atomic operations for all mutable data types: strings, lists, sets, sorted sets, geo (via `atomic_modify`)
   - Inline atomic operations: INCR, DECR, APPEND, SETNX operate directly within shard write guard
   - Cross-key atomicity via `lock_keys()` with sorted shard-level locking for deadlock prevention (SMOVE, GEOSEARCHSTORE, multi-shard MSET); MSET uses a single-shard fast path when all keys hash to the same shard, avoiding cross-shard lock overhead
   - Conditional SET operations: `set_nx()`, `set_xx()`, `set_with_options()` for NX/XX/GET flag combinations
   - Multi-database support (16 databases, SELECT routing via task-local `CURRENT_DB_INDEX`)
   - Runtime config store: `runtime_config` DashMap for CONFIG SET/GET with glob pattern matching
   - Per-shard expiration heaps with round-robin background expiration and cached clock (`CACHED_OFFSET_US: AtomicU64` for monotonic microseconds, `CACHED_WALL_MS: AtomicU64` for wall-clock milliseconds, both updated every 1ms via `src/storage/clock.rs`; `cached_now_ms()` provides fast epoch-millis for XADD auto-ID generation)
   - Probabilistic eviction with per-shard `ArrayVec<EvictionCandidate, 16>` candidate buffer
   - O(1) memory tracking via `cached_mem_size` field on `Entry` (`src/storage/item.rs`): `ModifyResult::Keep(i64)` carries byte deltas from mutation closures, eliminating O(n) `calculate_entry_size` calls from the write hot path. Used by XADD/XDEL/XTRIM for in-place stream mutation without deep-clone. Recalculated on RDB restore and AOF replay.
   - Conditional key versioning: `active_watch_count: AtomicU32` tracks active WATCH sessions; `bump_key_version` is skipped entirely when no clients are watching, avoiding DashMap insert on every write. `key_versions` map is periodically cleaned in the expiration task.
   - Transaction key locking with per-shard version tracking for WATCH
   - Per-shard memory tracking (summed for global INFO/CONFIG queries)
   - Blocking command infrastructure (per-key wait queues)

3. **Command Handler (`src/command/`)**:
   - 400+ Redis commands across all categories
   - Two-level byte dispatch: first byte (26 letters) then length + exact byte comparison, eliminating `to_lowercase()` allocation from the hot path
   - Fast path for top-20 commands (SET, GET, DEL, INCR, HSET, HGET, LPUSH, RPUSH, etc.) with fallback match for remaining commands
   - Transaction support (MULTI/EXEC/WATCH) in `src/command/transaction.rs`
   - Per-type handlers in `src/command/types/`

4. **Persistence (`src/persistence/`)**:
   - COW-based RDB snapshots: per-shard read-lock, clone HashMap (cheap via refcount), release lock, serialize in background — no global write lock on snapshot path
   - Lock-free AOF appending via MPSC channel: command threads send `AofEntry` to dedicated AOF writer thread with batch-drain and `fsync()` on everysec schedule
   - RDB snapshot persistence with background save, supports all data types across all 16 databases
   - AOF rewrite generates correct reconstruction commands per type, including stream consumer groups and pending entries
   - Full AOF replay via CommandHandler (routes all commands, not just a subset)
   - Hybrid persistence model (RDB + AOF combined)

5. **Replication (`src/replication/`)**:
   - Full and partial sync (PSYNC) with replication backlog
   - Command stream propagation with per-database routing (SELECT-aware, not hardcoded to DB 0)
   - Transaction atomicity: MULTI/EXEC blocks from master preserved as transactions on replicas
   - Replica read-only mode
   - FAILOVER command support

6. **Security (`src/security/`)**:
   - ACL system with per-user command/key/channel permissions (`src/security/acl.rs`)
   - Password and named-user authentication (AUTH username password)
   - ACL LOG for denied command tracking

7. **Pub/Sub (`src/pubsub/`)**:
   - Channel and pattern subscriptions
   - Sharded pub/sub (SSUBSCRIBE, SPUBLISH) for Redis 7.0+
   - Real-time message broadcasting

8. **Lua Scripting (`src/scripting/`)**:
   - Lua 5.1 via mlua with redis.call()/redis.pcall() bindings
   - EVAL/EVALSHA, SCRIPT LOAD/EXISTS/FLUSH
   - Redis 7.0 Functions: FUNCTION LOAD/FCALL

9. **Cluster (`src/cluster/`)**:
   - Hash slot calculation (CRC16 mod 16384) with hash tags
   - Gossip protocol with active bus listener (`start_cluster_bus`) and periodic cron (`cluster_cron`), spawned on startup when cluster mode is enabled
   - MOVED/ASK redirections with per-connection `asking` flag in command dispatch; slot-exempt commands (PING, INFO, CLUSTER, AUTH, etc.) bypass redirection checks
   - Slot migration, automatic failover

10. **Sentinel (`src/sentinel/`)**:
    - Master monitoring with quorum-based SDOWN/ODOWN detection
    - Automatic failover with leader election
    - Configuration provider mode

11. **Module System (`src/module/`)**:
    - MODULE LIST/LOAD/UNLOAD stubs for compatibility

12. **Utilities (`src/utils/`)**:
    - Shared glob pattern matching (`glob_match`, `glob_match_bytes`) used by KEYS, SCAN, ACL, Sentinel, Pub/Sub
    - Supports Redis glob syntax: `*`, `?`, `[abc]`, `[a-z]`, `[^abc]`, `\x` escapes

### Data Flow

1. Clients connect via TCP (TCP_NODELAY enabled) using the Redis protocol
2. The zero-copy RESP parser extracts commands as byte slices from the read buffer (`RawCommand`)
3. Two-level byte dispatch routes commands to handlers without string allocation
4. For pipelines, commands are sorted by shard index then grouped into batches under a single lock acquisition per shard; responses are reordered back to original command order; post-batch processing (prefix index updates, AOF logging, replication propagation) is done in bulk
5. Command handlers interact with the sharded storage engine using native data types (no serialization)
6. Results are serialized back as RESP responses (with static interning for common responses)
7. If replication is enabled, commands are propagated to replicas
8. If persistence is enabled, commands are sent via MPSC channel to the AOF writer or trigger COW RDB snapshots

## Configuration

Configuration is managed through TOML files, environment variables, or command-line arguments. Priority order:
1. Command-line arguments
2. Environment variables
3. Configuration file
4. Default values

Key configuration areas:
- Server: host, port, workers
- Storage: memory limits, eviction policy, TTL settings
- Persistence: mode (RDB/AOF/hybrid), paths, sync policies
- Replication: master/replica configuration
- Security: authentication settings, ACL configuration
- Cluster: cluster mode, node configuration
- Sentinel: monitoring, quorum, failover settings

## Documentation

### Building Documentation

The project includes a comprehensive documentation site built with MkDocs Material.

**Prerequisites:**
```bash
pip install mkdocs-material mkdocs-minify-plugin
```

**Local Development:**
```bash
# Serve documentation with live reload
cd site
mkdocs serve

# Access at http://localhost:8000
```

**Building Static Site:**
```bash
cd site
mkdocs build

# Output in site/site/ directory
```

**Docker Build:**
```bash
# Build documentation container
docker build -f Dockerfile.site -t rlightning-docs .

# Run documentation server
docker run -d -p 8080:8080 rlightning-docs

# Access at http://localhost:8080
```

### Documentation Structure

```
site/
├── mkdocs.yml           # MkDocs configuration
├── docs/                # Documentation source
│   ├── index.md         # Homepage (modern landing page)
│   ├── getting-started.md
│   ├── quick-start.md
│   ├── configuration.md
│   ├── architecture.md  # Includes cluster/sentinel diagrams
│   ├── benchmarks.md    # Performance comparison vs Redis
│   ├── use-cases.md
│   ├── commands/        # Command reference
│   │   ├── index.md     # Overview with all categories
│   │   ├── strings.md, hashes.md, lists.md, sets.md, sorted-sets.md
│   │   ├── bitmap.md, hyperloglog.md, geo.md, streams.md
│   │   ├── transactions.md, scripting.md, pubsub.md
│   │   ├── acl.md, cluster.md, sentinel.md, server.md
│   ├── stylesheets/
│   │   └── extra.css    # Dark navy/coral theme
│   └── javascripts/
│       └── extra.js     # Scroll animations
└── overrides/           # Theme overrides
```

### Updating Documentation

1. Edit markdown files in `site/docs/`
2. Update `site/mkdocs.yml` if adding new pages
3. Test locally with `mkdocs serve`
4. Commit changes - GitHub Actions will auto-deploy

## Claude instructions
- Use Redis commands to interact with the server
- Use Redis commands to manage data
- Use Redis commands to manage security
- Track progress in different files
- When updating documentation, follow the MkDocs structure in site/docs/
- **CRITICAL**: NEVER add Claude Code attribution footers to commit messages (no "🤖 Generated with Claude Code" or "Co-Authored-By: Claude" lines). Keep commit messages clean and professional.
