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
   - RESP2 and RESP3 protocol support with HELLO negotiation
   - Async TCP server with Tokio, per-connection state tracking
   - Buffered I/O with pipeline support

2. **Storage Engine (`src/storage/`)**:
   - Lock-free concurrent access via DashMap
   - All data types: strings, hashes, lists, sets, sorted sets, streams, HLL, bitmaps
   - TTL handling (lazy + periodic), configurable eviction (LRU, Random)
   - Blocking command infrastructure (per-key wait queues)

3. **Command Handler (`src/command/`)**:
   - 400+ Redis commands across all categories
   - Transaction support (MULTI/EXEC/WATCH) in `src/command/transaction.rs`
   - Per-type handlers in `src/command/types/`

4. **Persistence (`src/persistence/`)**:
   - RDB snapshot persistence with background save
   - AOF logging with configurable sync (always/everysec/no)
   - Hybrid persistence model

5. **Replication (`src/replication/`)**:
   - Full and partial sync (PSYNC) with replication backlog
   - Command stream propagation, replica read-only mode
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
   - Gossip protocol, MOVED/ASK redirections
   - Slot migration, automatic failover

10. **Sentinel (`src/sentinel/`)**:
    - Master monitoring with quorum-based SDOWN/ODOWN detection
    - Automatic failover with leader election
    - Configuration provider mode

11. **Module System (`src/module/`)**:
    - MODULE LIST/LOAD/UNLOAD stubs for compatibility

### Data Flow

1. Clients connect to the server using the Redis protocol
2. The server parses incoming RESP commands
3. Commands are validated and dispatched to appropriate handlers
4. Command handlers interact with the storage engine
5. Results are serialized back as RESP responses
6. If replication is enabled, commands are propagated to replicas
7. If persistence is enabled, commands are logged to AOF or trigger RDB snapshots

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
