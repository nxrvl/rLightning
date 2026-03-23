# rLightning ⚡

[![Build Status](https://github.com/nxrvl/rLightning/workflows/CI/badge.svg)](https://github.com/nxrvl/rLightning/actions)
[![Crates.io](https://img.shields.io/crates/v/rlightning.svg)](https://crates.io/crates/rlightning)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/docker/automated/nxrvl/rlightning.svg)](https://hub.docker.com/r/nxrvl/rlightning)

A high-performance, Redis 7.x compatible in-memory data store built from the ground up in Rust. Full protocol support, all data types, and blazing speed with memory safety guarantees.

## Why rLightning?

- **High Performance**: Sharded storage with cache-line aligned locks, Tokio async I/O, sub-millisecond latency
- **Full Redis 7.x Compatibility**: RESP2/RESP3 protocols, 400+ commands, drop-in replacement
- **All Data Types**: Strings, hashes, lists, sets, sorted sets, streams, bitmaps, HyperLogLog, geospatial
- **Transactions**: MULTI/EXEC with WATCH-based optimistic locking
- **Lua Scripting**: EVAL/EVALSHA, Redis 7.0 Functions (FCALL)
- **ACL Security**: Fine-grained per-user command, key, and channel permissions
- **Cluster Mode**: Automatic sharding with hash slots, MOVED/ASK redirections, slot migration
- **Sentinel HA**: Monitoring, automatic failover, quorum-based detection
- **Pub/Sub**: Channel, pattern, and sharded pub/sub (Redis 7.0+)
- **Persistence**: RDB snapshots, AOF logging, and hybrid mode
- **Replication**: Full/partial sync (PSYNC), replication backlog, replica read-only mode
- **Memory Safe**: Built in Rust with no unsafe code in the hot path

## Quick Start

### Using Docker (Recommended)

```bash
docker run -d -p 6379:6379 nxrvl/rlightning:latest
```

### Using Cargo

```bash
# Install from crates.io
cargo install rlightning

# Run the server
rlightning
```

### From Source

```bash
# Clone and build
git clone https://github.com/nxrvl/rLightning.git
cd rLightning
cargo build --release

# Run the server
./target/release/rlightning
```

## Basic Usage

Connect using any Redis client:

```bash
# Using redis-cli
redis-cli -h localhost -p 6379

# Basic commands
> SET mykey "Hello, rLightning!"
OK
> GET mykey
"Hello, rLightning!"
> EXPIRE mykey 60
(integer) 1
```

### Pub/Sub Messaging

rLightning supports Redis-compatible publish/subscribe messaging:

```bash
# Terminal 1 - Subscriber
redis-cli -h localhost -p 6379
> SUBSCRIBE news:sports
Reading messages... (press Ctrl-C to quit)
1) "subscribe"
2) "news:sports"
3) (integer) 1

# Terminal 2 - Publisher
redis-cli -h localhost -p 6379
> PUBLISH news:sports "Goal scored!"
(integer) 1

# Terminal 1 receives:
1) "message"
2) "news:sports"
3) "Goal scored!"

# Pattern subscriptions also supported
> PSUBSCRIBE news:*
> PSUBSCRIBE user:*:notifications
```

## Features

### Supported Redis Commands (400+)

#### Data Types
- **Strings**: GET, SET, MGET, MSET, INCR, DECR, APPEND, GETEX, GETDEL, LCS, PSETEX
- **Hashes**: HGET, HSET, HGETALL, HDEL, HEXISTS, HRANDFIELD, HSCAN
- **Lists**: LPUSH, RPUSH, LPOP, RPOP, LRANGE, LMOVE, LMPOP, BLPOP, BRPOP, BLMOVE, BLMPOP
- **Sets**: SADD, SREM, SMEMBERS, SMOVE, SINTERCARD, SMISMEMBER, SSCAN
- **Sorted Sets**: ZADD, ZRANGE (unified), ZSCORE, ZPOPMIN/MAX, BZPOPMIN/MAX, ZRANGESTORE, ZMPOP
- **Streams**: XADD, XREAD, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO
- **Bitmaps**: SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD
- **HyperLogLog**: PFADD, PFCOUNT, PFMERGE
- **Geospatial**: GEOADD, GEODIST, GEOSEARCH, GEOSEARCHSTORE, GEOPOS, GEOHASH

#### Features
- **Transactions**: MULTI, EXEC, DISCARD, WATCH, UNWATCH
- **Lua Scripting**: EVAL, EVALSHA, SCRIPT, FUNCTION, FCALL
- **Pub/Sub**: SUBSCRIBE, PUBLISH, PSUBSCRIBE, SSUBSCRIBE, SPUBLISH (sharded)
- **ACL**: ACL SETUSER, GETUSER, DELUSER, LIST, USERS, WHOAMI, CAT, LOG
- **Cluster**: CLUSTER INFO, NODES, SLOTS, SHARDS, MEET, FAILOVER, MIGRATE
- **Sentinel**: SENTINEL MASTERS, REPLICAS, FAILOVER, MONITOR, CKQUORUM
- **Server**: PING, INFO, CONFIG, CLIENT, COMMAND, MEMORY, SLOWLOG, LATENCY, DEBUG

### Memory Management

rLightning provides flexible memory management with configurable policies:

- **Memory Limits**: Set maximum memory usage
- **Eviction Policies**: 
  - LRU (Least Recently Used)
  - Random eviction
  - No eviction (return errors when full)
- **TTL-based Expiration**: Automatic cleanup of expired keys

### Persistence

Two persistence modes available:

**RDB (Snapshots)**
```bash
rlightning --persistence-mode rdb --rdb-path /data/dump.rdb
```

**AOF (Append-Only File)**
```bash
rlightning --persistence-mode aof --aof-path /data/appendonly.aof
```

**Hybrid Mode**
```bash
rlightning --persistence-mode hybrid
```

### Replication

Set up master-replica replication for high availability:

**Master Server**
```bash
rlightning --port 6379
```

**Replica Server**
```bash
rlightning --port 6380 --replica-of 127.0.0.1:6379
```

### Security

Enable authentication to secure your instance:

```bash
# Start with authentication
rlightning --requirepass "your-secure-password"

# Connect and authenticate
redis-cli -h localhost -p 6379
> AUTH your-secure-password
OK
```

## Configuration

### Configuration File

Create a `config.toml` file:

```toml
[server]
host = "0.0.0.0"
port = 6379
workers = 4

[storage]
max_memory_mb = 1024
eviction_policy = "lru"
enable_ttl_monitor = true

[persistence]
mode = "hybrid"  # "none", "rdb", "aof", "hybrid"
rdb_path = "dump.rdb"
aof_path = "appendonly.aof"

[security]
requirepass = "your-password"

[replication]
role = "master"  # or "replica"
# For replicas:
# master_host = "127.0.0.1"
# master_port = 6379
```

Run with config file:

```bash
rlightning --config config.toml
```

### Command-Line Options

```bash
rlightning [OPTIONS]

OPTIONS:
    --host <HOST>                  Host to bind to [default: 127.0.0.1]
    --port <PORT>                  Port to listen on [default: 6379]
    --config <FILE>                Configuration file path
    --max-memory-mb <MB>           Maximum memory in MB [default: 512]
    --eviction-policy <POLICY>     Eviction policy: lru, random, noeviction
    --persistence-mode <MODE>      Persistence mode: none, rdb, aof, hybrid
    --requirepass <PASSWORD>       Require password for authentication
    --replica-of <HOST:PORT>       Make this instance a replica
    -h, --help                     Print help information
    -V, --version                  Print version information
```

## Performance

rLightning is designed for high performance:

```bash
# Run benchmarks
cargo bench

# Specific benchmark
cargo bench --bench throughput_bench
```

### Typical Performance Metrics

- **SET operations**: ~200,000 ops/sec
- **GET operations**: ~250,000 ops/sec
- **Memory efficiency**: Optimized data structures with minimal overhead
- **Latency**: Sub-millisecond response times for most operations

## Docker Deployment

### Basic Deployment

```bash
docker run -d \
  --name rlightning \
  -p 6379:6379 \
  nxrvl/rlightning:latest
```

### With Persistence

```bash
docker run -d \
  --name rlightning \
  -p 6379:6379 \
  -v /path/to/data:/data \
  nxrvl/rlightning:latest \
  --persistence-mode hybrid \
  --rdb-path /data/dump.rdb \
  --aof-path /data/appendonly.aof
```

### Docker Compose

```yaml
version: '3.8'

services:
  rlightning:
    image: nxrvl/rlightning:latest
    ports:
      - "6379:6379"
    volumes:
      - ./data:/data
    command: >
      --persistence-mode hybrid
      --max-memory-mb 1024
      --requirepass your-password
    restart: unless-stopped
```

## Architecture

rLightning is built with a modular architecture:

- **Networking Layer**: RESP protocol implementation with async I/O
- **Command Handler**: Multi-threaded command processing
- **Storage Engine**: Sharded storage with parking_lot RwLock per shard
- **Persistence Layer**: Background RDB snapshots and AOF logging
- **Replication**: Async replication with automatic reconnection
- **Security**: Authentication and authorization layer

See the [Architecture Documentation](https://rlightning.mpanin.me/docs/architecture) for details.

## Development

### Building from Source

```bash
# Development build
cargo build

# Release build with optimizations
cargo build --release
```

### Running Tests

```bash
# All tests
cargo test

# Integration tests
cargo test --test integration_test

# Redis compatibility tests
cargo test --test redis_compatibility_test

# With output
cargo test -- --nocapture
```

### Running Benchmarks

```bash
# All benchmarks
cargo bench

# Specific benchmark suite
cargo bench --bench storage_bench
cargo bench --bench protocol_bench
cargo bench --bench throughput_bench
```

## Use Cases

rLightning excels in:

- **Session Management**: Fast session storage for web applications
- **Caching Layer**: Cache frequently accessed data
- **Rate Limiting**: Track request counts with TTL
- **Real-time Analytics**: Counters and sorted sets for leaderboards
- **Message Queues**: List-based job queues
- **Distributed Locking**: Coordinate distributed systems

## Roadmap

### Current Status (v2.0 - Full Redis 7.x Compatibility)
- All core Redis commands (400+)
- All data types including streams, bitmaps, HyperLogLog, geospatial
- RESP2 and RESP3 protocol support
- Transactions (MULTI/EXEC/WATCH)
- Lua scripting (EVAL/EVALSHA/Functions)
- ACL security system
- Cluster mode with hash slot sharding
- Sentinel high availability
- Pub/Sub with sharded channels
- Persistence (RDB/AOF/hybrid)
- Full/partial replication (PSYNC)
- Comprehensive benchmark and test suites

## Documentation

Full documentation is available at [rlightning.mpanin.me](https://rlightning.mpanin.me)

- [Getting Started Guide](https://rlightning.mpanin.me/docs/getting-started)
- [Quick Start](https://rlightning.mpanin.me/docs/quick-start)
- [Command Reference](https://rlightning.mpanin.me/docs/commands)
- [Configuration Guide](https://rlightning.mpanin.me/docs/configuration)
- [Docker Deployment](https://rlightning.mpanin.me/docs/docker)
- [Architecture Overview](https://rlightning.mpanin.me/docs/architecture)
- [Benchmarks](https://rlightning.mpanin.me/docs/benchmarks)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by Redis and its incredible ecosystem
- Built with the amazing Rust ecosystem
- Special thanks to all contributors

## Support

- 📖 [Documentation](https://rlightning.mpanin.me/docs/getting-started)
- 🐛 [Issue Tracker](https://github.com/nxrvl/rLightning/issues)
- 💬 [Discussions](https://github.com/nxrvl/rLightning/discussions)
---

Made with ⚡ by [nxrvl](https://github.com/nxrvl)
