# rLightning ⚡

[![Build Status](https://github.com/altista-tech/rLightning/workflows/CI/badge.svg)](https://github.com/altista-tech/rLightning/actions)
[![Crates.io](https://img.shields.io/crates/v/rlightning.svg)](https://crates.io/crates/rlightning)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Docker](https://img.shields.io/docker/automated/altista/rlightning.svg)](https://hub.docker.com/r/altista/rlightning)

A high-performance, Redis-compatible in-memory key-value store built in Rust, focused on session management and caching use cases.

## Why rLightning?

- **🚀 High Performance**: Built with Rust for maximum speed and safety
- **🔌 Redis Compatible**: Drop-in replacement for Redis in many scenarios
- **💾 Multiple Data Types**: Strings, hashes, lists, sets, sorted sets
- **⏰ TTL Support**: Automatic key expiration
- **🔄 Replication**: Master-replica setup for high availability
- **💿 Persistence**: RDB snapshots and AOF logging
- **🔒 Secure**: Built-in authentication support
- **📊 Resource Efficient**: Configurable memory limits and eviction policies

## Quick Start

### Using Docker (Recommended)

```bash
docker run -d -p 6379:6379 altista/rlightning:latest
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
git clone https://github.com/altista-tech/rLightning.git
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

### Supported Redis Commands

#### String Operations
- `GET`, `SET`, `MGET`, `MSET` - Basic string operations
- `INCR`, `DECR` - Atomic increment/decrement
- `APPEND` - Append to string values
- TTL support with `EXPIRE` and expiry options in `SET`

#### Hash Operations
- `HGET`, `HSET`, `HGETALL` - Field-value pairs
- `HDEL`, `HEXISTS` - Hash management
- Efficient storage for structured data

#### List Operations
- `LPUSH`, `RPUSH` - Add elements to lists
- `LPOP`, `RPOP` - Remove and return elements
- `LRANGE` - Retrieve list ranges

#### Set Operations
- `SADD`, `SREM` - Add/remove set members
- `SMEMBERS` - Get all members
- `SISMEMBER` - Check membership

#### Sorted Set Operations
- `ZADD`, `ZREM` - Scored set operations
- `ZRANGE` - Range queries
- `ZSCORE` - Get member scores

#### Pub/Sub Operations
- `SUBSCRIBE`, `UNSUBSCRIBE` - Channel subscriptions
- `PSUBSCRIBE`, `PUNSUBSCRIBE` - Pattern-based subscriptions
- `PUBLISH` - Send messages to channels
- `PUBSUB CHANNELS`, `PUBSUB NUMSUB`, `PUBSUB NUMPAT` - Introspection commands

#### Server Commands
- `PING` - Server health check
- `INFO` - Server statistics
- `AUTH` - Authentication
- `CONFIG` - Runtime configuration
- `KEYS` - Pattern matching
- `FLUSHALL`, `FLUSHDB` - Data management
- `MONITOR` - Real-time command monitoring

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
  altista/rlightning:latest
```

### With Persistence

```bash
docker run -d \
  --name rlightning \
  -p 6379:6379 \
  -v /path/to/data:/data \
  altista/rlightning:latest \
  --persistence-mode hybrid \
  --rdb-path /data/dump.rdb \
  --aof-path /data/appendonly.aof
```

### Docker Compose

```yaml
version: '3.8'

services:
  rlightning:
    image: altista/rlightning:latest
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
- **Storage Engine**: Lock-free data structures with DashMap
- **Persistence Layer**: Background RDB snapshots and AOF logging
- **Replication**: Async replication with automatic reconnection
- **Security**: Authentication and authorization layer

See the [Architecture Documentation](https://rlightning.io/architecture) for details.

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

### Current Status (v1.0)
- ✅ Core Redis commands
- ✅ Multiple data types
- ✅ TTL support
- ✅ Persistence (RDB/AOF)
- ✅ Replication
- ✅ Authentication
- ✅ Pub/Sub messaging

### Planned Features
- 🔄 Lua scripting support
- 🔄 Cluster mode
- 🔄 Transactions (MULTI/EXEC)
- 🔄 Streams
- 🔄 Modules API

## Documentation

Full documentation is available at [rlightning.io](https://rlightning.io)

- [Getting Started Guide](https://rlightning.io/getting-started)
- [Command Reference](https://rlightning.io/commands)
- [Configuration Guide](https://rlightning.io/configuration)
- [Replication Setup](https://rlightning.io/replication)
- [Persistence Options](https://rlightning.io/persistence)
- [Architecture Overview](https://rlightning.io/architecture)

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

- 📖 [Documentation](https://rlightning.io)
- 🐛 [Issue Tracker](https://github.com/altista-tech/rLightning/issues)
- 💬 [Discussions](https://github.com/altista-tech/rLightning/discussions)
- 📧 Email: support@altista.tech

---

Made with ⚡ by [Altista Tech](https://altista.tech)
