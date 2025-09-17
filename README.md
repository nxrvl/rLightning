# rLightning

A high-performance, Redis-compatible in-memory key-value store built in Rust, focused on session management and caching use cases.

## Current Implementation Status

### Core Components

- **RESP Protocol**: Fully implemented Redis Serialization Protocol (RESP) parser and serializer
- **Storage Engine**: In-memory storage with configurable eviction policies (LRU, Random)
- **TTL Support**: Automatic expiration of keys with TTL monitoring
- **Command Processing**: Command handler and dispatch system

### Implemented Redis Commands

#### Core/Key Commands
- `PING` - Test if server is responding
- `DEL` - Delete one or more keys
- `EXISTS` - Determine if a key exists
- `EXPIRE` - Set a key's time to live in seconds
- `TTL` - Get the time to live for a key in seconds

#### String Commands
- `SET` - Set key to hold string value (with expiry options)
- `GET` - Get the value of a key
- `MGET` - Get the values of multiple keys
- `MSET` - Set multiple keys to multiple values
- `INCR` - Increment the integer value of a key
- `DECR` - Decrement the integer value of a key
- `APPEND` - Append a value to a key

#### Hash Commands
- `HSET` - Set field in a hash
- `HGET` - Get field from a hash
- `HGETALL` - Get all fields and values in a hash
- `HDEL` - Delete one or more hash fields
- `HEXISTS` - Determine if a hash field exists

#### List Commands
- `LPUSH` - Prepend values to a list
- `RPUSH` - Append values to a list
- `LPOP` - Remove and get the first element in a list
- `RPOP` - Remove and get the last element in a list
- `LRANGE` - Get a range of elements from a list

#### Set Commands
- `SADD` - Add members to a set
- `SREM` - Remove members from a set
- `SMEMBERS` - Get all members in a set
- `SISMEMBER` - Determine if a value is a member of a set

#### Sorted Set Commands
- `ZADD` - Add members to a sorted set
- `ZRANGE` - Return a range of members from a sorted set
- `ZREM` - Remove members from a sorted set
- `ZSCORE` - Get the score of a member in a sorted set

#### Server Commands
- `INFO` - Get server information
- `AUTH` - Authenticate to the server
- `CONFIG` - Configuration management
- `KEYS` - Find all keys matching a pattern
- `RENAME` - Rename a key
- `FLUSHALL` - Remove all keys from all databases
- `FLUSHDB` - Remove all keys from the current database
- `MONITOR` - Listen for all commands processed by the server

### Features
- Configurable memory limits
- Multiple eviction policies (LRU, Random)
- TTL-based expiration

## What's Next: Completing a Redis-like Database

To make rLightning more feature-complete as a Redis-like database, these components should be considered:

### Core Functionality
1. **Persistence**:
   - RDB snapshot persistence
   - AOF (Append-Only File) logging
   - Hybrid persistence model

2. **Clustering and Replication**:
   - Master-Replica replication
   - Automatic failover
   - Cluster mode with sharding

3. **Transactions**:
   - MULTI/EXEC/DISCARD/WATCH commands
   - Optimistic locking

### Additional Commands
1. **More String Operations**:
   - `GETSET`, `SETEX`, `SETNX`
   - `GETRANGE`, `SETRANGE`
   - `STRLEN`, `BITOP` operations

2. **Pub/Sub Functionality**:
   - `PUBLISH`, `SUBSCRIBE`, `UNSUBSCRIBE`
   - Channel pattern matching

3. **More Data Structure Operations**:
   - Lists: `LTRIM`, `BLPOP`, `BRPOP`, `LSET`
   - Sets: `SINTER`, `SUNION`, `SDIFF` and their respective STORE variants
   - Sorted Sets: `ZREVRANGE`, `ZRANK`, `ZCOUNT`, `ZINCRBY`
   - Hashes: `HMSET`, `HMGET`, `HINCRBY`, `HKEYS`, `HVALS`

4. **Lua Scripting**:
   - `EVAL`, `EVALSHA` commands
   - Script management commands

5. **HyperLogLog Commands**:
   - `PFADD`, `PFCOUNT`, `PFMERGE`

### Performance Enhancements
1. **Improved Memory Management**:
   - LFU (Least Frequently Used) eviction policy implementation
   - Memory fragmentation handling
   - More efficient data structures

2. **Enhanced Concurrency**:
   - Optimistic locking for multi-operation sequences
   - Fine-grained locking to reduce contention

3. **Connection Handling**:
   - Connection pooling
   - Pipelining support

### Operations & Monitoring
1. **Enhanced Statistics**:
   - Detailed metrics for operations
   - Memory usage statistics
   - Latency monitoring

2. **Client Management**:
   - `CLIENT LIST`, `CLIENT KILL` and other client management commands
   - Client-side caching directives

3. **Administration Tools**:
   - Web-based admin interface
   - CLI management tools

## Getting Started

```bash
# Clone the repository
git clone https://github.com/yourusername/rLightning.git
cd rLightning

# Build the project
cargo build --release

# Run the server
./target/release/rlightning
```

## Configuration

See `config/default.toml` for configuration options.

## Website and Documentation

rLightning includes a website and comprehensive documentation.

### Prerequisites

- Node.js (v14 or later) for the website
- Python (v3.6 or later) for documentation
- pip for installing Python packages

### Installing Dependencies

```bash
# Install MkDocs and required plugins
pip install mkdocs mkdocs-material mkdocs-git-revision-date-localized-plugin mkdocs-minify-plugin

# Install website dependencies
cd website
npm install
cd ..
```

### Running the Website and Documentation

```bash
# From the project root
cd website
npm start
```

This will:
- Start the main website at http://localhost:3000
- Start the documentation at http://localhost:8000
- Set up http://localhost:3000/docs to access the documentation

For development with auto-reload:

```bash
cd website
npm run dev
```

### Building Documentation for Production

To generate static documentation:

```bash
mkdocs build
```

The built documentation will be in the `site` directory.

## Running Tests

rLightning has a comprehensive test suite to ensure code quality and correctness.

```bash
# Run all unit tests
cargo test

# Run a specific test
cargo test <test_name>

# Run integration tests
cargo test --test integration_test

# Run Redis compatibility tests
cargo test --test redis_compat_test

# Run tests with verbose output
cargo test -- --nocapture
```

## Running Benchmarks

Benchmarks are available to measure performance of key operations.

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

Benchmark results will be saved in the `target/criterion` directory with HTML reports you can view in your browser.

## License

MIT License
