---
title: Benchmarks
description: Performance benchmarks and comparisons
order: 7
category: reference
---

# Benchmarks

rLightning includes a comprehensive benchmarking suite built on [Criterion](https://github.com/bheisler/criterion.rs) for statistically rigorous performance measurement.

## Available Benchmarks

| Benchmark                  | Description                                           |
|----------------------------|-------------------------------------------------------|
| `storage_bench`            | Core storage engine operations (set, get, delete)     |
| `protocol_bench`           | RESP protocol parsing and serialization               |
| `throughput_bench`         | End-to-end request throughput                         |
| `redis_comparison_bench`   | Direct comparison against Redis server                |
| `replication_bench`        | Replication command propagation and sync              |
| `auth_bench`               | Authentication and ACL checking overhead              |
| `command_category_bench`   | Per-category command performance                      |
| `advanced_features_bench`  | Streams, scripting, and advanced data structures      |
| `cluster_bench`            | Cluster mode hash slot routing and redirection        |

## Running Benchmarks

### Run All Benchmarks

```bash
cargo bench
```

### Run a Specific Benchmark Suite

```bash
cargo bench --bench storage_bench
cargo bench --bench protocol_bench
cargo bench --bench throughput_bench
cargo bench --bench redis_comparison_bench
```

### Run a Specific Benchmark Function

Filter by function name within a benchmark suite:

```bash
cargo bench --bench storage_bench -- set_get
cargo bench --bench storage_bench -- concurrent_writes
cargo bench --bench protocol_bench -- parse_bulk_string
```

### View HTML Reports

Criterion generates detailed HTML reports with statistical analysis and comparison charts:

```bash
# After running benchmarks, open the report
open target/criterion/report/index.html
```

## Key Performance Optimizations

rLightning achieves high throughput through several architectural choices:

### jemalloc Allocator

rLightning uses jemalloc as its global memory allocator. jemalloc provides better performance than the system allocator for the allocation patterns typical in a data store -- many small, short-lived allocations mixed with long-lived cached data.

### Lock-Free Concurrency with DashMap

The storage engine uses `DashMap`, a concurrent hash map with sharded internal locking. This allows multiple threads to read and write different keys simultaneously without contention. Compared to a single `RwLock<HashMap>`, DashMap scales linearly with the number of CPU cores.

### Tokio Async Runtime

All network I/O is handled through Tokio's async runtime, enabling thousands of concurrent client connections with minimal thread overhead. The work-stealing scheduler distributes load evenly across worker threads.

### memchr Buffer Scanning

RESP protocol parsing uses the `memchr` crate for SIMD-accelerated byte scanning when searching for delimiters (`\r\n`) in the input buffer. This significantly speeds up protocol parsing for large payloads.

### Zero-Copy Parsing

Where possible, rLightning avoids copying data during protocol parsing. Byte slices from the read buffer are used directly, reducing memory allocations and improving cache locality.

### Byte-Level List Storage with Gap Buffer

List operations use a dual-format byte-level storage engine:
- **V0 format** for small lists (≤128 elements): flat sequential layout with minimal overhead
- **V1 gap buffer** for large lists (>128 elements): 24-byte header with left-gap enabling O(1) amortized LPUSH and O(1) LPOP without memory moves

## Performance vs Redis 7

Measured with `redis-benchmark -n 100000 -c 50` against Redis 7, both running in Docker containers on the same host:

| Operation | rLightning | Redis 7 | Ratio |
|-----------|-----------|---------|-------|
| SET       | ~73K rps  | ~67K rps | 1.09x |
| GET       | ~75K rps  | ~68K rps | 1.10x |
| RPUSH     | ~64K rps  | ~67K rps | 0.96x |
| LPUSH     | ~60K rps  | ~53K rps | 1.13x |
| LPOP      | ~64K rps  | ~63K rps | 1.01x |
| RPOP      | ~62K rps  | ~65K rps | 0.95x |
| SADD      | ~72K rps  | ~68K rps | 1.06x |
| HSET      | ~70K rps  | ~66K rps | 1.06x |

> Results vary by hardware and configuration. Run your own benchmarks for production sizing.

## Benchmarking with redis-benchmark

You can also benchmark rLightning using the standard `redis-benchmark` tool that ships with Redis. This provides a direct comparison under identical test conditions.

### Basic Throughput Test

```bash
# Start rLightning
cargo run --release

# Run redis-benchmark against it (default: 50 connections, 100000 requests)
redis-benchmark -h 127.0.0.1 -p 6379
```

### Specific Command Tests

```bash
# Benchmark SET/GET operations
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 1000000

# Benchmark with pipelining (16 commands per pipeline)
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 1000000 -P 16

# Benchmark with specific data size (256 bytes)
redis-benchmark -h 127.0.0.1 -p 6379 -t set -n 500000 -d 256

# Benchmark with multiple clients
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 1000000 -c 100
```

### Comparison Against Redis

To compare rLightning against Redis under identical conditions:

```bash
# Start Redis on port 6379
redis-server --port 6379

# Start rLightning on port 6380
cargo run --release -- --port 6380

# Benchmark Redis
redis-benchmark -h 127.0.0.1 -p 6379 -t set,get -n 1000000 -P 16 -q

# Benchmark rLightning
redis-benchmark -h 127.0.0.1 -p 6380 -t set,get -n 1000000 -P 16 -q
```

The `-q` flag outputs a quiet summary with just the operations-per-second figure, making it easy to compare results side by side.

### Recommended Test Parameters

For meaningful benchmarks, consider testing with:

- **Varying connection counts** (`-c 1`, `-c 50`, `-c 200`) to measure concurrency scaling
- **Varying pipeline depths** (`-P 1`, `-P 16`, `-P 64`) to measure batching efficiency
- **Varying data sizes** (`-d 8`, `-d 256`, `-d 4096`) to measure impact of payload size
- **Multiple command types** (`-t set,get,incr,lpush,rpush,sadd`) to measure different code paths
