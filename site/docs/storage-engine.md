# Storage Engine

The storage engine is the core of rLightning, managing all data operations with lock-free concurrent access.

## Architecture

The storage engine uses `DashMap` for lock-free concurrent access with fine-grained internal sharding. Each key-value pair includes metadata for TTL tracking, LRU timestamps, and type information.

## Data Types

| Type | Internal Structure | Use Case |
|------|-------------------|----------|
| String | `Vec<u8>` | General key-value, counters |
| Hash | `HashMap<String, String>` | Structured objects |
| List | `VecDeque<String>` | Queues, stacks |
| Set | `HashSet<String>` | Unique collections |
| Sorted Set | Skip list + HashMap | Rankings, ranges |
| Stream | Radix tree | Event logs, messaging |
| HyperLogLog | Sparse/Dense registers | Cardinality estimation |

## Memory Management

- Configurable memory limits with eviction policies
- Lazy expiration on key access
- Periodic background cleanup of expired keys
- Real-time memory usage tracking via `MEMORY USAGE`

## Concurrency Model

- Lock-free reads and writes via DashMap sharding
- Atomic operations for INCR/DECR family
- Transaction isolation through command queuing (MULTI/EXEC)
