---
title: Architecture
description: Internal architecture and design of rLightning
order: 5
category: reference
---

# Architecture

rLightning is built as a modular, high-performance in-memory data store in Rust. This page describes the internal architecture, core modules, and design decisions that enable Redis 7.x compatibility with strong concurrency guarantees.

## Module Overview

### Networking (`src/networking/`)

The networking layer handles all client connections and protocol handling.

- **RESP2 and RESP3** protocol support with `HELLO` command negotiation and per-connection protocol version tracking
- **Async I/O** powered by Tokio with per-connection state (database index, protocol version, client name, subscriptions)
- **Pipeline support** with buffered I/O and per-command error isolation -- errors in a pipeline are captured individually rather than aborting the entire batch
- **Connection tracking** via `Arc<DashMap<u64, ClientInfo>>` for `CLIENT LIST`, `CLIENT INFO`, `CLIENT ID`, and `CLIENT SETNAME`
- **RESP3 type mapping** -- `Map` for `HGETALL`/`CONFIG`/`XINFO`/scans, `Set` for `SMEMBERS`, `Double` for float commands, native `Null`/`Boolean`/`Push` types

### Storage Engine (`src/storage/`)

The storage engine is the core of rLightning, designed for lock-free concurrent access.

- **DashMap-based** lock-free concurrent storage with atomic read-modify-write primitives (`atomic_incr`, `atomic_append`, `atomic_modify`, `atomic_getdel`, `atomic_get_set_expiry`)
- **Multi-database** support with 16 databases and `SELECT` routing via task-local `CURRENT_DB_INDEX`
- **All data types** -- strings, hashes, lists, sets, sorted sets, streams, HyperLogLog, bitmaps, geospatial indexes
- **TTL handling** with lazy expiration plus periodic cleanup using a priority queue
- **Eviction policies** -- LRU, LFU, random, volatile variants
- **Cross-key atomicity** via `lock_keys()` with sorted-order deadlock prevention for multi-key operations (`MSET`, `SMOVE`, `GEOSEARCHSTORE`)
- **Runtime configuration** -- `runtime_config` DashMap for `CONFIG SET`/`GET` with glob pattern matching
- **Periodic maintenance** -- cleanup of stale key locks and key versions every 60 seconds

### Command Handler (`src/command/`)

The command layer dispatches and executes all Redis commands.

- **400+ commands** implemented across all Redis data type categories
- **Per-type handlers** in `src/command/types/` for clean separation of concerns
- **Transaction support** -- `MULTI`/`EXEC`/`DISCARD`/`WATCH`/`UNWATCH` in `src/command/transaction.rs`
- **Unified dispatch** with fast path and slow path routing

### Persistence (`src/persistence/`)

Persistence ensures data durability across restarts.

- **RDB snapshots** with background save (`BGSAVE`), supporting all data types across all 16 databases
- **AOF logging** with configurable sync policies: `always`, `everysec`, `no`
- **Atomic batch staging** to prevent partial writes in AOF
- **AOF rewrite** generates correct reconstruction commands per data type, including stream consumer groups and pending entries
- **Full AOF replay** via `CommandHandler` -- routes all commands through the same dispatch path
- **Hybrid mode** combines RDB snapshots with AOF for both fast recovery and minimal data loss

### Replication (`src/replication/`)

Replication provides high availability through leader-follower topology.

- **PSYNC** protocol with full and partial synchronization
- **Replication backlog** for efficient partial resync after brief disconnections
- **Command propagation** with per-database routing (`SELECT`-aware, not hardcoded to DB 0)
- **Transaction atomicity** -- `MULTI`/`EXEC` blocks from the master are preserved as transactions on replicas
- **Replica read-only mode** prevents accidental writes to replicas
- **FAILOVER** command support for controlled leader transitions

### Pub/Sub (`src/pubsub/`)

The publish/subscribe system supports real-time messaging.

- **Channel subscriptions** -- `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH`
- **Pattern subscriptions** -- `PSUBSCRIBE`, `PUNSUBSCRIBE` with glob matching
- **Sharded pub/sub** -- `SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH` for Redis 7.0+ cluster-aware messaging
- **Real-time broadcasting** to all matching subscribers

### Lua Scripting (`src/scripting/`)

Server-side scripting provides atomic command execution.

- **Lua 5.1** via `mlua` with `redis.call()` and `redis.pcall()` bindings
- **EVAL/EVALSHA** for script execution with SHA1 caching
- **SCRIPT** subcommands -- `LOAD`, `EXISTS`, `FLUSH`
- **Redis 7.0 Functions** -- `FUNCTION LOAD`, `FCALL` for named, reusable server-side functions

### Security (`src/security/`)

The security module implements Redis-compatible access control.

- **ACL system** with per-user command, key, and channel permissions
- **Password authentication** -- `AUTH password` and named-user `AUTH username password`
- **ACL LOG** for tracking denied commands and security audit
- **ACL rules** supporting command categories, individual commands, key patterns, and channel patterns

### Cluster (`src/cluster/`)

Cluster mode provides horizontal scaling and automatic failover.

- **Hash slot calculation** -- CRC16 mod 16384 with hash tag support
- **Gossip protocol** for node discovery and failure detection
- **MOVED/ASK redirections** for transparent client routing
- **Slot migration** for online resharding
- **Automatic failover** when master nodes become unreachable

### Sentinel (`src/sentinel/`)

Sentinel provides high availability monitoring and automatic failover for non-cluster deployments.

- **Master monitoring** with configurable check intervals
- **Quorum-based failure detection** -- SDOWN (subjective) and ODOWN (objective) states
- **Automatic failover** with leader election among Sentinel instances
- **Configuration provider** -- clients query Sentinel for current master address

## Data Flow

The request lifecycle follows a clean, linear path through the system:

```
Client Connection
       |
       v
  TCP Accept (Tokio async)
       |
       v
  RESP Protocol Parse (RESP2 or RESP3)
       |
       v
  Command Validation & ACL Check
       |
       v
  Command Dispatch (fast path / slow path)
       |
       v
  Storage Engine (DashMap atomic operations)
       |
       v
  RESP Response Serialization
       |
       v
  Client Response
```

In parallel with the main request path:

- **Replication**: Write commands are propagated to replicas via the replication backlog
- **Persistence**: Write commands are logged to AOF and/or trigger RDB snapshot conditions
- **Pub/Sub**: `PUBLISH` commands fan out to matching subscribers

## Key Design Decisions

### Lock-Free Concurrency

rLightning uses `DashMap` (a concurrent hash map based on sharded locking) instead of a single global lock. This enables high throughput under concurrent workloads. Individual entries can be read and modified without blocking other operations on different keys.

### Sorted-Order Key Locking

For multi-key operations like `MSET`, `SMOVE`, and `GEOSEARCHSTORE`, rLightning acquires locks in sorted key order to prevent deadlocks. This ensures that two concurrent operations on overlapping key sets will always acquire locks in the same order.

### Task-Local Database Index

Each connection's selected database index is stored in a Tokio task-local variable (`CURRENT_DB_INDEX`). This avoids passing the database index through every function call while maintaining per-connection isolation.

### Atomic Read-Modify-Write

Rather than exposing raw lock/unlock APIs, the storage engine provides atomic primitives (`atomic_incr`, `atomic_modify`, etc.) that accept closures. This guarantees that modifications are applied atomically without the caller needing to manage locks directly.
