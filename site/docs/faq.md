# FAQ

## General

**Is rLightning a drop-in replacement for Redis?**

Yes, for most use cases. rLightning implements the Redis RESP2 and RESP3 protocols with full command compatibility across all data types. Any Redis client library works without modification.

**What Redis version is compatible?**

rLightning targets Redis 7.x compatibility, including RESP3, Functions, ACL, and all data type commands.

**Is rLightning production-ready?**

rLightning includes persistence (RDB/AOF), replication, clustering, Sentinel HA, and ACL security. It is suitable for production workloads.

## Performance

**How does rLightning compare to Redis?**

rLightning delivers comparable or slightly better performance than Redis 7.x across most operations. See [Benchmarks](benchmarks.md) for detailed comparisons.

**Why is it fast?**

Rust's zero-cost abstractions, lock-free data structures (DashMap), Tokio async I/O, and memory-safe design without garbage collection pauses.

## Data

**What persistence options are available?**

RDB snapshots, AOF (append-only file), and hybrid mode. See [Persistence](persistence.md).

**Can I migrate from Redis?**

You can point your Redis clients at rLightning with no code changes. For data migration, use the DUMP/RESTORE commands or RDB file transfer.

## Operations

**Does it support clustering?**

Yes. Full cluster mode with hash slot sharding, MOVED/ASK redirections, slot migration, and automatic failover. See [Cluster Commands](commands/cluster.md).

**Does it support high availability?**

Yes. Sentinel mode provides monitoring, automatic failover, and configuration provider functionality. See [Sentinel Commands](commands/sentinel.md).

**Does it support Lua scripting?**

Yes. Full Lua 5.1 support with EVAL/EVALSHA and Redis 7.0 Functions (FCALL). See [Scripting Commands](commands/scripting.md).
