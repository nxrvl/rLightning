# Architecture

rLightning is designed with performance, safety, and maintainability in mind. This document provides an overview of the system architecture.

## High-Level Architecture

```mermaid
graph TB
    subgraph "Client Layer"
        C1[Redis Client 1]
        C2[Redis Client 2]
        C3[Redis Client N]
    end

    subgraph "Network Layer"
        TCP[TCP Server]
        RESP[RESP2/RESP3 Parser]
    end

    subgraph "Security Layer"
        ACL[ACL Engine]
        AUTH[Authentication]
    end

    subgraph "Command Layer"
        CMD[Command Handler]
        DISP[Command Dispatcher]
        TXN[Transaction Manager]
        LUA[Lua Scripting Engine]
    end

    subgraph "Core Engine"
        STORE[Storage Engine]
        MEM[Memory Manager]
        TTL[TTL Monitor]
        STREAM[Stream Engine]
        PUBSUB[Pub/Sub Manager]
    end

    subgraph "Persistence Layer"
        RDB[RDB Writer]
        AOF[AOF Logger]
    end

    subgraph "Distributed Layer"
        REP[Replication Manager]
        CLUSTER[Cluster Manager]
        SENTINEL[Sentinel Monitor]
    end

    C1 -->|RESP| TCP
    C2 -->|RESP| TCP
    C3 -->|RESP| TCP
    TCP --> RESP
    RESP --> ACL
    ACL --> CMD
    CMD --> DISP
    CMD --> TXN
    CMD --> LUA
    DISP --> STORE
    DISP --> STREAM
    DISP --> PUBSUB
    STORE --> MEM
    STORE --> TTL
    DISP --> RDB
    DISP --> AOF
    DISP --> REP
    REP --> CLUSTER
    CLUSTER --> SENTINEL
```

## Core Components

### 1. Network Layer

**Location**: `src/networking/`

Handles all network communication using both RESP2 and RESP3 protocols.

#### TCP Server
- Built on Tokio async runtime
- Handles concurrent client connections
- Per-connection protocol version tracking (RESP2/RESP3)

#### RESP Parser
- Supports all RESP2 and RESP3 data types
- RESP3: Map, Set, Null, Boolean, Double, Big Number, Verbatim String, Push
- Protocol negotiation via HELLO command

### 2. Security Layer

**Location**: `src/security/`

#### ACL Engine
- Per-user command permissions, key patterns, and channel restrictions
- Command category-based rules (+@read, -@write)
- Integrated into command dispatch pipeline

#### Authentication
- Password-based authentication (AUTH)
- Named user authentication (AUTH username password)
- Client session tracking

### 3. Command Processing Layer

**Location**: `src/command/`

#### Command Dispatcher
- Routes commands to handlers
- Validates command arguments
- Enforces ACL permissions

#### Transaction Manager (`src/command/transaction.rs`)
- MULTI/EXEC command queuing
- WATCH-based optimistic locking with key versioning
- Atomic execution with per-command error isolation

#### Lua Scripting Engine (`src/scripting/`)
- Lua 5.1 via mlua
- redis.call() and redis.pcall() bindings
- Script caching (EVALSHA)
- Redis 7.0 Functions (FCALL)
- Atomic execution (no other commands during script)

### 4. Storage Engine

**Location**: `src/storage/`

Uses `DashMap` for lock-free concurrent access with fine-grained internal sharding.

#### Supported Data Types
- **String**: Binary-safe strings
- **Hash**: Field-value pairs
- **List**: Double-ended queue
- **Set**: Hash set of unique values
- **Sorted Set**: Skip list with scores
- **Stream**: Radix tree with consumer groups
- **HyperLogLog**: Sparse and dense representations
- **Bitmap**: Bit-level operations on strings

#### Memory Management
- Configurable limits with LRU, Random, and No Eviction policies
- Real-time tracking via MEMORY USAGE/STATS
- Lazy expiration on access + periodic background cleanup

### 5. Persistence Layer

**Location**: `src/persistence/`

#### RDB (Redis Database)
- Point-in-time snapshots with background save
- Atomic writes with temp files

#### AOF (Append-Only File)
- Configurable sync: always, everysec, no
- Background rewrite for compaction

#### Hybrid Mode
- RDB for fast restarts + AOF for durability

### 6. Pub/Sub System

**Location**: `src/pubsub/`

- Channel and pattern subscriptions (SUBSCRIBE, PSUBSCRIBE)
- Sharded pub/sub (SSUBSCRIBE, SPUBLISH) for Redis 7.0+ compatibility
- Real-time message broadcasting

### 7. Stream Engine

**Location**: `src/storage/stream.rs`

- Append-only log with auto-generated entry IDs
- Consumer groups with pending entry lists (PEL)
- Blocking reads (XREAD BLOCK, XREADGROUP BLOCK)
- Trimming (MAXLEN, MINID)

## Cluster Architecture

```mermaid
graph TB
    subgraph "Cluster - 16384 Hash Slots"
        subgraph "Shard 1 - Slots 0 to 5460"
            M1[Master 1]
            R1[Replica 1a]
            R2[Replica 1b]
            M1 --> R1
            M1 --> R2
        end

        subgraph "Shard 2 - Slots 5461 to 10922"
            M2[Master 2]
            R3[Replica 2a]
            R4[Replica 2b]
            M2 --> R3
            M2 --> R4
        end

        subgraph "Shard 3 - Slots 10923 to 16383"
            M3[Master 3]
            R5[Replica 3a]
            R6[Replica 3b]
            M3 --> R5
            M3 --> R6
        end
    end

    CLIENT[Client] -->|CRC16 mod 16384| M1
    CLIENT -->|MOVED redirect| M2
    CLIENT -->|ASK redirect| M3

    M1 <-->|Gossip Bus| M2
    M2 <-->|Gossip Bus| M3
    M3 <-->|Gossip Bus| M1
```

**Location**: `src/cluster/`

### Key Components

- **Hash Slot Calculation**: CRC16 mod 16384 with hash tag support (`{tag}`)
- **Gossip Protocol**: Inter-node communication on port+10000
- **Slot Assignment**: Epoch-based versioning for consistency
- **Redirections**: MOVED (permanent) and ASK (temporary) for cross-slot operations
- **Slot Migration**: IMPORTING/MIGRATING states with MIGRATE command
- **Automatic Failover**: Detection and replica promotion

### Cluster Data Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant N1 as Node 1
    participant N2 as Node 2

    C->>N1: SET user:1001 alice
    Note over N1: CRC16 user:1001 mod 16384 = slot 5532
    Note over N1: Slot 5532 owned by Node 2
    N1->>C: MOVED 5532 192.168.1.2:6379
    C->>N2: SET user:1001 alice
    N2->>C: OK
```

## Sentinel Architecture

```mermaid
graph TB
    subgraph "Sentinel Quorum"
        S1[Sentinel 1]
        S2[Sentinel 2]
        S3[Sentinel 3]
        S1 <-->|Gossip| S2
        S2 <-->|Gossip| S3
        S3 <-->|Gossip| S1
    end

    subgraph "Monitored Deployment"
        MASTER[Master]
        REPL1[Replica 1]
        REPL2[Replica 2]
        MASTER --> REPL1
        MASTER --> REPL2
    end

    S1 -->|Monitor| MASTER
    S2 -->|Monitor| MASTER
    S3 -->|Monitor| MASTER
    S1 -.->|Monitor| REPL1
    S2 -.->|Monitor| REPL2
```

**Location**: `src/sentinel/`

### Key Components

- **Master Monitoring**: Configurable quorum-based down detection
- **SDOWN/ODOWN**: Subjective and Objective Down states
- **Leader Election**: Raft-like election for failover coordination
- **Automatic Failover**: Replica selection, promotion, and reconfiguration
- **Configuration Provider**: Clients query Sentinel for current master address

### Failover Sequence

```mermaid
sequenceDiagram
    participant S1 as Sentinel 1
    participant S2 as Sentinel 2
    participant S3 as Sentinel 3
    participant M as Master
    participant R as Best Replica

    Note over M: Master becomes unreachable
    S1->>S1: SDOWN detected
    S2->>S2: SDOWN detected
    S1->>S2: Is master down?
    S2->>S1: Yes - ODOWN reached
    S1->>S2: Vote for me as leader
    S2->>S1: Granted
    S3->>S1: Granted
    Note over S1: Elected as failover leader
    S1->>R: REPLICAOF NO ONE
    R->>R: Promoted to master
    S1->>S1: Reconfigure other replicas
    S1->>S1: Publish +switch-master
```

## Replication Flow

```mermaid
sequenceDiagram
    participant R as Replica
    participant M as Master

    R->>M: REPLCONF listening-port 6380
    M->>R: OK
    R->>M: PSYNC replication-id offset
    alt Full Sync
        M->>R: FULLRESYNC runid offset
        M->>R: RDB Snapshot Transfer
        R->>R: Load RDB
    else Partial Sync
        M->>R: CONTINUE
        M->>R: Backlog Commands
    end
    loop Continuous Replication
        M->>R: Write Commands
        R->>R: Apply Commands
        R->>M: REPLCONF ACK offset
    end
```

## Concurrency Model

- **Async Runtime**: Tokio with work-stealing scheduler
- **Lock-Free Storage**: DashMap with fine-grained internal sharding
- **Thread Safety**: `Arc<T>`, interior mutability, atomic operations
- **Blocking Commands**: Per-key wait queues with client wakeup mechanism

## Technology Stack

- **Language**: Rust 2024 Edition
- **Async Runtime**: Tokio
- **Concurrency**: DashMap, parking_lot
- **Scripting**: mlua (Lua 5.1)
- **Serialization**: bincode, serde
- **Memory**: jemalloc
- **CLI**: clap
- **Configuration**: figment

## Code Organization

```
src/
  main.rs              # Entry point
  networking/           # RESP2/RESP3 protocol, TCP server
  command/              # Command handlers and dispatch
    types/              # Per-type command implementations
    transaction.rs      # MULTI/EXEC
  storage/              # Storage engine, data types
    engine.rs           # Core storage
    stream.rs           # Stream data structure
  persistence/          # RDB/AOF
  replication/          # Master-replica sync
  security/             # Authentication, ACL
    acl.rs              # Access Control Lists
  pubsub/               # Pub/Sub messaging
  scripting/            # Lua scripting engine
  cluster/              # Cluster mode
    slot.rs             # Hash slot management
    gossip.rs           # Cluster bus protocol
    migration.rs        # Slot migration
  sentinel/             # Sentinel HA
    monitor.rs          # Master monitoring
    failover.rs         # Automatic failover
  module/               # Module system stubs
```

## Learn More

- [Storage Engine Details](storage-engine.md)
- [Networking Layer](networking.md)
- [Command Processing](command-processing.md)
- [Performance Benchmarks](benchmarks.md)
