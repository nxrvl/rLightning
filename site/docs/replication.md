# Replication

Master-replica replication for high availability and read scaling.

## Setup

### Master

```bash
rlightning --port 6379
```

### Replica

```bash
rlightning --port 6380 --replica-of 127.0.0.1:6379
```

Or dynamically:

```bash
REPLICAOF 127.0.0.1 6379
```

## Commands

```bash
ROLE                            # Show replication role
REPLICAOF host port             # Configure as replica
REPLICAOF NO ONE                # Promote to master
WAIT numreplicas timeout        # Wait for replica acknowledgement
INFO replication                # Replication status
```

## Sync Modes

- **Full sync (SYNC)**: Complete RDB transfer on initial connection
- **Partial sync (PSYNC)**: Offset-based incremental sync using replication backlog

## Features

- Automatic reconnection on network failure
- Replica read-only mode by default
- Replication backlog for efficient partial resync
- Manual failover with FAILOVER command
