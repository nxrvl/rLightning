# Cluster Commands

Cluster mode enables automatic data sharding across multiple rLightning nodes using hash slots. The key space is divided into 16384 hash slots distributed across cluster nodes.

## Core Commands

### CLUSTER INFO

Return cluster state information.

```bash
CLUSTER INFO
```

### CLUSTER NODES

Return the cluster topology as seen by the current node.

```bash
CLUSTER NODES
```

### CLUSTER SLOTS

Return hash slot to node mappings (deprecated, use SHARDS).

```bash
CLUSTER SLOTS
```

### CLUSTER SHARDS

Return shard information including slot ranges and node details (Redis 7.0+).

```bash
CLUSTER SHARDS
```

### CLUSTER MYID

Return the node ID of the current node.

```bash
CLUSTER MYID
```

## Cluster Management

### CLUSTER MEET

Connect a node to the cluster.

```bash
CLUSTER MEET ip port
```

### CLUSTER FORGET

Remove a node from the cluster.

```bash
CLUSTER FORGET node-id
```

### CLUSTER REPLICATE

Configure a node as a replica of another node.

```bash
CLUSTER REPLICATE node-id
```

### CLUSTER RESET

Reset the cluster configuration.

```bash
CLUSTER RESET [HARD|SOFT]
```

## Slot Management

### CLUSTER ADDSLOTS / DELSLOTS

Assign or remove hash slots from the current node.

```bash
CLUSTER ADDSLOTS slot [slot ...]
CLUSTER DELSLOTS slot [slot ...]
CLUSTER FLUSHSLOTS
```

### CLUSTER SETSLOT

Set the state of a hash slot for migration.

```bash
CLUSTER SETSLOT slot IMPORTING node-id
CLUSTER SETSLOT slot MIGRATING node-id
CLUSTER SETSLOT slot STABLE
CLUSTER SETSLOT slot NODE node-id
```

### CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT

Inspect keys in a slot.

```bash
CLUSTER COUNTKEYSINSLOT slot
CLUSTER GETKEYSINSLOT slot count
```

### CLUSTER KEYSLOT

Return the hash slot for a key.

```bash
CLUSTER KEYSLOT key
```

## Failover

### CLUSTER FAILOVER

Initiate a manual failover from a replica.

```bash
CLUSTER FAILOVER [FORCE|TAKEOVER]
```

## Client Commands

### ASKING

Indicate that the next command is for a slot being imported.

```bash
ASKING
```

### READONLY / READWRITE

Enable or disable read queries on replica nodes.

```bash
READONLY
READWRITE
```

### MIGRATE

Move a key to a different node.

```bash
MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password] [AUTH2 username password] [KEYS key [key ...]]
```

## Redirections

When a client sends a command to the wrong node, it receives a redirect:

- `MOVED slot ip:port` -- Permanent redirect (slot owned by another node)
- `ASK slot ip:port` -- Temporary redirect (slot being migrated)

## Hash Tags

Use hash tags `{...}` to ensure related keys map to the same slot:

```bash
SET {user:1001}.name "Alice"
SET {user:1001}.email "alice@example.com"
# Both keys are guaranteed to be in the same slot
```
