---
title: "Cluster"
description: "Cluster command reference"
order: 15
category: "commands"
---

# Cluster Commands

rLightning supports Redis Cluster mode, distributing data across multiple nodes using 16384 hash slots. The cluster provides automatic sharding, replication, and failover.

## CLUSTER INFO

Synopsis: `CLUSTER INFO`

Return information and statistics about the cluster.

```bash
> CLUSTER INFO
cluster_enabled:1
cluster_state:ok
cluster_slots_assigned:16384
cluster_slots_ok:16384
cluster_known_nodes:6
cluster_size:3
...
```

## CLUSTER NODES

Synopsis: `CLUSTER NODES`

Return the cluster configuration as seen by the current node, in the cluster nodes format.

```bash
> CLUSTER NODES
a1b2c3... 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-5460
d4e5f6... 127.0.0.1:7001@17001 master - 0 1678886400 2 connected 5461-10922
g7h8i9... 127.0.0.1:7002@17002 master - 0 1678886400 3 connected 10923-16383
```

## CLUSTER SLOTS

Synopsis: `CLUSTER SLOTS`

Return the mapping of hash slot ranges to nodes. Deprecated in Redis 7.0; use CLUSTER SHARDS instead.

```bash
> CLUSTER SLOTS
1) 1) (integer) 0
   2) (integer) 5460
   3) 1) "127.0.0.1"
      2) (integer) 7000
      3) "a1b2c3..."
```

## CLUSTER SHARDS

Synopsis: `CLUSTER SHARDS`

Return the mapping of hash slot ranges to nodes in a structured format. Available since Redis 7.0.

```bash
> CLUSTER SHARDS
1) 1) "slots"
   2) 1) (integer) 0
      2) (integer) 5460
   3) "nodes"
   4) 1) 1) "id"
         2) "a1b2c3..."
         3) "port"
         4) (integer) 7000
         ...
```

## CLUSTER MYID

Synopsis: `CLUSTER MYID`

Return the node ID of the current node.

```bash
> CLUSTER MYID
"a1b2c3d4e5f6g7h8i9j0..."
```

## CLUSTER KEYSLOT

Synopsis: `CLUSTER KEYSLOT key`

Return the hash slot for the given key. Useful for determining which node owns a key.

```bash
> CLUSTER KEYSLOT mykey
(integer) 14687
> CLUSTER KEYSLOT {user}.name
(integer) 5474
```

## CLUSTER MEET

Synopsis: `CLUSTER MEET ip port`

Connect the current node to another node, adding it to the cluster.

```bash
> CLUSTER MEET 127.0.0.1 7001
OK
```

## CLUSTER ADDSLOTS

Synopsis: `CLUSTER ADDSLOTS slot [slot ...]`

Assign one or more hash slots to the current node.

```bash
> CLUSTER ADDSLOTS 0 1 2 3 4 5
OK
```

## CLUSTER DELSLOTS

Synopsis: `CLUSTER DELSLOTS slot [slot ...]`

Remove one or more hash slot assignments from the current node.

```bash
> CLUSTER DELSLOTS 0 1 2
OK
```

## CLUSTER SETSLOT

Synopsis: `CLUSTER SETSLOT slot IMPORTING node-id | MIGRATING node-id | STABLE | NODE node-id`

Set the state of a hash slot for slot migration.

- `IMPORTING` -- Prepare this node to import the slot from another node.
- `MIGRATING` -- Prepare this node to migrate the slot to another node.
- `STABLE` -- Clear the importing/migrating state.
- `NODE` -- Assign the slot to the specified node.

```bash
> CLUSTER SETSLOT 1234 IMPORTING a1b2c3...
OK
> CLUSTER SETSLOT 1234 NODE d4e5f6...
OK
```

## CLUSTER REPLICATE

Synopsis: `CLUSTER REPLICATE node-id`

Configure the current node as a replica of the specified master node.

```bash
> CLUSTER REPLICATE a1b2c3d4e5f6...
OK
```

## CLUSTER FAILOVER

Synopsis: `CLUSTER FAILOVER [FORCE | TAKEOVER]`

Trigger a manual failover of the master. Must be called from a replica.

- No argument -- Graceful failover with master coordination.
- `FORCE` -- Force failover without master agreement.
- `TAKEOVER` -- Force failover without cluster consensus.

```bash
> CLUSTER FAILOVER
OK
```

## CLUSTER RESET

Synopsis: `CLUSTER RESET [HARD | SOFT]`

Reset the cluster configuration of the current node. HARD reset also generates a new node ID and flushes all data.

```bash
> CLUSTER RESET SOFT
OK
```

## CLUSTER COUNTKEYSINSLOT

Synopsis: `CLUSTER COUNTKEYSINSLOT slot`

Return the number of keys in the specified hash slot.

```bash
> CLUSTER COUNTKEYSINSLOT 7638
(integer) 42
```

## CLUSTER GETKEYSINSLOT

Synopsis: `CLUSTER GETKEYSINSLOT slot count`

Return up to `count` key names in the specified hash slot.

```bash
> CLUSTER GETKEYSINSLOT 7638 3
1) "user:1"
2) "user:2"
3) "user:3"
```
