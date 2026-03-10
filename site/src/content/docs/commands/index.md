---
title: "Commands Overview"
description: "Complete command reference for rLightning"
order: 1
category: "commands"
---

# Commands Overview

rLightning implements **400+ Redis 7.x compatible commands** across all major data types and subsystems. This reference documents every supported command with syntax, descriptions, and examples.

## Command Categories

| Category | Commands | Description |
|----------|----------|-------------|
| [Strings](/commands/strings/) | 22 | GET, SET, INCR, APPEND, and more |
| [Hashes](/commands/hashes/) | 15 | HSET, HGET, HGETALL, HSCAN, and more |
| [Lists](/commands/lists/) | 19 | LPUSH, RPOP, LRANGE, blocking ops, and more |
| [Sets](/commands/sets/) | 17 | SADD, SMEMBERS, SUNION, SINTER, and more |
| [Sorted Sets](/commands/sorted-sets/) | 26 | ZADD, ZRANGE, ZUNIONSTORE, and more |
| [Streams](/commands/streams/) | 14 | XADD, XREAD, XREADGROUP, consumer groups, and more |
| [Bitmap](/commands/bitmap/) | 7 | SETBIT, GETBIT, BITCOUNT, BITFIELD, and more |
| [HyperLogLog](/commands/hyperloglog/) | 4 | PFADD, PFCOUNT, PFMERGE, PFDEBUG |
| [Geospatial](/commands/geo/) | 8 | GEOADD, GEOSEARCH, GEODIST, and more |
| [Transactions](/commands/transactions/) | 5 | MULTI, EXEC, DISCARD, WATCH, UNWATCH |
| [Scripting](/commands/scripting/) | 10+ | EVAL, EVALSHA, FUNCTION, FCALL, and more |
| [Pub/Sub](/commands/pubsub/) | 9+ | SUBSCRIBE, PUBLISH, sharded pub/sub, and more |
| [ACL](/commands/acl/) | 11 | AUTH, ACL LIST, ACL SETUSER, and more |
| [Cluster](/commands/cluster/) | 15+ | CLUSTER INFO, CLUSTER NODES, slot management, and more |
| [Sentinel](/commands/sentinel/) | 13+ | SENTINEL MASTERS, monitoring, failover, and more |
| [Server](/commands/server/) | 70+ | PING, INFO, CONFIG, CLIENT, key management, and more |

## Protocol Support

rLightning supports both **RESP2** and **RESP3** protocols. Use the `HELLO` command to negotiate protocol version per connection.

## Command Compatibility

All commands follow Redis 7.x semantics. For known behavioral differences, see the [Known Incompatibilities](https://github.com/altista-tech/rLightning/blob/main/tests/docker-compat/KNOWN-INCOMPATIBILITIES.txt) file in the repository.
