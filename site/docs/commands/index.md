# Command Reference

rLightning implements full Redis 7.x command compatibility across all data types and feature areas.

## Command Categories

### Data Types

<div class="rl-grid rl-grid-3">
  <div class="rl-card">
    <h3><a href="strings/">String Commands</a></h3>
    <p>GET, SET, MGET, MSET, INCR, APPEND, GETEX, GETDEL, LCS, and more.</p>
  </div>
  <div class="rl-card">
    <h3><a href="hashes/">Hash Commands</a></h3>
    <p>HGET, HSET, HGETALL, HRANDFIELD, HSCAN, and all hash operations.</p>
  </div>
  <div class="rl-card">
    <h3><a href="lists/">List Commands</a></h3>
    <p>LPUSH, RPUSH, LPOP, RPOP, LRANGE, BLPOP, BRPOP, LMOVE, LMPOP, and more.</p>
  </div>
  <div class="rl-card">
    <h3><a href="sets/">Set Commands</a></h3>
    <p>SADD, SREM, SMEMBERS, SMOVE, SINTERCARD, SMISMEMBER, SSCAN.</p>
  </div>
  <div class="rl-card">
    <h3><a href="sorted-sets/">Sorted Set Commands</a></h3>
    <p>ZADD, ZRANGE, ZSCORE, ZPOPMIN, BZPOPMIN, ZRANGESTORE, ZSCAN, and more.</p>
  </div>
  <div class="rl-card">
    <h3><a href="streams/">Stream Commands</a></h3>
    <p>XADD, XREAD, XREADGROUP, XACK, XPENDING, XCLAIM, XINFO, and consumer groups.</p>
  </div>
</div>

### Specialized Data Types

<div class="rl-grid rl-grid-3">
  <div class="rl-card">
    <h3><a href="bitmap/">Bitmap Commands</a></h3>
    <p>SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD for bit-level operations.</p>
  </div>
  <div class="rl-card">
    <h3><a href="hyperloglog/">HyperLogLog Commands</a></h3>
    <p>PFADD, PFCOUNT, PFMERGE for probabilistic cardinality estimation.</p>
  </div>
  <div class="rl-card">
    <h3><a href="geo/">Geospatial Commands</a></h3>
    <p>GEOADD, GEODIST, GEOSEARCH, GEOPOS, GEOHASH for location-based queries.</p>
  </div>
</div>

### Features

<div class="rl-grid rl-grid-3">
  <div class="rl-card">
    <h3><a href="transactions/">Transaction Commands</a></h3>
    <p>MULTI, EXEC, DISCARD, WATCH, UNWATCH for atomic operations.</p>
  </div>
  <div class="rl-card">
    <h3><a href="scripting/">Scripting Commands</a></h3>
    <p>EVAL, EVALSHA, SCRIPT, FUNCTION, FCALL for Lua scripting.</p>
  </div>
  <div class="rl-card">
    <h3><a href="pubsub/">Pub/Sub Commands</a></h3>
    <p>SUBSCRIBE, PUBLISH, PSUBSCRIBE, SSUBSCRIBE, SPUBLISH.</p>
  </div>
</div>

### Infrastructure

<div class="rl-grid rl-grid-3">
  <div class="rl-card">
    <h3><a href="acl/">ACL Commands</a></h3>
    <p>ACL SETUSER, GETUSER, DELUSER, LIST, CAT, LOG for access control.</p>
  </div>
  <div class="rl-card">
    <h3><a href="cluster/">Cluster Commands</a></h3>
    <p>CLUSTER INFO, NODES, SLOTS, MEET, FAILOVER for cluster management.</p>
  </div>
  <div class="rl-card">
    <h3><a href="sentinel/">Sentinel Commands</a></h3>
    <p>SENTINEL MASTERS, FAILOVER, MONITOR for high availability.</p>
  </div>
  <div class="rl-card">
    <h3><a href="server/">Server Commands</a></h3>
    <p>PING, INFO, CONFIG, CLIENT, COMMAND, MEMORY, DEBUG, SLOWLOG.</p>
  </div>
</div>

## Command Compatibility

rLightning implements full Redis 7.x command compatibility:

| Category | Commands | Status |
|----------|----------|--------|
| Strings | GET, SET, MGET, MSET, INCR, DECR, APPEND, GETEX, GETDEL, LCS, PSETEX, SUBSTR | Complete |
| Hashes | HGET, HSET, HGETALL, HDEL, HEXISTS, HRANDFIELD, HSCAN | Complete |
| Lists | LPUSH, RPUSH, LPOP, RPOP, LRANGE, BLPOP, BRPOP, LMOVE, LMPOP, BLMOVE, BLMPOP | Complete |
| Sets | SADD, SREM, SMEMBERS, SMOVE, SINTERCARD, SMISMEMBER, SSCAN | Complete |
| Sorted Sets | ZADD, ZRANGE, ZSCORE, ZPOPMIN/MAX, BZPOPMIN/MAX, ZRANGESTORE, ZMPOP | Complete |
| Streams | XADD, XREAD, XREADGROUP, XACK, XPENDING, XCLAIM, XAUTOCLAIM, XINFO | Complete |
| Bitmap | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD | Complete |
| HyperLogLog | PFADD, PFCOUNT, PFMERGE | Complete |
| Geospatial | GEOADD, GEODIST, GEOSEARCH, GEOSEARCHSTORE, GEOPOS, GEOHASH | Complete |
| Transactions | MULTI, EXEC, DISCARD, WATCH, UNWATCH | Complete |
| Scripting | EVAL, EVALSHA, SCRIPT, FUNCTION, FCALL | Complete |
| Pub/Sub | SUBSCRIBE, PUBLISH, PSUBSCRIBE, SSUBSCRIBE, SPUBLISH | Complete |
| ACL | ACL SETUSER, GETUSER, DELUSER, LIST, USERS, WHOAMI, CAT, LOG | Complete |
| Cluster | CLUSTER INFO, NODES, SLOTS, SHARDS, MEET, FAILOVER, MIGRATE | Complete |
| Sentinel | SENTINEL MASTERS, REPLICAS, FAILOVER, MONITOR, CKQUORUM | Complete |
| Server | PING, INFO, CONFIG, CLIENT, COMMAND, MEMORY, SLOWLOG, DEBUG | Complete |
| Keys | KEYS, SCAN, DEL, EXISTS, EXPIRE, TTL, TYPE, RENAME, COPY, SORT | Complete |

## Common Patterns

### Caching

```bash
SET cache:user:1001 "user_data" EX 300
GET cache:user:1001
```

### Session Management

```bash
SET session:abc123 "user_id:1001" EX 3600
GET session:abc123
EXPIRE session:abc123 3600
DEL session:abc123
```

### Rate Limiting

```bash
INCR ratelimit:user:1001
EXPIRE ratelimit:user:1001 60
```

### Leaderboard

```bash
ZADD leaderboard 1000 player1
ZREVRANGE leaderboard 0 9 WITHSCORES
ZREVRANK leaderboard player1
```

### Message Queue

```bash
LPUSH jobs "process_order:1001"
BRPOP jobs 30
```
