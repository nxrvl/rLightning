---
title: Compatibility
description: Redis 7.x compatibility status and testing
order: 6
category: reference
---

# Compatibility

rLightning targets full compatibility with Redis 7.x. It implements the complete Redis Serialization Protocol (RESP2 and RESP3) and supports 400+ commands across all data types.

## Protocol Support

- **RESP2** -- Full support for the classic Redis protocol
- **RESP3** -- Full support including `HELLO` negotiation, typed responses (`Map`, `Set`, `Double`, `Null`, `Boolean`, `Push`), and per-connection protocol version tracking
- **Inline commands** -- Supported for compatibility with telnet and simple clients

## Command Coverage

rLightning implements 400+ Redis commands spanning every command category:

- **Strings** -- `GET`, `SET`, `MGET`, `MSET`, `INCR`, `APPEND`, `GETRANGE`, `SETRANGE`, and more
- **Hashes** -- `HGET`, `HSET`, `HMGET`, `HMSET`, `HGETALL`, `HINCRBY`, `HSCAN`, and more
- **Lists** -- `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LINDEX`, `LINSERT`, `BLPOP`, `BRPOP`, and more
- **Sets** -- `SADD`, `SREM`, `SMEMBERS`, `SINTER`, `SUNION`, `SDIFF`, `SSCAN`, and more
- **Sorted Sets** -- `ZADD`, `ZRANGE`, `ZRANGEBYSCORE`, `ZRANK`, `ZINCRBY`, `ZSCAN`, and more
- **Streams** -- `XADD`, `XREAD`, `XRANGE`, `XLEN`, `XGROUP`, `XACK`, `XCLAIM`, and more
- **Bitmap** -- `SETBIT`, `GETBIT`, `BITCOUNT`, `BITOP`, `BITFIELD`, `BITPOS`
- **HyperLogLog** -- `PFADD`, `PFCOUNT`, `PFMERGE`
- **Geospatial** -- `GEOADD`, `GEODIST`, `GEOHASH`, `GEOPOS`, `GEOSEARCH`, `GEOSEARCHSTORE`
- **Transactions** -- `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`
- **Pub/Sub** -- `SUBSCRIBE`, `PUBLISH`, `PSUBSCRIBE`, `SSUBSCRIBE`, `SPUBLISH`
- **Scripting** -- `EVAL`, `EVALSHA`, `SCRIPT`, `FUNCTION LOAD`, `FCALL`
- **ACL** -- `ACL SETUSER`, `ACL GETUSER`, `ACL LIST`, `ACL DELUSER`, `ACL LOG`
- **Server** -- `INFO`, `CONFIG`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `CLIENT`, `COMMAND`, and more
- **Cluster** -- `CLUSTER INFO`, `CLUSTER NODES`, `CLUSTER SLOTS`, `CLUSTER MEET`, and more
- **Sentinel** -- `SENTINEL MASTERS`, `SENTINEL MONITOR`, `SENTINEL FAILOVER`, and more

## Known Incompatibilities

As of v2.0.0, there are **no known incompatibilities** between rLightning and Redis 7.x. All previously identified behavioral differences have been resolved.

### Previously Resolved Issues

The following incompatibilities were identified through the multi-language compatibility test suite and have since been fixed:

- **GEOHASH accuracy** -- Geohash string precision now matches Redis 7.x output exactly
- **Pipeline error isolation** -- Per-command errors in a pipeline are captured as individual error responses instead of aborting the entire pipeline
- **CONFIG SET/GET** -- Runtime configuration changes and glob-pattern queries now match Redis behavior

## Multi-Language Compatibility Testing

rLightning includes a comprehensive compatibility test suite that validates behavior against a real Redis 7 server using three major client libraries.

### Client Libraries Tested

| Language   | Client Library | Version |
|------------|----------------|---------|
| Go         | go-redis/v9    | Latest  |
| JavaScript | ioredis        | Latest  |
| Python     | redis-py       | Latest  |

### Test Coverage

Each language implements approximately **231 tests** across **25 categories**:

- Connection & Auth
- Strings
- Hashes
- Lists
- Sets
- Sorted Sets
- Key Management
- Transactions
- Pub/Sub
- Pipelining
- Lua Scripting
- Lua Scripting Advanced
- Streams
- Streams Advanced
- Advanced Types (Bitmap/HLL/Geo)
- Edge Cases
- Server Commands
- ACL & Security
- Blocking Commands
- Memory Management & Eviction
- Persistence (RDB/AOF)
- Replication
- Cluster Mode
- Sentinel
- Language-Specific (Go/JS/Python)

### Running Compatibility Tests

The test suite runs each client against both Redis 7 and rLightning, then compares the results.

```bash
# Run the full suite in local mode (uses local Go/Node/Python, Docker for servers)
cd tests/docker-compat && ./run-tests.sh --local

# Run the full suite with Docker clients
cd tests/docker-compat && ./run-tests.sh

# Run an individual client against a specific server
REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=test_password \
  go run ./tests/docker-compat/go-client/...

REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=test_password \
  node tests/docker-compat/js-client/index.js

REDIS_HOST=localhost REDIS_PORT=6379 REDIS_PASSWORD=test_password \
  python tests/docker-compat/python-client/test_compat.py

# Generate a comparison report from collected results
python tests/docker-compat/report/generate-report.py --results-dir tests/docker-compat/results/
```

### Test Methodology

1. A Redis 7 container and an rLightning container are started with identical configurations
2. Each client library runs its full test suite against both servers
3. Results are collected in JSON format
4. A comparison report highlights any behavioral differences
5. Containers are cleaned up automatically after the run

For the current list of known behavioral differences (if any), see `tests/docker-compat/KNOWN-INCOMPATIBILITIES.txt` in the repository.
