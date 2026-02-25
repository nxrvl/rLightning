# Optimization Tips

## Pipeline Commands

Reduce round trips by sending multiple commands at once:

```bash
# Instead of separate calls, use pipelines
# Most Redis client libraries support pipelining
```

## Use Appropriate Data Types

- Hashes for objects (more memory-efficient than separate string keys)
- Sorted Sets for rankings and time-series
- Streams for event logs instead of Lists
- HyperLogLog for unique counting (12KB vs storing all elements)

## Set TTLs

Always set expiration on cache keys to prevent memory bloat:

```bash
SET key value EX 3600
```

## Avoid Expensive Commands

- Use `SCAN` instead of `KEYS` in production
- Use `HSCAN`, `SSCAN`, `ZSCAN` for large collections
- Limit `LRANGE`, `ZRANGEBYSCORE` results with COUNT

## Connection Management

- Use connection pooling in your application
- Enable pipelining for batch operations
- Use `CLIENT TRACKING` for client-side caching

## Memory Optimization

- Monitor with `MEMORY USAGE key` and `INFO memory`
- Use `MEMORY DOCTOR` for recommendations
- Set appropriate `max_memory_mb` limits
