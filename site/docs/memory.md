# Memory Management

## Configuration

```toml
[storage]
max_memory_mb = 1024
eviction_policy = "lru"
```

## Eviction Policies

| Policy | Description |
|--------|-------------|
| `lru` | Remove least recently used keys |
| `random` | Remove random keys |
| `noeviction` | Return errors when memory limit is reached |

## Memory Commands

```bash
MEMORY USAGE key [SAMPLES count]
MEMORY STATS
MEMORY DOCTOR
MEMORY PURGE
INFO memory
```

## Best Practices

- Set `max_memory_mb` to 75% of available RAM
- Use `lru` eviction for cache workloads
- Set TTLs on keys to enable automatic cleanup
- Monitor memory usage with `INFO memory`
