# Persistence

rLightning supports three persistence modes for data durability.

## RDB Snapshots

Point-in-time snapshots saved to disk at configurable intervals.

```toml
[persistence]
mode = "rdb"
rdb_path = "dump.rdb"
```

```bash
BGSAVE          # Trigger background save
SAVE            # Trigger blocking save
LASTSAVE        # Get last save timestamp
```

## AOF (Append-Only File)

Logs every write operation for maximum durability.

```toml
[persistence]
mode = "aof"
aof_path = "appendonly.aof"
aof_sync = "everysec"  # always, everysec, no
```

```bash
BGREWRITEAOF    # Compact AOF file
```

## Hybrid Mode

Combines RDB and AOF for fast restarts with full durability.

```toml
[persistence]
mode = "hybrid"
```

## Comparison

| Feature | RDB | AOF | Hybrid |
|---------|-----|-----|--------|
| Startup speed | Fast | Slower | Fast |
| Data loss window | Minutes | ~1 second | ~1 second |
| Disk usage | Compact | Larger | Moderate |
| CPU impact | Low (bg save) | Low | Low |
