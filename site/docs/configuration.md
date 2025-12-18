# Configuration

rLightning can be configured through command-line arguments, configuration files, or environment variables.

## Configuration Priority

Settings are applied in this order (highest to lowest priority):

1. Command-line arguments
2. Environment variables
3. Configuration file
4. Default values

## Configuration File

Create a `config.toml` file:

```toml
[server]
host = "0.0.0.0"
port = 6379
workers = 4

[storage]
max_memory_mb = 1024
eviction_policy = "lru"
enable_ttl_monitor = true
ttl_check_interval_secs = 1

[persistence]
mode = "hybrid"  # "none", "rdb", "aof", "hybrid"
rdb_path = "dump.rdb"
rdb_save_interval_secs = 300
aof_path = "appendonly.aof"
aof_sync_strategy = "everysec"  # "always", "everysec", "no"

[security]
requirepass = "your-secure-password"

[replication]
role = "master"  # "master" or "replica"
# For replicas:
# master_host = "127.0.0.1"
# master_port = 6379
# master_auth = "master-password"
```

Run with config file:

```bash
rlightning --config config.toml
```

## Command-Line Options

All options can be specified on the command line:

```bash
rlightning [OPTIONS]

OPTIONS:
  --host <HOST>
      Host to bind to [default: 127.0.0.1]

  --port <PORT>
      Port to listen on [default: 6379]

  --config <FILE>
      Path to configuration file

  --workers <N>
      Number of worker threads [default: num_cpus]

  --max-memory-mb <MB>
      Maximum memory in megabytes [default: 512]

  --eviction-policy <POLICY>
      Eviction policy: lru, random, noeviction [default: lru]

  --persistence-mode <MODE>
      Persistence mode: none, rdb, aof, hybrid [default: none]

  --rdb-path <PATH>
      Path to RDB file [default: dump.rdb]

  --aof-path <PATH>
      Path to AOF file [default: appendonly.aof]

  --requirepass <PASSWORD>
      Require password for authentication

  --replica-of <HOST:PORT>
      Make this instance a replica of another instance

  -h, --help
      Print help information

  -V, --version
      Print version information
```

### Examples

```bash
# Basic usage
rlightning --host 0.0.0.0 --port 6380

# With memory limit
rlightning --max-memory-mb 2048 --eviction-policy lru

# With persistence
rlightning --persistence-mode hybrid

# With authentication
rlightning --requirepass "my-secret-password"

# As replica
rlightning --replica-of 192.168.1.100:6379

# All together
rlightning \
  --host 0.0.0.0 \
  --port 6379 \
  --max-memory-mb 1024 \
  --persistence-mode hybrid \
  --requirepass "secret"
```

## Environment Variables

Set configuration via environment variables:

```bash
# Server settings
export RLIGHTNING_HOST="0.0.0.0"
export RLIGHTNING_PORT="6379"
export RLIGHTNING_WORKERS="4"

# Storage settings
export RLIGHTNING_MAX_MEMORY_MB="1024"
export RLIGHTNING_EVICTION_POLICY="lru"

# Persistence
export RLIGHTNING_PERSISTENCE_MODE="hybrid"
export RLIGHTNING_RDB_PATH="/data/dump.rdb"
export RLIGHTNING_AOF_PATH="/data/appendonly.aof"

# Security
export RLIGHTNING_REQUIREPASS="your-password"

# Run
rlightning
```

## Server Configuration

### Network Settings

#### Host
```bash
--host 0.0.0.0  # Listen on all interfaces
--host 127.0.0.1  # Listen only on localhost (default)
```

#### Port
```bash
--port 6379  # Default Redis port
--port 6380  # Custom port
```

#### Workers
```bash
--workers 4  # Specific number of worker threads
# Default: number of CPU cores
```

### Memory Management

See [Memory Management](memory.md) for detailed information.

#### Max Memory
```bash
--max-memory-mb 512   # 512 MB (default)
--max-memory-mb 1024  # 1 GB
--max-memory-mb 2048  # 2 GB
```

#### Eviction Policy
```bash
--eviction-policy lru         # Least Recently Used (default)
--eviction-policy random      # Random eviction
--eviction-policy noeviction  # Fail on memory full
```

## Persistence Configuration

See [Persistence](persistence.md) for detailed information.

### RDB (Snapshots)

```toml
[persistence]
mode = "rdb"
rdb_path = "dump.rdb"
rdb_save_interval_secs = 300  # Save every 5 minutes
```

### AOF (Append-Only File)

```toml
[persistence]
mode = "aof"
aof_path = "appendonly.aof"
aof_sync_strategy = "everysec"  # "always", "everysec", "no"
```

### Hybrid Mode

```toml
[persistence]
mode = "hybrid"
rdb_path = "dump.rdb"
rdb_save_interval_secs = 300
aof_path = "appendonly.aof"
aof_sync_strategy = "everysec"
```

## Replication Configuration

See [Replication](replication.md) for detailed information.

### Master Configuration

```toml
[replication]
role = "master"
```

### Replica Configuration

```toml
[replication]
role = "replica"
master_host = "192.168.1.100"
master_port = 6379
master_auth = "master-password"  # If master requires auth
```

## Security Configuration

See [Security](security.md) for detailed information.

### Authentication

```bash
# Command line
rlightning --requirepass "your-secure-password"

# Config file
[security]
requirepass = "your-secure-password"

# Environment
export RLIGHTNING_REQUIREPASS="your-secure-password"
```

## Runtime Configuration

Some settings can be changed at runtime using the `CONFIG` command:

```bash
# View configuration
CONFIG GET max-memory-mb

# Set configuration (if supported)
CONFIG SET max-memory-mb 2048
```

## Docker Configuration

### Environment Variables

```bash
docker run -d \
  -e RLIGHTNING_HOST="0.0.0.0" \
  -e RLIGHTNING_MAX_MEMORY_MB="1024" \
  -e RLIGHTNING_REQUIREPASS="secret" \
  -p 6379:6379 \
  altista/rlightning:latest
```

### Config File Mount

```bash
docker run -d \
  -v /path/to/config.toml:/etc/rlightning/config.toml \
  -p 6379:6379 \
  altista/rlightning:latest \
  --config /etc/rlightning/config.toml
```

### Docker Compose

```yaml
version: '3.8'

services:
  rlightning:
    image: altista/rlightning:latest
    ports:
      - "6379:6379"
    volumes:
      - ./config.toml:/etc/rlightning/config.toml
      - rlightning-data:/data
    environment:
      RLIGHTNING_MAX_MEMORY_MB: 1024
      RLIGHTNING_EVICTION_POLICY: lru
    command: --config /etc/rlightning/config.toml
    restart: unless-stopped

volumes:
  rlightning-data:
```

## Best Practices

1. **Use config files for complex setups** - Easier to manage than long command lines
2. **Set memory limits** - Prevent OOM issues
3. **Enable persistence** - Protect against data loss
4. **Use authentication in production** - Security first
5. **Monitor resource usage** - Use `INFO` command
6. **Set appropriate TTLs** - Automatic memory management
7. **Use replication for HA** - High availability setup

## Troubleshooting

### Can't Bind to Port

```bash
# Check if port is already in use
lsof -i :6379

# Use different port
rlightning --port 6380
```

### Memory Issues

```bash
# Increase memory limit
rlightning --max-memory-mb 2048

# Use more aggressive eviction
rlightning --eviction-policy lru
```

### Permission Issues

```bash
# Ensure data directory is writable
chmod 755 /data

# Run as specific user (Docker)
docker run --user 1000:1000 ...
```

## Next Steps

- [Memory Management](memory.md)
- [Persistence Options](persistence.md)
- [Replication Setup](replication.md)
- [Security Guide](security.md)
