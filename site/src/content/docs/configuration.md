---
title: Configuration
description: Configure rLightning server settings
order: 4
category: reference
---

# Configuration

rLightning can be configured through TOML configuration files, command-line arguments, and environment variables. Settings are resolved in the following priority order:

1. **Command-line arguments** (highest priority)
2. **Environment variables**
3. **Configuration file**
4. **Default values** (lowest priority)

## TOML Configuration File

By default, rLightning looks for `config/default.toml`. You can specify a custom path with `--config`.

### Full Example

```toml
[server]
host = "127.0.0.1"
port = 6379
workers = 4  # 0 = auto-detect based on CPU cores

[storage]
max_memory_mb = 0          # 0 = unlimited
eviction_policy = "noeviction"
default_ttl_seconds = 0    # 0 = no expiration

[storage.key_constraints]
max_key_size = 512             # bytes
max_value_size = 536870912     # 512 MB

[security]
require_auth = false
password = ""

[logging]
level = "info"    # trace, debug, info, warn, error
file = ""         # empty = stdout
```

### Server Section

| Setting   | Default       | Description                                       |
|-----------|---------------|---------------------------------------------------|
| `host`    | `"127.0.0.1"` | Bind address for incoming connections             |
| `port`    | `6379`        | TCP port to listen on                              |
| `workers` | `4`           | Number of worker threads (`0` = auto-detect CPUs) |

### Storage Section

| Setting              | Default          | Description                          |
|----------------------|------------------|--------------------------------------|
| `max_memory_mb`      | `0`              | Maximum memory in MB (`0` = unlimited) |
| `eviction_policy`    | `"noeviction"`   | Policy when memory limit is reached  |
| `default_ttl_seconds`| `0`              | Default TTL for new keys (`0` = none)|

**Available eviction policies:**

- `noeviction` -- Return errors when memory limit is reached
- `allkeys-lru` -- Evict least recently used keys
- `allkeys-random` -- Evict random keys
- `volatile-lru` -- Evict least recently used keys with an expiration set
- `volatile-random` -- Evict random keys with an expiration set
- `volatile-ttl` -- Evict keys with the shortest remaining TTL

### Key Constraints

| Setting          | Default       | Description                    |
|------------------|---------------|--------------------------------|
| `max_key_size`   | `512`         | Maximum key size in bytes      |
| `max_value_size` | `536870912`   | Maximum value size in bytes (512 MB) |

### Security Section

| Setting        | Default  | Description                              |
|----------------|----------|------------------------------------------|
| `require_auth` | `false`  | Whether clients must authenticate        |
| `password`     | `""`     | Password for AUTH command                 |

When `require_auth` is `true`, clients must issue `AUTH <password>` (or `AUTH <username> <password>` when using ACLs) before executing any other commands.

### Logging Section

| Setting | Default  | Description                                  |
|---------|----------|----------------------------------------------|
| `level` | `"info"` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `file`  | `""`     | Log file path (empty string = log to stdout) |

## Command-Line Arguments

Command-line arguments override all other configuration sources.

```bash
# Specify a config file
cargo run --release -- --config path/to/config.toml

# Override individual settings
cargo run --release -- --port 6380
cargo run --release -- --bind 0.0.0.0
cargo run --release -- --max-memory-mb 512
cargo run --release -- --password mysecretpassword

# Combine multiple arguments
cargo run --release -- --port 6380 --bind 0.0.0.0 --max-memory-mb 1024 --password secret
```

| Argument          | Description                          |
|-------------------|--------------------------------------|
| `--config`        | Path to TOML configuration file      |
| `--port`          | TCP port to listen on                |
| `--bind`          | Bind address                         |
| `--max-memory-mb` | Maximum memory limit in MB           |
| `--password`      | Authentication password              |

## Environment Variables

Environment variables use the `RLIGHTNING_` prefix followed by the uppercase setting name. They override config file values but are overridden by command-line arguments.

```bash
# Examples
export RLIGHTNING_PORT=6380
export RLIGHTNING_HOST=0.0.0.0
export RLIGHTNING_MAX_MEMORY_MB=1024
export RLIGHTNING_PASSWORD=mysecretpassword
export RLIGHTNING_LOG_LEVEL=debug

# Run with environment variable configuration
RLIGHTNING_PORT=6380 RLIGHTNING_PASSWORD=secret cargo run --release
```

## Configuration Examples

### Development Setup

```toml
[server]
host = "127.0.0.1"
port = 6379
workers = 0

[storage]
max_memory_mb = 0
eviction_policy = "noeviction"

[security]
require_auth = false

[logging]
level = "debug"
```

### Production Setup

```toml
[server]
host = "0.0.0.0"
port = 6379
workers = 0

[storage]
max_memory_mb = 4096
eviction_policy = "allkeys-lru"

[storage.key_constraints]
max_key_size = 512
max_value_size = 536870912

[security]
require_auth = true
password = "your-strong-password-here"

[logging]
level = "warn"
file = "/var/log/rlightning/server.log"
```

### Memory-Constrained Environment

```toml
[server]
host = "127.0.0.1"
port = 6379
workers = 2

[storage]
max_memory_mb = 256
eviction_policy = "volatile-lru"
default_ttl_seconds = 3600

[security]
require_auth = true
password = "secret"

[logging]
level = "info"
```
