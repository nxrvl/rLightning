---
title: "Docker"
description: "Run rLightning with Docker"
order: 3
category: "getting-started"
---

# Docker

rLightning publishes official Docker images to Docker Hub at [nxrvl/rlightning](https://hub.docker.com/r/nxrvl/rlightning).

## Supported Platforms

| Architecture    | Tag Suffix |
|-----------------|-----------|
| linux/amd64     | default   |
| linux/arm64     | default   |
| linux/arm/v7    | default   |

Multi-arch manifests are used, so `docker pull` automatically selects the correct image for your platform.

## Available Tags

| Tag       | Description                          |
|-----------|--------------------------------------|
| `latest`  | Most recent stable release           |
| `2.0.0`   | Specific version                     |
| `2.0`     | Latest patch within the 2.0.x line   |
| `2`       | Latest minor within the 2.x.x line   |

```bash
# Pull a specific version
docker pull nxrvl/rlightning:2.0.0

# Or always use the latest
docker pull nxrvl/rlightning:latest
```

## Basic Usage

Start rLightning with default settings on port 6379:

```bash
docker run -d --name rlightning -p 6379:6379 nxrvl/rlightning
```

Test the connection:

```bash
redis-cli -h localhost -p 6379 PING
# PONG
```

## With Authentication

Require a password for all client connections:

```bash
docker run -d --name rlightning \
  -p 6379:6379 \
  -e RLIGHTNING_PASSWORD=secret \
  nxrvl/rlightning
```

Connect with the password:

```bash
redis-cli -h localhost -p 6379 -a secret PING
```

## With a Custom Configuration File

Mount your own `config.toml` into the container:

```bash
docker run -d --name rlightning \
  -p 6379:6379 \
  -v ./config.toml:/etc/rlightning/config.toml:ro \
  nxrvl/rlightning --config /etc/rlightning/config.toml
```

## With Persistent Data

Use a named volume to persist data across container restarts:

```bash
docker run -d --name rlightning \
  -p 6379:6379 \
  -v rlightning-data:/data \
  nxrvl/rlightning
```

RDB snapshots and AOF files are written to `/data` inside the container.

## Docker Compose

A production-ready Compose file with persistence, health checking, and automatic restarts:

```yaml
services:
  rlightning:
    image: nxrvl/rlightning:2.0.0
    container_name: rlightning
    ports:
      - "6379:6379"
    environment:
      RLIGHTNING_PASSWORD: "${RLIGHTNING_PASSWORD:-changeme}"
      RLIGHTNING_MAXMEMORY: "256mb"
    volumes:
      - rlightning-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "-a", "${RLIGHTNING_PASSWORD:-changeme}", "PING"]
      interval: 10s
      timeout: 5s
      retries: 5
      start_period: 5s
    restart: unless-stopped
    deploy:
      resources:
        limits:
          memory: 512M

volumes:
  rlightning-data:
    driver: local
```

Start the service:

```bash
docker compose up -d
```

Check health status:

```bash
docker compose ps
```

## Environment Variables

Configure rLightning at startup using environment variables. These override values in the configuration file.

| Variable                  | Description                  | Default     |
|---------------------------|------------------------------|-------------|
| `RLIGHTNING_BIND`        | Bind address                 | `0.0.0.0`  |
| `RLIGHTNING_PORT`        | Listen port                  | `6379`      |
| `RLIGHTNING_PASSWORD`    | Require password for AUTH    | *(none)*    |
| `RLIGHTNING_MAXMEMORY`   | Maximum memory limit         | *(none)*    |
| `RLIGHTNING_DATABASES`   | Number of databases          | `16`        |
| `RLIGHTNING_LOGLEVEL`    | Log level (debug/info/warn)  | `info`      |
| `RLIGHTNING_DIR`         | Data directory               | `/data`     |
| `RLIGHTNING_APPENDONLY`  | Enable AOF persistence       | `no`        |

## Container Details

- **Base image:** `debian:bookworm-slim`
- **Runs as:** non-root user (`rlightning`)
- **Exposed port:** `6379`
- **Data directory:** `/data`
- **Config path:** `/etc/rlightning/config.toml` (when mounted)

## Health Checking

### Using redis-cli

```bash
docker exec rlightning redis-cli PING
```

### Using netcat

For lightweight checks when redis-cli is not available:

```bash
docker exec rlightning sh -c 'nc -z localhost 6379 && echo "healthy" || echo "unhealthy"'
```

### In Kubernetes or orchestrators

Use a TCP liveness probe on port 6379, or an exec probe running `redis-cli PING`.

```yaml
livenessProbe:
  exec:
    command: ["redis-cli", "PING"]
  initialDelaySeconds: 5
  periodSeconds: 10
readinessProbe:
  tcpSocket:
    port: 6379
  initialDelaySeconds: 3
  periodSeconds: 5
```

## Next Steps

- **[Configuration](/docs/configuration)** -- full reference for all configuration options
- **[Getting Started](/docs/getting-started)** -- installation without Docker
- **[Quick Start](/docs/quick-start)** -- learn the core commands
