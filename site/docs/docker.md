# Docker Setup

## Quick Start

```bash
docker run -d --name rlightning -p 6379:6379 altista/rlightning:latest
```

## With Persistence

```bash
docker run -d --name rlightning \
  -p 6379:6379 \
  -v /data/rlightning:/data \
  altista/rlightning:latest \
  --persistence-mode hybrid \
  --rdb-path /data/dump.rdb \
  --aof-path /data/appendonly.aof
```

## With Authentication

```bash
docker run -d --name rlightning \
  -p 6379:6379 \
  altista/rlightning:latest \
  --requirepass your-secure-password
```

## Docker Compose

```yaml
version: '3.8'

services:
  rlightning:
    image: altista/rlightning:latest
    ports:
      - "6379:6379"
    volumes:
      - rlightning-data:/data
    command: >
      --persistence-mode hybrid
      --max-memory-mb 1024
      --requirepass your-password
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "redis-cli", "-a", "your-password", "ping"]
      interval: 10s
      timeout: 5s
      retries: 3

volumes:
  rlightning-data:
```

## Building from Source

```bash
docker build -t rlightning:custom .
docker run -d -p 6379:6379 rlightning:custom
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `RLIGHTNING_PORT` | Listen port | 6379 |
| `RLIGHTNING_HOST` | Bind address | 0.0.0.0 |
| `RLIGHTNING_MAX_MEMORY` | Max memory in MB | 512 |
| `RLIGHTNING_REQUIREPASS` | Authentication password | (none) |
