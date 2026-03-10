---
title: "Getting Started"
description: "Install and set up rLightning"
order: 1
category: "getting-started"
---

# Getting Started

rLightning is a high-performance, Redis 7.x compatible in-memory data store built in Rust. This guide covers installation and initial setup.

## Prerequisites

- **Docker** (recommended) -- no other dependencies needed
- **Rust 1.82+** -- only required if building from source
- **redis-cli** or any Redis client -- for verifying your installation

## Installation

### Docker (Recommended)

The fastest way to get rLightning running. Images are available for linux/amd64, linux/arm64, and linux/arm/v7.

```bash
# Pull the latest image
docker pull nxrvl/rlightning

# Start rLightning on port 6379
docker run -d --name rlightning -p 6379:6379 nxrvl/rlightning
```

That's it. rLightning is now accepting connections on `localhost:6379`.

### Cargo Install

If you have Rust installed, you can install rLightning directly:

```bash
cargo install rlightning
```

Then start the server:

```bash
rlightning --port 6379
```

### Build from Source

Clone the repository and build with optimizations:

```bash
git clone https://github.com/nxrvl/rLightning.git
cd rLightning
cargo build --release
```

The compiled binary is at `./target/release/rlightning`:

```bash
./target/release/rlightning --port 6379
```

## Verify Installation

Connect with `redis-cli` and run a quick test:

```bash
redis-cli -h localhost -p 6379
```

```bash
127.0.0.1:6379> PING
PONG

127.0.0.1:6379> SET greeting "Hello from rLightning"
OK

127.0.0.1:6379> GET greeting
"Hello from rLightning"
```

If you see `PONG` and can store and retrieve a value, rLightning is running correctly.

## Next Steps

- **[Quick Start](/docs/quick-start)** -- learn the core commands with hands-on examples
- **[Configuration](/docs/configuration)** -- tune rLightning for your workload
- **[Docker](/docs/docker)** -- production Docker setup with volumes, compose, and health checks
