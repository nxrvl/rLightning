---
title: "Quick Start"
description: "Get up and running with rLightning in minutes"
order: 2
category: "getting-started"
---

# Quick Start

This guide walks through the core Redis commands supported by rLightning. Every example works identically against Redis 7.x -- rLightning is a drop-in replacement.

## Starting the Server

With Docker:

```bash
docker run -d --name rlightning -p 6379:6379 nxrvl/rlightning
```

Or with the binary directly:

```bash
./rlightning --port 6379
```

## Connecting

Use `redis-cli` or any Redis-compatible client:

```bash
redis-cli -h localhost -p 6379
```

## String Operations

Strings are the most basic data type. They can hold text, numbers, or binary data up to 512 MB.

```bash
# Set and get a value
SET user:name "Alice"
GET user:name
# "Alice"

# Set multiple keys at once
MSET user:email "alice@example.com" user:city "Seattle" user:role "admin"

# Get multiple keys at once
MGET user:email user:city user:role
# 1) "alice@example.com"
# 2) "Seattle"
# 3) "admin"

# Atomic counter
SET request:count 0
INCR request:count
# (integer) 1
INCR request:count
# (integer) 2
```

## Hashes

Hashes map fields to values under a single key -- ideal for representing objects.

```bash
# Set fields on a hash
HSET user:1001 name "Bob" email "bob@example.com" age "32"

# Get a single field
HGET user:1001 name
# "Bob"

# Get all fields and values
HGETALL user:1001
# 1) "name"
# 2) "Bob"
# 3) "email"
# 4) "bob@example.com"
# 5) "age"
# 6) "32"
```

## Lists

Lists are ordered sequences of strings. They support push/pop from both ends, making them useful as queues or stacks.

```bash
# Build a task queue
LPUSH tasks "send-email"
LPUSH tasks "resize-image"
RPUSH tasks "generate-report"

# View all tasks (0 to -1 means all elements)
LRANGE tasks 0 -1
# 1) "resize-image"
# 2) "send-email"
# 3) "generate-report"
```

## Sets

Sets are unordered collections of unique strings. They support membership tests and set operations.

```bash
# Add tags to articles
SADD article:1:tags "rust" "database" "performance"
SADD article:2:tags "rust" "networking" "async"

# List all members
SMEMBERS article:1:tags
# 1) "rust"
# 2) "database"
# 3) "performance"

# Find common tags between articles
SINTER article:1:tags article:2:tags
# 1) "rust"
```

## Key Expiration

Set keys to automatically expire after a given time.

```bash
# Set a key with a 60-second TTL
SET session:abc123 "user_data" EX 60

# Check remaining time to live
TTL session:abc123
# (integer) 58

# Set expiration on an existing key
SET cache:homepage "<html>...</html>"
EXPIRE cache:homepage 300
```

## Transactions

Group commands into an atomic unit with `MULTI`/`EXEC`. Either all commands execute or none do.

```bash
MULTI
SET account:alice:balance 750
SET account:bob:balance 250
EXEC
# 1) OK
# 2) OK
```

All commands between `MULTI` and `EXEC` are queued and executed atomically. Other clients will never see a partial state.

## Pub/Sub

Publish and subscribe to real-time message channels.

In one terminal, subscribe to a channel:

```bash
redis-cli SUBSCRIBE notifications
```

In another terminal, publish a message:

```bash
redis-cli PUBLISH notifications "deployment complete"
```

The subscriber immediately receives:

```
1) "message"
2) "notifications"
3) "deployment complete"
```

## Server Information

Inspect the running server:

```bash
# Server overview
INFO server

# Number of keys in the current database
DBSIZE

# View configuration
CONFIG GET maxmemory
CONFIG GET save
```

## Next Steps

- **[Configuration](/docs/configuration)** -- tune memory limits, persistence, eviction policies, and more
- **[Commands Reference](/docs/commands)** -- full reference for all 400+ supported commands
- **[Docker](/docs/docker)** -- production deployment with Docker Compose
