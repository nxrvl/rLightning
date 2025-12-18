# Quick Start

Get rLightning up and running in 5 minutes.

## Step 1: Install

Choose your preferred method:

=== "Docker"
    ```bash
    docker pull altista/rlightning:latest
    ```

=== "Cargo"
    ```bash
    cargo install rlightning
    ```

=== "Binary"
    Download from [releases](https://github.com/altista-tech/rLightning/releases)

## Step 2: Run

Start the server:

=== "Docker"
    ```bash
    docker run -d -p 6379:6379 --name rlightning altista/rlightning:latest
    ```

=== "Cargo/Binary"
    ```bash
    rlightning
    ```

## Step 3: Connect

Use any Redis client:

```bash
redis-cli -h localhost -p 6379
```

## Step 4: Try It Out

```bash
# Basic commands
PING
SET hello "world"
GET hello

# With expiration
SET session:1 "active" EX 60

# Counters
SET visitors 0
INCR visitors
INCR visitors

# Lists
LPUSH tasks "task1" "task2"
LRANGE tasks 0 -1

# Hashes
HSET user:1 name "John" email "john@example.com"
HGETALL user:1
```

## What's Next?

- [Full Installation Guide](getting-started.md)
- [Command Reference](commands/index.md)
- [Configuration Options](configuration.md)
