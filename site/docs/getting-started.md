# Getting Started

This guide will help you install and run rLightning quickly.

## Installation

### Docker (Recommended)

The easiest way to get started with rLightning is using Docker:

```bash
docker run -d -p 6379:6379 --name rlightning altista/rlightning:latest
```

Check if it's running:

```bash
docker ps | grep rlightning
```

### Cargo Install

If you have Rust installed, you can install rLightning from crates.io:

```bash
cargo install rlightning
```

Then run it:

```bash
rlightning
```

### Building from Source

Clone the repository and build:

```bash
# Clone the repository
git clone https://github.com/altista-tech/rLightning.git
cd rLightning

# Build in release mode
cargo build --release

# Run the binary
./target/release/rlightning
```

### Pre-built Binaries

Download pre-built binaries from the [releases page](https://github.com/altista-tech/rLightning/releases):

```bash
# Example for Linux
wget https://github.com/altista-tech/rLightning/releases/latest/download/rlightning-linux-amd64
chmod +x rlightning-linux-amd64
./rlightning-linux-amd64
```

## First Steps

### Starting the Server

Start rLightning with default settings:

```bash
rlightning
```

You should see output like:

```
[INFO] rLightning v1.0.0 starting...
[INFO] Listening on 127.0.0.1:6379
[INFO] Ready to accept connections
```

### Connecting to the Server

Use any Redis client to connect. The most common is `redis-cli`:

```bash
redis-cli -h 127.0.0.1 -p 6379
```

### Your First Commands

Try these basic commands:

```bash
# Test the connection
127.0.0.1:6379> PING
PONG

# Set a key
127.0.0.1:6379> SET mykey "Hello, rLightning!"
OK

# Get the key
127.0.0.1:6379> GET mykey
"Hello, rLightning!"

# Set a key with expiration (60 seconds)
127.0.0.1:6379> SET tempkey "temporary value" EX 60
OK

# Check time to live
127.0.0.1:6379> TTL tempkey
(integer) 57

# Increment a counter
127.0.0.1:6379> SET counter 0
OK
127.0.0.1:6379> INCR counter
(integer) 1
127.0.0.1:6379> INCR counter
(integer) 2
```

## Basic Configuration

### Command-Line Options

Run rLightning with custom options:

```bash
# Custom host and port
rlightning --host 0.0.0.0 --port 6380

# Set memory limit
rlightning --max-memory-mb 1024

# Enable authentication
rlightning --requirepass "your-secure-password"

# Multiple options
rlightning --host 0.0.0.0 --port 6379 --max-memory-mb 512 --requirepass "secret"
```

### Configuration File

Create a `config.toml` file:

```toml
[server]
host = "0.0.0.0"
port = 6379
workers = 4

[storage]
max_memory_mb = 512
eviction_policy = "lru"

[security]
requirepass = "your-password"
```

Run with the config file:

```bash
rlightning --config config.toml
```

## Using with Your Application

### Python Example

```python
import redis

# Connect to rLightning
r = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Set a value
r.set('user:1001', 'John Doe')

# Get a value
name = r.get('user:1001')
print(name)  # Output: John Doe

# Use hashes
r.hset('user:1002', mapping={
    'name': 'Jane Smith',
    'email': 'jane@example.com',
    'age': 30
})

# Get all hash fields
user = r.hgetall('user:1002')
print(user)
```

### Node.js Example

```javascript
const redis = require('redis');

// Connect to rLightning
const client = redis.createClient({
  host: 'localhost',
  port: 6379
});

await client.connect();

// Set a value
await client.set('session:abc123', 'user_data', {
  EX: 3600  // Expire in 1 hour
});

// Get a value
const data = await client.get('session:abc123');
console.log(data);

// Use lists
await client.lPush('jobs', 'job1', 'job2', 'job3');
const job = await client.rPop('jobs');
console.log(job);  // Output: job3
```

### Go Example

```go
package main

import (
    "context"
    "fmt"
    "github.com/go-redis/redis/v8"
    "time"
)

func main() {
    ctx := context.Background()
    
    // Connect to rLightning
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    
    // Set a value with expiration
    err := rdb.Set(ctx, "cache:user:1001", "user_data", 5*time.Minute).Err()
    if err != nil {
        panic(err)
    }
    
    // Get a value
    val, err := rdb.Get(ctx, "cache:user:1001").Result()
    if err != nil {
        panic(err)
    }
    fmt.Println(val)
    
    // Use sorted sets
    rdb.ZAdd(ctx, "leaderboard", &redis.Z{Score: 100, Member: "player1"})
    rdb.ZAdd(ctx, "leaderboard", &redis.Z{Score: 200, Member: "player2"})
    
    // Get top players
    players, err := rdb.ZRevRange(ctx, "leaderboard", 0, 9).Result()
    fmt.Println(players)
}
```

### Rust Example

```rust
use redis::{Client, Commands};

fn main() -> redis::RedisResult<()> {
    // Connect to rLightning
    let client = Client::open("redis://127.0.0.1:6379")?;
    let mut con = client.get_connection()?;
    
    // Set a value
    con.set("mykey", "myvalue")?;
    
    // Get a value
    let value: String = con.get("mykey")?;
    println!("{}", value);
    
    // Use sets
    con.sadd("myset", "member1")?;
    con.sadd("myset", "member2")?;
    
    let members: Vec<String> = con.smembers("myset")?;
    println!("{:?}", members);
    
    Ok(())
}
```

## Docker Compose Setup

Create a `docker-compose.yml` file for easy deployment:

```yaml
version: '3.8'

services:
  rlightning:
    image: altista/rlightning:latest
    container_name: rlightning
    ports:
      - "6379:6379"
    volumes:
      - rlightning-data:/data
    environment:
      - RLIGHTNING_MAX_MEMORY_MB=1024
      - RLIGHTNING_EVICTION_POLICY=lru
    command: >
      --host 0.0.0.0
      --port 6379
      --max-memory-mb 1024
      --persistence-mode hybrid
      --rdb-path /data/dump.rdb
      --aof-path /data/appendonly.aof
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 10s
      timeout: 3s
      retries: 3

volumes:
  rlightning-data:
```

Start the service:

```bash
docker-compose up -d
```

## Next Steps

Now that you have rLightning running, explore these topics:

- [Command Reference](commands/index.md) - Learn about all available commands
- [Configuration](configuration.md) - Deep dive into configuration options
- [Persistence](persistence.md) - Set up data persistence
- [Replication](replication.md) - Configure master-replica setup
- [Security](security.md) - Secure your instance

## Getting Help

If you encounter any issues:

1. Check the [FAQ](faq.md)
2. Search [GitHub Issues](https://github.com/altista-tech/rLightning/issues)
3. Ask in [Discussions](https://github.com/altista-tech/rLightning/discussions)
4. Email: support@altista.tech
