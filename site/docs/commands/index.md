# Command Reference

rLightning implements a subset of Redis commands, focusing on the most commonly used operations for session management and caching.

## Command Categories

### [String Commands](strings.md)
Basic key-value operations, the foundation of any key-value store.

- `GET`, `SET`, `MGET`, `MSET` - Basic operations
- `INCR`, `DECR` - Atomic counters
- `APPEND` - String manipulation

[View String Commands →](strings.md)

### [Hash Commands](hashes.md)
Store and manipulate field-value pairs within a single key.

- `HGET`, `HSET`, `HGETALL` - Field operations
- `HDEL`, `HEXISTS` - Hash management

[View Hash Commands →](hashes.md)

### [List Commands](lists.md)
Ordered collections of strings, useful for queues and stacks.

- `LPUSH`, `RPUSH` - Add elements
- `LPOP`, `RPOP` - Remove elements
- `LRANGE` - Retrieve ranges

[View List Commands →](lists.md)

### [Set Commands](sets.md)
Unordered collections of unique strings.

- `SADD`, `SREM` - Manage members
- `SMEMBERS` - Get all members
- `SISMEMBER` - Check membership

[View Set Commands →](sets.md)

### [Sorted Set Commands](sorted-sets.md)
Sets with scores, perfect for rankings and leaderboards.

- `ZADD`, `ZREM` - Manage scored members
- `ZRANGE` - Range queries
- `ZSCORE` - Get scores

[View Sorted Set Commands →](sorted-sets.md)

### [Pub/Sub Commands](pubsub.md)
Publish/Subscribe messaging for real-time communication.

- `SUBSCRIBE`, `UNSUBSCRIBE` - Channel subscriptions
- `PSUBSCRIBE`, `PUNSUBSCRIBE` - Pattern subscriptions
- `PUBLISH` - Send messages
- `PUBSUB` - Introspection commands

[View Pub/Sub Commands →](pubsub.md)

### [Server Commands](server.md)
Server management and information commands.

- `PING`, `INFO` - Server status
- `AUTH` - Authentication
- `CONFIG` - Configuration
- `KEYS`, `DEL`, `EXISTS` - Key management

[View Server Commands →](server.md)

## Command Compatibility

rLightning aims for high compatibility with Redis. The following matrix shows the current implementation status:

| Category | Implemented | Total | Coverage |
|----------|-------------|-------|----------|
| Strings  | 7/15        | 15    | 47%      |
| Hashes   | 5/13        | 13    | 38%      |
| Lists    | 5/17        | 17    | 29%      |
| Sets     | 4/15        | 15    | 27%      |
| Sorted Sets | 4/21     | 21    | 19%      |
| Pub/Sub  | 8/8         | 8     | 100%     |
| Keys     | 5/20        | 20    | 25%      |
| Server   | 8/30        | 30    | 27%      |

## Common Command Patterns

### Caching Pattern

```bash
# Try to get from cache
GET cache:user:1001

# If miss, fetch from source and cache
SET cache:user:1001 "user_data" EX 300
```

### Session Management

```bash
# Create session
SET session:abc123 "user_id:1001" EX 3600

# Check session
GET session:abc123

# Extend session
EXPIRE session:abc123 3600

# Delete session
DEL session:abc123
```

### Rate Limiting

```bash
# Increment request count
INCR ratelimit:user:1001:2024-01-15

# Set expiry if first request
EXPIRE ratelimit:user:1001:2024-01-15 86400

# Check if over limit
GET ratelimit:user:1001:2024-01-15
```

### Leaderboard

```bash
# Add scores
ZADD leaderboard 1000 player1
ZADD leaderboard 1500 player2
ZADD leaderboard 800 player3

# Get top 10
ZREVRANGE leaderboard 0 9 WITHSCORES

# Get player rank
ZREVRANK leaderboard player1
```

### Job Queue

```bash
# Producer: Add jobs
LPUSH jobs "process_order:1001"
LPUSH jobs "send_email:user@example.com"

# Consumer: Process jobs
RPOP jobs
```

### Real-time Notifications

```bash
# Subscriber: User notification service
SUBSCRIBE user:1001:notifications
PSUBSCRIBE user:*:alerts

# Publisher: Send notifications
PUBLISH user:1001:notifications "New message from Alice"
PUBLISH user:1001:alerts "Password changed"

# Multiple subscribers receive the same message
# Great for event broadcasting and real-time updates
```

## Command Conventions

### Keys
- Keys are binary safe (can contain any byte sequence)
- Recommended to use a naming scheme like `object-type:id:field`
- Examples: `user:1001`, `session:abc123`, `cache:article:5001`

### Return Values
Commands return different types of values:

- **Simple strings**: `OK`, `PONG`
- **Errors**: Error messages prefixed with `-ERR`
- **Integers**: Numbers prefixed with `:`
- **Bulk strings**: Binary-safe strings
- **Arrays**: Multiple values

### TTL (Time To Live)
Many commands support TTL options:

- `EX seconds` - Set expiry in seconds
- `PX milliseconds` - Set expiry in milliseconds
- `EXPIRE key seconds` - Set expiry on existing key
- `TTL key` - Get remaining time to live

## Performance Considerations

### Command Complexity

| Command | Time Complexity | Notes |
|---------|----------------|-------|
| GET     | O(1)           | Very fast |
| SET     | O(1)           | Very fast |
| DEL     | O(N)           | N = number of keys |
| KEYS    | O(N)           | Avoid in production |
| HGETALL | O(N)           | N = number of fields |
| LRANGE  | O(S+N)         | S = start offset, N = elements |
| SMEMBERS| O(N)           | N = set cardinality |
| ZRANGE  | O(log(N)+M)    | N = set size, M = results |

### Best Practices

1. **Avoid `KEYS` in production** - Use `SCAN` instead (when available)
2. **Use appropriate data types** - Hashes for objects, lists for queues
3. **Set TTLs** - Prevent memory bloat with automatic expiration
4. **Pipeline commands** - Reduce network round trips
5. **Use MGET/MSET** - Batch operations are more efficient

## Next Steps

- Explore each command category in detail
- Learn about [Configuration](../configuration.md)
- Understand [Performance](../benchmarks.md)
- Review [Use Cases](../use-cases.md)
