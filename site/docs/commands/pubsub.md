# Pub/Sub Commands

rLightning implements the complete Redis Pub/Sub messaging system, allowing applications to communicate through channels using a publish/subscribe pattern.

## Overview

Pub/Sub enables real-time messaging where publishers send messages to channels without knowing who (if anyone) will receive them, and subscribers receive messages from channels they're interested in.

### Key Features

- **Channel subscriptions** - Subscribe to specific channels by name
- **Pattern subscriptions** - Subscribe using glob-style patterns (`*`, `?`, `[abc]`)
- **Multiple subscribers** - Many clients can subscribe to the same channel
- **Non-blocking** - Publishers don't wait for subscribers
- **Introspection** - Query active channels and subscription counts

## Commands

### SUBSCRIBE

Subscribe to one or more channels.

**Syntax:**
```
SUBSCRIBE channel [channel ...]
```

**Return Value:**
For each channel, returns an array:
1. `"subscribe"` - Message type
2. Channel name
3. Total number of channels subscribed to

**Example:**
```bash
redis-cli> SUBSCRIBE news sports weather
1) "subscribe"
2) "news"
3) (integer) 1
1) "subscribe"
2) "sports"
3) (integer) 2
1) "subscribe"
2) "weather"
3) (integer) 3
```

**Notes:**
- Once a client subscribes, it enters "subscription mode"
- In subscription mode, only Pub/Sub commands are allowed
- To exit subscription mode, unsubscribe from all channels

---

### UNSUBSCRIBE

Unsubscribe from one or more channels.

**Syntax:**
```
UNSUBSCRIBE [channel [channel ...]]
```

**Return Value:**
For each channel, returns an array:
1. `"unsubscribe"` - Message type
2. Channel name
3. Remaining number of channels subscribed to

**Example:**
```bash
redis-cli> UNSUBSCRIBE news sports
1) "unsubscribe"
2) "news"
3) (integer) 1
1) "unsubscribe"
2) "sports"
3) (integer) 0
```

**Notes:**
- If no channels specified, unsubscribes from all channels
- Client exits subscription mode when all subscriptions are removed

---

### PSUBSCRIBE

Subscribe to one or more channel patterns.

**Syntax:**
```
PSUBSCRIBE pattern [pattern ...]
```

**Supported pattern syntax:**
- `*` - Matches any characters (e.g., `news:*` matches `news:sports`, `news:tech`)
- `?` - Matches exactly one character (e.g., `user:?` matches `user:1`, `user:a`)
- `[abc]` - Matches one character from set (e.g., `log:[123]` matches `log:1`, `log:2`, `log:3`)

**Return Value:**
For each pattern, returns an array:
1. `"psubscribe"` - Message type
2. Pattern
3. Total number of patterns subscribed to

**Example:**
```bash
redis-cli> PSUBSCRIBE news:* user:*:notifications
1) "psubscribe"
2) "news:*"
3) (integer) 1
1) "psubscribe"
2) "user:*:notifications"
3) (integer) 2
```

**Message Format:**
When a message is published to a matching channel, subscribers receive:
1. `"pmessage"` - Message type
2. Pattern that matched
3. Actual channel name
4. Message payload

```bash
1) "pmessage"
2) "news:*"
3) "news:sports"
4) "Goal scored!"
```

---

### PUNSUBSCRIBE

Unsubscribe from one or more patterns.

**Syntax:**
```
PUNSUBSCRIBE [pattern [pattern ...]]
```

**Return Value:**
For each pattern, returns an array:
1. `"punsubscribe"` - Message type
2. Pattern
3. Remaining number of patterns subscribed to

**Example:**
```bash
redis-cli> PUNSUBSCRIBE news:*
1) "punsubscribe"
2) "news:*"
3) (integer) 0
```

**Notes:**
- If no patterns specified, unsubscribes from all patterns
- Works independently from channel subscriptions

---

### PUBLISH

Publish a message to a channel.

**Syntax:**
```
PUBLISH channel message
```

**Return Value:**
Integer - the number of clients that received the message.

**Example:**
```bash
redis-cli> PUBLISH news:sports "Team wins championship!"
(integer) 3
```

**Notes:**
- Returns 0 if no subscribers are listening
- Messages are not stored - if no one is subscribed, the message is lost
- Both channel and pattern subscribers receive the message

---

### PUBSUB CHANNELS

List currently active channels (channels with at least one subscriber).

**Syntax:**
```
PUBSUB CHANNELS [pattern]
```

**Return Value:**
Array of active channel names.

**Example:**
```bash
redis-cli> PUBSUB CHANNELS
1) "news:sports"
2) "news:tech"
3) "weather:us"

redis-cli> PUBSUB CHANNELS news:*
1) "news:sports"
2) "news:tech"
```

---

### PUBSUB NUMSUB

Get the number of subscribers for specific channels.

**Syntax:**
```
PUBSUB NUMSUB [channel [channel ...]]
```

**Return Value:**
Array of channel names and their subscriber counts (interleaved).

**Example:**
```bash
redis-cli> PUBSUB NUMSUB news:sports news:tech unknown
1) "news:sports"
2) (integer) 5
3) "news:tech"
4) (integer) 3
5) "unknown"
6) (integer) 0
```

---

### PUBSUB NUMPAT

Get the number of active pattern subscriptions.

**Syntax:**
```
PUBSUB NUMPAT
```

**Return Value:**
Integer - total count of pattern subscriptions across all clients.

**Example:**
```bash
redis-cli> PUBSUB NUMPAT
(integer) 7
```

**Notes:**
- Counts pattern subscriptions, not unique patterns
- If 3 clients subscribe to `news:*`, the count is 3

---

## Usage Patterns

### Real-time Notifications

```bash
# Subscriber (user notification service)
SUBSCRIBE user:1001:notifications

# Publisher (various services)
PUBLISH user:1001:notifications "New message from Alice"
PUBLISH user:1001:notifications "Order #5001 shipped"
```

### Event Broadcasting

```bash
# Multiple subscribers for same event
# Subscriber 1: Analytics Service
SUBSCRIBE order:created

# Subscriber 2: Email Service
SUBSCRIBE order:created

# Subscriber 3: Inventory Service
SUBSCRIBE order:created

# Publisher: Order Service
PUBLISH order:created '{"order_id": 1001, "user_id": 5001}'
# All three subscribers receive the event
```

### Topic Filtering

```bash
# Subscribe to all log levels
PSUBSCRIBE log:*

# Or subscribe to specific levels only
SUBSCRIBE log:error log:warning

# Publishers send to appropriate channels
PUBLISH log:info "Server started"
PUBLISH log:error "Database connection failed"
PUBLISH log:debug "Query took 150ms"
```

### Multi-tenant Architecture

```bash
# Subscribe to all events for a specific tenant
PSUBSCRIBE tenant:acme:*

# Publishers organize by tenant
PUBLISH tenant:acme:user:created '{"user": "alice"}'
PUBLISH tenant:acme:order:placed '{"order": 1001}'
PUBLISH tenant:globex:user:created '{"user": "bob"}'
# Only tenant:acme:* pattern receives first two messages
```

---

## Performance Characteristics

| Command | Time Complexity | Notes |
|---------|----------------|-------|
| SUBSCRIBE | O(N) | N = number of channels |
| UNSUBSCRIBE | O(N) | N = number of channels |
| PSUBSCRIBE | O(N) | N = number of patterns |
| PUNSUBSCRIBE | O(N) | N = number of patterns |
| PUBLISH | O(N+M) | N = subscribers, M = pattern subscribers |
| PUBSUB CHANNELS | O(N) | N = active channels |
| PUBSUB NUMSUB | O(N) | N = requested channels |
| PUBSUB NUMPAT | O(1) | Constant time |

---

## Best Practices

### 1. Message Format

Use structured data formats like JSON for complex messages:

```bash
PUBLISH order:events '{"type":"created","order_id":1001,"timestamp":"2024-01-15T10:30:00Z"}'
```

### 2. Channel Naming

Use hierarchical naming with colons for organization:

```bash
# Good
SUBSCRIBE system:alerts:critical
SUBSCRIBE user:1001:messages
PSUBSCRIBE app:metrics:*

# Avoid
SUBSCRIBE alerts
SUBSCRIBE user_messages_1001
```

### 3. Error Handling

Subscribers should handle connection losses and resubscribe:

```python
while True:
    try:
        pubsub = redis.pubsub()
        pubsub.subscribe('notifications')
        for message in pubsub.listen():
            process(message)
    except ConnectionError:
        time.sleep(5)  # Retry after delay
```

### 4. Message Acknowledgment

Remember: Pub/Sub is fire-and-forget. For guaranteed delivery, use:
- Lists with `LPUSH`/`BRPOP` for job queues
- Streams (when available) for persistent messaging
- External message queues (RabbitMQ, Kafka) for critical messages

### 5. Pattern Efficiency

Be specific with patterns to reduce unnecessary matching:

```bash
# Efficient
PSUBSCRIBE user:1001:*

# Less efficient (matches too broadly)
PSUBSCRIBE user:*
```

---

## Subscription Mode

When a client issues SUBSCRIBE or PSUBSCRIBE, it enters "subscription mode":

**Allowed commands in subscription mode:**
- `SUBSCRIBE`
- `UNSUBSCRIBE`
- `PSUBSCRIBE`
- `PUNSUBSCRIBE`
- `PING`
- `QUIT`

**Not allowed:**
- All other commands return an error

**Exiting subscription mode:**
Unsubscribe from all channels and patterns:

```bash
UNSUBSCRIBE
PUNSUBSCRIBE
```

---

## Message Format Reference

### Subscribe Confirmation

```
1) "subscribe"      # Message type
2) "channel_name"   # Channel subscribed to
3) (integer) 1      # Total subscription count
```

### Channel Message

```
1) "message"        # Message type
2) "channel_name"   # Channel message was published to
3) "payload"        # Message content
```

### Pattern Subscribe Confirmation

```
1) "psubscribe"     # Message type
2) "pattern"        # Pattern subscribed to
3) (integer) 1      # Total pattern subscription count
```

### Pattern Message

```
1) "pmessage"       # Message type
2) "pattern"        # Pattern that matched
3) "channel_name"   # Actual channel name
4) "payload"        # Message content
```

### Unsubscribe Confirmation

```
1) "unsubscribe"    # Message type
2) "channel_name"   # Channel unsubscribed from
3) (integer) 0      # Remaining subscription count
```

---

## Code Examples

### Python (redis-py)

```python
import redis

# Subscriber
r = redis.Redis(host='localhost', port=6379)
pubsub = r.pubsub()

# Subscribe to channels
pubsub.subscribe('news', 'sports')
pubsub.psubscribe('user:*:notifications')

# Listen for messages
for message in pubsub.listen():
    if message['type'] == 'message':
        print(f"Channel: {message['channel']}")
        print(f"Data: {message['data']}")
    elif message['type'] == 'pmessage':
        print(f"Pattern: {message['pattern']}")
        print(f"Channel: {message['channel']}")
        print(f"Data: {message['data']}")

# Publisher
r.publish('news', 'Breaking: New feature released!')
r.publish('user:1001:notifications', 'You have a new message')
```

### Node.js (ioredis)

```javascript
const Redis = require('ioredis');

// Subscriber
const subscriber = new Redis({ host: 'localhost', port: 6379 });

subscriber.subscribe('news', 'sports', (err, count) => {
  console.log(`Subscribed to ${count} channels`);
});

subscriber.on('message', (channel, message) => {
  console.log(`${channel}: ${message}`);
});

// Pattern subscriber
subscriber.psubscribe('user:*', (err, count) => {
  console.log(`Subscribed to ${count} patterns`);
});

subscriber.on('pmessage', (pattern, channel, message) => {
  console.log(`${pattern} -> ${channel}: ${message}`);
});

// Publisher
const publisher = new Redis({ host: 'localhost', port: 6379 });
publisher.publish('news', 'Hello, World!');
```

### Rust (redis-rs)

```rust
use redis::{Client, Commands, PubSubCommands};

fn main() -> redis::RedisResult<()> {
    let client = Client::open("redis://localhost:6379")?;

    // Publisher
    let mut publisher = client.get_connection()?;
    let receivers: i32 = publisher.publish("news", "Hello from Rust!")?;
    println!("Message delivered to {} subscribers", receivers);

    // Subscriber
    let mut pubsub = client.get_connection()?.into_pubsub();
    pubsub.subscribe("news")?;
    pubsub.psubscribe("user:*")?;

    loop {
        let msg = pubsub.get_message()?;
        let payload: String = msg.get_payload()?;
        println!("Channel: {:?}, Message: {}", msg.get_channel_name(), payload);
    }
}
```

---

## Comparison with Redis

rLightning's Pub/Sub implementation is **fully compatible** with Redis:

| Feature | Redis | rLightning | Notes |
|---------|-------|------------|-------|
| SUBSCRIBE | ✅ | ✅ | Full support |
| UNSUBSCRIBE | ✅ | ✅ | Full support |
| PSUBSCRIBE | ✅ | ✅ | Full glob pattern support |
| PUNSUBSCRIBE | ✅ | ✅ | Full support |
| PUBLISH | ✅ | ✅ | Full support |
| PUBSUB CHANNELS | ✅ | ✅ | Full support |
| PUBSUB NUMSUB | ✅ | ✅ | Full support |
| PUBSUB NUMPAT | ✅ | ✅ | Full support |
| Pattern matching | ✅ | ✅ | `*`, `?`, `[abc]` supported |
| Subscription mode | ✅ | ✅ | Same restrictions |

**Not yet supported:**
- `PUBSUB SHARDCHANNELS` (Redis 7.0+ cluster feature)
- `PUBSUB SHARDNUMSUB` (Redis 7.0+ cluster feature)
- Sharded Pub/Sub (Redis 7.0+)

---

## Troubleshooting

### Messages not received

1. **Check subscription status:**
   ```bash
   PUBSUB NUMSUB your:channel
   ```

2. **Verify pattern matching:**
   ```bash
   PUBSUB CHANNELS your:*
   ```

3. **Ensure publisher uses exact channel name:**
   ```bash
   # Won't work if subscribed to "news"
   PUBLISH News "message"  # Case sensitive!
   ```

### High memory usage

- Pub/Sub messages are not stored in memory
- Check for subscriptions that are no longer needed
- Use `PUBSUB NUMPAT` and `PUBSUB CHANNELS` to audit

### Performance issues

- Pattern subscriptions require matching against all published channels
- Prefer specific channel subscriptions when possible
- Monitor with `PUBSUB NUMPAT` - too many patterns can slow publishing

---

## Next Steps

- Learn about [Server Commands](server.md)
- Explore [Use Cases](../use-cases.md)
- Read about [Architecture](../architecture.md)
- Check [Configuration](../configuration.md) for Pub/Sub tuning options
