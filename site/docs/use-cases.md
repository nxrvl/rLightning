# Use Cases

rLightning excels in scenarios requiring fast, reliable in-memory data access. Here are common use cases and implementation patterns.

## Session Management

### Web Application Sessions

Store user session data with automatic expiration:

```python
import redis
from datetime import timedelta

r = redis.Redis(host='localhost', port=6379)

def create_session(user_id, session_data):
    """Create a new session with 1-hour TTL"""
    session_id = generate_session_id()
    session_key = f"session:{session_id}"
    
    # Store session data as hash
    r.hset(session_key, mapping={
        'user_id': user_id,
        'created_at': time.time(),
        **session_data
    })
    
    # Auto-expire after 1 hour
    r.expire(session_key, timedelta(hours=1))
    
    return session_id

def get_session(session_id):
    """Retrieve session data"""
    session_key = f"session:{session_id}"
    return r.hgetall(session_key)

def extend_session(session_id):
    """Extend session TTL"""
    session_key = f"session:{session_id}"
    r.expire(session_key, timedelta(hours=1))

def delete_session(session_id):
    """Logout - delete session"""
    session_key = f"session:{session_id}"
    r.delete(session_key)
```

### Benefits
- Automatic expiration prevents memory leaks
- Fast session lookup (O(1))
- No database load for session storage
- Shared across multiple app servers

## Caching Layer

### Database Query Caching

Reduce database load with intelligent caching:

```javascript
const redis = require('redis');
const client = redis.createClient();

async function getUserById(userId) {
  const cacheKey = `cache:user:${userId}`;
  
  // Try cache first
  let user = await client.get(cacheKey);
  
  if (user) {
    console.log('Cache hit!');
    return JSON.parse(user);
  }
  
  console.log('Cache miss - querying database');
  // Query database
  user = await database.query('SELECT * FROM users WHERE id = ?', [userId]);
  
  // Cache for 5 minutes
  await client.setEx(cacheKey, 300, JSON.stringify(user));
  
  return user;
}

async function invalidateUserCache(userId) {
  const cacheKey = `cache:user:${userId}`;
  await client.del(cacheKey);
}
```

### Page Fragment Caching

```python
def get_cached_fragment(fragment_key, ttl=300):
    """Get or generate HTML fragment"""
    cache_key = f"fragment:{fragment_key}"
    
    # Try cache
    cached = r.get(cache_key)
    if cached:
        return cached
    
    # Generate and cache
    html = generate_fragment()
    r.setex(cache_key, ttl, html)
    return html
```

## Rate Limiting

### API Rate Limiting

Protect APIs from abuse with sliding window rate limiting:

```go
func checkRateLimit(userID string, limit int, window time.Duration) (bool, error) {
    ctx := context.Background()
    key := fmt.Sprintf("ratelimit:%s:%d", userID, time.Now().Unix()/int64(window.Seconds()))
    
    // Increment counter
    count, err := rdb.Incr(ctx, key).Result()
    if err != nil {
        return false, err
    }
    
    // Set expiry on first request
    if count == 1 {
        rdb.Expire(ctx, key, window)
    }
    
    // Check if over limit
    return count <= int64(limit), nil
}

// Usage
func apiHandler(w http.ResponseWriter, r *http.Request) {
    userID := getUserID(r)
    
    // Allow 100 requests per minute
    allowed, _ := checkRateLimit(userID, 100, time.Minute)
    
    if !allowed {
        http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)
        return
    }
    
    // Process request
    handleRequest(w, r)
}
```

### Distributed Rate Limiting

```python
def distributed_rate_limit(resource_id, max_requests, window_seconds):
    """Rate limit across multiple servers"""
    key = f"ratelimit:{resource_id}"
    pipe = r.pipeline()
    
    # Atomic increment and expire
    pipe.incr(key)
    pipe.expire(key, window_seconds)
    results = pipe.execute()
    
    current_count = results[0]
    return current_count <= max_requests
```

## Real-Time Analytics

### Page View Counters

```bash
# Increment page views
INCR pageviews:article:1001

# Get total views
GET pageviews:article:1001

# Daily counters
INCR pageviews:article:1001:2024-01-15

# Set daily expiry
EXPIRE pageviews:article:1001:2024-01-15 604800  # 7 days
```

### Active User Tracking

```python
def track_active_user(user_id):
    """Track active users in last 5 minutes"""
    key = "active:users"
    timestamp = time.time()
    
    # Add user with timestamp score
    r.zadd(key, {user_id: timestamp})
    
    # Remove users inactive for > 5 minutes
    cutoff = timestamp - 300
    r.zremrangebyscore(key, '-inf', cutoff)

def get_active_user_count():
    """Get count of active users"""
    return r.zcard("active:users")

def get_active_users():
    """Get list of active users"""
    return r.zrange("active:users", 0, -1)
```

## Leaderboards

### Gaming Leaderboard

```javascript
// Add player score
async function addScore(playerId, score) {
  await client.zAdd('leaderboard', { score, value: playerId });
}

// Get top 10 players
async function getTopPlayers(count = 10) {
  return await client.zRangeWithScores('leaderboard', 0, count - 1, {
    REV: true
  });
}

// Get player rank
async function getPlayerRank(playerId) {
  return await client.zRevRank('leaderboard', playerId);
}

// Get player score
async function getPlayerScore(playerId) {
  return await client.zScore('leaderboard', playerId);
}

// Increment player score
async function incrementScore(playerId, increment) {
  return await client.zIncrBy('leaderboard', increment, playerId);
}
```

### Time-Based Leaderboard

```python
def add_daily_score(user_id, score):
    """Add score to daily leaderboard"""
    date = datetime.now().strftime('%Y-%m-%d')
    key = f"leaderboard:daily:{date}"
    
    r.zadd(key, {user_id: score})
    # Expire after 30 days
    r.expire(key, 2592000)

def get_daily_top(date, count=10):
    """Get top scores for a specific day"""
    key = f"leaderboard:daily:{date}"
    return r.zrevrange(key, 0, count-1, withscores=True)
```

## Job Queues

### Task Queue Implementation

```ruby
require 'redis'

class JobQueue
  def initialize
    @redis = Redis.new
    @queue_key = 'jobs:pending'
  end
  
  # Producer: Add job to queue
  def enqueue(job_data)
    @redis.lpush(@queue_key, job_data.to_json)
  end
  
  # Consumer: Process jobs
  def process_jobs
    loop do
      # Block until job available
      _, job_json = @redis.brpop(@queue_key, timeout: 5)
      
      if job_json
        job = JSON.parse(job_json)
        process_job(job)
      end
    end
  end
  
  # Priority queue
  def enqueue_priority(job_data, priority)
    key = "jobs:priority"
    @redis.zadd(key, priority, job_data.to_json)
  end
  
  def dequeue_priority
    results = @redis.zpopmin("jobs:priority")
    JSON.parse(results[0]) if results
  end
end
```

## Distributed Locking

### Simple Distributed Lock

```go
func acquireLock(lockKey string, timeout time.Duration) (bool, error) {
    ctx := context.Background()
    lockValue := uuid.New().String()
    
    // Try to acquire lock
    success, err := rdb.SetNX(ctx, lockKey, lockValue, timeout).Result()
    if err != nil {
        return false, err
    }
    
    return success, nil
}

func releaseLock(lockKey string) error {
    ctx := context.Background()
    return rdb.Del(ctx, lockKey).Err()
}

// Usage
func criticalSection() {
    lockKey := "lock:resource:123"
    
    if acquired, _ := acquireLock(lockKey, 10*time.Second); acquired {
        defer releaseLock(lockKey)
        
        // Critical section code
        performCriticalOperation()
    } else {
        // Could not acquire lock
        log.Println("Resource is locked")
    }
}
```

## Shopping Cart

### E-commerce Cart Management

```python
class ShoppingCart:
    def __init__(self, user_id):
        self.user_id = user_id
        self.cart_key = f"cart:{user_id}"
        self.ttl = 86400  # 24 hours
    
    def add_item(self, product_id, quantity=1):
        """Add item to cart"""
        r.hincrby(self.cart_key, product_id, quantity)
        r.expire(self.cart_key, self.ttl)
    
    def remove_item(self, product_id):
        """Remove item from cart"""
        r.hdel(self.cart_key, product_id)
    
    def update_quantity(self, product_id, quantity):
        """Update item quantity"""
        if quantity > 0:
            r.hset(self.cart_key, product_id, quantity)
        else:
            r.hdel(self.cart_key, product_id)
        r.expire(self.cart_key, self.ttl)
    
    def get_cart(self):
        """Get all cart items"""
        items = r.hgetall(self.cart_key)
        return {k: int(v) for k, v in items.items()}
    
    def clear_cart(self):
        """Empty cart"""
        r.delete(self.cart_key)
    
    def get_item_count(self):
        """Get total number of items"""
        items = r.hvals(self.cart_key)
        return sum(int(qty) for qty in items)
```

## Temporary Data Storage

### OTP (One-Time Password) Storage

```javascript
// Generate and store OTP
async function generateOTP(userId) {
  const otp = Math.floor(100000 + Math.random() * 900000);
  const key = `otp:${userId}`;
  
  // Store for 5 minutes
  await client.setEx(key, 300, otp.toString());
  
  return otp;
}

// Verify OTP
async function verifyOTP(userId, enteredOTP) {
  const key = `otp:${userId}`;
  const storedOTP = await client.get(key);
  
  if (storedOTP === enteredOTP.toString()) {
    // Delete after successful verification
    await client.del(key);
    return true;
  }
  
  return false;
}
```

### Email Verification Tokens

```python
def create_verification_token(email):
    """Create email verification token"""
    token = secrets.token_urlsafe(32)
    key = f"verify:{token}"
    
    # Store email with 24-hour expiry
    r.setex(key, 86400, email)
    
    return token

def verify_email_token(token):
    """Verify and consume token"""
    key = f"verify:{token}"
    email = r.get(key)
    
    if email:
        # Delete token after use
        r.delete(key)
        return email
    
    return None
```

## Performance Tips

### Connection Pooling

Always use connection pools for better performance:

```python
# Good: Use connection pool
pool = redis.ConnectionPool(host='localhost', port=6379, max_connections=10)
r = redis.Redis(connection_pool=pool)

# Bad: New connection every time
# r = redis.Redis(host='localhost', port=6379)  # Don't do this in loops!
```

### Pipelining

Batch commands for better throughput:

```python
# Without pipeline: Multiple round trips
r.set('key1', 'value1')
r.set('key2', 'value2')
r.set('key3', 'value3')

# With pipeline: Single round trip
pipe = r.pipeline()
pipe.set('key1', 'value1')
pipe.set('key2', 'value2')
pipe.set('key3', 'value3')
pipe.execute()
```

### Appropriate Data Types

Choose the right data type for your use case:

- **Strings**: Simple key-value, counters
- **Hashes**: Objects with fields
- **Lists**: Queues, timelines
- **Sets**: Unique items, tags
- **Sorted Sets**: Rankings, time-series

## Next Steps

- [Command Reference](commands/index.md)
- [Performance Benchmarks](benchmarks.md)
- [Configuration Guide](configuration.md)
