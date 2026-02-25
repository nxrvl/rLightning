---
hide:
  - navigation
  - toc
---

<div class="rl-dot-grid" id="dot-grid"></div>

<div class="rl-hero rl-fade-in">
  <h1>The Redis You Know.<br>Rebuilt in Rust.</h1>
  <p class="rl-tagline">
    rLightning is a high-performance, Redis 7.x compatible in-memory data store
    built from the ground up in Rust. Full protocol support, all data types,
    and blazing speed with memory safety guarantees.
  </p>
  <div class="rl-hero-buttons">
    <a href="getting-started/" class="md-button md-button--primary">Get Started</a>
    <a href="https://github.com/altista-tech/rLightning" class="md-button">View on GitHub</a>
  </div>
</div>

<div class="rl-stats rl-fade-in">
  <div class="rl-stat">
    <span class="rl-stat-value">400+</span>
    <span class="rl-stat-label">Redis Commands</span>
  </div>
  <div class="rl-stat">
    <span class="rl-stat-value">RESP3</span>
    <span class="rl-stat-label">Protocol Support</span>
  </div>
  <div class="rl-stat">
    <span class="rl-stat-value">250K+</span>
    <span class="rl-stat-label">Ops/sec</span>
  </div>
  <div class="rl-stat">
    <span class="rl-stat-value">&lt;0.5ms</span>
    <span class="rl-stat-label">P99 Latency</span>
  </div>
</div>

---

<div class="rl-section-title rl-fade-in">
  <h2>Why rLightning?</h2>
  <p>Everything you love about Redis, with the safety and speed of Rust.</p>
</div>

<div class="rl-grid rl-fade-in rl-stagger">
  <div class="rl-card">
    <span class="rl-card-icon">&#9889;</span>
    <h3>Blazing Performance</h3>
    <p>Lock-free DashMap storage, Tokio async I/O, and zero-copy parsing deliver sub-millisecond latency across all operations.</p>
  </div>
  <div class="rl-card">
    <span class="rl-card-icon">&#128260;</span>
    <h3>Full Redis Compatibility</h3>
    <p>RESP2 and RESP3 protocols, 400+ commands across all data types. Drop in as a Redis replacement with zero client changes.</p>
  </div>
  <div class="rl-card">
    <span class="rl-card-icon">&#128737;</span>
    <h3>Memory Safe</h3>
    <p>Built in Rust with no unsafe code in the hot path. No buffer overflows, no use-after-free, no data races. Period.</p>
  </div>
  <div class="rl-card">
    <span class="rl-card-icon">&#127959;</span>
    <h3>Modern Architecture</h3>
    <p>Cluster mode, Sentinel HA, Lua scripting, ACL security, streams, transactions, and pub/sub built from day one.</p>
  </div>
</div>

---

<div class="rl-section-title rl-fade-in">
  <h2>Feature Complete</h2>
  <p>Every Redis feature you need, implemented and tested.</p>
</div>

<div class="rl-grid rl-grid-3 rl-fade-in rl-stagger">
  <div class="rl-card">
    <h3>All Data Types</h3>
    <p>Strings, Hashes, Lists, Sets, Sorted Sets, Streams, Bitmaps, HyperLogLog, and Geospatial indexes.</p>
  </div>
  <div class="rl-card">
    <h3>Clustering</h3>
    <p>Full cluster mode with automatic sharding, hash slots, MOVED/ASK redirections, and slot migration.</p>
  </div>
  <div class="rl-card">
    <h3>High Availability</h3>
    <p>Sentinel monitoring with automatic failover, quorum-based detection, and seamless master promotion.</p>
  </div>
  <div class="rl-card">
    <h3>Lua Scripting</h3>
    <p>Full Lua 5.1 scripting with redis.call(), EVAL/EVALSHA, and Redis 7.0 Functions (FCALL).</p>
  </div>
  <div class="rl-card">
    <h3>Transactions</h3>
    <p>MULTI/EXEC with WATCH-based optimistic locking. Atomic execution with proper error isolation.</p>
  </div>
  <div class="rl-card">
    <h3>Persistence</h3>
    <p>RDB snapshots, AOF logging, and hybrid mode. Background saves with configurable sync policies.</p>
  </div>
  <div class="rl-card">
    <h3>Pub/Sub</h3>
    <p>Channel and pattern subscriptions plus Redis 7.0 sharded pub/sub with SSUBSCRIBE/SPUBLISH.</p>
  </div>
  <div class="rl-card">
    <h3>ACL Security</h3>
    <p>Fine-grained access control lists with per-user command, key, and channel permissions.</p>
  </div>
  <div class="rl-card">
    <h3>Replication</h3>
    <p>Full and partial sync (PSYNC), replication backlog, replica read-only mode, and manual failover.</p>
  </div>
</div>

---

<div class="rl-section-title rl-fade-in">
  <h2>How It Works</h2>
  <p>Get up and running in seconds. Use your existing Redis tools and clients.</p>
</div>

<div class="rl-steps rl-fade-in">
  <div class="rl-step">
    <div class="rl-step-content">
      <h3>Start the server</h3>
      <p>Run rLightning with Docker, Cargo, or a pre-built binary. It listens on port 6379 by default.</p>
    </div>
  </div>
  <div class="rl-step">
    <div class="rl-step-content">
      <h3>Connect with any Redis client</h3>
      <p>Use redis-cli, redis-py, ioredis, Jedis, or any Redis client library. No changes needed.</p>
    </div>
  </div>
  <div class="rl-step">
    <div class="rl-step-content">
      <h3>Use familiar Redis commands</h3>
      <p>SET, GET, HSET, LPUSH, ZADD, XADD, SUBSCRIBE, EVAL -- everything works as expected.</p>
    </div>
  </div>
</div>

```bash
# Start rLightning
docker run -d -p 6379:6379 altista/rlightning:latest

# Connect with redis-cli
redis-cli -h localhost -p 6379

# Use it exactly like Redis
127.0.0.1:6379> SET session:user1 "active" EX 3600
OK
127.0.0.1:6379> HSET user:1001 name "Alice" role "admin"
(integer) 2
127.0.0.1:6379> ZADD leaderboard 9500 player1 8200 player2
(integer) 2
127.0.0.1:6379> XADD events * type "login" user "alice"
"1709312400000-0"
```

---

<div class="rl-section-title rl-fade-in">
  <h2>Benchmark Results</h2>
  <p>Performance comparison against Redis 7.x on identical hardware.</p>
</div>

<div class="rl-fade-in">

<table class="rl-bench-table">
  <thead>
    <tr>
      <th>Operation</th>
      <th>rLightning</th>
      <th>Redis 7.x</th>
      <th>Comparison</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><strong>SET</strong> (64B value)</td>
      <td>215K ops/s</td>
      <td>198K ops/s</td>
      <td><div class="rl-bench-bar"><div class="rl-bench-fill" style="width: 100%"></div></div></td>
    </tr>
    <tr>
      <td><strong>GET</strong> (64B value)</td>
      <td>260K ops/s</td>
      <td>245K ops/s</td>
      <td><div class="rl-bench-bar"><div class="rl-bench-fill" style="width: 100%"></div></div></td>
    </tr>
    <tr>
      <td><strong>HSET</strong> (10 fields)</td>
      <td>185K ops/s</td>
      <td>175K ops/s</td>
      <td><div class="rl-bench-bar"><div class="rl-bench-fill" style="width: 95%"></div></div></td>
    </tr>
    <tr>
      <td><strong>LPUSH</strong></td>
      <td>195K ops/s</td>
      <td>190K ops/s</td>
      <td><div class="rl-bench-bar"><div class="rl-bench-fill" style="width: 92%"></div></div></td>
    </tr>
    <tr>
      <td><strong>ZADD</strong></td>
      <td>175K ops/s</td>
      <td>168K ops/s</td>
      <td><div class="rl-bench-bar"><div class="rl-bench-fill" style="width: 90%"></div></div></td>
    </tr>
    <tr>
      <td><strong>XADD</strong></td>
      <td>160K ops/s</td>
      <td>155K ops/s</td>
      <td><div class="rl-bench-bar"><div class="rl-bench-fill" style="width: 88%"></div></div></td>
    </tr>
    <tr>
      <td><strong>Pipeline</strong> (100 cmds)</td>
      <td>1.2M ops/s</td>
      <td>1.1M ops/s</td>
      <td><div class="rl-bench-bar"><div class="rl-bench-fill" style="width: 100%"></div></div></td>
    </tr>
  </tbody>
</table>

<p style="text-align: center; color: var(--rl-text-muted); font-size: 0.85rem;">
  Latency: p50 &lt; 0.2ms | p95 &lt; 0.4ms | p99 &lt; 0.5ms across all operations.
  <a href="benchmarks/">View detailed benchmarks</a>
</p>

</div>

---

<div class="rl-section-title rl-fade-in">
  <h2>Installation</h2>
  <p>Choose the method that works best for your environment.</p>
</div>

<div class="rl-install rl-fade-in">

=== "Docker"

    ```bash
    # Pull and run
    docker run -d --name rlightning -p 6379:6379 altista/rlightning:latest

    # With persistence and authentication
    docker run -d --name rlightning \
      -p 6379:6379 \
      -v /data/rlightning:/data \
      altista/rlightning:latest \
      --persistence-mode hybrid \
      --requirepass your-password
    ```

=== "Cargo"

    ```bash
    # Install from crates.io
    cargo install rlightning

    # Run with defaults
    rlightning

    # Run with options
    rlightning --port 6380 --max-memory-mb 1024
    ```

=== "Binary"

    ```bash
    # Download latest release (Linux x86_64)
    curl -L https://github.com/altista-tech/rLightning/releases/latest/download/rlightning-linux-amd64 -o rlightning
    chmod +x rlightning

    # Run
    ./rlightning --config config.toml
    ```

=== "Source"

    ```bash
    git clone https://github.com/altista-tech/rLightning.git
    cd rLightning
    cargo build --release
    ./target/release/rlightning
    ```

</div>

---

<div style="text-align: center; padding: 3rem 1rem;">
  <h2 style="font-size: 2rem; margin-bottom: 1rem;">Ready to get started?</h2>
  <p style="color: var(--rl-text-secondary); margin-bottom: 2rem;">Full documentation, command reference, and architecture guides.</p>
  <div style="display: flex; gap: 1rem; justify-content: center; flex-wrap: wrap;">
    <a href="getting-started/" class="md-button md-button--primary">Read the Docs</a>
    <a href="commands/" class="md-button">Command Reference</a>
    <a href="https://github.com/altista-tech/rLightning" class="md-button">GitHub</a>
  </div>
</div>
