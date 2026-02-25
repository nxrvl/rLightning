# Testing

## Running Tests

```bash
# All tests
cargo test

# Unit tests only
cargo test --lib

# Integration tests
cargo test --test integration_test

# Redis compatibility tests (requires Docker)
cargo test --test redis_compatibility_test

# Acceptance tests
cargo test --test acceptance_test

# With verbose output
cargo test -- --nocapture
```

## Test Categories

| Suite | Command | Description |
|-------|---------|-------------|
| Unit tests | `cargo test --lib` | Internal logic tests |
| Integration tests | `cargo test --test integration_test` | End-to-end command testing |
| Redis compatibility | `cargo test --test redis_compatibility_test` | Side-by-side Redis comparison |
| Acceptance tests | `cargo test --test acceptance_test` | Real-world scenario testing |
| Cluster tests | `cargo test --test redis_cluster_test` | Multi-node cluster testing |
| Sentinel tests | `cargo test --test redis_sentinel_test` | HA failover testing |

## Benchmarks

```bash
cargo bench
cargo bench --bench storage_bench
cargo bench --bench throughput_bench
```

## Writing Tests

- Every new command must have unit tests and integration tests
- No `#[ignore]` attributes allowed
- Test edge cases: empty keys, binary data, large values, Unicode
- Use Docker dev containers for compatibility testing
