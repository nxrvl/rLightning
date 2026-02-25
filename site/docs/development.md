# Contributing

## Getting Started

```bash
git clone https://github.com/altista-tech/rLightning.git
cd rLightning
cargo build
cargo test
```

## Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Write tests for your changes
4. Implement the feature
5. Run `cargo test` and `cargo clippy`
6. Commit and push
7. Open a Pull Request

## Code Style

- Follow Rust conventions (rustfmt)
- Run `cargo fmt` before committing
- Run `cargo clippy -- -D warnings` for linting
- Write tests for all new functionality

## Project Structure

```
src/
  main.rs             # Entry point
  networking/          # RESP protocol, TCP server
  command/             # Command handlers
  storage/             # Storage engine
  persistence/         # RDB/AOF
  replication/         # Master-replica sync
  security/            # Authentication, ACL
  pubsub/              # Pub/Sub messaging
  scripting/           # Lua scripting
  cluster/             # Cluster mode
  sentinel/            # Sentinel HA
```
