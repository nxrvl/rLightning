# Building

## Prerequisites

- Rust toolchain (1.75+ recommended)
- Cargo package manager

## Build Commands

```bash
# Development build
cargo build

# Release build (optimized)
cargo build --release

# Check without building
cargo check

# Build documentation
cargo doc --open
```

## Release Binary

The release binary is located at `target/release/rlightning` after building with `--release`.

## Docker Build

```bash
docker build -t rlightning:latest .
```

## Cross-Compilation

```bash
# Linux (from macOS)
cargo build --release --target x86_64-unknown-linux-gnu

# Windows
cargo build --release --target x86_64-pc-windows-msvc
```
