# Contributing to rLightning

Thank you for your interest in contributing to rLightning! This guide will help you get started.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/rLightning.git`
3. Create a feature branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Run tests: `cargo test`
6. Push and open a Pull Request

## Development Setup

```bash
# Build
cargo build

# Run all tests
cargo test

# Run clippy linter
cargo clippy -- -D warnings

# Check formatting
cargo fmt -- --check

# Run benchmarks
cargo bench
```

## Code Style

- Follow standard Rust conventions and idioms
- Run `cargo fmt` before committing
- Ensure `cargo clippy -- -D warnings` passes with no warnings
- Write tests for new functionality

## Pull Requests

- Keep PRs focused on a single change
- Include tests for new features or bug fixes
- Update documentation if your change affects public APIs or behavior
- Ensure all CI checks pass

## Reporting Bugs

Open an issue with:
- A clear description of the bug
- Steps to reproduce
- Expected vs actual behavior
- rLightning version and OS

## Feature Requests

Open an issue describing:
- The use case and motivation
- How the feature should work
- Any Redis compatibility considerations

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
