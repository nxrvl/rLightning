# Changelog

All notable changes to rLightning will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.4] - 2025-12-14

### Fixed

- **Critical**: Fixed RESP protocol buffer corruption bug that caused SET operations to fail with values larger than ~9-20KB
  - Root cause: `parse_bulk_string()` was modifying the buffer before verifying all data was available
  - When large commands arrived split across multiple TCP packets, the parser would consume the length header but leave the buffer starting with value data
  - This caused subsequent parsing to fail with "Invalid RESP command format, starts with: <value_data>"
  - Fix ensures buffer is only modified after verifying complete command is available
- **Fixed**: Type mismatch errors on GET operations after failed large SET commands (side effect of buffer corruption)
- **Fixed**: Base64/JSON encoded data now handled correctly regardless of size

### Changed

- Improved RESP parser robustness for handling chunked TCP reads
- Enhanced buffer management to prevent corruption with incomplete commands

### Added

- Comprehensive test suite for large values (up to 512MB)
- Tests for exact boundary conditions (9004/9005 byte limit)
- Tests for chunked TCP packet handling
- Tests for base64-encoded data and JSON payloads

### Performance

- No performance impact - fix only affects correctness of buffer handling

### Compatibility

- Now 100% Redis-compatible for large values (up to 512MB standard limit)
- All existing functionality preserved

## [1.0.3] - 2025-10-10

### Fixed

- Fix base64 data protocol handling
- Prevent infinite error loops and add B64 data acceptance tests
- Clear buffers after protocol errors and improve error messaging

## [1.0.2] - 2025-10-09

### Fixed

- Fix protocol error handling to avoid infinite loops
- Handle malformed RESP data by clearing buffer and breaking connection

## [1.0.1] - 2025-10-05

### Added

- Initial release with Redis compatibility
- Support for strings, hashes, lists, sets, sorted sets
- TTL and expiration support
- RDB and AOF persistence
- Master-replica replication
- Authentication support
- Configurable eviction policies

[1.0.4]: https://github.com/nxrvl/rLightning/compare/v1.0.3...v1.0.4
[1.0.3]: https://github.com/nxrvl/rLightning/compare/v1.0.2...v1.0.3
[1.0.2]: https://github.com/nxrvl/rLightning/compare/v1.0.1...v1.0.2
[1.0.1]: https://github.com/nxrvl/rLightning/releases/tag/v1.0.1
