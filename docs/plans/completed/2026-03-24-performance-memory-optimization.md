# Performance & Memory Optimization

## Overview

Fix memory leaks, eliminate O(n) memory tracking in hot paths, and reduce write-path overhead. Based on verified analysis in docs/OPTIMIZATION_PLAN.md against benchmark results showing 20-100x slowdown on list ops and 3-10x on writes vs Redis 7.

## Context

- Files involved: src/storage/engine.rs, src/storage/item.rs, src/storage/value.rs, src/networking/server.rs, src/command/types/{list,set,sorted_set,stream,bitmap}.rs
- Related patterns: atomic_modify closure pattern, sharded storage with per-shard RwLock, ModifyResult enum for mutation signaling
- Dependencies: None external. All changes are internal to the storage and networking layers.

## Development Approach

- **Testing approach**: Regular (code first, then tests) - existing 2609 tests provide strong regression coverage
- Complete each task fully before moving to the next
- Each task must pass full cargo test before proceeding
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**

## Implementation Steps

### Task 1: Fix Memory Leaks

**Files:**
- Modify: `src/storage/engine.rs` (key_versions cleanup in expiration task)
- Modify: `src/networking/server.rs` (buffer shrink logic)

- [x] Add `active_watch_count: AtomicU32` field to `StorageEngine` struct, increment in WATCH handler, decrement in EXEC/DISCARD/UNWATCH/disconnect
- [x] In `start_expiration_task` (metadata_cleanup_counter >= 600 path): add `engine.key_versions.retain(|scoped_key, _| engine.key_exists_raw(scoped_key))`
- [x] After `response_buffer.clear()` calls: add shrink_to(buffer_size) when capacity > 64KB
- [x] After `partial_command_buffer.clear()` calls: replace with `Vec::new()` when capacity > 64KB
- [x] Write test: key_versions map is cleaned up after keys expire
- [x] Write test: buffer shrink behavior (verify capacity reduction after large command)
- [x] Run `cargo test` - all tests must pass

### Task 2: O(1) Memory Tracking

This is the core performance fix. Changes ModifyResult::Keep to carry a byte delta, eliminating O(n) calculate_entry_size calls from the hot path. Requires updating 69 call sites across 6 files - all must change atomically since the enum change breaks compilation.

**Files:**
- Modify: `src/storage/item.rs` (add cached_mem_size field to Entry)
- Modify: `src/storage/value.rs` (change ModifyResult::Keep to Keep(i64))
- Modify: `src/storage/engine.rs` (update atomic_modify + 18 callers)
- Modify: `src/command/types/list.rs` (27 callers)
- Modify: `src/command/types/set.rs` (14 callers)
- Modify: `src/command/types/sorted_set.rs` (3 callers)
- Modify: `src/command/types/stream.rs` (6 callers)
- Modify: `src/command/types/bitmap.rs` (1 caller)

- [x] Add `cached_mem_size: u64` field (serde skip, default 0) to Entry struct in item.rs
- [x] Update `Entry::new()` and `Entry::new_string()` to compute and store initial cached_mem_size
- [x] Change `ModifyResult::Keep` to `ModifyResult::Keep(i64)` in value.rs (i64 = byte delta)
- [x] Update `atomic_modify` in engine.rs: for Keep(delta), use entry.cached_mem_size + delta instead of calling calculate_entry_size twice; for Occupied entries, read old_size from cached_mem_size (O(1))
- [x] Update all 18 engine.rs callers to return Keep(delta) with correct byte deltas
- [x] Update all 27 list.rs callers: LPUSH/RPUSH return `Keep((value.len() + 24) as i64)`, LPOP/RPOP return `Keep(-((popped.len() + 24) as i64))`, etc.
- [x] Update all 14 set.rs callers: SADD `Keep((member.len() + 32) as i64)` per new member, SREM negative delta
- [x] Update 3 sorted_set.rs callers: ZADD `Keep((member.len() * 2 + 40) as i64)` per new entry
- [x] Update 6 stream.rs callers with appropriate deltas for XADD/XTRIM
- [x] Update 1 bitmap.rs caller with delta for bit operations
- [x] Add cached_mem_size recalculation in RDB restore and AOF replay paths (one-time full scan per entry)
- [x] Write test: LPUSH N items then LPOP N items - verify used_memory returns to baseline (validates delta accuracy)
- [x] Write test: cached_mem_size matches calculate_entry_size for each data type after operations
- [x] Run `cargo test` - all tests must pass

### Task 3: Write Path Optimizations

**Files:**
- Modify: `src/storage/engine.rs` (conditionals, ordering, fast-paths, prefix_index config)

- [x] Make `bump_key_version` conditional: early return when `active_watch_count.load(Relaxed) == 0`
- [x] Change write_counters `fetch_add` from SeqCst to Relaxed (lines ~1391, 1400) - only used for BGSAVE threshold
- [x] Change `global_version` `fetch_add` from SeqCst to Relaxed (lines ~1462, 1472) - uniqueness guaranteed by atomicity, not ordering
- [x] Add fast-path in `check_write_memory`: when maxmemory == 0 (unlimited), return Ok immediately without async overhead
- [x] Add `enable_prefix_index: bool` to runtime config (default false); skip `update_prefix_indices` calls when disabled
- [x] Write test: WATCH + concurrent SET still correctly invalidates transaction when active_watch_count > 0
- [x] Write test: WATCH behavior unchanged when bump_key_version is conditional
- [x] Run `cargo test` - all tests must pass

### Task 4: Verify Acceptance Criteria

- [x] Run full test suite: `cargo test`
- [x] Run clippy: `cargo clippy -- -D warnings`
- [x] Run benchmarks to measure improvement: `cargo bench`
- [x] Verify memory behavior: no unbounded growth pattern in key_versions or buffers

### Task 5: Update Documentation

- [x] Update CLAUDE.md architecture section to document cached_mem_size, O(1) memory tracking, and conditional key versioning
- [x] Move docs/OPTIMIZATION_PLAN.md to docs/plans/completed/
- [x] Move this plan to docs/plans/completed/
