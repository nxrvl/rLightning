# rLightning Performance Fixes + Cluster Activation

## Overview

Implement the three optimization areas from docs/OPTIMIZATION_PLAN.md: (A) batch pipeline post-processing to unblock SET throughput scaling, (B) eliminate double-clone in list operations, and (C) activate the existing cluster code (gossip, MOVED/ASK routing, INFO reporting).

## Context

- Files involved: src/networking/server.rs, src/storage/engine.rs, src/command/types/list.rs, src/main.rs, src/cluster/mod.rs, src/cluster/gossip.rs, src/command/types/connection.rs
- Related patterns: Batch methods already exist (update_prefix_indices_batch_for_db, log_commands_batch_for_db, propagate_commands_batch) but are not called from the pipeline batch loop
- The branch performance-optimization-v3 has infrastructure (CompactValue, sharded storage, pipeline batching) but the post-batch loop in server.rs still does per-command calls

## Development Approach

- Testing approach: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Parts A and B are low-risk mechanical changes; Part C is medium-risk activation of existing code
- CRITICAL: every task MUST include new/updated tests
- CRITICAL: all tests must pass before starting next task

At the end, a full run with performance benchmarks and compatibility tests is mandatory. There must be 100% compatibility with the Redis protocol. There must be zero errors and zero warnings during compilation.

## Implementation Steps

### Task 1: Batch prefix index updates in pipeline loop (A1)

**Files:**

- Modify: `src/networking/server.rs`
- Modify: `src/storage/engine.rs`

- [x] In the post-batch loop in server.rs (around lines 654-663), collect prefix index updates into a Vec instead of calling update_prefix_indices_for_db() per command
- [x] Verify update_prefix_indices_batch_for_db exists in engine.rs; if not, add it to accept a slice of (key, is_delete) pairs and acquire the write lock once
- [x] Call the batch method once after the loop completes (after ~line 817)
- [x] Write a test that pipelines multiple SET commands and verifies prefix index is updated correctly
- [x] Run cargo test -- must pass before Task 2

### Task 2: Batch AOF logging in pipeline loop (A2)

**Files:**

- Modify: `src/networking/server.rs`

- [x] In the post-batch loop (around lines 785-791), collect AOF-eligible commands into a Vec instead of calling log_command_for_db() per command
- [x] Call log_commands_batch_for_db() once after the loop with the collected Vec
- [x] Write a test that pipelines write commands and verifies AOF entries are logged correctly
- [x] Run cargo test -- must pass before Task 3

### Task 3: Batch replication propagation in pipeline loop (A3)

**Files:**

- Modify: `src/networking/server.rs`

- [x] In the post-batch loop (around lines 802-805), collect all (SELECT cmd, write cmd) pairs into a Vec instead of calling propagate_commands_batch per command
- [x] Call propagate_commands_batch once after the loop with all accumulated pairs
- [x] Write a test verifying batched replication preserves command ordering (SELECT + command pairs)
- [x] Run cargo test -- must pass before Task 4

### Task 4: Eliminate double-clone in list operations (B1)

**Files:**

- Modify: `src/command/types/list.rs`

- [ ] In lpush() (around lines 50-70): change the closure to consume elements by value instead of cloning -- replace `for e in &elements { deque.push_front(e.clone()) }` with `for e in elements { deque.push_front(e) }`
- [ ] Apply the same change to rpush() (around lines 82-105)
- [ ] Apply the same change to lpushx() and rpushx()
- [ ] Verify atomic_modify closure signature allows FnOnce/consuming; adjust if needed
- [ ] Write a test that verifies LPUSH/RPUSH with multiple elements works correctly (values stored and retrievable)
- [ ] Run cargo test -- must pass before Task 5

### Task 5: Activate gossip protocol (C1)

**Files:**

- Modify: `src/main.rs`
- Modify: `src/cluster/gossip.rs` (if signatures need adjustment)

- [ ] After cluster.init(addr).await in main.rs (~line 594), spawn gossip tasks: start_cluster_bus and cluster_cron
- [ ] Wrap the cluster manager in Arc for sharing across spawned tasks
- [ ] Verify start_cluster_bus and cluster_cron function signatures accept Arc<ClusterManager> (or adjust)
- [ ] Ensure standalone mode (cluster disabled) is unaffected -- gossip tasks only spawn when cluster.is_enabled()
- [ ] Write a test that verifies gossip tasks are spawned when cluster mode is enabled and not spawned when disabled
- [ ] Run cargo test -- must pass before Task 6

### Task 6: Add MOVED/ASK routing to command dispatch (C2)

**Files:**

- Modify: `src/networking/server.rs`

- [ ] Add `asking: bool` field to per-connection state
- [ ] Intercept the ASKING command -- set the flag and return +OK
- [ ] Before executing key-bearing commands in dispatch_command, call cluster_mgr.get_redirect(key, asking) if cluster mode is enabled
- [ ] If MOVED, return RespValue::Error("MOVED {slot} {host}:{port}")
- [ ] If ASK, return RespValue::Error("ASK {slot} {host}:{port}")
- [ ] Reset the asking flag after each non-ASKING command
- [ ] Skip slot checks for: PING, INFO, CLUSTER, COMMAND, AUTH, MULTI, EXEC, DISCARD, WATCH
- [ ] Write tests: MOVED response for wrong-slot key, ASK flow, no redirect for slot-exempt commands
- [ ] Run cargo test -- must pass before Task 7

### Task 7: Fix CLUSTER NODES format and dynamic INFO (C3 + C4)

**Files:**

- Modify: `src/cluster/mod.rs`
- Modify: `src/command/types/connection.rs`
- Modify: `src/storage/engine.rs` (if needed for runtime config access)

- [ ] Verify to_cluster_nodes_line() output matches Redis 7 format (node-id, ip:port@cport, flags, master-id, ping-sent, pong-recv, config-epoch, link-state, slots)
- [ ] Fix any format discrepancies found
- [ ] In connection.rs INFO handler (~line 745), replace hardcoded cluster_enabled:0 with dynamic value from cluster config
- [ ] Write a test for CLUSTER NODES output format validation
- [ ] Write a test verifying INFO reports cluster_enabled:1 when cluster mode is on
- [ ] Run cargo test -- must pass before Task 8

### Task 8: Full verification -- compilation, tests, performance, compatibility

- [ ] Run `cargo build --release` -- must produce zero errors and zero warnings
- [ ] Run `cargo clippy` -- must produce zero warnings
- [ ] Run `cargo test` -- all 1400+ tests must pass with no regressions
- [ ] Run performance benchmarks: `cargo bench` -- verify SET with P=16 >500K rps, LPUSH/RPUSH improvements
- [ ] Run multi-language compatibility tests: `cd tests/docker-compat && ./run-tests.sh --local` -- must show 100% Redis protocol compatibility
- [ ] Verify no regressions in any compatibility test category

### Task 9: Update documentation

- [ ] Update CLAUDE.md if internal patterns changed (e.g., new batch methods, cluster startup sequence)
- [ ] Move plan to docs/plans/completed/
