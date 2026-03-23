# Plan: rLightning Performance Fixes + Cluster Activation

## Context

Полный прогон бенчмарков (100K запросов, 50 клиентов, P=16) выявил три категории проблем:

1. **SET throughput ceiling** — 250K rps при любом pipeline depth (Redis: 1.7M→2.2M при P=1→P=64). rLightning не масштабируется.
2. **List operations катастрофически медленные** — LPUSH/RPUSH/LPOP/RPOP на 0.4-7% от Redis (p50=25-75ms vs <1.5ms).
3. **Кластер не работает** — код на ~95% написан, но gossip не запускается, MOVED/ASK не вызывается, ноды не объединяются.

---

## Part A: Performance — SET Throughput (Priority 1, Highest Impact)

### Root Cause
В post-batch loop (`server.rs:654-663`) для **каждой** команды вызывается:
- `update_prefix_indices_for_db().await` — async write lock на глобальный `prefix_index: tokio::sync::RwLock<HashMap>` (`engine.rs:720`)
- `pm.log_command_for_db().await` — per-command MPSC send для AOF (`server.rs:790`)
- `repl.propagate_commands_batch().await` — per-command replication (`server.rs:802-805`)

Pipeline из 64 SET = 64 последовательных async write lock acquisitions + 64 MPSC sends. Это сериализует весь pipeline.

### Step A1: Batch prefix index updates
**Files:** `src/storage/engine.rs`, `src/networking/server.rs`

1. Добавить метод `update_prefix_indices_batch_for_db(&self, updates: &[(&[u8], bool)], db_idx: usize)` в `StorageEngine` (рядом с line 739). Один lock acquisition для всех ключей в batch.
2. В `server.rs` (lines 654-663): вместо per-command вызова — собирать `Vec<(&[u8], bool)>`, вызвать batch метод **один раз после цикла** (после line 817).

### Step A2: Batch AOF logging
**Files:** `src/networking/server.rs`

В цикле (lines 785-791): вместо per-command `pm.log_command_for_db()` — собирать `Vec<RespCommand>`, вызвать **один** `pm.log_commands_batch_for_db()` после цикла. Метод уже существует в `persistence/mod.rs`.

### Step A3: Batch replication propagation
**Files:** `src/networking/server.rs`

В цикле (lines 792-806): вместо per-command `repl.propagate_commands_batch(&[select, cmd])` — собирать все пары (SELECT + write cmd) в Vec, вызвать **один** `propagate_commands_batch()` после цикла.

### Expected Impact
N lock acquisitions → 1. N MPSC sends → 1. SET throughput должен масштабироваться с pipeline depth (~250K → 1M+).

### Risk: Low
- `prefix_index` — кэш для KEYS, не влияет на корректность
- AOF batch метод уже существует и протестирован
- Порядок SELECT+command сохраняется в Vec

---

## Part B: Performance — List Operations (Priority 2)

### Root Cause
Double-clone элементов: `args[1..].to_vec()` (clone #1) → `e.clone()` внутри `atomic_modify` (clone #2). Каждый элемент клонируется дважды, причём второй clone происходит под write lock.

### Step B1: Eliminate double-clone (Quick Win)
**Files:** `src/command/types/list.rs`

В `lpush()` (lines 50-70) и `rpush()` (lines 82-105):

**Было:**
```rust
let elements: Vec<Vec<u8>> = args[1..].to_vec(); // clone #1
// inside atomic_modify:
for e in &elements {
    deque.push_front(e.clone()); // clone #2 — unnecessary
}
```

**Станет:**
```rust
let elements: Vec<Vec<u8>> = args[1..].to_vec(); // clone #1
// inside atomic_modify (closure is FnOnce, can consume):
for e in elements {          // move ownership, NO clone
    deque.push_front(e);
}
```

Аналогично для: `lpushx`, `rpushx`, и batch-вариантов в `batch.rs` где возможно.

### Step B2: CompactValue for list elements (Optional, если B1 недостаточно)
**Files:** `src/storage/value.rs`, `src/command/types/list.rs`, `src/storage/batch.rs`, persistence

Заменить `VecDeque<Vec<u8>>` → `VecDeque<CompactValue>`. Элементы ≤23 байт хранятся inline без heap allocation. Это более масштабное изменение, затрагивает все list-операции и сериализацию. Делать **только если** B1 не даёт достаточного улучшения.

### Expected Impact
B1: Устранение ~50% overhead на list mutations (N heap allocations на command).
B2: Дополнительные ~30% для малых элементов (inline storage).

### Risk
- B1: **Very low** — механическая замена clone на move, семантически идентично
- B2: **Medium** — меняет internal representation, затрагивает много файлов

---

## Part C: Cluster Activation (Priority 3)

### Root Cause
Код написан (~2600 строк), но не подключён: gossip не стартует, MOVED/ASK не вызывается.

### Step C1: Start gossip protocol
**Files:** `src/main.rs` (line 594), `src/cluster/gossip.rs`

После `cluster.init(addr).await` (main.rs:594), спавнить:
```rust
if cluster.is_enabled() {
    cluster.init(addr).await;
    let cluster_arc = Arc::new(cluster);
    // Start cluster bus listener (port+10000)
    tokio::spawn(cluster::gossip::start_cluster_bus(Arc::clone(&cluster_arc)));
    // Start cluster cron (periodic PING/PONG)
    tokio::spawn(cluster::gossip::cluster_cron(Arc::clone(&cluster_arc)));
    info!('Cluster mode enabled, gossip started at {}', addr);
}
```

Нужно проверить сигнатуры `start_cluster_bus` и `cluster_cron` — могут потребоваться `Arc<ClusterManager>`.

### Step C2: Add MOVED/ASK routing to command dispatch
**Files:** `src/networking/server.rs` (~line 1463-1590)

Перед выполнением каждой команды (в dispatch_command):
1. Добавить `asking: bool` в per-connection state
2. Перехватывать команду ASKING — устанавливать флаг, возвращать OK
3. Для key-bearing команд: вызвать `cluster_mgr.get_redirect(key, asking)`
4. Если MOVED — вернуть `RespValue::Error('MOVED slot host:port')`
5. Если ASK — вернуть `RespValue::Error('ASK slot host:port')`
6. Сбросить `asking` flag после каждой не-ASKING команды

Список команд без slot-проверки: PING, INFO, CLUSTER, COMMAND, AUTH, MULTI, EXEC, DISCARD, WATCH.

### Step C3: Fix CLUSTER NODES format
**Files:** `src/cluster/mod.rs` (~line 220-242)

`to_cluster_nodes_line()` использует string concatenation — нужно проверить формат соответствует Redis 7 (для совместимости с `redis-cli --cluster create`).

### Step C4: Dynamic cluster_enabled in INFO
**Files:** `src/command/types/connection.rs`, `src/storage/engine.rs`

INFO server/cluster должны читать `cluster_enabled` из runtime config, а не хардкодить `0`.

### Expected Impact
`redis-cli --cluster create` сможет инициализировать кластер. MOVED редиректы будут работать. Gossip обеспечит обнаружение нод.

### Risk: Medium
- Gossip code не тестировался в production — возможны баги при PING/PONG обмене
- MOVED routing добавляет overhead на каждую команду (read lock на cluster state)
- Нужно убедиться что standalone mode не затронут (проверка `cluster.is_enabled()`)

---

## Implementation Order

| # | Step | Files | Risk | Impact | Est. |
|---|------|-------|------|--------|------|
| 1 | A1: Batch prefix index | engine.rs, server.rs | Low | Very High | 1h |
| 2 | A2: Batch AOF | server.rs | Low | High | 30m |
| 3 | A3: Batch replication | server.rs | Low | Medium | 30m |
| 4 | **Verify A1-A3** | benchmark | — | — | 15m |
| 5 | B1: List double-clone | list.rs | Very Low | High | 30m |
| 6 | **Verify B1** | benchmark | — | — | 10m |
| 7 | C1: Start gossip | main.rs | Medium | High | 1h |
| 8 | C2: MOVED/ASK routing | server.rs | Medium | High | 2h |
| 9 | C3: CLUSTER NODES format | cluster/mod.rs | Low | Medium | 30m |
| 10 | C4: Dynamic INFO | connection.rs, engine.rs | Low | Low | 30m |
| 11 | **Verify C1-C4** | docker cluster test | — | — | 30m |
| 12 | B2: CompactValue lists | value.rs, list.rs, batch.rs + | Medium | Medium | 3h |

**Steps 1-6** — быстрые low-risk wins. **Steps 7-11** — кластер. **Step 12** — только если B1 недостаточно.

---

## Verification

### After Steps 1-6 (Performance)
```bash
cargo test                                    # все 1400+ тестов
cd .test-runs && bash run-all.sh --quick      # full benchmark suite vs Redis
```
**Критерии успеха:**
- SET с P=16: >500K rps (сейчас 253K)
- SET с P=64: >1M rps (сейчас 245K)
- LPUSH/RPUSH: >100K rps (сейчас 11-28K)
- Все существующие тесты проходят

### After Steps 7-11 (Cluster)
```bash
cargo test                                    # все тесты
# Docker cluster test:
cd .test-runs
docker compose --project-name test-runs --profile cluster up -d --pull never
# Init Redis cluster:
docker run --rm --network=test-runs_testnet redis:7-alpine \
  redis-cli --cluster create rl-cluster-1:6379 rl-cluster-2:6379 rl-cluster-3:6379 \
  -a test_password --cluster-yes
# Verify MOVED works:
docker run --rm --network=test-runs_testnet redis:7-alpine \
  redis-cli -h rl-cluster-1 -p 6379 -a test_password SET foo bar
```
**Критерии успеха:**
- `redis-cli --cluster create` успешно инициализирует кластер
- CLUSTER NODES показывает все 3 ноды с корректными слотами
- SET на ключ не-своего слота возвращает MOVED
- redis-benchmark --cluster работает

### Final: Compatibility
```bash
cd tests/docker-compat && bash run-tests.sh --local   # multi-language compat
```
