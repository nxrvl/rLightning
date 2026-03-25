# rLightning: Performance & Memory Optimization Plan

## Context

Бенчмарки `.test-runs/results/report-20260323-090056.txt` (100K req, 50 clients, pipeline 16) показывают:
- **List ops (LPUSH/RPUSH/LPOP/RPOP)**: 20-100x медленнее Redis 7
- **Write ops (SET/SADD/HSET/ZADD)**: 3-10x медленнее Redis 7
- **Read ops (GET, LRANGE)**: паритет или быстрее
- **Утечка памяти**: рост до 100GB при бенчмарках

Корневые причины верифицированы чтением кода.

---

## Phase 1: Memory Leaks (CRITICAL)

### 1A. `key_versions` DashMap никогда не очищается
**Файл**: `src/storage/engine.rs:181`
- `key_versions: DashMap<Vec<u8>, u64>` — `insert` на каждом write (строки 1463, 1473)
- **0 вызовов `remove`/`retain`** во всём проекте (верифицировано grep)
- При 10M ключей с 20-byte key: ~1GB неосвобождаемой памяти

**Исправление**: В `start_expiration_task` (строка 316, где `metadata_cleanup_counter >= 600`):
- Добавить `engine.key_versions.retain(|scoped_key, _| engine.key_exists_raw(scoped_key))`
- Или проще: добавить `AtomicU32 active_watch_count` — когда 0, пропускать `bump_key_version` и периодически чистить `key_versions.clear()`

### 1B. Response buffer никогда не shrink
**Файл**: `src/networking/server.rs:305,437`
- `response_buffer.clear()` сохраняет capacity
- Один LRANGE 1M элементов → buffer на мегабайты навсегда

**Исправление**: После `response_buffer.clear()` (строка 437):
```rust
if response_buffer.capacity() > 64 * 1024 {
    response_buffer.shrink_to(buffer_size);
}
```

### 1C. Partial command buffer не shrink
**Файл**: `src/networking/server.rs:306,452`
- `partial_command_buffer.clear()` при 16MB limit × 10K connections = 160GB worst case

**Исправление**: После `partial_command_buffer.clear()` (строки 452 и др.):
```rust
if partial_command_buffer.capacity() > 64 * 1024 {
    partial_command_buffer = Vec::new();
}
```

### Верификация Phase 1
- `cargo test` — все 2609 тестов
- Grep `key_versions` — убедиться в наличии retain/clear
- Ручной тест: `redis-benchmark -n 1000000 -t set -r 1000000`, наблюдать RSS

---

## Phase 2: O(n) → O(1) Memory Tracking (HIGH — 20-100x list fix)

### Корневая причина
`src/storage/engine.rs:2957,2989` — `calculate_entry_size()` вызывается **дважды** в `atomic_modify`:
- `old_size` перед мутацией (строка 2957)
- `new_size` после мутации (строка 2989)

`calculate_entry_size` → `value.mem_size()` (`src/storage/value.rs:389-390`):
```rust
StoreValue::List(deque) => deque.iter().map(|elem| elem.len() + 24).sum::<usize>()
```
LPUSH в список из 100K элементов = 200K итераций на **каждый** push.

### 2A. Добавить `cached_mem_size` в Entry
**Файл**: `src/storage/item.rs:44`
```rust
pub struct Entry {
    pub value: StoreValue,
    #[serde(skip)]
    pub expires_at: Option<Instant>,
    pub lru_clock: u32,
    pub access_count: u16,
    #[serde(skip, default)]
    pub cached_mem_size: u64,  // NEW
}
```
- Обновить `Entry::new()`, `Entry::new_string()` — вычислять начальный размер

### 2B. Изменить `ModifyResult::Keep` → `Keep(i64)` с дельтой
**Файл**: `src/storage/value.rs:348`
```rust
pub enum ModifyResult {
    Keep(i64),        // delta в байтах (+ или -)
    KeepUnchanged,
    Set(StoreValue),
    Delete,
}
```
- **61 место использования** в 6 файлах (list.rs:27, set.rs:12, engine.rs:14, stream.rs:6, sorted_set.rs:1, bitmap.rs:1)
- Каждый caller уже знает что добавляет/удаляет → тривиально вычислить дельту

### 2C. Обновить `atomic_modify` для работы с дельтой
**Файл**: `src/storage/engine.rs:2942-3043`

Для `ModifyResult::Keep(delta)`:
```rust
ModifyResult::Keep(delta) => {
    if expired {
        self.current_memory.fetch_sub(old_size, Ordering::Relaxed);
        occ.remove();
        self.key_count.fetch_sub(1, Ordering::Relaxed);
    } else {
        // O(1) вместо O(n)!
        let entry = occ.get_mut();
        if delta != 0 {
            let new_size = (entry.cached_mem_size as i64 + delta) as u64;
            if delta > 0 {
                self.current_memory.fetch_add(delta as u64, Ordering::Relaxed);
            } else {
                self.current_memory.fetch_sub((-delta) as u64, Ordering::Relaxed);
            }
            entry.cached_mem_size = new_size;
        }
        entry.touch();
    }
}
```
Для `Occupied` entry: `old_size` берём из `occ.get().cached_mem_size` (строка 2957) — тоже O(1).

### 2D. Обновить все 61 caller
Основные паттерны:
- **LPUSH/RPUSH**: `Keep((value.len() + 24) as i64)` на каждый элемент, или сумма при bulk
- **LPOP/RPOP**: `Keep(-((popped.len() + 24) as i64))`
- **SADD**: `Keep((member.len() + 32) as i64)` за каждый новый member
- **HSET**: `Keep((field.len() + value.len() + 48) as i64)` за каждое новое поле
- **ZADD**: `Keep((member.len() * 2 + 40) as i64)` — entries + scores

### 2E. Пересчёт cached_mem_size после RDB/AOF load
В `restore_from_rdb` и после AOF replay — однократный проход:
```rust
entry.cached_mem_size = calculate_entry_size(key, &entry.value) as u64;
```

### Файлы для изменения
| Файл | Изменения |
|------|-----------|
| `src/storage/item.rs` | Добавить `cached_mem_size` поле |
| `src/storage/value.rs` | `Keep` → `Keep(i64)` |
| `src/storage/engine.rs` | `atomic_modify`, `calculate_entry_size` usage, 14 callers |
| `src/command/types/list.rs` | 27 callers |
| `src/command/types/set.rs` | 12 callers |
| `src/command/types/stream.rs` | 6 callers |
| `src/command/types/sorted_set.rs` | 1 caller |
| `src/command/types/bitmap.rs` | 1 caller |

### Верификация Phase 2
- `cargo test` — все 2609 тестов
- Бенчмарк: `redis-benchmark -n 100000 -t lpush -P 1` — ожидаем 10-50x улучшение
- Тест корректности: LPUSH N → LPOP N → INFO memory → used_memory возвращается к baseline

---

## Phase 3: Write Path Overhead (MEDIUM — 3-10x general writes)

### 3A. Условный `bump_key_version` (только при активных WATCH)
**Файл**: `src/storage/engine.rs:1451-1473`
- Добавить `active_watch_count: AtomicU32` в `StorageEngine`
- WATCH → `fetch_add(1)`, EXEC/DISCARD/UNWATCH/disconnect → `fetch_sub(1)`
- `bump_key_version`: `if self.active_watch_count.load(Relaxed) == 0 { return; }`
- Экономия: ~50-200ns + DashMap insert на **каждый** write без WATCH

### 3B. `SeqCst` → `Relaxed` в write counters
**Файл**: `src/storage/engine.rs:1391,1400`
- `counter.fetch_add(1, Ordering::SeqCst)` → `Ordering::Relaxed`
- Счётчики используются для порога BGSAVE — точность не критична
- На ARM (M5) SeqCst значительно дороже Relaxed

### 3C. `global_version` SeqCst → Relaxed
**Файл**: `src/storage/engine.rs:1462,1472`
- `self.global_version.fetch_add(1, Ordering::SeqCst)` → `Ordering::Relaxed`
- Уникальность гарантируется атомарностью fetch_add, ordering не влияет

### 3D. `check_write_memory` fast-path без async
**Файл**: `src/storage/engine.rs` (check_write_memory)
- Когда `maxmemory == 0` (unlimited) — немедленный return без async overhead
- Добавить inline `#[inline]` проверку перед async вызовом

### 3E. prefix_index → opt-in (отключен по умолчанию)
**Файл**: `src/storage/engine.rs:177`
- Добавить `enable_prefix_index: bool` в конфиг
- Default: `false` (большинству workloads не нужен)
- При `false` — пропускать `update_prefix_indices` полностью

### Верификация Phase 3
- `cargo test`
- WATCH-тест: `WATCH key` + SET из другого клиента → EXEC returns nil
- `redis-benchmark -n 100000 -t set,sadd,hset -P 1` — ожидаем 2-3x улучшение

---

## Phase 4: Advanced (LOW PRIORITY, stretch)

### 4A. Расширить batch path
**Файл**: `src/networking/server.rs:563-569`
- Убрать ограничение `protocol_version == RESP2`
- Убрать `security.is_none()` ограничение
- Убрать `parsed_commands.len() > 1` — даже одна команда может использовать batch path

### 4B. RDB snapshot streaming
**Файл**: `src/storage/sharded.rs:604-615`
- Заменить `snapshot_cow()` → streaming по шардам: lock shard, serialize, unlock, next
- Устраняет 2x пик памяти при BGSAVE

### 4C. Buffer pooling
- Пул переиспользуемых буферов для response/partial command
- Сокращает allocation pressure при высокой concurrency

---

## Порядок выполнения

```
Phase 1 (Memory Leaks)     ← НАЧАТЬ ЗДЕСЬ, 1 день
    ↓
Phase 2 (O(n) → O(1))      ← Главный performance fix, 2-3 дня
    ↓
Phase 3 (Write overhead)   ← Добивание, 1-2 дня
    ↓
Phase 4 (Advanced)          ← По необходимости
```

Фазы 1, 2, 3 независимы и могут выполняться параллельно, но рекомендую последовательно для более простого review.

## Ожидаемый результат

| Операция | Текущее | После Phase 2+3 | Цель |
|----------|---------|-----------------|------|
| LPUSH/RPUSH | 10-28K rps | 500K-1M rps | ~Redis parity |
| LPOP/RPOP | 10-32K rps | 500K-1M rps | ~Redis parity |
| SET | 274K rps | 800K-1.2M rps | 0.5-0.7x Redis |
| SADD/HSET | 300-438K rps | 1-2M rps | 0.5-0.7x Redis |
| Memory leak | 100GB growth | Bounded | Fixed |
