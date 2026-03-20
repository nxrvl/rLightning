# rLightning v3.0 — Performance Optimization Architecture Plan

> **STATUS: ALL PHASES COMPLETED (2026-03-21)**
>
> All 11 phases (0-10) have been implemented and verified. See `docs/plans/completed/2026-03-20-performance-optimization-v3.md` for the detailed implementation plan with task completion status.
>
> **Results**: 1400+ tests passing, SET 174K/418K/443K rps at pipeline 1/16/64, GET 181K/1.9M/1.9M rps at pipeline 1/16/64. All persistence, replication, and compatibility tests pass.

> **Цель**: Достичь 3-10x прироста производительности по сравнению с Redis 7/8 при сохранении 100% совместимости с протоколом Redis (RESP2/RESP3, 400+ команд, persistence, replication, cluster, sentinel, ACL).
>
> **Подход**: Использование сильных сторон Rust (zero-cost abstractions, ownership model, fearless concurrency, SIMD, cache-friendly layouts) для архитектурных решений, недоступных в C-based Redis.
>
> **Ограничение**: Копирование кода запрещено. Все решения — оригинальная архитектура, вдохновлённая анализом подходов высокопроизводительных систем.

---

## Содержание

1. [Анализ текущих узких мест](#1-анализ-текущих-узких-мест)
2. [Фаза 0: Компиляция и профиль сборки](#2-фаза-0-компиляция-и-профиль-сборки)
3. [Фаза 1: Сетевой слой](#3-фаза-1-сетевой-слой)
4. [Фаза 2: Zero-Copy RESP парсер](#4-фаза-2-zero-copy-resp-парсер)
5. [Фаза 3: Шардированное хранилище](#5-фаза-3-шардированное-хранилище)
6. [Фаза 4: Pipeline Batching](#6-фаза-4-pipeline-batching)
7. [Фаза 5: Быстрый Command Dispatch](#7-фаза-5-быстрый-command-dispatch)
8. [Фаза 6: Нативные типы данных (устранение сериализации)](#8-фаза-6-нативные-типы-данных-устранение-сериализации)
9. [Фаза 7: Оптимизация памяти](#9-фаза-7-оптимизация-памяти)
10. [Фаза 8: Lock-Free Fast Paths](#10-фаза-8-lock-free-fast-paths)
11. [Фаза 9: TTL и Eviction](#11-фаза-9-ttl-и-eviction)
12. [Фаза 10: Persistence без блокировки](#12-фаза-10-persistence-без-блокировки)
13. [Стратегия совместимости](#13-стратегия-совместимости)
14. [Порядок реализации и приоритеты](#14-порядок-реализации-и-приоритеты)
15. [Ожидаемые результаты](#15-ожидаемые-результаты)

---

## 1. Анализ текущих узких мест

### Критические проблемы (блокируют производительность)

| # | Проблема | Влияние | Где |
|---|----------|---------|-----|
| 1 | **Нет `[profile.release]`** — сборка без LTO, без оптимизации codegen-units | 15-30% потери на всём | `Cargo.toml` |
| 2 | **TCP_NODELAY не включён для клиентов** — только для replication | Nagle delay 40ms на каждом ответе | `src/networking/server.rs` |
| 3 | **Bincode сериализация на каждую операцию** — Hash, Set, SortedSet хранятся как `Vec<u8>` и десериализуются/сериализуются при каждом доступе | 30-50% CPU на hot path | `src/storage/engine.rs` |
| 4 | **String аллокации в Command** — `name: String` создаёт heap allocation на каждую команду | Миллионы аллокаций/сек | `src/command/mod.rs` |
| 5 | **RespValue::SimpleString(String)** — heap allocation для 'OK', 'PONG' | Каждый ответ аллоцирует | `src/networking/resp.rs` |
| 6 | **Нет pipeline batching** — каждая команда в pipeline обрабатывается изолированно с отдельным захватом лока | Упущено 3-7x ускорение | `src/networking/server.rs` |
| 7 | **DashMap с default шардингом** — нет cache-line alignment, нет контроля числа шардов | False sharing, контенция | `src/storage/engine.rs` |
| 8 | **400+ String match для dispatch** — `command.name.to_lowercase()` + match на str | O(n) string comparison | `src/command/handler.rs` |

### Умеренные проблемы

| # | Проблема | Влияние |
|---|----------|---------|
| 9 | `Instant::now()` на каждый GET для LRU | Syscall overhead |
| 10 | `AcqRel` ordering на всех atomic счётчиках | Избыточная синхронизация для read-heavy |
| 11 | Отсутствие response streaming для больших коллекций | Memory spike на HGETALL/ZRANGE |
| 12 | Полный DB scan при eviction | Tail latency spike |
| 13 | RwLock на expiration queue | Контенция при высоком TTL трафике |

---

## 2. Фаза 0: Компиляция и профиль сборки [COMPLETED]

**Приоритет: КРИТИЧЕСКИЙ** | **Сложность: Минимальная** | **Ожидаемый эффект: +15-30%**

### Что сделать

Добавить в `Cargo.toml`:

```toml
[profile.release]
opt-level = 3
lto = 'fat'          # Link-Time Optimization — позволяет компилятору инлайнить через crate boundaries
codegen-units = 1    # Один codegen unit — максимальная оптимизация на уровне всего бинарника
strip = true         # Убрать debug символы — меньший бинарник, лучше кеширование
panic = 'abort'      # Не разматывать стек при panic — экономит 5-10% размера кода
```

### Почему это работает

- **LTO 'fat'**: Позволяет LLVM оптимизировать через границы crate'ов. Для rLightning это означает, что hot path от парсера через storage до сериализации ответа может быть оптимизирован как единый блок. Инлайнинг `DashMap::get()` → `atomic_read()` → бинарный ответ.
- **codegen-units = 1**: По умолчанию Rust компилирует в 16 параллельных единиц. Это ускоряет компиляцию, но лишает LLVM возможности видеть весь код сразу. С `codegen-units = 1` оптимизатор видит все функции и может принимать лучшие решения об инлайнинге и регистрах.
- **panic = 'abort'**: Экономит ~5% размера кода (нет unwinding tables), что улучшает instruction cache hit rate. Redis-сервер при панике всё равно должен перезапускаться.

### На что обратить внимание

- Время сборки увеличится в 2-3 раза. Это нормально — влияет только на release build.
- Проверить, что все тесты проходят с `panic = 'abort'` (catch_unwind не будет работать).
- Если используется `std::panic::catch_unwind` где-либо — заменить на explicit error handling.

---

## 3. Фаза 1: Сетевой слой [COMPLETED]

**Приоритет: КРИТИЧЕСКИЙ** | **Сложность: Средняя** | **Ожидаемый эффект: +10-20%**

### 3.1 TCP_NODELAY на клиентских соединениях

**Текущее состояние**: `set_nodelay(true)` вызывается только в `src/replication/server.rs` и `src/replication/client.rs`. Основные клиентские соединения работают С ВКЛЮЧЁННЫМ Nagle алгоритмом.

**Проблема**: Nagle алгоритм задерживает отправку маленьких пакетов до 40ms, ожидая агрегации. Для key-value store, где большинство ответов — маленькие ('+OK\r\n', '$5\r\nhello\r\n'), это катастрофа latency.

**Решение**: Немедленно после `accept()` в server.rs вызвать `stream.set_nodelay(true)`.

### 3.2 Оптимизация буферизации

**Текущее состояние**: 64KB read buffer, response накапливается в Vec, один `write_all` в конце.

**Архитектура нового решения**:

```
┌─────────────────────────────────────────────────────────┐
│                   Connection Handler                     │
│                                                          │
│  ┌──────────┐   ┌─────────────┐   ┌──────────────────┐ │
│  │ Read Ring │──▶│ RESP Parser │──▶│ Command Batch    │ │
│  │ Buffer    │   │ (zero-copy) │   │ (same-shard      │ │
│  │ 64KB      │   │             │   │  grouping)       │ │
│  └──────────┘   └─────────────┘   └────────┬─────────┘ │
│                                              │           │
│  ┌──────────┐   ┌─────────────┐   ┌────────▼─────────┐ │
│  │ Write    │◀──│ RESP Encoder│◀──│ Batch Executor   │ │
│  │ Buffer   │   │ (direct)    │   │ (one lock per    │ │
│  │ 64KB     │   │             │   │  shard batch)    │ │
│  └──────────┘   └─────────────┘   └──────────────────┘ │
└─────────────────────────────────────────────────────────┘
```

**Конкретные изменения**:

1. **Buffer Pool (per-thread)**: Вместо аллокации `BytesMut` на каждое соединение — пул переиспользуемых буферов. Tokio thread-local для zero-contention:
   ```
   thread_local! {
       static BUFFER_POOL: RefCell<Vec<BytesMut>> = RefCell::new(Vec::new());
   }
   ```

2. **Write Coalescing**: Для маленьких ответов (<1KB) — не вызывать `write_all` после каждой пачки. Накапливать в write buffer и flush только когда: (a) buffer полон, (b) нет больше команд в read buffer, (c) явный flush trigger.

3. **Writev / vectored writes**: Использовать `tokio::io::AsyncWrite::poll_write_vectored` для отправки нескольких iovec за один syscall. Особенно эффективно для pipeline responses.

### 3.3 Connection Handling

**Текущее состояние**: `Semaphore(10_000)` для ограничения соединений. Per-connection DashMap update.

**Оптимизация**:
- Убрать update `last_cmd` из hot path. Обновлять только при CLIENT LIST/CLIENT INFO запросе.
- Заменить `created_at: Instant` на unix timestamp (дешевле, не syscall).

---

## 4. Фаза 2: Zero-Copy RESP парсер [COMPLETED]

**Приоритет: ВЫСОКИЙ** | **Сложность: Высокая** | **Ожидаемый эффект: +15-25%**

### Текущая проблема

Парсер создаёт `RespValue` с owned data:
```
RespValue::BulkString(Option<Vec<u8>>)   // аллокация на каждый аргумент
RespValue::SimpleString(String)           // аллокация на каждый ответ
RespValue::Array(Option<Vec<RespValue>>)  // Vec + вложенные аллокации
```

Команда `SET mykey myvalue` создаёт:
- 1 Vec для Array
- 1 Vec<u8> для 'SET'
- 1 Vec<u8> для 'mykey'
- 1 Vec<u8> для 'myvalue'
- 1 String для command.name (дополнительно .to_lowercase())

**Это 5+ heap allocations на одну простейшую команду.**

### Архитектура решения: Borrowed Parser

Ввести второй, zero-copy парсер, который возвращает byte slices в read buffer:

```
// Новая структура — ссылки в исходный буфер, нулевые аллокации
struct RawCommand<'buf> {
    name: &'buf [u8],          // 'SET' — slice из read buffer
    args: SmallVec<[&'buf [u8]; 4]>,  // SmallVec — 4 аргумента inline, без heap
}
```

**Ключевые решения**:

1. **SmallVec<[&[u8]; 4]>**: Большинство Redis команд имеют 1-4 аргумента. SmallVec хранит до 4 элементов на стеке без heap allocation. Только для команд с >4 аргументами (MSET, ZADD с многими элементами) произойдёт аллокация.

2. **Case-insensitive comparison без аллокации**: Вместо `command.name.to_lowercase()` — побайтовое сравнение с `to_ascii_uppercase()` на каждом байте. Это один CPU цикл на байт vs. целая String аллокация.

3. **Lifetime привязка к буферу**: `RawCommand<'buf>` живёт ровно до следующего чтения из сокета. Это безопасно, т.к. все команды из одного read batch обрабатываются до следующего read.

4. **Fallback на owned RespValue**: Для MULTI/EXEC (команды нужно сохранить) и для AOF logging — конвертация в owned форму. Но это <5% команд.

### Паттерн: Dual-Path Parser

```
            ┌──────────── hot path (95%) ──────────────┐
            │                                           │
Read Buffer ──▶ try_parse_raw() ──▶ RawCommand<'buf> ──▶ execute_inline()
            │                                           │
            └── fallback (5%: MULTI queue, AOF) ──────┘
                     │
                     ▼
              parse_owned() ──▶ RespValue (old path)
```

### Совместимость

- Существующий `RespValue` остаётся для: AOF persistence, MULTI queue, RESP3 responses, клиент-сайд парсинг.
- Новый `RawCommand` используется только в hot path: парсинг → dispatch → выполнение → ответ.
- `try_parse_common_command()` из текущего кода уже идёт в этом направлении — нужно расширить и обобщить.

---

## 5. Фаза 3: Шардированное хранилище [COMPLETED]

**Приоритет: КРИТИЧЕСКИЙ** | **Сложность: Высокая** | **Ожидаемый эффект: +20-40%**

### Текущая проблема с DashMap

DashMap — отличная обобщённая concurrent HashMap, но для key-value store она имеет недостатки:

1. **Нет контроля шардов**: DashMap выбирает число шардов по default (~num_cpus * 4). На 32-core машине это ~128 шардов, что может быть недостаточно.
2. **Нет cache-line alignment**: Соседние шарды DashMap могут попадать в одну cache line (64 байта), вызывая false sharing.
3. **Двойное хеширование**: DashMap хеширует ключ для выбора шарда, затем hashbrown хеширует ещё раз внутри шарда.
4. **Абстракция не позволяет batch access**: Нельзя захватить лок на шард и выполнить N операций — каждая операция захватывает/отпускает лок.
5. **SipHash по умолчанию**: DashMap использует SipHash (DoS-resistant, но медленный). Для key-value store, где ключи приходят от аутентифицированных клиентов, можно использовать более быстрый хеш.

### Архитектура: Custom Sharded Store

```
┌─────────────────────────────────────────────────────┐
│                    ShardedStore                       │
│                                                       │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐     ┌─────────┐│
│  │ Shard 0 │ │ Shard 1 │ │ Shard 2 │ ... │Shard N-1││
│  │ align128│ │ align128│ │ align128│     │ align128││
│  ├─────────┤ ├─────────┤ ├─────────┤     ├─────────┤│
│  │ RwLock  │ │ RwLock  │ │ RwLock  │     │ RwLock  ││
│  │(parking │ │(parking │ │(parking │     │(parking ││
│  │  _lot)  │ │  _lot)  │ │  _lot)  │     │  _lot)  ││
│  ├─────────┤ ├─────────┤ ├─────────┤     ├─────────┤│
│  │hashbrown│ │hashbrown│ │hashbrown│     │hashbrown││
│  │HashMap  │ │HashMap  │ │HashMap  │     │HashMap  ││
│  │(FxHash) │ │(FxHash) │ │(FxHash) │     │(FxHash) ││
│  ├─────────┤ ├─────────┤ ├─────────┤     ├─────────┤│
│  │ version │ │ version │ │ version │     │ version ││
│  │ memory  │ │ memory  │ │ memory  │     │ memory  ││
│  └─────────┘ └─────────┘ └─────────┘     └─────────┘│
└─────────────────────────────────────────────────────┘
```

### Ключевые решения

#### 5.1 Cache-Line Aligned Shards

```
#[repr(align(128))]  // 2 cache lines — полная изоляция
struct Shard {
    data: hashbrown::HashMap<Box<[u8]>, Entry, FxBuildHasher>,
    version: u64,       // для WATCH
    used_memory: usize, // per-shard memory tracking
    key_count: u32,     // per-shard key count
}
```

**Почему `align(128)`, а не `align(64)`**: На современных CPU (Intel после Skylake, ARM после A76) hardware prefetcher может загружать пары cache lines. `align(128)` гарантирует, что ни один соседний шард не будет загружен в кеш вместе с рабочим.

#### 5.2 Число шардов: `num_cpus * 16`, power of 2, clamp(16, 1024)

**Формула**: `(num_cpus * 16).next_power_of_two().clamp(16, 1024)`

**Обоснование**:
- На 8-core (типичный VPS): 128 шардов — 16x мультипликатор компенсирует lock contention
- На 32-core (production): 512 шардов — хеш-коллизии шардов минимальны
- На 128-core (large server): 1024 шардов — upper bound, дальше overhead от памяти структуры
- Power-of-2 позволяет заменить `% N` на `& (N-1)` — одна инструкция вместо деления

#### 5.3 FxHash вместо SipHash

**FxHash** (аналог FNV-1a с multiply-rotate) — 3-5x быстрее SipHash для коротких ключей (<64 байт), что покрывает 95%+ Redis ключей. Мы используем его для:
- Маппинг ключ → шард
- Внутренний lookup в hashbrown HashMap

**Одноразовое хеширование**: Вычислить хеш один раз, использовать для выбора шарда И для lookup внутри шарда через `raw_entry()` API hashbrown:

```
let hash = fx_hash(key);
let shard_idx = hash as usize & (num_shards - 1);
let shard = &self.shards[shard_idx];

// hashbrown raw_entry — ищет по pre-computed hash
shard.data.raw_entry().from_hash(hash, |k| k == key)
```

Это устраняет двойное хеширование, которое происходит в DashMap.

#### 5.4 parking_lot::RwLock вместо std

**parking_lot RwLock** преимущества:
- Не аллоцирует (1 word на стеке vs. Box в std)
- Adaptive spinning: spin-wait перед переходом в kernel futex
- Нет writer starvation (fair scheduling)
- `RwLockReadGuard` is `Send` — можно держать через await points (с осторожностью)

#### 5.5 Per-Shard Memory Tracking

Вместо глобального `AtomicU64 current_memory` с AcqRel на каждый SET/DEL — каждый шард отслеживает свою память. Глобальный подсчёт — сумма per-shard (O(N_shards), но вызывается только при INFO/CONFIG).

**Преимущество**: Устраняет cache-line bouncing на атомарном счётчике, который в текущей реализации обновляется на КАЖДУЮ write-операцию.

### Миграция с DashMap

1. Создать `ShardedStore` как новый модуль рядом с `StorageEngine`
2. Реализовать тот же API (get, set, atomic_modify, etc.)
3. Заменить `data: DashMap<Vec<u8>, StorageItem>` на `store: ShardedStore`
4. `extra_dbs` — массив `ShardedStore` (по одному на DB)
5. Запускать тесты совместимости после каждого шага

### Совместимость с текущими фичами

| Фича | Текущая реализация | Новая реализация |
|------|-------------------|-----------------|
| WATCH | `key_versions: DashMap` | Per-shard `version: u64` — обновляется при любой записи в шард |
| MULTI/EXEC | `key_locks: DashMap` + sorted locking | Sorted shard-level locking (захватывать шарды в порядке индекса) |
| SWAPDB | `db_mapping: AtomicU64` | Без изменений — pointer swap между ShardedStore |
| SELECT | `CURRENT_DB_INDEX` task-local | Без изменений |
| Memory eviction | Full DB scan | Per-shard candidate queue (см. Фазу 9) |

---

## 6. Фаза 4: Pipeline Batching [COMPLETED]

**Приоритет: КРИТИЧЕСКИЙ** | **Сложность: Высокая** | **Ожидаемый эффект: +100-500% при pipeline > 1**

### Почему это главная оптимизация

При pipeline depth = 64 (типичный для высоконагруженных приложений через connection pooling):
- **Текущее поведение**: 64 команды → 64 захвата лока → 64 отпускания лока
- **С batching**: 64 команды → ~4-8 захватов лока (команды группируются по шарду)

При равномерном распределении ключей по 512 шардам, 64 команды дадут в среднем ~64 уникальных шардов (почти все разные). Но при реальных workloads с hot keys — группировка даёт огромный выигрыш.

### Архитектура

```
                         Pipeline Buffer (N commands)
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
              ┌──────────┐   ┌──────────┐   ┌──────────┐
              │ Shard 0  │   │ Shard 7  │   │ Shard 42 │
              │ commands: │   │ commands: │   │ commands: │
              │ [0,3,8]  │   │ [1,4]    │   │ [2,5,6,7]│
              └────┬─────┘   └────┬─────┘   └────┬─────┘
                   │              │              │
              1 write lock   1 read lock    1 write lock
              execute 3 ops  execute 2 ops  execute 4 ops
                   │              │              │
                   ▼              ▼              ▼
              Results placed at original indices [0..N]
```

### Алгоритм

```
fn process_pipeline(commands: &[RawCommand], store: &ShardedStore) -> Vec<Response> {
    let n = commands.len();
    let mut results = Vec::with_capacity(n);
    results.resize(n, Response::Pending);

    // Step 1: Classify — O(N), одна аллокация
    let mut shard_map: Vec<(u32, bool)> = Vec::with_capacity(n);  // (shard_idx, is_write)
    for cmd in commands {
        let key = cmd.primary_key();  // первый аргумент для большинства команд
        let shard = store.shard_for_key(key);
        let is_write = cmd.is_write();  // lookup по первому байту имени
        shard_map.push((shard, is_write));
    }

    // Step 2: Group consecutive same-shard commands
    // НЕ СОРТИРОВАТЬ — сохраняем порядок выполнения (Redis semantics)
    let mut i = 0;
    while i < n {
        let current_shard = shard_map[i].0;
        let mut batch_end = i + 1;
        let mut any_write = shard_map[i].1;

        // Расширяем batch пока шард совпадает
        while batch_end < n && shard_map[batch_end].0 == current_shard {
            any_write |= shard_map[batch_end].1;
            batch_end += 1;
        }

        // Step 3: Execute batch under one lock
        if any_write {
            let mut shard = store.write_shard(current_shard as usize);
            for j in i..batch_end {
                results[j] = execute_on_shard_mut(&mut shard, &commands[j]);
            }
        } else {
            let shard = store.read_shard(current_shard as usize);
            for j in i..batch_end {
                results[j] = execute_on_shard_ref(&shard, &commands[j]);
            }
        }

        i = batch_end;
    }

    results
}
```

### Критические детали

1. **Порядок выполнения**: Команды ДОЛЖНЫ выполняться в порядке получения (Redis semantics). Мы НЕ переупорядочиваем — только объединяем ПОСЛЕДОВАТЕЛЬНЫЕ команды на один шард.

2. **Read/Write classification**: Быстрая классификация по имени команды:
   - GET, MGET, EXISTS, TTL, TYPE, STRLEN, LLEN, SCARD, ZCARD — read
   - SET, DEL, INCR, LPUSH, SADD, ZADD — write
   - Таблица на ~50 самых частых команд, остальные — write по умолчанию (safe default)

3. **Multi-key commands**: MSET, MGET и т.д. — не batching'уются, выполняются как отдельная единица. Для MSET — sorted shard locking (как текущий `lock_keys()`).

4. **execute_on_shard_mut / execute_on_shard_ref**: Новые функции, которые оперируют **напрямую с HashMap шарда**, минуя абстракцию Store. Это ключ к производительности — никакого дополнительного lookup/lock.

5. **Fallback**: Если pipeline = 1 или команда требует cross-shard access — обычный путь.

### Совместимость

- MULTI/EXEC: Не batching'уется (команды уже кешируются в очереди и выполняются атомарно).
- EVAL/EVALSHA: Не batching'уется (Lua скрипт может обращаться к любым ключам).
- Pub/Sub: Subscription-mode команды не batching'уются.
- Pipeline error isolation сохраняется: каждая команда получает свой результат (Ok или Error).

---

## 7. Фаза 5: Быстрый Command Dispatch [COMPLETED]

**Приоритет: СРЕДНИЙ** | **Сложность: Средняя** | **Ожидаемый эффект: +3-8%**

### Текущая проблема

```rust
let cmd_lowercase = command.name.to_lowercase();  // String аллокация!
match cmd_lowercase.as_str() {                     // 400+ сравнений
    'set' => ...
    'get' => ...
    ...
}
```

На каждую команду: 1 String аллокация + O(log N) string comparisons.

### Архитектура: Two-Level Byte Dispatch

**Level 1**: Dispatch по первому байту (26 вариантов + fallback):

```
match cmd_name[0] | 0x20 {   // to_ascii_lowercase за 1 instruction
    b's' => dispatch_s(cmd_name, args, shard),
    b'g' => dispatch_g(cmd_name, args, shard),
    b'l' => dispatch_l(cmd_name, args, shard),
    b'h' => dispatch_h(cmd_name, args, shard),
    b'z' => dispatch_z(cmd_name, args, shard),
    ...
}
```

**Level 2**: Dispatch по длине + точное сравнение:

```
fn dispatch_s(name: &[u8], ...) {
    match name.len() {
        3 => if cmd_eq(name, b'SET') { ... },
        4 => {
            if cmd_eq(name, b'SREM') { ... }
            else if cmd_eq(name, b'SADD') { ... }
            else if cmd_eq(name, b'SCAN') { ... }
            else if cmd_eq(name, b'SORT') { ... }
        },
        6 => if cmd_eq(name, b'SELECT') { ... },
        7 => if cmd_eq(name, b'SETNX') { ... },
        ...
    }
}
```

**cmd_eq**: Case-insensitive byte comparison без аллокаций:

```
fn cmd_eq(input: &[u8], expected: &[u8]) -> bool {
    input.len() == expected.len()
        && input.iter().zip(expected).all(|(a, b)| a.to_ascii_uppercase() == *b)
}
```

### Преимущества

- **Нулевые аллокации**: Никаких String, никаких to_lowercase
- **Предсказуемые ветвления**: Первый байт покрывает 26 вариантов, длина дополнительно сужает
- **Cache-friendly**: Сравнения коротких byte slices помещаются в регистры

### Альтернатива: Perfect Hash Map (phf)

Для 400+ команд можно использовать `phf` crate (compile-time perfect hash):

```
static COMMANDS: phf::Map<&[u8], CommandFn> = phf_map! {
    b'SET' => handle_set,
    b'GET' => handle_get,
    ...
};
```

**Компромисс**: phf даёт O(1) lookup, но каждый lookup — один hash + один memory access. Two-level dispatch для top-20 команд будет быстрее (предсказуемый branch), но для tail-50 команд phf лучше. Рекомендую **гибридный подход**: fast path для top-20 команд + phf fallback для остальных.

---

## 8. Фаза 6: Нативные типы данных (устранение сериализации) [COMPLETED]

**Приоритет: КРИТИЧЕСКИЙ** | **Сложность: Очень высокая** | **Ожидаемый эффект: +30-50% для сложных типов**

### Текущая проблема

Все сложные типы (Hash, Set, SortedSet, List) хранятся как `Vec<u8>` с bincode сериализацией. Каждый HGET:

```
1. DashMap::get(key)                              // O(1)
2. bincode::deserialize::<HashMap>(value.as_slice()) // O(N) — десериализация ВСЕЙ хеш-таблицы
3. hashmap.get(field)                              // O(1)
4. Clone result                                    // O(field_len)
```

**HGET на хеше с 1000 полями десериализует все 1000 полей, чтобы вернуть одно.**

### Архитектура: Нативный Value Enum

```
enum StoreValue {
    // Строки — zero-copy через bytes::Bytes
    Str(Bytes),

    // Хеши — нативный HashMap, без сериализации
    Hash(hashbrown::HashMap<Box<[u8]>, Bytes, FxBuildHasher>),

    // Списки — текущий gap buffer остаётся (хорошая оптимизация)
    // но внутри хранит Bytes вместо Vec<u8>
    List(GapBuffer<Bytes>),

    // Множества — нативный HashSet
    Set(hashbrown::HashSet<Box<[u8]>, FxBuildHasher>),

    // Сортированные множества — dual index
    SortedSet {
        by_score: BTreeMap<(OrderedFloat<f64>, Box<[u8]>), ()>,
        by_member: hashbrown::HashMap<Box<[u8]>, f64, FxBuildHasher>,
    },

    // Streams — без изменений (уже нативные)
    Stream(StreamData),

    // Bitmap — без изменений
    Bitmap(Vec<u8>),

    // HyperLogLog — без изменений
    HyperLogLog(Vec<u8>),
}
```

### Ключевые решения

#### 8.1 `bytes::Bytes` вместо `Vec<u8>` для значений

**`Bytes`** — reference-counted byte buffer с O(1) clone:
- `clone()` увеличивает счётчик ссылок (atomic increment), не копирует данные
- `slice(range)` создаёт view без аллокации
- MGET на 100 ключей с `Bytes` — 100 atomic increments vs. 100 × memcpy с `Vec<u8>`

#### 8.2 `Box<[u8]>` вместо `String` для ключей

- `Box<[u8]>` на 8 байт меньше `String` (нет capacity field)
- Ключи иммутабельны после создания — capacity не нужна
- Экономия: 8 байт × миллионы ключей = десятки МБ

#### 8.3 hashbrown::HashMap с FxBuildHasher для Hash полей

- hashbrown (Swiss Tables) — та же библиотека, что используется в std, но с доступом к `raw_entry()` API
- FxBuildHasher — быстрый хешер для коротких ключей (типичные hash field names)
- SIMD-accelerated probing для поиска среди control bytes

#### 8.4 Dual-Index SortedSet

```
SortedSet {
    by_score: BTreeMap<(OrderedFloat<f64>, Box<[u8]>), ()>,  // для ZRANGEBYSCORE, ZRANGE
    by_member: HashMap<Box<[u8]>, f64>,                       // для ZSCORE, O(1) lookup
}
```

**Текущее решение** уже использует BTreeMap для sorted sets, но через bincode сериализацию. Нативный dual-index устраняет ser/deser overhead.

### Миграция данных

1. Persistence (RDB/AOF) сериализует/десериализует при записи/чтении на диск — отдельный codec
2. In-memory данные НИКОГДА не сериализуются — прямой доступ к нативным структурам
3. При загрузке RDB — десериализация один раз в нативную форму
4. При записи AOF — сериализация команд (уже работает)
5. При записи RDB snapshot — per-shard сериализация (см. Фазу 10)

### Entry Metadata

```
struct Entry {
    value: StoreValue,
    expires_at: Option<Instant>,  // TTL
    lru_clock: u32,               // approximated LRU (4 байта вместо 8)
    access_count: u16,            // LFU counter (saturating, 65K max)
    // НЕ ХРАНИТЬ created_at, last_accessed как Instant — дорого
}
```

**Оптимизация LRU clock**: Глобальный clock обновляется каждые 100ms. Entry хранит 32-bit snapshot. При eviction — разница clocks. Экономия 12 байт на entry vs. два `Instant` (16 байт каждый).

---

## 9. Фаза 7: Оптимизация памяти [COMPLETED]

**Приоритет: СРЕДНИЙ** | **Сложность: Средняя** | **Ожидаемый эффект: +5-15%**

### 9.1 Small String Optimization (SSO)

Для значений ≤ 23 байт (покрывает ~40% типичных Redis значений: counters, flags, short tokens) — хранить inline в enum variant:

```
enum CompactValue {
    Inline { len: u8, data: [u8; 23] },  // 24 байта, нулевые аллокации
    Heap(Bytes),                           // для больших значений
}
```

**Обоснование**: `Bytes` имеет overhead 8 байт (pointer) + 8 байт (length) + 8 байт (refcount/vtable) = 24 байта на heap. Для значения '42' (2 байта) — 12x overhead. SSO устраняет heap allocation полностью.

### 9.2 Интернирование частых ответов

Статические `Bytes` для типичных ответов:

```
static OK_RESPONSE: Bytes = Bytes::from_static(b'+OK\r\n');
static PONG_RESPONSE: Bytes = Bytes::from_static(b'+PONG\r\n');
static ZERO_RESPONSE: Bytes = Bytes::from_static(b':0\r\n');
static ONE_RESPONSE: Bytes = Bytes::from_static(b':1\r\n');
static NIL_RESPONSE: Bytes = Bytes::from_static(b'$-1\r\n');
static EMPTY_ARRAY: Bytes = Bytes::from_static(b'*0\r\n');
```

Для pipeline из 1000 PING команд: 0 аллокаций вместо 1000 × `String::from('PONG')`.

### 9.3 Response Serialization без промежуточных структур

Вместо:
```
Command result → RespValue enum → serialize to bytes → write to buffer
```

Новый hot path:
```
Command result → write directly to buffer
```

Для GET:
```
fn write_bulk_string(buf: &mut BytesMut, data: &[u8]) {
    buf.put_u8(b'$');
    // itoa — быстрая integer-to-string конвертация без аллокаций
    itoa::write(&mut buf.writer(), data.len());
    buf.extend_from_slice(b'\r\n');
    buf.extend_from_slice(data);
    buf.extend_from_slice(b'\r\n');
}
```

Никаких промежуточных `RespValue`, никаких `format!()`, никаких String аллокаций.

### 9.4 Зависимости для оптимизации

Добавить в `Cargo.toml`:

```toml
parking_lot = '0.12'       # Быстрые RwLock/Mutex
smallvec = '1.11'          # Stack-allocated small vectors
itoa = '1'                 # Fast integer-to-string
rustc-hash = '2'           # FxHash (FNV-1a variant)
```

---

## 10. Фаза 8: Lock-Free Fast Paths [COMPLETED]

**Приоритет: ВЫСОКИЙ** | **Сложность: Средняя** | **Ожидаемый эффект: +5-15% для read-heavy workloads**

### 10.1 Read-Optimized Shard Locking

Для read-heavy workloads (GET, EXISTS, TTL, TYPE) — RwLock позволяет неограниченное число concurrent readers:

```
// Множество потоков одновременно читают из шарда — нулевая блокировка
let guard = shard.read();  // parking_lot: spinlock → futex fallback
let entry = guard.data.raw_entry().from_hash(hash, |k| k == key);
```

### 10.2 Atomic Operations без lock-free overhead

Для простых операций (INCR, DECR, APPEND, SETNX) — использовать write lock на шард минимальное время:

```
fn incr(shard: &mut ShardGuard, key: &[u8]) -> Result<i64> {
    match shard.data.get_mut(key) {
        Some(entry) => {
            if let StoreValue::Str(bytes) = &mut entry.value {
                let n = parse_i64(bytes)?;
                let new = n + 1;
                *bytes = Bytes::from(new.to_string());
                Ok(new)
            } else {
                Err(wrong_type())
            }
        }
        None => {
            shard.data.insert(key.into(), Entry::new(StoreValue::Str(Bytes::from_static(b'1'))));
            Ok(1)
        }
    }
}
```

**Ключевое отличие от текущего кода**: Операция выполняется ВНУТРИ shard guard, без промежуточной десериализации/сериализации.

### 10.3 Relaxed Atomic Ordering где безопасно

| Операция | Текущий | Новый | Обоснование |
|----------|---------|-------|-------------|
| `memory_used.load()` (для INFO) | Acquire | Relaxed | Приблизительное значение допустимо |
| `key_count.load()` (для DBSIZE) | Acquire | Relaxed | Сумма per-shard, eventual consistency OK |
| `global_version.fetch_add()` | AcqRel | Release | Читается только через shard lock |
| `lru_clock.store()` | Release | Relaxed | Approximated LRU, точность не критична |

---

## 11. Фаза 9: TTL и Eviction [COMPLETED]

**Приоритет: СРЕДНИЙ** | **Сложность: Средняя** | **Ожидаемый эффект: Устранение tail latency spikes**

### 11.1 Per-Shard Expiration

Текущее: Одна глобальная `BinaryHeap` с `RwLock` — contention point.

Новое: Каждый шард поддерживает свою мини-очередь expiration:

```
#[repr(align(128))]
struct Shard {
    data: HashMap<...>,
    expiry_heap: BinaryHeap<ExpirationEntry>,
    // ...
}
```

Background task перебирает шарды round-robin, обрабатывая N expires за итерацию. Лок на шард уже захвачен — не нужен отдельный RwLock для очереди.

### 11.2 Probabilistic Eviction с Candidate Buffer

Вместо сканирования всей DB при eviction — per-shard candidate buffer:

```
struct EvictionCandidate {
    key_hash: u64,       // hash, не сам ключ (экономия памяти)
    lru_clock: u32,
    memory_estimate: u32,
}

struct Shard {
    // ...
    eviction_candidates: ArrayVec<EvictionCandidate, 16>,  // 16 кандидатов на стеке
}
```

При каждом read access — с вероятностью 1/16 записываем текущий entry как кандидата. При eviction — выбираем worst кандидата из всех шардов. Время eviction: O(N_shards × 16) = O(N_shards), а не O(N_keys).

### 11.3 Lazy Expiration без Instant::now()

Текущее: Каждый GET вызывает `Instant::now()` для проверки TTL.

Новое: Кешированный clock (обновляется каждые 1ms через background timer):

```
static CACHED_NOW: AtomicU64 = AtomicU64::new(0);  // millis since start

fn is_expired(entry: &Entry) -> bool {
    match entry.expires_at {
        None => false,
        Some(expiry) => CACHED_NOW.load(Relaxed) >= expiry,
    }
}
```

`Instant::now()` — это syscall (`clock_gettime`). На Linux ~20ns, на macOS ~40ns. При 1M ops/sec — 20-40ms CPU time сэкономлено.

---

## 12. Фаза 10: Persistence без блокировки [COMPLETED]

**Приоритет: СРЕДНИЙ** | **Сложность: Высокая** | **Ожидаемый эффект: Стабильность latency**

### 12.1 COW-Based RDB Snapshots

Текущее: `snapshot_all_dbs` захватывает `cross_db_lock` write lock — блокирует ВСЕ операции.

Новое: Per-shard snapshot с incremental versioning:

```
1. Захватить shard read lock
2. Клонировать HashMap (Bytes — cheap clone через refcount)
3. Отпустить lock
4. Сериализовать клон в background
5. Перейти к следующему шарду
```

**Время блокировки одного шарда**: ~1-10µs (clone metadata, increment refcounts).
**Общее время snapshot**: Сумма per-shard clones + background serialization. Latency per-request не страдает.

### 12.2 Lock-Free AOF Appending

Текущее: AOF batch staging с sync на каждый everysec.

Оптимизация: Lock-free MPSC channel для AOF entries:

```
// Producer (command thread) — zero blocking
aof_sender.send(AofEntry { db, command, args });

// Consumer (AOF writer thread) — batch drain + fsync
loop {
    let batch = aof_receiver.drain();
    write_batch_to_file(&batch);
    if everysec_elapsed { fsync(); }
}
```

Tokio `mpsc::unbounded_channel` или crossbeam channel для lock-free command streaming.

---

## 13. Стратегия совместимости

### 13.1 Принципы

1. **Протокол** (RESP2/RESP3): Парсер/сериализатор не меняют формат. Zero-copy парсер — внутренняя оптимизация, wire protocol идентичен.

2. **Семантика команд**: Все 400+ команд сохраняют точное поведение. Тестирование через существующие integration tests + docker-compat suite.

3. **Persistence**: RDB/AOF формат на диске не меняется. Сериализация при записи, десериализация при чтении.

4. **Replication**: PSYNC протокол не меняется. Propagation идёт через AOF entries.

5. **Transactions**: MULTI/EXEC сохраняют текущую изоляцию (sorted shard-level locking).

### 13.2 Тестирование на каждой фазе

```
# После каждой фазы:
cargo test                                    # Unit tests
cargo test --test integration_test            # Integration tests
cargo test --test redis_compatibility_test    # Docker compat
cd tests/docker-compat && ./run-tests.sh --local  # Multi-language compat (Go/JS/Python)
```

### 13.3 Regression Benchmarks

Создать benchmark suite, запускаемый после каждой фазы:

```bash
# Baseline (до изменений)
cargo bench --bench storage_bench -- --save-baseline before

# После изменений
cargo bench --bench storage_bench -- --baseline before
```

Критерий: performance НИКОГДА не должна деградировать по сравнению с предыдущей фазой.

---

## 14. Порядок реализации и приоритеты

```
                    ┌─────────────────┐
                    │ Фаза 0: Profile │ ◄── 1 час, +15-30%
                    │  (Cargo.toml)   │
                    └────────┬────────┘
                             │
                    ┌────────▼────────┐
                    │ Фаза 1: Network │ ◄── 2-3 дня, +10-20%
                    │ (TCP_NODELAY,   │
                    │  buffers)       │
                    └────────┬────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼                              ▼
    ┌─────────────────┐          ┌──────────────────┐
    │ Фаза 2: Parser  │          │ Фаза 5: Dispatch │
    │ (zero-copy RESP)│          │ (byte dispatch)  │
    │  5-7 дней       │          │  3-4 дня         │
    └────────┬────────┘          └────────┬─────────┘
             │                            │
             └──────────┬─────────────────┘
                        ▼
              ┌─────────────────┐
              │ Фаза 6: Native  │ ◄── 7-10 дней, +30-50% для complex types
              │   Data Types    │
              │ (remove bincode)│
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │ Фаза 3: Sharded │ ◄── 7-10 дней, +20-40%
              │    Store        │
              │ (replace        │
              │  DashMap)       │
              └────────┬────────┘
                       │
              ┌────────▼────────┐
              │ Фаза 4: Pipeline│ ◄── 5-7 дней, +100-500% at pipeline > 1
              │   Batching      │
              └────────┬────────┘
                       │
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
   ┌──────────┐ ┌──────────┐ ┌──────────┐
   │ Фаза 7:  │ │ Фаза 8:  │ │ Фаза 9:  │
   │ Memory   │ │ Lock-Free│ │ TTL &    │
   │ Optim    │ │ Paths    │ │ Eviction │
   │ 3-4 дня  │ │ 3-4 дня  │ │ 3-4 дня  │
   └──────────┘ └──────────┘ └──────────┘
                       │
              ┌────────▼────────┐
              │ Фаза 10:       │
              │ Persistence    │
              │ (COW snapshot) │
              │ 5-7 дней       │
              └────────────────┘
```

### Порядок и обоснование

| # | Фаза | Сложность | Эффект | Обоснование порядка |
|---|-------|-----------|--------|---------------------|
| 0 | Release profile | Минимальная | +15-30% | Одна строка в Cargo.toml, мгновенный результат |
| 1 | Network (TCP_NODELAY) | Низкая | +10-20% | Исправление бага, 5 минут работы |
| 2 | Zero-copy parser | Высокая | +15-25% | Фундамент для Фазы 4 (pipeline batching) |
| 5 | Byte dispatch | Средняя | +3-8% | Параллельно с парсером, нет зависимостей |
| 6 | Native types | Очень высокая | +30-50% | Устраняет главный bottleneck, нужна ДО шардирования |
| 3 | Sharded store | Высокая | +20-40% | Нужна новая Entry struct (из фазы 6) |
| 4 | Pipeline batching | Высокая | +100-500% | Требует шардированный store |
| 7-9 | Memory/Locks/TTL | Средняя | +5-15% each | Полировка после основных изменений |
| 10 | Persistence | Высокая | Стабильность | Последняя — не влияет на throughput |

---

## 15. Ожидаемые результаты

### Benchmark Targets (redis-benchmark, 50 clients, 1M requests)

| Команда | Pipeline | Redis 8.x | rLightning v2.1 (est.) | rLightning v3.0 (target) | Ratio vs Redis |
|---------|----------|-----------|------------------------|--------------------------|----------------|
| SET | 1 | 286K | ~200K | 300-350K | ~1.1x |
| SET | 16 | 2.07M | ~1.5M | 4-5M | ~2.2x |
| SET | 64 | 2.71M | ~2M | 8-12M | ~3.5x |
| SET | 256 | 2.99M | ~2.5M | 15-20M | ~6x |
| GET | 1 | 290K | ~200K | 320-380K | ~1.2x |
| GET | 64 | 2.8M | ~2M | 10-15M | ~4.5x |
| HSET | 1 | 200K | ~100K | 250-300K | ~1.3x |
| HGET | 64 | 2M | ~800K | 6-10M | ~4x |
| LPUSH | 64 | 2.5M | ~1.5M | 8-12M | ~4x |
| MSET (10) | 64 | 727K | ~500K | 700-900K | ~1.1x |

### Компромиссы

| Что выигрываем | Что теряем/усложняем |
|----------------|---------------------|
| 3-10x throughput при pipelining | Более сложная кодовая база |
| Лучшая latency для простых команд | Более длительная компиляция |
| Меньшее потребление памяти | Больше unsafe в critical sections (raw_entry) |
| Стабильная latency при persistence | Два пути парсинга (zero-copy + owned) |
| Масштабирование на многоядерные CPU | Debugging усложняется (per-shard state) |

### Что остаётся уникальным преимуществом rLightning vs Lux

| Фича | rLightning v3.0 | Lux 0.7.x |
|------|-----------------|-----------|
| RESP3 | Да | Нет |
| Команд | 400+ | ~190 |
| RDB persistence | Да | Snapshot only |
| AOF persistence | Да, with rewrite | Нет |
| Hybrid persistence | Да | Нет |
| Replication (PSYNC) | Да | Нет |
| Cluster mode | Да | Нет |
| Sentinel HA | Да | Нет |
| ACL system | Да | Password only |
| MULTI/EXEC isolation | Полная (shard-level) | Нет (intermediate state visible) |
| Multi-database | 16 DB (SELECT) | Нет |
| Sharded Pub/Sub | Да | Нет |
| Lua Functions | Да | Нет |
| Pipeline batching | Да (Фаза 4) | Да |
| Zero-copy parser | Да (Фаза 2) | Да |
| Performance (pipeline) | Target: 3-10x vs Redis | Measured: 3-7x vs Redis |

---

## Приложение A: Новые зависимости

```toml
# Добавить в Cargo.toml [dependencies]
parking_lot = '0.12'       # Быстрые RwLock/Mutex (используется в шардированном store)
smallvec = '1.11'          # Stack-allocated small vectors (zero-copy parser args)
itoa = '1'                 # Fast integer→string (RESP serialization)
rustc-hash = '2'           # FxHashMap/FxHashSet (быстрый хешер для коротких ключей)

# Опционально (Phase 7+)
arrayvec = '0.7'           # Fixed-capacity stack arrays (eviction candidates)
```

## Приложение B: Метрики для отслеживания прогресса

После каждой фазы запускать:

```bash
# 1. Совместимость
cargo test && cargo test --test redis_compatibility_test
cd tests/docker-compat && ./run-tests.sh --local

# 2. Performance
redis-benchmark -h 127.0.0.1 -p 6379 -c 50 -n 1000000 -t set,get -P 1
redis-benchmark -h 127.0.0.1 -p 6379 -c 50 -n 1000000 -t set,get -P 16
redis-benchmark -h 127.0.0.1 -p 6379 -c 50 -n 1000000 -t set,get -P 64

# 3. Latency percentiles
redis-benchmark -h 127.0.0.1 -p 6379 -c 50 -n 100000 -t set,get --csv -P 1
```

## Приложение C: Checklist безопасности на каждой фазе

- [ ] Все тесты проходят (cargo test)
- [ ] Integration tests проходят
- [ ] Docker compat tests проходят
- [ ] Multi-language compat (Go/JS/Python) 100%
- [ ] Benchmark не деградировал vs. предыдущая фаза
- [ ] Нет data races (запустить с `RUSTFLAGS='-Z sanitizer=thread'` на nightly)
- [ ] Нет memory leaks (запустить с valgrind или `RUSTFLAGS='-Z sanitizer=address'`)
- [ ] Persistence: save → kill → restart → verify data intact
- [ ] Replication: master-replica sync после изменений работает
- [ ] MULTI/EXEC: concurrent transactions с WATCH работают корректно
