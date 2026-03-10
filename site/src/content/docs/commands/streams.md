---
title: "Streams"
description: "Stream command reference"
order: 7
category: "commands"
---

# Stream Commands

Streams are an append-only log data structure introduced in Redis 5.0. They support consumer groups for reliable message processing, making them suitable for event sourcing, message queues, and real-time data pipelines.

## XADD

Synopsis: `XADD key [NOMKSTREAM] [MAXLEN | MINID [= | ~] threshold] id | * field value [field value ...]`

Append a new entry to a stream. Use `*` to auto-generate the entry ID. Returns the ID of the added entry.

- `NOMKSTREAM` -- Do not create the stream if it does not exist.
- `MAXLEN` -- Cap the stream to approximately the given number of entries.
- `MINID` -- Remove entries with IDs lower than the given threshold.
- `~` -- Allow approximate trimming for performance.

```bash
> XADD mystream * name "Alice" action "login"
"1678886400000-0"
> XADD mystream MAXLEN ~ 1000 * name "Bob" action "purchase"
"1678886400001-0"
```

## XLEN

Synopsis: `XLEN key`

Return the number of entries in a stream.

```bash
> XADD mystream * msg "hello"
"1678886400000-0"
> XLEN mystream
(integer) 1
```

## XRANGE

Synopsis: `XRANGE key start end [COUNT count]`

Return a range of entries from a stream between two IDs (inclusive). Use `-` for the minimum ID and `+` for the maximum ID.

```bash
> XRANGE mystream - +
1) 1) "1678886400000-0"
   2) 1) "name"
      2) "Alice"
2) 1) "1678886400001-0"
   2) 1) "name"
      2) "Bob"
> XRANGE mystream - + COUNT 1
1) 1) "1678886400000-0"
   2) 1) "name"
      2) "Alice"
```

## XREVRANGE

Synopsis: `XREVRANGE key end start [COUNT count]`

Return a range of entries in reverse order. Note the argument order is end then start.

```bash
> XREVRANGE mystream + - COUNT 1
1) 1) "1678886400001-0"
   2) 1) "name"
      2) "Bob"
```

## XREAD

Synopsis: `XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]`

Read entries from one or more streams, starting after the given ID. Use `$` to read only new entries (with BLOCK).

```bash
> XREAD COUNT 2 STREAMS mystream 0
1) 1) "mystream"
   2) 1) 1) "1678886400000-0"
         2) 1) "name"
            2) "Alice"
      2) 1) "1678886400001-0"
         2) 1) "name"
            2) "Bob"
```

## XINFO

The XINFO family of commands returns information about streams, consumer groups, and consumers.

### XINFO STREAM

Synopsis: `XINFO STREAM key [FULL [COUNT count]]`

Return information about a stream including length, first/last entry, and consumer group count.

```bash
> XINFO STREAM mystream
 1) "length"
 2) (integer) 2
 3) "first-entry"
 4) 1) "1678886400000-0"
    2) 1) "name"
       2) "Alice"
 ...
```

### XINFO GROUPS

Synopsis: `XINFO GROUPS key`

Return information about all consumer groups of a stream.

```bash
> XINFO GROUPS mystream
1) 1) "name"
   2) "mygroup"
   3) "consumers"
   4) (integer) 1
   5) "pending"
   6) (integer) 0
   ...
```

### XINFO CONSUMERS

Synopsis: `XINFO CONSUMERS key group`

Return information about consumers in a consumer group.

```bash
> XINFO CONSUMERS mystream mygroup
1) 1) "name"
   2) "consumer1"
   3) "pending"
   4) (integer) 2
   ...
```

## XGROUP

The XGROUP family manages consumer groups.

### XGROUP CREATE

Synopsis: `XGROUP CREATE key group id | $ [MKSTREAM] [ENTRIESREAD entries-read]`

Create a new consumer group. Use `$` to start from the latest entry or `0` to start from the beginning.

```bash
> XGROUP CREATE mystream mygroup $ MKSTREAM
OK
```

### XGROUP DESTROY

Synopsis: `XGROUP DESTROY key group`

Destroy a consumer group.

```bash
> XGROUP DESTROY mystream mygroup
(integer) 1
```

### XGROUP DELCONSUMER

Synopsis: `XGROUP DELCONSUMER key group consumer`

Remove a consumer from a group. Returns the number of pending entries the consumer had.

```bash
> XGROUP DELCONSUMER mystream mygroup consumer1
(integer) 0
```

### XGROUP SETID

Synopsis: `XGROUP SETID key group id | $ [ENTRIESREAD entries-read]`

Set the last delivered ID of a consumer group.

```bash
> XGROUP SETID mystream mygroup 0
OK
```

### XGROUP CREATECONSUMER

Synopsis: `XGROUP CREATECONSUMER key group consumer`

Create a consumer in a consumer group. Returns 1 if the consumer was created, 0 if it already existed.

```bash
> XGROUP CREATECONSUMER mystream mygroup consumer1
(integer) 1
```

## XREADGROUP

Synopsis: `XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]`

Read entries from a stream via a consumer group. Use `>` as the ID to receive new (undelivered) entries. Use a specific ID (e.g., `0`) to re-read pending entries.

```bash
> XREADGROUP GROUP mygroup consumer1 COUNT 1 STREAMS mystream >
1) 1) "mystream"
   2) 1) 1) "1678886400000-0"
         2) 1) "name"
            2) "Alice"
```

## XACK

Synopsis: `XACK key group id [id ...]`

Acknowledge one or more entries as processed by a consumer group. Returns the number of entries acknowledged.

```bash
> XACK mystream mygroup "1678886400000-0"
(integer) 1
```

## XCLAIM

Synopsis: `XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms] [RETRYCOUNT count] [FORCE] [JUSTID] [LASTID id]`

Claim ownership of pending entries. Changes the consumer that owns the entry.

```bash
> XCLAIM mystream mygroup consumer2 0 "1678886400000-0"
1) 1) "1678886400000-0"
   2) 1) "name"
      2) "Alice"
```

## XAUTOCLAIM

Synopsis: `XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]`

Automatically claim pending entries that have been idle for at least `min-idle-time` milliseconds. Returns a cursor, the claimed entries, and deleted entry IDs.

```bash
> XAUTOCLAIM mystream mygroup consumer2 3600000 0-0 COUNT 10
1) "0-0"
2) 1) 1) "1678886400000-0"
      2) 1) "name"
         2) "Alice"
3) (empty array)
```

## XPENDING

Synopsis: `XPENDING key group [[IDLE min-idle-time] start end count [consumer]]`

Inspect the pending entries list (PEL) of a consumer group. Without optional arguments, returns a summary. With range arguments, returns details about specific pending entries.

```bash
> XPENDING mystream mygroup - + 10
1) 1) "1678886400000-0"
   2) "consumer1"
   3) (integer) 120000
   4) (integer) 1
```

## XTRIM

Synopsis: `XTRIM key MAXLEN | MINID [= | ~] threshold`

Trim a stream to the specified length or minimum ID.

```bash
> XTRIM mystream MAXLEN 1000
(integer) 5
> XTRIM mystream MINID "1678886400000-0"
(integer) 3
```

## XDEL

Synopsis: `XDEL key id [id ...]`

Remove one or more entries from a stream by ID. Returns the number of entries deleted.

```bash
> XDEL mystream "1678886400000-0"
(integer) 1
```
