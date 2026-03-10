---
title: "Transactions"
description: "Transaction command reference"
order: 11
category: "commands"
---

# Transaction Commands

Transactions allow grouping multiple commands into an atomic unit using MULTI/EXEC. All commands in a transaction are serialized and executed sequentially without interruption from other clients. rLightning also supports optimistic locking via WATCH.

## How Transactions Work

1. Begin a transaction with `MULTI`.
2. Queue commands -- each command returns `QUEUED` instead of executing immediately.
3. Execute all queued commands atomically with `EXEC`.
4. Or discard the transaction with `DISCARD`.

If any command has a syntax error, the entire transaction is rejected at EXEC time.

## Optimistic Locking with WATCH

WATCH provides a check-and-set (CAS) mechanism:

1. Call `WATCH` on one or more keys before `MULTI`.
2. Read the current values and compute updates.
3. Begin the transaction with `MULTI` and queue commands.
4. Call `EXEC` -- if any watched key was modified by another client since the WATCH, EXEC returns nil and no commands are executed.

This pattern enables safe read-modify-write operations without locks.

```bash
> WATCH mykey
OK
> GET mykey
"100"
> MULTI
OK
> SET mykey "101"
QUEUED
> EXEC
1) OK
```

If another client modifies `mykey` between WATCH and EXEC:

```bash
> WATCH mykey
OK
> GET mykey
"100"
> MULTI
OK
> SET mykey "101"
QUEUED
> EXEC
(nil)
```

## MULTI

Synopsis: `MULTI`

Mark the start of a transaction block. Subsequent commands will be queued until EXEC or DISCARD.

```bash
> MULTI
OK
> SET key1 "a"
QUEUED
> SET key2 "b"
QUEUED
> EXEC
1) OK
2) OK
```

## EXEC

Synopsis: `EXEC`

Execute all commands queued after MULTI. Returns an array of results, one per command. Returns nil if a WATCH condition failed.

```bash
> MULTI
OK
> INCR counter
QUEUED
> INCR counter
QUEUED
> EXEC
1) (integer) 1
2) (integer) 2
```

## DISCARD

Synopsis: `DISCARD`

Discard all queued commands and exit the transaction state. Unwatches all keys.

```bash
> MULTI
OK
> SET key1 "a"
QUEUED
> DISCARD
OK
```

## WATCH

Synopsis: `WATCH key [key ...]`

Watch one or more keys for modifications. If any watched key is changed before EXEC, the transaction will be aborted. Can only be called before MULTI.

```bash
> WATCH balance
OK
> GET balance
"100"
> MULTI
OK
> DECRBY balance 50
QUEUED
> EXEC
1) (integer) 50
```

## UNWATCH

Synopsis: `UNWATCH`

Unwatch all previously watched keys. Automatically called after EXEC or DISCARD.

```bash
> WATCH mykey
OK
> UNWATCH
OK
```
