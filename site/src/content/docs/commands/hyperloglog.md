---
title: "HyperLogLog"
description: "HyperLogLog command reference"
order: 9
category: "commands"
---

# HyperLogLog Commands

HyperLogLog is a probabilistic data structure for estimating the cardinality (number of unique elements) of a set. It uses a fixed amount of memory (~12 KB per key) regardless of the number of elements added, with a standard error of 0.81%.

## PFADD

Synopsis: `PFADD key [element [element ...]]`

Add one or more elements to a HyperLogLog. Returns 1 if the internal representation was modified, 0 otherwise.

```bash
> PFADD visitors "alice" "bob" "charlie"
(integer) 1
> PFADD visitors "alice"
(integer) 0
> PFCOUNT visitors
(integer) 3
```

## PFCOUNT

Synopsis: `PFCOUNT key [key ...]`

Return the approximate cardinality of the set(s) observed by the HyperLogLog(s). When called with multiple keys, returns the cardinality of the union.

```bash
> PFADD hll1 "a" "b" "c"
(integer) 1
> PFADD hll2 "c" "d" "e"
(integer) 1
> PFCOUNT hll1
(integer) 3
> PFCOUNT hll1 hll2
(integer) 5
```

## PFMERGE

Synopsis: `PFMERGE destkey sourcekey [sourcekey ...]`

Merge multiple HyperLogLog values into a single one. The destination key will contain the union of all source HyperLogLogs.

```bash
> PFADD hll1 "a" "b" "c"
(integer) 1
> PFADD hll2 "c" "d" "e"
(integer) 1
> PFMERGE merged hll1 hll2
OK
> PFCOUNT merged
(integer) 5
```

## PFDEBUG

Synopsis: `PFDEBUG subcommand key`

An internal debugging command for HyperLogLog values. Not intended for production use.

```bash
> PFDEBUG GETREG myhll
(array of register values)
```
