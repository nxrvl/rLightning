---
title: "Sets"
description: "Set command reference"
order: 5
category: "commands"
---

# Set Commands

Sets are unordered collections of unique strings. They support fast membership testing, set operations (union, intersection, difference), and random element retrieval.

## SADD

Synopsis: `SADD key member [member ...]`

Add one or more members to a set. Creates the set if it does not exist. Returns the number of members that were added (ignoring members already present).

```bash
> SADD myset "a" "b" "c"
(integer) 3
> SADD myset "c" "d"
(integer) 1
```

## SREM

Synopsis: `SREM key member [member ...]`

Remove one or more members from a set. Returns the number of members that were removed.

```bash
> SADD myset "a" "b" "c"
(integer) 3
> SREM myset "b" "d"
(integer) 1
```

## SMEMBERS

Synopsis: `SMEMBERS key`

Return all members of a set.

```bash
> SADD myset "a" "b" "c"
(integer) 3
> SMEMBERS myset
1) "a"
2) "b"
3) "c"
```

## SISMEMBER

Synopsis: `SISMEMBER key member`

Determine whether a member belongs to a set. Returns 1 if the member exists, 0 otherwise.

```bash
> SADD myset "a" "b"
(integer) 2
> SISMEMBER myset "a"
(integer) 1
> SISMEMBER myset "z"
(integer) 0
```

## SMISMEMBER

Synopsis: `SMISMEMBER key member [member ...]`

Check membership for multiple members at once. Returns an array of 0/1 values.

```bash
> SADD myset "a" "b"
(integer) 2
> SMISMEMBER myset "a" "z" "b"
1) (integer) 1
2) (integer) 0
3) (integer) 1
```

## SCARD

Synopsis: `SCARD key`

Return the number of members in a set (the cardinality).

```bash
> SADD myset "a" "b" "c"
(integer) 3
> SCARD myset
(integer) 3
```

## SPOP

Synopsis: `SPOP key [count]`

Remove and return one or more random members from a set.

```bash
> SADD myset "a" "b" "c" "d"
(integer) 4
> SPOP myset
"c"
> SPOP myset 2
1) "a"
2) "d"
```

## SRANDMEMBER

Synopsis: `SRANDMEMBER key [count]`

Return one or more random members from a set without removing them.

- Positive `count`: return up to `count` distinct members.
- Negative `count`: return `|count|` members, possibly with duplicates.

```bash
> SADD myset "a" "b" "c"
(integer) 3
> SRANDMEMBER myset
"b"
> SRANDMEMBER myset 2
1) "a"
2) "c"
```

## SUNION

Synopsis: `SUNION key [key ...]`

Return the union of all given sets.

```bash
> SADD set1 "a" "b"
(integer) 2
> SADD set2 "b" "c"
(integer) 2
> SUNION set1 set2
1) "a"
2) "b"
3) "c"
```

## SINTER

Synopsis: `SINTER key [key ...]`

Return the intersection of all given sets.

```bash
> SADD set1 "a" "b" "c"
(integer) 3
> SADD set2 "b" "c" "d"
(integer) 3
> SINTER set1 set2
1) "b"
2) "c"
```

## SDIFF

Synopsis: `SDIFF key [key ...]`

Return the difference between the first set and all subsequent sets.

```bash
> SADD set1 "a" "b" "c"
(integer) 3
> SADD set2 "b" "c" "d"
(integer) 3
> SDIFF set1 set2
1) "a"
```

## SUNIONSTORE

Synopsis: `SUNIONSTORE destination key [key ...]`

Compute the union of all given sets and store the result in `destination`. Returns the cardinality of the resulting set.

```bash
> SADD set1 "a" "b"
(integer) 2
> SADD set2 "b" "c"
(integer) 2
> SUNIONSTORE result set1 set2
(integer) 3
```

## SINTERSTORE

Synopsis: `SINTERSTORE destination key [key ...]`

Compute the intersection of all given sets and store the result in `destination`. Returns the cardinality of the resulting set.

```bash
> SADD set1 "a" "b" "c"
(integer) 3
> SADD set2 "b" "c" "d"
(integer) 3
> SINTERSTORE result set1 set2
(integer) 2
```

## SDIFFSTORE

Synopsis: `SDIFFSTORE destination key [key ...]`

Compute the difference between the first set and all subsequent sets, storing the result in `destination`. Returns the cardinality of the resulting set.

```bash
> SADD set1 "a" "b" "c"
(integer) 3
> SADD set2 "b" "c" "d"
(integer) 3
> SDIFFSTORE result set1 set2
(integer) 1
```

## SMOVE

Synopsis: `SMOVE source destination member`

Atomically move a member from one set to another. Returns 1 if the member was moved, 0 if it was not found in the source set.

```bash
> SADD src "a" "b"
(integer) 2
> SADD dst "c"
(integer) 1
> SMOVE src dst "b"
(integer) 1
> SMEMBERS dst
1) "b"
2) "c"
```

## SINTERCARD

Synopsis: `SINTERCARD numkeys key [key ...] [LIMIT limit]`

Return the cardinality of the intersection of the given sets. The optional `LIMIT` stops counting once the limit is reached (useful for large sets).

```bash
> SADD set1 "a" "b" "c"
(integer) 3
> SADD set2 "b" "c" "d"
(integer) 3
> SINTERCARD 2 set1 set2
(integer) 2
> SINTERCARD 2 set1 set2 LIMIT 1
(integer) 1
```

## SSCAN

Synopsis: `SSCAN key cursor [MATCH pattern] [COUNT count]`

Incrementally iterate over members of a set.

```bash
> SADD myset "a" "b" "c"
(integer) 3
> SSCAN myset 0 MATCH "*" COUNT 10
1) "0"
2) 1) "a"
   2) "b"
   3) "c"
```
