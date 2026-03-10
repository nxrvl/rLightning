---
title: "Sorted Sets"
description: "Sorted set command reference"
order: 6
category: "commands"
---

# Sorted Set Commands

Sorted sets combine the properties of sets (unique members) with a score for each member, maintaining elements in score order. They enable efficient range queries by score or rank.

## ZADD

Synopsis: `ZADD key [NX | XX] [GT | LT] [CH] [INCR] score member [score member ...]`

Add one or more members to a sorted set, or update the score of existing members.

- `NX` -- Only add new members, do not update existing.
- `XX` -- Only update existing members, do not add new.
- `GT` -- Only update when the new score is greater than the current score.
- `LT` -- Only update when the new score is less than the current score.
- `CH` -- Return the number of changed (added + updated) members.
- `INCR` -- Act like ZINCRBY; returns the new score.

```bash
> ZADD leaderboard 100 "alice" 200 "bob" 150 "charlie"
(integer) 3
> ZADD leaderboard GT CH 180 "alice"
(integer) 1
```

## ZREM

Synopsis: `ZREM key member [member ...]`

Remove one or more members from a sorted set. Returns the number of members removed.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZREM myset "b"
(integer) 1
```

## ZSCORE

Synopsis: `ZSCORE key member`

Return the score of a member in a sorted set. Returns nil if the member or key does not exist.

```bash
> ZADD myset 1.5 "a"
(integer) 1
> ZSCORE myset "a"
"1.5"
```

## ZRANK

Synopsis: `ZRANK key member [WITHSCORE]`

Return the rank (zero-based, ascending by score) of a member. Returns nil if the member does not exist. With `WITHSCORE`, returns both rank and score.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZRANK myset "b"
(integer) 1
```

## ZREVRANK

Synopsis: `ZREVRANK key member [WITHSCORE]`

Return the rank of a member with scores ordered from high to low. Returns nil if the member does not exist.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZREVRANK myset "b"
(integer) 1
```

## ZRANGE

Synopsis: `ZRANGE key min max [BYSCORE | BYLEX] [REV] [LIMIT offset count] [WITHSCORES]`

Return a range of members from a sorted set. The unified ZRANGE (Redis 6.2+) replaces ZRANGEBYSCORE, ZRANGEBYLEX, and ZREVRANGE.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZRANGE myset 0 -1 WITHSCORES
1) "a"
2) "1"
3) "b"
4) "2"
5) "c"
6) "3"
> ZRANGE myset 1 3 BYSCORE WITHSCORES
1) "a"
2) "1"
3) "b"
4) "2"
5) "c"
6) "3"
```

## ZREVRANGE

Synopsis: `ZREVRANGE key start stop [WITHSCORES]`

Return a range of members in reverse order (highest to lowest score). Deprecated; use `ZRANGE ... REV` instead.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZREVRANGE myset 0 -1 WITHSCORES
1) "c"
2) "3"
3) "b"
4) "2"
5) "a"
6) "1"
```

## ZRANGEBYSCORE

Synopsis: `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`

Return members with scores between min and max. Deprecated; use `ZRANGE ... BYSCORE` instead. Use `-inf` and `+inf` for unbounded ranges. Prefix a value with `(` for exclusive bounds.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZRANGEBYSCORE myset 1 2
1) "a"
2) "b"
```

## ZRANGEBYLEX

Synopsis: `ZRANGEBYLEX key min max [LIMIT offset count]`

Return members in a sorted set between lexicographic range (all members must have the same score). Use `[` for inclusive and `(` for exclusive bounds. `-` and `+` represent negative and positive infinity.

```bash
> ZADD myset 0 "a" 0 "b" 0 "c" 0 "d"
(integer) 4
> ZRANGEBYLEX myset "[a" "[c"
1) "a"
2) "b"
3) "c"
```

## ZCARD

Synopsis: `ZCARD key`

Return the number of members in a sorted set.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZCARD myset
(integer) 3
```

## ZCOUNT

Synopsis: `ZCOUNT key min max`

Return the number of members with scores between min and max (inclusive). Use `-inf`/`+inf` for unbounded and `(` for exclusive.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZCOUNT myset 1 2
(integer) 2
> ZCOUNT myset "(1" 3
(integer) 2
```

## ZLEXCOUNT

Synopsis: `ZLEXCOUNT key min max`

Return the number of members between the given lexicographic range (all members must have the same score).

```bash
> ZADD myset 0 "a" 0 "b" 0 "c"
(integer) 3
> ZLEXCOUNT myset "[a" "[c"
(integer) 3
```

## ZINCRBY

Synopsis: `ZINCRBY key increment member`

Increment the score of a member in a sorted set. If the member does not exist, it is added with the increment as its score. Returns the new score.

```bash
> ZADD myset 10 "a"
(integer) 1
> ZINCRBY myset 5 "a"
"15"
```

## ZUNIONSTORE

Synopsis: `ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]`

Compute the union of sorted sets and store the result in a new sorted set. Returns the cardinality of the resulting set.

```bash
> ZADD set1 1 "a" 2 "b"
(integer) 2
> ZADD set2 3 "b" 4 "c"
(integer) 2
> ZUNIONSTORE result 2 set1 set2
(integer) 3
```

## ZINTERSTORE

Synopsis: `ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight [weight ...]] [AGGREGATE SUM | MIN | MAX]`

Compute the intersection of sorted sets and store the result. Returns the cardinality of the resulting set.

```bash
> ZADD set1 1 "a" 2 "b"
(integer) 2
> ZADD set2 3 "b" 4 "c"
(integer) 2
> ZINTERSTORE result 2 set1 set2
(integer) 1
```

## ZDIFFSTORE

Synopsis: `ZDIFFSTORE destination numkeys key [key ...]`

Compute the difference of sorted sets and store the result. Returns the cardinality of the resulting set.

```bash
> ZADD set1 1 "a" 2 "b" 3 "c"
(integer) 3
> ZADD set2 2 "b"
(integer) 1
> ZDIFFSTORE result 2 set1 set2
(integer) 2
```

## ZRANDMEMBER

Synopsis: `ZRANDMEMBER key [count [WITHSCORES]]`

Return one or more random members from a sorted set. Positive count returns distinct members; negative count allows duplicates.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZRANDMEMBER myset 2 WITHSCORES
1) "b"
2) "2"
3) "a"
4) "1"
```

## ZMSCORE

Synopsis: `ZMSCORE key member [member ...]`

Return the scores of multiple members. Returns nil for members that do not exist.

```bash
> ZADD myset 1 "a" 2 "b"
(integer) 2
> ZMSCORE myset "a" "b" "nonexistent"
1) "1"
2) "2"
3) (nil)
```

## ZPOPMIN

Synopsis: `ZPOPMIN key [count]`

Remove and return the member(s) with the lowest score from a sorted set.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZPOPMIN myset
1) "a"
2) "1"
```

## ZPOPMAX

Synopsis: `ZPOPMAX key [count]`

Remove and return the member(s) with the highest score from a sorted set.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZPOPMAX myset
1) "c"
2) "3"
```

## BZPOPMIN

Synopsis: `BZPOPMIN key [key ...] timeout`

Blocking version of ZPOPMIN. Blocks until an element is available or the timeout expires.

```bash
> BZPOPMIN myset 5
(blocks until element available or timeout)
```

## BZPOPMAX

Synopsis: `BZPOPMAX key [key ...] timeout`

Blocking version of ZPOPMAX. Blocks until an element is available or the timeout expires.

```bash
> BZPOPMAX myset 5
(blocks until element available or timeout)
```

## ZRANGESTORE

Synopsis: `ZRANGESTORE dst src min max [BYSCORE | BYLEX] [REV] [LIMIT offset count]`

Store a range of members from a sorted set into a new key. Returns the number of elements in the resulting sorted set.

```bash
> ZADD src 1 "a" 2 "b" 3 "c" 4 "d"
(integer) 4
> ZRANGESTORE dst src 0 2
(integer) 3
```

## ZSCAN

Synopsis: `ZSCAN key cursor [MATCH pattern] [COUNT count]`

Incrementally iterate over members and scores in a sorted set.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZSCAN myset 0 MATCH "*"
1) "0"
2) 1) "a"
   2) "1"
   3) "b"
   4) "2"
   5) "c"
   6) "3"
```

## ZMPOP

Synopsis: `ZMPOP numkeys key [key ...] MIN | MAX [COUNT count]`

Pop one or more members with the lowest or highest score from the first non-empty sorted set.

```bash
> ZADD myset 1 "a" 2 "b" 3 "c"
(integer) 3
> ZMPOP 1 myset MIN COUNT 2
1) "myset"
2) 1) 1) "a"
      2) "1"
   2) 1) "b"
      2) "2"
```

## BZMPOP

Synopsis: `BZMPOP timeout numkeys key [key ...] MIN | MAX [COUNT count]`

Blocking version of ZMPOP. Blocks until an element is available or the timeout expires.

```bash
> BZMPOP 5 1 myset MIN
(blocks until element available or timeout)
```
