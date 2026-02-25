# Sorted Set Commands

Sorted sets combine the uniqueness of sets with an associated score for each member, enabling range queries by score or rank.

## Basic Operations

### ZADD

Add members with scores.

```bash
ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member [score member ...]
```

### ZSCORE / ZMSCORE

Get the score of one or multiple members.

```bash
ZSCORE key member
ZMSCORE key member [member ...]
```

### ZCARD

Get the number of members.

```bash
ZCARD key
```

### ZREM

Remove members.

```bash
ZREM key member [member ...]
```

### ZINCRBY

Increment a member's score.

```bash
ZINCRBY key increment member
```

## Range Queries

### ZRANGE (Unified, Redis 6.2+)

The unified ZRANGE supports rank, score, and lex ranges.

```bash
ZRANGE key min max [BYSCORE|BYLEX] [REV] [LIMIT offset count] [WITHSCORES]
```

### ZRANGEBYSCORE / ZREVRANGEBYSCORE

Range by score (legacy).

```bash
ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
```

### ZRANGEBYLEX / ZREVRANGEBYLEX

Range by lexicographic order.

```bash
ZRANGEBYLEX key min max [LIMIT offset count]
ZREVRANGEBYLEX key max min [LIMIT offset count]
```

### ZRANK / ZREVRANK

Get the rank of a member.

```bash
ZRANK key member
ZREVRANK key member
```

### ZCOUNT / ZLEXCOUNT

Count members in a score or lex range.

```bash
ZCOUNT key min max
ZLEXCOUNT key min max
```

## Removal

### ZREMRANGEBYRANK / ZREMRANGEBYSCORE / ZREMRANGEBYLEX

Remove members by rank, score, or lex range.

```bash
ZREMRANGEBYRANK key start stop
ZREMRANGEBYSCORE key min max
ZREMRANGEBYLEX key min max
```

## Pop Operations

### ZPOPMIN / ZPOPMAX

Pop members with lowest or highest scores.

```bash
ZPOPMIN key [count]
ZPOPMAX key [count]
```

### BZPOPMIN / BZPOPMAX

Blocking pop variants.

```bash
BZPOPMIN key [key ...] timeout
BZPOPMAX key [key ...] timeout
```

### ZMPOP / BZMPOP

Pop from multiple sorted sets.

```bash
ZMPOP numkeys key [key ...] MIN|MAX [COUNT count]
BZMPOP timeout numkeys key [key ...] MIN|MAX [COUNT count]
```

## Set Operations

### ZINTER / ZUNION / ZDIFF

Return intersection, union, or difference without storing.

```bash
ZINTER numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
ZUNION numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX] [WITHSCORES]
ZDIFF numkeys key [key ...] [WITHSCORES]
```

### ZINTERSTORE / ZUNIONSTORE / ZDIFFSTORE

Store results.

```bash
ZINTERSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
ZUNIONSTORE destination numkeys key [key ...] [WEIGHTS weight ...] [AGGREGATE SUM|MIN|MAX]
ZDIFFSTORE destination numkeys key [key ...]
```

### ZRANGESTORE

Store a range result.

```bash
ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT offset count]
```

## Other

### ZRANDMEMBER

Return random members.

```bash
ZRANDMEMBER key [count [WITHSCORES]]
```

### ZSCAN

Incrementally iterate members.

```bash
ZSCAN key cursor [MATCH pattern] [COUNT count]
```
