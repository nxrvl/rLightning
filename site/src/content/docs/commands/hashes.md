---
title: "Hashes"
description: "Hash command reference"
order: 3
category: "commands"
---

# Hash Commands

Hashes are maps of field-value pairs, ideal for representing objects. Each hash can store over 4 billion field-value pairs.

## HSET

Synopsis: `HSET key field value [field value ...]`

Set one or more field-value pairs in a hash. Creates the hash if it does not exist. Returns the number of new fields added (fields that were updated are not counted).

```bash
> HSET user:1 name "Alice" age "30" email "alice@example.com"
(integer) 3
> HSET user:1 age "31"
(integer) 0
```

## HGET

Synopsis: `HGET key field`

Return the value associated with a field in a hash. Returns nil if the field or hash does not exist.

```bash
> HSET user:1 name "Alice"
OK
> HGET user:1 name
"Alice"
> HGET user:1 nonexistent
(nil)
```

## HMSET

Synopsis: `HMSET key field value [field value ...]`

Set multiple field-value pairs in a hash. Deprecated in favor of HSET which now accepts multiple pairs.

```bash
> HMSET user:1 name "Alice" age "30"
OK
```

## HMGET

Synopsis: `HMGET key field [field ...]`

Return the values associated with the specified fields in a hash. Returns nil for fields that do not exist.

```bash
> HSET user:1 name "Alice" age "30"
(integer) 2
> HMGET user:1 name age nonexistent
1) "Alice"
2) "30"
3) (nil)
```

## HGETALL

Synopsis: `HGETALL key`

Return all fields and values in a hash. The result is a flat list of alternating field-name and field-value entries.

```bash
> HSET user:1 name "Alice" age "30"
(integer) 2
> HGETALL user:1
1) "name"
2) "Alice"
3) "age"
4) "30"
```

## HDEL

Synopsis: `HDEL key field [field ...]`

Remove one or more fields from a hash. Returns the number of fields that were removed.

```bash
> HSET user:1 name "Alice" age "30"
(integer) 2
> HDEL user:1 age
(integer) 1
> HDEL user:1 nonexistent
(integer) 0
```

## HEXISTS

Synopsis: `HEXISTS key field`

Determine whether a field exists in a hash. Returns 1 if the field exists, 0 otherwise.

```bash
> HSET user:1 name "Alice"
(integer) 1
> HEXISTS user:1 name
(integer) 1
> HEXISTS user:1 nonexistent
(integer) 0
```

## HLEN

Synopsis: `HLEN key`

Return the number of fields in a hash.

```bash
> HSET user:1 name "Alice" age "30"
(integer) 2
> HLEN user:1
(integer) 2
```

## HKEYS

Synopsis: `HKEYS key`

Return all field names in a hash.

```bash
> HSET user:1 name "Alice" age "30"
(integer) 2
> HKEYS user:1
1) "name"
2) "age"
```

## HVALS

Synopsis: `HVALS key`

Return all values in a hash.

```bash
> HSET user:1 name "Alice" age "30"
(integer) 2
> HVALS user:1
1) "Alice"
2) "30"
```

## HINCRBY

Synopsis: `HINCRBY key field increment`

Increment the integer value of a hash field by the given amount. If the field does not exist, it is set to 0 before the operation. Returns the new value.

```bash
> HSET user:1 age "30"
(integer) 1
> HINCRBY user:1 age 5
(integer) 35
```

## HINCRBYFLOAT

Synopsis: `HINCRBYFLOAT key field increment`

Increment the float value of a hash field by the given amount. Returns the new value as a bulk string.

```bash
> HSET product:1 price "19.99"
(integer) 1
> HINCRBYFLOAT product:1 price 5.01
"25"
```

## HSETNX

Synopsis: `HSETNX key field value`

Set a field in a hash only if the field does not already exist. Returns 1 if the field was set, 0 if it already existed.

```bash
> HSETNX user:1 name "Alice"
(integer) 1
> HSETNX user:1 name "Bob"
(integer) 0
> HGET user:1 name
"Alice"
```

## HRANDFIELD

Synopsis: `HRANDFIELD key [count [WITHVALUES]]`

Return one or more random fields from a hash.

- Without `count`: returns a single random field name.
- With positive `count`: returns up to `count` distinct fields.
- With negative `count`: returns `|count|` fields, possibly with duplicates.
- With `WITHVALUES`: includes the corresponding values.

```bash
> HSET myhash a "1" b "2" c "3"
(integer) 3
> HRANDFIELD myhash
"b"
> HRANDFIELD myhash 2 WITHVALUES
1) "a"
2) "1"
3) "c"
4) "3"
```

## HSCAN

Synopsis: `HSCAN key cursor [MATCH pattern] [COUNT count]`

Incrementally iterate over fields and values in a hash. Returns a cursor and a list of field-value pairs.

```bash
> HSET myhash a "1" b "2" c "3"
(integer) 3
> HSCAN myhash 0 MATCH "*" COUNT 10
1) "0"
2) 1) "a"
   2) "1"
   3) "b"
   4) "2"
   5) "c"
   6) "3"
```
