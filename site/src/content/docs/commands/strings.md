---
title: "Strings"
description: "String command reference"
order: 2
category: "commands"
---

# String Commands

Strings are the most basic data type in rLightning. A string value can hold up to 512 MB and can store text, integers, floating-point numbers, or binary data.

## SET

Synopsis: `SET key value [NX | XX] [GET] [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]`

Set a key to a string value. Overwrites any existing value and discards any previous TTL.

- `NX` -- Only set if the key does not exist.
- `XX` -- Only set if the key already exists.
- `GET` -- Return the old value stored at the key (or nil if none).
- `EX` / `PX` / `EXAT` / `PXAT` -- Set expiration.
- `KEEPTTL` -- Retain the existing TTL.

```bash
> SET mykey "hello"
OK
> SET mykey "world" NX
(nil)
> SET counter 10 EX 60
OK
> SET mykey "new" GET
"hello"
```

## GET

Synopsis: `GET key`

Get the value of a key. Returns nil if the key does not exist. An error is returned if the value is not a string.

```bash
> SET mykey "hello"
OK
> GET mykey
"hello"
> GET nonexistent
(nil)
```

## MSET

Synopsis: `MSET key value [key value ...]`

Set multiple keys to multiple values atomically. MSET never fails.

```bash
> MSET key1 "val1" key2 "val2" key3 "val3"
OK
```

## MGET

Synopsis: `MGET key [key ...]`

Return the values of all specified keys. For keys that do not exist, nil is returned.

```bash
> MSET a "1" b "2" c "3"
OK
> MGET a b c nonexistent
1) "1"
2) "2"
3) "3"
4) (nil)
```

## SETNX

Synopsis: `SETNX key value`

Set a key to a value only if the key does not already exist. Returns 1 if the key was set, 0 otherwise.

```bash
> SETNX mykey "hello"
(integer) 1
> SETNX mykey "world"
(integer) 0
> GET mykey
"hello"
```

## SETEX

Synopsis: `SETEX key seconds value`

Set a key to a value with an expiration in seconds. Atomic shorthand for SET + EXPIRE.

```bash
> SETEX mykey 10 "hello"
OK
> TTL mykey
(integer) 10
```

## PSETEX

Synopsis: `PSETEX key milliseconds value`

Set a key to a value with an expiration in milliseconds.

```bash
> PSETEX mykey 10000 "hello"
OK
> PTTL mykey
(integer) 9998
```

## GETSET

Synopsis: `GETSET key value`

Atomically set a key to a new value and return the old value. Deprecated in Redis 6.2; use `SET key value GET` instead.

```bash
> SET mykey "hello"
OK
> GETSET mykey "world"
"hello"
> GET mykey
"world"
```

## GETDEL

Synopsis: `GETDEL key`

Get the value of a key and delete it. Returns nil if the key does not exist.

```bash
> SET mykey "hello"
OK
> GETDEL mykey
"hello"
> GET mykey
(nil)
```

## GETEX

Synopsis: `GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds | PXAT unix-time-milliseconds | PERSIST]`

Get the value of a key and optionally set or remove its expiration.

```bash
> SET mykey "hello"
OK
> GETEX mykey EX 100
"hello"
> TTL mykey
(integer) 100
> GETEX mykey PERSIST
"hello"
> TTL mykey
(integer) -1
```

## APPEND

Synopsis: `APPEND key value`

Append a value to the end of the string stored at key. If the key does not exist, it is created with the given value. Returns the length of the string after the append.

```bash
> SET mykey "hello"
OK
> APPEND mykey " world"
(integer) 11
> GET mykey
"hello world"
```

## STRLEN

Synopsis: `STRLEN key`

Return the length of the string value stored at key. Returns 0 if the key does not exist.

```bash
> SET mykey "hello"
OK
> STRLEN mykey
(integer) 5
```

## INCR

Synopsis: `INCR key`

Increment the integer value of a key by one. If the key does not exist, it is initialized to 0 before the operation. Returns the new value.

```bash
> SET counter "10"
OK
> INCR counter
(integer) 11
```

## INCRBY

Synopsis: `INCRBY key increment`

Increment the integer value of a key by the given amount. Returns the new value.

```bash
> SET counter "10"
OK
> INCRBY counter 5
(integer) 15
```

## INCRBYFLOAT

Synopsis: `INCRBYFLOAT key increment`

Increment the float value of a key by the given amount. Returns the new value as a bulk string.

```bash
> SET temperature "23.5"
OK
> INCRBYFLOAT temperature 1.5
"25"
> INCRBYFLOAT temperature -2.0
"23"
```

## DECR

Synopsis: `DECR key`

Decrement the integer value of a key by one. Returns the new value.

```bash
> SET counter "10"
OK
> DECR counter
(integer) 9
```

## DECRBY

Synopsis: `DECRBY key decrement`

Decrement the integer value of a key by the given amount. Returns the new value.

```bash
> SET counter "10"
OK
> DECRBY counter 3
(integer) 7
```

## SETRANGE

Synopsis: `SETRANGE key offset value`

Overwrite part of the string stored at key starting at the specified offset. Returns the length of the string after modification.

```bash
> SET mykey "Hello World"
OK
> SETRANGE mykey 6 "Redis"
(integer) 11
> GET mykey
"Hello Redis"
```

## GETRANGE

Synopsis: `GETRANGE key start end`

Return a substring of the string stored at key, determined by the offsets start and end (both inclusive). Negative offsets count from the end.

```bash
> SET mykey "Hello World"
OK
> GETRANGE mykey 0 4
"Hello"
> GETRANGE mykey -5 -1
"World"
```

## MSETNX

Synopsis: `MSETNX key value [key value ...]`

Set multiple keys to multiple values, only if none of the specified keys exist. Atomic: either all keys are set or none are. Returns 1 if all keys were set, 0 if no keys were set.

```bash
> MSETNX key1 "val1" key2 "val2"
(integer) 1
> MSETNX key2 "new" key3 "val3"
(integer) 0
```

## LCS

Synopsis: `LCS key1 key2 [LEN] [IDX] [MINMATCHLEN min] [WITHMATCHLEN]`

Find the longest common substring between two string values. Available since Redis 7.0.

```bash
> SET a "ohmytext"
OK
> SET b "mynewtext"
OK
> LCS a b
"mytext"
> LCS a b LEN
(integer) 6
```

## SUBSTR

Synopsis: `SUBSTR key start end`

Return a substring of the string stored at key. Deprecated alias for GETRANGE.

```bash
> SET mykey "Hello World"
OK
> SUBSTR mykey 0 4
"Hello"
```
