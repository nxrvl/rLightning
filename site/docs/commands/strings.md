# String Commands

String commands operate on string values, the simplest Redis data type. Strings can hold any binary data up to 512MB.

## Basic Operations

### SET

Set a key to hold a string value.

```bash
SET key value [EX seconds] [PX milliseconds] [EXAT unix-time] [PXAT unix-time-ms] [NX|XX] [GET] [KEEPTTL]
```

Options:

- `EX seconds` -- Set expiry in seconds
- `PX milliseconds` -- Set expiry in milliseconds
- `EXAT timestamp` -- Set expiry at absolute Unix timestamp
- `PXAT timestamp` -- Set expiry at absolute Unix timestamp in milliseconds
- `NX` -- Only set if key does not exist
- `XX` -- Only set if key already exists
- `GET` -- Return the old value before setting
- `KEEPTTL` -- Retain existing TTL

### GET

Get the value of a key.

```bash
GET key
```

### MSET / MGET

Set or get multiple keys atomically.

```bash
MSET key value [key value ...]
MGET key [key ...]
```

### GETEX

Get a value and optionally set its expiration.

```bash
GETEX key [EX seconds] [PX milliseconds] [EXAT timestamp] [PXAT timestamp] [PERSIST]
```

### GETDEL

Get a value and delete the key atomically.

```bash
GETDEL key
```

## Counter Operations

### INCR / DECR

Atomically increment or decrement an integer value.

```bash
INCR key
DECR key
INCRBY key increment
DECRBY key decrement
INCRBYFLOAT key increment
```

## String Manipulation

### APPEND

Append a value to a key.

```bash
APPEND key value
```

### GETRANGE / SUBSTR

Get a substring of the string stored at key.

```bash
GETRANGE key start end
SUBSTR key start end
```

### SETRANGE

Overwrite part of a string at the specified offset.

```bash
SETRANGE key offset value
```

### STRLEN

Get the length of the value stored at key.

```bash
STRLEN key
```

### LCS

Find the Longest Common Subsequence between two strings.

```bash
LCS key1 key2 [LEN] [IDX] [MINMATCHLEN len] [WITHMATCHLEN]
```

## Expiration Variants

### PSETEX

Set a key with millisecond expiration (equivalent to `SET key value PX ms`).

```bash
PSETEX key milliseconds value
```

### SETEX / SETNX

```bash
SETEX key seconds value
SETNX key value
```

### MSETNX

Set multiple keys only if none of them exist.

```bash
MSETNX key value [key value ...]
```
