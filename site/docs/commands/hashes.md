# Hash Commands

Hash commands operate on hash data types, which map string fields to string values within a single key. Ideal for representing objects.

## Basic Operations

### HSET / HGET

Set or get a field in a hash.

```bash
HSET key field value [field value ...]
HGET key field
```

### HMSET / HMGET

Set or get multiple fields.

```bash
HMSET key field value [field value ...]
HMGET key field [field ...]
```

### HGETALL

Get all fields and values.

```bash
HGETALL key
```

### HDEL

Delete one or more fields.

```bash
HDEL key field [field ...]
```

### HEXISTS

Check if a field exists.

```bash
HEXISTS key field
```

### HLEN

Get the number of fields.

```bash
HLEN key
```

### HKEYS / HVALS

Get all field names or all values.

```bash
HKEYS key
HVALS key
```

## Advanced Operations

### HINCRBY / HINCRBYFLOAT

Increment a field's numeric value.

```bash
HINCRBY key field increment
HINCRBYFLOAT key field increment
```

### HSETNX

Set a field only if it does not exist.

```bash
HSETNX key field value
```

### HRANDFIELD

Return random fields from a hash.

```bash
HRANDFIELD key [count [WITHVALUES]]
```

### HSCAN

Incrementally iterate hash fields.

```bash
HSCAN key cursor [MATCH pattern] [COUNT count]
```

### HSTRLEN

Get the length of a field's value.

```bash
HSTRLEN key field
```
