# Set Commands

Set commands operate on unordered collections of unique strings. Sets support membership testing, intersection, union, and difference operations.

## Basic Operations

### SADD / SREM

Add or remove members.

```bash
SADD key member [member ...]
SREM key member [member ...]
```

### SMEMBERS

Get all members.

```bash
SMEMBERS key
```

### SISMEMBER / SMISMEMBER

Check membership for one or multiple members.

```bash
SISMEMBER key member
SMISMEMBER key member [member ...]
```

### SCARD

Get the number of members.

```bash
SCARD key
```

### SRANDMEMBER

Return random members.

```bash
SRANDMEMBER key [count]
```

### SPOP

Remove and return random members.

```bash
SPOP key [count]
```

## Set Operations

### SINTER / SINTERCARD / SINTERSTORE

Intersection operations.

```bash
SINTER key [key ...]
SINTERCARD numkeys key [key ...] [LIMIT limit]
SINTERSTORE destination key [key ...]
```

### SUNION / SUNIONSTORE

Union operations.

```bash
SUNION key [key ...]
SUNIONSTORE destination key [key ...]
```

### SDIFF / SDIFFSTORE

Difference operations.

```bash
SDIFF key [key ...]
SDIFFSTORE destination key [key ...]
```

### SMOVE

Move a member from one set to another.

```bash
SMOVE source destination member
```

### SSCAN

Incrementally iterate set members.

```bash
SSCAN key cursor [MATCH pattern] [COUNT count]
```
