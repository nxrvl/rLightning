# List Commands

List commands operate on ordered collections of strings. Lists support push/pop operations from both ends and blocking variants for queue patterns.

## Basic Operations

### LPUSH / RPUSH

Add elements to the head or tail of a list.

```bash
LPUSH key element [element ...]
RPUSH key element [element ...]
```

### LPUSHX / RPUSHX

Push only if the list already exists.

```bash
LPUSHX key element [element ...]
RPUSHX key element [element ...]
```

### LPOP / RPOP

Remove and return elements from the head or tail.

```bash
LPOP key [count]
RPOP key [count]
```

### LRANGE

Get elements within a range.

```bash
LRANGE key start stop
```

### LLEN

Get the length of a list.

```bash
LLEN key
```

## Manipulation

### LINDEX / LSET

Get or set an element at a specific index.

```bash
LINDEX key index
LSET key index element
```

### LINSERT

Insert an element before or after a pivot element.

```bash
LINSERT key BEFORE|AFTER pivot element
```

### LPOS

Return the index of matching elements.

```bash
LPOS key element [RANK rank] [COUNT count] [MAXLEN len]
```

### LREM

Remove elements equal to a value.

```bash
LREM key count element
```

### LTRIM

Trim a list to the specified range.

```bash
LTRIM key start stop
```

### LMOVE

Atomically move an element between lists.

```bash
LMOVE source destination LEFT|RIGHT LEFT|RIGHT
```

### LMPOP

Pop elements from the first non-empty list.

```bash
LMPOP numkeys key [key ...] LEFT|RIGHT [COUNT count]
```

## Blocking Operations

### BLPOP / BRPOP

Blocking pop from the head or tail with timeout.

```bash
BLPOP key [key ...] timeout
BRPOP key [key ...] timeout
```

### BLMOVE

Blocking variant of LMOVE.

```bash
BLMOVE source destination LEFT|RIGHT LEFT|RIGHT timeout
```

### BLMPOP

Blocking variant of LMPOP.

```bash
BLMPOP timeout numkeys key [key ...] LEFT|RIGHT [COUNT count]
```
