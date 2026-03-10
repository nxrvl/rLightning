---
title: "Lists"
description: "List command reference"
order: 4
category: "commands"
---

# List Commands

Lists are ordered sequences of strings. Elements can be pushed and popped from both ends, making them suitable for queues, stacks, and more.

## LPUSH

Synopsis: `LPUSH key element [element ...]`

Insert one or more elements at the head (left) of a list. Creates the list if it does not exist. Returns the length of the list after the operation.

```bash
> LPUSH mylist "world" "hello"
(integer) 2
> LRANGE mylist 0 -1
1) "hello"
2) "world"
```

## RPUSH

Synopsis: `RPUSH key element [element ...]`

Insert one or more elements at the tail (right) of a list. Returns the length of the list after the operation.

```bash
> RPUSH mylist "hello" "world"
(integer) 2
> LRANGE mylist 0 -1
1) "hello"
2) "world"
```

## LPOP

Synopsis: `LPOP key [count]`

Remove and return one or more elements from the head of a list. Without `count`, returns a single element. With `count`, returns up to `count` elements as an array.

```bash
> RPUSH mylist "a" "b" "c"
(integer) 3
> LPOP mylist
"a"
> LPOP mylist 2
1) "b"
2) "c"
```

## RPOP

Synopsis: `RPOP key [count]`

Remove and return one or more elements from the tail of a list.

```bash
> RPUSH mylist "a" "b" "c"
(integer) 3
> RPOP mylist
"c"
> RPOP mylist 2
1) "b"
2) "a"
```

## LLEN

Synopsis: `LLEN key`

Return the length of a list. Returns 0 if the key does not exist.

```bash
> RPUSH mylist "a" "b" "c"
(integer) 3
> LLEN mylist
(integer) 3
```

## LRANGE

Synopsis: `LRANGE key start stop`

Return a range of elements from a list. Both start and stop are zero-based, inclusive. Negative indices count from the end.

```bash
> RPUSH mylist "a" "b" "c" "d"
(integer) 4
> LRANGE mylist 0 -1
1) "a"
2) "b"
3) "c"
4) "d"
> LRANGE mylist 1 2
1) "b"
2) "c"
```

## LINDEX

Synopsis: `LINDEX key index`

Return the element at the specified index in a list. Negative indices count from the end.

```bash
> RPUSH mylist "a" "b" "c"
(integer) 3
> LINDEX mylist 0
"a"
> LINDEX mylist -1
"c"
```

## LSET

Synopsis: `LSET key index element`

Set the element at the specified index in a list. An error is returned for out-of-range indices.

```bash
> RPUSH mylist "a" "b" "c"
(integer) 3
> LSET mylist 1 "B"
OK
> LRANGE mylist 0 -1
1) "a"
2) "B"
3) "c"
```

## LINSERT

Synopsis: `LINSERT key BEFORE | AFTER pivot element`

Insert an element before or after the pivot element in a list. Returns the length of the list after the insert, or -1 if the pivot was not found.

```bash
> RPUSH mylist "a" "c"
(integer) 2
> LINSERT mylist BEFORE "c" "b"
(integer) 3
> LRANGE mylist 0 -1
1) "a"
2) "b"
3) "c"
```

## LREM

Synopsis: `LREM key count element`

Remove elements from a list. `count > 0`: remove from head. `count < 0`: remove from tail. `count = 0`: remove all. Returns the number of removed elements.

```bash
> RPUSH mylist "a" "b" "a" "c" "a"
(integer) 5
> LREM mylist 2 "a"
(integer) 2
> LRANGE mylist 0 -1
1) "b"
2) "c"
3) "a"
```

## LTRIM

Synopsis: `LTRIM key start stop`

Trim a list to the specified range. Elements outside the range are removed.

```bash
> RPUSH mylist "a" "b" "c" "d" "e"
(integer) 5
> LTRIM mylist 1 3
OK
> LRANGE mylist 0 -1
1) "b"
2) "c"
3) "d"
```

## RPOPLPUSH

Synopsis: `RPOPLPUSH source destination`

Atomically pop from the tail of one list and push to the head of another. Deprecated in Redis 6.2; use LMOVE instead.

```bash
> RPUSH src "a" "b" "c"
(integer) 3
> RPOPLPUSH src dst
"c"
> LRANGE dst 0 -1
1) "c"
```

## LMOVE

Synopsis: `LMOVE source destination LEFT | RIGHT LEFT | RIGHT`

Atomically pop an element from one end of a list and push it to one end of another list. Replaces RPOPLPUSH with more flexibility.

```bash
> RPUSH src "a" "b" "c"
(integer) 3
> LMOVE src dst LEFT RIGHT
"a"
> LRANGE dst 0 -1
1) "a"
```

## LPOS

Synopsis: `LPOS key element [RANK rank] [COUNT count] [MAXLEN maxlen]`

Return the index of matching elements in a list.

- `RANK`: skip the first N matches (negative to search from tail).
- `COUNT`: return up to N matches (0 = all).
- `MAXLEN`: limit the comparison to the first N elements.

```bash
> RPUSH mylist "a" "b" "c" "b" "d"
(integer) 5
> LPOS mylist "b"
(integer) 1
> LPOS mylist "b" COUNT 0
1) (integer) 1
2) (integer) 3
```

## LMPOP

Synopsis: `LMPOP numkeys key [key ...] LEFT | RIGHT [COUNT count]`

Pop one or more elements from the first non-empty list among the specified keys. Returns a two-element array: the key name and the popped elements.

```bash
> RPUSH list1 "a" "b" "c"
(integer) 3
> LMPOP 1 list1 LEFT COUNT 2
1) "list1"
2) 1) "a"
   2) "b"
```

## Blocking Commands

The following commands block the connection until an element becomes available or a timeout is reached.

### BLPOP

Synopsis: `BLPOP key [key ...] timeout`

Blocking version of LPOP. Pops an element from the head of the first non-empty list, or blocks until one is available. Timeout is in seconds (0 = block indefinitely).

```bash
> BLPOP mylist 5
(blocks until element available or timeout)
```

### BRPOP

Synopsis: `BRPOP key [key ...] timeout`

Blocking version of RPOP. Pops an element from the tail of the first non-empty list, or blocks until one is available.

```bash
> BRPOP mylist 5
(blocks until element available or timeout)
```

### BLMOVE

Synopsis: `BLMOVE source destination LEFT | RIGHT LEFT | RIGHT timeout`

Blocking version of LMOVE. Blocks until an element is available in the source list.

```bash
> BLMOVE src dst LEFT RIGHT 5
(blocks until element available or timeout)
```

### BLMPOP

Synopsis: `BLMPOP timeout numkeys key [key ...] LEFT | RIGHT [COUNT count]`

Blocking version of LMPOP. Blocks until an element is available in one of the specified lists.

```bash
> BLMPOP 5 1 mylist LEFT
(blocks until element available or timeout)
```
