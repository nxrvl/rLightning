# HyperLogLog Commands

HyperLogLog is a probabilistic data structure used for estimating the cardinality (number of unique elements) of a set. It uses very little memory (~12KB per key) regardless of the number of elements.

## Commands

### PFADD

Add elements to a HyperLogLog.

```bash
PFADD key element [element ...]
```

Returns 1 if the internal representation was altered, 0 otherwise.

```bash
PFADD visitors "alice" "bob" "charlie"
# (integer) 1
PFADD visitors "alice"
# (integer) 0
```

### PFCOUNT

Return the approximated cardinality of the set(s).

```bash
PFCOUNT key [key ...]
```

When called with multiple keys, returns the cardinality of the union.

```bash
PFADD hll1 "a" "b" "c"
PFADD hll2 "c" "d" "e"
PFCOUNT hll1
# (integer) 3
PFCOUNT hll1 hll2
# (integer) 5
```

### PFMERGE

Merge multiple HyperLogLog values into a single one.

```bash
PFMERGE destkey sourcekey [sourcekey ...]
```

```bash
PFADD hll1 "a" "b" "c"
PFADD hll2 "c" "d" "e"
PFMERGE merged hll1 hll2
PFCOUNT merged
# (integer) 5
```

## Accuracy

HyperLogLog provides an approximation with a standard error of 0.81%. For most use cases, this level of accuracy is acceptable given the significant memory savings compared to exact counting.

## Use Cases

### Unique Visitor Counting

```bash
# Track unique visitors per page
PFADD page:/home "user:1001"
PFADD page:/home "user:1002"
PFADD page:/home "user:1001"  # duplicate

PFCOUNT page:/home
# (integer) 2

# Daily unique visitors across pages
PFMERGE daily:2024-01-15 page:/home page:/about page:/pricing
PFCOUNT daily:2024-01-15
```

### Unique Event Counting

```bash
# Count unique search queries
PFADD searches:2024-01-15 "redis tutorial" "rust programming"
PFCOUNT searches:2024-01-15
```
