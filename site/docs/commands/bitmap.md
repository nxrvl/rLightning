# Bitmap Commands

Bitmap commands perform bit-level operations on string values, treating them as arrays of bits.

## Commands

### SETBIT

Set or clear the bit at offset in the string value stored at key.

```bash
SETBIT key offset value
```

- `offset`: Bit position (0-based)
- `value`: 0 or 1
- Returns: The original bit value at the offset

```bash
SETBIT mybitmap 7 1
# (integer) 0
SETBIT mybitmap 7 0
# (integer) 1
```

### GETBIT

Returns the bit value at offset in the string value stored at key.

```bash
GETBIT key offset
```

```bash
SETBIT mybitmap 7 1
GETBIT mybitmap 0
# (integer) 0
GETBIT mybitmap 7
# (integer) 1
```

### BITCOUNT

Count the number of set bits (population counting) in a string.

```bash
BITCOUNT key [start end [BYTE|BIT]]
```

- `start`, `end`: Optional byte or bit range
- `BYTE` (default) or `BIT`: Range unit specifier (Redis 7.0+)

```bash
SET mykey "foobar"
BITCOUNT mykey
# (integer) 26
BITCOUNT mykey 0 0
# (integer) 4
BITCOUNT mykey 1 1
# (integer) 6
BITCOUNT mykey 0 0 BIT
# (integer) 0
```

### BITPOS

Return the position of the first bit set to 1 or 0 in a string.

```bash
BITPOS key bit [start [end [BYTE|BIT]]]
```

```bash
SET mykey "\xff\xf0\x00"
BITPOS mykey 0
# (integer) 12
BITPOS mykey 1
# (integer) 0
```

### BITOP

Perform bitwise operations between strings.

```bash
BITOP operation destkey key [key ...]
```

Operations: `AND`, `OR`, `XOR`, `NOT`

```bash
SET key1 "abc"
SET key2 "def"
BITOP AND destkey key1 key2
BITOP OR destkey key1 key2
BITOP XOR destkey key1 key2
BITOP NOT destkey key1
```

### BITFIELD

Perform arbitrary bitfield integer operations on strings.

```bash
BITFIELD key [GET encoding offset] [SET encoding offset value] [INCRBY encoding offset increment] [OVERFLOW WRAP|SAT|FAIL]
```

Encodings: `i<bits>` (signed), `u<bits>` (unsigned), e.g., `u8`, `i16`

```bash
BITFIELD mykey SET u8 0 200
# 1) (integer) 0
BITFIELD mykey GET u8 0
# 1) (integer) 200
BITFIELD mykey INCRBY u8 0 100 OVERFLOW SAT
# 1) (integer) 255
```

### BITFIELD_RO

Read-only variant of BITFIELD. Only supports GET subcommand.

```bash
BITFIELD_RO key GET encoding offset [GET encoding offset ...]
```

## Use Cases

### User Activity Tracking

```bash
# Mark user 1001 as active on day 0
SETBIT active:2024-01-15 1001 1

# Check if user was active
GETBIT active:2024-01-15 1001

# Count active users
BITCOUNT active:2024-01-15

# Users active on both days
BITOP AND active:both active:2024-01-15 active:2024-01-16
BITCOUNT active:both
```

### Feature Flags

```bash
# Enable feature for user 42
SETBIT feature:dark_mode 42 1

# Check if feature is enabled
GETBIT feature:dark_mode 42
```
