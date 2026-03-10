---
title: "Bitmap"
description: "Bitmap command reference"
order: 8
category: "commands"
---

# Bitmap Commands

Bitmaps are not a separate data type but a set of bit-oriented operations on string values. They allow setting, getting, and counting individual bits, enabling space-efficient storage of boolean flags.

## SETBIT

Synopsis: `SETBIT key offset value`

Set or clear the bit at the given offset in the string value stored at key. The offset is zero-based. Returns the original bit value at that offset.

```bash
> SETBIT mybitmap 7 1
(integer) 0
> SETBIT mybitmap 7 0
(integer) 1
```

## GETBIT

Synopsis: `GETBIT key offset`

Return the bit value at the given offset. Returns 0 if the key does not exist or the offset is beyond the string length.

```bash
> SETBIT mybitmap 7 1
(integer) 0
> GETBIT mybitmap 7
(integer) 1
> GETBIT mybitmap 100
(integer) 0
```

## BITCOUNT

Synopsis: `BITCOUNT key [start end [BYTE | BIT]]`

Count the number of set bits (1s) in a string. Without range arguments, counts all bits. With start/end, counts bits in the specified range.

- `BYTE` (default) -- Interpret start/end as byte offsets.
- `BIT` -- Interpret start/end as bit offsets (Redis 7.0+).

```bash
> SET mykey "foobar"
OK
> BITCOUNT mykey
(integer) 26
> BITCOUNT mykey 0 0
(integer) 4
> BITCOUNT mykey 0 0 BIT
(integer) 0
```

## BITOP

Synopsis: `BITOP AND | OR | XOR | NOT destkey key [key ...]`

Perform a bitwise operation between strings and store the result in `destkey`. NOT takes exactly one source key. Returns the size of the resulting string in bytes.

```bash
> SET key1 "abc"
OK
> SET key2 "abd"
OK
> BITOP AND result key1 key2
(integer) 3
> BITOP NOT inverted key1
(integer) 3
```

## BITPOS

Synopsis: `BITPOS key bit [start [end [BYTE | BIT]]]`

Find the position of the first bit set to 0 or 1 in a string. Returns -1 if the bit is not found within the range.

```bash
> SET mykey "\xff\xf0\x00"
OK
> BITPOS mykey 0
(integer) 12
> BITPOS mykey 1
(integer) 0
> BITPOS mykey 1 2
(integer) -1
```

## BITFIELD

Synopsis: `BITFIELD key [GET encoding offset | SET encoding offset value | INCRBY encoding offset increment | OVERFLOW WRAP | SAT | FAIL] ...`

Perform arbitrary bitfield integer operations on strings. Supports multiple sub-commands in a single call.

- **Encodings**: `i<bits>` for signed, `u<bits>` for unsigned (e.g., `u8`, `i16`).
- **OVERFLOW**: Controls overflow behavior for SET and INCRBY.

```bash
> BITFIELD mykey SET u8 0 200
1) (integer) 0
> BITFIELD mykey GET u8 0
1) (integer) 200
> BITFIELD mykey INCRBY u8 0 100 OVERFLOW SAT
1) (integer) 255
```

## BITFIELD_RO

Synopsis: `BITFIELD_RO key [GET encoding offset ...]`

A read-only variant of BITFIELD that only supports GET sub-commands. Safe to use on replicas.

```bash
> BITFIELD mykey SET u8 0 42
1) (integer) 0
> BITFIELD_RO mykey GET u8 0
1) (integer) 42
```
