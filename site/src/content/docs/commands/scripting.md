---
title: "Scripting"
description: "Lua scripting command reference"
order: 12
category: "commands"
---

# Scripting Commands

rLightning supports server-side Lua 5.1 scripting for atomic multi-command operations. Scripts execute atomically -- no other commands run while a script is executing. rLightning supports both legacy EVAL scripts and Redis 7.0 Functions.

## redis.call() and redis.pcall()

Inside Lua scripts, use `redis.call()` to execute Redis commands. If a command fails, `redis.call()` raises a Lua error that aborts the script and returns the error to the client.

Use `redis.pcall()` for protected calls -- errors are caught and returned as Lua tables with an `err` field, allowing the script to handle them.

```lua
-- redis.call() -- errors abort the script
local val = redis.call('GET', KEYS[1])

-- redis.pcall() -- errors are caught
local ok, err = pcall(function()
    return redis.call('INCR', KEYS[1])
end)
```

## EVAL

Synopsis: `EVAL script numkeys [key [key ...]] [arg [arg ...]]`

Execute a Lua script. Keys should be passed via `KEYS` and arguments via `ARGV` to ensure compatibility with cluster mode.

```bash
> EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 mykey "hello"
OK
> EVAL "return redis.call('GET', KEYS[1])" 1 mykey
"hello"
> EVAL "return {KEYS[1], ARGV[1], ARGV[2]}" 1 key1 "arg1" "arg2"
1) "key1"
2) "arg1"
3) "arg2"
```

## EVALSHA

Synopsis: `EVALSHA sha1 numkeys [key [key ...]] [arg [arg ...]]`

Execute a cached Lua script by its SHA1 hash. The script must have been previously loaded with SCRIPT LOAD or executed with EVAL.

```bash
> SCRIPT LOAD "return redis.call('GET', KEYS[1])"
"a42059b356c875f0717db19a51f6aaa9161571a2"
> EVALSHA "a42059b356c875f0717db19a51f6aaa9161571a2" 1 mykey
"hello"
```

## EVALRO

Synopsis: `EVALRO script numkeys [key [key ...]] [arg [arg ...]]`

Execute a read-only Lua script. The script is not allowed to call write commands. Safe for use on replicas.

```bash
> EVALRO "return redis.call('GET', KEYS[1])" 1 mykey
"hello"
```

## EVALSHA_RO

Synopsis: `EVALSHA_RO sha1 numkeys [key [key ...]] [arg [arg ...]]`

Execute a cached read-only Lua script by its SHA1 hash.

```bash
> EVALSHA_RO "a42059b356c875f0717db19a51f6aaa9161571a2" 1 mykey
"hello"
```

## SCRIPT LOAD

Synopsis: `SCRIPT LOAD script`

Load a Lua script into the script cache without executing it. Returns the SHA1 hash of the script.

```bash
> SCRIPT LOAD "return 'hello'"
"2067d915024a3e1657c4169c84f809f8ec823687"
```

## SCRIPT EXISTS

Synopsis: `SCRIPT EXISTS sha1 [sha1 ...]`

Check if one or more scripts exist in the script cache. Returns an array of 0/1 values.

```bash
> SCRIPT EXISTS "a42059b356c875f0717db19a51f6aaa9161571a2" "0000000000000000000000000000000000000000"
1) (integer) 1
2) (integer) 0
```

## SCRIPT FLUSH

Synopsis: `SCRIPT FLUSH [ASYNC | SYNC]`

Flush the script cache, removing all cached scripts.

```bash
> SCRIPT FLUSH
OK
```

## Redis 7.0 Functions

Functions provide a more structured and persistent alternative to EVAL scripts. Functions are named, organized into libraries, and persist across restarts.

### FUNCTION LOAD

Synopsis: `FUNCTION LOAD [REPLACE] function-code`

Load a Lua function library. The library code must use `redis.register_function()` to define functions. Use `REPLACE` to overwrite an existing library.

```bash
> FUNCTION LOAD "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return redis.call('GET', keys[1]) end)"
"mylib"
```

### FUNCTION LIST

Synopsis: `FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]`

List all registered function libraries with their functions.

```bash
> FUNCTION LIST
1) 1) "library_name"
   2) "mylib"
   3) "functions"
   4) 1) 1) "name"
         2) "myfunc"
         ...
```

### FUNCTION DELETE

Synopsis: `FUNCTION DELETE library-name`

Delete a function library and all its functions.

```bash
> FUNCTION DELETE mylib
OK
```

### FUNCTION DUMP

Synopsis: `FUNCTION DUMP`

Return a serialized payload of all loaded function libraries. Used for replication and backup.

```bash
> FUNCTION DUMP
"\x00\x..."
```

### FUNCTION RESTORE

Synopsis: `FUNCTION RESTORE serialized-value [FLUSH | APPEND | REPLACE]`

Restore function libraries from a serialized payload produced by FUNCTION DUMP.

```bash
> FUNCTION RESTORE "\x00\x..." REPLACE
OK
```

## FCALL

Synopsis: `FCALL function numkeys [key [key ...]] [arg [arg ...]]`

Call a previously loaded function by name.

```bash
> FCALL myfunc 1 mykey
"hello"
```

## FCALL_RO

Synopsis: `FCALL_RO function numkeys [key [key ...]] [arg [arg ...]]`

Call a read-only function. The function must not perform write operations. Safe for use on replicas.

```bash
> FCALL_RO myfunc 1 mykey
"hello"
```
