# Scripting Commands

rLightning supports Lua 5.1 scripting for executing complex operations atomically on the server side. Scripts run atomically -- no other commands are executed while a script is running.

## EVAL Commands

### EVAL

Execute a Lua script.

```bash
EVAL script numkeys key [key ...] arg [arg ...]
```

```bash
EVAL "return redis.call('SET', KEYS[1], ARGV[1])" 1 mykey "hello"
# OK

EVAL "return redis.call('GET', KEYS[1])" 1 mykey
# "hello"

# Script with multiple keys and args
EVAL "redis.call('SET', KEYS[1], ARGV[1]); redis.call('SET', KEYS[2], ARGV[2]); return 'OK'" 2 key1 key2 val1 val2
```

### EVALSHA

Execute a cached script by its SHA1 hash.

```bash
EVALSHA sha1 numkeys key [key ...] arg [arg ...]
```

```bash
# First load the script
SCRIPT LOAD "return redis.call('GET', KEYS[1])"
# "a1b2c3d4..."

# Then execute by hash
EVALSHA a1b2c3d4... 1 mykey
```

### EVAL_RO / EVALSHA_RO

Read-only variants that can be executed on replicas.

```bash
EVAL_RO script numkeys key [key ...] arg [arg ...]
EVALSHA_RO sha1 numkeys key [key ...] arg [arg ...]
```

## SCRIPT Commands

### SCRIPT LOAD

Load a script into the script cache without executing it.

```bash
SCRIPT LOAD script
```

Returns the SHA1 hash of the script.

### SCRIPT EXISTS

Check if scripts exist in the cache.

```bash
SCRIPT EXISTS sha1 [sha1 ...]
```

Returns an array of 0/1 values.

### SCRIPT FLUSH

Remove all scripts from the cache.

```bash
SCRIPT FLUSH [ASYNC|SYNC]
```

### SCRIPT KILL

Kill the currently executing script (if it hasn't performed any writes).

```bash
SCRIPT KILL
```

## Redis 7.0 Functions

Functions provide named, reusable Lua code that persists across restarts.

### FUNCTION LOAD

Load a function library.

```bash
FUNCTION LOAD [REPLACE] function-code
```

```bash
FUNCTION LOAD "#!lua name=mylib\nredis.register_function('myfunc', function(keys, args) return redis.call('GET', keys[1]) end)"
```

### FCALL / FCALL_RO

Call a registered function.

```bash
FCALL function numkeys key [key ...] arg [arg ...]
FCALL_RO function numkeys key [key ...] arg [arg ...]
```

```bash
FCALL myfunc 1 mykey
```

### FUNCTION LIST

List loaded function libraries.

```bash
FUNCTION LIST [LIBRARYNAME pattern] [WITHCODE]
```

### FUNCTION DELETE

Delete a function library.

```bash
FUNCTION DELETE library-name
```

### FUNCTION FLUSH

Delete all function libraries.

```bash
FUNCTION FLUSH [ASYNC|SYNC]
```

### FUNCTION DUMP / RESTORE

Serialize and restore function libraries.

```bash
FUNCTION DUMP
FUNCTION RESTORE serialized-value [FLUSH|APPEND|REPLACE]
```

## Lua API

Within scripts, use `redis.call()` and `redis.pcall()` to execute Redis commands:

- `redis.call(command, ...)` -- Execute command, raise error on failure
- `redis.pcall(command, ...)` -- Execute command, return error as table on failure
- `KEYS[n]` -- Access the nth key argument
- `ARGV[n]` -- Access the nth non-key argument

## Use Cases

### Atomic Compare-and-Set

```bash
EVAL "if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('SET', KEYS[1], ARGV[2]) else return 0 end" 1 mykey "old_value" "new_value"
```

### Rate Limiter

```bash
EVAL "local current = redis.call('INCR', KEYS[1]); if current == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end; return current" 1 ratelimit:user:1001 60
```
