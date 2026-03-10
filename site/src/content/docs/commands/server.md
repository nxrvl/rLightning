---
title: "Server"
description: "Server command reference"
order: 17
category: "commands"
---

# Server Commands

Server commands handle connection management, database operations, key management, configuration, introspection, and persistence. This page covers the full set of general-purpose commands.

---

## Connection & General

### PING

Synopsis: `PING [message]`

Test the connection. Returns `PONG` without arguments, or echoes the given message.

```bash
> PING
PONG
> PING "hello"
"hello"
```

### ECHO

Synopsis: `ECHO message`

Return the given message. Useful for testing.

```bash
> ECHO "Hello, rLightning!"
"Hello, rLightning!"
```

### HELLO

Synopsis: `HELLO [protover [AUTH username password] [SETNAME clientname]]`

Switch the connection to RESP2 or RESP3 protocol, optionally authenticating and setting the client name in a single command.

```bash
> HELLO 3
1) "server"
2) "rlightning"
3) "version"
4) "7.0.0"
5) "proto"
6) (integer) 3
...
```

### QUIT

Synopsis: `QUIT`

Close the connection.

```bash
> QUIT
OK
```

### RESET

Synopsis: `RESET`

Reset the connection state: exit any MULTI/SUBSCRIBE mode, unwatch keys, select DB 0, and clear the client name.

```bash
> RESET
+RESET
```

### SELECT

Synopsis: `SELECT index`

Switch to a different database. rLightning supports 16 databases (0-15) by default.

```bash
> SELECT 1
OK
```

---

## Key Management

### KEYS

Synopsis: `KEYS pattern`

Return all keys matching a glob-style pattern. Use with caution in production -- consider SCAN instead.

```bash
> SET user:1 "Alice"
OK
> SET user:2 "Bob"
OK
> KEYS user:*
1) "user:1"
2) "user:2"
```

### SCAN

Synopsis: `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]`

Incrementally iterate over keys. Returns a cursor for the next call and an array of keys.

```bash
> SCAN 0 MATCH user:* COUNT 10
1) "0"
2) 1) "user:1"
   2) "user:2"
```

### EXISTS

Synopsis: `EXISTS key [key ...]`

Check if one or more keys exist. Returns the count of existing keys.

```bash
> SET key1 "hello"
OK
> EXISTS key1 key2
(integer) 1
```

### TYPE

Synopsis: `TYPE key`

Return the type of value stored at key (string, list, set, zset, hash, stream).

```bash
> SET mykey "hello"
OK
> TYPE mykey
string
```

### RENAME

Synopsis: `RENAME key newkey`

Rename a key. Overwrites `newkey` if it already exists.

```bash
> SET mykey "hello"
OK
> RENAME mykey newkey
OK
```

### RENAMENX

Synopsis: `RENAMENX key newkey`

Rename a key only if the new key does not already exist. Returns 1 if renamed, 0 otherwise.

```bash
> RENAMENX mykey newkey
(integer) 1
```

### DEL

Synopsis: `DEL key [key ...]`

Delete one or more keys. Returns the number of keys that were removed.

```bash
> SET key1 "a"
OK
> SET key2 "b"
OK
> DEL key1 key2 key3
(integer) 2
```

### UNLINK

Synopsis: `UNLINK key [key ...]`

Delete one or more keys asynchronously. Like DEL but the memory reclamation happens in the background.

```bash
> UNLINK key1 key2
(integer) 2
```

### COPY

Synopsis: `COPY source destination [DB destination-db] [REPLACE]`

Copy the value stored at the source key to the destination key. With `REPLACE`, overwrites the destination if it exists.

```bash
> SET src "hello"
OK
> COPY src dst
(integer) 1
> GET dst
"hello"
```

### RANDOMKEY

Synopsis: `RANDOMKEY`

Return a random key from the current database.

```bash
> RANDOMKEY
"user:42"
```

---

## TTL & Expiration

### EXPIRE

Synopsis: `EXPIRE key seconds [NX | XX | GT | LT]`

Set a timeout on a key in seconds. Returns 1 if the timeout was set, 0 otherwise.

```bash
> SET mykey "hello"
OK
> EXPIRE mykey 60
(integer) 1
```

### EXPIREAT

Synopsis: `EXPIREAT key unix-time-seconds [NX | XX | GT | LT]`

Set the expiration as a Unix timestamp in seconds.

```bash
> EXPIREAT mykey 1893456000
(integer) 1
```

### EXPIRETIME

Synopsis: `EXPIRETIME key`

Return the absolute Unix timestamp (seconds) at which the key will expire. Returns -1 if the key has no TTL, -2 if the key does not exist.

```bash
> EXPIRETIME mykey
(integer) 1893456000
```

### PEXPIRE

Synopsis: `PEXPIRE key milliseconds [NX | XX | GT | LT]`

Set a timeout on a key in milliseconds.

```bash
> PEXPIRE mykey 60000
(integer) 1
```

### PEXPIREAT

Synopsis: `PEXPIREAT key unix-time-milliseconds [NX | XX | GT | LT]`

Set the expiration as a Unix timestamp in milliseconds.

```bash
> PEXPIREAT mykey 1893456000000
(integer) 1
```

### PEXPIRETIME

Synopsis: `PEXPIRETIME key`

Return the absolute Unix timestamp (milliseconds) at which the key will expire.

```bash
> PEXPIRETIME mykey
(integer) 1893456000000
```

### TTL

Synopsis: `TTL key`

Return the remaining time to live of a key in seconds. Returns -1 if the key has no TTL, -2 if the key does not exist.

```bash
> SET mykey "hello"
OK
> EXPIRE mykey 60
(integer) 1
> TTL mykey
(integer) 59
```

### PTTL

Synopsis: `PTTL key`

Return the remaining time to live of a key in milliseconds.

```bash
> PTTL mykey
(integer) 59432
```

### PERSIST

Synopsis: `PERSIST key`

Remove the expiration from a key, making it persistent. Returns 1 if the timeout was removed, 0 if the key has no TTL.

```bash
> EXPIRE mykey 60
(integer) 1
> PERSIST mykey
(integer) 1
> TTL mykey
(integer) -1
```

---

## Serialization

### DUMP

Synopsis: `DUMP key`

Return a serialized version of the value stored at key. The format is opaque and can be restored with RESTORE.

```bash
> SET mykey "hello"
OK
> DUMP mykey
"\x00\x05hello\n\x00..."
```

### RESTORE

Synopsis: `RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]`

Create a key from a serialized value produced by DUMP. TTL of 0 means no expiration.

```bash
> RESTORE newkey 0 "\x00\x05hello\n\x00..."
OK
```

---

## Sorting

### SORT

Synopsis: `SORT key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA] [STORE destination]`

Sort the elements in a list, set, or sorted set. With `STORE`, saves the result to a destination key.

```bash
> RPUSH mylist 3 1 2
(integer) 3
> SORT mylist
1) "1"
2) "2"
3) "3"
> SORT mylist DESC ALPHA
1) "3"
2) "2"
3) "1"
```

### SORT_RO

Synopsis: `SORT_RO key [BY pattern] [LIMIT offset count] [GET pattern [GET pattern ...]] [ASC | DESC] [ALPHA]`

Read-only variant of SORT (no STORE option). Safe for use on replicas.

```bash
> SORT_RO mylist ASC
1) "1"
2) "2"
3) "3"
```

---

## Object Introspection

### OBJECT ENCODING

Synopsis: `OBJECT ENCODING key`

Return the internal encoding used for the value stored at key.

```bash
> SET mykey "hello"
OK
> OBJECT ENCODING mykey
"embstr"
> SET mykey 12345
OK
> OBJECT ENCODING mykey
"int"
```

### OBJECT REFCOUNT

Synopsis: `OBJECT REFCOUNT key`

Return the reference count of the object stored at key.

```bash
> OBJECT REFCOUNT mykey
(integer) 1
```

### OBJECT IDLETIME

Synopsis: `OBJECT IDLETIME key`

Return the idle time (seconds since last access) of the object stored at key.

```bash
> OBJECT IDLETIME mykey
(integer) 5
```

### OBJECT FREQ

Synopsis: `OBJECT FREQ key`

Return the access frequency of the object stored at key. Only available when the LFU eviction policy is active.

```bash
> OBJECT FREQ mykey
(integer) 10
```

### OBJECT HELP

Synopsis: `OBJECT HELP`

Return help text describing the available OBJECT sub-commands.

```bash
> OBJECT HELP
1) "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:"
2) "ENCODING <key>"
3) "FREQ <key>"
4) "HELP"
5) "IDLETIME <key>"
6) "REFCOUNT <key>"
```

---

## Database Operations

### DBSIZE

Synopsis: `DBSIZE`

Return the number of keys in the current database.

```bash
> DBSIZE
(integer) 42
```

### FLUSHDB

Synopsis: `FLUSHDB [ASYNC | SYNC]`

Delete all keys in the current database.

```bash
> FLUSHDB
OK
```

### FLUSHALL

Synopsis: `FLUSHALL [ASYNC | SYNC]`

Delete all keys in all databases.

```bash
> FLUSHALL
OK
```

### SWAPDB

Synopsis: `SWAPDB index1 index2`

Swap two databases atomically. All clients connected to either database will see the other database's data.

```bash
> SWAPDB 0 1
OK
```

---

## Persistence

### SAVE

Synopsis: `SAVE`

Perform a synchronous RDB save. Blocks the server until complete.

```bash
> SAVE
OK
```

### BGSAVE

Synopsis: `BGSAVE [SCHEDULE]`

Perform a background RDB save. Returns immediately.

```bash
> BGSAVE
Background saving started
```

### BGREWRITEAOF

Synopsis: `BGREWRITEAOF`

Trigger a background AOF rewrite.

```bash
> BGREWRITEAOF
Background append only file rewriting started
```

### LASTSAVE

Synopsis: `LASTSAVE`

Return the Unix timestamp of the last successful RDB save.

```bash
> LASTSAVE
(integer) 1678886400
```

---

## Server Information

### INFO

Synopsis: `INFO [section [section ...]]`

Return information and statistics about the server. Sections include: server, clients, memory, persistence, stats, replication, cpu, modules, keyspace, and all.

```bash
> INFO server
# Server
redis_version:7.0.0
...
> INFO memory keyspace
# Memory
used_memory:1048576
...
# Keyspace
db0:keys=42,expires=10,avg_ttl=30000
```

### TIME

Synopsis: `TIME`

Return the current server time as a two-element array: Unix timestamp in seconds and microseconds.

```bash
> TIME
1) "1678886400"
2) "123456"
```

### LOLWUT

Synopsis: `LOLWUT [VERSION version]`

Display the rLightning version in a fun way.

```bash
> LOLWUT
rLightning ver. 1.0.0
...
```

---

## Configuration

### CONFIG GET

Synopsis: `CONFIG GET parameter [parameter ...]`

Get the value of one or more configuration parameters. Supports glob-style patterns.

```bash
> CONFIG GET maxmemory
1) "maxmemory"
2) "0"
> CONFIG GET max*
1) "maxmemory"
2) "0"
3) "maxmemory-policy"
4) "noeviction"
```

### CONFIG SET

Synopsis: `CONFIG SET parameter value [parameter value ...]`

Set one or more configuration parameters at runtime.

```bash
> CONFIG SET maxmemory 256mb
OK
> CONFIG SET maxmemory-policy allkeys-lru
OK
```

### CONFIG RESETSTAT

Synopsis: `CONFIG RESETSTAT`

Reset the statistics reported by INFO (e.g., keyspace hits/misses, total commands processed).

```bash
> CONFIG RESETSTAT
OK
```

### CONFIG REWRITE

Synopsis: `CONFIG REWRITE`

Rewrite the configuration file to reflect the current in-memory configuration.

```bash
> CONFIG REWRITE
OK
```

---

## Command Introspection

### COMMAND COUNT

Synopsis: `COMMAND COUNT`

Return the total number of commands supported by the server.

```bash
> COMMAND COUNT
(integer) 400
```

### COMMAND DOCS

Synopsis: `COMMAND DOCS [command-name [command-name ...]]`

Return documentation for the specified commands.

```bash
> COMMAND DOCS GET
1) "GET"
2) 1) "summary"
   2) "Get the value of a key"
   ...
```

### COMMAND INFO

Synopsis: `COMMAND INFO [command-name [command-name ...]]`

Return detailed information about the specified commands including arity, flags, and key positions.

```bash
> COMMAND INFO GET SET
1) 1) "get"
   2) (integer) 2
   3) 1) "readonly"
      2) "fast"
   ...
```

### COMMAND LIST

Synopsis: `COMMAND LIST [FILTERBY MODULE module-name | ACLCAT category | PATTERN pattern]`

List all commands, optionally filtered by module, ACL category, or pattern.

```bash
> COMMAND LIST FILTERBY ACLCAT string
1) "append"
2) "get"
3) "set"
...
```

### COMMAND GETKEYS

Synopsis: `COMMAND GETKEYS command [arg ...]`

Extract the keys from a command without executing it.

```bash
> COMMAND GETKEYS SET mykey myvalue
1) "mykey"
> COMMAND GETKEYS MSET k1 v1 k2 v2
1) "k1"
2) "k2"
```

---

## Client Management

### CLIENT LIST

Synopsis: `CLIENT LIST [TYPE NORMAL | MASTER | REPLICA | PUBSUB] [ID client-id [client-id ...]]`

Return information about connected clients.

```bash
> CLIENT LIST
id=1 addr=127.0.0.1:54321 fd=5 name= db=0 ...
```

### CLIENT GETNAME

Synopsis: `CLIENT GETNAME`

Return the name of the current connection, or nil if no name is set.

```bash
> CLIENT GETNAME
(nil)
```

### CLIENT SETNAME

Synopsis: `CLIENT SETNAME connection-name`

Set the name of the current connection for identification in CLIENT LIST.

```bash
> CLIENT SETNAME myapp-worker-1
OK
```

### CLIENT ID

Synopsis: `CLIENT ID`

Return the unique ID of the current connection.

```bash
> CLIENT ID
(integer) 42
```

### CLIENT INFO

Synopsis: `CLIENT INFO`

Return information about the current client connection.

```bash
> CLIENT INFO
id=42 addr=127.0.0.1:54321 ...
```

### CLIENT KILL

Synopsis: `CLIENT KILL [ID client-id | ADDR ip:port | LADDR ip:port | USER username | SKIPME yes | no | MAXAGE seconds]`

Close client connections matching the specified filters. Returns the number of clients killed.

```bash
> CLIENT KILL ID 42
(integer) 1
```

### CLIENT NO-EVICT

Synopsis: `CLIENT NO-EVICT ON | OFF`

Set the client eviction mode. When ON, the client will not be evicted even under memory pressure.

```bash
> CLIENT NO-EVICT ON
OK
```

---

## Monitoring & Debugging

### SLOWLOG

Synopsis: `SLOWLOG GET [count] | SLOWLOG LEN | SLOWLOG RESET`

Manage the slow queries log.

```bash
> SLOWLOG GET 2
1) 1) (integer) 1
   2) (integer) 1678886400
   3) (integer) 15000
   4) 1) "SORT"
      2) "mylist"
> SLOWLOG LEN
(integer) 5
> SLOWLOG RESET
OK
```

### DEBUG

Synopsis: `DEBUG subcommand [argument]`

Server debugging commands. Not intended for production use.

```bash
> DEBUG SLEEP 0.5
OK
> DEBUG SET-ACTIVE-EXPIRE 0
OK
```

### LATENCY

Synopsis: `LATENCY LATEST | LATENCY HISTORY event | LATENCY RESET [event ...]`

Monitor and analyze server latency events.

```bash
> LATENCY LATEST
(empty array)
```

---

## Memory

### MEMORY USAGE

Synopsis: `MEMORY USAGE key [SAMPLES count]`

Return the memory usage in bytes of a key and its value.

```bash
> SET mykey "hello"
OK
> MEMORY USAGE mykey
(integer) 56
```

### MEMORY STATS

Synopsis: `MEMORY STATS`

Return detailed memory consumption statistics.

```bash
> MEMORY STATS
 1) "peak.allocated"
 2) (integer) 1048576
 3) "total.allocated"
 4) (integer) 524288
 ...
```

### MEMORY DOCTOR

Synopsis: `MEMORY DOCTOR`

Return memory diagnostic advice.

```bash
> MEMORY DOCTOR
"Sam, I have no memory problems"
```

### MEMORY MALLOC-STATS

Synopsis: `MEMORY MALLOC-STATS`

Return the memory allocator's internal statistics.

```bash
> MEMORY MALLOC-STATS
(allocator stats output)
```

---

## Replication & Sync

### WAIT

Synopsis: `WAIT numreplicas timeout`

Block until all preceding write commands are acknowledged by at least `numreplicas` replicas, or the timeout (ms) expires. Returns the number of replicas that acknowledged.

```bash
> SET mykey "hello"
OK
> WAIT 1 5000
(integer) 1
```

---

## Lifecycle

### SHUTDOWN

Synopsis: `SHUTDOWN [NOSAVE | SAVE] [NOW] [FORCE]`

Shut down the server. With `SAVE`, triggers an RDB save before shutting down. With `NOSAVE`, shuts down without saving.

```bash
> SHUTDOWN SAVE
(server exits)
```

---

## Module System

### MODULE LIST

Synopsis: `MODULE LIST`

List all loaded modules.

```bash
> MODULE LIST
(empty array)
```

### MODULE LOAD

Synopsis: `MODULE LOAD path [arg ...]`

Load a module from the given path.

```bash
> MODULE LOAD /path/to/module.so
OK
```

### MODULE UNLOAD

Synopsis: `MODULE UNLOAD name`

Unload a module by name.

```bash
> MODULE UNLOAD mymodule
OK
```
