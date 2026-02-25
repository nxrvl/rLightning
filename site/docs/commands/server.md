# Server Commands

Server commands manage the rLightning instance, connections, configuration, and diagnostics.

## Connection

### PING / ECHO

```bash
PING [message]
ECHO message
```

### QUIT / RESET

```bash
QUIT
RESET
```

### SELECT

Switch to a database.

```bash
SELECT index
```

### HELLO

Negotiate protocol version (RESP2/RESP3).

```bash
HELLO [protover [AUTH username password] [SETNAME clientname]]
```

## Client Management

### CLIENT LIST / INFO / ID

```bash
CLIENT LIST [TYPE normal|master|replica|pubsub]
CLIENT INFO
CLIENT ID
```

### CLIENT SETNAME / GETNAME

```bash
CLIENT SETNAME connection-name
CLIENT GETNAME
```

### CLIENT KILL / PAUSE / UNPAUSE

```bash
CLIENT KILL [ID client-id] [TYPE normal|master|replica|pubsub] [ADDR ip:port]
CLIENT PAUSE timeout [WRITE|ALL]
CLIENT UNPAUSE
```

### CLIENT TRACKING

Enable server-assisted client-side caching.

```bash
CLIENT TRACKING ON|OFF [REDIRECT client-id] [PREFIX prefix ...] [BCAST] [OPTIN] [OPTOUT] [NOLOOP]
```

## Configuration

### CONFIG GET / SET / REWRITE / RESETSTAT

```bash
CONFIG GET parameter [parameter ...]
CONFIG SET parameter value [parameter value ...]
CONFIG REWRITE
CONFIG RESETSTAT
```

## Information

### INFO

Return server information and statistics.

```bash
INFO [section ...]
```

Sections: server, clients, memory, persistence, stats, replication, cpu, modules, keyspace, cluster, all, everything

### COMMAND COUNT / INFO / DOCS / LIST / GETKEYS

```bash
COMMAND COUNT
COMMAND INFO [command-name ...]
COMMAND DOCS [command-name ...]
COMMAND LIST [FILTERBY MODULE module | ACLCAT category | PATTERN pattern]
COMMAND GETKEYS command [arg ...]
```

### DBSIZE / TIME / LOLWUT

```bash
DBSIZE
TIME
LOLWUT [VERSION version]
```

## Persistence

### SAVE / BGSAVE / BGREWRITEAOF / LASTSAVE

```bash
SAVE
BGSAVE [SCHEDULE]
BGREWRITEAOF
LASTSAVE
```

## Memory

### MEMORY USAGE / STATS / DOCTOR / MALLOC-STATS / PURGE

```bash
MEMORY USAGE key [SAMPLES count]
MEMORY STATS
MEMORY DOCTOR
MEMORY MALLOC-STATS
MEMORY PURGE
```

## Diagnostics

### SLOWLOG

```bash
SLOWLOG GET [count]
SLOWLOG LEN
SLOWLOG RESET
```

### LATENCY

```bash
LATENCY LATEST
LATENCY HISTORY event
LATENCY RESET [event ...]
LATENCY GRAPH event
```

### DEBUG

```bash
DEBUG SLEEP seconds
DEBUG SET-ACTIVE-EXPIRE 0|1
DEBUG RELOAD
```

## Key Management

### KEYS / SCAN / DEL / EXISTS / TYPE / RENAME

```bash
KEYS pattern
SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
DEL key [key ...]
UNLINK key [key ...]
EXISTS key [key ...]
TYPE key
RENAME key newkey
RENAMENX key newkey
```

### Expiration

```bash
EXPIRE key seconds [NX|XX|GT|LT]
PEXPIRE key milliseconds [NX|XX|GT|LT]
EXPIREAT key unix-time [NX|XX|GT|LT]
PEXPIREAT key unix-time-ms [NX|XX|GT|LT]
TTL key
PTTL key
EXPIRETIME key
PEXPIRETIME key
PERSIST key
```

### COPY / MOVE / TOUCH / SORT / DUMP / RESTORE

```bash
COPY source destination [DB destination-db] [REPLACE]
MOVE key db
TOUCH key [key ...]
SORT key [BY pattern] [LIMIT offset count] [GET pattern ...] [ASC|DESC] [ALPHA] [STORE destination]
DUMP key
RESTORE key ttl serialized-value [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]
```

### OBJECT

```bash
OBJECT REFCOUNT key
OBJECT ENCODING key
OBJECT IDLETIME key
OBJECT FREQ key
OBJECT HELP
```

## Administration

### FLUSHDB / FLUSHALL

```bash
FLUSHDB [ASYNC|SYNC]
FLUSHALL [ASYNC|SYNC]
```

### SWAPDB

```bash
SWAPDB index1 index2
```

### SHUTDOWN

```bash
SHUTDOWN [NOSAVE|SAVE] [NOW] [FORCE]
```

### WAIT / WAITAOF

```bash
WAIT numreplicas timeout
WAITAOF numlocal numreplicas timeout
```

### ROLE / REPLICAOF

```bash
ROLE
REPLICAOF host port
REPLICAOF NO ONE
```

### MODULE LIST / LOAD / UNLOAD

```bash
MODULE LIST
MODULE LOADEX path [CONFIG name value ...] [ARGS arg ...]
MODULE LOAD path [arg ...]
MODULE UNLOAD name
```
