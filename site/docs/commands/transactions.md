# Transaction Commands

Transactions allow executing a group of commands atomically. All commands in a transaction are serialized and executed sequentially without interruption from other clients.

## Commands

### MULTI

Mark the start of a transaction block. Subsequent commands will be queued for atomic execution.

```bash
MULTI
```

Returns `OK`.

### EXEC

Execute all commands issued after MULTI.

```bash
EXEC
```

Returns an array of replies, one for each queued command.

```bash
MULTI
SET key1 "hello"
SET key2 "world"
GET key1
EXEC
# 1) OK
# 2) OK
# 3) "hello"
```

### DISCARD

Discard all queued commands and exit the transaction.

```bash
DISCARD
```

```bash
MULTI
SET key1 "hello"
DISCARD
# OK (transaction discarded, SET was not executed)
```

### WATCH

Watch keys for changes. If any watched key is modified before EXEC, the transaction is aborted (EXEC returns nil).

```bash
WATCH key [key ...]
```

This implements optimistic locking:

```bash
WATCH balance
val = GET balance
# val = "100"
MULTI
SET balance 90  # deduct 10
EXEC
# If another client modified "balance" after WATCH,
# EXEC returns (nil) and no commands are executed
```

### UNWATCH

Clear all watched keys for the current connection.

```bash
UNWATCH
```

## Error Handling

### Command Errors During Queuing

If a command has a syntax error, it fails at queue time and the entire transaction is aborted:

```bash
MULTI
SET key1 "hello"
INVALIDCMD  # error: unknown command
SET key2 "world"
EXEC
# (error) EXECABORT Transaction discarded because of previous errors
```

### Runtime Errors

If a command fails at execution time (e.g., wrong type), other commands still execute:

```bash
SET key1 "hello"
MULTI
INCR key1  # will fail (not an integer)
SET key2 "world"
EXEC
# 1) (error) ERR value is not an integer
# 2) OK
```

## Use Cases

### Atomic Counter Update

```bash
WATCH counter
val = GET counter
MULTI
SET counter (val + 1)
EXEC
```

### Transfer Between Accounts

```bash
WATCH account:A account:B
balA = GET account:A
balB = GET account:B
MULTI
SET account:A (balA - 100)
SET account:B (balB + 100)
EXEC
```
