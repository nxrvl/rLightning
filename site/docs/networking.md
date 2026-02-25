# Networking

## RESP Protocol

rLightning implements both RESP2 and RESP3 protocols for Redis compatibility.

### RESP2 Types

- Simple Strings (`+OK\r\n`)
- Errors (`-ERR message\r\n`)
- Integers (`:1000\r\n`)
- Bulk Strings (`$6\r\nfoobar\r\n`)
- Arrays (`*2\r\n...`)

### RESP3 Types (Redis 7.0+)

- Map (`%`)
- Set (`~`)
- Null (`_`)
- Boolean (`#`)
- Double (`,`)
- Big Number (`(`)
- Verbatim String (`=`)
- Push (`>`)

Switch to RESP3 with:

```bash
HELLO 3
```

## Async I/O

Built on Tokio async runtime with non-blocking TCP connections. Each client connection is handled by an async task with efficient buffered I/O.

## Connection Management

- Configurable max connections
- Client tracking and naming (CLIENT SETNAME)
- Connection timeouts
- Pipeline support for batched commands
