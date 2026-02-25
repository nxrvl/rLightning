# Stream Commands

Streams are an append-only log data structure. They are ideal for event sourcing, message queues, and real-time data processing with consumer groups.

## Core Commands

### XADD

Append an entry to a stream.

```bash
XADD key [NOMKSTREAM] [MAXLEN|MINID [=|~] threshold] *|id field value [field value ...]
```

- `*`: Auto-generate ID (recommended)
- `MAXLEN ~ 1000`: Approximate trimming to ~1000 entries
- `NOMKSTREAM`: Don't create stream if it doesn't exist

```bash
XADD events * type "login" user "alice"
# "1709312400000-0"
XADD events MAXLEN ~ 1000 * type "purchase" item "widget"
# "1709312400001-0"
```

### XLEN

Return the number of entries in a stream.

```bash
XLEN key
```

### XRANGE / XREVRANGE

Return entries within an ID range.

```bash
XRANGE key start end [COUNT count]
XREVRANGE key end start [COUNT count]
```

Use `-` and `+` for minimum and maximum IDs.

```bash
XRANGE events - + COUNT 10
XRANGE events 1709312400000-0 +
XREVRANGE events + - COUNT 5
```

### XREAD

Read entries from one or more streams.

```bash
XREAD [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] id [id ...]
```

Use `$` to read only new entries (with BLOCK).

```bash
XREAD COUNT 10 STREAMS events 0
XREAD BLOCK 5000 STREAMS events $
```

### XTRIM

Trim the stream to a given length or minimum ID.

```bash
XTRIM key MAXLEN|MINID [=|~] threshold
```

### XDEL

Delete entries by ID.

```bash
XDEL key id [id ...]
```

### XINFO

Get information about streams, groups, or consumers.

```bash
XINFO STREAM key [FULL [COUNT count]]
XINFO GROUPS key
XINFO CONSUMERS key groupname
```

## Consumer Groups

### XGROUP

Manage consumer groups.

```bash
XGROUP CREATE key groupname id|$ [MKSTREAM]
XGROUP SETID key groupname id|$
XGROUP DESTROY key groupname
XGROUP DELCONSUMER key groupname consumername
XGROUP CREATECONSUMER key groupname consumername
```

```bash
XGROUP CREATE events mygroup $ MKSTREAM
XGROUP CREATE events mygroup 0  # read from beginning
```

### XREADGROUP

Read from a consumer group.

```bash
XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] [NOACK] STREAMS key [key ...] id [id ...]
```

Use `>` to read new (undelivered) messages.

```bash
XREADGROUP GROUP mygroup consumer1 COUNT 10 STREAMS events >
XREADGROUP GROUP mygroup consumer1 BLOCK 5000 STREAMS events >
```

### XACK

Acknowledge processed messages.

```bash
XACK key group id [id ...]
```

```bash
XACK events mygroup 1709312400000-0
```

### XPENDING

Get information about pending (unacknowledged) messages.

```bash
XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
```

```bash
XPENDING events mygroup - + 10
XPENDING events mygroup IDLE 60000 - + 10
```

### XCLAIM

Claim ownership of pending messages.

```bash
XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [TIME ms-unix-time] [RETRYCOUNT count] [FORCE] [JUSTID]
```

### XAUTOCLAIM

Automatically claim pending messages that have been idle.

```bash
XAUTOCLAIM key group consumer min-idle-time start [COUNT count] [JUSTID]
```

## Use Cases

### Event Log

```bash
XADD events * action "user_signup" email "alice@example.com"
XADD events * action "purchase" amount "49.99"
XRANGE events - + COUNT 100
```

### Work Queue with Consumer Groups

```bash
# Create stream and consumer group
XGROUP CREATE tasks workers $ MKSTREAM

# Producer adds tasks
XADD tasks * type "email" to "user@example.com"
XADD tasks * type "resize" image "photo.jpg"

# Workers process tasks
XREADGROUP GROUP workers worker1 COUNT 1 BLOCK 5000 STREAMS tasks >
# Process the task...
XACK tasks workers <message-id>
```
