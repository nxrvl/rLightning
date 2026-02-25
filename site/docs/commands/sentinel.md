# Sentinel Commands

Redis Sentinel provides high availability for rLightning through monitoring, notification, automatic failover, and configuration provider functionality.

## Monitoring Commands

### SENTINEL MASTERS

Show all monitored masters and their state.

```bash
SENTINEL MASTERS
```

### SENTINEL MASTER

Show information about a specific monitored master.

```bash
SENTINEL MASTER master-name
```

### SENTINEL REPLICAS / SLAVES

List replicas of a monitored master.

```bash
SENTINEL REPLICAS master-name
SENTINEL SLAVES master-name
```

### SENTINEL SENTINELS

List other Sentinel instances monitoring a master.

```bash
SENTINEL SENTINELS master-name
```

### SENTINEL GET-MASTER-ADDR-BY-NAME

Return the address of a master by its name.

```bash
SENTINEL GET-MASTER-ADDR-BY-NAME master-name
```

```bash
SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
# 1) "127.0.0.1"
# 2) "6379"
```

### SENTINEL IS-MASTER-DOWN-BY-ADDR

Check if a master is down (used internally between Sentinels).

```bash
SENTINEL IS-MASTER-DOWN-BY-ADDR ip port current-epoch runid
```

## Failover Commands

### SENTINEL FAILOVER

Force a failover for a master.

```bash
SENTINEL FAILOVER master-name
```

### SENTINEL CKQUORUM

Check if the current Sentinel configuration can reach quorum.

```bash
SENTINEL CKQUORUM master-name
```

## Configuration Commands

### SENTINEL MONITOR

Start monitoring a new master.

```bash
SENTINEL MONITOR master-name ip port quorum
```

```bash
SENTINEL MONITOR mymaster 127.0.0.1 6379 2
```

### SENTINEL REMOVE

Stop monitoring a master.

```bash
SENTINEL REMOVE master-name
```

### SENTINEL SET

Change monitoring configuration for a master.

```bash
SENTINEL SET master-name option value [option value ...]
```

Options: `down-after-milliseconds`, `failover-timeout`, `parallel-syncs`, `auth-pass`, `auth-user`

### SENTINEL RESET

Reset all masters matching a pattern.

```bash
SENTINEL RESET pattern
```

### SENTINEL FLUSHCONFIG

Rewrite the Sentinel configuration on disk.

```bash
SENTINEL FLUSHCONFIG
```

### SENTINEL CONFIG

Get or set Sentinel configuration parameters.

```bash
SENTINEL CONFIG GET parameter
SENTINEL CONFIG SET parameter value
```

### SENTINEL MYID

Return the Sentinel instance ID.

```bash
SENTINEL MYID
```

## Failover Process

1. Sentinel detects master is unreachable (SDOWN - Subjective Down)
2. Multiple Sentinels agree master is down (ODOWN - Objective Down)
3. A Sentinel is elected as leader through Raft-like election
4. Leader selects the best replica for promotion
5. Leader promotes the replica to master
6. Other replicas are reconfigured to replicate from the new master
7. Clients are notified through pub/sub channels

## Pub/Sub Channels

Sentinel publishes events on these channels:

- `+switch-master` -- Master address changed
- `+sdown` -- Instance entered SDOWN state
- `-sdown` -- Instance exited SDOWN state
- `+odown` -- Instance entered ODOWN state
- `-odown` -- Instance exited ODOWN state
- `+failover-state-reconf-slaves` -- Failover reconfiguring replicas

```bash
SUBSCRIBE +switch-master +sdown -sdown
```

## Setup Example

```bash
# Sentinel configuration
sentinel monitor mymaster 127.0.0.1 6379 2
sentinel down-after-milliseconds mymaster 5000
sentinel failover-timeout mymaster 60000
sentinel parallel-syncs mymaster 1
```
