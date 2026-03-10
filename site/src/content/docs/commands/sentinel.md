---
title: "Sentinel"
description: "Sentinel command reference"
order: 16
category: "commands"
---

# Sentinel Commands

rLightning Sentinel provides high availability through master monitoring, automatic failover, and configuration provider services. Sentinel monitors master and replica instances, detects failures via quorum-based voting, and promotes replicas when a master becomes unavailable.

## SENTINEL MASTERS

Synopsis: `SENTINEL MASTERS`

Return a list of all monitored masters and their current state.

```bash
> SENTINEL MASTERS
1)  1) "name"
    2) "mymaster"
    3) "ip"
    4) "127.0.0.1"
    5) "port"
    6) (integer) 6379
    7) "flags"
    8) "master"
    9) "num-slaves"
   10) (integer) 2
   11) "num-other-sentinels"
   12) (integer) 2
   13) "quorum"
   14) (integer) 2
   ...
```

## SENTINEL MASTER

Synopsis: `SENTINEL MASTER master-name`

Return the state and information of the specified master.

```bash
> SENTINEL MASTER mymaster
 1) "name"
 2) "mymaster"
 3) "ip"
 4) "127.0.0.1"
 5) "port"
 6) (integer) 6379
 ...
```

## SENTINEL REPLICAS

Synopsis: `SENTINEL REPLICAS master-name`

Return a list of replica instances for the specified master and their state.

```bash
> SENTINEL REPLICAS mymaster
1)  1) "name"
    2) "127.0.0.1:6380"
    3) "ip"
    4) "127.0.0.1"
    5) "port"
    6) (integer) 6380
    7) "flags"
    8) "slave"
    ...
```

## SENTINEL SLAVES

Synopsis: `SENTINEL SLAVES master-name`

Deprecated alias for SENTINEL REPLICAS.

```bash
> SENTINEL SLAVES mymaster
(same output as SENTINEL REPLICAS)
```

## SENTINEL GET-MASTER-ADDR-BY-NAME

Synopsis: `SENTINEL GET-MASTER-ADDR-BY-NAME master-name`

Return the IP address and port of the current master for the given name. This is the primary command used by clients to discover the current master.

```bash
> SENTINEL GET-MASTER-ADDR-BY-NAME mymaster
1) "127.0.0.1"
2) "6379"
```

## SENTINEL IS-MASTER-DOWN-BY-ADDR

Synopsis: `SENTINEL IS-MASTER-DOWN-BY-ADDR ip port current-epoch runid`

Used internally by Sentinels to check if a master is down and to request votes during failover leader election.

```bash
> SENTINEL IS-MASTER-DOWN-BY-ADDR 127.0.0.1 6379 0 *
1) (integer) 0
2) "*"
3) (integer) 0
```

## SENTINEL MONITOR

Synopsis: `SENTINEL MONITOR master-name ip port quorum`

Start monitoring a new master with the given name, address, and quorum.

```bash
> SENTINEL MONITOR mymaster 127.0.0.1 6379 2
OK
```

## SENTINEL SET

Synopsis: `SENTINEL SET master-name option value [option value ...]`

Change the configuration of a monitored master at runtime.

Common options:
- `down-after-milliseconds` -- Time before considering a master as down.
- `failover-timeout` -- Timeout for failover operations.
- `parallel-syncs` -- Number of replicas to reconfigure simultaneously during failover.

```bash
> SENTINEL SET mymaster down-after-milliseconds 5000
OK
> SENTINEL SET mymaster failover-timeout 60000
OK
```

## SENTINEL REMOVE

Synopsis: `SENTINEL REMOVE master-name`

Stop monitoring the specified master.

```bash
> SENTINEL REMOVE mymaster
OK
```

## SENTINEL FAILOVER

Synopsis: `SENTINEL FAILOVER master-name`

Trigger a manual failover for the specified master. A replica is promoted to master without agreement from other Sentinels.

```bash
> SENTINEL FAILOVER mymaster
OK
```

## SENTINEL PENDING-SCRIPTS

Synopsis: `SENTINEL PENDING-SCRIPTS`

Return a list of pending scripts (notification and reconfig scripts) that have not yet completed.

```bash
> SENTINEL PENDING-SCRIPTS
(empty array)
```

## SENTINEL CKQUORUM

Synopsis: `SENTINEL CKQUORUM master-name`

Check if the current Sentinel configuration is able to reach the quorum needed to authorize a failover and the majority needed to authorize the configuration change.

```bash
> SENTINEL CKQUORUM mymaster
OK 3 usable Sentinels. Quorum and samples are reachable.
```

## SENTINEL FLUSHCONFIG

Synopsis: `SENTINEL FLUSHCONFIG`

Force Sentinel to rewrite its configuration on disk, including the current state.

```bash
> SENTINEL FLUSHCONFIG
OK
```
