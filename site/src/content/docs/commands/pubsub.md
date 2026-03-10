---
title: "Pub/Sub"
description: "Pub/Sub command reference"
order: 13
category: "commands"
---

# Pub/Sub Commands

Pub/Sub (Publish/Subscribe) enables real-time messaging between clients. Publishers send messages to channels without knowing who receives them. Subscribers listen on channels and receive messages as they are published. rLightning supports channel subscriptions, pattern subscriptions, and Redis 7.0 sharded pub/sub.

## SUBSCRIBE

Synopsis: `SUBSCRIBE channel [channel ...]`

Subscribe to one or more channels. The client enters subscriber mode and can only execute SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, SSUBSCRIBE, SUNSUBSCRIBE, PING, and RESET.

```bash
> SUBSCRIBE news alerts
1) "subscribe"
2) "news"
3) (integer) 1
1) "subscribe"
2) "alerts"
3) (integer) 2
```

When a message arrives:

```bash
1) "message"
2) "news"
3) "Breaking: rLightning 1.0 released!"
```

## UNSUBSCRIBE

Synopsis: `UNSUBSCRIBE [channel [channel ...]]`

Unsubscribe from one or more channels. Without arguments, unsubscribes from all channels.

```bash
> UNSUBSCRIBE news
1) "unsubscribe"
2) "news"
3) (integer) 1
```

## PUBLISH

Synopsis: `PUBLISH channel message`

Publish a message to a channel. Returns the number of subscribers that received the message.

```bash
> PUBLISH news "Hello, subscribers!"
(integer) 3
```

## PSUBSCRIBE

Synopsis: `PSUBSCRIBE pattern [pattern ...]`

Subscribe to channels matching one or more glob-style patterns. Supported glob characters: `*`, `?`, `[...]`.

```bash
> PSUBSCRIBE news.*
1) "psubscribe"
2) "news.*"
3) (integer) 1
```

When a matching message arrives:

```bash
1) "pmessage"
2) "news.*"
3) "news.tech"
4) "New CPU benchmark results"
```

## PUNSUBSCRIBE

Synopsis: `PUNSUBSCRIBE [pattern [pattern ...]]`

Unsubscribe from one or more patterns. Without arguments, unsubscribes from all patterns.

```bash
> PUNSUBSCRIBE news.*
1) "punsubscribe"
2) "news.*"
3) (integer) 0
```

## PUBSUB

The PUBSUB command provides introspection into the pub/sub subsystem.

### PUBSUB CHANNELS

Synopsis: `PUBSUB CHANNELS [pattern]`

List active channels (channels with at least one subscriber). With a pattern, only matching channels are returned.

```bash
> PUBSUB CHANNELS
1) "news"
2) "alerts"
> PUBSUB CHANNELS news*
1) "news"
```

### PUBSUB NUMSUB

Synopsis: `PUBSUB NUMSUB [channel [channel ...]]`

Return the number of subscribers for the specified channels.

```bash
> PUBSUB NUMSUB news alerts
1) "news"
2) (integer) 3
3) "alerts"
4) (integer) 1
```

### PUBSUB NUMPAT

Synopsis: `PUBSUB NUMPAT`

Return the number of active pattern subscriptions.

```bash
> PUBSUB NUMPAT
(integer) 2
```

### PUBSUB SHARDCHANNELS

Synopsis: `PUBSUB SHARDCHANNELS [pattern]`

List active shard channels. Available since Redis 7.0.

```bash
> PUBSUB SHARDCHANNELS
1) "orders"
```

### PUBSUB SHARDNUMSUB

Synopsis: `PUBSUB SHARDNUMSUB [channel [channel ...]]`

Return the number of subscribers for the specified shard channels.

```bash
> PUBSUB SHARDNUMSUB orders
1) "orders"
2) (integer) 5
```

## Sharded Pub/Sub

Sharded pub/sub (Redis 7.0+) routes messages by hash slot, making pub/sub compatible with Redis Cluster. Messages are only delivered within the same shard.

### SSUBSCRIBE

Synopsis: `SSUBSCRIBE shardchannel [shardchannel ...]`

Subscribe to one or more shard channels.

```bash
> SSUBSCRIBE orders
1) "ssubscribe"
2) "orders"
3) (integer) 1
```

When a message arrives:

```bash
1) "smessage"
2) "orders"
3) "order:12345 placed"
```

### SUNSUBSCRIBE

Synopsis: `SUNSUBSCRIBE [shardchannel [shardchannel ...]]`

Unsubscribe from one or more shard channels. Without arguments, unsubscribes from all shard channels.

```bash
> SUNSUBSCRIBE orders
1) "sunsubscribe"
2) "orders"
3) (integer) 0
```

### SPUBLISH

Synopsis: `SPUBLISH shardchannel message`

Publish a message to a shard channel. The message is only delivered within the shard that owns the channel's hash slot.

```bash
> SPUBLISH orders "order:12345 shipped"
(integer) 2
```
