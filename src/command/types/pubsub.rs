//! Pub/Sub command handlers
//!
//! These commands handle Redis Pub/Sub functionality:
//! - SUBSCRIBE/UNSUBSCRIBE - Channel subscriptions
//! - PSUBSCRIBE/PUNSUBSCRIBE - Pattern subscriptions
//! - PUBLISH - Publish messages
//! - PUBSUB - Introspection commands

use crate::command::error::{CommandError, CommandResult};
use crate::networking::resp::RespValue;
use std::sync::Arc;

// Re-export pubsub types for convenience
pub use crate::pubsub::{ClientId, PubSubManager, SubscriptionMessage};

/// Build a RESP array response for subscription confirmation
fn build_subscribe_response(msg_type: &str, channel: &[u8], count: usize) -> RespValue {
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(msg_type.as_bytes().to_vec())),
        RespValue::BulkString(Some(channel.to_vec())),
        RespValue::Integer(count as i64),
    ]))
}

/// Build a RESP array response for message delivery
fn build_message_response(channel: &[u8], data: &[u8]) -> RespValue {
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"message".to_vec())),
        RespValue::BulkString(Some(channel.to_vec())),
        RespValue::BulkString(Some(data.to_vec())),
    ]))
}

/// Build a RESP array response for pattern message delivery
fn build_pmessage_response(pattern: &[u8], channel: &[u8], data: &[u8]) -> RespValue {
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"pmessage".to_vec())),
        RespValue::BulkString(Some(pattern.to_vec())),
        RespValue::BulkString(Some(channel.to_vec())),
        RespValue::BulkString(Some(data.to_vec())),
    ]))
}

/// Build a RESP array response for shard message delivery
fn build_smessage_response(channel: &[u8], data: &[u8]) -> RespValue {
    RespValue::Array(Some(vec![
        RespValue::BulkString(Some(b"smessage".to_vec())),
        RespValue::BulkString(Some(channel.to_vec())),
        RespValue::BulkString(Some(data.to_vec())),
    ]))
}

/// Convert a SubscriptionMessage to RESP format
pub fn subscription_message_to_resp(msg: &SubscriptionMessage) -> RespValue {
    match msg {
        SubscriptionMessage::Subscribe { channel, count } => {
            build_subscribe_response("subscribe", channel, *count)
        }
        SubscriptionMessage::Unsubscribe { channel, count } => {
            build_subscribe_response("unsubscribe", channel, *count)
        }
        SubscriptionMessage::Message { channel, data } => build_message_response(channel, data),
        SubscriptionMessage::PSubscribe { pattern, count } => {
            build_subscribe_response("psubscribe", pattern, *count)
        }
        SubscriptionMessage::PUnsubscribe { pattern, count } => {
            build_subscribe_response("punsubscribe", pattern, *count)
        }
        SubscriptionMessage::PMessage {
            pattern,
            channel,
            data,
        } => build_pmessage_response(pattern, channel, data),
        SubscriptionMessage::SSubscribe { channel, count } => {
            build_subscribe_response("ssubscribe", channel, *count)
        }
        SubscriptionMessage::SUnsubscribe { channel, count } => {
            build_subscribe_response("sunsubscribe", channel, *count)
        }
        SubscriptionMessage::SMessage { channel, data } => build_smessage_response(channel, data),
    }
}

/// SUBSCRIBE channel [channel ...]
///
/// Subscribe to the given channels. Once subscribed, the client enters
/// Pub/Sub mode and can only execute Pub/Sub commands.
///
/// Returns an array for each channel: ["subscribe", channel_name, subscription_count]
pub async fn subscribe(
    pubsub: &Arc<PubSubManager>,
    client_id: ClientId,
    args: &[Vec<u8>],
) -> Result<Vec<RespValue>, CommandError> {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let channels: Vec<Vec<u8>> = args.to_vec();
    let results = pubsub.subscribe(client_id, channels).await;

    Ok(results
        .into_iter()
        .map(|(channel, count)| build_subscribe_response("subscribe", &channel, count))
        .collect())
}

/// UNSUBSCRIBE [channel [channel ...]]
///
/// Unsubscribe from the given channels. If no channels are specified,
/// unsubscribe from all channels.
///
/// Returns an array for each channel: ["unsubscribe", channel_name, subscription_count]
pub async fn unsubscribe(
    pubsub: &Arc<PubSubManager>,
    client_id: ClientId,
    args: &[Vec<u8>],
) -> Result<Vec<RespValue>, CommandError> {
    let channels = if args.is_empty() {
        None
    } else {
        Some(args.to_vec())
    };

    let results = pubsub.unsubscribe(client_id, channels).await;

    // If no subscriptions existed, still return a response
    if results.is_empty() {
        return Ok(vec![build_subscribe_response("unsubscribe", b"", 0)]);
    }

    Ok(results
        .into_iter()
        .map(|(channel, count)| build_subscribe_response("unsubscribe", &channel, count))
        .collect())
}

/// PSUBSCRIBE pattern [pattern ...]
///
/// Subscribe to channels matching the given patterns.
/// Supports glob-style patterns: *, ?, [abc], [^abc]
///
/// Returns an array for each pattern: ["psubscribe", pattern, subscription_count]
pub async fn psubscribe(
    pubsub: &Arc<PubSubManager>,
    client_id: ClientId,
    args: &[Vec<u8>],
) -> Result<Vec<RespValue>, CommandError> {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let patterns: Vec<Vec<u8>> = args.to_vec();
    let results = pubsub.psubscribe(client_id, patterns).await;

    Ok(results
        .into_iter()
        .map(|(pattern, count)| build_subscribe_response("psubscribe", &pattern, count))
        .collect())
}

/// PUNSUBSCRIBE [pattern [pattern ...]]
///
/// Unsubscribe from the given patterns. If no patterns are specified,
/// unsubscribe from all patterns.
///
/// Returns an array for each pattern: ["punsubscribe", pattern, subscription_count]
pub async fn punsubscribe(
    pubsub: &Arc<PubSubManager>,
    client_id: ClientId,
    args: &[Vec<u8>],
) -> Result<Vec<RespValue>, CommandError> {
    let patterns = if args.is_empty() {
        None
    } else {
        Some(args.to_vec())
    };

    let results = pubsub.punsubscribe(client_id, patterns).await;

    // If no subscriptions existed, still return a response
    if results.is_empty() {
        return Ok(vec![build_subscribe_response("punsubscribe", b"", 0)]);
    }

    Ok(results
        .into_iter()
        .map(|(pattern, count)| build_subscribe_response("punsubscribe", &pattern, count))
        .collect())
}

/// PUBLISH channel message
///
/// Publish a message to a channel.
///
/// Returns the number of clients that received the message.
pub async fn publish(pubsub: &Arc<PubSubManager>, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let channel = args[0].clone();
    let message = args[1].clone();

    let recipients = pubsub.publish(channel, message).await;

    Ok(RespValue::Integer(recipients as i64))
}

/// SSUBSCRIBE shardchannel [shardchannel ...]
///
/// Subscribe to the given shard channels (Redis 7.0+).
/// Shard channels are node-local in cluster mode.
///
/// Returns an array for each channel: ["ssubscribe", channel_name, subscription_count]
pub async fn ssubscribe(
    pubsub: &Arc<PubSubManager>,
    client_id: ClientId,
    args: &[Vec<u8>],
) -> Result<Vec<RespValue>, CommandError> {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let channels: Vec<Vec<u8>> = args.to_vec();
    let results = pubsub.ssubscribe(client_id, channels).await;

    Ok(results
        .into_iter()
        .map(|(channel, count)| build_subscribe_response("ssubscribe", &channel, count))
        .collect())
}

/// SUNSUBSCRIBE [shardchannel [shardchannel ...]]
///
/// Unsubscribe from the given shard channels.
/// If no channels are specified, unsubscribe from all shard channels.
///
/// Returns an array for each channel: ["sunsubscribe", channel_name, subscription_count]
pub async fn sunsubscribe(
    pubsub: &Arc<PubSubManager>,
    client_id: ClientId,
    args: &[Vec<u8>],
) -> Result<Vec<RespValue>, CommandError> {
    let channels = if args.is_empty() {
        None
    } else {
        Some(args.to_vec())
    };

    let results = pubsub.sunsubscribe(client_id, channels).await;

    if results.is_empty() {
        return Ok(vec![build_subscribe_response("sunsubscribe", b"", 0)]);
    }

    Ok(results
        .into_iter()
        .map(|(channel, count)| build_subscribe_response("sunsubscribe", &channel, count))
        .collect())
}

/// SPUBLISH shardchannel message
///
/// Publish a message to a shard channel (Redis 7.0+).
/// Unlike PUBLISH, only delivers to SSUBSCRIBE subscribers (no pattern matching).
///
/// Returns the number of clients that received the message.
pub async fn spublish(pubsub: &Arc<PubSubManager>, args: &[Vec<u8>]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let channel = args[0].clone();
    let message = args[1].clone();

    let recipients = pubsub.spublish(channel, message).await;

    Ok(RespValue::Integer(recipients as i64))
}

/// PUBSUB subcommand [argument [argument ...]]
///
/// Introspection commands for the Pub/Sub system:
/// - PUBSUB CHANNELS [pattern] - List active channels
/// - PUBSUB NUMSUB [channel ...] - Get subscriber counts
/// - PUBSUB NUMPAT - Get the number of pattern subscriptions
/// - PUBSUB SHARDCHANNELS [pattern] - List active shard channels
/// - PUBSUB SHARDNUMSUB [channel ...] - Get shard channel subscriber counts
pub async fn pubsub_command(pubsub: &Arc<PubSubManager>, args: &[Vec<u8>]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArguments);
    }

    let subcommand = String::from_utf8_lossy(&args[0]).to_uppercase();

    match subcommand.as_str() {
        "CHANNELS" => {
            let pattern = args.get(1).map(|p| p.as_slice());
            let channels = pubsub.pubsub_channels(pattern).await;

            Ok(RespValue::Array(Some(
                channels
                    .into_iter()
                    .map(|c| RespValue::BulkString(Some(c)))
                    .collect(),
            )))
        }
        "NUMSUB" => {
            let channel_names: Vec<Vec<u8>> = args[1..].to_vec();
            let counts = pubsub.pubsub_numsub(&channel_names).await;

            // Return alternating channel names and counts
            let mut result = Vec::with_capacity(counts.len() * 2);
            for (channel, count) in counts {
                result.push(RespValue::BulkString(Some(channel)));
                result.push(RespValue::Integer(count as i64));
            }

            Ok(RespValue::Array(Some(result)))
        }
        "NUMPAT" => {
            let count = pubsub.pubsub_numpat().await;
            Ok(RespValue::Integer(count as i64))
        }
        "SHARDCHANNELS" => {
            let pattern = args.get(1).map(|p| p.as_slice());
            let channels = pubsub.pubsub_shardchannels(pattern).await;

            Ok(RespValue::Array(Some(
                channels
                    .into_iter()
                    .map(|c| RespValue::BulkString(Some(c)))
                    .collect(),
            )))
        }
        "SHARDNUMSUB" => {
            let channel_names: Vec<Vec<u8>> = args[1..].to_vec();
            let counts = pubsub.pubsub_shardnumsub(&channel_names).await;

            let mut result = Vec::with_capacity(counts.len() * 2);
            for (channel, count) in counts {
                result.push(RespValue::BulkString(Some(channel)));
                result.push(RespValue::Integer(count as i64));
            }

            Ok(RespValue::Array(Some(result)))
        }
        _ => Err(CommandError::InvalidArgument(format!(
            "Unknown PUBSUB subcommand '{}'",
            subcommand
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_subscribe_command() {
        let pubsub = Arc::new(PubSubManager::new());
        let (client_id, _rx) = pubsub.register_client().await;

        let responses = subscribe(&pubsub, client_id, &[b"news".to_vec(), b"sports".to_vec()])
            .await
            .unwrap();

        assert_eq!(responses.len(), 2);

        // Check first response
        if let RespValue::Array(Some(arr)) = &responses[0] {
            assert_eq!(arr[0], RespValue::BulkString(Some(b"subscribe".to_vec())));
            assert_eq!(arr[1], RespValue::BulkString(Some(b"news".to_vec())));
            assert_eq!(arr[2], RespValue::Integer(1));
        } else {
            panic!("Expected Array response");
        }

        // Check second response
        if let RespValue::Array(Some(arr)) = &responses[1] {
            assert_eq!(arr[0], RespValue::BulkString(Some(b"subscribe".to_vec())));
            assert_eq!(arr[1], RespValue::BulkString(Some(b"sports".to_vec())));
            assert_eq!(arr[2], RespValue::Integer(2));
        } else {
            panic!("Expected Array response");
        }
    }

    #[tokio::test]
    async fn test_publish_command() {
        let pubsub = Arc::new(PubSubManager::new());

        // Register and subscribe a client
        let (client_id, _rx) = pubsub.register_client().await;
        pubsub.subscribe(client_id, vec![b"news".to_vec()]).await;

        // Publish a message
        let result = publish(&pubsub, &[b"news".to_vec(), b"Hello!".to_vec()])
            .await
            .unwrap();

        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_pubsub_channels() {
        let pubsub = Arc::new(PubSubManager::new());

        let (client_id, _rx) = pubsub.register_client().await;
        pubsub
            .subscribe(
                client_id,
                vec![b"news:sports".to_vec(), b"news:tech".to_vec()],
            )
            .await;

        // Get all channels
        let result = pubsub_command(&pubsub, &[b"CHANNELS".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(channels)) = result {
            assert_eq!(channels.len(), 2);
        } else {
            panic!("Expected Array response");
        }

        // Get channels matching pattern
        let result = pubsub_command(&pubsub, &[b"CHANNELS".to_vec(), b"news:*".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(channels)) = result {
            assert_eq!(channels.len(), 2);
        } else {
            panic!("Expected Array response");
        }
    }

    #[tokio::test]
    async fn test_pubsub_numsub() {
        let pubsub = Arc::new(PubSubManager::new());

        let (client1, _rx1) = pubsub.register_client().await;
        let (client2, _rx2) = pubsub.register_client().await;

        pubsub.subscribe(client1, vec![b"news".to_vec()]).await;
        pubsub.subscribe(client2, vec![b"news".to_vec()]).await;

        let result = pubsub_command(&pubsub, &[b"NUMSUB".to_vec(), b"news".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(b"news".to_vec())));
            assert_eq!(arr[1], RespValue::Integer(2));
        } else {
            panic!("Expected Array response");
        }
    }

    #[tokio::test]
    async fn test_pubsub_numpat() {
        let pubsub = Arc::new(PubSubManager::new());

        let (client_id, _rx) = pubsub.register_client().await;
        pubsub
            .psubscribe(client_id, vec![b"news:*".to_vec(), b"tech:*".to_vec()])
            .await;

        let result = pubsub_command(&pubsub, &[b"NUMPAT".to_vec()])
            .await
            .unwrap();

        assert_eq!(result, RespValue::Integer(2));
    }

    // ==================== Sharded Pub/Sub Command Tests ====================

    #[tokio::test]
    async fn test_ssubscribe_command() {
        let pubsub = Arc::new(PubSubManager::new());
        let (client_id, _rx) = pubsub.register_client().await;

        let responses = ssubscribe(
            &pubsub,
            client_id,
            &[b"shard:news".to_vec(), b"shard:sports".to_vec()],
        )
        .await
        .unwrap();

        assert_eq!(responses.len(), 2);

        // Check first response
        if let RespValue::Array(Some(arr)) = &responses[0] {
            assert_eq!(arr[0], RespValue::BulkString(Some(b"ssubscribe".to_vec())));
            assert_eq!(arr[1], RespValue::BulkString(Some(b"shard:news".to_vec())));
            assert_eq!(arr[2], RespValue::Integer(1));
        } else {
            panic!("Expected Array response");
        }

        // Check second response
        if let RespValue::Array(Some(arr)) = &responses[1] {
            assert_eq!(arr[0], RespValue::BulkString(Some(b"ssubscribe".to_vec())));
            assert_eq!(
                arr[1],
                RespValue::BulkString(Some(b"shard:sports".to_vec()))
            );
            assert_eq!(arr[2], RespValue::Integer(2));
        } else {
            panic!("Expected Array response");
        }
    }

    #[tokio::test]
    async fn test_ssubscribe_no_args() {
        let pubsub = Arc::new(PubSubManager::new());
        let (client_id, _rx) = pubsub.register_client().await;

        let result = ssubscribe(&pubsub, client_id, &[]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_sunsubscribe_command() {
        let pubsub = Arc::new(PubSubManager::new());
        let (client_id, _rx) = pubsub.register_client().await;

        // Subscribe first
        pubsub
            .ssubscribe(
                client_id,
                vec![b"shard:news".to_vec(), b"shard:sports".to_vec()],
            )
            .await;

        // Unsubscribe from one
        let responses = sunsubscribe(&pubsub, client_id, &[b"shard:news".to_vec()])
            .await
            .unwrap();

        assert_eq!(responses.len(), 1);
        if let RespValue::Array(Some(arr)) = &responses[0] {
            assert_eq!(
                arr[0],
                RespValue::BulkString(Some(b"sunsubscribe".to_vec()))
            );
            assert_eq!(arr[1], RespValue::BulkString(Some(b"shard:news".to_vec())));
            assert_eq!(arr[2], RespValue::Integer(1)); // 1 remaining
        } else {
            panic!("Expected Array response");
        }
    }

    #[tokio::test]
    async fn test_sunsubscribe_all() {
        let pubsub = Arc::new(PubSubManager::new());
        let (client_id, _rx) = pubsub.register_client().await;

        // Subscribe first
        pubsub
            .ssubscribe(client_id, vec![b"shard:a".to_vec(), b"shard:b".to_vec()])
            .await;

        // Unsubscribe from all (no args)
        let responses = sunsubscribe(&pubsub, client_id, &[]).await.unwrap();
        assert_eq!(responses.len(), 2);

        // Counts decrease as each channel is unsubscribed; last one should be 0
        let mut counts: Vec<i64> = responses
            .iter()
            .filter_map(|resp| {
                if let RespValue::Array(Some(arr)) = resp
                    && let RespValue::Integer(n) = arr[2]
                {
                    return Some(n);
                }
                None
            })
            .collect();
        counts.sort();
        assert_eq!(counts, vec![0, 1]);
    }

    #[tokio::test]
    async fn test_sunsubscribe_empty() {
        let pubsub = Arc::new(PubSubManager::new());
        let (client_id, _rx) = pubsub.register_client().await;

        // Unsubscribe with no active subscriptions
        let responses = sunsubscribe(&pubsub, client_id, &[]).await.unwrap();
        assert_eq!(responses.len(), 1); // Returns a default empty response
    }

    #[tokio::test]
    async fn test_spublish_command() {
        let pubsub = Arc::new(PubSubManager::new());

        let (client_id, _rx) = pubsub.register_client().await;
        pubsub
            .ssubscribe(client_id, vec![b"shard:news".to_vec()])
            .await;

        let result = spublish(&pubsub, &[b"shard:news".to_vec(), b"Shard Hello!".to_vec()])
            .await
            .unwrap();

        assert_eq!(result, RespValue::Integer(1));
    }

    #[tokio::test]
    async fn test_spublish_wrong_args() {
        let pubsub = Arc::new(PubSubManager::new());

        // Too few args
        let result = spublish(&pubsub, &[b"shard:news".to_vec()]).await;
        assert!(result.is_err());

        // Too many args
        let result = spublish(&pubsub, &[b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_pubsub_shardchannels() {
        let pubsub = Arc::new(PubSubManager::new());

        let (client_id, _rx) = pubsub.register_client().await;
        pubsub
            .ssubscribe(
                client_id,
                vec![b"shard:news".to_vec(), b"shard:tech".to_vec()],
            )
            .await;

        // Get all shard channels
        let result = pubsub_command(&pubsub, &[b"SHARDCHANNELS".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(channels)) = result {
            assert_eq!(channels.len(), 2);
        } else {
            panic!("Expected Array response");
        }

        // Get shard channels matching pattern
        let result = pubsub_command(&pubsub, &[b"SHARDCHANNELS".to_vec(), b"shard:n*".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(channels)) = result {
            assert_eq!(channels.len(), 1);
        } else {
            panic!("Expected Array response");
        }
    }

    #[tokio::test]
    async fn test_pubsub_shardnumsub() {
        let pubsub = Arc::new(PubSubManager::new());

        let (client1, _rx1) = pubsub.register_client().await;
        let (client2, _rx2) = pubsub.register_client().await;

        pubsub
            .ssubscribe(client1, vec![b"shard:news".to_vec()])
            .await;
        pubsub
            .ssubscribe(client2, vec![b"shard:news".to_vec()])
            .await;

        let result = pubsub_command(&pubsub, &[b"SHARDNUMSUB".to_vec(), b"shard:news".to_vec()])
            .await
            .unwrap();

        if let RespValue::Array(Some(arr)) = result {
            assert_eq!(arr.len(), 2);
            assert_eq!(arr[0], RespValue::BulkString(Some(b"shard:news".to_vec())));
            assert_eq!(arr[1], RespValue::Integer(2));
        } else {
            panic!("Expected Array response");
        }
    }
}
