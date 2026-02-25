//! PubSub Manager - Central registry for channels and subscriptions
//!
//! Handles:
//! - Channel subscriptions (SUBSCRIBE/UNSUBSCRIBE)
//! - Pattern subscriptions (PSUBSCRIBE/PUNSUBSCRIBE)
//! - Message publishing (PUBLISH)
//! - Subscription introspection (PUBSUB CHANNELS/NUMSUB/NUMPAT)

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{broadcast, RwLock};

/// Unique identifier for a client connection
pub type ClientId = u64;

/// Messages sent to subscribers
#[derive(Debug, Clone)]
pub enum SubscriptionMessage {
    /// Confirmation of channel subscription: (channel, subscription_count)
    Subscribe {
        channel: Vec<u8>,
        count: usize,
    },
    /// Confirmation of channel unsubscription: (channel, subscription_count)
    Unsubscribe {
        channel: Vec<u8>,
        count: usize,
    },
    /// Message from a channel: (channel, message)
    Message {
        channel: Vec<u8>,
        data: Vec<u8>,
    },
    /// Confirmation of pattern subscription: (pattern, subscription_count)
    PSubscribe {
        pattern: Vec<u8>,
        count: usize,
    },
    /// Confirmation of pattern unsubscription: (pattern, subscription_count)
    PUnsubscribe {
        pattern: Vec<u8>,
        count: usize,
    },
    /// Message matching a pattern: (pattern, channel, message)
    PMessage {
        pattern: Vec<u8>,
        channel: Vec<u8>,
        data: Vec<u8>,
    },
    /// Confirmation of shard channel subscription: (channel, subscription_count)
    SSubscribe {
        channel: Vec<u8>,
        count: usize,
    },
    /// Confirmation of shard channel unsubscription: (channel, subscription_count)
    SUnsubscribe {
        channel: Vec<u8>,
        count: usize,
    },
    /// Message from a shard channel: (channel, message)
    SMessage {
        channel: Vec<u8>,
        data: Vec<u8>,
    },
}

/// State for a single client's subscriptions
pub struct ClientSubscription {
    /// Channel to send messages to this client
    pub tx: broadcast::Sender<SubscriptionMessage>,
    /// Channels this client is subscribed to
    pub channels: HashSet<Vec<u8>>,
    /// Patterns this client is subscribed to
    pub patterns: HashSet<Vec<u8>>,
    /// Shard channels this client is subscribed to (Redis 7.0+)
    pub shard_channels: HashSet<Vec<u8>>,
}

impl ClientSubscription {
    fn new(tx: broadcast::Sender<SubscriptionMessage>) -> Self {
        Self {
            tx,
            channels: HashSet::new(),
            patterns: HashSet::new(),
            shard_channels: HashSet::new(),
        }
    }

    /// Total number of subscriptions (channels + patterns + shard channels)
    pub fn subscription_count(&self) -> usize {
        self.channels.len() + self.patterns.len() + self.shard_channels.len()
    }
}

/// Central Pub/Sub manager
///
/// Thread-safe registry for all channel subscriptions and pattern subscriptions.
/// Uses broadcast channels for efficient message distribution.
pub struct PubSubManager {
    /// Channel name -> Set of subscribed client IDs
    channels: RwLock<HashMap<Vec<u8>, HashSet<ClientId>>>,

    /// Pattern -> Set of subscribed client IDs
    patterns: RwLock<HashMap<Vec<u8>, HashSet<ClientId>>>,

    /// Shard channel name -> Set of subscribed client IDs (Redis 7.0+)
    shard_channels: RwLock<HashMap<Vec<u8>, HashSet<ClientId>>>,

    /// Client ID -> Client subscription state
    clients: RwLock<HashMap<ClientId, ClientSubscription>>,

    /// Next client ID counter
    next_client_id: AtomicU64,

    /// Broadcast channel capacity (messages buffered per subscriber)
    channel_capacity: usize,
}

impl Default for PubSubManager {
    fn default() -> Self {
        Self::new()
    }
}

impl PubSubManager {
    /// Create a new PubSubManager with default settings
    pub fn new() -> Self {
        Self::with_capacity(1024) // Default: buffer 1024 messages per subscriber
    }

    /// Create a new PubSubManager with custom channel capacity
    pub fn with_capacity(channel_capacity: usize) -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            patterns: RwLock::new(HashMap::new()),
            shard_channels: RwLock::new(HashMap::new()),
            clients: RwLock::new(HashMap::new()),
            next_client_id: AtomicU64::new(1),
            channel_capacity,
        }
    }

    /// Register a new client and return its ID and message receiver
    pub async fn register_client(&self) -> (ClientId, broadcast::Receiver<SubscriptionMessage>) {
        let client_id = self.next_client_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = broadcast::channel(self.channel_capacity);

        let subscription = ClientSubscription::new(tx);

        let mut clients = self.clients.write().await;
        clients.insert(client_id, subscription);

        (client_id, rx)
    }

    /// Unregister a client and clean up all its subscriptions
    pub async fn unregister_client(&self, client_id: ClientId) {
        // Get the client's subscriptions
        let client_sub = {
            let mut clients = self.clients.write().await;
            clients.remove(&client_id)
        };

        if let Some(sub) = client_sub {
            // Remove from all subscribed channels
            let mut channels = self.channels.write().await;
            for channel in sub.channels {
                if let Some(subscribers) = channels.get_mut(&channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        channels.remove(&channel);
                    }
                }
            }
            drop(channels);

            // Remove from all subscribed patterns
            let mut patterns = self.patterns.write().await;
            for pattern in sub.patterns {
                if let Some(subscribers) = patterns.get_mut(&pattern) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        patterns.remove(&pattern);
                    }
                }
            }
            drop(patterns);

            // Remove from all subscribed shard channels
            let mut shard_channels = self.shard_channels.write().await;
            for channel in sub.shard_channels {
                if let Some(subscribers) = shard_channels.get_mut(&channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        shard_channels.remove(&channel);
                    }
                }
            }
        }
    }

    /// Subscribe a client to one or more channels
    /// Returns a list of (channel, subscription_count) for each subscription
    pub async fn subscribe(
        &self,
        client_id: ClientId,
        channel_names: Vec<Vec<u8>>,
    ) -> Vec<(Vec<u8>, usize)> {
        let mut results = Vec::with_capacity(channel_names.len());

        let mut channels = self.channels.write().await;
        let mut clients = self.clients.write().await;

        if let Some(client_sub) = clients.get_mut(&client_id) {
            for channel in channel_names {
                // Add channel to client's subscription set
                client_sub.channels.insert(channel.clone());

                // Add client to channel's subscriber set
                channels
                    .entry(channel.clone())
                    .or_insert_with(HashSet::new)
                    .insert(client_id);

                let count = client_sub.subscription_count();

                // Note: Subscription confirmations are returned as results
                // and sent directly to the client by the server.
                // The broadcast channel is only used for actual messages.

                results.push((channel, count));
            }
        }

        results
    }

    /// Unsubscribe a client from channels
    /// If no channels specified, unsubscribe from all channels
    /// Returns a list of (channel, subscription_count) for each unsubscription
    pub async fn unsubscribe(
        &self,
        client_id: ClientId,
        channel_names: Option<Vec<Vec<u8>>>,
    ) -> Vec<(Vec<u8>, usize)> {
        let mut results = Vec::new();

        let mut channels_lock = self.channels.write().await;
        let mut clients = self.clients.write().await;

        if let Some(client_sub) = clients.get_mut(&client_id) {
            let channels_to_unsub: Vec<Vec<u8>> = match channel_names {
                Some(names) => names,
                None => client_sub.channels.iter().cloned().collect(),
            };

            for channel in channels_to_unsub {
                // Remove channel from client's subscription set
                client_sub.channels.remove(&channel);

                // Remove client from channel's subscriber set
                if let Some(subscribers) = channels_lock.get_mut(&channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        channels_lock.remove(&channel);
                    }
                }

                let count = client_sub.subscription_count();

                // Note: Unsubscription confirmations are returned as results
                // and sent directly to the client by the server.

                results.push((channel, count));
            }
        }

        results
    }

    /// Subscribe a client to one or more patterns
    pub async fn psubscribe(
        &self,
        client_id: ClientId,
        pattern_names: Vec<Vec<u8>>,
    ) -> Vec<(Vec<u8>, usize)> {
        let mut results = Vec::with_capacity(pattern_names.len());

        let mut patterns = self.patterns.write().await;
        let mut clients = self.clients.write().await;

        if let Some(client_sub) = clients.get_mut(&client_id) {
            for pattern in pattern_names {
                // Add pattern to client's subscription set
                client_sub.patterns.insert(pattern.clone());

                // Add client to pattern's subscriber set
                patterns
                    .entry(pattern.clone())
                    .or_insert_with(HashSet::new)
                    .insert(client_id);

                let count = client_sub.subscription_count();

                // Note: Pattern subscription confirmations are returned as results
                // and sent directly to the client by the server.

                results.push((pattern, count));
            }
        }

        results
    }

    /// Unsubscribe a client from patterns
    /// If no patterns specified, unsubscribe from all patterns
    pub async fn punsubscribe(
        &self,
        client_id: ClientId,
        pattern_names: Option<Vec<Vec<u8>>>,
    ) -> Vec<(Vec<u8>, usize)> {
        let mut results = Vec::new();

        let mut patterns_lock = self.patterns.write().await;
        let mut clients = self.clients.write().await;

        if let Some(client_sub) = clients.get_mut(&client_id) {
            let patterns_to_unsub: Vec<Vec<u8>> = match pattern_names {
                Some(names) => names,
                None => client_sub.patterns.iter().cloned().collect(),
            };

            for pattern in patterns_to_unsub {
                // Remove pattern from client's subscription set
                client_sub.patterns.remove(&pattern);

                // Remove client from pattern's subscriber set
                if let Some(subscribers) = patterns_lock.get_mut(&pattern) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        patterns_lock.remove(&pattern);
                    }
                }

                let count = client_sub.subscription_count();

                // Note: Pattern unsubscription confirmations are returned as results
                // and sent directly to the client by the server.

                results.push((pattern, count));
            }
        }

        results
    }

    /// Publish a message to a channel
    /// Returns the number of clients that received the message
    pub async fn publish(&self, channel: Vec<u8>, message: Vec<u8>) -> usize {
        let mut recipients = 0;

        // Send to direct channel subscribers
        {
            let channels = self.channels.read().await;
            let clients = self.clients.read().await;

            if let Some(subscriber_ids) = channels.get(&channel) {
                for &client_id in subscriber_ids {
                    if let Some(client_sub) = clients.get(&client_id) {
                        if client_sub.tx.send(SubscriptionMessage::Message {
                            channel: channel.clone(),
                            data: message.clone(),
                        }).is_ok() {
                            recipients += 1;
                        }
                    }
                }
            }
        }

        // Send to pattern subscribers
        {
            let patterns = self.patterns.read().await;
            let clients = self.clients.read().await;

            for (pattern, subscriber_ids) in patterns.iter() {
                if Self::matches_pattern(pattern, &channel) {
                    for &client_id in subscriber_ids {
                        if let Some(client_sub) = clients.get(&client_id) {
                            if client_sub.tx.send(SubscriptionMessage::PMessage {
                                pattern: pattern.clone(),
                                channel: channel.clone(),
                                data: message.clone(),
                            }).is_ok() {
                                recipients += 1;
                            }
                        }
                    }
                }
            }
        }

        recipients
    }

    /// Get the subscription count for a client (for checking if in subscription mode)
    pub async fn get_subscription_count(&self, client_id: ClientId) -> usize {
        let clients = self.clients.read().await;
        clients.get(&client_id)
            .map(|c| c.subscription_count())
            .unwrap_or(0)
    }

    /// Check if a client is registered
    pub async fn is_client_registered(&self, client_id: ClientId) -> bool {
        let clients = self.clients.read().await;
        clients.contains_key(&client_id)
    }

    /// Get receiver for a client (for receiving messages)
    pub async fn get_receiver(&self, client_id: ClientId) -> Option<broadcast::Receiver<SubscriptionMessage>> {
        let clients = self.clients.read().await;
        clients.get(&client_id).map(|c| c.tx.subscribe())
    }

    // ==================== Sharded Pub/Sub (Redis 7.0+) ====================

    /// Subscribe a client to one or more shard channels
    /// Returns a list of (channel, subscription_count) for each subscription
    pub async fn ssubscribe(
        &self,
        client_id: ClientId,
        channel_names: Vec<Vec<u8>>,
    ) -> Vec<(Vec<u8>, usize)> {
        let mut results = Vec::with_capacity(channel_names.len());

        let mut shard_channels = self.shard_channels.write().await;
        let mut clients = self.clients.write().await;

        if let Some(client_sub) = clients.get_mut(&client_id) {
            for channel in channel_names {
                client_sub.shard_channels.insert(channel.clone());

                shard_channels
                    .entry(channel.clone())
                    .or_insert_with(HashSet::new)
                    .insert(client_id);

                let count = client_sub.subscription_count();
                results.push((channel, count));
            }
        }

        results
    }

    /// Unsubscribe a client from shard channels
    /// If no channels specified, unsubscribe from all shard channels
    pub async fn sunsubscribe(
        &self,
        client_id: ClientId,
        channel_names: Option<Vec<Vec<u8>>>,
    ) -> Vec<(Vec<u8>, usize)> {
        let mut results = Vec::new();

        let mut shard_channels_lock = self.shard_channels.write().await;
        let mut clients = self.clients.write().await;

        if let Some(client_sub) = clients.get_mut(&client_id) {
            let channels_to_unsub: Vec<Vec<u8>> = match channel_names {
                Some(names) => names,
                None => client_sub.shard_channels.iter().cloned().collect(),
            };

            for channel in channels_to_unsub {
                client_sub.shard_channels.remove(&channel);

                if let Some(subscribers) = shard_channels_lock.get_mut(&channel) {
                    subscribers.remove(&client_id);
                    if subscribers.is_empty() {
                        shard_channels_lock.remove(&channel);
                    }
                }

                let count = client_sub.subscription_count();
                results.push((channel, count));
            }
        }

        results
    }

    /// Publish a message to a shard channel
    /// Returns the number of clients that received the message
    /// Unlike regular PUBLISH, shard publish only delivers to shard channel subscribers
    /// (no pattern matching)
    pub async fn spublish(&self, channel: Vec<u8>, message: Vec<u8>) -> usize {
        let mut recipients = 0;

        let shard_channels = self.shard_channels.read().await;
        let clients = self.clients.read().await;

        if let Some(subscriber_ids) = shard_channels.get(&channel) {
            for &client_id in subscriber_ids {
                if let Some(client_sub) = clients.get(&client_id) {
                    if client_sub
                        .tx
                        .send(SubscriptionMessage::SMessage {
                            channel: channel.clone(),
                            data: message.clone(),
                        })
                        .is_ok()
                    {
                        recipients += 1;
                    }
                }
            }
        }

        recipients
    }

    // ==================== PUBSUB Introspection Commands ====================

    /// PUBSUB CHANNELS [pattern] - List active channels
    /// Returns channels with at least one subscriber, optionally filtered by pattern
    pub async fn pubsub_channels(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        let channels = self.channels.read().await;

        channels.keys()
            .filter(|channel| {
                pattern.map(|p| Self::matches_pattern(p, channel)).unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    /// PUBSUB NUMSUB [channel ...] - Get subscriber counts for channels
    pub async fn pubsub_numsub(&self, channel_names: &[Vec<u8>]) -> Vec<(Vec<u8>, usize)> {
        let channels = self.channels.read().await;

        channel_names.iter()
            .map(|channel| {
                let count = channels.get(channel)
                    .map(|s| s.len())
                    .unwrap_or(0);
                (channel.clone(), count)
            })
            .collect()
    }

    /// PUBSUB NUMPAT - Get the number of unique patterns
    pub async fn pubsub_numpat(&self) -> usize {
        let patterns = self.patterns.read().await;
        patterns.len()
    }

    /// PUBSUB SHARDCHANNELS [pattern] - List active shard channels
    /// Returns shard channels with at least one subscriber, optionally filtered by pattern
    pub async fn pubsub_shardchannels(&self, pattern: Option<&[u8]>) -> Vec<Vec<u8>> {
        let shard_channels = self.shard_channels.read().await;

        shard_channels
            .keys()
            .filter(|channel| {
                pattern
                    .map(|p| Self::matches_pattern(p, channel))
                    .unwrap_or(true)
            })
            .cloned()
            .collect()
    }

    /// PUBSUB SHARDNUMSUB [channel ...] - Get subscriber counts for shard channels
    pub async fn pubsub_shardnumsub(&self, channel_names: &[Vec<u8>]) -> Vec<(Vec<u8>, usize)> {
        let shard_channels = self.shard_channels.read().await;

        channel_names
            .iter()
            .map(|channel| {
                let count = shard_channels.get(channel).map(|s| s.len()).unwrap_or(0);
                (channel.clone(), count)
            })
            .collect()
    }

    // ==================== Pattern Matching ====================

    /// Match a pattern against a channel name
    /// Supports Redis glob-style patterns:
    /// - `*` matches any sequence of characters
    /// - `?` matches any single character
    /// - `[abc]` matches any character in the brackets
    /// - `[^abc]` or `[!abc]` matches any character not in the brackets
    /// - `\x` escapes character x
    pub fn matches_pattern(pattern: &[u8], channel: &[u8]) -> bool {
        Self::glob_match(pattern, channel, 0, 0)
    }

    fn glob_match(pattern: &[u8], text: &[u8], mut pi: usize, mut ti: usize) -> bool {
        let mut star_pi = None;
        let mut star_ti = None;

        while ti < text.len() {
            if pi < pattern.len() {
                match pattern[pi] {
                    b'*' => {
                        // Star: save position and skip
                        star_pi = Some(pi);
                        star_ti = Some(ti);
                        pi += 1;
                        continue;
                    }
                    b'?' => {
                        // Question mark: match any single character
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                    b'[' => {
                        // Character class
                        if let Some((matched, new_pi)) = Self::match_char_class(pattern, pi, text[ti]) {
                            if matched {
                                pi = new_pi;
                                ti += 1;
                                continue;
                            }
                        }
                    }
                    b'\\' if pi + 1 < pattern.len() => {
                        // Escaped character
                        if pattern[pi + 1] == text[ti] {
                            pi += 2;
                            ti += 1;
                            continue;
                        }
                    }
                    c if c == text[ti] => {
                        // Exact match
                        pi += 1;
                        ti += 1;
                        continue;
                    }
                    _ => {}
                }
            }

            // No match - try backtracking to last star
            if let (Some(sp), Some(st)) = (star_pi, star_ti) {
                pi = sp + 1;
                star_ti = Some(st + 1);
                ti = st + 1;
            } else {
                return false;
            }
        }

        // Consume remaining stars in pattern
        while pi < pattern.len() && pattern[pi] == b'*' {
            pi += 1;
        }

        pi == pattern.len()
    }

    fn match_char_class(pattern: &[u8], start: usize, ch: u8) -> Option<(bool, usize)> {
        let mut i = start + 1;
        let mut negated = false;
        let mut matched = false;

        if i < pattern.len() && (pattern[i] == b'^' || pattern[i] == b'!') {
            negated = true;
            i += 1;
        }

        let mut first = true;
        while i < pattern.len() {
            if pattern[i] == b']' && !first {
                return Some((if negated { !matched } else { matched }, i + 1));
            }

            first = false;

            // Handle range (e.g., a-z)
            if i + 2 < pattern.len() && pattern[i + 1] == b'-' && pattern[i + 2] != b']' {
                let start_ch = pattern[i];
                let end_ch = pattern[i + 2];
                if ch >= start_ch && ch <= end_ch {
                    matched = true;
                }
                i += 3;
            } else {
                if pattern[i] == ch {
                    matched = true;
                }
                i += 1;
            }
        }

        None // Malformed pattern
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_matching_star() {
        assert!(PubSubManager::matches_pattern(b"*", b"anything"));
        assert!(PubSubManager::matches_pattern(b"*", b""));
        assert!(PubSubManager::matches_pattern(b"news:*", b"news:sports"));
        assert!(PubSubManager::matches_pattern(b"news:*", b"news:"));
        assert!(!PubSubManager::matches_pattern(b"news:*", b"weather:sports"));
        assert!(PubSubManager::matches_pattern(b"*:sports", b"news:sports"));
        assert!(PubSubManager::matches_pattern(b"*:*", b"news:sports"));
        assert!(PubSubManager::matches_pattern(b"user:*:messages", b"user:123:messages"));
    }

    #[test]
    fn test_pattern_matching_question() {
        assert!(PubSubManager::matches_pattern(b"h?llo", b"hello"));
        assert!(PubSubManager::matches_pattern(b"h?llo", b"hallo"));
        assert!(!PubSubManager::matches_pattern(b"h?llo", b"hllo"));
        assert!(!PubSubManager::matches_pattern(b"h?llo", b"heello"));
    }

    #[test]
    fn test_pattern_matching_brackets() {
        assert!(PubSubManager::matches_pattern(b"h[ae]llo", b"hello"));
        assert!(PubSubManager::matches_pattern(b"h[ae]llo", b"hallo"));
        assert!(!PubSubManager::matches_pattern(b"h[ae]llo", b"hillo"));

        // Negated brackets
        assert!(!PubSubManager::matches_pattern(b"h[^ae]llo", b"hello"));
        assert!(PubSubManager::matches_pattern(b"h[^ae]llo", b"hillo"));

        // Range
        assert!(PubSubManager::matches_pattern(b"user:[0-9]", b"user:5"));
        assert!(!PubSubManager::matches_pattern(b"user:[0-9]", b"user:a"));
    }

    #[test]
    fn test_pattern_matching_escape() {
        assert!(PubSubManager::matches_pattern(b"hello\\*world", b"hello*world"));
        assert!(!PubSubManager::matches_pattern(b"hello\\*world", b"helloXworld"));
    }

    #[tokio::test]
    async fn test_subscribe_unsubscribe() {
        let manager = PubSubManager::new();

        // Register client
        let (client_id, _rx) = manager.register_client().await;

        // Subscribe to channels
        let results = manager.subscribe(client_id, vec![b"news".to_vec(), b"sports".to_vec()]).await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, 1); // First subscription
        assert_eq!(results[1].1, 2); // Second subscription

        // Check subscription count
        assert_eq!(manager.get_subscription_count(client_id).await, 2);

        // Unsubscribe from one channel
        let results = manager.unsubscribe(client_id, Some(vec![b"news".to_vec()])).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 1); // One subscription remaining

        // Check subscription count
        assert_eq!(manager.get_subscription_count(client_id).await, 1);
    }

    #[tokio::test]
    async fn test_publish() {
        let manager = PubSubManager::new();

        // Register two clients
        let (client1, mut rx1) = manager.register_client().await;
        let (client2, mut rx2) = manager.register_client().await;

        // Subscribe both to "news"
        manager.subscribe(client1, vec![b"news".to_vec()]).await;
        manager.subscribe(client2, vec![b"news".to_vec()]).await;

        // Note: Subscription confirmations are now sent directly to clients via response buffer,
        // not through the broadcast channel. Only actual messages go through broadcast.

        // Publish message
        let recipients = manager.publish(b"news".to_vec(), b"Hello!".to_vec()).await;
        assert_eq!(recipients, 2);

        // Check that both clients received the message
        if let Ok(SubscriptionMessage::Message { channel, data }) = rx1.recv().await {
            assert_eq!(channel, b"news".to_vec());
            assert_eq!(data, b"Hello!".to_vec());
        } else {
            panic!("Expected Message");
        }

        if let Ok(SubscriptionMessage::Message { channel, data }) = rx2.recv().await {
            assert_eq!(channel, b"news".to_vec());
            assert_eq!(data, b"Hello!".to_vec());
        } else {
            panic!("Expected Message");
        }
    }

    #[tokio::test]
    async fn test_pattern_subscription() {
        let manager = PubSubManager::new();

        let (client_id, mut rx) = manager.register_client().await;

        // Subscribe to pattern
        manager.psubscribe(client_id, vec![b"news:*".to_vec()]).await;

        // Note: Subscription confirmations are now sent directly to clients via response buffer,
        // not through the broadcast channel. Only actual messages go through broadcast.

        // Publish to matching channel
        let recipients = manager.publish(b"news:sports".to_vec(), b"Goal!".to_vec()).await;
        assert_eq!(recipients, 1);

        // Check pattern message
        if let Ok(SubscriptionMessage::PMessage { pattern, channel, data }) = rx.recv().await {
            assert_eq!(pattern, b"news:*".to_vec());
            assert_eq!(channel, b"news:sports".to_vec());
            assert_eq!(data, b"Goal!".to_vec());
        } else {
            panic!("Expected PMessage");
        }

        // Publish to non-matching channel
        let recipients = manager.publish(b"weather:rain".to_vec(), b"It's raining".to_vec()).await;
        assert_eq!(recipients, 0);
    }

    #[tokio::test]
    async fn test_pubsub_channels() {
        let manager = PubSubManager::new();

        let (client_id, _rx) = manager.register_client().await;

        manager.subscribe(client_id, vec![
            b"news:sports".to_vec(),
            b"news:weather".to_vec(),
            b"tech:ai".to_vec(),
        ]).await;

        // Get all channels
        let channels = manager.pubsub_channels(None).await;
        assert_eq!(channels.len(), 3);

        // Get channels matching pattern
        let channels = manager.pubsub_channels(Some(b"news:*")).await;
        assert_eq!(channels.len(), 2);
    }

    #[tokio::test]
    async fn test_pubsub_numsub() {
        let manager = PubSubManager::new();

        let (client1, _rx1) = manager.register_client().await;
        let (client2, _rx2) = manager.register_client().await;

        manager.subscribe(client1, vec![b"news".to_vec()]).await;
        manager.subscribe(client2, vec![b"news".to_vec(), b"sports".to_vec()]).await;

        let counts = manager.pubsub_numsub(&[b"news".to_vec(), b"sports".to_vec(), b"unknown".to_vec()]).await;

        assert_eq!(counts.len(), 3);
        assert_eq!(counts[0], (b"news".to_vec(), 2));
        assert_eq!(counts[1], (b"sports".to_vec(), 1));
        assert_eq!(counts[2], (b"unknown".to_vec(), 0));
    }

    #[tokio::test]
    async fn test_pubsub_numpat() {
        let manager = PubSubManager::new();

        let (client1, _rx1) = manager.register_client().await;
        let (client2, _rx2) = manager.register_client().await;

        manager.psubscribe(client1, vec![b"news:*".to_vec()]).await;
        manager.psubscribe(client2, vec![b"news:*".to_vec(), b"tech:*".to_vec()]).await;

        // Should count unique patterns, not subscriptions
        let numpat = manager.pubsub_numpat().await;
        assert_eq!(numpat, 2);
    }

    #[tokio::test]
    async fn test_unregister_client() {
        let manager = PubSubManager::new();

        let (client_id, _rx) = manager.register_client().await;

        manager.subscribe(client_id, vec![b"news".to_vec()]).await;
        manager.psubscribe(client_id, vec![b"tech:*".to_vec()]).await;

        // Verify subscriptions exist
        let channels = manager.pubsub_channels(None).await;
        assert_eq!(channels.len(), 1);
        assert_eq!(manager.pubsub_numpat().await, 1);

        // Unregister client
        manager.unregister_client(client_id).await;

        // Verify subscriptions are cleaned up
        let channels = manager.pubsub_channels(None).await;
        assert_eq!(channels.len(), 0);
        assert_eq!(manager.pubsub_numpat().await, 0);
    }

    // ==================== Sharded Pub/Sub Tests ====================

    #[tokio::test]
    async fn test_ssubscribe_sunsubscribe() {
        let manager = PubSubManager::new();

        let (client_id, _rx) = manager.register_client().await;

        // Subscribe to shard channels
        let results = manager
            .ssubscribe(client_id, vec![b"shard:news".to_vec(), b"shard:sports".to_vec()])
            .await;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1, 1); // First subscription
        assert_eq!(results[1].1, 2); // Second subscription

        // Check subscription count includes shard channels
        assert_eq!(manager.get_subscription_count(client_id).await, 2);

        // Unsubscribe from one shard channel
        let results = manager
            .sunsubscribe(client_id, Some(vec![b"shard:news".to_vec()]))
            .await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1, 1); // One subscription remaining

        assert_eq!(manager.get_subscription_count(client_id).await, 1);
    }

    #[tokio::test]
    async fn test_spublish() {
        let manager = PubSubManager::new();

        let (client1, mut rx1) = manager.register_client().await;
        let (client2, mut rx2) = manager.register_client().await;

        // Subscribe both to shard channel
        manager
            .ssubscribe(client1, vec![b"shard:news".to_vec()])
            .await;
        manager
            .ssubscribe(client2, vec![b"shard:news".to_vec()])
            .await;

        // Publish to shard channel
        let recipients = manager
            .spublish(b"shard:news".to_vec(), b"Shard Hello!".to_vec())
            .await;
        assert_eq!(recipients, 2);

        // Check that both clients received the shard message
        if let Ok(SubscriptionMessage::SMessage { channel, data }) = rx1.recv().await {
            assert_eq!(channel, b"shard:news".to_vec());
            assert_eq!(data, b"Shard Hello!".to_vec());
        } else {
            panic!("Expected SMessage");
        }

        if let Ok(SubscriptionMessage::SMessage { channel, data }) = rx2.recv().await {
            assert_eq!(channel, b"shard:news".to_vec());
            assert_eq!(data, b"Shard Hello!".to_vec());
        } else {
            panic!("Expected SMessage");
        }
    }

    #[tokio::test]
    async fn test_spublish_no_pattern_matching() {
        let manager = PubSubManager::new();

        let (client_id, mut rx) = manager.register_client().await;

        // Subscribe to a pattern
        manager
            .psubscribe(client_id, vec![b"shard:*".to_vec()])
            .await;

        // Publish to shard channel - should NOT trigger pattern matching
        let recipients = manager
            .spublish(b"shard:news".to_vec(), b"Shard msg".to_vec())
            .await;
        assert_eq!(recipients, 0);

        // Verify no message received (try_recv should fail)
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_spublish_regular_publish_independence() {
        let manager = PubSubManager::new();

        let (shard_client, mut shard_rx) = manager.register_client().await;
        let (regular_client, mut regular_rx) = manager.register_client().await;

        // One client subscribes to shard channel, other to regular channel (same name)
        manager
            .ssubscribe(shard_client, vec![b"channel1".to_vec()])
            .await;
        manager
            .subscribe(regular_client, vec![b"channel1".to_vec()])
            .await;

        // Regular PUBLISH should only go to regular subscriber
        let recipients = manager
            .publish(b"channel1".to_vec(), b"regular msg".to_vec())
            .await;
        assert_eq!(recipients, 1);

        if let Ok(SubscriptionMessage::Message { channel, data }) = regular_rx.recv().await {
            assert_eq!(channel, b"channel1".to_vec());
            assert_eq!(data, b"regular msg".to_vec());
        } else {
            panic!("Expected Message for regular subscriber");
        }

        // SPUBLISH should only go to shard subscriber
        let recipients = manager
            .spublish(b"channel1".to_vec(), b"shard msg".to_vec())
            .await;
        assert_eq!(recipients, 1);

        if let Ok(SubscriptionMessage::SMessage { channel, data }) = shard_rx.recv().await {
            assert_eq!(channel, b"channel1".to_vec());
            assert_eq!(data, b"shard msg".to_vec());
        } else {
            panic!("Expected SMessage for shard subscriber");
        }
    }

    #[tokio::test]
    async fn test_pubsub_shardchannels() {
        let manager = PubSubManager::new();

        let (client_id, _rx) = manager.register_client().await;

        manager
            .ssubscribe(
                client_id,
                vec![
                    b"shard:sports".to_vec(),
                    b"shard:weather".to_vec(),
                    b"other:ai".to_vec(),
                ],
            )
            .await;

        // Get all shard channels
        let channels = manager.pubsub_shardchannels(None).await;
        assert_eq!(channels.len(), 3);

        // Get shard channels matching pattern
        let channels = manager.pubsub_shardchannels(Some(b"shard:*")).await;
        assert_eq!(channels.len(), 2);
    }

    #[tokio::test]
    async fn test_pubsub_shardnumsub() {
        let manager = PubSubManager::new();

        let (client1, _rx1) = manager.register_client().await;
        let (client2, _rx2) = manager.register_client().await;

        manager
            .ssubscribe(client1, vec![b"shard:news".to_vec()])
            .await;
        manager
            .ssubscribe(
                client2,
                vec![b"shard:news".to_vec(), b"shard:sports".to_vec()],
            )
            .await;

        let counts = manager
            .pubsub_shardnumsub(&[
                b"shard:news".to_vec(),
                b"shard:sports".to_vec(),
                b"unknown".to_vec(),
            ])
            .await;

        assert_eq!(counts.len(), 3);
        assert_eq!(counts[0], (b"shard:news".to_vec(), 2));
        assert_eq!(counts[1], (b"shard:sports".to_vec(), 1));
        assert_eq!(counts[2], (b"unknown".to_vec(), 0));
    }

    #[tokio::test]
    async fn test_sunsubscribe_all() {
        let manager = PubSubManager::new();

        let (client_id, _rx) = manager.register_client().await;

        manager
            .ssubscribe(
                client_id,
                vec![b"shard:a".to_vec(), b"shard:b".to_vec(), b"shard:c".to_vec()],
            )
            .await;

        assert_eq!(manager.get_subscription_count(client_id).await, 3);

        // Unsubscribe from all shard channels (None = all)
        let results = manager.sunsubscribe(client_id, None).await;
        assert_eq!(results.len(), 3);

        // Counts decrease as each channel is unsubscribed (order is non-deterministic)
        // The last result should have count 0
        let mut counts: Vec<usize> = results.iter().map(|r| r.1).collect();
        counts.sort();
        assert_eq!(counts, vec![0, 1, 2]);

        assert_eq!(manager.get_subscription_count(client_id).await, 0);
    }

    #[tokio::test]
    async fn test_unregister_client_with_shard_channels() {
        let manager = PubSubManager::new();

        let (client_id, _rx) = manager.register_client().await;

        manager
            .subscribe(client_id, vec![b"regular".to_vec()])
            .await;
        manager
            .psubscribe(client_id, vec![b"pattern:*".to_vec()])
            .await;
        manager
            .ssubscribe(client_id, vec![b"shard:ch".to_vec()])
            .await;

        // Verify all subscriptions exist
        assert_eq!(manager.pubsub_channels(None).await.len(), 1);
        assert_eq!(manager.pubsub_numpat().await, 1);
        assert_eq!(manager.pubsub_shardchannels(None).await.len(), 1);

        // Unregister client - should clean up everything
        manager.unregister_client(client_id).await;

        assert_eq!(manager.pubsub_channels(None).await.len(), 0);
        assert_eq!(manager.pubsub_numpat().await, 0);
        assert_eq!(manager.pubsub_shardchannels(None).await.len(), 0);
    }

    #[tokio::test]
    async fn test_mixed_subscription_count() {
        let manager = PubSubManager::new();

        let (client_id, _rx) = manager.register_client().await;

        // Subscribe to regular channel
        let results = manager
            .subscribe(client_id, vec![b"regular".to_vec()])
            .await;
        assert_eq!(results[0].1, 1);

        // Subscribe to pattern
        let results = manager
            .psubscribe(client_id, vec![b"pattern:*".to_vec()])
            .await;
        assert_eq!(results[0].1, 2);

        // Subscribe to shard channel
        let results = manager
            .ssubscribe(client_id, vec![b"shard:ch".to_vec()])
            .await;
        assert_eq!(results[0].1, 3);

        // Total should be 3
        assert_eq!(manager.get_subscription_count(client_id).await, 3);
    }
}
