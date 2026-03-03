use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::watch;

/// Manages blocking operations for list commands (BLPOP, BRPOP, BLMOVE, BLMPOP).
///
/// Uses per-key watch channels to signal when data is pushed to a key.
/// When a blocking command finds no data, it subscribes to the relevant keys'
/// watch channels and waits for a signal. When LPUSH/RPUSH adds data, it signals
/// the channel to wake up waiting clients.
pub struct BlockingManager {
    signals: DashMap<Vec<u8>, watch::Sender<u64>>,
    counter: AtomicU64,
}

impl Default for BlockingManager {
    fn default() -> Self {
        Self::new()
    }
}

impl BlockingManager {
    pub fn new() -> Self {
        BlockingManager {
            signals: DashMap::new(),
            counter: AtomicU64::new(0),
        }
    }

    /// Signal that data has been added to a key, waking up any blocked clients.
    pub fn notify_key(&self, key: &[u8]) {
        if let Some(sender) = self.signals.get(key) {
            let new_val = self.counter.fetch_add(1, Ordering::Relaxed);
            let _ = sender.send(new_val);
        }
    }

    /// Subscribe to notifications for a key. Returns a watch::Receiver that will
    /// be notified when data is pushed to this key.
    pub fn subscribe(&self, key: &[u8]) -> watch::Receiver<u64> {
        let entry = self.signals.entry(key.to_vec()).or_insert_with(|| {
            let (tx, _rx) = watch::channel(0);
            tx
        });
        entry.subscribe()
    }

    /// Remove entries where all receivers have been dropped to prevent memory leaks.
    /// Should be called periodically (e.g., from the TTL cleanup timer).
    pub fn cleanup_stale_entries(&self) {
        self.signals
            .retain(|_key, sender| sender.receiver_count() > 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_blocking_manager_notify() {
        let mgr = Arc::new(BlockingManager::new());
        let key = b"testkey".to_vec();

        let mut rx = mgr.subscribe(&key);

        // Notify should wake up the receiver
        mgr.notify_key(&key);

        let result = timeout(Duration::from_millis(100), rx.changed()).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_blocking_manager_no_notify_timeout() {
        let mgr = Arc::new(BlockingManager::new());
        let key = b"testkey".to_vec();

        let mut rx = mgr.subscribe(&key);

        // Without notify, changed() should not resolve
        let result = timeout(Duration::from_millis(50), rx.changed()).await;
        assert!(result.is_err()); // Timeout
    }

    #[tokio::test]
    async fn test_blocking_manager_multiple_keys() {
        let mgr = Arc::new(BlockingManager::new());
        let key1 = b"key1".to_vec();
        let key2 = b"key2".to_vec();

        let _rx1 = mgr.subscribe(&key1);
        let mut rx2 = mgr.subscribe(&key2);

        // Only notify key2
        mgr.notify_key(&key2);

        let result = timeout(Duration::from_millis(100), rx2.changed()).await;
        assert!(result.is_ok());
    }
}
