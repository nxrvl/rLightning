//! Cached monotonic clock to avoid frequent `Instant::now()` syscalls.
//!
//! Provides `cached_now()` which returns a cached `Instant` value updated
//! every 1ms by a background Tokio timer. On hot paths (GET, EXISTS, TTL),
//! this avoids the ~25ns syscall overhead per operation.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::LazyLock;
use std::time::Instant;

/// The epoch instant from which all cached timestamps are measured.
static CLOCK_EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Cached offset from CLOCK_EPOCH in microseconds.
/// Updated every 1ms by the background timer task.
static CACHED_OFFSET_US: AtomicU64 = AtomicU64::new(0);

/// Cached wall-clock time in milliseconds since UNIX epoch.
/// Updated every 1ms by the background timer task alongside CACHED_OFFSET_US.
static CACHED_WALL_MS: AtomicU64 = AtomicU64::new(0);

/// Get the current cached `Instant`.
///
/// Returns a value that is at most ~1ms stale compared to `Instant::now()`.
/// This is acceptable for TTL expiration checks (Redis itself uses 1-second
/// resolution for active expiry) and LRU clock updates.
#[inline]
pub fn cached_now() -> Instant {
    let offset_us = CACHED_OFFSET_US.load(Ordering::Relaxed);
    *CLOCK_EPOCH + std::time::Duration::from_micros(offset_us)
}

/// Get the current cached wall-clock time in milliseconds since UNIX epoch.
///
/// Returns a value that is at most ~1ms stale compared to `SystemTime::now()`.
/// Used by XADD stream ID generation to avoid `SystemTime::now()` syscalls
/// on the hot path.
#[inline]
pub fn cached_now_ms() -> u64 {
    CACHED_WALL_MS.load(Ordering::Relaxed)
}

/// Start the background clock updater task.
/// Should be called once during server startup (from StorageEngine::new).
/// Note: In test environments with multiple Tokio runtimes, this may spawn
/// duplicate tasks. This is harmless — all updaters write to the same AtomicU64,
/// and each test runtime needs its own updater since previous runtimes' tasks
/// are dropped when the runtime shuts down.
pub fn start_clock_updater() {
    // Force CLOCK_EPOCH initialization
    let _ = *CLOCK_EPOCH;

    // Initialize wall-clock time immediately so cached_now_ms() returns
    // a reasonable value even before the first tick.
    let wall_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    CACHED_WALL_MS.store(wall_ms, Ordering::Relaxed);

    tokio::spawn(async {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(1));
        loop {
            interval.tick().await;
            let elapsed = CLOCK_EPOCH.elapsed();
            CACHED_OFFSET_US.store(elapsed.as_micros() as u64, Ordering::Relaxed);
            let wall_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            CACHED_WALL_MS.store(wall_ms, Ordering::Relaxed);
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_cached_clock_accuracy() {
        start_clock_updater();

        // Give the updater a moment to start
        tokio::time::sleep(Duration::from_millis(10)).await;

        // cached_now should be close to Instant::now()
        let real = Instant::now();
        let cached = cached_now();

        // Use a generous tolerance (50ms) appropriate for CI environments
        let diff = if real > cached {
            real - cached
        } else {
            cached - real
        };
        assert!(
            diff < Duration::from_millis(50),
            "Cached clock drift too large: {:?}",
            diff
        );
    }

    #[tokio::test]
    async fn test_cached_clock_advances() {
        start_clock_updater();
        tokio::time::sleep(Duration::from_millis(5)).await;

        let t1 = cached_now();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let t2 = cached_now();

        assert!(t2 > t1, "Cached clock should advance over time");
    }

    #[test]
    fn test_cached_now_does_not_panic_without_updater() {
        // Even without the background task, cached_now should return a valid instant
        let _ = cached_now();
    }

    #[tokio::test]
    async fn test_cached_now_ms_returns_reasonable_epoch() {
        start_clock_updater();
        tokio::time::sleep(Duration::from_millis(10)).await;

        let cached = cached_now_ms();
        let real = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Should be within 100ms of real wall-clock time
        let diff = if real > cached {
            real - cached
        } else {
            cached - real
        };
        assert!(
            diff < 100,
            "cached_now_ms drift too large: {}ms",
            diff
        );

        // Should be a reasonable epoch time (after 2020-01-01)
        assert!(
            cached > 1_577_836_800_000,
            "cached_now_ms too small: {} (expected > 2020 epoch)",
            cached
        );
    }

    #[tokio::test]
    async fn test_cached_now_ms_monotonic() {
        start_clock_updater();
        tokio::time::sleep(Duration::from_millis(5)).await;

        let t1 = cached_now_ms();
        tokio::time::sleep(Duration::from_millis(10)).await;
        let t2 = cached_now_ms();

        assert!(
            t2 >= t1,
            "cached_now_ms should be monotonic: t1={}, t2={}",
            t1,
            t2
        );
    }
}
