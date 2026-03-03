//! Pub/Sub (Publish/Subscribe) module for rLightning
//!
//! This module implements Redis-compatible Pub/Sub functionality allowing
//! publishers to send messages to channels without knowing who will receive them,
//! and subscribers to receive messages from channels they're interested in.
//!
//! # Key Components
//!
//! - `PubSubManager`: Central registry for channels and subscriptions
//! - `SubscriptionMessage`: Message types for pub/sub communication
//! - Pattern matching support for PSUBSCRIBE/PUNSUBSCRIBE
//!
//! # Example Flow
//!
//! ```text
//! Publisher → PUBLISH "news" "hello" → PubSubManager → Subscribers
//!                                           ↓
//!                                    Pattern matchers
//! ```

pub mod manager;

pub use manager::{ClientId, PubSubManager, SubscriptionMessage};
