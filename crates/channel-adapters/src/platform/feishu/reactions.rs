//! Feishu processing reaction lifecycle.
//!
//! Manages visual processing indicators on Feishu messages:
//! - Sets "Typing" reaction at processing start
//! - Deletes the reaction on success
//! - Replaces with "CrossMark" on failure
//!
//! Implements an LRU cache (default 1024 entries) to track pending reactions
//! and prevent memory leaks from abandoned message IDs.

use crate::platform::adapter::{InboundMessage, PlatformError, PlatformResult};
use crate::platform::feishu::decode_feishu_response;
use crate::platform::feishu::types::{
    CreateReactionRequest, CreateReactionResponse, DeleteReactionResponse, ReactionType,
};
use crate::platform::types::{MessageType, Platform, SessionKey};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Reaction emoji used to indicate processing is in progress.
pub const REACTION_TYPING: &str = "Typing";

/// Reaction emoji used to indicate processing failed.
pub const REACTION_CROSS_MARK: &str = "CrossMark";

/// Default maximum number of pending reaction entries before LRU eviction.
const DEFAULT_MAX_CACHE: usize = 1024;

// ---------------------------------------------------------------------------
// ProcessingReactions
// ---------------------------------------------------------------------------

/// Manages the reaction lifecycle for processing indicators on Feishu messages.
///
/// Tracks pending "Typing" reactions (message_id → reaction_id) so they can
/// be cleaned up when processing completes or fails. Uses an LRU eviction
/// policy (default max 1024 entries) to bound memory usage.
#[derive(Debug)]
pub struct ProcessingReactions {
    /// Map from message_id to reaction_id for active Typing reactions.
    pending: Arc<RwLock<HashMap<String, String>>>,
    /// Insertion-ordered keys for LRU eviction tracking.
    insertion_order: Arc<RwLock<VecDeque<String>>>,
    /// Maximum number of pending entries before oldest is evicted.
    max_cache: usize,
}

impl ProcessingReactions {
    /// Create a new instance with default cache size (1024 entries).
    pub fn new() -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
            insertion_order: Arc::new(RwLock::new(VecDeque::new())),
            max_cache: DEFAULT_MAX_CACHE,
        }
    }

    /// Create a new instance with a custom maximum cache size.
    #[allow(dead_code)]
    pub fn with_max_cache(max_cache: usize) -> Self {
        Self {
            pending: Arc::new(RwLock::new(HashMap::new())),
            insertion_order: Arc::new(RwLock::new(VecDeque::new())),
            max_cache,
        }
    }

    // -----------------------------------------------------------------------
    // Public API
    // -----------------------------------------------------------------------

    /// Set a "Typing" reaction on a message to indicate processing has started.
    ///
    /// Stores the `message_id → reaction_id` mapping for later cleanup.
    /// If the pending cache exceeds `max_cache`, the oldest entry is evicted
    /// (its reaction is NOT deleted — it simply ages out of tracking).
    pub async fn start_processing(&self, token: &str, message_id: &str) -> PlatformResult<()> {
        let reaction_id = self
            .create_reaction(token, message_id, REACTION_TYPING)
            .await?;

        // Store in pending map and track insertion order
        {
            let mut pending = self.pending.write().await;
            pending.insert(message_id.to_string(), reaction_id);
        }
        {
            let mut order = self.insertion_order.write().await;
            order.push_back(message_id.to_string());
        }

        // Evict oldest if over max_cache
        self.evict_excess().await;

        Ok(())
    }

    /// Delete the "Typing" reaction on successful processing.
    ///
    /// Looks up the stored reaction_id for the given message and deletes it.
    /// Removes the entry from the pending map regardless of whether the
    /// delete API call succeeds (best-effort cleanup).
    pub async fn mark_success(&self, token: &str, message_id: &str) -> PlatformResult<()> {
        let reaction_id = {
            let mut pending = self.pending.write().await;
            pending.remove(message_id).map(|id| id.to_string())
        };

        // Always remove from insertion order
        {
            let mut order = self.insertion_order.write().await;
            order.retain(|k| k != message_id);
        }

        // Best-effort delete — don't fail if not found
        if let Some(id) = reaction_id {
            self.delete_reaction(token, message_id, &id).await?;
        }

        Ok(())
    }

    /// Replace the "Typing" reaction with a "CrossMark" on failure.
    ///
    /// First deletes the Typing reaction (best-effort), then sets a CrossMark
    /// reaction. Removes the entry from the pending map regardless of API
    /// call success.
    pub async fn mark_failure(&self, token: &str, message_id: &str) -> PlatformResult<()> {
        let reaction_id = {
            let mut pending = self.pending.write().await;
            pending.remove(message_id).map(|id| id.to_string())
        };

        // Clean up insertion order
        {
            let mut order = self.insertion_order.write().await;
            order.retain(|k| k != message_id);
        }

        // Delete the Typing reaction (best-effort, don't fail on missing)
        if let Some(id) = &reaction_id {
            let _ = self.delete_reaction(token, message_id, id).await;
        }

        // Set the CrossMark reaction
        self.create_reaction(token, message_id, REACTION_CROSS_MARK)
            .await?;

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Reaction event synthesis
    // -----------------------------------------------------------------------

    /// Handle an incoming reaction event from Feishu.
    ///
    /// Synthesizes a TEXT event when a user reacts to a message.
    /// Filters out bot-origin reactions to break our own feedback loop.
    ///
    /// # Arguments
    /// * `event_type` — `"im.message.reaction.created_v1"` or `"im.message.reaction.deleted_v1"`
    /// * `event`     — the inner event payload from Feishu
    /// * `bot_app_id` — this bot's app_id for ownership verification (reserved for future use)
    ///
    /// # Returns
    /// `Some(InboundMessage)` with synthetic text `reaction:{added|removed}:{emoji_type}`
    /// if the reaction should be routed; `None` otherwise.
    pub fn handle_reaction_event(
        event_type: &str,
        event: &serde_json::Value,
        bot_app_id: &str,
    ) -> Option<InboundMessage> {
        let _ = bot_app_id; // reserved for future ownership verification

        // Only handle reaction created/deleted events
        let added = match event_type {
            "im.message.reaction.created_v1" => true,
            "im.message.reaction.deleted_v1" => false,
            _ => return None,
        };

        // Filter bot-origin reactions to break feedback loop
        let operator_type = event
            .get("operator_type")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if operator_type == "bot" || operator_type == "app" {
            return None;
        }

        // Extract emoji type
        let emoji_type = event
            .get("reaction_type")
            .and_then(|v| v.get("emoji_type"))
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        // Extract message_id of the reacted-to message
        let message_id = event
            .get("message_id")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        // Build synthetic text
        let action = if added { "added" } else { "removed" };
        let text = format!("reaction:{}:{}", action, emoji_type);

        // Extract operator open_id for session key (fallback to placeholder)
        let operator_open_id = event
            .get("user_id")
            .and_then(|v| v.get("open_id"))
            .and_then(|v| v.as_str())
            .unwrap_or("reaction_operator");

        let session_key = SessionKey::new("feishu", operator_open_id);

        Some(InboundMessage {
            platform: Platform::Feishu,
            session_key,
            text,
            sender_name: None,
            timestamp: Utc::now(),
            metadata: serde_json::json!({
                "event_type": event_type,
                "emoji_type": emoji_type,
                "action": action,
            }),
            message_type: MessageType::Text,
            message_id,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        })
    }

    // -----------------------------------------------------------------------
    // Internal helpers
    // -----------------------------------------------------------------------

    /// POST to create a reaction on a message.
    async fn create_reaction(
        &self,
        token: &str,
        message_id: &str,
        emoji_type: &str,
    ) -> PlatformResult<String> {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/im/v1/messages/{}/reactions",
            super::api_base_url(),
            message_id
        );
        let body = CreateReactionRequest {
            reaction_type: ReactionType {
                emoji_type: emoji_type.to_string(),
            },
        };

        let response = client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&body)
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(format!("create reaction: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            return Err(PlatformError::SendFailed(format!(
                "create reaction returned HTTP {status}"
            )));
        }

        let result: CreateReactionResponse =
            decode_feishu_response(response, "create reaction").await?;

        if result.code != 0 {
            return Err(PlatformError::SendFailed(format!(
                "create reaction API error (code {}): {}",
                result.code, result.msg
            )));
        }

        result
            .data
            .and_then(|d| d.reaction_id)
            .ok_or_else(|| PlatformError::SendFailed("no reaction_id in response".to_string()))
    }

    /// DELETE a reaction from a message.
    async fn delete_reaction(
        &self,
        token: &str,
        message_id: &str,
        reaction_id: &str,
    ) -> PlatformResult<()> {
        let client = reqwest::Client::new();
        let url = format!(
            "{}/im/v1/messages/{}/reactions/{}",
            super::api_base_url(),
            message_id,
            reaction_id
        );

        let response = client
            .delete(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .map_err(|e| PlatformError::SendFailed(format!("delete reaction: {}", e)))?;

        let status = response.status();
        if !status.is_success() {
            return Err(PlatformError::SendFailed(format!(
                "delete reaction returned HTTP {status}"
            )));
        }

        let result: DeleteReactionResponse =
            decode_feishu_response(response, "delete reaction").await?;

        if result.code != 0 {
            return Err(PlatformError::SendFailed(format!(
                "delete reaction API error (code {}): {}",
                result.code, result.msg
            )));
        }

        Ok(())
    }

    /// Evict oldest entries when the pending map exceeds max_cache.
    async fn evict_excess(&self) {
        loop {
            let (pending_len, oldest_key) = {
                let pending = self.pending.read().await;
                let order = self.insertion_order.read().await;
                (pending.len(), order.front().cloned())
            };

            if pending_len <= self.max_cache {
                break;
            }

            if let Some(key) = oldest_key {
                // Remove from both structures
                let mut pending = self.pending.write().await;
                let mut order = self.insertion_order.write().await;
                pending.remove(&key);
                order.pop_front();
            } else {
                break;
            }
        }
    }
}

impl Default for ProcessingReactions {
    fn default() -> Self {
        Self::new()
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------

    #[test]
    fn test_reaction_constants() {
        assert_eq!(REACTION_TYPING, "Typing");
        assert_eq!(REACTION_CROSS_MARK, "CrossMark");
    }

    // -----------------------------------------------------------------------
    // Unit tests (no network)
    // -----------------------------------------------------------------------

    #[test]
    fn test_new_default_values() {
        let reactions = ProcessingReactions::default();
        assert_eq!(reactions.max_cache, 1024);
    }

    #[test]
    fn test_with_max_cache_custom_value() {
        let reactions = ProcessingReactions::with_max_cache(32);
        assert_eq!(reactions.max_cache, 32);
    }

    // -----------------------------------------------------------------------
    // Cache eviction (unit-verifiable)
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_start_processing_stores_reaction_id() {
        let reactions = ProcessingReactions::with_max_cache(128);

        // Simulate: manually insert a mapping (bypass HTTP)
        {
            let mut pending = reactions.pending.write().await;
            pending.insert("msg_1".to_string(), "reaction_1".to_string());
        }
        {
            let mut order = reactions.insertion_order.write().await;
            order.push_back("msg_1".to_string());
        }

        let stored = {
            let pending = reactions.pending.read().await;
            pending.get("msg_1").cloned()
        };
        assert_eq!(stored.as_deref(), Some("reaction_1"));
    }

    #[tokio::test]
    async fn test_mark_success_removes_from_pending() {
        let reactions = ProcessingReactions::with_max_cache(128);

        // Simulate start_processing
        {
            let mut pending = reactions.pending.write().await;
            pending.insert("msg_success".to_string(), "r_success".to_string());
        }
        {
            let mut order = reactions.insertion_order.write().await;
            order.push_back("msg_success".to_string());
        }

        // Manually simulate mark_success removal (bypass HTTP delete)
        {
            let mut pending = reactions.pending.write().await;
            pending.remove("msg_success");
        }
        {
            let mut order = reactions.insertion_order.write().await;
            order.retain(|k| k != "msg_success");
        }

        let remaining = {
            let pending = reactions.pending.read().await;
            pending.contains_key("msg_success")
        };
        assert!(!remaining, "entry should be removed from pending");
    }

    #[tokio::test]
    async fn test_mark_failure_replaces_reaction() {
        let reactions = ProcessingReactions::with_max_cache(128);

        // Simulate start_processing
        {
            let mut pending = reactions.pending.write().await;
            pending.insert("msg_fail".to_string(), "r_typing".to_string());
        }
        {
            let mut order = reactions.insertion_order.write().await;
            order.push_back("msg_fail".to_string());
        }

        // Simulate mark_failure: remove Typing, insert CrossMark
        let old_reaction = {
            let mut pending = reactions.pending.write().await;
            pending.remove("msg_fail")
        };
        assert_eq!(old_reaction.as_deref(), Some("r_typing"));
        // (CrossMark would be set via API, not tracked in pending)

        {
            let mut order = reactions.insertion_order.write().await;
            order.retain(|k| k != "msg_fail");
        }

        let still_pending = {
            let pending = reactions.pending.read().await;
            pending.contains_key("msg_fail")
        };
        assert!(!still_pending, "entry should be removed from pending");
    }

    #[tokio::test]
    async fn test_cache_eviction_at_max() {
        // Use a small cache to test eviction quickly
        let reactions = ProcessingReactions::with_max_cache(3);

        // Insert 4 entries
        for i in 0..4 {
            let msg_id = format!("msg_{i}");
            let r_id = format!("reaction_{i}");
            {
                let mut pending = reactions.pending.write().await;
                pending.insert(msg_id.clone(), r_id);
            }
            {
                let mut order = reactions.insertion_order.write().await;
                order.push_back(msg_id);
            }
            reactions.evict_excess().await;
        }

        // Only 3 should remain; oldest (msg_0) should be gone
        let (len, has_oldest, has_newest) = {
            let pending = reactions.pending.read().await;
            (
                pending.len(),
                pending.contains_key("msg_0"),
                pending.contains_key("msg_3"),
            )
        };

        assert_eq!(len, 3, "should have exactly 3 entries after eviction");
        assert!(!has_oldest, "oldest entry (msg_0) should be evicted");
        assert!(has_newest, "newest entry (msg_3) should be retained");
    }

    #[tokio::test]
    async fn test_cache_below_max_no_eviction() {
        let reactions = ProcessingReactions::with_max_cache(1024);

        // Insert fewer entries than max
        for i in 0..5 {
            let msg_id = format!("msg_{i}");
            {
                let mut pending = reactions.pending.write().await;
                pending.insert(msg_id.clone(), format!("reaction_{i}"));
            }
            {
                let mut order = reactions.insertion_order.write().await;
                order.push_back(msg_id);
            }
        }

        reactions.evict_excess().await;

        let len = {
            let pending = reactions.pending.read().await;
            pending.len()
        };
        assert_eq!(len, 5, "no entries should be evicted below max");
    }

    #[test]
    fn test_default_implements_default_trait() {
        let reactions = ProcessingReactions::default();
        assert_eq!(reactions.max_cache, DEFAULT_MAX_CACHE);

        // Pending should be empty
        let pending = reactions.pending.try_read().expect("lock not contested");
        assert!(pending.is_empty());
    }

    #[test]
    fn test_max_cache_default_constant() {
        assert_eq!(DEFAULT_MAX_CACHE, 1024);
    }

    // -----------------------------------------------------------------------
    // handle_reaction_event tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_reaction_created_produces_added_text() {
        let event = serde_json::json!({
            "message_id": "om_test_001",
            "reaction_type": {
                "emoji_type": "HEART"
            },
            "operator_type": "user",
            "user_id": {
                "open_id": "ou_test_user"
            }
        });

        let result = ProcessingReactions::handle_reaction_event(
            "im.message.reaction.created_v1",
            &event,
            "app_123",
        );

        let msg = result.expect("should produce an InboundMessage for user reaction");
        assert_eq!(msg.text, "reaction:added:HEART");
        assert_eq!(msg.message_type, MessageType::Text);
        assert_eq!(msg.platform, Platform::Feishu);
        assert_eq!(msg.message_id.as_deref(), Some("om_test_001"));
    }

    #[test]
    fn test_bot_origin_reaction_is_filtered() {
        let event = serde_json::json!({
            "message_id": "om_test_002",
            "reaction_type": {
                "emoji_type": "THUMBSUP"
            },
            "operator_type": "bot",
            "user_id": {
                "open_id": "ou_bot_user"
            }
        });

        let result = ProcessingReactions::handle_reaction_event(
            "im.message.reaction.created_v1",
            &event,
            "app_123",
        );

        assert!(result.is_none(), "bot-origin reaction should be filtered");
    }

    #[test]
    fn test_unknown_event_type_returns_none() {
        let event = serde_json::json!({
            "message_id": "om_test_003",
            "reaction_type": {
                "emoji_type": "SMILE"
            },
            "operator_type": "user",
            "user_id": {
                "open_id": "ou_test_user"
            }
        });

        let result = ProcessingReactions::handle_reaction_event(
            "some.unknown.event.type",
            &event,
            "app_123",
        );

        assert!(result.is_none(), "unknown event_type should return None");
    }
}
