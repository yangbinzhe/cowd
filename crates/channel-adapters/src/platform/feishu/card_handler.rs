//! Feishu interactive card action handler.
//!
//! Routes card button clicks as synthetic `COMMAND` events, matching
//! Hermes' `_handle_card_action_event` (feishu.py:2491-2540).
//!
//! ## Card Action Event Format (Feishu v2)
//!
//! When a user clicks a button on an interactive card, Feishu pushes
//! an event with the following structure:
//!
//! ```json
//! {
//!   "action": {
//!     "value": { "action": "approve" },
//!     "tag": "button",
//!     "option": "optional_option_value"
//!   },
//!   "open_id": "ou_xxx",
//!   "open_message_id": "om_xxx",
//!   "open_chat_id": "oc_xxx"
//! }
//! ```
//!
//! This module parses such events and produces synthetic `InboundMessage`
//! values with `message_type = Command` and text formatted as
//! `/card {tag} {value_json}`.

use crate::platform::adapter::{InboundMessage, MessageType};
use crate::platform::types::{Platform, SessionKey};
use chrono::Utc;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

/// Deduplication window for card action tokens (15 minutes).
const DEDUP_TTL: Duration = Duration::from_secs(15 * 60);

/// Global dedup store for card action tokens.
///
/// Maps `token → Instant` (insertion time).  Expired entries are pruned
/// on every `is_duplicate()` call to prevent unbounded growth.
static ACTION_TOKENS: std::sync::LazyLock<RwLock<HashMap<String, Instant>>> =
    std::sync::LazyLock::new(|| RwLock::new(HashMap::new()));

/// Handler for Feishu interactive card action events.
///
/// All methods are static — there is no instance state.
pub struct CardActionHandler;

impl CardActionHandler {
    /// Parse a card action trigger event and build a synthetic `COMMAND` MessageEvent.
    ///
    /// Extracts `action.value` and `action.tag` from the event JSON and
    /// constructs an `InboundMessage` with:
    ///
    /// - `message_type` = `Command`
    /// - `text` = `/card {tag} {value_json}`
    /// - `metadata` includes `operator_open_id`, `chat_id`, `message_id`
    ///
    /// Returns `None` when the event does not contain an `action` object.
    pub fn handle_card_action(
        event: &serde_json::Value,
        message_id: &str,
        chat_id: &str,
        operator_open_id: &str,
    ) -> Option<InboundMessage> {
        let action = event.get("action")?;
        let tag = action
            .get("tag")
            .and_then(|v| v.as_str())
            .unwrap_or("button");

        let value = action
            .get("value")
            .cloned()
            .unwrap_or(serde_json::Value::Null);

        // Build the synthetic command text: `/card {tag} {value_json}`
        let value_json = serde_json::to_string(&value).unwrap_or_else(|_| String::from("{}"));
        let text = format!("/card {} {}", tag, value_json);

        let session_key = SessionKey::with_thread("feishu", operator_open_id, chat_id);

        let mut metadata = serde_json::json!({
            "message_id": message_id,
            "chat_id": chat_id,
            "operator_open_id": operator_open_id,
            "action_tag": tag,
            "action_value": value,
        });
        if let Some(ha) = value.get("hermes_action").and_then(|v| v.as_str()) {
            metadata["hermes_action"] = serde_json::Value::String(ha.to_string());
        }
        if let Some(aid) = value.get("approval_id") {
            metadata["approval_id"] = aid.clone();
        }

        Some(InboundMessage {
            platform: Platform::Feishu,
            session_key,
            text,
            sender_name: None,
            timestamp: Utc::now(),
            metadata,
            message_type: MessageType::Command,
            message_id: Some(message_id.to_string()),
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        })
    }

    pub fn extract_hermes_action(event: &serde_json::Value) -> Option<String> {
        event
            .get("action")
            .and_then(|a| a.get("value"))
            .and_then(|v| v.get("hermes_action"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
    }

    pub fn extract_approval_id(event: &serde_json::Value) -> Option<u64> {
        event
            .get("action")
            .and_then(|a| a.get("value"))
            .and_then(|v| v.get("approval_id"))
            .and_then(|v| v.as_u64())
    }

    pub fn is_approval_card_action(event: &serde_json::Value) -> bool {
        Self::extract_hermes_action(event).is_some() && Self::extract_approval_id(event).is_some()
    }

    /// Check whether a card action token is a duplicate.
    ///
    /// Tokens are tracked in a global `RwLock<HashMap<String, Instant>>`
    /// with a **15-minute TTL**.  Each call prunes expired entries to
    /// keep the map bounded.
    ///
    /// Returns `true` if the token was already seen within the TTL window.
    pub fn is_duplicate(token: &str) -> bool {
        let mut map = ACTION_TOKENS
            .write()
            .expect("ACTION_TOKENS RwLock poisoned");

        // Prune expired entries
        let now = Instant::now();
        map.retain(|_, inserted_at| now.duration_since(*inserted_at) < DEDUP_TTL);

        // Check if this token exists
        if map.contains_key(token) {
            return true;
        }

        // Insert and return false (first time seen)
        map.insert(token.to_string(), now);
        false
    }

    /// Clear all dedup tokens (exposed for testing).
    #[cfg(test)]
    fn clear_tokens() {
        ACTION_TOKENS.write().expect("poisoned").clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ------------------------------------------------------------------
    // handle_card_action tests
    // ------------------------------------------------------------------

    #[test]
    fn test_button_action_builds_command_event() {
        let event = serde_json::json!({
            "action": {
                "value": { "action": "approve" },
                "tag": "button"
            },
            "open_id": "ou_abc123",
            "open_message_id": "om_msg001",
            "open_chat_id": "oc_chat001"
        });

        let msg =
            CardActionHandler::handle_card_action(&event, "om_msg001", "oc_chat001", "ou_abc123")
                .expect("should produce an InboundMessage");

        assert_eq!(msg.message_type, MessageType::Command);
        assert_eq!(msg.platform, Platform::Feishu);
        assert!(msg.text.starts_with("/card button "));
        assert!(msg.text.contains(r#""action""#));
        assert!(msg.text.contains("approve"));
        assert_eq!(msg.session_key.user_id, "ou_abc123");
        assert_eq!(msg.session_key.thread_id, Some("oc_chat001".to_string()));
        assert_eq!(msg.message_id.as_deref(), Some("om_msg001"));
        // Metadata should include both the raw action_value and tracking fields
        assert_eq!(msg.metadata["chat_id"], "oc_chat001");
        assert_eq!(msg.metadata["operator_open_id"], "ou_abc123");
        assert_eq!(msg.metadata["action_tag"], "button");
    }

    #[test]
    fn test_non_button_tag_preserved_in_command() {
        // Card actions can have tags other than "button" (e.g., "select_static",
        // "date_picker"). The tag should be preserved in the command text.
        let event = serde_json::json!({
            "action": {
                "value": { "option": "choice_a" },
                "tag": "select_static"
            }
        });

        let msg =
            CardActionHandler::handle_card_action(&event, "om_002", "oc_chat002", "ou_user002")
                .expect("should produce an InboundMessage");

        assert!(msg.text.starts_with("/card select_static "));
        assert_eq!(msg.message_type, MessageType::Command);
    }

    #[test]
    fn test_missing_action_field_returns_none() {
        let event = serde_json::json!({
            "open_id": "ou_xxx",
            "open_message_id": "om_yyy"
        });

        let result = CardActionHandler::handle_card_action(&event, "om_yyy", "oc_zzz", "ou_xxx");
        assert!(result.is_none());
    }

    #[test]
    fn test_missing_tag_defaults_to_button() {
        let event = serde_json::json!({
            "action": {
                "value": { "action": "do_something" }
            }
        });

        let msg =
            CardActionHandler::handle_card_action(&event, "om_003", "oc_chat003", "ou_user003")
                .expect("should produce an InboundMessage");

        assert!(msg.text.starts_with("/card button "));
    }

    #[test]
    fn test_empty_value_handled_as_null() {
        let event = serde_json::json!({
            "action": {
                "tag": "button"
            }
        });

        let msg =
            CardActionHandler::handle_card_action(&event, "om_004", "oc_chat004", "ou_user004")
                .expect("should produce an InboundMessage");

        // When value is missing, it's null
        assert!(msg.text.contains("null"));
        assert_eq!(msg.metadata["action_value"], serde_json::Value::Null);
    }

    // ------------------------------------------------------------------
    // is_duplicate tests
    // ------------------------------------------------------------------

    #[test]
    fn test_duplicate_token_returns_true() {
        CardActionHandler::clear_tokens();

        let token = "test_token_dup_001";
        // First call: not a duplicate
        assert!(!CardActionHandler::is_duplicate(token));
        // Second call: duplicate
        assert!(CardActionHandler::is_duplicate(token));
    }

    #[test]
    fn test_non_duplicate_token_returns_false() {
        CardActionHandler::clear_tokens();

        assert!(!CardActionHandler::is_duplicate("unique_token_a"));
        assert!(!CardActionHandler::is_duplicate("unique_token_b"));
    }

    #[test]
    fn test_different_tokens_are_not_duplicates() {
        CardActionHandler::clear_tokens();

        assert!(!CardActionHandler::is_duplicate("token_x"));
        assert!(!CardActionHandler::is_duplicate("token_y"));
        assert!(!CardActionHandler::is_duplicate("token_z"));
    }

    #[test]
    fn test_expired_tokens_are_pruned() {
        CardActionHandler::clear_tokens();

        // Insert a token with an artificially old timestamp
        {
            let mut map = ACTION_TOKENS.write().expect("poisoned");
            let expired = Instant::now()
                .checked_sub(DEDUP_TTL + Duration::from_secs(1))
                .expect("valid instant");
            map.insert("expired_token".to_string(), expired);
        }

        // The expired token should be pruned on the next is_duplicate call,
        // so this should return false (token not found).
        assert!(!CardActionHandler::is_duplicate("expired_token"));
    }

    #[test]
    fn test_prune_removes_multiple_expired_entries() {
        CardActionHandler::clear_tokens();

        // Insert several expired tokens and one fresh one
        {
            let mut map = ACTION_TOKENS.write().expect("poisoned");
            let expired = Instant::now()
                .checked_sub(DEDUP_TTL + Duration::from_secs(1))
                .expect("valid instant");
            map.insert("exp_a".to_string(), expired);
            map.insert("exp_b".to_string(), expired);
            map.insert("fresh".to_string(), Instant::now());
        }

        // Querying a new token triggers prune — expired ones should be removed
        assert!(!CardActionHandler::is_duplicate("new_token"));

        // The fresh token should still exist
        let map = ACTION_TOKENS.read().expect("poisoned");
        assert!(
            map.contains_key("fresh"),
            "fresh token should survive pruning"
        );
        assert!(!map.contains_key("exp_a"), "expired token should be pruned");
        assert!(!map.contains_key("exp_b"), "expired token should be pruned");
    }

    // ------------------------------------------------------------------
    // Approval card action extraction tests
    // ------------------------------------------------------------------

    #[test]
    fn test_extract_hermes_action_approve_once() {
        let event = serde_json::json!({
            "action": {
                "tag": "button",
                "value": {"hermes_action": "approve_once", "approval_id": 42}
            }
        });
        assert_eq!(
            CardActionHandler::extract_hermes_action(&event),
            Some("approve_once".to_string())
        );
        assert_eq!(CardActionHandler::extract_approval_id(&event), Some(42));
        assert!(CardActionHandler::is_approval_card_action(&event));
    }

    #[test]
    fn test_extract_hermes_action_deny() {
        let event = serde_json::json!({
            "action": {
                "tag": "button",
                "value": {"hermes_action": "deny", "approval_id": 99}
            }
        });
        assert_eq!(
            CardActionHandler::extract_hermes_action(&event),
            Some("deny".to_string())
        );
        assert_eq!(CardActionHandler::extract_approval_id(&event), Some(99));
        assert!(CardActionHandler::is_approval_card_action(&event));
    }

    #[test]
    fn test_extract_hermes_action_non_approval_card() {
        let event = serde_json::json!({
            "action": {
                "tag": "button",
                "value": {"action": "other"}
            }
        });
        assert_eq!(CardActionHandler::extract_hermes_action(&event), None);
        assert_eq!(CardActionHandler::extract_approval_id(&event), None);
        assert!(!CardActionHandler::is_approval_card_action(&event));
    }

    #[test]
    fn test_extract_hermes_action_missing_fields() {
        let event = serde_json::json!({
            "action": {
                "tag": "button",
                "value": {"hermes_action": "approve_once"}
            }
        });
        assert!(CardActionHandler::extract_hermes_action(&event).is_some());
        assert_eq!(CardActionHandler::extract_approval_id(&event), None);
        assert!(!CardActionHandler::is_approval_card_action(&event));
    }

    #[test]
    fn test_handle_card_action_includes_hermes_metadata() {
        let event = serde_json::json!({
            "action": {
                "tag": "button",
                "value": {"hermes_action": "approve_always", "approval_id": 7}
            },
            "open_id": "ou_user",
            "open_message_id": "om_msg",
            "open_chat_id": "oc_chat"
        });

        let msg =
            CardActionHandler::handle_card_action(&event, "om_msg", "oc_chat", "ou_user").unwrap();

        assert_eq!(msg.metadata["hermes_action"], "approve_always");
        assert_eq!(msg.metadata["approval_id"], 7);
    }
}
