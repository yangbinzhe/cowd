//! Message Mirror System.
//!
//! Provides cross-platform message context synchronization.
//! Messages from one platform can be mirrored to another platform's session,
//! enabling unified conversation context across channels.
//!
//! Usage: Telegram messages visible in WebUI session, and vice versa.
//! Format: In JSONL, mirrored messages are tagged with `mirror: true`.

use crate::platform::adapter::InboundMessage;
#[cfg(test)]
use crate::platform::adapter::{MessageType, Platform};
#[cfg(test)]
use crate::platform::types::SessionKey;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Direction of message mirroring.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MirrorDirection {
    /// Only source → target.
    OneWay,
    /// Both directions: source ↔ target.
    Bidirectional,
}

/// A rule describing how messages should be mirrored between sessions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirrorRule {
    /// Unique rule identifier.
    pub id: String,
    /// Source platform name (e.g., "feishu", "wecom", "email").
    pub source_platform: String,
    /// Source session key string.
    pub source_session: String,
    /// Target platform name (e.g., "webui", "api_server").
    /// 3A-7 fix: added target_platform — needed to route mirrored messages
    /// to the correct platform adapter.
    #[serde(default)]
    pub target_platform: Option<String>,
    /// Target session key string.
    pub target_session: String,
    /// Mirror direction.
    pub direction: MirrorDirection,
    /// Whether this rule is active.
    pub enabled: bool,
    /// Rule creation timestamp.
    pub created_at: DateTime<Utc>,
}

/// A mirrored message representation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MirroredMessage {
    /// The mirrored text content, prefixed with source info.
    pub content: String,
    /// The target session key where this message should be delivered.
    pub target_session: String,
    /// The target platform name (from the mirror rule), if known.
    pub target_platform: Option<String>,
    /// The source platform name.
    pub mirror_source: String,
    /// The original session key.
    pub original_session: String,
    /// Timestamp of the original message.
    pub original_timestamp: DateTime<Utc>,
    /// Whether this is a mirrored message.
    pub mirror: bool,
}

impl MirroredMessage {
    /// Create a mirrored message from an inbound message.
    pub fn from_inbound(msg: &InboundMessage) -> Self {
        Self {
            content: format!("[Mirror from {}] {}", msg.platform.name(), msg.text),
            target_session: String::new(),
            target_platform: None,
            mirror_source: msg.platform.name().to_string(),
            original_session: msg.session_key.as_str(),
            original_timestamp: msg.timestamp,
            mirror: true,
        }
    }

    /// Convert to a JSON-serializable metadata value for storage.
    pub fn to_metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "mirror": self.mirror,
            "mirror_source": self.mirror_source,
            "original_session": self.original_session,
            "original_timestamp": self.original_timestamp.to_rfc3339(),
        })
    }
}

/// Message mirror manager.
///
/// Maintains a set of rules that define how messages are mirrored
/// between platform sessions. When an inbound message arrives,
/// the mirror is consulted to determine which target sessions
/// should receive a copy.
pub struct MessageMirror {
    /// Active mirror rules.
    rules: Arc<RwLock<Vec<MirrorRule>>>,
}

impl MessageMirror {
    /// Create a new empty message mirror.
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Create a message mirror with predefined rules.
    pub fn with_rules(rules: Vec<MirrorRule>) -> Self {
        Self {
            rules: Arc::new(RwLock::new(rules)),
        }
    }

    /// Add a mirror rule.
    pub async fn add_rule(&self, rule: MirrorRule) {
        self.rules.write().await.push(rule);
    }

    /// Remove a mirror rule by ID.
    pub async fn remove_rule(&self, id: &str) -> bool {
        let mut rules = self.rules.write().await;
        let before = rules.len();
        rules.retain(|r| r.id != id);
        rules.len() < before
    }

    /// Enable or disable a rule by ID.
    pub async fn toggle_rule(&self, id: &str, enabled: bool) -> bool {
        let mut rules = self.rules.write().await;
        if let Some(rule) = rules.iter_mut().find(|r| r.id == id) {
            rule.enabled = enabled;
            true
        } else {
            false
        }
    }

    /// List all rules.
    pub async fn list_rules(&self) -> Vec<MirrorRule> {
        self.rules.read().await.clone()
    }

    /// Find target sessions for a given inbound message.
    ///
    /// Returns a list of target session key strings that should receive
    /// a mirrored copy of this message.
    pub async fn find_targets(&self, msg: &InboundMessage) -> Vec<String> {
        let rules = self.rules.read().await;
        let mut targets = Vec::new();
        let source_platform = msg.platform.name();
        let source_session = msg.session_key.as_str();

        for rule in rules.iter() {
            if !rule.enabled {
                continue;
            }

            // Check if message source matches rule source
            let source_matches = rule.source_platform == source_platform
                && (rule.source_session == "*" || rule.source_session == source_session);

            if source_matches {
                targets.push(rule.target_session.clone());
            }

            // Check reverse direction for bidirectional rules
            if rule.direction == MirrorDirection::Bidirectional {
                let target_matches = rule.target_session == source_session;
                if target_matches {
                    targets.push(rule.source_session.clone());
                }
            }
        }

        targets
    }

    /// Mirror an inbound message to all target sessions.
    ///
    /// Returns a list of MirroredMessage instances, each with its target
    /// session and target platform populated from the matching rule.
    pub async fn mirror(&self, msg: &InboundMessage) -> Vec<MirroredMessage> {
        let rules = self.rules.read().await;
        let source_platform = msg.platform.name();
        let source_session = msg.session_key.as_str();

        let mut results = Vec::new();
        let base = MirroredMessage::from_inbound(msg);

        for rule in rules.iter() {
            if !rule.enabled {
                continue;
            }

            let source_matches = rule.source_platform == source_platform
                && (rule.source_session == "*" || rule.source_session == source_session);

            if source_matches {
                let mut mirrored = base.clone();
                mirrored.target_session = rule.target_session.clone();
                mirrored.target_platform = rule.target_platform.clone();
                results.push(mirrored);
            }

            if rule.direction == MirrorDirection::Bidirectional {
                let target_matches = rule.target_session == source_session;
                if target_matches {
                    let mut mirrored = base.clone();
                    mirrored.target_session = rule.source_session.clone();
                    mirrored.target_platform = Some(source_platform.to_string());
                    results.push(mirrored);
                }
            }
        }

        results
    }

    /// Create a simple bidirectional mirror rule between two sessions.
    pub fn create_bidirectional_rule(
        id: impl Into<String>,
        platform_a: impl Into<String>,
        session_a: impl Into<String>,
        session_b: impl Into<String>,
    ) -> [MirrorRule; 2] {
        let id = id.into();
        let platform_a = platform_a.into();
        let session_a = session_a.into();
        let session_b = session_b.into();
        [
            MirrorRule {
                id: format!("{}_a2b", id),
                source_platform: platform_a.clone(),
                source_session: session_a.clone(),
                target_platform: None,
                target_session: session_b.clone(),
                direction: MirrorDirection::OneWay,
                enabled: true,
                created_at: Utc::now(),
            },
            MirrorRule {
                id: format!("{}_b2a", id),
                source_platform: platform_a,
                source_session: session_b,
                target_platform: None,
                target_session: session_a,
                direction: MirrorDirection::OneWay,
                enabled: true,
                created_at: Utc::now(),
            },
        ]
    }
}

impl Default for MessageMirror {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mirror_basic() {
        let mirror = MessageMirror::new();

        let rule = MirrorRule {
            id: "test1".to_string(),
            source_platform: "feishu".to_string(),
            source_session: "feishu:user1".to_string(),
            target_platform: None,
            target_session: "webui:session1".to_string(),
            direction: MirrorDirection::OneWay,
            enabled: true,
            created_at: Utc::now(),
        };

        mirror.add_rule(rule).await;

        let msg = InboundMessage {
            platform: Platform::Feishu,
            session_key: SessionKey::new("feishu", "user1"),
            text: "Hello from Feishu!".to_string(),
            sender_name: Some("Alice".to_string()),
            timestamp: Utc::now(),
            metadata: serde_json::json!({}),
            message_type: MessageType::Text,
            message_id: None,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        };

        let results = mirror.mirror(&msg).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].target_session, "webui:session1");
        assert!(results[0].content.contains("Mirror from feishu"));
        assert!(results[0].mirror);
    }

    #[tokio::test]
    async fn test_mirror_no_match() {
        let mirror = MessageMirror::new();

        let msg = InboundMessage {
            platform: Platform::Email,
            session_key: SessionKey::new("email", "user@example.com"),
            text: "Hello".to_string(),
            sender_name: None,
            timestamp: Utc::now(),
            metadata: serde_json::json!({}),
            message_type: MessageType::Text,
            message_id: None,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        };

        let results = mirror.mirror(&msg).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_mirror_disabled_rule() {
        let mirror = MessageMirror::new();

        let rule = MirrorRule {
            id: "disabled1".to_string(),
            source_platform: "feishu".to_string(),
            source_session: "feishu:user1".to_string(),
            target_platform: None,
            target_session: "webui:session1".to_string(),
            direction: MirrorDirection::OneWay,
            enabled: false,
            created_at: Utc::now(),
        };

        mirror.add_rule(rule).await;

        let msg = InboundMessage {
            platform: Platform::Feishu,
            session_key: SessionKey::new("feishu", "user1"),
            text: "Hello".to_string(),
            sender_name: None,
            timestamp: Utc::now(),
            metadata: serde_json::json!({}),
            message_type: MessageType::Text,
            message_id: None,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        };

        let results = mirror.mirror(&msg).await;
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_mirror_wildcard_session() {
        let mirror = MessageMirror::new();

        let rule = MirrorRule {
            id: "wildcard1".to_string(),
            source_platform: "feishu".to_string(),
            source_session: "*".to_string(),
            target_platform: None,
            target_session: "webui:all".to_string(),
            direction: MirrorDirection::OneWay,
            enabled: true,
            created_at: Utc::now(),
        };

        mirror.add_rule(rule).await;

        let msg = InboundMessage {
            platform: Platform::Feishu,
            session_key: SessionKey::new("feishu", "any_user"),
            text: "Hello".to_string(),
            sender_name: None,
            timestamp: Utc::now(),
            metadata: serde_json::json!({}),
            message_type: MessageType::Text,
            message_id: None,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        };

        let results = mirror.mirror(&msg).await;
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn test_bidirectional_rule() {
        let rules = MessageMirror::create_bidirectional_rule(
            "bridge1",
            "feishu",
            "feishu:user1",
            "webui:session1",
        );

        let mirror = MessageMirror::with_rules(rules.to_vec());

        // Forward direction: feishu → webui
        let msg = InboundMessage {
            platform: Platform::Feishu,
            session_key: SessionKey::new("feishu", "user1"),
            text: "Hi from Feishu".to_string(),
            sender_name: None,
            timestamp: Utc::now(),
            metadata: serde_json::json!({}),
            message_type: MessageType::Text,
            message_id: None,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        };
        let results = mirror.mirror(&msg).await;
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].target_session, "webui:session1");
    }

    #[test]
    fn test_mirrored_message_metadata() {
        let msg = InboundMessage {
            platform: Platform::WeChat,
            session_key: SessionKey::new("wecom", "user1"),
            text: "Test message".to_string(),
            sender_name: None,
            timestamp: Utc::now(),
            metadata: serde_json::json!({}),
            message_type: MessageType::Text,
            message_id: None,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        };

        let mirrored = MirroredMessage::from_inbound(&msg);
        assert!(mirrored.mirror);
        assert_eq!(mirrored.mirror_source, "wecom");
        assert!(mirrored.content.contains("Mirror from wecom"));

        let meta = mirrored.to_metadata();
        assert_eq!(meta["mirror"], true);
        assert_eq!(meta["mirror_source"], "wecom");
    }
}
