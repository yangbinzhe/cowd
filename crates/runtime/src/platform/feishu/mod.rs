//! Feishu platform adapter.
//!
//! This module provides integration with Feishu (Lark) messaging platform,
//! supporting both sending and receiving messages through the Feishu Open API.

pub mod adapter;
pub mod auth;
pub mod batch;
pub mod card_handler;
pub mod comment;
pub mod doc;
pub mod markdown;
pub mod media;
pub mod normalize;
pub mod processing;
pub mod proto;
pub mod reactions;
pub mod rules;
pub mod types;
pub mod ws;

// Re-export types from sibling modules
pub use adapter::{FeishuAdapter, FeishuConfig, CardAction};
pub use auth::*;
pub use batch::*;
pub use comment::{CommentHandler, CommentStatus, FeishuComment, CreateCommentRequest, ReplyCommentRequest, UpdateCommentRequest, CommentFilter};
pub use doc::{DocumentClient, DocumentContent, DocumentMetadata, DocumentType, DocumentElement, SearchDocumentsRequest, SearchResult, SearchDocumentsResponse};
pub use markdown::*;
pub use media::*;
pub use normalize::*;
pub use processing::*;
pub use reactions::*;
pub use rules::{RulesEngine, RoutingRule, RuleCondition, RuleAction, RuleMatch};
pub use types::*;
pub use ws::*;

// Re-export from parent module (platform)
pub use super::{PlatformError, PlatformResult};

/// Create a Feishu adapter from config settings.
pub fn create_feishu_adapter(settings: &serde_json::Value) -> PlatformResult<FeishuAdapter> {
    let app_id = settings
        .get("app_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing app_id".to_string()))?;

    let app_secret = settings
        .get("app_secret")
        .and_then(|v| v.as_str())
        .ok_or_else(|| PlatformError::ConfigError("missing app_secret".to_string()))?;

    let mut config = FeishuConfig::new(app_id, app_secret);

    if let Some(bot_open_id) = settings.get("bot_open_id").and_then(|v| v.as_str()) {
        config = config.with_bot_open_id(bot_open_id);
    }

    if let Some(bot_name) = settings.get("bot_name").and_then(|v| v.as_str()) {
        config = config.with_bot_name(bot_name);
    }

    Ok(FeishuAdapter::new(config))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feishu_config() {
        let config = FeishuConfig::new("app_id_123", "app_secret_456");
        assert_eq!(config.app_id, "app_id_123");
        assert_eq!(config.app_secret, "app_secret_456");
    }

    #[tokio::test]
    async fn test_rules_engine() {
        let engine = RulesEngine::new();
        let rules = engine.list_rules().await;
        assert!(!rules.is_empty());
    }

    #[test]
    fn test_document_type_serialization() {
        let json = serde_json::to_string(&DocumentType::Doc).unwrap();
        assert_eq!(json, "\"doc\"");
    }

    #[test]
    fn test_comment_status_serialization() {
        let json = serde_json::to_string(&CommentStatus::Open).unwrap();
        assert_eq!(json, "\"open\"");
    }
}
