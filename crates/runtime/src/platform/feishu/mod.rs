//! Feishu platform adapter.
//!
//! This module provides integration with Feishu (Lark) messaging platform,
//! supporting both sending and receiving messages through the Feishu Open API.

pub mod adapter;
pub mod comment;
pub mod doc;
pub mod rules;

// Re-export types from sibling modules
pub use adapter::{FeishuAdapter, FeishuConfig, CardAction};
pub use comment::{CommentHandler, CommentStatus, FeishuComment, CreateCommentRequest, ReplyCommentRequest, UpdateCommentRequest, CommentFilter};
pub use doc::{DocumentClient, DocumentContent, DocumentMetadata, DocumentType, DocumentElement, SearchDocumentsRequest, SearchResult, SearchDocumentsResponse};
pub use rules::{RulesEngine, RoutingRule, RuleCondition, RuleAction, RuleMatch};

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

    if let Some(token) = settings.get("verify_token").and_then(|v| v.as_str()) {
        config = config.with_verify_token(token);
    }

    if let Some(key) = settings.get("encrypt_key").and_then(|v| v.as_str()) {
        config = config.with_encrypt_key(key);
    }

    if let Some(timeout) = settings.get("long_polling_timeout").and_then(|v| v.as_u64()) {
        config.long_polling_timeout = timeout;
    }

    if let Some(enable) = settings.get("enable_events").and_then(|v| v.as_bool()) {
        config.enable_events = enable;
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

    #[test]
    fn test_feishu_config_with_tokens() {
        let config = FeishuConfig::new("app_id", "secret")
            .with_verify_token("verify_token")
            .with_encrypt_key("encrypt_key");
        assert!(config.verify_token.is_some());
        assert!(config.encrypt_key.is_some());
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
