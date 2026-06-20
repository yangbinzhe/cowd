//! Feishu platform adapter.
//!
//! This module provides integration with Feishu (Lark) messaging platform,
//! supporting both sending and receiving messages through the Feishu Open API.

use std::sync::OnceLock;

use serde::de::DeserializeOwned;

pub use super::{PlatformError, PlatformResult};

/// Shared Feishu API base URL, set once during adapter initialization.
/// Defaults to `https://open.feishu.cn` for China; set to `https://open.larksuite.com` for Lark.
static FEISHU_API_BASE: OnceLock<String> = OnceLock::new();

/// Set the global Feishu API base URL. Call once during adapter startup.
pub(crate) fn set_api_base_url(url: &str) {
    let _ = FEISHU_API_BASE.set(url.to_string());
}

/// Get the current Feishu API base URL (e.g. `https://open.feishu.cn/open-apis`).
pub(crate) fn api_base_url() -> &'static str {
    FEISHU_API_BASE
        .get()
        .map(|s| s.as_str())
        .unwrap_or("https://open.feishu.cn/open-apis")
}

/// Check if a URL string belongs to an allowed Feishu/Lark domain (SSRF protection).
pub(crate) fn is_feishu_domain(url_str: &str) -> bool {
    // Quick validation
    if url_str.is_empty() {
        return false;
    }

    // Only allow HTTPS
    if !url_str.starts_with("https://") {
        return false;
    }

    // Extract host
    let without_protocol = &url_str["https://".len()..];
    let host = match without_protocol.split('/').next() {
        Some(h) => h,
        None => return false,
    };

    if host.is_empty() {
        return false;
    }

    // Block raw IP addresses (IPv4 and IPv6)
    if host.parse::<std::net::Ipv4Addr>().is_ok() || host.parse::<std::net::Ipv6Addr>().is_ok() {
        return false;
    }

    // Check against allowed domains (Feishu China + Lark International)
    host == "open.feishu.cn"
        || host.ends_with(".open.feishu.cn")
        || host == "feishu.cn"
        || host.ends_with(".feishu.cn")
        || host == "open.larksuite.com"
        || host.ends_with(".open.larksuite.com")
        || host == "larksuite.com"
        || host.ends_with(".larksuite.com")
}

/// Decode a Feishu API response with rich error context.
///
/// Unlike plain `.json()`, this helper reads the raw body first and
/// includes HTTP status, content-type, and a truncated body preview in
/// the error message when deserialization fails. This avoids losing
/// diagnostic information when the Feishu API returns non-JSON errors.
pub(crate) async fn decode_feishu_response<T>(
    response: reqwest::Response,
    operation: &str,
) -> PlatformResult<T>
where
    T: DeserializeOwned,
{
    let status = response.status();
    let content_type = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let body = response
        .text()
        .await
        .map_err(|e| PlatformError::SendFailed(format!("{operation}: read response: {e}")))?;

    serde_json::from_str::<T>(&body).map_err(|e| {
        let preview: String = body.chars().take(500).collect();
        PlatformError::SendFailed(format!(
            "{operation}: decode response failed: {e}; status={status}; content_type={content_type}; body={preview}"
        ))
    })
}

pub mod adapter;
pub mod approval;
pub mod auth;
pub mod batch;
pub mod card_handler;
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
pub use adapter::{CardAction, FeishuAdapter, FeishuConfig};
pub use approval::{
    ApprovalCard, CardActionDedup, APPROVAL_HEADER_TEXT, CARD_ACTION_DEDUP_TTL_SECONDS,
    HERMES_ACTION_APPROVE_ALWAYS, HERMES_ACTION_APPROVE_ONCE, HERMES_ACTION_APPROVE_SESSION,
    HERMES_ACTION_DENY, LABEL_ALLOW_ONCE, LABEL_APPROVE_ALWAYS, LABEL_APPROVE_SESSION, LABEL_DENY,
};
pub use auth::*;
pub use batch::*;
pub use markdown::*;
pub use media::*;
pub use normalize::*;
pub use processing::*;
pub use reactions::*;
pub use rules::{RoutingRule, RuleAction, RuleCondition, RuleMatch, RulesEngine};
pub use types::*;
pub use ws::*;

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

    if let Some(base_url) = settings.get("base_url").and_then(|v| v.as_str()) {
        config = config.with_base_url(base_url);
    }

    // Set the global API base URL for messaging, media, reactions, and card presentation modules.
    set_api_base_url(&config.api_base_url);

    let mut adapter = FeishuAdapter::new(config);

    // ── Access control ──────────────────────────────────────────

    let require_mention = settings
        .get("require_mention")
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let allow_bots = settings
        .get("allow_bots")
        .and_then(|v| v.as_str())
        .unwrap_or("none");

    let admins: std::collections::HashSet<String> = settings
        .get("admins")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();

    let default_group_policy = settings
        .get("default_group_policy")
        .and_then(|v| v.as_str())
        .unwrap_or("open");

    adapter.access_control.require_mention = require_mention;
    adapter.access_control.allow_bots = match allow_bots {
        "mentions" => AllowBots::Mentions,
        "all" => AllowBots::All,
        _ => AllowBots::None,
    };
    adapter.access_control.admins = admins;
    adapter.access_control.default_group_policy = match default_group_policy {
        "allowlist" => Policy::Allowlist,
        "blacklist" => Policy::Blacklist,
        "admin_only" => Policy::AdminOnly,
        "disabled" => Policy::Disabled,
        _ => Policy::Open,
    };

    // ── Processing queue ────────────────────────────────────────

    let max_queue_depth = settings
        .get("max_queue_depth")
        .and_then(|v| v.as_u64())
        .unwrap_or(1000) as usize;

    adapter.processing_queue = ChatProcessingQueue::new(max_queue_depth);

    // ── Reactions cache (ProcessingReactions::new() uses default 1024; custom cache_size NYI) ──

    let _reactions_cache_size = settings
        .get("reactions_cache_size")
        .and_then(|v| v.as_u64())
        .unwrap_or(1024) as usize;

    Ok(adapter)
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
}
