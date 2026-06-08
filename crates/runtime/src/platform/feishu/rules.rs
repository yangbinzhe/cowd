//! Feishu Rules Engine.
//!
//! Provides rule-based message processing and routing for Feishu platform.

#[cfg(test)]
use crate::platform::adapter::MessageType;
use crate::platform::adapter::{InboundMessage, PlatformError, PlatformResult};
use crate::platform::feishu::FeishuAdapter;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Rule matching result.
#[derive(Debug, Clone)]
pub struct RuleMatch {
    /// The rule that matched.
    pub rule_id: String,
    /// Extracted parameters from the match.
    pub params: HashMap<String, String>,
    /// The matched action to execute.
    pub action: RuleAction,
}

/// Rule action types.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RuleAction {
    /// Respond with a static message.
    Respond { text: String },
    /// Forward to AI agent for processing.
    ForwardToAgent,
    /// Execute a command.
    ExecuteCommand { command: String },
    /// Forward to another platform.
    ForwardToPlatform {
        platform: String,
        session_id: Option<String>,
    },
    /// No action (silently ignore).
    Ignore,
}

/// A routing rule for Feishu messages.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Unique rule identifier.
    pub id: String,
    /// Rule name for logging.
    pub name: String,
    /// Rule priority (higher = evaluated first).
    pub priority: i32,
    /// Whether the rule is enabled.
    pub enabled: bool,
    /// Matching conditions.
    pub conditions: Vec<RuleCondition>,
    /// Action to take when matched.
    pub action: RuleAction,
}

/// Condition types for rule matching.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RuleCondition {
    /// Match by keyword in message text.
    Keyword {
        keyword: String,
        case_sensitive: bool,
    },
    /// Match by regex pattern.
    Pattern { pattern: String },
    /// Match by message sender.
    Sender {
        open_id: Option<String>,
        user_id: Option<String>,
    },
    /// Match by chat type (p2p or group).
    ChatType { chat_type: String },
    /// Match by presence of mention.
    Mentioned { mention_all: bool },
    /// Composite condition - all must match.
    All { conditions: Vec<RuleCondition> },
    /// Composite condition - any must match.
    Any { conditions: Vec<RuleCondition> },
    /// Negate a condition.
    Not { condition: Box<RuleCondition> },
}

impl RoutingRule {
    /// Check if this rule matches the given message.
    pub fn matches(&self, message: &InboundMessage) -> bool {
        if !self.enabled {
            return false;
        }

        self.conditions.iter().all(|c| c.matches(message))
    }

    /// Evaluate the rule and return the match result.
    pub fn evaluate(&self, message: &InboundMessage) -> Option<RuleMatch> {
        if self.matches(message) {
            Some(RuleMatch {
                rule_id: self.id.clone(),
                params: HashMap::new(),
                action: self.action.clone(),
            })
        } else {
            None
        }
    }
}

impl RuleCondition {
    /// Check if this condition matches the given message.
    pub fn matches(&self, message: &InboundMessage) -> bool {
        match self {
            RuleCondition::Keyword {
                keyword,
                case_sensitive,
            } => {
                if *case_sensitive {
                    message.text.contains(keyword)
                } else {
                    message
                        .text
                        .to_lowercase()
                        .contains(&keyword.to_lowercase())
                }
            }
            RuleCondition::Pattern { pattern } => {
                if let Ok(re) = regex::Regex::new(pattern) {
                    re.is_match(&message.text)
                } else {
                    false
                }
            }
            RuleCondition::Sender { open_id, user_id } => {
                if let Some(oid) = open_id {
                    if message.session_key.user_id == *oid {
                        return true;
                    }
                }
                if let Some(uid) = user_id {
                    if message.session_key.user_id == *uid {
                        return true;
                    }
                }
                false
            }
            RuleCondition::ChatType { chat_type } => message
                .session_key
                .thread_id
                .as_ref()
                .map(|t| t == chat_type)
                .unwrap_or(false),
            RuleCondition::Mentioned { mention_all: _ } => {
                // Check if @all or @someone is present
                message.text.contains("@all")
                    || message.text.contains("@someone")
                    || message.text.contains("<at")
            }
            RuleCondition::All { conditions } => conditions.iter().all(|c| c.matches(message)),
            RuleCondition::Any { conditions } => conditions.iter().any(|c| c.matches(message)),
            RuleCondition::Not { condition } => !condition.matches(message),
        }
    }
}

/// Default routing rules.
pub fn default_rules() -> Vec<RoutingRule> {
    vec![
        // High priority: bot mention rules
        RoutingRule {
            id: "mention_response".to_string(),
            name: "Bot Mention Response".to_string(),
            priority: 100,
            enabled: true,
            conditions: vec![
                RuleCondition::Mentioned { mention_all: true },
            ],
            action: RuleAction::ForwardToAgent,
        },
        // Keyword-based routing
        RoutingRule {
            id: "help_keyword".to_string(),
            name: "Help Command".to_string(),
            priority: 50,
            enabled: true,
            conditions: vec![
                RuleCondition::Keyword {
                    keyword: "help".to_string(),
                    case_sensitive: false,
                },
            ],
            action: RuleAction::Respond {
                text: "我是 Cowd AI 助手。你可以:\n1. 直接发送消息与我对话\n2. 使用 /help 查看所有命令\n3. @我 获取帮助".to_string(),
            },
        },
        RoutingRule {
            id: "status_keyword".to_string(),
            name: "Status Command".to_string(),
            priority: 50,
            enabled: true,
            conditions: vec![
                RuleCondition::Keyword {
                    keyword: "/status".to_string(),
                    case_sensitive: false,
                },
            ],
            action: RuleAction::Respond {
                text: "Cowd 运行正常".to_string(),
            },
        },
        // Default: forward to agent
        RoutingRule {
            id: "default".to_string(),
            name: "Default Handler".to_string(),
            priority: 0,
            enabled: true,
            conditions: vec![],
            action: RuleAction::ForwardToAgent,
        },
    ]
}

/// Feishu Rules Engine.
pub struct RulesEngine {
    rules: Arc<RwLock<Vec<RoutingRule>>>,
}

impl RulesEngine {
    /// Create a new rules engine with default rules.
    pub fn new() -> Self {
        Self {
            rules: Arc::new(RwLock::new(default_rules())),
        }
    }

    /// Create with custom rules.
    pub fn with_rules(rules: Vec<RoutingRule>) -> Self {
        let mut sorted_rules = rules;
        sorted_rules.sort_by(|a, b| b.priority.cmp(&a.priority));
        Self {
            rules: Arc::new(RwLock::new(sorted_rules)),
        }
    }

    /// Add a new rule.
    pub async fn add_rule(&self, rule: RoutingRule) {
        let mut rules = self.rules.write().await;
        rules.push(rule);
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));
    }

    /// Remove a rule by ID.
    pub async fn remove_rule(&self, rule_id: &str) -> bool {
        let mut rules = self.rules.write().await;
        let len_before = rules.len();
        rules.retain(|r| r.id != rule_id);
        rules.len() < len_before
    }

    /// Update a rule.
    pub async fn update_rule(&self, rule: RoutingRule) -> PlatformResult<()> {
        let mut rules = self.rules.write().await;
        if let Some(existing) = rules.iter_mut().find(|r| r.id == rule.id) {
            *existing = rule;
            rules.sort_by(|a, b| b.priority.cmp(&a.priority));
            Ok(())
        } else {
            Err(PlatformError::Unknown(format!(
                "rule '{}' not found",
                rule.id
            )))
        }
    }

    /// List all rules.
    pub async fn list_rules(&self) -> Vec<RoutingRule> {
        self.rules.read().await.clone()
    }

    /// Enable or disable a rule.
    pub async fn set_rule_enabled(&self, rule_id: &str, enabled: bool) -> PlatformResult<()> {
        let mut rules = self.rules.write().await;
        if let Some(rule) = rules.iter_mut().find(|r| r.id == rule_id) {
            rule.enabled = enabled;
            Ok(())
        } else {
            Err(PlatformError::Unknown(format!(
                "rule '{}' not found",
                rule_id
            )))
        }
    }

    /// Evaluate a message against all rules.
    pub async fn evaluate(&self, message: &InboundMessage) -> Option<RuleMatch> {
        let rules = self.rules.read().await;
        for rule in rules.iter() {
            if let Some(matched) = rule.evaluate(message) {
                return Some(matched);
            }
        }
        None
    }

    /// Execute the action for a matched rule.
    pub async fn execute(
        &self,
        message: &InboundMessage,
        adapter: &FeishuAdapter,
    ) -> PlatformResult<Option<String>> {
        let matched = self.evaluate(message).await;

        match matched {
            Some(rule_match) => {
                tracing::info!(
                    rule_id = %rule_match.rule_id,
                    "rule matched for message"
                );

                match &rule_match.action {
                    RuleAction::Respond { text } => {
                        adapter.send_message(&message.session_key, text).await?;
                        Ok(Some(text.clone()))
                    }
                    RuleAction::ForwardToAgent => {
                        // Return None to signal that the message should be
                        // forwarded to the AI agent for processing
                        Ok(None)
                    }
                    RuleAction::ExecuteCommand { command } => {
                        // Command execution would be handled separately
                        tracing::info!(command = %command, "executing command");
                        adapter
                            .send_message(&message.session_key, &format!("Executing: {}", command))
                            .await?;
                        Ok(Some(format!("Executing: {}", command)))
                    }
                    RuleAction::ForwardToPlatform {
                        platform,
                        session_id,
                    } => {
                        tracing::info!(
                            platform = %platform,
                            session_id = ?session_id,
                            "forwarding to platform"
                        );
                        Ok(None)
                    }
                    RuleAction::Ignore => {
                        tracing::debug!("message ignored by rule");
                        Ok(None)
                    }
                }
            }
            None => {
                // No rule matched, forward to agent by default
                Ok(None)
            }
        }
    }
}

impl Default for RulesEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::platform::types::SessionKey;

    fn create_test_message(text: &str) -> InboundMessage {
        InboundMessage {
            platform: crate::platform::Platform::Feishu,
            session_key: SessionKey::new("feishu", "user123"),
            text: text.to_string(),
            sender_name: None,
            timestamp: chrono::Utc::now(),
            metadata: serde_json::json!({}),
            message_type: MessageType::Text,
            message_id: None,
            reply_to_message_id: None,
            media_urls: vec![],
            media_types: vec![],
        }
    }

    #[test]
    fn test_keyword_condition() {
        let condition = RuleCondition::Keyword {
            keyword: "hello".to_string(),
            case_sensitive: false,
        };
        let message = create_test_message("Hello World");
        assert!(condition.matches(&message));

        let message2 = create_test_message("Goodbye");
        assert!(!condition.matches(&message2));
    }

    #[test]
    fn test_pattern_condition() {
        let condition = RuleCondition::Pattern {
            pattern: r"/help|/status".to_string(),
        };
        let message = create_test_message("/help");
        assert!(condition.matches(&message));

        let message2 = create_test_message("/status");
        assert!(condition.matches(&message2));
    }

    #[test]
    fn test_composite_conditions() {
        let all_condition = RuleCondition::All {
            conditions: vec![
                RuleCondition::Keyword {
                    keyword: "test".to_string(),
                    case_sensitive: false,
                },
                RuleCondition::Pattern {
                    pattern: r"^test".to_string(),
                },
            ],
        };
        let message = create_test_message("test message");
        assert!(all_condition.matches(&message));

        let message2 = create_test_message("other message");
        assert!(!all_condition.matches(&message2));
    }

    #[tokio::test]
    async fn test_rules_engine_default_rules() {
        let engine = RulesEngine::new();
        let rules = engine.list_rules().await;
        assert!(!rules.is_empty());
    }

    #[tokio::test]
    async fn test_rules_engine_add_rule() {
        let engine = RulesEngine::new();
        let rule = RoutingRule {
            id: "test_rule".to_string(),
            name: "Test Rule".to_string(),
            priority: 200,
            enabled: true,
            conditions: vec![RuleCondition::Keyword {
                keyword: "test".to_string(),
                case_sensitive: false,
            }],
            action: RuleAction::Respond {
                text: "Test response".to_string(),
            },
        };

        engine.add_rule(rule).await;
        let rules = engine.list_rules().await;
        assert_eq!(rules.len(), 5);
        // Highest priority should be first
        assert_eq!(rules[0].id, "test_rule");
    }

    #[tokio::test]
    async fn test_rules_engine_remove_rule() {
        let engine = RulesEngine::new();
        let removed = engine.remove_rule("help_keyword").await;
        assert!(removed);
        let rules = engine.list_rules().await;
        assert_eq!(rules.len(), 3);
    }

    #[tokio::test]
    async fn test_rules_engine_evaluate() {
        let engine = RulesEngine::new();
        let message = create_test_message("help");

        let result = engine.evaluate(&message).await;
        assert!(result.is_some());
        let matched = result.unwrap();
        assert_eq!(matched.rule_id, "help_keyword");
    }
}
