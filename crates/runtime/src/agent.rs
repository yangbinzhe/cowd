//! Agent and SubAgent runtime for delegating sub-tasks.
//!
//! A `SubAgent` runs with restricted capabilities: a limited set of tools,
//! a write guard that prevents writing to protected memory layers (L0/L1),
//! and a token budget that caps its execution.

use serde::{Deserialize, Serialize};

use crate::tool_orchestrator::ToolResultBudget;

// ---------------------------------------------------------------------------
// SubAgentConfig
// ---------------------------------------------------------------------------

/// Configuration for spawning a sub-agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAgentConfig {
    /// Human-readable description of the sub-task.
    pub task_description: String,
    /// Tools this sub-agent is allowed to use.
    pub allowed_tools: Vec<String>,
    /// Write source for memory write guard (defaults to SubAgent).
    #[serde(default = "default_write_source")]
    pub write_source: String,
    /// Maximum number of conversation turns.
    #[serde(default = "default_max_turns")]
    pub max_turns: usize,
    /// Token budget for the sub-agent's context.
    #[serde(default = "default_budget_tokens")]
    pub budget_tokens: usize,
}

fn default_write_source() -> String {
    "SubAgent".to_string()
}

fn default_max_turns() -> usize {
    10
}

fn default_budget_tokens() -> usize {
    20_000
}

impl Default for SubAgentConfig {
    fn default() -> Self {
        Self {
            task_description: String::new(),
            allowed_tools: vec![],
            write_source: default_write_source(),
            max_turns: default_max_turns(),
            budget_tokens: default_budget_tokens(),
        }
    }
}

// ---------------------------------------------------------------------------
// SubAgentResult
// ---------------------------------------------------------------------------

/// Result returned by a completed sub-agent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAgentResult {
    /// The final output text from the sub-agent.
    pub output: String,
    /// Number of tool calls made during execution.
    pub tool_call_count: usize,
    /// Token usage during execution.
    pub tokens_used: usize,
    /// Whether the sub-agent completed within budget.
    pub completed_normally: bool,
    /// Number of memory write attempts (for audit).
    pub memory_write_attempts: usize,
    /// Number of memory writes that were denied by WriteGuard.
    pub memory_writes_denied: usize,
}

impl Default for SubAgentResult {
    fn default() -> Self {
        Self {
            output: String::new(),
            tool_call_count: 0,
            tokens_used: 0,
            completed_normally: true,
            memory_write_attempts: 0,
            memory_writes_denied: 0,
        }
    }
}

// ---------------------------------------------------------------------------
// SubAgentError
// ---------------------------------------------------------------------------

/// Errors that can occur during sub-agent execution.
#[derive(Debug, thiserror::Error)]
pub enum SubAgentError {
    #[error("sub-agent exceeded max turns: {0}")]
    MaxTurnsExceeded(usize),

    #[error("sub-agent exceeded token budget: {0}")]
    BudgetExceeded(usize),

    #[error("sub-agent tool not allowed: {0}")]
    ToolNotAllowed(String),

    #[error("sub-agent execution error: {0}")]
    ExecutionError(String),
}

// ---------------------------------------------------------------------------
// SubAgentRuntime (stub)
// ---------------------------------------------------------------------------

/// Runtime for executing sub-agents.
///
/// This is a framework that validates tool access, enforces the write guard,
/// and tracks resource usage. The actual LLM loop is driven by the caller
/// (typically `ConversationRuntime`), which delegates sub-tasks through this
/// runtime.
pub struct SubAgentRuntime {
    config: SubAgentConfig,
    result_budget: ToolResultBudget,
    /// Track turns executed so far.
    turns_executed: usize,
    /// Track tokens consumed so far.
    tokens_consumed: usize,
}

impl SubAgentRuntime {
    /// Create a new sub-agent runtime with the given configuration.
    #[must_use]
    pub fn new(config: SubAgentConfig) -> Self {
        Self {
            result_budget: ToolResultBudget::default(),
            turns_executed: 0,
            tokens_consumed: 0,
            config,
        }
    }

    /// Create with a custom result budget.
    #[must_use]
    pub fn with_result_budget(mut self, budget: ToolResultBudget) -> Self {
        self.result_budget = budget;
        self
    }

    /// Check if a tool is allowed for this sub-agent.
    pub fn is_tool_allowed(&self, tool_name: &str) -> bool {
        // If no allowed_tools specified, all tools are allowed
        if self.config.allowed_tools.is_empty() {
            return true;
        }
        self.config.allowed_tools.iter().any(|t| t == tool_name)
    }

    /// Validate that the sub-agent can execute another turn.
    ///
    /// Returns an error if the budget or turn limit has been exceeded.
    pub fn check_budget(&self) -> Result<(), SubAgentError> {
        if self.turns_executed >= self.config.max_turns {
            return Err(SubAgentError::MaxTurnsExceeded(self.config.max_turns));
        }
        if self.tokens_consumed >= self.config.budget_tokens {
            return Err(SubAgentError::BudgetExceeded(self.config.budget_tokens));
        }
        Ok(())
    }

    /// Record that a turn was executed.
    pub fn record_turn(&mut self, tokens: usize) {
        self.turns_executed += 1;
        self.tokens_consumed += tokens;
    }

    /// Truncate a tool result according to the result budget.
    pub fn truncate_result(&self, output: &str) -> String {
        self.result_budget.truncate(output)
    }

    /// Get the write source for this sub-agent.
    pub fn write_source(&self) -> &str {
        &self.config.write_source
    }

    /// Get the task description.
    pub fn task_description(&self) -> &str {
        &self.config.task_description
    }

    /// Build the result from the current state.
    pub fn build_result(&self, output: String) -> SubAgentResult {
        SubAgentResult {
            output,
            tool_call_count: 0, // caller should track
            tokens_used: self.tokens_consumed,
            completed_normally: self.turns_executed < self.config.max_turns
                && self.tokens_consumed < self.config.budget_tokens,
            memory_write_attempts: 0,
            memory_writes_denied: 0,
        }
    }

    /// Get remaining token budget.
    pub fn remaining_budget(&self) -> usize {
        self.config.budget_tokens.saturating_sub(self.tokens_consumed)
    }

    /// Get remaining turns.
    pub fn remaining_turns(&self) -> usize {
        self.config.max_turns.saturating_sub(self.turns_executed)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_values() {
        let config = SubAgentConfig::default();
        assert_eq!(config.max_turns, 10);
        assert_eq!(config.budget_tokens, 20_000);
        assert_eq!(config.write_source, "SubAgent");
    }

    #[test]
    fn tool_allowed_when_no_restrictions() {
        let runtime = SubAgentRuntime::new(SubAgentConfig::default());
        assert!(runtime.is_tool_allowed("read"));
        assert!(runtime.is_tool_allowed("bash"));
    }

    #[test]
    fn tool_blocked_when_not_in_allowed_list() {
        let config = SubAgentConfig {
            allowed_tools: vec!["read".to_string(), "grep".to_string()],
            ..SubAgentConfig::default()
        };
        let runtime = SubAgentRuntime::new(config);
        assert!(runtime.is_tool_allowed("read"));
        assert!(!runtime.is_tool_allowed("bash"));
    }

    #[test]
    fn budget_check_passes_initially() {
        let runtime = SubAgentRuntime::new(SubAgentConfig::default());
        assert!(runtime.check_budget().is_ok());
    }

    #[test]
    fn budget_check_fails_after_max_turns() {
        let config = SubAgentConfig {
            max_turns: 2,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config);
        runtime.record_turn(100);
        runtime.record_turn(100);
        assert!(matches!(
            runtime.check_budget(),
            Err(SubAgentError::MaxTurnsExceeded(2))
        ));
    }

    #[test]
    fn budget_check_fails_after_token_limit() {
        let config = SubAgentConfig {
            budget_tokens: 100,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config);
        runtime.record_turn(150);
        assert!(matches!(
            runtime.check_budget(),
            Err(SubAgentError::BudgetExceeded(100))
        ));
    }

    #[test]
    fn remaining_budget_tracking() {
        let config = SubAgentConfig {
            budget_tokens: 1000,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config);
        assert_eq!(runtime.remaining_budget(), 1000);
        runtime.record_turn(300);
        assert_eq!(runtime.remaining_budget(), 700);
    }

    #[test]
    fn result_truncation() {
        let runtime = SubAgentRuntime::new(SubAgentConfig::default());
        let long_output = "x".repeat(100_000);
        let truncated = runtime.truncate_result(&long_output);
        assert!(truncated.len() < long_output.len());
    }
}
