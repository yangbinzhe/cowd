//! Tool orchestration: safety classification, concurrency control, and result budgeting.
//!
//! # Safety categories
//! - `ReadOnly` — safe to run concurrently (grep, file_search, etc.)
//! - `WriteLocal` — local file writes, serialized per file (write, bash)
//! - `Network` — network access, limited concurrency (web_search, web_fetch)
//! - `Destructive` — destructive operations, require confirmation (rm, kill)
//!
//! # Result budgeting
//! Each tool result is checked against a token budget. Oversized results are
//! truncated using a configurable strategy (HeadOnly, TailOnly, HeadAndTail).

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// ToolSafetyCategory
// ---------------------------------------------------------------------------

/// Safety classification for tool execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolSafetyCategory {
    /// Read-only tools — safe to run concurrently.
    ReadOnly,
    /// Local file writes — serialized per file path, different files can run concurrently.
    WriteLocal,
    /// Network access tools — limited concurrency (default: 3).
    Network,
    /// Destructive operations — require explicit confirmation.
    Destructive,
}

impl ToolSafetyCategory {
    /// Classify a tool by name using a built-in mapping.
    pub fn from_tool_name(name: &str) -> Self {
        match name {
            "read" | "read_file" | "cat" | "head" | "tail"
            | "grep" | "grep_search" | "rg"
            | "glob" | "glob_search" | "find" | "ls" | "list_directory"
            | "file_search"
            | "git_status" | "git_log" | "git_diff" | "git_show"
            | "memory_search" | "memory_list" | "memory_get" | "session_list"
            | "session_get" | "skill_list" | "skill_view" => Self::ReadOnly,

            "write" | "write_file" | "edit" | "edit_file"
            | "bash" | "create_file" | "delete_file"
            | "memory_create" | "memory_delete" | "session_create" => Self::WriteLocal,

            // Network tools
            "web_search" | "web_fetch" | "http_request" | "mcp_call" => Self::Network,

            // Destructive tools
            "rm" | "kill" | "sudo" | "truncate" | "drop" => Self::Destructive,

            // Default: treat unknown tools as WriteLocal (conservative)
            _ => Self::WriteLocal,
        }
    }

    /// Maximum concurrent executions for this category.
    pub fn max_concurrency(&self) -> usize {
        match self {
            Self::ReadOnly => usize::MAX,
            Self::WriteLocal => 4,
            Self::Network => 3,
            Self::Destructive => 1,
        }
    }
}

// ---------------------------------------------------------------------------
// TruncationStrategy
// ---------------------------------------------------------------------------

/// Strategy for truncating oversized tool results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TruncationStrategy {
    /// Keep only the beginning of the output.
    HeadOnly,
    /// Keep only the end of the output.
    TailOnly,
    /// Keep both head and tail, omit the middle.
    HeadAndTail,
    /// Compress via summary (requires LLM — falls back to HeadAndTail).
    Summary,
}

impl Default for TruncationStrategy {
    fn default() -> Self {
        Self::HeadAndTail
    }
}

// ---------------------------------------------------------------------------
// ToolResultBudget
// ---------------------------------------------------------------------------

/// Budget configuration for tool result sizes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ToolResultBudget {
    /// Maximum total tokens across all tool results in one turn.
    pub max_total_tokens: usize,
    /// Maximum tokens for a single tool result.
    pub per_tool_max_tokens: usize,
    /// Strategy to use when truncating oversized results.
    pub truncation_strategy: TruncationStrategy,
    /// Number of characters to keep at head (for HeadAndTail strategy).
    pub head_chars: usize,
    /// Number of characters to keep at tail (for HeadAndTail strategy).
    pub tail_chars: usize,
}

impl Default for ToolResultBudget {
    fn default() -> Self {
        Self {
            max_total_tokens: 50_000,
            per_tool_max_tokens: 10_000,
            truncation_strategy: TruncationStrategy::HeadAndTail,
            head_chars: 3000,
            tail_chars: 2000,
        }
    }
}

impl ToolResultBudget {
    /// Truncate output text according to the configured strategy.
    pub fn truncate(&self, output: &str) -> String {
        let budget = self.per_tool_max_tokens;
        // Rough estimate: ~4 chars per token
        let max_chars = budget * 4;

        if output.len() <= max_chars {
            return output.to_string();
        }

        match self.truncation_strategy {
            TruncationStrategy::HeadOnly => {
                format!("{}...\n[truncated: {} chars omitted]", &output[..max_chars.min(output.len())], output.len().saturating_sub(max_chars))
            }
            TruncationStrategy::TailOnly => {
                let start = output.len().saturating_sub(max_chars);
                format!("[truncated: {} chars omitted]\n{}", output.len().saturating_sub(max_chars), &output[start..])
            }
            TruncationStrategy::HeadAndTail | TruncationStrategy::Summary => {
                let head_end = self.head_chars.min(output.len());
                let tail_start = output.len().saturating_sub(self.tail_chars);
                if tail_start <= head_end {
                    // Output is small enough after all
                    output.to_string()
                } else {
                    let omitted = tail_start - head_end;
                    format!(
                        "{}\n\n... [truncated: {} chars omitted] ...\n\n{}",
                        &output[..head_end],
                        omitted,
                        &output[tail_start..]
                    )
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ToolOrchestrator
// ---------------------------------------------------------------------------

/// Orchestrates tool execution based on safety categories and budgets.
pub struct ToolOrchestrator {
    /// Budget configuration for tool results.
    pub budget: ToolResultBudget,
    /// Override map: tool_name → safety_category.
    overrides: HashMap<String, ToolSafetyCategory>,
}

impl ToolOrchestrator {
    /// Create a new orchestrator with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            budget: ToolResultBudget::default(),
            overrides: HashMap::new(),
        }
    }

    /// Create with custom budget.
    #[must_use]
    pub fn with_budget(budget: ToolResultBudget) -> Self {
        Self {
            budget,
            overrides: HashMap::new(),
        }
    }

    /// Override the safety category for a specific tool.
    pub fn set_override(&mut self, tool_name: String, category: ToolSafetyCategory) {
        self.overrides.insert(tool_name, category);
    }

    /// Get the safety category for a tool (checking overrides first).
    pub fn category_for(&self, tool_name: &str) -> ToolSafetyCategory {
        self.overrides
            .get(tool_name)
            .copied()
            .unwrap_or_else(|| ToolSafetyCategory::from_tool_name(tool_name))
    }

    /// Check if a tool can be executed concurrently with another.
    pub fn can_run_concurrently(&self, tool_a: &str, tool_b: &str) -> bool {
        let cat_a = self.category_for(tool_a);
        let cat_b = self.category_for(tool_b);

        // Destructive tools never run concurrently
        if cat_a == ToolSafetyCategory::Destructive || cat_b == ToolSafetyCategory::Destructive {
            return false;
        }

        // Read-only tools always safe
        if cat_a == ToolSafetyCategory::ReadOnly && cat_b == ToolSafetyCategory::ReadOnly {
            return true;
        }

        // Network tools respect concurrency limits (caller must check slot count)
        if cat_a == ToolSafetyCategory::Network || cat_b == ToolSafetyCategory::Network {
            return false; // conservative: serialize with non-read-only
        }

        // WriteLocal: different "files" could be concurrent, but we
        // can't easily tell, so we serialize conservatively.
        if cat_a == ToolSafetyCategory::WriteLocal || cat_b == ToolSafetyCategory::WriteLocal {
            return false;
        }

        true
    }

    /// Truncate a tool result according to the budget.
    pub fn truncate_result(&self, output: &str) -> String {
        self.budget.truncate(output)
    }
}

impl Default for ToolOrchestrator {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_read_tools() {
        assert_eq!(ToolSafetyCategory::from_tool_name("read"), ToolSafetyCategory::ReadOnly);
        assert_eq!(ToolSafetyCategory::from_tool_name("read_file"), ToolSafetyCategory::ReadOnly);
        assert_eq!(ToolSafetyCategory::from_tool_name("grep"), ToolSafetyCategory::ReadOnly);
        assert_eq!(ToolSafetyCategory::from_tool_name("grep_search"), ToolSafetyCategory::ReadOnly);
        assert_eq!(ToolSafetyCategory::from_tool_name("glob"), ToolSafetyCategory::ReadOnly);
        assert_eq!(ToolSafetyCategory::from_tool_name("glob_search"), ToolSafetyCategory::ReadOnly);
    }

    #[test]
    fn classifies_write_tools() {
        assert_eq!(ToolSafetyCategory::from_tool_name("write"), ToolSafetyCategory::WriteLocal);
        assert_eq!(ToolSafetyCategory::from_tool_name("write_file"), ToolSafetyCategory::WriteLocal);
        assert_eq!(ToolSafetyCategory::from_tool_name("edit"), ToolSafetyCategory::WriteLocal);
        assert_eq!(ToolSafetyCategory::from_tool_name("edit_file"), ToolSafetyCategory::WriteLocal);
        assert_eq!(ToolSafetyCategory::from_tool_name("bash"), ToolSafetyCategory::WriteLocal);
    }

    #[test]
    fn classifies_network_tools() {
        assert_eq!(ToolSafetyCategory::from_tool_name("web_search"), ToolSafetyCategory::Network);
    }

    #[test]
    fn classifies_destructive_tools() {
        assert_eq!(ToolSafetyCategory::from_tool_name("rm"), ToolSafetyCategory::Destructive);
    }

    #[test]
    fn unknown_defaults_to_write_local() {
        assert_eq!(ToolSafetyCategory::from_tool_name("custom_tool"), ToolSafetyCategory::WriteLocal);
    }

    #[test]
    fn read_only_concurrent() {
        let orch = ToolOrchestrator::new();
        assert!(orch.can_run_concurrently("read", "grep"));
        assert!(orch.can_run_concurrently("read_file", "grep_search"));
        assert!(orch.can_run_concurrently("glob", "file_search"));
        assert!(orch.can_run_concurrently("glob_search", "grep_search"));
    }

    #[test]
    fn destructive_never_concurrent() {
        let orch = ToolOrchestrator::new();
        assert!(!orch.can_run_concurrently("rm", "read"));
        assert!(!orch.can_run_concurrently("rm", "write"));
    }

    #[test]
    fn override_changes_category() {
        let mut orch = ToolOrchestrator::new();
        orch.set_override("custom_read".to_string(), ToolSafetyCategory::ReadOnly);
        assert_eq!(orch.category_for("custom_read"), ToolSafetyCategory::ReadOnly);
    }

    #[test]
    fn truncation_head_and_tail() {
        let budget = ToolResultBudget {
            per_tool_max_tokens: 10,
            truncation_strategy: TruncationStrategy::HeadAndTail,
            head_chars: 20,
            tail_chars: 20,
            ..ToolResultBudget::default()
        };
        let long_text = "a".repeat(200);
        let truncated = budget.truncate(&long_text);
        assert!(truncated.contains("[truncated"));
        assert!(truncated.starts_with(&"a".repeat(20)));
    }

    #[test]
    fn truncation_short_text_unchanged() {
        let budget = ToolResultBudget::default();
        let short = "hello world";
        assert_eq!(budget.truncate(short), short);
    }

    #[test]
    fn max_concurrency_values() {
        assert_eq!(ToolSafetyCategory::ReadOnly.max_concurrency(), usize::MAX);
        assert_eq!(ToolSafetyCategory::Network.max_concurrency(), 3);
        assert_eq!(ToolSafetyCategory::Destructive.max_concurrency(), 1);
    }
}
