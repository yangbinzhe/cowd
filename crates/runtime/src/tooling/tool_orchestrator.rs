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
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

use crate::bash_validation::{classify_command, CommandIntent};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCachePolicy {
    ScopedRead,
    GlobalRead,
    ScopeInvalidatingWrite,
    GlobalInvalidatingWrite,
    Uncached,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolExecutionProfile {
    pub name: String,
    pub safety_category: ToolSafetyCategory,
    pub max_concurrency: usize,
    pub timeout_secs: u64,
    pub cache_policy: ToolCachePolicy,
    pub prepared_readonly_supported: bool,
}

impl ToolSafetyCategory {
    /// Classify a tool by name using a built-in mapping.
    pub fn from_tool_name(name: &str) -> Self {
        match normalize_tool_name_for_safety(name).as_str() {
            "read"
            | "read_file"
            | "read_many"
            | "cat"
            | "head"
            | "tail"
            | "grep"
            | "grep_search"
            | "grep_many"
            | "rg"
            | "glob"
            | "glob_search"
            | "glob_many"
            | "find"
            | "ls"
            | "list_directory"
            | "file_search"
            | "workspace_snapshot"
            | "tool_batch_readonly"
            | "tool_cache_stats"
            | "mutation_preview"
            | "edit_many_preview"
            | "patch_plan"
            | "checkpoint_list"
            | "checkpoint_diff"
            | "git_status"
            | "git_log"
            | "git_diff"
            | "git_show"
            | "memory_search"
            | "memory_list"
            | "memory_get"
            | "session_list"
            | "session_get"
            | "skill_list"
            | "skill_view"
            | "question"
            | "ask_user_question"
            | "tool_search"
            | "runtime_capabilities"
            | "list_mcp_resources"
            | "read_mcp_resource" => Self::ReadOnly,

            "add" | "calculator" => Self::ReadOnly,

            "write"
            | "write_file"
            | "edit"
            | "edit_file"
            | "create_file"
            | "delete_file"
            | "memory_create"
            | "memory_delete"
            | "session_create"
            | "todo_write"
            | "apply_patch_transaction"
            | "checkpoint_create"
            | "checkpoint_restore" => Self::WriteLocal,

            "web_search" | "web_fetch" | "http_request" => Self::Network,

            "bash"
            | "powershell"
            | "repl"
            | "mcp"
            | "mcp_auth"
            | "remote_trigger"
            | "agent"
            | "runtime_orchestrate"
            | "task_create"
            | "run_task_packet"
            | "task_stop"
            | "task_update"
            | "worker_create"
            | "worker_send_prompt"
            | "worker_restart"
            | "worker_terminate"
            | "team_create"
            | "team_delete"
            | "cron_create"
            | "cron_delete"
            | "config"
            | "notebook_edit"
            | "structured_output"
            | "execute_code"
            | "rm"
            | "kill"
            | "sudo"
            | "truncate"
            | "drop" => Self::Destructive,

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

    /// Default timeout in seconds for tools in this category.
    /// ReadOnly tools get 30s (fast read operations), others get 120s.
    pub fn default_timeout_secs(&self) -> u64 {
        match self {
            Self::ReadOnly => 30,
            Self::WriteLocal | Self::Network | Self::Destructive => 120,
        }
    }
}

#[must_use]
pub fn tool_execution_profile(tool_name: &str) -> ToolExecutionProfile {
    let normalized = normalize_tool_name_for_safety(tool_name);
    let safety_category = ToolSafetyCategory::from_tool_name(&normalized);
    ToolExecutionProfile {
        name: normalized.clone(),
        safety_category,
        max_concurrency: safety_category.max_concurrency(),
        timeout_secs: safety_category.default_timeout_secs(),
        cache_policy: cache_policy_for_tool(&normalized, safety_category),
        prepared_readonly_supported: prepared_readonly_supported(&normalized),
    }
}

fn cache_policy_for_tool(
    normalized_name: &str,
    safety_category: ToolSafetyCategory,
) -> ToolCachePolicy {
    match normalized_name {
        "read_file" | "glob_search" | "grep_search" | "workspace_snapshot" => {
            ToolCachePolicy::ScopedRead
        }
        "tool_cache_stats" => ToolCachePolicy::GlobalRead,
        "write_file" | "edit_file" | "apply_patch_transaction" => {
            ToolCachePolicy::ScopeInvalidatingWrite
        }
        "checkpoint_restore" => ToolCachePolicy::GlobalInvalidatingWrite,
        _ if safety_category == ToolSafetyCategory::ReadOnly => ToolCachePolicy::Uncached,
        _ => ToolCachePolicy::Uncached,
    }
}

fn prepared_readonly_supported(normalized_name: &str) -> bool {
    matches!(
        normalized_name,
        "read_file" | "glob_search" | "grep_search" | "workspace_snapshot" | "tool_cache_stats"
    )
}

fn normalize_tool_name_for_safety(name: &str) -> String {
    let normalized = name.trim().replace('-', "_").to_ascii_lowercase();
    match normalized.as_str() {
        "webfetch" => "web_fetch".to_string(),
        "websearch" => "web_search".to_string(),
        "todowrite" => "todo_write".to_string(),
        "askuserquestion" => "ask_user_question".to_string(),
        "toolsearch" => "tool_search".to_string(),
        "runtimecapabilities" => "runtime_capabilities".to_string(),
        "runtimeorchestrate" => "runtime_orchestrate".to_string(),
        "listmcpresources" => "list_mcp_resources".to_string(),
        "readmcpresource" => "read_mcp_resource".to_string(),
        "mcpauth" => "mcp_auth".to_string(),
        "remotetrigger" => "remote_trigger".to_string(),
        "notebookedit" => "notebook_edit".to_string(),
        "structuredoutput" => "structured_output".to_string(),
        "execute_code" => "execute_code".to_string(),
        "taskcreate" => "task_create".to_string(),
        "runtaskpacket" => "run_task_packet".to_string(),
        "taskget" => "task_get".to_string(),
        "tasklist" => "task_list".to_string(),
        "taskstop" => "task_stop".to_string(),
        "taskupdate" => "task_update".to_string(),
        "taskoutput" => "task_output".to_string(),
        "workercreate" => "worker_create".to_string(),
        "workerget" => "worker_get".to_string(),
        "workerobserve" => "worker_observe".to_string(),
        "workerresolvertrust" => "worker_resolve_trust".to_string(),
        "workerawaitready" => "worker_await_ready".to_string(),
        "workersendprompt" => "worker_send_prompt".to_string(),
        "workerrestart" => "worker_restart".to_string(),
        "workerterminate" => "worker_terminate".to_string(),
        "workerobservecompletion" => "worker_observe_completion".to_string(),
        "teamcreate" => "team_create".to_string(),
        "teamdelete" => "team_delete".to_string(),
        "croncreate" => "cron_create".to_string(),
        "crondelete" => "cron_delete".to_string(),
        "cronlist" => "cron_list".to_string(),
        other => other.to_string(),
    }
}

// ---------------------------------------------------------------------------
// ToolSafetyRegistry
// ---------------------------------------------------------------------------

static TOOL_REGISTRY: OnceLock<ToolSafetyRegistry> = OnceLock::new();

/// A registry for classifying tool safety by name.
///
/// Checks an explicit map first, then prefix patterns in registration order,
/// and finally falls back to a default category.
pub struct ToolSafetyRegistry {
    explicit: HashMap<String, ToolSafetyCategory>,
    patterns: Vec<(String, ToolSafetyCategory)>, // prefix patterns, checked in order
    default: ToolSafetyCategory,
    /// Per-tool timeout overrides (seconds). If a tool is not in this map,
    /// the category default is used (30s for ReadOnly, 120s for others).
    tool_timeout_secs: HashMap<String, u64>,
}

impl ToolSafetyRegistry {
    /// Access the global singleton (initialized with builtin rules on first access).
    pub fn global() -> &'static ToolSafetyRegistry {
        TOOL_REGISTRY.get_or_init(|| Self::builtin())
    }

    /// Create the built-in registry with default prefix-based classification.
    pub fn builtin() -> Self {
        let mut explicit = HashMap::new();
        for name in [
            "mutation_preview",
            "edit_many_preview",
            "patch_plan",
            "checkpoint_list",
            "checkpoint_diff",
        ] {
            explicit.insert(name.to_string(), ToolSafetyCategory::ReadOnly);
        }
        explicit.insert(
            "apply_patch_transaction".to_string(),
            ToolSafetyCategory::WriteLocal,
        );
        explicit.insert(
            "checkpoint_create".to_string(),
            ToolSafetyCategory::WriteLocal,
        );
        explicit.insert(
            "checkpoint_restore".to_string(),
            ToolSafetyCategory::WriteLocal,
        );
        let patterns = vec![
            ("read".into(), ToolSafetyCategory::ReadOnly),
            ("grep".into(), ToolSafetyCategory::ReadOnly),
            ("glob".into(), ToolSafetyCategory::ReadOnly),
            ("lsp".into(), ToolSafetyCategory::ReadOnly),
            ("write".into(), ToolSafetyCategory::WriteLocal),
            ("edit".into(), ToolSafetyCategory::WriteLocal),
            ("ast_grep".into(), ToolSafetyCategory::WriteLocal),
            ("bash".into(), ToolSafetyCategory::Destructive),
            ("rm".into(), ToolSafetyCategory::Destructive),
            ("web_fetch".into(), ToolSafetyCategory::Network),
            ("web_search".into(), ToolSafetyCategory::Network),
        ];
        Self {
            explicit,
            patterns,
            default: ToolSafetyCategory::WriteLocal,
            tool_timeout_secs: HashMap::new(),
        }
    }

    /// Classify a tool name into a safety category.
    ///
    /// 1. Check explicit (exact-match) entries.
    /// 2. Check prefix patterns in registration order.
    /// 3. Fall back to the default category.
    pub fn classify(&self, tool_name: &str) -> ToolSafetyCategory {
        let normalized = normalize_tool_name_for_safety(tool_name);
        if let Some(cat) = self.explicit.get(&normalized) {
            return *cat;
        }
        for (prefix, cat) in &self.patterns {
            if normalized.starts_with(prefix) {
                return *cat;
            }
        }
        let built_in = ToolSafetyCategory::from_tool_name(&normalized);
        if built_in == ToolSafetyCategory::WriteLocal {
            self.default
        } else {
            built_in
        }
    }

    /// Classify a tool request, refining command-bearing shell tools when the
    /// input exposes enough structured information to do so safely.
    pub fn classify_request(&self, tool_name: &str, input: &str) -> ToolSafetyCategory {
        let normalized = normalize_tool_name_for_safety(tool_name);
        let fallback = self.classify(&normalized);
        if normalized != "bash" {
            return fallback;
        }

        let command = serde_json::from_str::<serde_json::Value>(input)
            .ok()
            .and_then(|value| {
                value
                    .get("command")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
        command.map_or(fallback, |command| match classify_command(&command) {
            CommandIntent::ReadOnly => ToolSafetyCategory::ReadOnly,
            CommandIntent::Write => ToolSafetyCategory::WriteLocal,
            CommandIntent::Network => ToolSafetyCategory::Network,
            CommandIntent::Destructive
            | CommandIntent::ProcessManagement
            | CommandIntent::PackageManagement
            | CommandIntent::SystemAdmin
            | CommandIntent::Unknown => ToolSafetyCategory::Destructive,
        })
    }

    /// Register a custom tool with an explicit category (for plugin tools).
    pub fn register(&mut self, tool_name: &str, category: ToolSafetyCategory) {
        self.explicit
            .insert(normalize_tool_name_for_safety(tool_name), category);
    }

    /// Get the timeout in seconds for a tool.
    ///
    /// 1. Check per-tool override in `tool_timeout_secs`.
    /// 2. Fall back to the category default (`ReadOnly` → 30s, others → 120s).
    pub fn get_timeout_secs(&self, tool_name: &str) -> u64 {
        let normalized = normalize_tool_name_for_safety(tool_name);
        if let Some(&timeout) = self.tool_timeout_secs.get(&normalized) {
            return timeout;
        }
        let cat = self.classify(&normalized);
        cat.default_timeout_secs()
    }

    /// Set a per-tool timeout override (seconds).
    pub fn set_tool_timeout(&mut self, tool_name: &str, timeout_secs: u64) {
        self.tool_timeout_secs
            .insert(normalize_tool_name_for_safety(tool_name), timeout_secs);
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
        crate::budget_policy::RuntimeBudgetPlan::derive(
            crate::budget_policy::RuntimeBudgetInputs::new(
                crate::budget_policy::FALLBACK_MODEL_CONTEXT_WINDOW,
                4_096,
            ),
        )
        .tool_result_budget
        .to_tool_result_budget()
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
                let end = char_prefix_end(output, max_chars);
                format!(
                    "{}...\n[truncated: {} chars omitted]",
                    &output[..end],
                    output.chars().count().saturating_sub(max_chars)
                )
            }
            TruncationStrategy::TailOnly => {
                let start = char_tail_start(output, max_chars);
                format!(
                    "[truncated: {} chars omitted]\n{}",
                    output.chars().count().saturating_sub(max_chars),
                    &output[start..]
                )
            }
            TruncationStrategy::HeadAndTail | TruncationStrategy::Summary => {
                let head_end = char_prefix_end(output, self.head_chars);
                let tail_start = char_tail_start(output, self.tail_chars);
                if tail_start <= head_end {
                    // Output is small enough after all
                    output.to_string()
                } else {
                    let omitted = output[head_end..tail_start].chars().count();
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

fn char_prefix_end(value: &str, max_chars: usize) -> usize {
    value
        .char_indices()
        .nth(max_chars)
        .map(|(idx, _)| idx)
        .unwrap_or(value.len())
}

fn char_tail_start(value: &str, max_chars: usize) -> usize {
    let total = value.chars().count();
    if total <= max_chars {
        return 0;
    }
    value
        .char_indices()
        .nth(total - max_chars)
        .map(|(idx, _)| idx)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// ToolOrchestrator
// ---------------------------------------------------------------------------

/// Orchestrates tool execution based on safety categories and budgets.
#[derive(Clone)]
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

    pub fn set_budget(&mut self, budget: ToolResultBudget) {
        self.budget = budget;
    }

    /// Override the safety category for a specific tool.
    pub fn set_override(&mut self, tool_name: String, category: ToolSafetyCategory) {
        self.overrides
            .insert(normalize_tool_name_for_safety(&tool_name), category);
    }

    /// Get the safety category for a tool (checking overrides first).
    pub fn category_for(&self, tool_name: &str) -> ToolSafetyCategory {
        let normalized = normalize_tool_name_for_safety(tool_name);
        self.overrides
            .get(&normalized)
            .copied()
            .unwrap_or_else(|| ToolSafetyCategory::from_tool_name(&normalized))
    }

    /// Alias for `category_for` — used by concurrency semaphore dispatch.
    pub fn classify(&self, tool_name: &str) -> ToolSafetyCategory {
        self.category_for(tool_name)
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
        assert_eq!(
            ToolSafetyCategory::from_tool_name("read"),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("read_file"),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("grep"),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("grep_search"),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("glob"),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("glob_search"),
            ToolSafetyCategory::ReadOnly
        );
    }

    #[test]
    fn classifies_write_tools() {
        assert_eq!(
            ToolSafetyCategory::from_tool_name("write"),
            ToolSafetyCategory::WriteLocal
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("write_file"),
            ToolSafetyCategory::WriteLocal
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("edit"),
            ToolSafetyCategory::WriteLocal
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("edit_file"),
            ToolSafetyCategory::WriteLocal
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("bash"),
            ToolSafetyCategory::Destructive
        );
    }

    #[test]
    fn classifies_network_tools() {
        assert_eq!(
            ToolSafetyCategory::from_tool_name("web_search"),
            ToolSafetyCategory::Network
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("WebSearch"),
            ToolSafetyCategory::Network
        );
        assert_eq!(
            ToolSafetyCategory::from_tool_name("WebFetch"),
            ToolSafetyCategory::Network
        );
    }

    #[test]
    fn classifies_pure_computation_tools_as_read_only() {
        let registry = ToolSafetyRegistry::builtin();

        for name in ["add", "calculator"] {
            assert_eq!(
                ToolSafetyCategory::from_tool_name(name),
                ToolSafetyCategory::ReadOnly
            );
            assert_eq!(registry.classify(name), ToolSafetyCategory::ReadOnly);
        }
    }

    #[test]
    fn registry_refines_bash_json_commands_by_intent() {
        let registry = ToolSafetyRegistry::builtin();

        assert_eq!(
            registry.classify_request("bash", r#"{"command":"git status"}"#),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            registry.classify_request("bash", r#"{"command":"mkdir target/new"}"#),
            ToolSafetyCategory::WriteLocal
        );
        assert_eq!(
            registry.classify_request("bash", r#"{"command":"curl https://example.com"}"#),
            ToolSafetyCategory::Network
        );
        assert_eq!(
            registry.classify_request("bash", r#"{"command":"rm -rf target"}"#),
            ToolSafetyCategory::Destructive
        );
        assert_eq!(
            registry.classify_request("bash", "not-json"),
            ToolSafetyCategory::Destructive
        );
    }

    #[test]
    fn classifies_destructive_tools() {
        assert_eq!(
            ToolSafetyCategory::from_tool_name("rm"),
            ToolSafetyCategory::Destructive
        );
    }

    #[test]
    fn unknown_defaults_to_write_local() {
        assert_eq!(
            ToolSafetyCategory::from_tool_name("custom_tool"),
            ToolSafetyCategory::WriteLocal
        );
        assert_eq!(
            ToolSafetyRegistry::builtin().classify("custom_tool"),
            ToolSafetyCategory::WriteLocal
        );
    }

    #[test]
    fn registry_matches_direct_classifier_for_core_aliases() {
        let registry = ToolSafetyRegistry::builtin();
        for name in [
            "read",
            "read_file",
            "read_many",
            "grep",
            "grep_search",
            "grep_many",
            "glob",
            "glob_search",
            "glob_many",
            "workspace_snapshot",
            "tool_batch_readonly",
            "tool_cache_stats",
            "mutation_preview",
            "edit_many_preview",
            "patch_plan",
            "checkpoint_list",
            "checkpoint_diff",
            "write",
            "write_file",
            "edit",
            "edit_file",
            "WebFetch",
            "WebSearch",
            "bash",
            "PowerShell",
        ] {
            assert_eq!(
                registry.classify(name),
                ToolSafetyCategory::from_tool_name(name),
                "classification mismatch for {name}"
            );
        }
    }

    #[test]
    fn high_risk_agent_and_runtime_tools_are_not_read_only() {
        let registry = ToolSafetyRegistry::builtin();
        for name in [
            "Agent",
            "TaskCreate",
            "RunTaskPacket",
            "WorkerCreate",
            "WorkerSendPrompt",
            "TeamCreate",
            "RuntimeOrchestrate",
            "MCP",
            "McpAuth",
            "REPL",
            "PowerShell",
            "Config",
            "NotebookEdit",
            "RemoteTrigger",
            "execute_code",
        ] {
            assert_ne!(
                registry.classify(name),
                ToolSafetyCategory::ReadOnly,
                "{name} must not be treated as read-only"
            );
        }
    }

    #[test]
    fn read_only_git_queries_are_not_caught_by_prefix_rules() {
        let registry = ToolSafetyRegistry::builtin();
        for name in ["git_status", "git_log", "git_diff", "git_show"] {
            assert_eq!(
                registry.classify(name),
                ToolSafetyCategory::ReadOnly,
                "{name} should stay read-only"
            );
        }
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
        assert_eq!(
            orch.category_for("custom_read"),
            ToolSafetyCategory::ReadOnly
        );
    }

    #[test]
    fn override_lookup_uses_normalized_tool_names() {
        let mut orch = ToolOrchestrator::new();
        orch.set_override("WebFetch".to_string(), ToolSafetyCategory::Destructive);
        assert_eq!(
            orch.category_for("web_fetch"),
            ToolSafetyCategory::Destructive
        );
        assert_eq!(
            orch.category_for("WebFetch"),
            ToolSafetyCategory::Destructive
        );
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
    fn truncation_is_utf8_boundary_safe() {
        let budget = ToolResultBudget {
            per_tool_max_tokens: 10,
            truncation_strategy: TruncationStrategy::HeadAndTail,
            head_chars: 7,
            tail_chars: 7,
            ..ToolResultBudget::default()
        };
        let long_text = format!("{}{}{}", "─".repeat(20), "中文内容", "─".repeat(20));
        let truncated = budget.truncate(&long_text);
        assert!(truncated.contains("[truncated"));
        assert!(truncated.is_char_boundary(truncated.len()));
    }

    #[test]
    fn truncation_head_only_is_utf8_boundary_safe() {
        let budget = ToolResultBudget {
            per_tool_max_tokens: 750,
            truncation_strategy: TruncationStrategy::HeadOnly,
            ..ToolResultBudget::default()
        };
        let long_text = format!("{}{}", "─".repeat(1200), "中文内容".repeat(1200));

        let truncated = budget.truncate(&long_text);

        assert!(truncated.contains("[truncated"));
        assert!(truncated.starts_with('─'));
    }

    #[test]
    fn max_concurrency_values() {
        assert_eq!(ToolSafetyCategory::ReadOnly.max_concurrency(), usize::MAX);
        assert_eq!(ToolSafetyCategory::Network.max_concurrency(), 3);
        assert_eq!(ToolSafetyCategory::Destructive.max_concurrency(), 1);
    }

    #[test]
    fn execution_profile_exposes_cache_and_prepared_capabilities() {
        let read = tool_execution_profile("read_file");
        assert_eq!(read.safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(read.cache_policy, ToolCachePolicy::ScopedRead);
        assert!(read.prepared_readonly_supported);
        assert_eq!(read.max_concurrency, usize::MAX);

        let write = tool_execution_profile("write_file");
        assert_eq!(write.safety_category, ToolSafetyCategory::WriteLocal);
        assert_eq!(write.cache_policy, ToolCachePolicy::ScopeInvalidatingWrite);
        assert!(!write.prepared_readonly_supported);

        let restore = tool_execution_profile("checkpoint_restore");
        assert_eq!(
            restore.cache_policy,
            ToolCachePolicy::GlobalInvalidatingWrite
        );
    }

    #[test]
    fn runtime_capabilities_is_readonly_and_orchestrate_is_stateful_runtime_entry() {
        let direct = ToolSafetyCategory::from_tool_name("runtime_capabilities");
        let alias = ToolSafetyCategory::from_tool_name("RuntimeCapabilities");
        let orchestrate = ToolSafetyCategory::from_tool_name("RuntimeOrchestrate");
        let registry = ToolSafetyRegistry::builtin();
        let profile = tool_execution_profile("runtime_capabilities");
        let orchestrate_profile = tool_execution_profile("runtime_orchestrate");

        assert_eq!(direct, ToolSafetyCategory::ReadOnly);
        assert_eq!(alias, ToolSafetyCategory::ReadOnly);
        assert_eq!(orchestrate, ToolSafetyCategory::Destructive);
        assert_eq!(
            registry.classify("runtime_capabilities"),
            ToolSafetyCategory::ReadOnly
        );
        assert_eq!(
            registry.classify("runtime_orchestrate"),
            ToolSafetyCategory::Destructive
        );
        assert_eq!(profile.safety_category, ToolSafetyCategory::ReadOnly);
        assert_eq!(
            orchestrate_profile.safety_category,
            ToolSafetyCategory::Destructive
        );
        assert_eq!(profile.max_concurrency, usize::MAX);
        assert_eq!(orchestrate_profile.max_concurrency, 1);
    }
}
