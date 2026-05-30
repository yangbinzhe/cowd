//! Agent and SubAgent runtime for delegating sub-tasks.
//!
//! A `SubAgent` runs with restricted capabilities: a limited set of tools,
//! a write guard that prevents writing to protected memory layers (L0/L1),
//! and a token budget that caps its execution.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::tool_orchestrator::ToolResultBudget;

use memory::cognitive::CognitiveContextManager;
use memory::types::{MemoryLayer, MemoryCategory, MemorySource, Priority};
use memory::project_scope::MemoryScope;
use memory::types::AgentVisibility;

pub trait SubAgentProgressCallback: Send + Sync {
    fn on_turn_complete(&self, turn: u32, max_turns: usize, tokens_used: usize);
    fn on_tool_call(&self, tool_name: &str, input_preview: &str);
    fn on_budget_warning(&self, remaining_tokens: usize);
}

// ---------------------------------------------------------------------------
// SubAgentExecutor
// ---------------------------------------------------------------------------

/// Trait for executing a sub-agent with a given configuration and task.
///
/// Implementations provide the runtime machinery to spawn and run a sub-agent,
/// returning a structured `SubAgentResult` or a `SubAgentError`.
/// This is a static dispatch trait — callers invoke `E::execute(config, task)`
/// for a concrete type `E`.
pub trait SubAgentExecutor: Send + Sync {
    fn execute(
        config: SubAgentConfig,
        task: &str,
    ) -> impl std::future::Future<Output = Result<SubAgentResult, SubAgentError>> + Send;
}

// ---------------------------------------------------------------------------
// AgentRole
// ---------------------------------------------------------------------------

/// The role assigned to a SubAgent, determining its default tool set and behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AgentRole {
    /// Full unrestricted tool access.
    General,
    /// Planning/analysis tools (read, grep, glob, lsp).
    Planner,
    /// Execution/modification tools (read, write, edit, bash).
    Executor,
    /// Review/inspection tools (read, grep, glob, lsp diagnostics, diff).
    Reviewer,
}

impl Default for AgentRole {
    fn default() -> Self {
        Self::General
    }
}

/// Return the default tool allowlist for a given `AgentRole`.
fn role_tools(role: AgentRole) -> Vec<String> {
    match role {
        AgentRole::Planner => vec!["read", "grep", "glob", "lsp_symbols", "lsp_goto_definition"],
        AgentRole::Executor => vec!["read", "write", "edit", "bash", "grep", "glob"],
        AgentRole::Reviewer => vec!["read", "grep", "glob", "lsp_diagnostics", "git_diff"],
        AgentRole::General => vec![],
    }
    .into_iter()
    .map(String::from)
    .collect()
}

// ---------------------------------------------------------------------------
// SubAgentToolMode
// ---------------------------------------------------------------------------

/// Controls which tools a sub-agent is allowed to use.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SubAgentToolMode {
    /// Full tool access — no restrictions (default).
    FullToolSet,
    /// Read-only tools only (read, grep, glob, lsp, etc.).
    ReadOnly,
    /// Custom allowlist of tool names.
    Custom(Vec<String>),
}

impl Default for SubAgentToolMode {
    fn default() -> Self {
        Self::FullToolSet
    }
}

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
    /// Optional timeout in seconds. None means no timeout.
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Maximum number of sub-agent tasks to execute in parallel (default 4).
    #[serde(default = "default_max_parallel")]
    pub max_parallel: usize,
    /// Optional model override — allows different LLM per agent role.
    /// When `None`, the parent's model is used.
    #[serde(default)]
    pub model: Option<String>,
    /// Tool access mode: full, read-only, or custom allowlist.
    #[serde(default)]
    pub tool_mode: SubAgentToolMode,
    /// Human-readable role label for agent discovery (e.g. "Executor", "Reviewer").
    #[serde(default = "default_agent_role")]
    pub agent_role: String,
    /// Declared capabilities for mutual discovery (e.g. ["rust", "testing"]).
    #[serde(default)]
    pub capabilities: Vec<String>,
    /// The agent's role — determines default tool set (overridden by tool_mode/allowed_tools).
    #[serde(default)]
    pub role: AgentRole,
    /// Inject peer agent context from AgentDirectory into the system prompt.
    #[serde(default = "default_true")]
    pub inject_peer_context: bool,
    /// Inject parent agent memory into the system prompt via `prepare_context`.
    #[serde(default = "default_true")]
    pub inject_memory: bool,
    /// Capture the sub-agent's reasoning trace (ThinkingDelta events) in the result.
    #[serde(default = "default_true")]
    pub retain_reasoning: bool,
    /// Parent session ID for delegation traceability.
    #[serde(default)]
    pub session_id: Option<String>,
}

fn default_write_source() -> String {
    "SubAgent".to_string()
}

fn default_agent_role() -> String {
    "SubAgent".to_string()
}

fn default_max_turns() -> usize {
    10
}

fn default_budget_tokens() -> usize {
    20_000
}

fn default_max_parallel() -> usize {
    4
}

fn default_true() -> bool {
    true
}

impl Default for SubAgentConfig {
    fn default() -> Self {
        Self {
            task_description: String::new(),
            allowed_tools: vec![],
            write_source: default_write_source(),
            max_turns: default_max_turns(),
            budget_tokens: default_budget_tokens(),
            timeout_secs: None,
            max_parallel: default_max_parallel(),
            model: None,
            tool_mode: SubAgentToolMode::default(),
            agent_role: default_agent_role(),
            capabilities: vec![],
            role: AgentRole::default(),
            inject_peer_context: true,
            inject_memory: true,
            retain_reasoning: true,
            session_id: None,
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
    /// Reasoning trace captured from ThinkingDelta events (when retain_reasoning is enabled).
    #[serde(default)]
    pub reasoning_trace: Option<String>,
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
            reasoning_trace: None,
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
    #[error("sub-agent timed out after {0}s")]
    Timeout(u64),
}

/// Request to delegate a task to a sub-agent.
#[derive(Debug, Clone)]
pub struct DelegationRequest {
    /// The task description to execute.
    pub task: String,
    /// Context information for the sub-agent.
    pub context: String,
    /// Expected output format/description.
    pub expected_output: String,
    /// Parent session ID for traceability.
    pub parent_session_id: String,
}

// ---------------------------------------------------------------------------
// SubAgentRuntime
// ---------------------------------------------------------------------------

use crate::conversation::{ApiClient, ConversationRuntime, ToolExecutor};

/// Runtime for executing sub-agents with independent LLM reasoning.
///
/// Each `SubAgentRuntime` owns its own `ConversationRuntime`, enabling:
/// - Independent model selection per agent role
/// - Filtered system prompts scoped to the sub-task
/// - Memory injection from the parent agent's context
/// - Tool access restricted by `SubAgentToolMode`
/// - Results shared back to parent via L4 `team_remember`
pub struct SubAgentRuntime<C: ApiClient, T: ToolExecutor> {
    config: SubAgentConfig,
    runtime: ConversationRuntime<C, T>,
    result_budget: ToolResultBudget,
    turns_executed: usize,
    tokens_consumed: usize,
    started_at: Instant,
    progress_callback: Option<Arc<dyn SubAgentProgressCallback>>,
    /// Reference to parent's memory manager for L4 `team_remember` sharing.
    parent_memory: Option<Arc<CognitiveContextManager>>,
    /// Unique identifier for this agent instance (used for AgentDirectory registration).
    agent_id: String,
    /// Whether this agent has been registered in the global AgentDirectory.
    registered: AtomicBool,
    /// Captured reasoning trace from ThinkingDelta events.
    reasoning_trace: Option<String>,
    /// Reputation manager for recording task completion metrics (P9.1).
    reputation_manager: Option<memory::agent_reputation::ReputationManager>,
}

impl<C: ApiClient, T: ToolExecutor> SubAgentRuntime<C, T> {
    /// Create a new sub-agent runtime with the given configuration and conversation runtime.
    #[must_use]
    pub fn new(config: SubAgentConfig, runtime: ConversationRuntime<C, T>) -> Self {
        let agent_id = uuid::Uuid::new_v4().to_string();

        // Register this sub-agent in the global agent directory.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let info = memory::agent_directory::AgentInfo {
            agent_id: agent_id.clone(),
            role: config.agent_role.clone(),
            capabilities: config.capabilities.clone(),
            status: memory::agent_directory::AgentStatus::Active,
            registered_at_ms: now_ms,
            last_heartbeat_ms: now_ms,
            reputation: None,
        };
        memory::agent_directory::AgentDirectory::global().register(info);

        Self {
            result_budget: ToolResultBudget::default(),
            turns_executed: 0,
            tokens_consumed: 0,
            started_at: Instant::now(),
            progress_callback: None,
            parent_memory: None,
            agent_id,
            registered: AtomicBool::new(true),
            reasoning_trace: None,
            reputation_manager: None,
            config,
            runtime,
        }
    }

    /// Set the parent memory manager for L4 team knowledge sharing.
    #[must_use]
    pub fn with_parent_memory(mut self, memory: Arc<CognitiveContextManager>) -> Self {
        self.parent_memory = Some(memory);
        self
    }

    /// Create with a custom result budget.
    #[must_use]
    pub fn with_result_budget(mut self, budget: ToolResultBudget) -> Self {
        self.result_budget = budget;
        self
    }

    /// Attach a reputation manager for P9.1 agent performance tracking.
    #[must_use]
    pub fn with_reputation_manager(
        mut self,
        mgr: memory::agent_reputation::ReputationManager,
    ) -> Self {
        self.reputation_manager = Some(mgr);
        self
    }

    pub fn set_progress_callback(&mut self, cb: Arc<dyn SubAgentProgressCallback>) {
        self.progress_callback = Some(cb);
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

    pub fn check_timeout(&self) -> Result<(), SubAgentError> {
        if let Some(timeout) = self.config.timeout_secs {
            if self.started_at.elapsed() >= Duration::from_secs(timeout) {
                return Err(SubAgentError::Timeout(timeout));
            }
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
        // Unregister from the global agent directory on first result build.
        if self.registered.swap(false, Ordering::SeqCst) {
            memory::agent_directory::AgentDirectory::global()
                .unregister(&self.agent_id);
        }

        if let Some(ref m) = self.parent_memory {
            m.observe_delegation("sub-agent", self.task_description(), &output, None);
        }

        let completed_normally = self.turns_executed <= self.config.max_turns
            && self.tokens_consumed <= self.config.budget_tokens;

        // P9.1: Record completion metrics for reputation tracking.
        if let Some(ref rep_mgr) = self.reputation_manager {
            let quality = if completed_normally { 0.85 } else { 0.4 };
            let domains: Vec<String> = self.config.capabilities.clone();
            let _ = rep_mgr.record_completion(
                &self.agent_id,
                quality,
                completed_normally,
                &domains,
            );

            // P9.1: Sync reputation to AgentDirectory for TeamDiscovery consumption.
            if let Ok(Some(metrics)) = rep_mgr.get(&self.agent_id) {
                use memory::agent_directory::ReputationScore;
                memory::agent_directory::AgentDirectory::global().update_reputation(
                    &self.agent_id,
                    ReputationScore {
                        success_rate: metrics.avg_quality_score,
                        task_count: metrics.tasks_completed,
                        peer_rating: 0.0,
                        last_success_at_ms: metrics.updated_at.timestamp_millis() as u64,
                        recent_failures: 0,
                    },
                );
            }
        }

        SubAgentResult {
            output,
            tool_call_count: 0, // caller should track
            tokens_used: self.tokens_consumed,
            // 3A-4 fix: use <= instead of < — using exactly max_turns or
            // exactly budget_tokens is still within budget and counts as
            // normal completion.
            completed_normally,
            memory_write_attempts: 0,
            memory_writes_denied: 0,
            reasoning_trace: self.reasoning_trace.clone(),
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

    /// Run the sub-agent loop using the owned `ConversationRuntime` for
    /// independent LLM reasoning with its own model, system prompt, and
    /// tool filtering.
    ///
    /// After completion, results are shared with the parent agent via
    /// L4 `team_remember` if a parent memory manager is configured.
    pub async fn run_loop_async(
        &mut self,
        initial_prompt: &str,
    ) -> SubAgentResult {
        use crate::permissions::SharedPrompter;

        let mut output_parts: Vec<String> = Vec::new();
        let mut tool_call_count: usize = 0;
        let memory_write_attempts: usize = 0;
        let memory_writes_denied: usize = 0;
        let mut current_prompt = initial_prompt.to_string();
        let mut completed_normally = true;

        // P7.4: Inject parent agent memory into the system prompt before the main loop.
        if self.config.inject_memory {
            if let Some(ref mem) = self.parent_memory {
                match mem.prepare_context(initial_prompt, &[], None).await {
                    Ok(prepared) => {
                        let mem_section = format_prepared_context(&prepared);
                        current_prompt = format!("{mem_section}\n\n{current_prompt}");
                        tracing::debug!(
                            "Injected {} memory entries into sub-agent context",
                            prepared.entries.len()
                        );
                    }
                    Err(e) => {
                        tracing::warn!("Failed to prepare memory context for sub-agent: {}", e);
                    }
                }
            }
        }

        loop {
            if let Err(e) = self.check_budget() {
                tracing::warn!("SubAgent budget exhausted: {}", e);
                completed_normally = false;
                break;
            }
            if let Err(e) = self.check_timeout() {
                tracing::warn!("SubAgent timed out: {}", e);
                completed_normally = false;
                break;
            }

            let prompter = SharedPrompter::none();
            let summary = match self.runtime.run_turn_async(&current_prompt, &prompter).await {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("SubAgent turn failed: {}", e);
                    completed_normally = false;
                    break;
                }
            };

            let turn_text: String = summary
                .assistant_messages
                .iter()
                .flat_map(|msg| &msg.blocks)
                .filter_map(|block| match block {
                    crate::session::ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join("\n");

            let turn_tool_calls = summary
                .assistant_messages
                .iter()
                .flat_map(|msg| &msg.blocks)
                .filter(|block| matches!(block, crate::session::ContentBlock::ToolUse { .. }))
                .count();

            let tokens = summary.usage.total_tokens() as usize;
            self.record_turn(tokens);
            tool_call_count += turn_tool_calls;

            // P7.4: Capture reasoning/thinking trace from the assistant's response.
            if self.config.retain_reasoning {
                for msg in &summary.assistant_messages {
                    for block in &msg.blocks {
                        if let crate::session::ContentBlock::Thinking { thinking, .. } = block {
                            self.reasoning_trace
                                .get_or_insert_with(String::new)
                                .push_str(thinking);
                        }
                    }
                }
            }

            if let Some(ref cb) = self.progress_callback {
                cb.on_turn_complete(
                    self.turns_executed as u32,
                    self.config.max_turns,
                    self.tokens_consumed,
                );
            }

            output_parts.push(turn_text.clone());

            if turn_tool_calls == 0 {
                break;
            }

            let tool_outputs: Vec<String> = summary
                .tool_results
                .iter()
                .flat_map(|msg| &msg.blocks)
                .filter_map(|block| match block {
                    crate::session::ContentBlock::ToolResult { tool_name, output, .. } => {
                        Some(format!(
                            "Tool {} returned: {}",
                            tool_name,
                            truncate_str(output, 500)
                        ))
                    }
                    _ => None,
                })
                .collect();

            if !tool_outputs.is_empty() {
                current_prompt =
                    format!("Continue based on tool results:\n{}", tool_outputs.join("\n"));
            } else {
                break;
            }
        }

        let final_output = output_parts.join("\n");

        if let Some(ref mem) = self.parent_memory {
            let task_desc = self.config.task_description.clone();
            let output_snippet = truncate_str(&final_output, 2000);
            let parent_agent = "primary".to_string();
            let _ = team_remember_result(
                mem,
                &task_desc,
                &output_snippet,
                &parent_agent,
            ).await;
        }

        // P9.1: Record completion metrics for reputation tracking.
        if let Some(ref rep_mgr) = self.reputation_manager {
            let quality = if completed_normally { 0.85 } else { 0.4 };
            let on_time = completed_normally;
            let domains: Vec<String> = self.config.capabilities.clone();
            let _ = rep_mgr.record_completion(
                &self.agent_id,
                quality,
                on_time,
                &domains,
            );

            // P9.1: Sync reputation to AgentDirectory for TeamDiscovery consumption.
            if let Ok(Some(metrics)) = rep_mgr.get(&self.agent_id) {
                use memory::agent_directory::ReputationScore;
                memory::agent_directory::AgentDirectory::global().update_reputation(
                    &self.agent_id,
                    ReputationScore {
                        success_rate: metrics.avg_quality_score,
                        task_count: metrics.tasks_completed,
                        peer_rating: 0.0,
                        last_success_at_ms: metrics.updated_at.timestamp_millis() as u64,
                        recent_failures: 0,
                    },
                );
            }
        }

        SubAgentResult {
            output: final_output,
            tool_call_count,
            tokens_used: self.tokens_consumed,
            completed_normally,
            memory_write_attempts,
            memory_writes_denied,
            reasoning_trace: self.reasoning_trace.take(),
        }
    }

    /// Execute a single sub-agent request to completion using the owned runtime.
    pub async fn execute_single(
        config: SubAgentConfig,
        req: DelegationRequest,
        runtime: ConversationRuntime<C, T>,
    ) -> SubAgentResult {
        let prompt = format!(
            "Task: {}\nContext: {}\nExpected output: {}",
            req.task, req.context, req.expected_output
        );
        let mut sub_runtime = SubAgentRuntime::new(config, runtime);
        sub_runtime.run_loop_async(&prompt).await
    }

    /// Execute multiple sub-agent requests sequentially.
    ///
    /// Each sub-agent gets its own `ConversationRuntime` created by
    /// `runtime_factory`. Processing is sequential to avoid Send issues
    /// with the memory subsystem.
    pub async fn execute_parallel(
        config: SubAgentConfig,
        requests: Vec<DelegationRequest>,
        runtime_factory: impl Fn() -> ConversationRuntime<C, T>,
    ) -> Vec<SubAgentResult> {
        let mut results = Vec::with_capacity(requests.len());
        for req in requests {
            let rt = runtime_factory();
            let result = SubAgentRuntime::execute_single(config.clone(), req, rt).await;
            results.push(result);
        }
        results
    }
}

/// Truncate a string to at most `max_len` characters, appending "..." if truncated.
fn truncate_str(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max_len).collect();
        format!("{}...", truncated)
    }
}

/// Format a `PreparedContext` into a memory section for injection into the system prompt.
fn format_prepared_context(prepared: &memory::types::PreparedContext) -> String {
    let mut buf = String::from("## Memory Context (injected)\n\n");
    if !prepared.entries.is_empty() {
        buf.push_str("### Relevant Memories\n\n");
        for entry in &prepared.entries {
            buf.push_str(&format!(
                "- **{}** ({:?}, confidence: {:.2})\n  {}\n",
                entry.title,
                entry.layer,
                entry.confidence,
                truncate_str(&entry.content, 300),
            ));
        }
    }
    if let Some(ref code_ctx) = prepared.code_context {
        if !code_ctx.is_empty() {
            buf.push_str("\n### Code Context\n\n");
            buf.push_str(code_ctx);
            buf.push('\n');
        }
    }
    buf
}

/// Share sub-agent results with the parent agent via L4 `team_remember`.
async fn team_remember_result(
    memory: &CognitiveContextManager,
    task_description: &str,
    result_output: &str,
    parent_agent: &str,
) {
    use memory::types::{MemoryEntry, MemoryId};
    use chrono::Utc;

    let entry = MemoryEntry {
        id: MemoryId::new_v4(),
        layer: MemoryLayer::L4,
        category: MemoryCategory::Shared,
        priority: Priority::Normal,
        source: MemorySource::Import,
        title: format!("sub-agent: {}", truncate_str(task_description, 120)),
        content: format!(
            "## Sub-Agent Task\n\n{}\n\n## Result\n\n{}",
            task_description, result_output
        ),
        embedding: None,
        tags: vec!["sub-agent".to_string(), "team-shared".to_string()],
        relations: vec![],
        confidence: 0.85,
        access_count: 0,
        staleness: 0.0,
        created_at: Utc::now(),
        updated_at: Utc::now(),
        last_accessed_at: None,
        scope: MemoryScope::Project("default".to_string()),
        session_id: None,
        source_agent: Some(parent_agent.to_string()),
        visibility: AgentVisibility::Shared,
    };

    if let Err(e) = memory.remember(entry).await {
        tracing::warn!("failed to share sub-agent result via L4: {}", e);
    } else {
        tracing::debug!("sub-agent result shared to L4 team memory");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::pin::Pin;
    use futures::stream::Stream;

    struct MockApiClient;

    impl ApiClient for MockApiClient {
        fn stream(
            &mut self,
            _request: crate::conversation::ApiRequest,
        ) -> Pin<Box<dyn Stream<Item = Result<crate::conversation::AssistantEvent, crate::conversation::RuntimeError>> + Send + '_>> {
            Box::pin(futures::stream::iter(vec![
                Ok(crate::conversation::AssistantEvent::TextDelta("mock".to_string())),
                Ok(crate::conversation::AssistantEvent::MessageStop),
            ]))
        }
    }

    struct MockToolExecutor;

    impl crate::conversation::ToolExecutor for MockToolExecutor {
        fn execute(&self, _tool_name: &str, _input: &str) -> Result<String, crate::conversation::ToolError> {
            Ok("mock result".to_string())
        }
    }

    fn make_dummy_runtime() -> crate::conversation::ConversationRuntime<MockApiClient, MockToolExecutor> {
        use crate::permissions::{PermissionMode, PermissionPolicy};
        use crate::session::Session;

        crate::conversation::ConversationRuntime::new(
            Session::new(),
            MockApiClient,
            MockToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["system".to_string()],
        )
    }

    #[test]
    fn default_config_values() {
        let config = SubAgentConfig::default();
        assert_eq!(config.max_turns, 10);
        assert_eq!(config.budget_tokens, 20_000);
        assert_eq!(config.write_source, "SubAgent");
        assert_eq!(config.model, None);
        assert_eq!(config.tool_mode, SubAgentToolMode::FullToolSet);
    }

    #[test]
    fn tool_allowed_when_no_restrictions() {
        let runtime = SubAgentRuntime::new(SubAgentConfig::default(), make_dummy_runtime());
        assert!(runtime.is_tool_allowed("read"));
        assert!(runtime.is_tool_allowed("bash"));
    }

    #[test]
    fn tool_blocked_when_not_in_allowed_list() {
        let config = SubAgentConfig {
            allowed_tools: vec!["read".to_string(), "grep".to_string()],
            ..SubAgentConfig::default()
        };
        let runtime = SubAgentRuntime::new(config, make_dummy_runtime());
        assert!(runtime.is_tool_allowed("read"));
        assert!(!runtime.is_tool_allowed("bash"));
    }

    #[test]
    fn budget_check_passes_initially() {
        let runtime = SubAgentRuntime::new(SubAgentConfig::default(), make_dummy_runtime());
        assert!(runtime.check_budget().is_ok());
    }

    #[test]
    fn budget_check_fails_after_max_turns() {
        let config = SubAgentConfig {
            max_turns: 2,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config, make_dummy_runtime());
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
        let mut runtime = SubAgentRuntime::new(config, make_dummy_runtime());
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
        let mut runtime = SubAgentRuntime::new(config, make_dummy_runtime());
        assert_eq!(runtime.remaining_budget(), 1000);
        runtime.record_turn(300);
        assert_eq!(runtime.remaining_budget(), 700);
    }

    #[test]
    fn result_truncation() {
        let runtime = SubAgentRuntime::new(SubAgentConfig::default(), make_dummy_runtime());
        let long_output = "x".repeat(100_000);
        let truncated = runtime.truncate_result(&long_output);
        assert!(truncated.len() < long_output.len());
    }

    #[test]
    fn completed_normally_true_when_within_budget() {
        let config = SubAgentConfig {
            max_turns: 3,
            budget_tokens: 1000,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config, make_dummy_runtime());
        runtime.record_turn(100);
        runtime.record_turn(100);
        runtime.record_turn(100);
        let result = runtime.build_result("done".to_string());
        assert!(result.completed_normally);
    }

    #[test]
    fn completed_normally_false_when_over_budget() {
        let config = SubAgentConfig {
            max_turns: 3,
            budget_tokens: 100,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config, make_dummy_runtime());
        runtime.record_turn(200);
        let result = runtime.build_result("done".to_string());
        assert!(!result.completed_normally);
    }

    #[test]
    fn role_tools_returns_appropriate_sets() {
        // General: empty list
        assert!(role_tools(AgentRole::General).is_empty());

        // Planner: read, grep, glob, lsp
        let planner = role_tools(AgentRole::Planner);
        assert!(planner.contains(&"read".to_string()));
        assert!(planner.contains(&"grep".to_string()));
        assert!(planner.contains(&"glob".to_string()));
        assert!(planner.contains(&"lsp_symbols".to_string()));
        assert!(!planner.contains(&"bash".to_string()));
        assert!(!planner.contains(&"write".to_string()));

        // Executor: read, write, edit, bash, grep, glob
        let executor = role_tools(AgentRole::Executor);
        assert!(executor.contains(&"bash".to_string()));
        assert!(executor.contains(&"write".to_string()));
        assert!(executor.contains(&"edit".to_string()));
        assert!(executor.contains(&"read".to_string()));
        assert!(!executor.contains(&"lsp_diagnostics".to_string()));

        // Reviewer: read, grep, glob, lsp_diagnostics, git_diff
        let reviewer = role_tools(AgentRole::Reviewer);
        assert!(reviewer.contains(&"lsp_diagnostics".to_string()));
        assert!(reviewer.contains(&"git_diff".to_string()));
        assert!(reviewer.contains(&"read".to_string()));
        assert!(!reviewer.contains(&"bash".to_string()));
        assert!(!reviewer.contains(&"write".to_string()));
    }

    #[test]
    fn new_config_defaults() {
        let config = SubAgentConfig::default();
        assert_eq!(config.role, AgentRole::General);
        assert!(config.inject_peer_context);
        assert!(config.inject_memory);
        assert!(config.retain_reasoning);
    }

    #[test]
    fn result_default_has_no_reasoning_trace() {
        let result = SubAgentResult::default();
        assert!(result.reasoning_trace.is_none());
    }
}
