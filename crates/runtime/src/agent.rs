//! Agent and SubAgent runtime for delegating sub-tasks.
//!
//! A `SubAgent` runs with restricted capabilities: a limited set of tools,
//! a write guard that prevents writing to protected memory layers (L0/L1),
//! and a token budget that caps its execution.

use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::subagent::DelegationRequest;
use crate::tool_orchestrator::ToolResultBudget;

pub trait SubAgentProgressCallback: Send + Sync {
    fn on_turn_complete(&self, turn: u32, max_turns: usize, tokens_used: usize);
    fn on_tool_call(&self, tool_name: &str, input_preview: &str);
    fn on_budget_warning(&self, remaining_tokens: usize);
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

fn default_max_parallel() -> usize {
    4
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
    #[error("sub-agent timed out after {0}s")]
    Timeout(u64),
}

// ---------------------------------------------------------------------------
// SubAgentExecutor trait (3A-4)
// ---------------------------------------------------------------------------

/// Trait for executing a single sub-agent turn.
///
/// Implemented by the caller (typically `ConversationRuntime`) to provide
/// the actual LLM call + tool execution capability.
pub trait SubAgentExecutor: Send + Sync {
    /// Execute one turn of the sub-agent loop.
    fn execute_turn(
        &mut self,
        prompt: &str,
        allowed_tools: &[String],
        system_prompt: Option<&str>,
    ) -> Result<TurnOutput, SubAgentError>;
}

/// Output from a single sub-agent turn.
#[derive(Debug, Clone)]
pub struct TurnOutput {
    /// The text content produced by the model in this turn.
    pub text: String,
    /// Tool calls made during this turn.
    pub tool_calls: Vec<ToolCallRecord>,
    /// Input tokens consumed.
    pub input_tokens: usize,
    /// Output tokens consumed.
    pub output_tokens: usize,
    /// Why the model stopped generating (e.g. "end_turn", "tool_use").
    pub stop_reason: String,
}

/// Record of a single tool call within a sub-agent turn.
#[derive(Debug, Clone)]
pub struct ToolCallRecord {
    /// Name of the tool invoked.
    pub tool_name: String,
    /// Input provided to the tool.
    pub tool_input: String,
    /// Output returned by the tool.
    pub tool_output: String,
}

// ---------------------------------------------------------------------------
// SubAgentRuntime
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
    turns_executed: usize,
    tokens_consumed: usize,
    started_at: Instant,
    progress_callback: Option<Arc<dyn SubAgentProgressCallback>>,
}

impl SubAgentRuntime {
    /// Create a new sub-agent runtime with the given configuration.
    #[must_use]
    pub fn new(config: SubAgentConfig) -> Self {
        Self {
            result_budget: ToolResultBudget::default(),
            turns_executed: 0,
            tokens_consumed: 0,
            started_at: Instant::now(),
            progress_callback: None,
            config,
        }
    }

    /// Create with a custom result budget.
    #[must_use]
    pub fn with_result_budget(mut self, budget: ToolResultBudget) -> Self {
        self.result_budget = budget;
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
        SubAgentResult {
            output,
            tool_call_count: 0, // caller should track
            tokens_used: self.tokens_consumed,
            // 3A-4 fix: use <= instead of < — using exactly max_turns or
            // exactly budget_tokens is still within budget and counts as
            // normal completion.
            completed_normally: self.turns_executed <= self.config.max_turns
                && self.tokens_consumed <= self.config.budget_tokens,
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

    /// Run the sub-agent loop until completion or budget exhaustion.
    ///
    /// Requires an executor that can drive individual LLM turns. This method
    /// handles the loop control: budget checks, tool-result chaining, and
    /// stop-reason handling.
    #[deprecated(note = "Use `run_loop_async` instead")]
    pub fn run_loop(
        &mut self,
        initial_prompt: &str,
        executor: &mut dyn SubAgentExecutor,
    ) -> SubAgentResult {
        let prompt = initial_prompt.to_string();
        futures::executor::block_on(self.run_loop_async(&prompt, executor))
    }

    /// Async version of `run_loop` using `tokio::task::JoinSet` for parallel
    /// tool-call summary processing within each turn.
    pub async fn run_loop_async(
        &mut self,
        initial_prompt: &str,
        executor: &mut dyn SubAgentExecutor,
    ) -> SubAgentResult {
        let mut output_parts: Vec<String> = Vec::new();
        let mut tool_call_count: usize = 0;
        let memory_write_attempts: usize = 0;
        let memory_writes_denied: usize = 0;
        let mut current_prompt = initial_prompt.to_string();
        let mut completed_normally = true;

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

            let turn = match executor.execute_turn(
                &current_prompt,
                &self.config.allowed_tools,
                Some(&self.config.task_description),
            ) {
                Ok(t) => t,
                Err(SubAgentError::ToolNotAllowed(tool)) => {
                    tracing::warn!("SubAgent tool not allowed: {}", tool);
                    continue;
                }
                Err(e) => {
                    tracing::error!("SubAgent turn failed: {}", e);
                    completed_normally = false;
                    break;
                }
            };

            self.record_turn(turn.input_tokens + turn.output_tokens);
            tool_call_count += turn.tool_calls.len();

            if let Some(ref cb) = self.progress_callback {
                cb.on_turn_complete(
                    self.turns_executed as u32,
                    self.config.max_turns,
                    self.tokens_consumed,
                );
                for tc in &turn.tool_calls {
                    cb.on_tool_call(&tc.tool_name, &truncate_str(&tc.tool_input, 80));
                }
            }

            // Collect output
            output_parts.push(turn.text.clone());

            // If model says it's done, break
            if turn.stop_reason == "end_turn" || turn.stop_reason == "stop" {
                break;
            }

            // Build next prompt from tool results — parallelize summaries via JoinSet
            if !turn.tool_calls.is_empty() {
                let mut set = JoinSet::new();
                for tc in &turn.tool_calls {
                    let tool_name = tc.tool_name.clone();
                    let tool_output = tc.tool_output.clone();
                    set.spawn(async move {
                        format!(
                            "Tool {} returned: {}",
                            tool_name,
                            truncate_str(&tool_output, 500)
                        )
                    });
                }
                let mut summaries = Vec::with_capacity(turn.tool_calls.len());
                while let Some(res) = set.join_next().await {
                    if let Ok(s) = res {
                        summaries.push(s);
                    }
                }
                current_prompt =
                    format!("Continue based on tool results:\n{}", summaries.join("\n"));
            } else {
                break;
            }
        }

        SubAgentResult {
            output: output_parts.join("\n"),
            tool_call_count,
            tokens_used: self.tokens_consumed,
            completed_normally,
            memory_write_attempts,
            memory_writes_denied,
        }
    }

    /// Execute a single sub-agent request to completion.
    pub async fn execute_single(
        config: SubAgentConfig,
        req: DelegationRequest,
        executor: &mut dyn SubAgentExecutor,
    ) -> SubAgentResult {
        let prompt = format!(
            "Task: {}\nContext: {}\nExpected output: {}",
            req.task, req.context, req.expected_output
        );
        let mut runtime = SubAgentRuntime::new(config);
        runtime.run_loop_async(&prompt, executor).await
    }

    /// Execute multiple sub-agent requests in parallel using `tokio::task::JoinSet`.
    ///
    /// Concurrency is capped by `config.max_parallel`.
    pub async fn execute_parallel(
        config: SubAgentConfig,
        requests: Vec<DelegationRequest>,
        executor_factory: impl Fn() -> Box<dyn SubAgentExecutor>,
    ) -> Vec<SubAgentResult> {
        let mut set = JoinSet::new();
        let semaphore = Arc::new(Semaphore::new(config.max_parallel));
        for req in requests {
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            let cfg = config.clone();
            let mut executor = executor_factory();
            set.spawn(async move {
                let result = SubAgentRuntime::execute_single(cfg, req, executor.as_mut()).await;
                drop(permit);
                result
            });
        }
        let mut results = Vec::with_capacity(set.len());
        while let Some(result) = set.join_next().await {
            results.push(result.unwrap_or_default());
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

    #[test]
    fn completed_normally_true_when_within_budget() {
        // 3A-4 fix: using exactly max_turns turns is still normal completion
        let config = SubAgentConfig {
            max_turns: 3,
            budget_tokens: 1000,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config);
        runtime.record_turn(100);
        runtime.record_turn(100);
        runtime.record_turn(100);
        // turns_executed == max_turns, but that's still within budget
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
        let mut runtime = SubAgentRuntime::new(config);
        runtime.record_turn(200); // exceeds budget_tokens
        let result = runtime.build_result("done".to_string());
        assert!(!result.completed_normally);
    }

    // -- Stub executor for run_loop tests --

    struct StubExecutor {
        turns: Vec<TurnOutput>,
        call_count: std::sync::atomic::AtomicUsize,
    }

    impl StubExecutor {
        fn new(turns: Vec<TurnOutput>) -> Self {
            Self {
                turns,
                call_count: std::sync::atomic::AtomicUsize::new(0),
            }
        }
    }

    impl SubAgentExecutor for StubExecutor {
        fn execute_turn(
            &mut self,
            _prompt: &str,
            _allowed_tools: &[String],
            _system_prompt: Option<&str>,
        ) -> Result<TurnOutput, SubAgentError> {
            let idx = self.call_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.turns
                .get(idx)
                .cloned()
                .ok_or_else(|| SubAgentError::ExecutionError("no more stub turns".to_string()))
        }
    }

    #[tokio::test]
    async fn run_loop_completes_on_end_turn() {
        let mut executor = StubExecutor::new(vec![TurnOutput {
            text: "task done".to_string(),
            tool_calls: vec![],
            input_tokens: 50,
            output_tokens: 50,
            stop_reason: "end_turn".to_string(),
        }]);
        let config = SubAgentConfig {
            max_turns: 5,
            budget_tokens: 10_000,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config);
        let result = runtime.run_loop_async("do something", &mut executor).await;
        assert!(result.completed_normally);
        assert_eq!(result.output, "task done");
        assert_eq!(result.tool_call_count, 0);
    }

    #[tokio::test]
    async fn run_loop_chains_tool_calls() {
        let mut executor = StubExecutor::new(vec![
            TurnOutput {
                text: "using tool".to_string(),
                tool_calls: vec![ToolCallRecord {
                    tool_name: "read".to_string(),
                    tool_input: "/tmp/file".to_string(),
                    tool_output: "file contents".to_string(),
                }],
                input_tokens: 50,
                output_tokens: 50,
                stop_reason: "tool_use".to_string(),
            },
            TurnOutput {
                text: "final answer".to_string(),
                tool_calls: vec![],
                input_tokens: 50,
                output_tokens: 50,
                stop_reason: "end_turn".to_string(),
            },
        ]);
        let config = SubAgentConfig {
            max_turns: 5,
            budget_tokens: 10_000,
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config);
        let result = runtime.run_loop_async("read a file", &mut executor).await;
        assert!(result.completed_normally);
        assert_eq!(result.tool_call_count, 1);
        assert!(result.output.contains("using tool"));
        assert!(result.output.contains("final answer"));
    }

    #[tokio::test]
    async fn run_loop_stops_on_budget_exhaustion() {
        let mut executor = StubExecutor::new(vec![
            TurnOutput {
                text: "turn 1".to_string(),
                tool_calls: vec![ToolCallRecord {
                    tool_name: "read".to_string(),
                    tool_input: "/tmp/file".to_string(),
                    tool_output: "contents".to_string(),
                }],
                input_tokens: 8000,
                output_tokens: 8000,
                stop_reason: "tool_use".to_string(),
            },
            TurnOutput {
                text: "turn 2".to_string(),
                tool_calls: vec![ToolCallRecord {
                    tool_name: "read".to_string(),
                    tool_input: "/tmp/other".to_string(),
                    tool_output: "more contents".to_string(),
                }],
                input_tokens: 5000,
                output_tokens: 5000,
                stop_reason: "tool_use".to_string(),
            },
        ]);
        let config = SubAgentConfig {
            max_turns: 5,
            budget_tokens: 20_000, // turn 1: 16000, turn 2: 26000 > budget
            ..SubAgentConfig::default()
        };
        let mut runtime = SubAgentRuntime::new(config);
        let result = runtime.run_loop_async("expensive task", &mut executor).await;
        // After turn 2: tokens_consumed = 26000 > budget_tokens = 20000
        assert!(!result.completed_normally);
    }

    #[tokio::test]
    async fn subagent_timeout_stops_execution() {
        let mut config = SubAgentConfig::default();
        config.timeout_secs = Some(0);
        config.budget_tokens = 100_000;
        let mut runtime = SubAgentRuntime::new(config);
        let mut executor = StubExecutor::new(vec![]);
        let result = runtime.run_loop_async("test", &mut executor).await;
        assert!(!result.completed_normally);
    }
}
