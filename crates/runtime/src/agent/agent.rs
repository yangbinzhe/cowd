//! Declarative delegation types retained for collaboration templates.
//!
//! This module intentionally contains no provider loop, process spawning, or
//! lifecycle state. Real agent execution is owned by `AgentRuntime`.

#[path = "binding.rs"]
pub mod binding;
#[path = "definition/mod.rs"]
pub mod definition;

use harness_contract::context::{
    AgentReturnContextEnvelope, ContextArtifact, ContextArtifactKind, ContextRetentionPolicy,
    EvidenceRef,
};
use serde::{Deserialize, Serialize};

use crate::budget_policy::DEFAULT_SUBAGENT_BUDGET_TOKENS;
use crate::context_runtime::{AgentContextLease, AgentReturnRequirement, ContextSourceKind};

pub trait SubAgentProgressCallback: Send + Sync {
    fn on_turn_complete(&self, turn: u32, max_turns: usize, tokens_used: usize);
    fn on_tool_call(&self, tool_name: &str, input_preview: &str);
    fn on_budget_warning(&self, remaining_tokens: usize);
}

/// Template-level adapter used by V5 collaboration templates. It cannot own a
/// provider loop; production implementations submit canonical AgentTask nodes.
pub trait SubAgentExecutor: Send + Sync {
    fn execute(
        &self,
        config: SubAgentConfig,
        task: &str,
    ) -> impl std::future::Future<Output = Result<SubAgentResult, SubAgentError>> + Send;
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Planner,
    Executor,
    Reviewer,
    #[default]
    General,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SubAgentToolMode {
    FullToolSet,
    ReadOnly,
    Custom(Vec<String>),
}

impl Default for SubAgentToolMode {
    fn default() -> Self {
        Self::FullToolSet
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAgentConfig {
    pub task_description: String,
    pub allowed_tools: Vec<String>,
    #[serde(default = "default_write_source")]
    pub write_source: String,
    #[serde(default = "default_max_turns")]
    pub max_turns: usize,
    #[serde(default = "default_budget_tokens")]
    pub budget_tokens: usize,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default = "default_max_parallel")]
    pub max_parallel: usize,
    #[serde(default)]
    pub model: Option<String>,
    #[serde(default)]
    pub tool_mode: SubAgentToolMode,
    #[serde(default = "default_agent_role")]
    pub agent_role: String,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub role: AgentRole,
    #[serde(default = "default_true")]
    pub inject_peer_context: bool,
    #[serde(default = "default_true")]
    pub inject_memory: bool,
    #[serde(default = "default_true")]
    pub retain_reasoning: bool,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub context_lease: Option<AgentContextLease>,
}

impl Default for SubAgentConfig {
    fn default() -> Self {
        Self {
            task_description: String::new(),
            allowed_tools: Vec::new(),
            write_source: default_write_source(),
            max_turns: default_max_turns(),
            budget_tokens: default_budget_tokens(),
            timeout_secs: None,
            max_parallel: default_max_parallel(),
            model: None,
            tool_mode: SubAgentToolMode::default(),
            agent_role: default_agent_role(),
            capabilities: Vec::new(),
            role: AgentRole::default(),
            inject_peer_context: true,
            inject_memory: true,
            retain_reasoning: true,
            session_id: None,
            context_lease: None,
        }
    }
}

impl SubAgentConfig {
    pub fn ensure_context_lease(
        &mut self,
        parent_session_id: impl Into<String>,
        parent_agent_id: impl Into<String>,
    ) -> AgentContextLease {
        let parent_session_id = parent_session_id.into();
        let lease = self
            .context_lease
            .clone()
            .unwrap_or_else(|| AgentContextLease {
                parent_session_id: parent_session_id.clone(),
                parent_agent_id: parent_agent_id.into(),
                child_agent_id: uuid::Uuid::new_v4().to_string(),
                task_contract: self.task_description.clone(),
                allowed_sources: vec![
                    ContextSourceKind::Memory,
                    ContextSourceKind::ToolTrace,
                    ContextSourceKind::AgentPeer,
                    ContextSourceKind::Workspace,
                    ContextSourceKind::Handoff,
                ],
                max_tokens: self.budget_tokens as u64,
                required_return: vec![
                    AgentReturnRequirement::ResultSummary,
                    AgentReturnRequirement::Evidence,
                    AgentReturnRequirement::Decisions,
                    AgentReturnRequirement::Conflicts,
                    AgentReturnRequirement::MemoryCandidates,
                    AgentReturnRequirement::NextActions,
                ],
            });
        self.session_id = Some(parent_session_id);
        self.context_lease = Some(lease.clone());
        lease
    }

    #[must_use]
    pub fn context_lease(&self) -> Option<&AgentContextLease> {
        self.context_lease.as_ref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubAgentResult {
    pub output: String,
    pub tool_call_count: usize,
    pub tokens_used: usize,
    pub completed_normally: bool,
    pub memory_write_attempts: usize,
    pub memory_writes_denied: usize,
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

impl SubAgentResult {
    #[must_use]
    pub fn to_context_item(
        &self,
        parent_session_id: impl Into<String>,
        child_agent_id: impl Into<String>,
    ) -> crate::context_runtime::ContextItem {
        let child_agent_id = child_agent_id.into();
        let packet = crate::context_runtime::AgentReturnContextProjection {
            parent_session_id: parent_session_id.into(),
            child_agent_id: child_agent_id.clone(),
            result_summary: preview_text(&self.output, 500),
            evidence: vec![format!(
                "tools={} tokens={} completed={}",
                self.tool_call_count, self.tokens_used, self.completed_normally
            )],
            decisions: prefixed_lines(&self.output, &["decision:", "decided:", "conclusion:"]),
            conflicts: prefixed_lines(&self.output, &["conflict:", "risk:", "blocked:"]),
            memory_candidates: prefixed_lines(&self.output, &["memory:", "remember:"]),
            next_actions: prefixed_lines(&self.output, &["next:", "todo:", "action:"]),
            failed: !self.completed_normally,
        };
        crate::context_runtime::ContextRuntimeKernel::agent_return_item(&packet)
    }

    #[must_use]
    pub fn to_agent_return_context_envelope(
        &self,
        parent_session_id: impl Into<String>,
        child_agent_id: impl Into<String>,
    ) -> AgentReturnContextEnvelope {
        let child_agent_id = child_agent_id.into();
        let mut packet = AgentReturnContextEnvelope::new(
            parent_session_id.into(),
            child_agent_id.clone(),
            preview_text(&self.output, 700),
        )
        .with_evidence_ref(EvidenceRef::new(
            "agent",
            format!("agent://{child_agent_id}"),
        ))
        .with_artifact(
            ContextArtifact::new(
                ContextArtifactKind::AgentSummary,
                ContextRetentionPolicy::RetainForSession,
                format!(
                    "delegation_result tools={} tokens={}",
                    self.tool_call_count, self.tokens_used
                ),
            )
            .with_evidence_ref(EvidenceRef::new(
                "agent",
                format!("agent://{child_agent_id}"),
            )),
        );
        packet.failed = !self.completed_normally;
        packet
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SubAgentError {
    #[error("sub-agent execution error: {0}")]
    ExecutionError(String),
    #[error("sub-agent exceeded token budget: {0}")]
    BudgetExceeded(usize),
    #[error("sub-agent exceeded max turns: {0}")]
    MaxTurnsExceeded(usize),
    #[error("sub-agent timed out after {0}s")]
    Timeout(u64),
}

#[derive(Debug, Clone)]
pub struct DelegationRequest {
    pub task: String,
    pub context: String,
    pub expected_output: String,
    pub parent_session_id: String,
}

fn default_write_source() -> String {
    "SubAgent".into()
}
fn default_agent_role() -> String {
    "SubAgent".into()
}
const fn default_max_turns() -> usize {
    10
}
fn default_budget_tokens() -> usize {
    DEFAULT_SUBAGENT_BUDGET_TOKENS
}
const fn default_max_parallel() -> usize {
    4
}
const fn default_true() -> bool {
    true
}

fn preview_text(text: &str, max_chars: usize) -> String {
    let normalized = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if normalized.chars().count() <= max_chars {
        normalized
    } else {
        format!(
            "{}...",
            normalized.chars().take(max_chars).collect::<String>()
        )
    }
}

fn prefixed_lines(text: &str, prefixes: &[&str]) -> Vec<String> {
    text.lines()
        .filter_map(|line| {
            let trimmed = line.trim();
            let lower = trimmed.to_ascii_lowercase();
            prefixes.iter().find_map(|prefix| {
                lower
                    .strip_prefix(prefix)
                    .map(|_| trimmed[prefix.len()..].trim().to_string())
            })
        })
        .filter(|line| !line.is_empty())
        .collect()
}
