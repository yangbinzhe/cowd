use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AgentRole {
    Reasoner,
    Executor,
    Reviewer,
    General,
}

impl AgentRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reasoner => "reasoner",
            Self::Executor => "executor",
            Self::Reviewer => "reviewer",
            Self::General => "general",
        }
    }
}

#[deprecated(note = "Use crate::agent::SubAgentConfig instead")]
#[derive(Debug, Clone)]
pub struct SubAgentConfig {
    pub role: AgentRole,
    pub model: Option<String>,
    pub max_iterations: usize,
    pub allowed_tools: Vec<String>,
}

impl Default for SubAgentConfig {
    fn default() -> Self {
        Self {
            role: AgentRole::General,
            model: None,
            max_iterations: 30,
            allowed_tools: vec!["read".into(), "bash".into(), "grep".into()],
        }
    }
}

#[derive(Debug, Clone)]
pub struct DelegationRequest {
    pub role: AgentRole,
    pub task: String,
    pub context: String,
    pub expected_output: String,
    pub parent_session_id: String,
}

#[deprecated(note = "Use crate::agent::SubAgentResult instead")]
#[derive(Debug, Clone)]
pub struct SubAgentResult {
    pub role: AgentRole,
    pub status: SubAgentStatus,
    pub output: String,
    pub tool_executions: usize,
    pub tokens_used: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubAgentStatus {
    Success,
    PartialSuccess { warnings: Vec<String> },
    Failed { reason: String },
    TimedOut,
}

#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    pub max_parallel_agents: usize,
    pub retry_attempts: usize,
    pub default_timeout_secs: u64,
}

impl Default for OrchestratorConfig {
    fn default() -> Self {
        Self {
            max_parallel_agents: 4,
            retry_attempts: 3,
            default_timeout_secs: 300,
        }
    }
}
