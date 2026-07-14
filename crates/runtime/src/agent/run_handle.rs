use harness_contract::agent::AgentStatus;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentBackendKind {
    InProcess,
    ProcessJsonl,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentBackendCapabilities {
    pub backend: AgentBackendKind,
    pub supports_input: bool,
    pub supports_interrupt: bool,
    pub supports_pause: bool,
    pub supports_resume: bool,
    pub supports_cancel: bool,
    pub supports_shutdown: bool,
}

impl AgentBackendCapabilities {
    #[must_use]
    pub const fn in_process() -> Self {
        Self {
            backend: AgentBackendKind::InProcess,
            // Supplemental input enters the child SessionInputStream and is
            // consumed at the same checkpoints as a primary turn. Pause and
            // resume remain unavailable until a persisted checkpoint can be
            // restored after a process restart.
            supports_input: true,
            supports_interrupt: true,
            supports_pause: false,
            supports_resume: false,
            supports_cancel: true,
            supports_shutdown: true,
        }
    }

    #[must_use]
    pub const fn process_jsonl() -> Self {
        Self {
            backend: AgentBackendKind::ProcessJsonl,
            supports_input: true,
            supports_interrupt: true,
            supports_pause: false,
            supports_resume: false,
            supports_cancel: true,
            supports_shutdown: true,
        }
    }
}

/// A stable reference to a backend run. It is data-only so it can be returned
/// to Gateway and reconstructed after restart; lifecycle truth stays in the
/// Agent event stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentRunHandle {
    pub run_id: String,
    pub agent_id: String,
    pub backend: AgentBackendKind,
    pub revision: u64,
    pub status: AgentStatus,
}
