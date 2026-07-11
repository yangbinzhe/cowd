use serde::{Deserialize, Serialize};

use crate::execution_core::ProtocolRef;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationRequest {
    pub intent: String,
    #[serde(default)]
    pub session_id: Option<String>,
    #[serde(default)]
    pub action: RuntimeOrchestrationAction,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub template_hint: Option<String>,
    /// Optional explicit protocol contract. When omitted, runtime selects a
    /// compatible protocol from the requested action and template semantics.
    #[serde(default)]
    pub protocol: Option<ProtocolRef>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub evidence_refs: Vec<String>,
    #[serde(default)]
    pub constraints: RuntimeOrchestrationConstraints,
    #[serde(default)]
    pub surface: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeOrchestrationAction {
    PlanOnly,
    RequestTeam,
    RequestSubagent,
    RequestVerification,
    RequestParallelTools,
    RequestRewooEvidence,
    RequestDeliberation,
    RequestReflexionRetry,
    RequestBackgroundReview,
    RequestRiskGate,
    RequestSessionLink,
}

impl Default for RuntimeOrchestrationAction {
    fn default() -> Self {
        Self::PlanOnly
    }
}

impl RuntimeOrchestrationAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlanOnly => "plan_only",
            Self::RequestTeam => "request_team",
            Self::RequestSubagent => "request_subagent",
            Self::RequestVerification => "request_verification",
            Self::RequestParallelTools => "request_parallel_tools",
            Self::RequestRewooEvidence => "request_rewoo_evidence",
            Self::RequestDeliberation => "request_deliberation",
            Self::RequestReflexionRetry => "request_reflexion_retry",
            Self::RequestBackgroundReview => "request_background_review",
            Self::RequestRiskGate => "request_risk_gate",
            Self::RequestSessionLink => "request_session_link",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuntimeOrchestrationConstraints {
    #[serde(default)]
    pub max_parallel_agents: Option<usize>,
    #[serde(default)]
    pub risk: Option<String>,
    #[serde(default)]
    pub approval_id: Option<String>,
    #[serde(default)]
    pub requires_write: Option<bool>,
    #[serde(default)]
    pub surface_latency_sensitive: Option<bool>,
}
