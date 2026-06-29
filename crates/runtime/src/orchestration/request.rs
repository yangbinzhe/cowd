use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct RuntimeOrchestrationConstraints {
    #[serde(default)]
    pub max_parallel_agents: Option<usize>,
    #[serde(default)]
    pub risk: Option<String>,
    #[serde(default)]
    pub requires_write: Option<bool>,
    #[serde(default)]
    pub surface_latency_sensitive: Option<bool>,
}
