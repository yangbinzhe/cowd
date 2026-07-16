use serde::{Deserialize, Serialize};

use harness_contract::team::FocusPartitionPlan;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeOrchestrationRequest {
    pub intent: String,
    /// Runtime-owned execution binding. Gateway adapters inject the active
    /// session model before compilation; model-generated tool input must never
    /// select an arbitrary provider lease for a spawned agent graph.
    #[serde(default)]
    pub model_lease: Option<String>,
    #[serde(default)]
    pub session_id: Option<String>,
    /// Required for `dispatch_session`; `session_id` remains the source.
    #[serde(default)]
    pub target_session_id: Option<String>,
    #[serde(default)]
    pub action: RuntimeOrchestrationAction,
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub template_hint: Option<String>,
    /// Optional explicit focus plan for the selected Team template.
    /// This is an API/human authoring contract. Provider-originated tool calls
    /// are intentionally stripped at Gateway: Runtime owns model-selected
    /// template topology and resolves its role slots itself.
    #[serde(default)]
    pub focus_partition_plans: Vec<FocusPartitionPlan>,
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
#[derive(Default)]
pub enum RuntimeOrchestrationAction {
    #[default]
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
    DispatchSession,
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
            Self::DispatchSession => "dispatch_session",
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
