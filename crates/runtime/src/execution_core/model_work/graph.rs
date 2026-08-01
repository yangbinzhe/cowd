use harness_contract::execution_graph::{
    ExecutionAcceptance, ExecutionDependencyPolicy, ExecutionNodeKind, ExecutionRetryPolicy,
    ExecutionWorkRole,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelWorkNode {
    pub id: String,
    pub role: ExecutionWorkRole,
    pub kind: ExecutionNodeKind,
    pub executor_kind: String,
    pub payload_ref: String,
    pub depends_on: Vec<String>,
    pub required: bool,
    pub dependency: ExecutionDependencyPolicy,
    pub cancellation_group: Option<String>,
    pub required_evidence_refs: Vec<String>,
    pub context_view_ref: Option<String>,
    pub model_profile: Option<String>,
    pub reasoning_effort: Option<String>,
    pub expected_input_tokens: u64,
    pub expected_output_tokens: u64,
    pub expected_duration_ms: u64,
    pub acceptance: ExecutionAcceptance,
    pub retry_policy: ExecutionRetryPolicy,
    pub resource_scopes: Vec<String>,
}

impl ModelWorkNode {
    #[must_use]
    pub fn new(
        id: impl Into<String>,
        role: ExecutionWorkRole,
        kind: ExecutionNodeKind,
        executor_kind: impl Into<String>,
        payload_ref: impl Into<String>,
    ) -> Self {
        Self {
            id: id.into(),
            role,
            kind,
            executor_kind: executor_kind.into(),
            payload_ref: payload_ref.into(),
            depends_on: Vec::new(),
            required: true,
            dependency: ExecutionDependencyPolicy::All,
            cancellation_group: None,
            required_evidence_refs: Vec::new(),
            context_view_ref: None,
            model_profile: None,
            reasoning_effort: None,
            expected_input_tokens: 0,
            expected_output_tokens: 0,
            expected_duration_ms: 0,
            acceptance: ExecutionAcceptance::default(),
            retry_policy: ExecutionRetryPolicy::default(),
            resource_scopes: Vec::new(),
        }
    }

    #[must_use]
    pub fn after(mut self, node_id: impl Into<String>) -> Self {
        self.depends_on.push(node_id.into());
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelWorkPlan {
    pub objective: String,
    pub graph_id: Option<String>,
    pub nodes: Vec<ModelWorkNode>,
}

impl ModelWorkPlan {
    #[must_use]
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            objective: objective.into(),
            graph_id: None,
            nodes: Vec::new(),
        }
    }
}
