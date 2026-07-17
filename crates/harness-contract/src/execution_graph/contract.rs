use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::context::{ContextBudgetLeaseRef, EvidenceAccessRef};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionNodeKind {
    InlineModel,
    ToolBatch,
    AgentTask,
    Verify,
    Synthesize,
    Approval,
    SessionDispatch,
    Timer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionNodeStatus {
    Planned,
    Ready,
    Running,
    WaitingInput,
    WaitingApproval,
    WaitingExternal,
    Paused,
    Completed,
    Blocked,
    Failed,
    Cancelled,
}

impl ExecutionNodeStatus {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Blocked | Self::Failed | Self::Cancelled
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionEdgeKind {
    DependsOn,
    Verifies,
    Produces,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEdge {
    pub from: String,
    pub to: String,
    pub kind: ExecutionEdgeKind,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionAcceptance {
    pub criteria: Vec<String>,
    pub required_evidence: Vec<String>,
    pub minimum_score_basis_points: Option<u16>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRetryPolicy {
    pub max_attempts: u32,
    pub retryable_failure_kinds: Vec<String>,
    pub base_backoff_ms: u64,
    pub maximum_backoff_ms: u64,
}

impl Default for ExecutionRetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 1,
            retryable_failure_kinds: Vec::new(),
            base_backoff_ms: 500,
            maximum_backoff_ms: 30_000,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNodeSpec {
    pub id: String,
    pub kind: ExecutionNodeKind,
    pub payload_ref: String,
    pub executor_kind: String,
    pub idempotency_key: String,
    pub lease_ref: Option<ContextBudgetLeaseRef>,
    pub acceptance: ExecutionAcceptance,
    pub retry_policy: ExecutionRetryPolicy,
    pub resource_scopes: Vec<String>,
}

impl ExecutionNodeSpec {
    #[must_use]
    pub fn new(
        kind: ExecutionNodeKind,
        executor_kind: impl Into<String>,
        payload_ref: impl Into<String>,
    ) -> Self {
        let id = format!("execution-node-{}", uuid::Uuid::new_v4());
        Self {
            idempotency_key: id.clone(),
            id,
            kind,
            payload_ref: payload_ref.into(),
            executor_kind: executor_kind.into(),
            lease_ref: None,
            acceptance: ExecutionAcceptance::default(),
            retry_policy: ExecutionRetryPolicy::default(),
            resource_scopes: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionFailure {
    pub kind: String,
    pub message: String,
    pub retryable: bool,
    pub evidence_refs: Vec<EvidenceAccessRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionUsage {
    /// The provider model that actually produced this node result. This is
    /// distinct from a requested model because Runtime may use a configured
    /// fallback before any provider output is emitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
    pub duration_ms: u64,
    pub tool_calls: u64,
    #[serde(default)]
    pub duplicate_tool_calls: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_write_attempt_paths: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_observed_resource_scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNodeResult {
    pub status: ExecutionNodeStatus,
    pub result_ref: Option<String>,
    /// Bounded semantic outcome for downstream collaborators. Raw model traces
    /// and complete tool payloads remain in evidence storage and are referenced
    /// through `evidence_refs`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub summary: Option<String>,
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub failure: Option<ExecutionFailure>,
    pub usage: ExecutionUsage,
    pub finished_at_ms: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionRecoveryCursor {
    pub commit_cursor: u64,
    pub node_attempts: BTreeMap<String, u32>,
}

/// Durable lineage from a nested execution back to the graph node that
/// requested it. This is runtime-owned metadata: model tool JSON must never
/// be trusted to populate it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionParentBinding {
    pub execution_id: String,
    pub node_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraph {
    pub id: String,
    pub revision: u64,
    pub objective: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    pub nodes: Vec<ExecutionNodeSpec>,
    pub edges: Vec<ExecutionEdge>,
    pub node_statuses: BTreeMap<String, ExecutionNodeStatus>,
    pub node_results: BTreeMap<String, ExecutionNodeResult>,
    pub recovery_cursor: ExecutionRecoveryCursor,
}

impl ExecutionGraph {
    #[must_use]
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            id: format!("execution-graph-{}", uuid::Uuid::new_v4()),
            revision: 0,
            objective: objective.into(),
            parent_execution: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            node_statuses: BTreeMap::new(),
            node_results: BTreeMap::new(),
            recovery_cursor: ExecutionRecoveryCursor::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case")]
pub enum ExecutionGraphCommand {
    Start {
        expected_revision: u64,
    },
    Advance {
        expected_revision: u64,
    },
    Pause {
        expected_revision: u64,
        reason: String,
    },
    Resume {
        expected_revision: u64,
    },
    Cancel {
        expected_revision: u64,
        reason: String,
    },
    SubmitApproval {
        expected_revision: u64,
        node_id: String,
        approved: bool,
        decision_ref: String,
    },
    /// Resolve a node that is waiting on a durable external result. The
    /// command keeps the transition revision-checked and auditable.
    ResolveExternal {
        expected_revision: u64,
        node_id: String,
        result_ref: String,
        correlation_id: String,
    },
    Replan {
        expected_revision: u64,
        reason: String,
        replacement_payload_ref: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraphQualityReport {
    pub node_count: usize,
    pub edge_count: usize,
    pub ready_count: usize,
    pub blocked_count: usize,
    pub failed_count: usize,
    pub has_verify_node: bool,
    pub has_synthesize_node: bool,
    pub is_dag: bool,
    pub warnings: Vec<String>,
}
