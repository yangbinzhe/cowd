use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::context::{ContextBudgetLeaseRef, EvidenceAccessRef};
use crate::outcome::{DeliveryEnvelope, TerminalPresentation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionNodeKind {
    InlineModel,
    ToolBatch,
    AgentTask,
    Verify,
    Materialize,
    Synthesize,
    Approval,
    SessionDispatch,
    Timer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ExecutionMaterializationContent {
    /// Preserve a non-empty existing target; otherwise write the complete
    /// durable source result summary.
    ExistingOrSourceSummary,
    SourceSummary,
    SourceJsonField {
        field: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionMaterializationRequest {
    pub source_node_id: String,
    pub target_path: String,
    pub artifact_kind: String,
    pub content: ExecutionMaterializationContent,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_sha256: Option<String>,
}

impl ExecutionMaterializationRequest {
    pub fn validate(&self) -> Result<(), String> {
        if self.source_node_id.trim().is_empty()
            || self.target_path.trim().is_empty()
            || self.artifact_kind.trim().is_empty()
        {
            return Err(
                "materialization source, target and artifact kind are required".to_string(),
            );
        }
        let target = std::path::Path::new(&self.target_path);
        if target.is_absolute()
            || target.components().any(|component| {
                matches!(
                    component,
                    std::path::Component::ParentDir
                        | std::path::Component::RootDir
                        | std::path::Component::Prefix(_)
                )
            })
        {
            return Err("materialization target must be a workspace-relative path".to_string());
        }
        if let ExecutionMaterializationContent::SourceJsonField { field } = &self.content {
            if field.trim().is_empty() {
                return Err("materialization JSON field is empty".to_string());
            }
        }
        if self.expected_sha256.as_ref().is_some_and(|digest| {
            digest.strip_prefix("sha256:").is_none_or(|hex| {
                hex.len() != 64 || !hex.bytes().all(|byte| byte.is_ascii_hexdigit())
            })
        }) {
            return Err("materialization expected_sha256 is invalid".to_string());
        }
        Ok(())
    }
}

/// Semantic responsibility inside the canonical execution graph.
///
/// This is planning and projection metadata, not a second node identity or
/// executor registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionWorkRole {
    Plan,
    Tool,
    EvidenceAnalyze,
    CrossCheck,
    Synthesize,
    Verify,
}

/// A typed dependency predicate. It consumes only Runtime-attested terminal
/// facts; it must not degrade into a presentation boolean or an arbitrary
/// evidence count.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum DependencyPredicate {
    EvidenceReady {
        minimum: u16,
        required_fact_kinds: Vec<crate::acceptance::TerminalFactKind>,
        accepted_execution_statuses: Vec<ExecutionNodeStatus>,
        accepted_acceptance_verdicts: Vec<crate::acceptance::AcceptanceVerdict>,
        #[serde(default)]
        require_committed_effect: bool,
    },
}

/// Readiness rule applied to one node's `DependsOn` predecessors.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ExecutionDependencyPolicy {
    #[default]
    All,
    Any {
        #[serde(default)]
        cancel_remaining: bool,
    },
    Quorum {
        minimum: u16,
        #[serde(default)]
        cancel_remaining: bool,
    },
    /// Ready once enough predecessors satisfy the attached typed predicate.
    EvidenceReady {
        predicate: DependencyPredicate,
        #[serde(default)]
        cancel_remaining: bool,
    },
    /// Ready after every predecessor reaches any terminal state.  Unlike
    /// `All`, predecessor success is not required, which guarantees that the
    /// Runtime finally reducer can close partial and failed executions.
    Finally,
}

impl ExecutionDependencyPolicy {
    #[must_use]
    pub const fn cancel_remaining(&self) -> bool {
        match self {
            Self::All => false,
            Self::Any { cancel_remaining }
            | Self::Quorum {
                cancel_remaining, ..
            } => *cancel_remaining,
            Self::EvidenceReady {
                cancel_remaining, ..
            } => *cancel_remaining,
            Self::Finally => false,
        }
    }
}

/// Runtime-owned work metadata. Complete prompts, tool payloads and private
/// evidence stay behind governed Runtime ports.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionWorkContract {
    pub role: ExecutionWorkRole,
    #[serde(default = "default_required_work")]
    pub required: bool,
    #[serde(default)]
    pub dependency: ExecutionDependencyPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancellation_group: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub required_evidence_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub input_artifact_refs: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub output_artifact_kinds: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_view_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_effort: Option<String>,
    #[serde(default)]
    pub expected_input_tokens: u64,
    #[serde(default)]
    pub expected_output_tokens: u64,
    #[serde(default)]
    pub expected_duration_ms: u64,
    /// Soft scheduling preference for an unstarted work item. ResourceManager
    /// still owns all permit, deadline and capacity decisions.
    #[serde(default)]
    pub scheduling_priority: u8,
}

const fn default_required_work() -> bool {
    true
}

impl ExecutionWorkContract {
    #[must_use]
    pub fn new(role: ExecutionWorkRole) -> Self {
        Self {
            role,
            required: true,
            dependency: ExecutionDependencyPolicy::All,
            cancellation_group: None,
            required_evidence_refs: Vec::new(),
            input_artifact_refs: Vec::new(),
            output_artifact_kinds: Vec::new(),
            context_view_ref: None,
            model_profile: None,
            reasoning_effort: None,
            expected_input_tokens: 0,
            expected_output_tokens: 0,
            expected_duration_ms: 0,
            scheduling_priority: 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default, schemars::JsonSchema)]
pub struct ExecutionCompletionContract {
    #[serde(default)]
    pub required_node_ids: Vec<String>,
    #[serde(default)]
    pub required_artifact_kinds: Vec<String>,
    #[serde(default)]
    pub allow_unresolved_conflicts: bool,
}

/// Canonical business lineage attached before an execution graph is admitted.
/// Graph planning may happen before this identity is known, but a graph must
/// carry this scope before Runtime commits any activity or side effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionGraphLineage {
    pub session_id: String,
    pub turn_id: String,
    pub root_task_id: String,
    pub task_id: String,
    pub generation: u64,
}

impl ExecutionGraphLineage {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.session_id.trim().is_empty() {
            return Err("execution graph session_id must not be empty");
        }
        if self.turn_id.trim().is_empty() {
            return Err("execution graph turn_id must not be empty");
        }
        if self.root_task_id.trim().is_empty() {
            return Err("execution graph root_task_id must not be empty");
        }
        if self.task_id.trim().is_empty() {
            return Err("execution graph task_id must not be empty");
        }
        if self.generation == 0 {
            return Err("execution graph generation must be positive");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionEdgeKind {
    DependsOn,
    /// Organizational relation retained only as generic graph metadata.
    /// It is not itself a scheduler barrier. Runtime compilation emits a
    /// companion `ArtifactRequires` edge for every executable Team-to-Team
    /// dependency so the concrete consumer cannot outrun durable delivery.
    CrossTeamHandoff,
    /// Physical readiness edge for a typed artifact/evidence consumer.
    ArtifactRequires,
    Verifies,
    Produces,
}

impl ExecutionEdgeKind {
    #[must_use]
    pub const fn is_dependency(self) -> bool {
        matches!(self, Self::DependsOn | Self::ArtifactRequires)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionEdge {
    pub from: String,
    pub to: String,
    pub kind: ExecutionEdgeKind,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionAcceptance {
    /// Runtime-compiled requirement truth. The legacy fields below remain
    /// deserialization inputs until graph construction has compiled them;
    /// they are never observations.
    #[serde(default)]
    pub required: crate::context::RequiredAcceptance,
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work: Option<ExecutionWorkContract>,
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
            work: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionFailure {
    pub kind: String,
    pub message: String,
    pub retryable: bool,
    pub evidence_refs: Vec<EvidenceAccessRef>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionUsage {
    /// The exact requirement contract used for this execution attempt. This
    /// may include deterministic predecessor-derived obligations that were
    /// unavailable when the original graph node was compiled.
    #[serde(default)]
    pub required_acceptance: crate::context::RequiredAcceptance,
    #[serde(default)]
    pub observed_acceptance: crate::context::ObservedAcceptance,
    /// Immutable acceptance evaluation written by the terminal Runtime
    /// producer. Dependency, verification, delivery and projection consumers
    /// read this value; they never re-run a matcher over raw observations.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acceptance_evaluation: Option<crate::acceptance::AcceptanceEvaluation>,
    /// The provider model that actually produced this node result. This is
    /// distinct from a requested model because Runtime may use a configured
    /// fallback before any provider output is emitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    pub input_tokens: u64,
    pub output_tokens: u64,
    /// Provider-reported cache population tokens. These are not cache hits.
    #[serde(default)]
    pub cache_creation_input_tokens: u64,
    /// Provider-reported prompt tokens served from cache.
    #[serde(default)]
    pub cache_read_input_tokens: u64,
    /// Deprecated presentation alias for cache-read tokens only. New code
    /// must use the two explicit dimensions above.
    #[serde(default)]
    pub cached_tokens: u64,
    pub duration_ms: u64,
    pub tool_calls: u64,
    #[serde(default)]
    pub duplicate_tool_calls: u64,
    #[serde(default)]
    pub max_tool_concurrency_observed: u64,
    #[serde(default)]
    pub parallel_tool_batches: u64,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_write_attempt_paths: Vec<String>,
    /// Durable pre-R1 projection only. New node results carry observation
    /// truth in `observed_acceptance` and never populate this field.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub runtime_observed_resource_scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionNodeResult {
    pub status: ExecutionNodeStatus,
    pub result_ref: Option<String>,
    /// Lossless semantic outcome for downstream collaborators. Raw model traces
    /// and complete tool payloads remain in evidence storage and are referenced
    /// through `evidence_refs`; the authored result contract itself is never
    /// character-truncated or replaced with presentation sentinels.
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionParentBinding {
    pub execution_id: String,
    pub node_id: String,
}

/// Runtime-owned service class for one durable execution graph.
///
/// This is persisted with the graph so recovery cannot silently promote
/// background or maintenance work based on a process-local naming heuristic.
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Serialize,
    Deserialize,
    schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionServiceClass {
    #[default]
    Interactive,
    Foreground,
    Background,
    Maintenance,
}

impl ExecutionServiceClass {
    /// A child may inherit or lower its service class, but cannot promote
    /// itself above the parent class supplied by Runtime.
    #[must_use]
    pub const fn bounded_by(self, parent_ceiling: Option<Self>) -> Self {
        match parent_ceiling {
            Some(parent) if self.rank() < parent.rank() => parent,
            _ => self,
        }
    }

    const fn rank(self) -> usize {
        match self {
            Self::Interactive => 0,
            Self::Foreground => 1,
            Self::Background => 2,
            Self::Maintenance => 3,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionGraph {
    pub id: String,
    pub revision: u64,
    pub objective: String,
    #[serde(default)]
    pub service_class: ExecutionServiceClass,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution: Option<ExecutionParentBinding>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage: Option<ExecutionGraphLineage>,
    /// Immutable, authorization-checked source binding for a root that
    /// continues a completed collaboration.  It is graph truth rather than
    /// a prompt reconstruction: retries and recovery retain the exact
    /// source Team set, durable result references and ingress idempotency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuation_binding: Option<crate::turn::CollaborationContinuationBinding>,
    pub nodes: Vec<ExecutionNodeSpec>,
    pub edges: Vec<ExecutionEdge>,
    pub node_statuses: BTreeMap<String, ExecutionNodeStatus>,
    pub node_results: BTreeMap<String, ExecutionNodeResult>,
    pub recovery_cursor: ExecutionRecoveryCursor,
    /// Durable fact packet produced by the Runtime finally reducer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_envelope: Option<DeliveryEnvelope>,
    /// The committed or latest recoverable root presentation attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub terminal_presentation: Option<TerminalPresentation>,
}

impl ExecutionGraph {
    #[must_use]
    pub fn new(objective: impl Into<String>) -> Self {
        Self {
            id: format!("execution-graph-{}", uuid::Uuid::new_v4()),
            revision: 0,
            objective: objective.into(),
            service_class: ExecutionServiceClass::Interactive,
            parent_execution: None,
            lineage: None,
            continuation_binding: None,
            nodes: Vec::new(),
            edges: Vec::new(),
            node_statuses: BTreeMap::new(),
            node_results: BTreeMap::new(),
            recovery_cursor: ExecutionRecoveryCursor::default(),
            delivery_envelope: None,
            terminal_presentation: None,
        }
    }

    #[must_use]
    pub fn with_lineage(mut self, lineage: ExecutionGraphLineage) -> Self {
        self.lineage = Some(lineage);
        self
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
    CancelNode {
        expected_revision: u64,
        node_id: String,
        reason: String,
    },
    SubmitApproval {
        expected_revision: u64,
        node_id: String,
        decision: Box<crate::policy::ApprovalDecisionCommand>,
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

#[cfg(test)]
mod dependency_policy_tests {
    use super::*;

    #[test]
    fn dependency_policy_defaults_to_all_for_legacy_work_contracts() {
        let contract: ExecutionWorkContract = serde_json::from_value(serde_json::json!({
            "role": "synthesize"
        }))
        .expect("legacy work contract remains readable");

        assert_eq!(contract.dependency, ExecutionDependencyPolicy::All);
    }

    #[test]
    fn finally_has_a_stable_json_shape_and_never_cancels_predecessors() {
        let encoded = serde_json::to_value(&ExecutionDependencyPolicy::Finally)
            .expect("finally policy serializes");
        assert_eq!(encoded, serde_json::json!({"mode": "finally"}));
        let decoded: ExecutionDependencyPolicy =
            serde_json::from_value(encoded).expect("finally policy deserializes");
        assert_eq!(decoded, ExecutionDependencyPolicy::Finally);
        assert!(!decoded.cancel_remaining());
    }

    #[test]
    fn dependency_policy_json_schema_contains_finally() {
        let schema = schemars::schema_for!(ExecutionDependencyPolicy);
        let encoded = serde_json::to_string(&schema).expect("schema serializes");
        assert!(encoded.contains("finally"));
    }

    #[test]
    fn legacy_graph_defaults_terminal_delivery_fields() {
        let graph = ExecutionGraph::new("legacy graph");
        let mut encoded = serde_json::to_value(graph).expect("graph serializes");
        let object = encoded.as_object_mut().expect("graph is an object");
        object.remove("delivery_envelope");
        object.remove("terminal_presentation");

        let decoded: ExecutionGraph =
            serde_json::from_value(encoded).expect("legacy graph remains readable");
        assert!(decoded.delivery_envelope.is_none());
        assert!(decoded.terminal_presentation.is_none());
    }
    #[test]
    fn materialization_request_rejects_scope_escape_and_invalid_digest() {
        let mut request = ExecutionMaterializationRequest {
            source_node_id: "source".to_string(),
            target_path: "reports/final.md".to_string(),
            artifact_kind: "report".to_string(),
            content: ExecutionMaterializationContent::SourceSummary,
            expected_sha256: Some(format!("sha256:{}", "a".repeat(64))),
        };
        assert!(request.validate().is_ok());
        request.target_path = "../outside.md".to_string();
        assert!(request.validate().is_err());
        request.target_path = "reports/final.md".to_string();
        request.expected_sha256 = Some("sha256:short".to_string());
        assert!(request.validate().is_err());
    }
}
