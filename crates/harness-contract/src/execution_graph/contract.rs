use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::context::{ContextBudgetLeaseRef, EvidenceAccessRef};
use crate::outcome::{DeliveryEnvelope, TerminalPresentation};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionNodeKind {
    InlineModel,
    ToolBatch,
    AgentTask,
    Subgraph,
    Verify,
    Synthesize,
    Approval,
    SessionDispatch,
    Timer,
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
            context_view_ref: None,
            model_profile: None,
            reasoning_effort: None,
            expected_input_tokens: 0,
            expected_output_tokens: 0,
            expected_duration_ms: 0,
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

/// Typed relationship between two Team instances in one collaboration
/// program.  It is an immutable planning fact; graph execution status and
/// delivery remain the source of lifecycle truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationEdgeKind {
    EvidenceFeed,
    ReviewOf,
    Handoff,
    Aggregate,
    Dispute,
}

/// Program lifecycle is coordination truth, deliberately separate from node
/// execution state and from acceptance/effect verdicts.  A program may be
/// waiting for a resource while every already-admitted node remains healthy.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CollaborationProgramLifecycle {
    #[default]
    Planning,
    AwaitingApproval,
    AwaitingResource,
    Admitting,
    Running,
    Reconciling,
    Completed,
    Partial,
    Blocked,
    Failed,
    Cancelled,
}

impl CollaborationProgramLifecycle {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Partial | Self::Blocked | Self::Failed | Self::Cancelled
        )
    }
}

/// Durable disposition of one required Team instance.  This records an
/// obligation, not a scheduler lease: the graph Supervisor remains the only
/// executor and permit owner.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum TeamAdmissionState {
    #[default]
    Pending,
    AwaitingApproval,
    AwaitingResource,
    Admitting,
    Admitted,
    BlockedPolicy,
    Cancelled,
}

impl TeamAdmissionState {
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::BlockedPolicy | Self::Cancelled)
    }
}

/// One exact Team admission obligation compiled from an accepted collaboration
/// intent.  `binding_ref` is a frozen TeamBinding digest/ref; labels and role
/// display names never participate in this authority identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct TeamAdmissionObligation {
    pub instance_id: String,
    pub binding_ref: String,
    #[serde(default)]
    pub state: TeamAdmissionState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub child_graph_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason_kind: Option<String>,
    pub revision: u64,
}

/// State of a cross-Team delivery edge.  It is append-only receipt driven:
/// neither a model summary nor a consumer display state can mark an edge
/// delivered.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum CrossTeamEdgeState {
    #[default]
    Pending,
    AwaitingProducer,
    Delivered,
    Claimed,
    Blocked,
    Cancelled,
}

/// Bounded input contract for a cross-Team edge.  Raw prompts, arbitrary tool
/// inputs and private model reasoning cannot cross this boundary.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CrossTeamInputContract {
    #[serde(default)]
    pub required_artifact_kinds: Vec<String>,
    #[serde(default)]
    pub required_fact_kinds: Vec<crate::acceptance::TerminalFactKind>,
    #[serde(default)]
    pub require_committed_effect: bool,
    #[serde(default)]
    pub require_satisfied_acceptance: bool,
}

/// Runtime-derived producer receipt for one cross-Team delivery.  The
/// Coordinator stores only stable graph/node/attempt identity, the terminal
/// result locator, and already-authorized evidence references.  It never
/// copies a prompt, tool input, or private model reasoning into another Team.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CrossTeamEdgeDeliveryReceipt {
    pub receipt_ref: String,
    pub producer_node_id: String,
    pub producer_attempt: u32,
    pub producer_result_ref: String,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceAccessRef>,
}

/// Runtime-derived consumer acknowledgement for a delivered cross-Team edge.
/// A claim is fenced by the consumer node attempt so a stale Team retry cannot
/// overwrite the delivery state selected by the current graph revision.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CrossTeamEdgeClaimReceipt {
    pub claim_ref: String,
    pub consumer_node_id: String,
    pub consumer_attempt: u32,
}

/// The narrow AddTeam delta a managed Agent may request at an effect-safe
/// checkpoint.  It deliberately carries no graph identity, executor, lease,
/// permission ceiling or arbitrary template: the parent Program compiler owns
/// all of those facts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CollaborationEscalationAddTeam {
    pub semantic_node_id: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
    #[serde(default)]
    pub output_artifacts: Vec<String>,
    #[serde(default)]
    pub evidence_contract: Vec<String>,
    #[serde(default = "default_required_team_instance")]
    pub required: bool,
    #[serde(default)]
    pub parallelism_hint: u16,
}

/// A bounded escalation proposed by a managed Team at an effect-safe
/// checkpoint.  The coordinator validates its base revision and digest before
/// applying a separate program command.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CollaborationEscalationRequest {
    pub source_attempt: String,
    pub base_revision: u64,
    pub request_kind: String,
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requested_add_team: Option<CollaborationEscalationAddTeam>,
    /// Optional semantic Team-template content for a new turn-bound Team.
    /// Runtime, not the caller, compiles it with the parent Program's bound
    /// lineage and permission ceiling into an immutable snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub template_proposal: Option<serde_json::Value>,
}

/// Durable receipt for an escalation that actually expanded its parent
/// Program. Rejected requests are deliberately not represented here: this is
/// execution truth, not an audit log of arbitrary caller input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CollaborationEscalationReceipt {
    pub escalation_id: String,
    pub source_attempt: String,
    pub base_program_revision: u64,
    pub request_kind: String,
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub applied_graph_revision: u64,
}

impl CollaborationEscalationRequest {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("source_attempt", &self.source_attempt),
            ("request_kind", &self.request_kind),
            ("reason", &self.reason),
            ("digest", &self.digest),
        ] {
            if value.trim().is_empty() {
                return Err(format!("collaboration escalation {field} is empty"));
            }
        }
        if self.base_revision == 0 {
            return Err("collaboration escalation base_revision is zero".to_string());
        }
        if self.request_kind != "add_team" {
            return Err("collaboration escalation request_kind is unsupported".to_string());
        }
        let team = self
            .requested_add_team
            .as_ref()
            .ok_or_else(|| "collaboration escalation is missing requested_add_team".to_string())?;
        validate_escalation_add_team(team)?;
        Ok(())
    }

    #[must_use]
    pub fn as_add_team_patch(&self, program_id: String) -> CollaborationIntentPatch {
        let team = self
            .requested_add_team
            .as_ref()
            .expect("validated escalation has requested_add_team");
        CollaborationIntentPatch {
            program_id,
            base_revision: self.base_revision,
            source_attempt: self.source_attempt.clone(),
            reason: self.reason.clone(),
            evidence_refs: self.evidence_refs.clone(),
            canonical_digest: self.digest.clone(),
            user_confirmation_ref: None,
            escalation: Some(CollaborationEscalationReceipt {
                escalation_id: self.digest.clone(),
                source_attempt: self.source_attempt.clone(),
                base_program_revision: self.base_revision,
                request_kind: self.request_kind.clone(),
                reason: self.reason.clone(),
                evidence_refs: self.evidence_refs.clone(),
                // The commit service is the only owner of the succeeding
                // graph revision, so it fills this field atomically.
                applied_graph_revision: 0,
            }),
            operation: CollaborationIntentPatchOperation::AddTeam {
                team: CollaborationPatchTeam {
                    semantic_node_id: team.semantic_node_id.clone(),
                    objective: team.objective.clone(),
                    depends_on: team.depends_on.clone(),
                    behavior_facets: Vec::new(),
                    ephemeral_template: None,
                    resource_scopes: team.resource_scopes.clone(),
                    output_artifacts: team.output_artifacts.clone(),
                    evidence_contract: team.evidence_contract.clone(),
                    required: team.required,
                    parallelism_hint: team.parallelism_hint,
                },
            },
        }
    }
}

fn validate_escalation_add_team(team: &CollaborationEscalationAddTeam) -> Result<(), String> {
    validate_patch_team(&CollaborationPatchTeam {
        semantic_node_id: team.semantic_node_id.clone(),
        objective: team.objective.clone(),
        depends_on: team.depends_on.clone(),
        behavior_facets: Vec::new(),
        ephemeral_template: None,
        resource_scopes: team.resource_scopes.clone(),
        output_artifacts: team.output_artifacts.clone(),
        evidence_contract: team.evidence_contract.clone(),
        required: team.required,
        parallelism_hint: team.parallelism_hint,
    })
}

/// The only model/managed-Agent proposal format allowed to change a live
/// CollaborationProgram.  It identifies a durable Program revision and a
/// source attempt; it is never an arbitrary replacement graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationIntentPatch {
    pub program_id: String,
    pub base_revision: u64,
    pub source_attempt: String,
    pub reason: String,
    #[serde(default)]
    pub evidence_refs: Vec<EvidenceAccessRef>,
    pub canonical_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_confirmation_ref: Option<String>,
    /// Present only when this patch originates from a Runtime-attested Agent
    /// escalation. It cannot be supplied by a generic model patch route.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub escalation: Option<CollaborationEscalationReceipt>,
    pub operation: CollaborationIntentPatchOperation,
}

/// Data-only Team workstream shape. Display labels are intentionally absent:
/// behavior, authority and acceptance are compiled from typed facets and
/// contracts rather than a role or template display name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollaborationPatchTeam {
    pub semantic_node_id: String,
    pub objective: String,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub behavior_facets: Vec<crate::team::RoleBehaviorFacet>,
    /// A complete session/turn-scoped template for a custom Team.  The
    /// snapshot, not the display name or loose facets, is its executable
    /// behavior/authority contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ephemeral_template: Option<EphemeralTeamTemplateSnapshot>,
    #[serde(default)]
    pub resource_scopes: Vec<String>,
    #[serde(default)]
    pub output_artifacts: Vec<String>,
    #[serde(default)]
    pub evidence_contract: Vec<String>,
    #[serde(default = "default_required_team_instance")]
    pub required: bool,
    #[serde(default)]
    pub parallelism_hint: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CollaborationIntentPatchOperation {
    AddTeam {
        team: CollaborationPatchTeam,
    },
    RetireTeam {
        instance_id: String,
    },
    RequestReview {
        review: CollaborationPatchTeam,
        reviewed_instance_ids: Vec<String>,
    },
    ChangeEdge {
        edge_id: String,
        from_instance_id: String,
        to_instance_id: String,
        edge_kind: CollaborationEdgeKind,
        input_contract: CrossTeamInputContract,
    },
    NarrowObjective {
        semantic_node_id: String,
        objective: String,
    },
    SetParallelismHint {
        semantic_node_id: String,
        parallelism_hint: u16,
    },
}

impl CollaborationIntentPatch {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("program_id", &self.program_id),
            ("source_attempt", &self.source_attempt),
            ("reason", &self.reason),
            ("canonical_digest", &self.canonical_digest),
        ] {
            if value.trim().is_empty() {
                return Err(format!("collaboration patch {field} is empty"));
            }
        }
        if self.base_revision == 0 {
            return Err("collaboration patch base_revision is zero".to_string());
        }
        match &self.operation {
            CollaborationIntentPatchOperation::AddTeam { team } => validate_patch_team(team)?,
            CollaborationIntentPatchOperation::RetireTeam { instance_id } => {
                require_patch_value("instance_id", instance_id)?;
            }
            CollaborationIntentPatchOperation::RequestReview {
                review,
                reviewed_instance_ids,
                ..
            } => {
                validate_patch_team(review)?;
                if reviewed_instance_ids.is_empty() {
                    return Err("review patch has no reviewed Team instances".to_string());
                }
                validate_unique_patch_values("reviewed_instance_ids", reviewed_instance_ids)?;
            }
            CollaborationIntentPatchOperation::ChangeEdge {
                edge_id,
                from_instance_id,
                to_instance_id,
                ..
            } => {
                require_patch_value("edge_id", edge_id)?;
                require_patch_value("from_instance_id", from_instance_id)?;
                require_patch_value("to_instance_id", to_instance_id)?;
                if from_instance_id == to_instance_id {
                    return Err("cross-Team patch edge cannot be self-referential".to_string());
                }
            }
            CollaborationIntentPatchOperation::NarrowObjective {
                semantic_node_id,
                objective,
            } => {
                require_patch_value("semantic_node_id", semantic_node_id)?;
                require_patch_value("objective", objective)?;
            }
            CollaborationIntentPatchOperation::SetParallelismHint {
                semantic_node_id,
                parallelism_hint,
            } => {
                require_patch_value("semantic_node_id", semantic_node_id)?;
                if *parallelism_hint == 0 {
                    return Err("parallelism_hint is zero".to_string());
                }
            }
        }
        Ok(())
    }
}

fn validate_patch_team(team: &CollaborationPatchTeam) -> Result<(), String> {
    require_patch_value("team.semantic_node_id", &team.semantic_node_id)?;
    require_patch_value("team.objective", &team.objective)?;
    validate_unique_patch_values("team.depends_on", &team.depends_on)?;
    if team
        .depends_on
        .iter()
        .any(|id| id == &team.semantic_node_id)
    {
        return Err("Team patch cannot depend on itself".to_string());
    }
    if let Some(snapshot) = &team.ephemeral_template {
        snapshot.validate()?;
    } else if !team.behavior_facets.is_empty() {
        return Err(
            "Team patch behavior facets require an ephemeral template snapshot".to_string(),
        );
    }
    Ok(())
}

fn require_patch_value(field: &str, value: &str) -> Result<(), String> {
    (!value.trim().is_empty())
        .then_some(())
        .ok_or_else(|| format!("collaboration patch {field} is empty"))
}

fn validate_unique_patch_values(field: &str, values: &[String]) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for value in values {
        require_patch_value(field, value)?;
        if !seen.insert(value.as_str()) {
            return Err(format!(
                "collaboration patch {field} has duplicate `{value}`"
            ));
        }
    }
    Ok(())
}

/// Session/turn-bound custom Team template.  It is executable without global
/// publication, but cannot outlive its terminal fence or become a catalog
/// selection authority by accident.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EphemeralTeamTemplateSnapshot {
    pub session_id: String,
    pub turn_id: String,
    pub template_digest: String,
    /// The complete immutable revision used for this execution.  It is held
    /// in the Team request/graph payload instead of a mutable catalog slot;
    /// restart therefore never reselects a newer custom template.
    pub revision: crate::team::TeamTemplateRevision,
    /// Normalized TEAM.md content covered by `revision.content_digest`.
    pub team_markdown: String,
    #[serde(default)]
    pub role_ids: Vec<String>,
    pub policy_ref: String,
    pub expires_at_ms: u64,
    pub terminal_fence: String,
}

impl EphemeralTeamTemplateSnapshot {
    pub fn validate(&self) -> Result<(), String> {
        for (field, value) in [
            ("session_id", &self.session_id),
            ("turn_id", &self.turn_id),
            ("template_digest", &self.template_digest),
            ("policy_ref", &self.policy_ref),
            ("terminal_fence", &self.terminal_fence),
        ] {
            if value.trim().is_empty() {
                return Err(format!("ephemeral Team template {field} is empty"));
            }
        }
        if self.expires_at_ms == 0 {
            return Err("ephemeral Team template expires_at_ms is zero".to_string());
        }
        if self.team_markdown.trim().is_empty() || self.team_markdown.contains('\0') {
            return Err("ephemeral Team template markdown is invalid".to_string());
        }
        self.revision
            .validate()
            .map_err(|error| error.to_string())?;
        if self.template_digest != self.revision.content_digest {
            return Err("ephemeral Team template digest does not match revision".to_string());
        }
        let expected_role_ids = self
            .revision
            .manifest
            .roles
            .iter()
            .map(|role| role.role_id.as_str())
            .collect::<BTreeSet<_>>();
        let role_ids = self
            .role_ids
            .iter()
            .map(String::as_str)
            .collect::<BTreeSet<_>>();
        if expected_role_ids.is_empty()
            || role_ids.len() != self.role_ids.len()
            || role_ids != expected_role_ids
        {
            return Err(
                "ephemeral Team template role identities do not match revision".to_string(),
            );
        }
        Ok(())
    }
}

/// Technical (not monetary) resource snapshot owned by the program revision.
/// It is changed only on admission/revision/terminal boundaries; token streaming
/// does not write a database ledger per chunk.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProgramResourceLedger {
    pub context_reservation_tokens: u64,
    pub output_reservation_tokens: u64,
    pub parallel_demand: u16,
    pub deadline_at_ms: u64,
    pub confidence_basis_points: u16,
    pub revision: u64,
}

/// Durable control-plane state.  Existing pre-0821 graph metadata decodes as
/// `Planning`; P1 is responsible for moving any new admitted program through
/// a non-planning state together with exact obligations.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CollaborationProgramControlState {
    #[serde(default)]
    pub lifecycle: CollaborationProgramLifecycle,
    #[serde(default)]
    pub obligations: Vec<TeamAdmissionObligation>,
    #[serde(default)]
    pub resource_ledger: ProgramResourceLedger,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub waiting_relation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blocker_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_action: Option<String>,
}

/// One stable Team obligation compiled into a root execution graph.
///
/// `semantic_node_id` deliberately points to the planner's semantic node,
/// rather than a presentation label or a mutable child graph id.  A Team can
/// therefore be recovered or reprojected without turning its display name
/// into an authority key.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CollaborationTeamInstance {
    pub instance_id: String,
    pub semantic_node_id: String,
    #[serde(default = "default_required_team_instance")]
    pub required: bool,
}

const fn default_required_team_instance() -> bool {
    true
}

/// Cross-Team relation compiled from the semantic proposal.  `from` and `to`
/// are `CollaborationTeamInstance::instance_id` values; execution edges carry
/// the physical graph-node relationship separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CollaborationProgramEdge {
    pub edge_id: String,
    pub from: String,
    pub to: String,
    pub kind: CollaborationEdgeKind,
    #[serde(default)]
    pub input_contract: CrossTeamInputContract,
    #[serde(default)]
    pub state: CrossTeamEdgeState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_receipt: Option<CrossTeamEdgeDeliveryReceipt>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_receipt: Option<CrossTeamEdgeClaimReceipt>,
}

/// Immutable, graph-owned description of the Team obligations for one root
/// execution.  It is not a second scheduler: the canonical `ExecutionGraph`
/// remains responsible for admission, recovery, effects and terminal state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CollaborationProgram {
    pub program_id: String,
    pub revision: u64,
    pub required_team_count: u16,
    #[serde(default)]
    pub team_instances: Vec<CollaborationTeamInstance>,
    #[serde(default)]
    pub edges: Vec<CollaborationProgramEdge>,
    /// Durable semantic-to-physical graph mapping. It lets a later program
    /// patch add a typed handoff to an already admitted Team without parsing
    /// a generated node id or consulting a mutable in-memory scheduler.
    #[serde(default)]
    pub semantic_node_instances: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub control: CollaborationProgramControlState,
}

impl CollaborationProgram {
    pub fn validate(&self) -> Result<(), String> {
        if self.program_id.trim().is_empty() {
            return Err("collaboration program id is empty".to_string());
        }
        if self.required_team_count == 0 {
            return Err("collaboration program requires at least one Team".to_string());
        }
        let required_instances = self
            .team_instances
            .iter()
            .filter(|team| team.required)
            .count();
        if usize::from(self.required_team_count) != required_instances {
            return Err(format!(
                "collaboration program requires {} Team instances but carries {required_instances} required instances",
                self.required_team_count
            ));
        }
        let ids = self
            .team_instances
            .iter()
            .map(|team| team.instance_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if ids.len() != self.team_instances.len()
            || self.team_instances.iter().any(|team| {
                team.instance_id.trim().is_empty() || team.semantic_node_id.trim().is_empty()
            })
        {
            return Err("collaboration program Team identities are invalid".to_string());
        }
        let edge_ids = self
            .edges
            .iter()
            .map(|edge| edge.edge_id.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if edge_ids.len() != self.edges.len()
            || self.edges.iter().any(|edge| {
                edge.edge_id.trim().is_empty()
                    || edge.from == edge.to
                    || !ids.contains(edge.from.as_str())
                    || !ids.contains(edge.to.as_str())
            })
        {
            return Err("collaboration program edges are invalid".to_string());
        }
        for edge in &self.edges {
            let receipt_is_valid = edge.delivery_receipt.as_ref().is_none_or(|receipt| {
                !receipt.receipt_ref.trim().is_empty()
                    && !receipt.producer_node_id.trim().is_empty()
                    && receipt.producer_attempt > 0
                    && !receipt.producer_result_ref.trim().is_empty()
            });
            let claim_is_valid = edge.claim_receipt.as_ref().is_none_or(|claim| {
                !claim.claim_ref.trim().is_empty()
                    && !claim.consumer_node_id.trim().is_empty()
                    && claim.consumer_attempt > 0
            });
            let receipt_state_is_valid = match edge.state {
                CrossTeamEdgeState::Pending | CrossTeamEdgeState::AwaitingProducer => {
                    edge.delivery_receipt.is_none() && edge.claim_receipt.is_none()
                }
                CrossTeamEdgeState::Delivered => {
                    edge.delivery_receipt.is_some() && edge.claim_receipt.is_none()
                }
                CrossTeamEdgeState::Claimed => {
                    edge.delivery_receipt.is_some() && edge.claim_receipt.is_some()
                }
                CrossTeamEdgeState::Blocked | CrossTeamEdgeState::Cancelled => {
                    edge.delivery_receipt.is_none() && edge.claim_receipt.is_none()
                }
            };
            if !receipt_is_valid || !claim_is_valid || !receipt_state_is_valid {
                return Err(format!(
                    "cross-Team edge `{}` receipt state is invalid",
                    edge.edge_id
                ));
            }
        }
        if self.semantic_node_instances.values().any(|instances| {
            instances.is_empty() || instances.iter().any(|instance| instance.trim().is_empty())
        }) {
            return Err("collaboration program physical node mapping is invalid".to_string());
        }
        if !self.semantic_node_instances.is_empty()
            && (self.team_instances.iter().any(|team| {
                !self
                    .semantic_node_instances
                    .contains_key(&team.semantic_node_id)
            }) || self.semantic_node_instances.keys().any(|semantic_id| {
                !self
                    .team_instances
                    .iter()
                    .any(|team| &team.semantic_node_id == semantic_id)
            }))
        {
            return Err(
                "collaboration program physical mapping does not match Team semantics".to_string(),
            );
        }
        if !self.semantic_node_instances.is_empty() {
            let mut mapped_nodes = std::collections::BTreeSet::new();
            for (semantic_id, physical_nodes) in &self.semantic_node_instances {
                let expected_instances = self
                    .team_instances
                    .iter()
                    .filter(|team| &team.semantic_node_id == semantic_id)
                    .count();
                if physical_nodes.len() != expected_instances
                    || !physical_nodes
                        .iter()
                        .all(|node_id| mapped_nodes.insert(node_id.as_str()))
                {
                    return Err(
                        "collaboration program physical mapping is not one-to-one with Team instances"
                            .to_string(),
                    );
                }
            }
        }
        if !matches!(
            self.control.lifecycle,
            CollaborationProgramLifecycle::Planning
        ) {
            if self.control.resource_ledger.revision != self.revision
                || self.control.resource_ledger.parallel_demand == 0
                || self.control.resource_ledger.deadline_at_ms == 0
            {
                return Err("active collaboration program control state is incomplete".to_string());
            }
            if self.control.obligations.len() != self.team_instances.len() {
                return Err(
                    "active collaboration program has incomplete Team obligations".to_string(),
                );
            }
            let obligation_ids = self
                .control
                .obligations
                .iter()
                .map(|obligation| obligation.instance_id.as_str())
                .collect::<std::collections::BTreeSet<_>>();
            if obligation_ids.len() != self.control.obligations.len()
                || obligation_ids != ids
                || self.control.obligations.iter().any(|obligation| {
                    obligation.binding_ref.trim().is_empty() || obligation.revision != self.revision
                })
            {
                return Err("active collaboration program obligations are invalid".to_string());
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionOrchestrationMetadata {
    pub mutation_id: String,
    #[serde(default)]
    pub applied_mutation_ids: Vec<String>,
    /// Applied managed-Agent escalations. Entries are appended in the same
    /// graph transaction as their semantic expansion, so projection/recovery
    /// never infer an escalation from a mutation-id string.
    #[serde(default)]
    pub collaboration_escalations: Vec<CollaborationEscalationReceipt>,
    pub semantic_revision: u64,
    #[serde(default)]
    pub source_generation: u64,
    pub completion: ExecutionCompletionContract,
    /// Present exactly when the graph contains Team obligations. The program
    /// is immutable planning metadata; its lifecycle is derived from this
    /// graph's nodes and delivery envelope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collaboration_program: Option<CollaborationProgram>,
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
    /// A typed cross-Team relation. It has the same scheduler dependency
    /// semantics as `DependsOn`, but keeps its ownership distinct so a live
    /// Program patch can replace only its own handoff without touching an
    /// ordinary graph dependency.
    CrossTeamHandoff,
    Verifies,
    Produces,
}

impl ExecutionEdgeKind {
    #[must_use]
    pub const fn is_dependency(self) -> bool {
        matches!(self, Self::DependsOn | Self::CrossTeamHandoff)
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub orchestration: Option<ExecutionOrchestrationMetadata>,
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
            orchestration: None,
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
    /// Resolve a Runtime-owned child graph join from its durable terminal
    /// state. Unlike a generic external result, this preserves the typed
    /// child status, evidence and aggregate leaf usage.
    ResolveChildExecution {
        expected_revision: u64,
        receipt: Box<ChildExecutionTerminalReceipt>,
    },
    /// Coordinator-owned control-plane update. It carries the complete
    /// revisioned state so a partial in-memory Team admission cannot be
    /// mistaken for a durable Program transition after restart.
    UpdateCollaborationProgramControl {
        expected_revision: u64,
        control: Box<CollaborationProgramControlState>,
    },
    /// Coordinator-owned delivery transition for an authorized cross-Team
    /// handoff. The commit service derives the receipt from the terminal
    /// producer node; callers cannot supply free-form cross-Team context.
    RecordCrossTeamEdgeDelivery {
        expected_revision: u64,
        edge_id: String,
        producer_node_id: String,
        producer_attempt: u32,
    },
    /// Coordinator-owned acknowledgement that the exact consumer node
    /// accepted a previously delivered cross-Team receipt.
    ClaimCrossTeamEdgeDelivery {
        expected_revision: u64,
        edge_id: String,
        consumer_node_id: String,
        consumer_attempt: u32,
    },
    /// Coordinator-owned atomic replacement of one not-yet-started typed
    /// cross-Team relation. The commit service updates the Program edge and
    /// its matching `CrossTeamHandoff` execution edge in one graph revision.
    ApplyCrossTeamEdgePatch {
        expected_revision: u64,
        patch: Box<CollaborationIntentPatch>,
    },
    /// Coordinator-owned atomic retirement of one not-yet-started Team. The
    /// commit service cancels its physical node and removes every matching
    /// Program obligation, handoff and completion requirement together.
    ApplyCollaborationTeamRetirement {
        expected_revision: u64,
        patch: Box<CollaborationIntentPatch>,
    },
    /// Coordinator-owned narrowing of an unstarted semantic Team objective.
    /// Runtime rewrites only the matching durable Team request payloads and
    /// advances the Program revision in the same graph transaction.
    ApplyCollaborationObjectiveNarrowing {
        expected_revision: u64,
        patch: Box<CollaborationIntentPatch>,
    },
    Replan {
        expected_revision: u64,
        reason: String,
        replacement_payload_ref: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChildExecutionTerminalReceipt {
    pub parent_execution_id: String,
    pub parent_node_id: String,
    pub parent_attempt: u32,
    pub child_execution_id: String,
    pub child_revision: u64,
    pub result: ExecutionNodeResult,
    pub correlation_id: String,
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
    fn collaboration_program_requires_exact_team_obligations_and_bound_edges() {
        let valid = CollaborationProgram {
            program_id: "program-1".to_string(),
            revision: 1,
            required_team_count: 2,
            team_instances: vec![
                CollaborationTeamInstance {
                    instance_id: "research:1".to_string(),
                    semantic_node_id: "research".to_string(),
                    required: true,
                },
                CollaborationTeamInstance {
                    instance_id: "review:1".to_string(),
                    semantic_node_id: "review".to_string(),
                    required: true,
                },
            ],
            edges: vec![CollaborationProgramEdge {
                edge_id: "research:1->review:1".to_string(),
                from: "research:1".to_string(),
                to: "review:1".to_string(),
                kind: CollaborationEdgeKind::ReviewOf,
                input_contract: Default::default(),
                state: Default::default(),
                delivery_receipt: None,
                claim_receipt: None,
            }],
            semantic_node_instances: BTreeMap::new(),
            control: Default::default(),
        };
        assert!(valid.validate().is_ok());

        let mut invalid = valid;
        invalid.required_team_count = 3;
        assert!(invalid.validate().is_err());
        invalid.required_team_count = 2;
        invalid.edges[0].to = "unknown:1".to_string();
        assert!(invalid.validate().is_err());
        invalid.edges[0].to = "review:1".to_string();
        invalid
            .semantic_node_instances
            .insert("unrelated".to_string(), vec!["graph-node".to_string()]);
        assert!(invalid.validate().is_err());

        let mut mapped = CollaborationProgram {
            program_id: "program-2".to_string(),
            revision: 1,
            required_team_count: 2,
            team_instances: vec![
                CollaborationTeamInstance {
                    instance_id: "research:1".to_string(),
                    semantic_node_id: "research".to_string(),
                    required: true,
                },
                CollaborationTeamInstance {
                    instance_id: "research:2".to_string(),
                    semantic_node_id: "research".to_string(),
                    required: true,
                },
            ],
            edges: Vec::new(),
            semantic_node_instances: BTreeMap::from([(
                "research".to_string(),
                vec![
                    "graph:research:1".to_string(),
                    "graph:research:2".to_string(),
                ],
            )]),
            control: Default::default(),
        };
        assert!(mapped.validate().is_ok());
        mapped
            .semantic_node_instances
            .get_mut("research")
            .expect("mapping")
            .pop();
        assert!(
            mapped.validate().is_err(),
            "one Program instance cannot be left unmapped"
        );
    }

    #[test]
    fn active_program_control_requires_exact_obligations_and_technical_ledger() {
        let mut program = CollaborationProgram {
            program_id: "program-control".to_string(),
            revision: 7,
            required_team_count: 1,
            team_instances: vec![CollaborationTeamInstance {
                instance_id: "research:1".to_string(),
                semantic_node_id: "research".to_string(),
                required: true,
            }],
            edges: Vec::new(),
            semantic_node_instances: BTreeMap::new(),
            control: CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Running,
                obligations: vec![TeamAdmissionObligation {
                    instance_id: "research:1".to_string(),
                    binding_ref: "team-binding:sha256:abc".to_string(),
                    state: TeamAdmissionState::Admitted,
                    child_graph_ref: Some("execution-graph:child".to_string()),
                    reason_kind: None,
                    revision: 7,
                }],
                resource_ledger: ProgramResourceLedger {
                    context_reservation_tokens: 4_000,
                    output_reservation_tokens: 1_000,
                    parallel_demand: 1,
                    deadline_at_ms: 123,
                    confidence_basis_points: 8_000,
                    revision: 7,
                },
                waiting_relation: None,
                blocker_ref: None,
                next_action: None,
            },
        };
        assert!(program.validate().is_ok());

        program.control.obligations[0].binding_ref.clear();
        assert!(program.validate().is_err());
        program.control.obligations[0].binding_ref = "team-binding:sha256:abc".to_string();
        program.control.resource_ledger.parallel_demand = 0;
        assert!(program.validate().is_err());

        let mut encoded = serde_json::to_value(&program).expect("program serializes");
        let object = encoded.as_object_mut().expect("program object");
        object.remove("control");
        let legacy: CollaborationProgram =
            serde_json::from_value(encoded).expect("legacy program remains readable");
        assert_eq!(
            legacy.control.lifecycle,
            CollaborationProgramLifecycle::Planning,
            "legacy metadata cannot impersonate an admitted program"
        );
        assert!(legacy.validate().is_ok());
    }

    #[test]
    fn collaboration_intent_patch_requires_fenced_identity_and_non_self_team() {
        let patch = CollaborationIntentPatch {
            program_id: "program-p3".to_string(),
            base_revision: 2,
            source_attempt: "agent-attempt-7".to_string(),
            reason: "independent review is required".to_string(),
            evidence_refs: Vec::new(),
            canonical_digest: "a".repeat(64),
            user_confirmation_ref: None,
            escalation: None,
            operation: CollaborationIntentPatchOperation::AddTeam {
                team: CollaborationPatchTeam {
                    semantic_node_id: "independent-review".to_string(),
                    objective: "review the evidence independently".to_string(),
                    depends_on: vec!["research".to_string()],
                    behavior_facets: Vec::new(),
                    ephemeral_template: None,
                    resource_scopes: vec!["network:*".to_string()],
                    output_artifacts: vec!["review".to_string()],
                    evidence_contract: vec!["evidence".to_string()],
                    required: true,
                    parallelism_hint: 1,
                },
            },
        };
        assert!(patch.validate().is_ok());

        let mut invalid = patch.clone();
        invalid.base_revision = 0;
        assert!(invalid.validate().is_err());
        let CollaborationIntentPatchOperation::AddTeam { team } = &mut invalid.operation else {
            unreachable!("test patch is an AddTeam")
        };
        invalid.base_revision = 2;
        team.depends_on = vec![team.semantic_node_id.clone()];
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn collaboration_escalation_requires_fenced_attempt_identity() {
        let escalation = CollaborationEscalationRequest {
            source_attempt: "team-agent:attempt:3".to_string(),
            base_revision: 4,
            request_kind: "add_team".to_string(),
            reason: "independent verification is required".to_string(),
            evidence_refs: Vec::new(),
            digest: "b".repeat(64),
            requested_add_team: Some(CollaborationEscalationAddTeam {
                semantic_node_id: "verification".to_string(),
                objective: "verify the existing evidence independently".to_string(),
                depends_on: vec!["research".to_string()],
                resource_scopes: vec!["network:*".to_string()],
                output_artifacts: vec!["verification".to_string()],
                evidence_contract: vec!["evidence".to_string()],
                required: true,
                parallelism_hint: 1,
            }),
            template_proposal: None,
        };
        assert!(escalation.validate().is_ok());
        let patch = escalation.as_add_team_patch("program-1".to_string());
        assert!(patch.validate().is_ok());
        assert_eq!(patch.source_attempt, escalation.source_attempt);
        assert_eq!(patch.canonical_digest, escalation.digest);
        assert_eq!(
            patch
                .escalation
                .as_ref()
                .map(|receipt| receipt.escalation_id.as_str()),
            Some(escalation.digest.as_str())
        );
        let mut invalid = escalation;
        invalid.base_revision = 0;
        assert!(invalid.validate().is_err());
    }
}
