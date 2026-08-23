//! Runtime-owned compilation of a Team template into one immutable graph.
//!
//! This module is deliberately the only point where a Team request becomes
//! AgentTask packets. Gateway, model orchestration, eval, and Surfaces submit
//! the same declarative [`TeamInstantiationRequest`]; none of them select
//! executor nodes or mutable Agent identities.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::execution_core::graph::executors::{
    AgentTaskExecutor, SynthesizeNodeExecutor, VerifyNodeExecutor,
};
use crate::{
    resolve_agent_capability, AgentBindingCompiler, AgentCapabilityRequest,
    EvolutionCandidateSubject, EvolutionGovernanceService, EvolutionReleaseAssignment,
    RuntimeDefinitionRegistry,
};
use harness_contract::agent::{
    AgentDefinitionRevisionRef, AgentTaskIntent, AgentTaskPacket, RevisionSelector,
};
use harness_contract::context::ChildExecutionBudgetReservation;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
};
use harness_contract::reality::EvidenceRef;
use harness_contract::task::{
    TaskCreateCommand, TaskExecutionPolicy, TaskKind, TaskMissionAssignment, TaskOrigin,
    TaskPhaseSpec, TaskSpec,
};
use harness_contract::team::{
    FocusPartitionPlan, FocusPartitionSlot, RoleBehaviorFacet, RoleCardinalityPolicy,
    RolePartitionPolicy, TeamAcceptanceCheck, TeamAcceptanceRequirement, TeamInstantiationRequest,
    TeamRoleAssignment, TeamRoleBindingOverride, TeamRoleDefinition, TeamRoleIdentity,
    TeamStructuredOutputField, TeamTemplateSelector,
};

/// Hard ceiling for AgentTask nodes in one immutable Team graph.
///
/// Runtime capacity is intentionally absent: ResourceManager owns admission
/// and queues nodes when the currently available capacity is insufficient.
const MAX_TEAM_GRAPH_AGENT_NODES: usize = 32;
pub(crate) const DEFAULT_PARENT_EXECUTION_TOKEN_BUDGET: u64 = 65_536;

pub(crate) fn bounded_parent_execution_budget(
    budget_id: impl Into<String>,
    max_tokens: u64,
    deadline_at_ms: u64,
    max_parallel: usize,
) -> harness_contract::context::ParentExecutionBudget {
    harness_contract::context::ParentExecutionBudget::new(
        budget_id,
        max_tokens,
        deadline_at_ms,
        max_parallel.min(MAX_TEAM_GRAPH_AGENT_NODES).max(1),
        1,
    )
}

fn compile_required_acceptance(
    criteria: &[String],
    contract: &[TeamAcceptanceRequirement],
    resolver: &crate::path_identity::WorkspacePathIdentityResolver,
    allow_whole_workspace_root: bool,
) -> harness_contract::context::RequiredAcceptance {
    let mut scopes = Vec::new();
    for requirement in contract {
        match &requirement.check {
            TeamAcceptanceCheck::ScopedEvidence { scopes: required }
            | TeamAcceptanceCheck::LegacyEvidenceBound { scopes: required } => {
                scopes.extend(required.iter().map(|scope| {
                    if scope == "network:*" || scope.contains(':') {
                        scope.clone()
                    } else {
                        format!("read:{scope}")
                    }
                }));
            }
            TeamAcceptanceCheck::WorkspaceChange {
                scopes: required, ..
            } => scopes.extend(required.iter().map(|scope| {
                if scope.contains(':') {
                    scope.clone()
                } else {
                    format!("write:{scope}")
                }
            })),
            TeamAcceptanceCheck::SourceVerification { scopes: required } => {
                for scope in required {
                    let path = scope.strip_prefix("write:").unwrap_or(scope);
                    scopes.push(format!("write:{path}"));
                    scopes.push(format!("verify_after_write:{path}"));
                }
            }
            TeamAcceptanceCheck::StructuredField { .. }
            | TeamAcceptanceCheck::UpstreamReview
            | TeamAcceptanceCheck::UpstreamEvidence => {}
        }
    }
    scopes.sort();
    scopes.dedup();
    resolver.compile_required_acceptance_with_root_alias(
        criteria,
        &scopes,
        allow_whole_workspace_root,
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFocusPartition {
    pub focus_id: String,
    pub boundary: String,
    pub evidence_responsibility: String,
    pub output_contract: Vec<String>,
    pub output_acceptance: Vec<String>,
    pub shared_baseline: Vec<String>,
    pub capability_cropped_refs: Vec<String>,
    pub scope_hash: String,
    pub overlap_budget_bp: u16,
    pub novelty_target_bp: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRoleSlot {
    pub role_id: String,
    pub slot: usize,
    pub focus_partition: ResolvedFocusPartition,
    pub definition_ref: AgentDefinitionRevisionRef,
    /// Frozen per-slot Agent Binding compiled from the exact published
    /// Definition revision. Team Binding compilation consumes this once;
    /// active runs never re-resolve a default pointer.
    pub agent_binding: Option<harness_contract::agent::AgentBindingSnapshot>,
    /// Frozen Definition display metadata for the Binding snapshot.
    pub agent_name: String,
    pub agent_description: String,
}

/// The durable explanation of one resolved role cardinality.  A graph
/// contains the resolved slots; this record makes the policy decision
/// inspectable without treating a resource queue as a second Team scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleCardinalityResolution {
    pub role_id: String,
    pub requested: RoleCardinalityPolicy,
    pub resolved_count: u16,
    pub static_graph_ceiling: u16,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct TeamInstantiation {
    pub graph: ExecutionGraph,
    pub task_commands: Vec<TaskCreateCommand>,
    pub task_policy_parent_id: String,
    pub task_permission_ceiling: harness_contract::policy::PermissionMode,
    pub template_ref: harness_contract::team::TeamTemplateRevisionRef,
    pub template_digest: String,
    /// Frozen typed Team Binding compiled from the published Template and
    /// per-slot Agent Bindings. `None` is fail-closed for non-Team callers;
    /// Team admission must persist it before any Task link is created.
    pub binding: Option<harness_contract::team::TeamBindingSnapshot>,
    /// Immutable Runtime authorization used for this graph's Template
    /// selection. `None` means a normal Stable/default resolution.
    pub release_assignment: Option<EvolutionReleaseAssignment>,
    pub role_slots: Vec<ResolvedRoleSlot>,
    pub cardinality_resolutions: Vec<RoleCardinalityResolution>,
    /// Deterministic repairs applied while compiling this Team (e.g. role
    /// alias resolution). Every repair is recorded for audit and surfaced to
    /// the caller as a structured receipt (P2/B).
    pub repairs: Vec<String>,
}

#[derive(Debug)]
pub struct TeamInstantiationService {
    registry: Arc<RuntimeDefinitionRegistry>,
    binding_compiler: AgentBindingCompiler,
    evolution_governance: Arc<EvolutionGovernanceService>,
    workspace_id: String,
    path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
}

impl TeamInstantiationService {
    #[must_use]
    pub fn new(
        registry: Arc<RuntimeDefinitionRegistry>,
        evolution_governance: Arc<EvolutionGovernanceService>,
        workspace_id: impl Into<String>,
        path_identity_resolver: Arc<crate::path_identity::WorkspacePathIdentityResolver>,
    ) -> Self {
        Self {
            binding_compiler: AgentBindingCompiler::new(Arc::clone(&registry)),
            registry,
            evolution_governance,
            workspace_id: workspace_id.into(),
            path_identity_resolver,
        }
    }

    pub fn instantiate(
        &self,
        request: TeamInstantiationRequest,
    ) -> Result<TeamInstantiation, String> {
        request.validate().map_err(|error| error.to_string())?;
        let (template, release_assignment) = self.resolve_template(&request)?;
        self.instantiate_resolved(request, template, release_assignment, None)
    }

    /// Compile an evaluation-only Team graph after Runtime has selected the
    /// exact published candidate revision. This path is crate-visible and has
    /// no release assignment, so a Gateway or model cannot use it as a
    /// general bypass around Stable/default Team resolution.
    pub(crate) fn instantiate_evaluation(
        &self,
        request: TeamInstantiationRequest,
        candidate_revision: &harness_contract::team::TeamTemplateRevisionRef,
        allowed_tools: &[String],
    ) -> Result<TeamInstantiation, String> {
        request.validate().map_err(|error| error.to_string())?;
        let template = self
            .registry
            .resolve_team_canary(candidate_revision)
            .map_err(|error| error.to_string())?;
        self.instantiate_resolved(request, template, None, Some(allowed_tools))
    }

    /// Compile the approved baseline through the ordinary resolver, but carry
    /// the same read-only evaluation tool ceiling as its paired candidate.
    /// This preserves a fair workload without granting an evaluation caller
    /// a separate normal-execution path.
    pub(crate) fn instantiate_evaluation_baseline(
        &self,
        request: TeamInstantiationRequest,
        allowed_tools: &[String],
    ) -> Result<TeamInstantiation, String> {
        request.validate().map_err(|error| error.to_string())?;
        let (template, release_assignment) = self.resolve_template(&request)?;
        self.instantiate_resolved(request, template, release_assignment, Some(allowed_tools))
    }

    fn instantiate_resolved(
        &self,
        request: TeamInstantiationRequest,
        template: crate::team_definition::ResolvedTeamTemplate,
        release_assignment: Option<EvolutionReleaseAssignment>,
        evaluation_allowed_tools: Option<&[String]>,
    ) -> Result<TeamInstantiation, String> {
        let manifest = &template.revision.manifest;
        let binding_overrides = role_binding_overrides(&request, &manifest.roles)?;
        let cardinality_overrides = role_cardinality_overrides(&request, &manifest.roles)?;
        let (focus_plans, mut focus_repairs) =
            focus_partition_plans(&request, &manifest.roles, &manifest.role_aliases)?;
        for plan in focus_plans.values() {
            for slot in &plan.slots {
                for reference in &slot.capability_cropped_refs {
                    if !request.resource_scopes.contains(reference) {
                        return Err(format!(
                            "focus `{}` evidence ref `{reference}` is outside the Team resource/capability lease",
                            slot.focus_id
                        ));
                    }
                }
            }
        }
        let mut graph = ExecutionGraph::new(request.objective.clone());
        graph.id = format!("team-graph:{}", request.team_id);
        graph.parent_execution = request.parent_execution.clone();
        graph.lineage = Some(request.lineage.clone());
        graph.service_class = if evaluation_allowed_tools.is_some() {
            harness_contract::execution_graph::ExecutionServiceClass::Maintenance
        } else if request.managed_invocation.is_some() {
            harness_contract::execution_graph::ExecutionServiceClass::Background
        } else {
            harness_contract::execution_graph::ExecutionServiceClass::Foreground
        };

        let mut slots_by_role = BTreeMap::<String, Vec<(String, String)>>::new();
        let mut role_slots = Vec::new();
        let mut cardinality_resolutions = Vec::new();
        let mut task_commands = Vec::new();
        let source_turn_id = request.lineage.turn_id.clone();
        // Reserve the parent hard budget once across the complete immutable
        // Team topology. Copying the parent ceiling into every role slot
        // multiplies spend by cardinality and makes the advertised parent
        // budget unenforceable.
        let planned_agent_slots = manifest.roles.iter().try_fold(0usize, |total, role| {
            let (focuses, _) = resolve_focuses(
                role,
                cardinality_overrides.get(&role.role_id),
                focus_plans.get(&role.role_id),
            )?;
            total
                .checked_add(focuses.len())
                .ok_or_else(|| "Team graph Agent node count overflowed".to_string())
        })?;
        ensure_static_graph_ceiling(0, planned_agent_slots)?;
        validate_finite_team_budget_capacity(
            &request.execution_budget.budget_id,
            request.execution_budget.predicted_tokens(),
            planned_agent_slots,
        )?;
        for role in &manifest.roles {
            let override_ = binding_overrides.get(&role.role_id);
            let (definition_ref, grant_ceiling) = resolved_role_binding(role, override_)?;
            let capability = resolve_agent_capability(AgentCapabilityRequest {
                role_id: role.role_id.clone(),
                allowed_capabilities: grant_ceiling
                    .iter()
                    .map(|capability| capability.as_str().to_string())
                    .collect(),
                evidence_duties: role.task_contract.acceptance.clone(),
            });
            let reducer_only_role = role
                .behavior
                .iter()
                .any(|facet| matches!(facet, RoleBehaviorFacet::Reducer { .. }));
            let requires_reacquisition = role.behavior.iter().any(|facet| {
                matches!(
                    facet,
                    RoleBehaviorFacet::ReacquireEvidence { required: true }
                )
            });
            let upstream_only_reducer = reducer_only_role && !requires_reacquisition;
            let role_allowed_tools = if upstream_only_reducer {
                Vec::new()
            } else {
                match evaluation_allowed_tools {
                    Some(evaluation_tools) => evaluation_tools
                        .iter()
                        .filter(|tool| capability.allowed_tools.contains(*tool))
                        .cloned()
                        .collect::<Vec<_>>(),
                    None => capability.allowed_tools.iter().cloned().collect(),
                }
            };
            let (focuses, cardinality_resolution) = resolve_focuses(
                role,
                cardinality_overrides.get(&role.role_id),
                focus_plans.get(&role.role_id),
            )?;
            let role_definition = self
                .registry
                .resolve_agent(
                    &definition_ref.definition_id,
                    RevisionSelector::ExactApprovedRevision {
                        revision: definition_ref.revision,
                    },
                )
                .map_err(|error| {
                    format!(
                        "resolve exact Agent Definition {}@{} for Team role {}: {error}",
                        definition_ref.definition_id.as_str(),
                        definition_ref.revision,
                        role.role_id
                    )
                })?;
            let role_allowed_skills = role_definition
                .revision
                .manifest
                .capability_contract
                .skill_refs
                .clone();
            ensure_static_graph_ceiling(role_slots.len(), focuses.len())?;
            cardinality_resolutions.push(cardinality_resolution);
            for (slot, focus_partition) in focuses.into_iter().enumerate() {
                let mut slot_acceptance = role.task_contract.acceptance.clone();
                slot_acceptance.extend(focus_partition.output_acceptance.iter().cloned());
                slot_acceptance.sort();
                slot_acceptance.dedup();
                let node_id = format!("{}:{}:{}", graph.id, role.role_id, slot + 1);
                let run_id = format!("{}:run:{}:{}", request.team_id, role.role_id, slot + 1);
                let task_id = format!("{}:task:{}:{}", request.team_id, role.role_id, slot + 1);
                let resource_scopes = bounded_slot_resource_scopes(
                    &request.resource_scopes,
                    &focus_partition.capability_cropped_refs,
                );
                let instance_allowed_tools =
                    crop_tools_to_resource_lease(&role_allowed_tools, &resource_scopes);
                let acceptance_contract = team_acceptance_contract(
                    &slot_acceptance,
                    &resource_scopes,
                    !role.task_contract.contract_ref.starts_with("builtin/"),
                    reducer_only_role,
                )?;
                let required_acceptance = compile_required_acceptance(
                    &slot_acceptance,
                    &acceptance_contract,
                    &self.path_identity_resolver,
                    request
                        .permission_ceiling
                        .permits(harness_contract::policy::PermissionMode::DangerFullAccess),
                );
                let objective_context = bounded_objective_context(&request.objective);
                let intent = AgentTaskIntent {
                    selected_agent_id: Some(definition_ref.definition_id.as_str().to_string()),
                    definition_ref: Some(definition_ref.clone()),
                    granted_capabilities: grant_ceiling.clone(),
                    principal_id: "runtime.team".to_string(),
                    source_turn_id: source_turn_id.clone(),
                    run_id,
                    task_id,
                    root_task_id: request.lineage.root_task_id.clone(),
                    parent_task_id: Some(request.lineage.task_id.clone()),
                    session_id: request.lineage.session_id.clone(),
                    mission_id: request.mission_id.clone(),
                    team_id: Some(request.team_id.clone()),
                    team_role_identity: Some(TeamRoleIdentity {
                        role_id: role.role_id.clone(),
                        slot: u32::try_from(slot.saturating_add(1))
                            .map_err(|_| "Team role slot overflows u32".to_string())?,
                        focus_id: focus_partition.focus_id.clone(),
                        focus_boundary: focus_partition.boundary.clone(),
                        evidence_responsibility: focus_partition.evidence_responsibility.clone(),
                        focus_scope_hash: focus_partition.scope_hash.clone(),
                        overlap_budget_bp: focus_partition.overlap_budget_bp,
                        novelty_target_bp: focus_partition.novelty_target_bp,
                        output_acceptance: focus_partition.output_acceptance.clone(),
                    }),
                    graph_id: graph.id.clone(),
                    node_id: node_id.clone(),
                    attempt: 1,
                    expected_graph_revision: 0,
                    objective: format!(
                        "## Parent objective (context only)\n{}\n\nParent-level orchestration directives are owned by Runtime. Do not claim that this Agent created, observed, or completed Teams or peer roles. Report only this bounded role's verified work.\n\n## Team role\nRole: {}\nResponsibility: {}\nFocus: {}\nBoundary: {}\nEvidence responsibility: {}\nShared baseline: {}\nOutput contract: {}\n{}Complete only this bounded focus and state evidence plus unresolved items explicitly.",
                        objective_context,
                        role.role_id,
                        role.responsibility,
                        focus_partition.focus_id,
                        focus_partition.boundary,
                        focus_partition.evidence_responsibility,
                        focus_partition.shared_baseline.join("; "),
                        focus_partition.output_contract.join(", "),
                        if upstream_only_reducer {
                            "Use only the canonical upstream results attached by Runtime. No workspace or network tools are authorized; do not reacquire predecessor evidence. Your success criterion is this Team's bounded Focus only. Peer Teams are outside your visibility and authority: never claim that another Team is missing, failed, incomplete, or needs to be rerun, and never judge whether the parent objective is complete. Return only this Team's positive verified conclusion plus genuine gaps inside this Team's own upstream results.\n"
                        } else {
                            ""
                        },
                    ),
                    required_acceptance,
                    output_acceptance: acceptance_contract,
                    acceptance: slot_acceptance,
                    constraints: vec![
                        format!("team_template:{}@{}", template.revision.revision_ref.template_id.as_str(), template.revision.revision_ref.revision),
                        "nested_orchestration:forbidden".to_string(),
                        "parent_merge:exactly_once".to_string(),
                        "team_working_state:visible".to_string(),
                    ]
                    .into_iter()
                    .chain(
                        upstream_only_reducer
                            .then_some("upstream_evidence_only:no_tool_reacquisition".to_string()),
                    )
                    .chain(request.strategy_binding.iter().flat_map(|binding| {
                        [
                            format!("strategy_decision_id:{}", binding.decision_id),
                            format!("strategy_decision_revision:{}", binding.decision_revision),
                            format!("collaboration_lease:{}", binding.decision_lease),
                            format!("turn_ref:{}", binding.turn_ref),
                        ]
                    }))
                    .collect(),
                    context_refs: {
                        // Deterministic ordering keeps the shared context
                        // prefix byte-stable across every role consuming the
                        // same team evidence, maximizing provider cache reuse.
                        let mut refs = focus_partition
                            .capability_cropped_refs
                            .iter()
                            .cloned()
                            .chain(request.upstream_artifact_refs.iter().cloned())
                            .collect::<Vec<_>>();
                        refs.sort();
                        refs.dedup();
                        refs
                    },
                    // A resource lease authorizes discovery but is not itself
                    // evidence. Durable tool audits populate result evidence.
                    evidence_refs: {
                        let mut refs = request.upstream_evidence_refs.clone();
                        refs.sort_by(|left, right| {
                            left.evidence_ref
                                .ref_type
                                .cmp(&right.evidence_ref.ref_type)
                                .then_with(|| left.evidence_ref.id.cmp(&right.evidence_ref.id))
                        });
                        refs.dedup_by(|left, right| {
                            left.evidence_ref.ref_type == right.evidence_ref.ref_type
                                && left.evidence_ref.id == right.evidence_ref.id
                        });
                        refs
                    },
                    resource_scopes,
                    // Runtime derives normal role tools from the immutable
                    // capability grant. Evaluation may only narrow that set;
                    // it cannot grant a tool absent from the role contract.
                    allowed_tools: instance_allowed_tools,
                    allowed_skills: role_allowed_skills.clone(),
                    permission_ceiling: request.permission_ceiling,
                    model_lease: request.model_lease.clone(),
                    budget_lease: slot_budget_lease(
                        &request,
                        &node_id,
                        role_slots.len(),
                        planned_agent_slots,
                    ),
                    deadline_at_ms: request.deadline_at_ms,
                    managed_invocation: request.managed_invocation.clone(),
                    idempotency_key: format!("team:{}:{}:{}", request.team_id, role.role_id, slot + 1),
                };
                task_commands.push(TaskCreateCommand {
                    task_id: intent.task_id.clone(),
                    mission_id: intent.mission_id.clone(),
                    kind: TaskKind::Delegated,
                    origin: TaskOrigin::Delegated,
                    origin_session_id: intent.session_id.clone(),
                    origin_turn_id: intent.source_turn_id.clone(),
                    root_task_id: intent.root_task_id.clone(),
                    parent_task_id: intent.parent_task_id.clone(),
                    predecessor_task_id: None,
                    mission_assignment: TaskMissionAssignment::Automatic,
                    mission_assigned_by: "runtime.team".to_string(),
                    spec: TaskSpec {
                        objective: intent.objective.clone(),
                        phases: vec![TaskPhaseSpec {
                            name: format!("{}:{}", role.role_id, focus_partition.focus_id),
                            objective: intent.objective.clone(),
                            dependency_refs: Vec::new(),
                            plan: Vec::new(),
                            acceptance: intent.acceptance.clone(),
                            test_commands: Vec::new(),
                        }],
                        execution_policy: TaskExecutionPolicy::default(),
                        application_provenance: None,
                    },
                    evidence_refs: vec![EvidenceRef::observed(
                        "team_request",
                        format!(
                            "team://{}/requests/{}/roles/{}/{}",
                            request.team_id,
                            request.request_id,
                            role.role_id,
                            slot + 1
                        ),
                    )],
                });
                let graph_identity =
                    harness_contract::execution::ExecutionIdentity::for_task_graph(
                        intent.principal_id.clone(),
                        self.workspace_id.clone(),
                        intent.mission_id.clone(),
                        intent.task_id.clone(),
                        intent.session_id.clone(),
                        intent.source_turn_id.clone(),
                        intent.graph_id.clone(),
                    )
                    .map_err(|error| error.to_string())?;
                let team_identity = harness_contract::execution::ExecutionIdentity::for_team_node(
                    &graph_identity,
                    &request.team_id,
                    &graph.id,
                )
                .map_err(|error| error.to_string())?;
                let execution_identity =
                    harness_contract::execution::ExecutionIdentity::for_agent_node(
                        &team_identity,
                        &intent.run_id,
                        &intent.node_id,
                    )
                    .map_err(|error| error.to_string())?;
                let packet = self
                    .binding_compiler
                    .compile_task_intent(intent, None, execution_identity)
                    .map_err(|error| {
                        format!(
                            "compile Team role `{}` slot {} Binding: {error}",
                            role.role_id,
                            slot + 1
                        )
                    })?;
                let agent_binding = packet.binding.clone().ok_or_else(|| {
                    format!(
                        "Team role `{}` slot {} packet has no immutable Agent Binding",
                        role.role_id,
                        slot + 1
                    )
                })?;
                let resolved_agent = self
                    .registry
                    .resolve_agent(
                        &definition_ref.definition_id,
                        RevisionSelector::ExactApprovedRevision {
                            revision: definition_ref.revision,
                        },
                    )
                    .map_err(|error| {
                        format!(
                            "resolve Team role `{}` slot {} Definition display metadata: {error}",
                            role.role_id,
                            slot + 1
                        )
                    })?;
                let mut node = ExecutionNodeSpec::new(
                    ExecutionNodeKind::AgentTask,
                    AgentTaskExecutor::KIND,
                    serde_json::to_string(&packet)
                        .map_err(|error| format!("encode Team role packet: {error}"))?,
                );
                node.id = node_id.clone();
                node.idempotency_key = packet.idempotency_key.clone();
                node.acceptance.criteria = packet.acceptance.clone();
                node.acceptance.required = packet.required_acceptance.clone();
                node.resource_scopes = packet.resource_scopes.clone();
                graph.nodes.push(node);
                slots_by_role
                    .entry(role.role_id.clone())
                    .or_default()
                    .push((node_id, focus_partition.focus_id.clone()));
                role_slots.push(ResolvedRoleSlot {
                    role_id: role.role_id.clone(),
                    slot: slot + 1,
                    focus_partition,
                    definition_ref: definition_ref.clone(),
                    agent_binding: Some(agent_binding),
                    agent_name: resolved_agent.revision.manifest.name.clone(),
                    agent_description: resolved_agent.revision.manifest.description.clone(),
                });
            }
        }

        for dependency in &manifest.dependencies {
            let from = slots_by_role.get(&dependency.from_role_id).ok_or_else(|| {
                format!(
                    "resolved Team graph lacks source role `{}`",
                    dependency.from_role_id
                )
            })?;
            let to = slots_by_role.get(&dependency.to_role_id).ok_or_else(|| {
                format!(
                    "resolved Team graph lacks target role `{}`",
                    dependency.to_role_id
                )
            })?;
            for (from_node, _) in from {
                for (to_node, _) in to {
                    graph.edges.push(ExecutionEdge {
                        from: from_node.clone(),
                        to: to_node.clone(),
                        kind: ExecutionEdgeKind::DependsOn,
                    });
                }
            }
        }

        let mut verify = ExecutionNodeSpec::new(
            ExecutionNodeKind::Verify,
            VerifyNodeExecutor::KIND,
            format!("team:{}:verify", request.team_id),
        );
        verify.id = format!("{}:verify", graph.id);
        verify.work = Some(harness_contract::execution_graph::ExecutionWorkContract {
            dependency: harness_contract::execution_graph::ExecutionDependencyPolicy::Finally,
            ..harness_contract::execution_graph::ExecutionWorkContract::new(
                harness_contract::execution_graph::ExecutionWorkRole::Verify,
            )
        });
        let mut team_acceptance = manifest.result_contract.required_fields.clone();
        team_acceptance.extend(request.acceptance.iter().cloned());
        team_acceptance.sort();
        team_acceptance.dedup();
        verify.acceptance.criteria = team_acceptance.clone();
        let mut synthesize = ExecutionNodeSpec::new(
            ExecutionNodeKind::Synthesize,
            SynthesizeNodeExecutor::KIND,
            format!("team:{}", request.team_id),
        );
        synthesize.id = format!("{}:synthesize", graph.id);
        synthesize.work = Some(harness_contract::execution_graph::ExecutionWorkContract {
            dependency: harness_contract::execution_graph::ExecutionDependencyPolicy::Finally,
            ..harness_contract::execution_graph::ExecutionWorkContract::new(
                harness_contract::execution_graph::ExecutionWorkRole::Synthesize,
            )
        });
        synthesize.acceptance.criteria = team_acceptance;
        for node in graph
            .nodes
            .iter()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        {
            graph.edges.push(ExecutionEdge {
                from: node.id.clone(),
                to: verify.id.clone(),
                kind: ExecutionEdgeKind::DependsOn,
            });
        }
        graph.edges.push(ExecutionEdge {
            from: verify.id.clone(),
            to: synthesize.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes.extend([verify, synthesize]);
        harness_contract::execution_graph::validate_execution_graph(&graph)
            .map_err(|error| error.to_string())?;
        let binding = crate::team_binding::compile_team_binding(
            &request,
            manifest,
            &template.revision.content_digest,
            &template.team_markdown,
            &role_slots,
            request.strategy_binding.as_ref(),
        )?;
        // Phase C: freeze the immutable human-facing display identity into
        // every Team-slot Binding before the graph is persisted. The display
        // is compiled from the same frozen Team role snapshot and never
        // participates in behavior, permission, or acceptance decisions.
        let roles_by_slot = binding
            .roles
            .iter()
            .map(|role| (format!("{}:{}", role.role_id, role.slot), role))
            .collect::<BTreeMap<_, _>>();
        for node in graph
            .nodes
            .iter_mut()
            .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
        {
            let mut packet: AgentTaskPacket = serde_json::from_str(&node.payload_ref)
                .map_err(|error| format!("decode Team role packet for display: {error}"))?;
            let agent_id = packet.agent_id().to_string();
            let Some(agent_binding) = packet.binding.as_mut() else {
                continue;
            };
            let role_slot_id = packet
                .team_role_identity
                .as_ref()
                .map(|identity| format!("{}:{}", identity.role_id, identity.slot))
                .ok_or_else(|| {
                    format!(
                        "Team AgentTask {} has no typed role identity before graph persistence",
                        node.id
                    )
                })?;
            let Some(role) = roles_by_slot.get(&role_slot_id) else {
                return Err(format!(
                    "Team AgentTask {} role identity `{role_slot_id}` is absent from its frozen binding",
                    node.id
                ));
            };
            let identity = packet.team_role_identity.clone().expect("checked above");
            packet.team_role = Some(TeamRoleAssignment {
                team_binding_id: binding.binding_id.clone(),
                team_binding_digest: binding.binding_digest.clone(),
                identity,
                behavior: role.behavior.clone(),
            });
            let role_display_name = request
                .role_display_overrides
                .iter()
                .find(|override_| override_.role_id == role.role_id)
                .map(|override_| override_.display_name.clone())
                .or_else(|| {
                    manifest
                        .display
                        .as_ref()
                        .and_then(|display| {
                            display
                                .role_display_names
                                .iter()
                                .find(|name| name.role_id == role.role_id)
                        })
                        .map(|name| name.display_name.clone())
                })
                .or_else(|| {
                    // Every role definition carries its own human-facing
                    // display name (e.g. 供应链专家 / CTO). The centralized
                    // `display.role_display_names` list is optional and empty
                    // for many authored templates, so the role-level name is
                    // the authoritative fallback. Display only: never used for
                    // behavior, permissions or acceptance.
                    manifest
                        .roles
                        .iter()
                        .find(|role_definition| role_definition.role_id == role.role_id)
                        .and_then(|role_definition| role_definition.display_name.clone())
                });
            agent_binding.display = Some(crate::display_identity::compile_agent_display_identity(
                agent_binding,
                role,
                &agent_id,
                &role.role_id,
                role_display_name.as_deref(),
                &role.agent_name,
                &role.agent_description,
            ));
            agent_binding.binding_digest = crate::agent::binding::recompute_binding_digest(
                agent_binding,
            )
            .map_err(|error| format!("recompute Team role packet display digest: {error}"))?;
            node.payload_ref = serde_json::to_string(&packet)
                .map_err(|error| format!("encode Team role packet with display: {error}"))?;
        }

        // Shared-context occupancy prediction (record/display only). Each role
        // estimates base prompt + evidence + coordination against the meter
        // window; these predictions are surfaced in the receipt repairs and
        // never gate admission.
        focus_repairs.extend(
            manifest
                .roles
                .iter()
                .map(|role| {
                    let base_chars = role
                        .responsibility
                        .chars()
                        .count()
                        .saturating_add(request.objective.chars().count());
                    let evidence_chars = role
                        .task_contract
                        .acceptance
                        .iter()
                        .map(|field| field.len())
                        .sum::<usize>();
                    crate::context_occupancy::estimate_role_occupancy(
                        role.role_id.clone(),
                        base_chars,
                        evidence_chars,
                        0,
                        request.execution_budget.max_tokens,
                    )
                })
                .map(|estimate| {
                    format!(
                        "predicted_occupancy:{}:{}bp",
                        estimate.owner, estimate.utilization_bp
                    )
                }),
        );

        Ok(TeamInstantiation {
            graph,
            task_commands,
            task_policy_parent_id: request.lineage.task_id,
            task_permission_ceiling: request.permission_ceiling,
            template_ref: template.revision.revision_ref,
            template_digest: template.revision.content_digest,
            binding: Some(binding),
            release_assignment,
            role_slots,
            cardinality_resolutions,
            repairs: focus_repairs,
        })
    }

    /// Recheck an approved Canary immediately before graph admission. This
    /// closes the planning-to-start race without cancelling already running
    /// graphs whose template revision was immutable at start time.
    pub fn validate_release(&self, instantiation: &TeamInstantiation) -> Result<(), String> {
        let Some(assignment) = &instantiation.release_assignment else {
            return Ok(());
        };
        let EvolutionCandidateSubject::TeamTemplate { revision_ref } = &assignment.subject else {
            return Err("Team instantiation carries a non-Team release assignment".to_string());
        };
        self.evolution_governance
            .validate_team_canary_binding(
                revision_ref,
                &assignment.assignment_id,
                assignment.generation,
            )
            .map_err(|error| format!("Team Canary release is no longer active: {error}"))
    }

    fn resolve_template(
        &self,
        request: &TeamInstantiationRequest,
    ) -> Result<
        (
            crate::team_definition::ResolvedTeamTemplate,
            Option<EvolutionReleaseAssignment>,
        ),
        String,
    > {
        let (template_id, selector) = match &request.template_selector {
            TeamTemplateSelector::Exact { revision_ref } => (
                revision_ref.template_id.clone(),
                RevisionSelector::ExactApprovedRevision {
                    revision: revision_ref.revision,
                },
            ),
            TeamTemplateSelector::LatestStable { template_id } => {
                (template_id.clone(), RevisionSelector::LatestApprovedStable)
            }
            TeamTemplateSelector::Default { template_id } => {
                (template_id.clone(), RevisionSelector::DefaultPointer)
            }
            TeamTemplateSelector::Ephemeral { snapshot } => {
                return self.resolve_ephemeral_template(request, snapshot);
            }
            TeamTemplateSelector::Automatic => {
                return Err(
                    "automatic Team template selection is retired; the Coordinator must bind an approved catalog revision or an ephemeral snapshot"
                        .to_string(),
                );
            }
        };
        let routing_identity = format!(
            "{}|{}|{}|{}",
            request.lineage.session_id,
            request.team_id,
            request.mission_id,
            request
                .parent_execution
                .as_ref()
                .map(|parent| format!("{}:{}", parent.execution_id, parent.node_id))
                .unwrap_or_else(|| "root".to_string())
        );
        if let Some(assignment) = self
            .evolution_governance
            .select_team_canary_assignment(&template_id, &selector, &routing_identity)
            .map_err(|error| error.to_string())?
        {
            let EvolutionCandidateSubject::TeamTemplate { revision_ref } = &assignment.subject
            else {
                return Err("Team Canary router selected a non-Team candidate".to_string());
            };
            let resolved = self
                .registry
                .resolve_team_canary(revision_ref)
                .map_err(|error| error.to_string())?;
            return Ok((resolved, Some(assignment)));
        }
        self.registry
            .resolve_team(&template_id, selector)
            .map(|resolved| (resolved, None))
            .map_err(|error| error.to_string())
    }

    fn resolve_ephemeral_template(
        &self,
        request: &TeamInstantiationRequest,
        snapshot: &harness_contract::execution_graph::EphemeralTeamTemplateSnapshot,
    ) -> Result<
        (
            crate::team_definition::ResolvedTeamTemplate,
            Option<EvolutionReleaseAssignment>,
        ),
        String,
    > {
        snapshot.validate()?;
        if snapshot.session_id != request.lineage.session_id
            || snapshot.turn_id != request.lineage.turn_id
        {
            return Err("ephemeral_template_scope_mismatch".to_string());
        }
        if snapshot.expires_at_ms <= crate::tool_invocation::now_ms() {
            return Err("ephemeral_template_expired".to_string());
        }
        let expected_fence = format!(
            "task:{}:turn:{}",
            request.lineage.root_task_id, request.lineage.turn_id
        );
        if snapshot.terminal_fence != expected_fence {
            return Err("ephemeral_template_terminal_fence_mismatch".to_string());
        }
        let (revision, normalized_markdown) = crate::team_definition::build_revision(
            snapshot.revision.manifest.clone(),
            &snapshot.team_markdown,
        )
        .map_err(|error| format!("ephemeral_template_invalid:{error}"))?;
        if revision != snapshot.revision || revision.content_digest != snapshot.template_digest {
            return Err("ephemeral_template_revision_digest_mismatch".to_string());
        }
        for role in &revision.manifest.roles {
            let RevisionSelector::ExactApprovedRevision { revision } = role.agent_selector else {
                return Err(format!(
                    "ephemeral_template_role_agent_not_exact:{}",
                    role.role_id
                ));
            };
            self.registry
                .resolve_agent(
                    &role.agent_definition_id,
                    RevisionSelector::ExactApprovedRevision { revision },
                )
                .map_err(|error| {
                    format!(
                        "ephemeral_template_role_agent_unavailable:{}:{error}",
                        role.role_id
                    )
                })?;
        }
        Ok((
            crate::team_definition::ResolvedTeamTemplate {
                revision: snapshot.revision.clone(),
                team_markdown: normalized_markdown,
                selected_by: RevisionSelector::ExactApprovedRevision {
                    revision: snapshot.revision.revision_ref.revision,
                },
            },
            None,
        ))
    }
}

fn ensure_static_graph_ceiling(
    existing_agent_nodes: usize,
    additional_agent_nodes: usize,
) -> Result<(), String> {
    let requested = existing_agent_nodes
        .checked_add(additional_agent_nodes)
        .ok_or_else(|| "Team graph Agent node count overflowed".to_string())?;
    if requested > MAX_TEAM_GRAPH_AGENT_NODES {
        return Err(format!(
            "Team graph requests {requested} Agent nodes, exceeding the static ceiling of {MAX_TEAM_GRAPH_AGENT_NODES}"
        ));
    }
    Ok(())
}

pub(crate) fn team_acceptance_contract(
    criteria: &[String],
    resource_scopes: &[String],
    allow_legacy_custom_contract: bool,
    upstream_synthesis_role: bool,
) -> Result<Vec<TeamAcceptanceRequirement>, String> {
    let bounded = |scope: &String| {
        // Whole-workspace aliases are tool-authorization leases, not verifiable
        // acceptance obligations: `read:.` can never be "fully satisfied" and
        // would block a role that already committed its real write target.
        !matches!(
            scope.trim(),
            "read:." | "read:./" | "write:." | "write:./" | "workspace" | "workspace:."
        )
    };
    let evidence_scopes = resource_scopes
        .iter()
        .filter(|scope| bounded(scope))
        .filter(|scope| {
            scope.starts_with("read:")
                || scope.starts_with("workspace:")
                || scope.as_str() == "network:*"
            // `write:` scopes are deliverable obligations, not evidence
            // scopes. A role that merely shares a node-level write lease
            // must not be forced to write the final artifact just because
            // its acceptance includes an `evidence` criterion. Write
            // obligations are minted only by explicit write criteria
            // (implementation / mitigation / source_verification); a
            // designated writer still receives them through those checks.
        })
        .cloned()
        .collect::<Vec<_>>();
    // A `session:` lease is authorization context, not verifiable evidence:
    // no tool receipt can close a `session:` obligation, so a role that
    // returns a terminal answer would be blocked after Focus acceptance
    // recovery ("returned a second final answer"). Prefer the whole-workspace
    // read alias already present in the lease: it compiles (under a
    // full-trust ceiling) to a workspace-root ScopedContent obligation that
    // any descendant exact read satisfies, so research roles close it with
    // their first read_file receipt. `session:` remains only as a last-resort
    // compile-time bound for legacy session-only fixtures.
    let evidence_scopes = if evidence_scopes.is_empty() {
        let workspace_read = resource_scopes
            .iter()
            .filter(|scope| matches!(scope.trim(), "read:." | "read:./"))
            .cloned()
            .collect::<Vec<_>>();
        if !workspace_read.is_empty() {
            workspace_read
        } else {
            resource_scopes
                .iter()
                .filter(|scope| bounded(scope))
                .filter(|scope| scope.starts_with("session:"))
                .cloned()
                .collect::<Vec<_>>()
        }
    } else {
        evidence_scopes
    };
    // A full-trust `write:.` lease is dynamic rather than an unbounded
    // acceptance claim: the canonical ToolHost receipts determine the exact
    // paths that were actually changed.  Keep it as a root write obligation
    // so the evaluator can derive one terminal verification per committed
    // file, instead of rejecting a legitimate adaptive implementation Team
    // before it can plan.
    let write_scopes = resource_scopes
        .iter()
        .filter(|scope| {
            (scope.starts_with("write:") || scope.starts_with("workspace:"))
                && (bounded(scope)
                    || matches!(scope.trim(), "write:." | "write:./" | "workspace:."))
        })
        .cloned()
        .collect::<Vec<_>>();
    // A downstream reviewer must independently observe the implementation
    // surface, not every unrelated read-only guard in the parent Team lease.
    // `UpstreamReview` below still verifies the exact committed digest; this
    // narrows the generic evidence criterion to the paths an upstream role
    // could have changed.
    let review_evidence_scopes = criteria
        .iter()
        .any(|criterion| criterion == "review")
        .then(|| {
            write_scopes
                .iter()
                .filter_map(|scope| {
                    scope
                        .strip_prefix("write:")
                        .or_else(|| scope.strip_prefix("workspace:"))
                        .map(|path| format!("read:{path}"))
                })
                .collect::<Vec<_>>()
        })
        .filter(|scopes| !scopes.is_empty());
    let structured = |criterion: &str, field| TeamAcceptanceRequirement {
        criterion: criterion.to_string(),
        check: TeamAcceptanceCheck::StructuredField { field },
    };
    criteria
        .iter()
        .map(|criterion| {
            let check = match criterion.as_str() {
                "summary" => structured(criterion, TeamStructuredOutputField::Summary),
                "findings" => structured(criterion, TeamStructuredOutputField::Findings),
                "plan" => structured(criterion, TeamStructuredOutputField::Plan),
                "risks" => structured(criterion, TeamStructuredOutputField::Risks),
                "unresolved" => structured(criterion, TeamStructuredOutputField::Unresolved),
                "key_decisions" => structured(criterion, TeamStructuredOutputField::KeyDecisions),
                "unresolved_or_risks" => {
                    structured(criterion, TeamStructuredOutputField::UnresolvedOrRisks)
                }
                "proposal" => structured(criterion, TeamStructuredOutputField::Proposal),
                "critique" => structured(criterion, TeamStructuredOutputField::Critique),
                "checkpoint" => structured(criterion, TeamStructuredOutputField::Checkpoint),
                "implementation" => TeamAcceptanceRequirement {
                    criterion: criterion.clone(),
                    check: TeamAcceptanceCheck::WorkspaceChange {
                        field: TeamStructuredOutputField::Implementation,
                        scopes: write_scopes.clone(),
                    },
                },
                "mitigation" => TeamAcceptanceRequirement {
                    criterion: criterion.clone(),
                    check: TeamAcceptanceCheck::WorkspaceChange {
                        field: TeamStructuredOutputField::Mitigation,
                        scopes: write_scopes.clone(),
                    },
                },
                "source_verification" => TeamAcceptanceRequirement {
                    criterion: criterion.clone(),
                    check: TeamAcceptanceCheck::SourceVerification {
                        scopes: write_scopes.clone(),
                    },
                },
                "review" => TeamAcceptanceRequirement {
                    criterion: criterion.clone(),
                    check: TeamAcceptanceCheck::UpstreamReview,
                },
                "evidence" => TeamAcceptanceRequirement {
                    criterion: criterion.clone(),
                    check: if upstream_synthesis_role {
                        TeamAcceptanceCheck::UpstreamEvidence
                    } else {
                        TeamAcceptanceCheck::ScopedEvidence {
                            scopes: review_evidence_scopes
                                .clone()
                                .unwrap_or_else(|| evidence_scopes.clone()),
                        }
                    },
                },
                _ => {
                    if let Some(scope) = criterion.strip_prefix("evidence_scope:") {
                        if scope.trim().is_empty() {
                            return Err("Team evidence scope criterion is empty".to_string());
                        }
                        TeamAcceptanceRequirement {
                            criterion: criterion.clone(),
                            check: TeamAcceptanceCheck::ScopedEvidence {
                                scopes: vec![scope.trim().to_string()],
                            },
                        }
                    } else if allow_legacy_custom_contract {
                        TeamAcceptanceRequirement {
                            criterion: criterion.clone(),
                            check: TeamAcceptanceCheck::LegacyEvidenceBound {
                                scopes: evidence_scopes.clone(),
                            },
                        }
                    } else {
                        return Err(format!(
                            "Team acceptance criterion `{criterion}` has no typed Runtime check"
                        ));
                    }
                }
            };
            let missing_scope = match &check.check {
                TeamAcceptanceCheck::ScopedEvidence { scopes }
                | TeamAcceptanceCheck::LegacyEvidenceBound { scopes } => scopes.is_empty(),
                TeamAcceptanceCheck::WorkspaceChange { scopes, .. }
                | TeamAcceptanceCheck::SourceVerification { scopes } => scopes.is_empty(),
                TeamAcceptanceCheck::StructuredField { .. }
                | TeamAcceptanceCheck::UpstreamReview
                | TeamAcceptanceCheck::UpstreamEvidence => false,
            };
            if missing_scope {
                return Err(format!(
                    "Team acceptance criterion `{criterion}` has no bounded Runtime resource scope"
                ));
            }
            Ok(check)
        })
        .collect()
}

fn role_binding_overrides<'a>(
    request: &'a TeamInstantiationRequest,
    roles: &[TeamRoleDefinition],
) -> Result<BTreeMap<String, &'a TeamRoleBindingOverride>, String> {
    let known = roles
        .iter()
        .map(|role| role.role_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut overrides = BTreeMap::new();
    for override_ in &request.role_binding_overrides {
        if !known.contains(override_.role_id.as_str()) {
            return Err(format!(
                "binding override names unknown role `{}`",
                override_.role_id
            ));
        }
        overrides.insert(override_.role_id.clone(), override_);
    }
    Ok(overrides)
}

fn role_cardinality_overrides<'a>(
    request: &'a TeamInstantiationRequest,
    roles: &[TeamRoleDefinition],
) -> Result<BTreeMap<String, &'a RoleCardinalityPolicy>, String> {
    let known = roles
        .iter()
        .map(|role| role.role_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut overrides = BTreeMap::new();
    for override_ in &request.cardinality_overrides {
        if !known.contains(override_.role_id.as_str()) {
            return Err(format!(
                "cardinality override names unknown role `{}`",
                override_.role_id
            ));
        }
        overrides.insert(override_.role_id.clone(), &override_.cardinality);
    }
    Ok(overrides)
}

fn focus_partition_plans<'a>(
    request: &'a TeamInstantiationRequest,
    roles: &[TeamRoleDefinition],
    role_aliases: &BTreeMap<String, String>,
) -> Result<(BTreeMap<String, FocusPartitionPlan>, Vec<String>), String> {
    let known = roles
        .iter()
        .map(|role| role.role_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut plans = BTreeMap::new();
    let mut repairs = Vec::new();
    for plan in &request.focus_partition_plans {
        if !known.contains(plan.role_id.as_str()) {
            if let Some(canonical) = role_aliases
                .get(&plan.role_id)
                .filter(|candidate| known.contains(candidate.as_str()))
            {
                let mut repaired = plan.clone();
                repaired.role_id = canonical.clone();
                repairs.push(format!("role_alias:{}:{}", plan.role_id, canonical));
                plans.insert(canonical.clone(), repaired);
                continue;
            }
            let mut sorted = known.iter().copied().collect::<Vec<_>>();
            sorted.sort();
            return Err(format!(
                "focus partition plan names unknown role `{}`; valid roles for this template: {}; copy exact role ids from runtime_capabilities(detail=team_templates).collaboration_templates[].roles before re-proposing",
                plan.role_id,
                sorted.join(", ")
            ));
        }
        plans.insert(plan.role_id.clone(), plan.clone());
    }
    Ok((plans, repairs))
}

fn resolved_role_binding(
    role: &TeamRoleDefinition,
    override_: Option<&&TeamRoleBindingOverride>,
) -> Result<
    (
        AgentDefinitionRevisionRef,
        Vec<harness_contract::agent::AgentCapability>,
    ),
    String,
> {
    let definition_ref = if let Some(override_) =
        override_.and_then(|override_| override_.definition_ref.as_ref())
    {
        let RevisionSelector::ExactApprovedRevision { revision } = role.agent_selector else {
            return Err(format!(
                "role `{}` does not pin an exact Agent revision",
                role.role_id
            ));
        };
        let expected = AgentDefinitionRevisionRef::new(role.agent_definition_id.clone(), revision)
            .map_err(|error| error.to_string())?;
        if override_ != &expected {
            return Err(format!(
                "role `{}` cannot replace the template-pinned Agent revision",
                role.role_id
            ));
        }
        override_.clone()
    } else {
        let RevisionSelector::ExactApprovedRevision { revision } = role.agent_selector else {
            return Err(format!(
                "role `{}` does not pin an exact Agent revision",
                role.role_id
            ));
        };
        AgentDefinitionRevisionRef::new(role.agent_definition_id.clone(), revision)
            .map_err(|error| error.to_string())?
    };
    let grant_ceiling = override_
        .filter(|override_| !override_.grant_ceiling.is_empty())
        .map(|override_| override_.grant_ceiling.clone())
        .unwrap_or_else(|| role.grant_ceiling.clone());
    if grant_ceiling.is_empty()
        || grant_ceiling
            .iter()
            .any(|capability| !role.grant_ceiling.contains(capability))
    {
        return Err(format!(
            "role `{}` Binding override expands or removes its grant ceiling",
            role.role_id
        ));
    }
    Ok((definition_ref, grant_ceiling))
}

fn resolve_focuses(
    role: &TeamRoleDefinition,
    override_: Option<&&RoleCardinalityPolicy>,
    plan: Option<&FocusPartitionPlan>,
) -> Result<(Vec<ResolvedFocusPartition>, RoleCardinalityResolution), String> {
    let requested = override_.copied().unwrap_or(&role.cardinality).clone();
    if requested.min() < role.cardinality.min() || requested.max() > role.cardinality.max() {
        return Err(format!(
            "role `{}` cardinality override exceeds template ceiling",
            role.role_id
        ));
    }
    let planned = plan.map(|plan| plan.slots.clone()).unwrap_or_default();
    let minimum_slots = requested.min().max(role.cardinality.min());
    let requested_count = match &role.partition {
        RolePartitionPolicy::Single => 1,
        RolePartitionPolicy::Explicit { partitions } => u16::try_from(partitions.len())
            .map_err(|_| format!("role `{}` has too many explicit partitions", role.role_id))?,
        RolePartitionPolicy::ByFocus { .. } if !planned.is_empty() => {
            let planned_count = u16::try_from(planned.len())
                .map_err(|_| format!("role `{}` has too many focus partitions", role.role_id))?;
            planned_count.max(minimum_slots)
        }
        RolePartitionPolicy::ByFocus { .. } => requested.preferred(),
    };
    if !requested.permits(requested_count) || !role.cardinality.permits(requested_count) {
        return Err(format!(
            "role `{}` focus partition count is outside the resolved cardinality",
            role.role_id
        ));
    }
    let resolved_count = requested_count;
    let shared_baseline = plan
        .map(|plan| plan.shared_baseline.clone())
        .unwrap_or_default();
    let mut focuses = match &role.partition {
        RolePartitionPolicy::Single if !planned.is_empty() => {
            vec![planned[0].clone()]
        }
        RolePartitionPolicy::Single => vec![FocusPartitionSlot {
            focus_id: "default".to_string(),
            boundary: role.responsibility.clone(),
            evidence_responsibility: "role-local evidence".to_string(),
            capability_cropped_refs: Vec::new(),
            scope_hash: harness_contract::team::focus_scope_hash(
                &role.role_id,
                &role.responsibility,
                &[],
            ),
            overlap_budget_bp: 0,
            novelty_target_bp: 2_500,
            output_contract: role.task_contract.acceptance.clone(),
            output_acceptance: role.task_contract.acceptance.clone(),
        }],
        RolePartitionPolicy::Explicit { partitions } => partitions
            .iter()
            .map(|partition| FocusPartitionSlot {
                focus_id: partition.clone(),
                boundary: partition.clone(),
                evidence_responsibility: "partition-specific evidence".to_string(),
                capability_cropped_refs: Vec::new(),
                scope_hash: harness_contract::team::focus_scope_hash(&role.role_id, partition, &[]),
                overlap_budget_bp: 0,
                novelty_target_bp: 2_500,
                output_contract: role.task_contract.acceptance.clone(),
                output_acceptance: role.task_contract.acceptance.clone(),
            })
            .collect(),
        RolePartitionPolicy::ByFocus { partition_key } if planned.is_empty() => (1
            ..=resolved_count)
            .map(|slot| FocusPartitionSlot {
                focus_id: format!("{partition_key}:{slot}"),
                boundary: format!("{partition_key}:{slot}"),
                evidence_responsibility: "independent evidence for this focus".to_string(),
                capability_cropped_refs: Vec::new(),
                scope_hash: harness_contract::team::focus_scope_hash(
                    &role.role_id,
                    &format!("{partition_key}:{slot}"),
                    &[],
                ),
                overlap_budget_bp: 0,
                novelty_target_bp: 2_500,
                output_contract: role.task_contract.acceptance.clone(),
                output_acceptance: role.task_contract.acceptance.clone(),
            })
            .collect(),
        RolePartitionPolicy::ByFocus { .. } => {
            let mut expanded = planned;
            // The authority may intentionally provide fewer distinct resource
            // partitions than this published role's cardinality requires.  Do
            // not let it invent unrelated paths merely to satisfy an old
            // runtime minimum.  Instead, the immutable Template contract
            // expands the existing authorized partition deterministically and
            // marks the overlap in the typed focus facts.
            let target = usize::from(resolved_count);
            if !expanded.is_empty() && expanded.len() < target {
                let originals = expanded.clone();
                for index in expanded.len()..target {
                    let source = &originals[index % originals.len()];
                    // An expansion is Runtime-authorized duplication of this
                    // exact focus.  The overlap checker uses the minimum of
                    // both participants' budgets, so marking only the new
                    // replica would still reject the pair against the
                    // original slot's zero-overlap default.
                    for existing in &mut expanded {
                        if existing.focus_id == source.focus_id
                            && existing.capability_cropped_refs == source.capability_cropped_refs
                        {
                            existing.overlap_budget_bp = 10_000;
                        }
                    }
                    let replica_index = index + 1;
                    let boundary = format!(
                        "{} (Template-required replica {replica_index}; no additional authority)",
                        source.boundary
                    );
                    expanded.push(FocusPartitionSlot {
                        focus_id: format!("{}:replica:{replica_index}", source.focus_id),
                        boundary: boundary.clone(),
                        evidence_responsibility: source.evidence_responsibility.clone(),
                        capability_cropped_refs: source.capability_cropped_refs.clone(),
                        scope_hash: harness_contract::team::focus_scope_hash(
                            &role.role_id,
                            &boundary,
                            &source.capability_cropped_refs,
                        ),
                        // The replica intentionally overlaps its source; this
                        // is visible to the downstream novelty/evidence gates.
                        overlap_budget_bp: 10_000,
                        novelty_target_bp: source.novelty_target_bp,
                        output_contract: source.output_contract.clone(),
                        output_acceptance: source.output_acceptance.clone(),
                    });
                }
            }
            expanded
        }
    };
    focuses.truncate(usize::from(resolved_count));
    let reason =
        "cardinality resolved from template, focus plan, override, and static graph ceiling"
            .to_string();
    let focus_partitions = focuses
        .into_iter()
        .map(|slot| ResolvedFocusPartition {
            focus_id: slot.focus_id,
            boundary: slot.boundary,
            evidence_responsibility: slot.evidence_responsibility,
            output_contract: if slot.output_contract.is_empty() {
                role.task_contract.acceptance.clone()
            } else {
                slot.output_contract
            },
            output_acceptance: if slot.output_acceptance.is_empty() {
                role.task_contract.acceptance.clone()
            } else {
                slot.output_acceptance
            },
            shared_baseline: shared_baseline.clone(),
            capability_cropped_refs: slot.capability_cropped_refs,
            scope_hash: slot.scope_hash,
            overlap_budget_bp: slot.overlap_budget_bp,
            novelty_target_bp: slot.novelty_target_bp,
        })
        .collect::<Vec<_>>();
    Ok((
        focus_partitions,
        RoleCardinalityResolution {
            role_id: role.role_id.clone(),
            requested,
            resolved_count,
            static_graph_ceiling: MAX_TEAM_GRAPH_AGENT_NODES as u16,
            reason,
        },
    ))
}

fn bounded_slot_resource_scopes(team_scopes: &[String], focus_refs: &[String]) -> Vec<String> {
    if focus_refs.is_empty() {
        return team_scopes.to_vec();
    }
    let mut scopes = focus_refs.to_vec();
    scopes.extend(
        team_scopes
            .iter()
            .filter(|scope| {
                !scope.starts_with("read:")
                    && !scope.starts_with("write:")
                    && !scope.starts_with("worktree:")
            })
            .cloned(),
    );
    scopes.sort();
    scopes.dedup();
    scopes
}

fn bounded_objective_context(parent_objective: &str) -> String {
    let mut context = "Runtime intentionally withholds the parent cross-Team objective from delegated Team roles. Evaluate only this role's bounded Focus, resource scopes, acceptance contract, and canonical upstream results."
        .to_string();
    if parent_objective.contains("request_collaboration_escalation") {
        context.push_str(" The parent contract explicitly requires one Runtime-attested collaboration escalation. After acquiring your first source receipt and before terminal synthesis, use the native request_collaboration_escalation tool once to propose one bounded follow-up Team. Supply only its semantic reason and requested_add_team; Runtime derives all fences. Do not replace this native call with prose.");
    }
    context
}

fn crop_tools_to_resource_lease(tools: &[String], scopes: &[String]) -> Vec<String> {
    let network = scopes.iter().any(|scope| scope == "network:*");
    let read_paths = scopes
        .iter()
        .filter_map(|scope| scope.strip_prefix("read:"))
        .collect::<Vec<_>>();
    let workspace_read = scopes.iter().any(|scope| {
        scope.starts_with("read:") || scope.starts_with("write:") || scope.starts_with("worktree:")
    });
    let exact_file_read = !read_paths.is_empty()
        && read_paths.iter().all(|path| {
            !path.contains('*')
                && !path.ends_with('/')
                && std::path::Path::new(path).extension().is_some()
        });
    let workspace_write = scopes.iter().any(|scope| scope.starts_with("write:"));
    tools
        .iter()
        .filter(|tool| match tool.as_str() {
            "web_search" | "web_fetch" => network,
            "read_file" => workspace_read,
            "grep_search" | "glob_search" => workspace_read && !exact_file_read,
            "write_file" | "edit_file" | "bash" => workspace_write,
            // Context continuity, discovery over the already-cropped catalog,
            // and Team exchange do not widen a resource lease.
            "context_retrieve"
            | "tool_search"
            | "evidence_retrieve"
            | "request_collaboration_escalation" => true,
            // Capability expansion must add an explicit resource classification
            // here. Unknown tools fail closed instead of inheriting an
            // unrelated network/read lease.
            _ => false,
        })
        .cloned()
        .collect()
}

fn slot_budget_lease(
    request: &TeamInstantiationRequest,
    node_id: &str,
    slot_index: usize,
    total_slots: usize,
) -> ChildExecutionBudgetReservation {
    ChildExecutionBudgetReservation {
        lease_id: format!("{}:{node_id}", request.execution_budget.budget_id),
        parent_budget: request.execution_budget.clone(),
        parent_budget_id: request.execution_budget.budget_id.clone(),
        owner_id: format!("{}:slot:{}", request.team_id, slot_index + 1),
        scope: "team_agent".to_string(),
        max_tokens: partition_initial_budget_target(
            request.execution_budget.max_tokens,
            total_slots,
            slot_index,
        ),
        consumed_tokens: 0,
        deadline_at_ms: request.execution_budget.deadline_at_ms,
        max_parallel: request.execution_budget.max_parallel,
        revision: request.execution_budget.revision,
        slot_index,
        total_slots,
    }
}

fn partition_initial_budget_target(limit: u64, total_slots: usize, slot_index: usize) -> u64 {
    if total_slots == 0 || slot_index >= total_slots {
        return 0;
    }
    let slots = total_slots as u64;
    let base = limit / slots;
    base + u64::from((slot_index as u64) < limit % slots)
}

fn validate_finite_team_budget_capacity(
    lease_id: &str,
    remaining_tokens: u64,
    total_slots: usize,
) -> Result<(), String> {
    if remaining_tokens < total_slots as u64 {
        return Err(format!(
            "Team resource lease `{lease_id}` has {remaining_tokens} tokens for {total_slots} Agent slots; every reservation must be positive"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod acceptance_contract_tests {
    use super::*;

    #[test]
    fn four_agent_initial_targets_cover_but_never_multiply_parent_budget() {
        let reservations = (0..4)
            .map(|slot| partition_initial_budget_target(10_003, 4, slot))
            .collect::<Vec<_>>();
        assert_eq!(reservations, vec![2_501, 2_501, 2_501, 2_500]);
        assert_eq!(reservations.iter().sum::<u64>(), 10_003);
        assert!(validate_finite_team_budget_capacity("finite", 3, 4).is_err());
        assert!(validate_finite_team_budget_capacity("finite", 4, 4).is_ok());
        assert_eq!(partition_initial_budget_target(10_003, 4, 4), 0);
    }

    #[test]
    fn network_only_agent_never_receives_workspace_tools() {
        let tools = vec![
            "web_search".to_string(),
            "web_fetch".to_string(),
            "read_file".to_string(),
            "glob_search".to_string(),
            "grep_search".to_string(),
            "write_file".to_string(),
            "context_retrieve".to_string(),
            "team_board".to_string(),
            "evidence_retrieve".to_string(),
        ];
        let cropped = crop_tools_to_resource_lease(&tools, &["network:*".to_string()]);
        assert_eq!(
            cropped,
            vec![
                "web_search".to_string(),
                "web_fetch".to_string(),
                "context_retrieve".to_string(),
                "evidence_retrieve".to_string(),
            ]
        );
    }

    #[test]
    fn upstream_reducer_cannot_observe_or_judge_peer_team_objectives() {
        let parent = "Team A reads a.toml; Team B reads b.toml; combine both.";
        let bounded = bounded_objective_context(parent);
        assert!(!bounded.contains("a.toml"));
        assert!(!bounded.contains("b.toml"));
        assert!(!bounded.contains("Team A"));
        assert!(bounded.contains("bounded Focus"));
    }

    #[test]
    fn workspace_leases_separate_read_and_write_tools() {
        let tools = vec![
            "read_file".to_string(),
            "grep_search".to_string(),
            "write_file".to_string(),
            "edit_file".to_string(),
            "bash".to_string(),
            "unclassified_extension".to_string(),
        ];
        assert_eq!(
            crop_tools_to_resource_lease(&tools, &["read:crates/runtime".to_string()]),
            vec!["read_file".to_string(), "grep_search".to_string()]
        );
        assert_eq!(
            crop_tools_to_resource_lease(&tools, &["write:crates/runtime".to_string()]),
            vec![
                "read_file".to_string(),
                "grep_search".to_string(),
                "write_file".to_string(),
                "edit_file".to_string(),
                "bash".to_string(),
            ]
        );
    }

    #[test]
    fn exact_file_focus_exposes_read_file_without_discovery_or_manual_team_publish() {
        let tools = vec![
            "read_file".to_string(),
            "glob_search".to_string(),
            "grep_search".to_string(),
            "team_board".to_string(),
            "evidence_retrieve".to_string(),
        ];
        assert_eq!(
            crop_tools_to_resource_lease(&tools, &["read:crates/runtime/Cargo.toml".to_string()]),
            vec!["read_file".to_string(), "evidence_retrieve".to_string()]
        );
    }

    #[test]
    fn explicit_escalation_contract_survives_role_scope_cropping() {
        assert_eq!(
            crop_tools_to_resource_lease(
                &[
                    "read_file".to_string(),
                    "request_collaboration_escalation".to_string(),
                ],
                &["read:crates/runtime".to_string()],
            ),
            vec![
                "read_file".to_string(),
                "request_collaboration_escalation".to_string(),
            ]
        );
        assert!(bounded_objective_context(
            "the managed Agent must call request_collaboration_escalation after evidence"
        )
        .contains("native request_collaboration_escalation tool"));
    }

    #[test]
    fn unknown_acceptance_text_fails_closed() {
        assert!(team_acceptance_contract(
            &["looks reasonable".to_string()],
            &["read:crates/runtime".to_string()],
            false,
            false,
        )
        .is_err());
    }

    #[test]
    fn typed_acceptance_uses_exact_scopes_and_checks() {
        let contract = team_acceptance_contract(
            &[
                "implementation".to_string(),
                "source_verification".to_string(),
                "evidence_scope:crates/runtime".to_string(),
            ],
            &["write:crates/runtime".to_string()],
            false,
            false,
        )
        .expect("typed contract");
        assert_eq!(contract.len(), 3);
        assert!(matches!(
            contract[0].check,
            TeamAcceptanceCheck::WorkspaceChange { .. }
        ));
        assert!(matches!(
            contract[1].check,
            TeamAcceptanceCheck::SourceVerification { .. }
        ));
        assert_eq!(
            contract[2].check,
            TeamAcceptanceCheck::ScopedEvidence {
                scopes: vec!["crates/runtime".to_string()]
            }
        );
    }

    #[test]
    fn unqualified_evidence_scope_compiles_to_a_typed_read_obligation() {
        let root = tempfile::tempdir().expect("workspace");
        std::fs::create_dir_all(root.path().join("crates/runtime")).expect("fixture scope");
        let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identity resolver");
        let required = compile_required_acceptance(
            &["evidence_scope:crates/runtime".to_string()],
            &[TeamAcceptanceRequirement {
                criterion: "evidence_scope:crates/runtime".to_string(),
                check: TeamAcceptanceCheck::ScopedEvidence {
                    scopes: vec!["crates/runtime".to_string()],
                },
            }],
            &resolver,
            false,
        );
        assert!(matches!(
            required.evidence_obligations[0].target,
            harness_contract::context::EvidenceTargetIdentity::Workspace { .. }
        ));
    }

    #[test]
    fn session_authority_is_not_misclassified_as_a_workspace_evidence_scope() {
        let contract = team_acceptance_contract(
            &["evidence-backed output".to_string()],
            &[
                "read:crates/runtime".to_string(),
                "session:session-1".to_string(),
            ],
            true,
            false,
        )
        .expect("legacy custom contract has an exact evidence-bound adapter");
        assert_eq!(
            contract[0].check,
            TeamAcceptanceCheck::LegacyEvidenceBound {
                scopes: vec!["read:crates/runtime".to_string()]
            }
        );
    }

    #[test]
    fn network_evidence_is_a_bounded_team_acceptance_scope() {
        let contract = team_acceptance_contract(
            &["findings".to_string(), "evidence".to_string()],
            &["network:*".to_string(), "session:session-1".to_string()],
            false,
            false,
        )
        .expect("network evidence contract");

        assert!(contract.iter().any(|requirement| {
            requirement.criterion == "evidence"
                && requirement.check
                    == TeamAcceptanceCheck::ScopedEvidence {
                        scopes: vec!["network:*".to_string()],
                    }
        }));
    }

    #[test]
    fn session_lease_is_authorization_context_not_verifiable_evidence() {
        // A session-only lease keeps the legacy compile-time bound, but a
        // whole-workspace read alias is preferred whenever it exists because
        // it is satisfiable by real read receipts.
        let session_only = team_acceptance_contract(
            &["evidence".to_string()],
            &["session:session-1".to_string()],
            false,
            false,
        )
        .expect("session lease remains the legacy last-resort bound");
        assert!(session_only.iter().any(|requirement| {
            requirement.criterion == "evidence"
                && requirement.check
                    == TeamAcceptanceCheck::ScopedEvidence {
                        scopes: vec!["session:session-1".to_string()],
                    }
        }));

        let network = team_acceptance_contract(
            &["evidence".to_string()],
            &["session:session-1".to_string(), "network:*".to_string()],
            false,
            false,
        )
        .expect("network evidence contract");
        assert!(network.iter().any(|requirement| {
            requirement.criterion == "evidence"
                && requirement.check
                    == TeamAcceptanceCheck::ScopedEvidence {
                        scopes: vec!["network:*".to_string()],
                    }
        }));
        let workspace_root = team_acceptance_contract(
            &["evidence".to_string()],
            &["read:.".to_string(), "session:session-1".to_string()],
            false,
            false,
        )
        .expect("whole-workspace read alias keeps the evidence bound satisfiable");
        assert!(workspace_root.iter().any(|requirement| {
            requirement.criterion == "evidence"
                && requirement.check
                    == TeamAcceptanceCheck::ScopedEvidence {
                        scopes: vec!["read:.".to_string()],
                    }
        }));
        assert!(team_acceptance_contract(&["evidence".to_string()], &[], false, false).is_err());
    }

    #[test]
    fn static_graph_ceiling_accepts_boundary_and_rejects_overflow() {
        assert!(ensure_static_graph_ceiling(24, 8).is_ok());
        assert!(ensure_static_graph_ceiling(25, 8).is_err());
    }

    #[test]
    fn occupancy_levels_do_not_change_team_topology_or_hash() {
        use std::hash::{DefaultHasher, Hash, Hasher};

        let role = TeamRoleDefinition {
            role_id: "researcher".to_string(),
            display_name: None,
            responsibility: "investigate one bounded focus".to_string(),
            agent_definition_id: harness_contract::agent::AgentDefinitionId::new(
                harness_contract::agent::DefinitionScope::Builtin,
                "cowd/explore",
            )
            .expect("agent definition"),
            agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
            cardinality: RoleCardinalityPolicy::Adaptive {
                min: 2,
                target: 4,
                max: 8,
            },
            partition: RolePartitionPolicy::ByFocus {
                partition_key: "investigation".to_string(),
            },
            behavior: vec![
                harness_contract::team::RoleBehaviorFacet::ReacquireEvidence { required: true },
            ],
            grant_ceiling: vec![harness_contract::agent::AgentCapability::Read],
            task_contract: harness_contract::team::TeamRoleTaskContract {
                contract_ref: "builtin/team-role/researcher@1".to_string(),
                acceptance: vec!["findings".to_string(), "evidence".to_string()],
            },
        };
        let topology_hash = || {
            let (focuses, resolution) =
                resolve_focuses(&role, None, None).expect("static topology");
            let mut hasher = DefaultHasher::new();
            resolution.resolved_count.hash(&mut hasher);
            for focus in focuses {
                focus.focus_id.hash(&mut hasher);
                focus.scope_hash.hash(&mut hasher);
            }
            hasher.finish()
        };

        // Occupancy is deliberately not an input. These labels document the
        // required 0/50/90% equivalence without introducing a second resource
        // interface into Team topology compilation.
        let hashes = [0_u8, 50, 90].map(|_occupancy_percent| topology_hash());
        assert_eq!(hashes, [hashes[0]; 3]);
    }

    #[test]
    fn template_cardinality_expands_an_authorized_focus_without_authority_widening() {
        let role = TeamRoleDefinition {
            role_id: "researcher".to_string(),
            display_name: None,
            responsibility: "investigate one bounded focus".to_string(),
            agent_definition_id: harness_contract::agent::AgentDefinitionId::new(
                harness_contract::agent::DefinitionScope::Builtin,
                "cowd/explore",
            )
            .expect("agent definition"),
            agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
            cardinality: RoleCardinalityPolicy::Adaptive {
                min: 2,
                target: 2,
                max: 4,
            },
            partition: RolePartitionPolicy::ByFocus {
                partition_key: "investigation".to_string(),
            },
            behavior: vec![
                harness_contract::team::RoleBehaviorFacet::ReacquireEvidence { required: true },
            ],
            grant_ceiling: vec![harness_contract::agent::AgentCapability::Read],
            task_contract: harness_contract::team::TeamRoleTaskContract {
                contract_ref: "builtin/team-role/researcher@1".to_string(),
                acceptance: vec!["evidence".to_string()],
            },
        };
        let plan = FocusPartitionPlan {
            role_id: role.role_id.clone(),
            shared_baseline: Vec::new(),
            slots: vec![FocusPartitionSlot {
                focus_id: "actual-authorized-scope".to_string(),
                boundary: "inspect one declared scope".to_string(),
                evidence_responsibility: "collect evidence".to_string(),
                capability_cropped_refs: vec!["read:crates/runtime".to_string()],
                scope_hash: "scope-1".to_string(),
                overlap_budget_bp: 0,
                novelty_target_bp: 2_500,
                output_contract: vec!["findings".to_string()],
                output_acceptance: vec!["evidence".to_string()],
            }],
        };
        let (focuses, resolution) =
            resolve_focuses(&role, None, Some(&plan)).expect("resolve Template role");
        assert_eq!(resolution.resolved_count, 2);
        assert_eq!(focuses.len(), 2);
        assert_eq!(
            focuses[1].capability_cropped_refs,
            vec!["read:crates/runtime".to_string()],
            "Template expansion must not widen resource authority"
        );
        assert_eq!(focuses[1].overlap_budget_bp, 10_000);
        assert_eq!(
            focuses[0].overlap_budget_bp, 10_000,
            "both sides of a template-required focus replica authorize its overlap"
        );
        assert!(focuses[1].focus_id.contains("replica"));
    }

    #[test]
    fn typed_reducer_behavior_consumes_predecessor_evidence_without_reacquisition() {
        let reducer = vec![RoleBehaviorFacet::Reducer {
            mode: "finally".to_string(),
        }];
        let evidence_producer = vec![RoleBehaviorFacet::ReacquireEvidence { required: true }];
        assert!(reducer
            .iter()
            .any(|facet| matches!(facet, RoleBehaviorFacet::Reducer { .. })));
        assert!(!evidence_producer
            .iter()
            .any(|facet| matches!(facet, RoleBehaviorFacet::Reducer { .. })));
        let contract = team_acceptance_contract(
            &[
                "summary".to_string(),
                "evidence".to_string(),
                "unresolved".to_string(),
            ],
            &[
                "read:crates/runtime".to_string(),
                "read:crates/gateway".to_string(),
                "session:session-1".to_string(),
            ],
            false,
            true,
        )
        .expect("upstream reducer contract");
        assert!(contract.iter().any(|requirement| {
            requirement.criterion == "evidence"
                && requirement.check == TeamAcceptanceCheck::UpstreamEvidence
        }));
        assert!(!contract.iter().any(|requirement| {
            matches!(
                &requirement.check,
                TeamAcceptanceCheck::ScopedEvidence { .. }
            )
        }));
    }

    #[test]
    fn reviewer_evidence_is_scoped_to_upstream_write_paths() {
        let contract = team_acceptance_contract(
            &[
                "review".to_string(),
                "evidence".to_string(),
                "risks".to_string(),
            ],
            &[
                "read:fixtures/protected/sentinel.txt".to_string(),
                "read:fixtures/write/target.txt".to_string(),
                "write:fixtures/write/target.txt".to_string(),
            ],
            false,
            false,
        )
        .expect("review contract");

        assert!(contract.iter().any(|requirement| {
            requirement.criterion == "evidence"
                && requirement.check
                    == TeamAcceptanceCheck::ScopedEvidence {
                        scopes: vec!["read:fixtures/write/target.txt".to_string()],
                    }
        }));
    }

    #[test]
    fn single_role_consumes_its_runtime_planned_focus() {
        let role = TeamRoleDefinition {
            role_id: "reviewer".to_string(),
            display_name: None,
            responsibility: "review committed output".to_string(),
            agent_definition_id: harness_contract::agent::AgentDefinitionId::new(
                harness_contract::agent::DefinitionScope::Builtin,
                "cowd/direct",
            )
            .expect("definition"),
            agent_selector: RevisionSelector::ExactApprovedRevision { revision: 1 },
            cardinality: RoleCardinalityPolicy::Fixed { count: 1 },
            partition: RolePartitionPolicy::Single,
            behavior: vec![
                harness_contract::team::RoleBehaviorFacet::TerminalCandidate { required: true },
            ],
            grant_ceiling: vec![harness_contract::agent::AgentCapability::Read],
            task_contract: harness_contract::team::TeamRoleTaskContract {
                contract_ref: "builtin/team-role/reviewer@1".to_string(),
                acceptance: vec!["review".to_string(), "evidence".to_string()],
            },
        };
        let plan = FocusPartitionPlan {
            role_id: "reviewer".to_string(),
            shared_baseline: Vec::new(),
            slots: vec![FocusPartitionSlot {
                focus_id: "bounded-review".to_string(),
                boundary: "review report only".to_string(),
                evidence_responsibility: "independent report read".to_string(),
                capability_cropped_refs: vec!["read:evidence/report.html".to_string()],
                scope_hash: "scope".to_string(),
                overlap_budget_bp: 0,
                novelty_target_bp: 0,
                output_contract: vec!["review".to_string(), "evidence".to_string()],
                output_acceptance: vec!["review".to_string(), "evidence".to_string()],
            }],
        };

        let (focuses, resolution) =
            resolve_focuses(&role, None, Some(&plan)).expect("planned single focus");
        assert_eq!(resolution.resolved_count, 1);
        assert_eq!(focuses[0].focus_id, "bounded-review");
        assert_eq!(
            focuses[0].capability_cropped_refs,
            vec!["read:evidence/report.html"]
        );
    }
}
