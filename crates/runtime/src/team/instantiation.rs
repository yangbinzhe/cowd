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
use crate::execution_core::graph::{ExecutionResourceKind, ExecutionResourceManager};
use crate::{
    resolve_agent_capability, AgentBindingCompiler, AgentCapabilityRequest,
    EvolutionCandidateSubject, EvolutionGovernanceService, EvolutionReleaseAssignment,
    RuntimeDefinitionRegistry,
};
use harness_contract::agent::{AgentDefinitionRevisionRef, AgentTaskIntent, RevisionSelector};
use harness_contract::context::ContextBudgetLeaseRef;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
};
use harness_contract::reality::EvidenceRef;
use harness_contract::task::{TaskCreateCommand, TaskExecutionPolicy, TaskPhaseSpec, TaskSpec};
use harness_contract::team::{
    FocusPartitionPlan, FocusPartitionSlot, RoleCardinalityPolicy, RolePartitionPolicy,
    TeamAcceptanceCheck, TeamAcceptanceRequirement, TeamInstantiationRequest,
    TeamRoleBindingOverride, TeamRoleDefinition, TeamStructuredOutputField,
    TeamTemplateDefinitionId, TeamTemplateSelector,
};

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
}

/// The durable explanation of one resolved role cardinality.  A graph
/// contains the resolved slots; this record makes the policy decision
/// inspectable without treating a resource queue as a second Team scheduler.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleCardinalityResolution {
    pub role_id: String,
    pub requested: RoleCardinalityPolicy,
    pub resolved_count: u16,
    pub runtime_parallel_capacity: u16,
    pub reason: String,
}

#[derive(Debug, Clone)]
pub struct TeamInstantiation {
    pub graph: ExecutionGraph,
    pub task_commands: Vec<TaskCreateCommand>,
    pub template_ref: harness_contract::team::TeamTemplateRevisionRef,
    pub template_digest: String,
    /// Immutable Runtime authorization used for this graph's Template
    /// selection. `None` means a normal Stable/default resolution.
    pub release_assignment: Option<EvolutionReleaseAssignment>,
    pub role_slots: Vec<ResolvedRoleSlot>,
    pub cardinality_resolutions: Vec<RoleCardinalityResolution>,
}

#[derive(Debug)]
pub struct TeamInstantiationService {
    registry: Arc<RuntimeDefinitionRegistry>,
    binding_compiler: AgentBindingCompiler,
    resources: Arc<ExecutionResourceManager>,
    evolution_governance: Arc<EvolutionGovernanceService>,
    workspace_id: String,
}

impl TeamInstantiationService {
    #[must_use]
    pub fn new(
        registry: Arc<RuntimeDefinitionRegistry>,
        resources: Arc<ExecutionResourceManager>,
        evolution_governance: Arc<EvolutionGovernanceService>,
        workspace_id: impl Into<String>,
    ) -> Self {
        Self {
            binding_compiler: AgentBindingCompiler::new(Arc::clone(&registry)),
            registry,
            resources,
            evolution_governance,
            workspace_id: workspace_id.into(),
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
        let focus_plans = focus_partition_plans(&request, &manifest.roles)?;
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
        let runtime_parallel_capacity = self.runtime_parallel_capacity()?;

        let mut graph = ExecutionGraph::new(request.objective.clone());
        graph.id = format!("team-graph:{}", request.team_id);
        graph.parent_execution = request.parent_execution.clone();

        let mut slots_by_role = BTreeMap::<String, Vec<(String, String)>>::new();
        let mut role_slots = Vec::new();
        let mut cardinality_resolutions = Vec::new();
        let mut task_commands = Vec::new();
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
            let role_allowed_tools = match evaluation_allowed_tools {
                Some(evaluation_tools) => evaluation_tools
                    .iter()
                    .filter(|tool| capability.allowed_tools.contains(*tool))
                    .cloned()
                    .collect::<Vec<_>>(),
                None => capability.allowed_tools.iter().cloned().collect(),
            };
            let (focuses, cardinality_resolution) = resolve_focuses(
                role,
                cardinality_overrides.get(&role.role_id),
                focus_plans.get(&role.role_id),
                runtime_parallel_capacity,
            )?;
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
                let acceptance_contract = team_acceptance_contract(
                    &slot_acceptance,
                    &resource_scopes,
                    !role.task_contract.contract_ref.starts_with("builtin/"),
                    manifest
                        .dependencies
                        .iter()
                        .any(|dependency| dependency.to_role_id == role.role_id)
                        && matches!(
                            role.role_id.as_str(),
                            "synthesizer" | "arbiter" | "commander" | "comparator" | "coordinator"
                        ),
                )?;
                let intent = AgentTaskIntent {
                    selected_agent_id: Some(definition_ref.definition_id.as_str().to_string()),
                    definition_ref: Some(definition_ref.clone()),
                    granted_capabilities: grant_ceiling.clone(),
                    principal_id: "runtime.team".to_string(),
                    source_turn_id: request.request_id.clone(),
                    run_id,
                    task_id,
                    session_id: request.session_id.clone(),
                    mission_id: request.mission_id.clone(),
                    team_id: Some(request.team_id.clone()),
                    graph_id: graph.id.clone(),
                    node_id: node_id.clone(),
                    attempt: 1,
                    expected_graph_revision: 0,
                    objective: format!(
                        "{}\n\n## Team role\nRole: {}\nResponsibility: {}\nFocus: {}\nBoundary: {}\nEvidence responsibility: {}\nShared baseline: {}\nOutput contract: {}\nComplete only this bounded focus and state evidence plus unresolved items explicitly.",
                        request.objective,
                        role.role_id,
                        role.responsibility,
                        focus_partition.focus_id,
                        focus_partition.boundary,
                        focus_partition.evidence_responsibility,
                        focus_partition.shared_baseline.join("; "),
                        focus_partition.output_contract.join(", "),
                    ),
                    acceptance: slot_acceptance,
                    constraints: vec![
                        format!("team_template:{}@{}", template.revision.revision_ref.template_id.as_str(), template.revision.revision_ref.revision),
                        format!("team_role:{}", role.role_id),
                        format!("role_slot:{}", slot + 1),
                        format!("focus_partition:{}", focus_partition.focus_id),
                        format!("focus_boundary:{}", focus_partition.boundary),
                        format!(
                            "focus_evidence_responsibility:{}",
                            focus_partition.evidence_responsibility
                        ),
                        format!("focus_scope_hash:{}", focus_partition.scope_hash),
                        format!(
                            "focus_overlap_budget_bp:{}",
                            focus_partition.overlap_budget_bp
                        ),
                        format!(
                            "focus_novelty_target_bp:{}",
                            focus_partition.novelty_target_bp
                        ),
                        format!(
                            "focus_output_acceptance:{}",
                            focus_partition.output_acceptance.join(", ")
                        ),
                        format!(
                            "team_acceptance_contract:{}",
                            serde_json::to_string(&acceptance_contract).map_err(|error| {
                                format!("encode Team acceptance contract: {error}")
                            })?
                        ),
                        "nested_team:forbidden".to_string(),
                        "parent_merge:exactly_once".to_string(),
                        "team_working_state:visible".to_string(),
                    ]
                    .into_iter()
                    .chain(request.strategy_binding.iter().flat_map(|binding| {
                        [
                            format!("strategy_decision_id:{}", binding.decision_id),
                            format!("strategy_decision_revision:{}", binding.decision_revision),
                            format!("collaboration_lease:{}", binding.decision_lease),
                            format!("turn_ref:{}", binding.turn_ref),
                        ]
                    }))
                    .collect(),
                    context_refs: focus_partition.capability_cropped_refs.clone(),
                    // A resource lease authorizes discovery but is not itself
                    // evidence. Durable tool audits populate result evidence.
                    evidence_refs: Vec::new(),
                    resource_scopes,
                    // Runtime derives normal role tools from the immutable
                    // capability grant. Evaluation may only narrow that set;
                    // it cannot grant a tool absent from the role contract.
                    allowed_tools: role_allowed_tools.clone(),
                    allowed_skills: Vec::new(),
                    permission_lease: request.permission_lease.clone(),
                    model_lease: request.model_lease.clone(),
                    budget_lease: slot_budget_lease(&request, &node_id, slot),
                    managed_invocation: request.managed_invocation.clone(),
                    idempotency_key: format!("team:{}:{}:{}", request.team_id, role.role_id, slot + 1),
                };
                task_commands.push(TaskCreateCommand {
                    task_id: intent.task_id.clone(),
                    mission_id: intent.mission_id.clone(),
                    source_session_id: intent.session_id.clone(),
                    source_turn_id: intent.source_turn_id.clone(),
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
                    },
                    evidence_refs: vec![EvidenceRef::new(
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
                let mut node = ExecutionNodeSpec::new(
                    ExecutionNodeKind::AgentTask,
                    AgentTaskExecutor::KIND,
                    serde_json::to_string(&packet)
                        .map_err(|error| format!("encode Team role packet: {error}"))?,
                );
                node.id = node_id.clone();
                node.idempotency_key = packet.idempotency_key.clone();
                node.acceptance.criteria = packet.acceptance.clone();
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

        Ok(TeamInstantiation {
            graph,
            task_commands,
            template_ref: template.revision.revision_ref,
            template_digest: template.revision.content_digest,
            release_assignment,
            role_slots,
            cardinality_resolutions,
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
            TeamTemplateSelector::Automatic => {
                let template = if request.permission_lease == "workspace_write"
                    || request.permission_lease == "workspace-write"
                {
                    "builtin/cowd/execute-review"
                } else {
                    "builtin/cowd/parallel-research-synthesis"
                };
                (
                    TeamTemplateDefinitionId::try_from(template)
                        .map_err(|error| error.to_string())?,
                    RevisionSelector::LatestApprovedStable,
                )
            }
        };
        let routing_identity = format!(
            "{}|{}|{}|{}",
            request.session_id,
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

    fn runtime_parallel_capacity(&self) -> Result<u16, String> {
        let agent = self
            .resources
            .snapshot(&ExecutionResourceKind::Agent)
            .map_err(|error| format!("read Agent resource capacity: {error}"))?;
        let provider = self
            .resources
            .snapshot(&ExecutionResourceKind::Provider)
            .map_err(|error| format!("read Provider resource capacity: {error}"))?;
        let tool = self
            .resources
            .snapshot(&ExecutionResourceKind::Tool)
            .map_err(|error| format!("read Tool resource capacity: {error}"))?;
        let agent_available = agent.effective_limit.saturating_sub(agent.active_leases);
        let provider_available = provider
            .effective_limit
            .saturating_sub(provider.active_leases);
        let tool_available = tool.effective_limit.saturating_sub(tool.active_leases);
        available_team_parallel_capacity(agent_available, provider_available, tool_available)
    }
}

fn available_team_parallel_capacity(
    agent_available: usize,
    provider_available: usize,
    tool_available: usize,
) -> Result<u16, String> {
    if agent_available == 0 || provider_available == 0 || tool_available == 0 {
        return Err(format!(
            "Team resource capacity exhausted: agent={agent_available}, provider={provider_available}, tool={tool_available}"
        ));
    }
    u16::try_from(agent_available.min(provider_available).min(tool_available)).map_err(|_| {
        "Runtime resource capacity exceeds Team cardinality representation".to_string()
    })
}

fn team_acceptance_contract(
    criteria: &[String],
    resource_scopes: &[String],
    allow_legacy_custom_contract: bool,
    upstream_synthesis_role: bool,
) -> Result<Vec<TeamAcceptanceRequirement>, String> {
    let workspace_scopes = resource_scopes
        .iter()
        .filter(|scope| {
            scope.starts_with("read:")
                || scope.starts_with("write:")
                || scope.starts_with("workspace:")
        })
        .cloned()
        .collect::<Vec<_>>();
    let write_scopes = workspace_scopes
        .iter()
        .filter(|scope| scope.starts_with("write:") || scope.starts_with("workspace:"))
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
        .then_some(write_scopes.clone())
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
                                .unwrap_or_else(|| workspace_scopes.clone()),
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
                                scopes: workspace_scopes.clone(),
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
) -> Result<BTreeMap<String, &'a FocusPartitionPlan>, String> {
    let known = roles
        .iter()
        .map(|role| role.role_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut plans = BTreeMap::new();
    for plan in &request.focus_partition_plans {
        if !known.contains(plan.role_id.as_str()) {
            return Err(format!(
                "focus partition plan names unknown role `{}`",
                plan.role_id
            ));
        }
        plans.insert(plan.role_id.clone(), plan);
    }
    Ok(plans)
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
    plan: Option<&&FocusPartitionPlan>,
    runtime_parallel_capacity: u16,
) -> Result<(Vec<ResolvedFocusPartition>, RoleCardinalityResolution), String> {
    let requested = override_.copied().unwrap_or(&role.cardinality).clone();
    if requested.min() < role.cardinality.min() || requested.max() > role.cardinality.max() {
        return Err(format!(
            "role `{}` cardinality override exceeds template ceiling",
            role.role_id
        ));
    }
    let planned = plan.map(|plan| plan.slots.clone()).unwrap_or_default();
    let requested_count = match &role.partition {
        RolePartitionPolicy::Single => 1,
        RolePartitionPolicy::Explicit { partitions } => u16::try_from(partitions.len())
            .map_err(|_| format!("role `{}` has too many explicit partitions", role.role_id))?,
        RolePartitionPolicy::ByFocus { .. } if !planned.is_empty() => u16::try_from(planned.len())
            .map_err(|_| format!("role `{}` has too many focus partitions", role.role_id))?,
        RolePartitionPolicy::ByFocus { .. } => requested.preferred().min(runtime_parallel_capacity),
    };
    if !requested.permits(requested_count) || !role.cardinality.permits(requested_count) {
        return Err(format!(
            "role `{}` focus partition count is outside the resolved cardinality",
            role.role_id
        ));
    }
    let resolved_count = if requested_count <= runtime_parallel_capacity {
        requested_count
    } else if matches!(
        requested,
        RoleCardinalityPolicy::Range { .. } | RoleCardinalityPolicy::Adaptive { .. }
    ) && planned.is_empty()
    {
        runtime_parallel_capacity
    } else {
        return Err(format!(
            "role `{}` requires {requested_count} slots but Runtime currently admits only {runtime_parallel_capacity}; submit a smaller valid partition or wait for capacity",
            role.role_id
        ));
    };
    if resolved_count < requested.min() {
        return Err(format!(
            "role `{}` requires at least {} slots but Runtime capacity is {runtime_parallel_capacity}",
            role.role_id,
            requested.min()
        ));
    }
    let shared_baseline = plan
        .map(|plan| plan.shared_baseline.clone())
        .unwrap_or_default();
    let mut focuses = match &role.partition {
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
        RolePartitionPolicy::ByFocus { .. } => planned,
    };
    focuses.truncate(usize::from(resolved_count));
    let reason = if focuses.len() as u16 == requested_count {
        "requested/template cardinality accepted by current Runtime capacity".to_string()
    } else {
        "adaptive cardinality reduced to current Runtime Agent/Provider/Tool capacity before graph creation"
            .to_string()
    };
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
            runtime_parallel_capacity,
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

fn slot_budget_lease(
    request: &TeamInstantiationRequest,
    node_id: &str,
    slot: usize,
) -> ContextBudgetLeaseRef {
    let (prefix, max_tokens, revision) = request
        .budget_lease
        .as_ref()
        .map(|lease| (lease.lease_id.as_str(), lease.max_tokens, lease.revision))
        .unwrap_or(("team-budget", 0, 0));
    ContextBudgetLeaseRef::new(
        format!("{prefix}:{node_id}"),
        format!("{}:slot:{}", request.team_id, slot + 1),
        "team_agent",
        max_tokens,
        revision,
    )
}

#[cfg(test)]
mod acceptance_contract_tests {
    use super::*;

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
    fn evidence_acceptance_without_a_bounded_scope_fails_at_instantiation() {
        assert!(team_acceptance_contract(
            &["evidence".to_string()],
            &["session:session-1".to_string()],
            false,
            false,
        )
        .is_err());
    }

    #[test]
    fn zero_runtime_capacity_fails_closed_instead_of_inventing_one_slot() {
        assert!(available_team_parallel_capacity(0, 3, 3).is_err());
        assert!(available_team_parallel_capacity(3, 0, 3).is_err());
        assert!(available_team_parallel_capacity(3, 3, 0).is_err());
        assert_eq!(available_team_parallel_capacity(4, 2, 3), Ok(2));
    }

    #[test]
    fn upstream_synthesizer_consumes_predecessor_evidence_without_reacquisition() {
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
                        scopes: vec!["write:fixtures/write/target.txt".to_string()],
                    }
        }));
    }
}
