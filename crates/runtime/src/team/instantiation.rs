//! Runtime-owned compilation of a Team template into one immutable graph.
//!
//! This module is deliberately the only point where a Team request becomes
//! AgentTask packets. Gateway, model orchestration, eval, and Surfaces submit
//! the same declarative [`TeamInstantiationRequest`]; none of them select
//! executor nodes or mutable Agent identities.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use harness_contract::agent::{AgentDefinitionRevisionRef, AgentTaskIntent, RevisionSelector};
use harness_contract::context::ContextBudgetLeaseRef;
use harness_contract::execution_graph::{
    ExecutionEdge, ExecutionEdgeKind, ExecutionGraph, ExecutionNodeKind, ExecutionNodeSpec,
};
use harness_contract::team::{
    FocusPartitionPlan, FocusPartitionSlot, RoleCardinalityPolicy, RolePartitionPolicy,
    TeamInstantiationRequest, TeamRoleBindingOverride, TeamRoleDefinition,
    TeamTemplateDefinitionId, TeamTemplateSelector,
};

use crate::execution_core::graph::executors::{
    AgentTaskExecutor, SynthesizeNodeExecutor, VerifyNodeExecutor,
};
use crate::execution_core::graph::{ExecutionResourceKind, ExecutionResourceManager};
use crate::{
    resolve_agent_capability, AgentBindingCompiler, AgentCapabilityRequest,
    EvolutionCandidateSubject, EvolutionGovernanceService, EvolutionReleaseAssignment,
    RuntimeDefinitionRegistry,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedFocusPartition {
    pub focus_id: String,
    pub boundary: String,
    pub evidence_responsibility: String,
    pub output_contract: Vec<String>,
    pub shared_baseline: Vec<String>,
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
}

impl TeamInstantiationService {
    #[must_use]
    pub fn new(
        registry: Arc<RuntimeDefinitionRegistry>,
        resources: Arc<ExecutionResourceManager>,
        evolution_governance: Arc<EvolutionGovernanceService>,
    ) -> Self {
        Self {
            binding_compiler: AgentBindingCompiler::new(Arc::clone(&registry)),
            registry,
            resources,
            evolution_governance,
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
        let runtime_parallel_capacity = self.runtime_parallel_capacity()?;

        let mut graph = ExecutionGraph::new(request.objective.clone());
        graph.id = format!("team-graph:{}", request.team_id);
        graph.parent_execution = request.parent_execution.clone();

        let mut slots_by_role = BTreeMap::<String, Vec<(String, String)>>::new();
        let mut role_slots = Vec::new();
        let mut cardinality_resolutions = Vec::new();
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
                let node_id = format!("{}:{}:{}", graph.id, role.role_id, slot + 1);
                let run_id = format!("{}:run:{}:{}", request.team_id, role.role_id, slot + 1);
                let task_id = format!("{}:task:{}:{}", request.team_id, role.role_id, slot + 1);
                let intent = AgentTaskIntent {
                    selected_agent_id: Some(definition_ref.definition_id.as_str().to_string()),
                    definition_ref: Some(definition_ref.clone()),
                    granted_capabilities: grant_ceiling.clone(),
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
                    acceptance: role.task_contract.acceptance.clone(),
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
                        "team_working_state:visible".to_string(),
                    ],
                    context_refs: Vec::new(),
                    evidence_refs: Vec::new(),
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
                let packet = self
                    .binding_compiler
                    .compile_task_intent(intent, None)
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
                node.resource_scopes = request.resource_scopes.clone();
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
            TeamTemplateSelector::Automatic => (
                TeamTemplateDefinitionId::try_from("builtin/cowd/execute-review")
                    .map_err(|error| error.to_string())?,
                RevisionSelector::LatestApprovedStable,
            ),
        };
        let routing_identity = format!(
            "{}|{}|{}|{}",
            request.session_id,
            request.team_id,
            request.mission_id.as_deref().unwrap_or("direct"),
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
        let agent_available = agent
            .effective_limit
            .saturating_sub(agent.active_leases)
            .max(1);
        let provider_available = provider
            .effective_limit
            .saturating_sub(provider.active_leases)
            .max(1);
        let tool_available = tool
            .effective_limit
            .saturating_sub(tool.active_leases)
            .max(1);
        u16::try_from(agent_available.min(provider_available).min(tool_available)).map_err(|_| {
            "Runtime resource capacity exceeds Team cardinality representation".to_string()
        })
    }
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
            output_contract: role.task_contract.acceptance.clone(),
        }],
        RolePartitionPolicy::Explicit { partitions } => partitions
            .iter()
            .map(|partition| FocusPartitionSlot {
                focus_id: partition.clone(),
                boundary: partition.clone(),
                evidence_responsibility: "partition-specific evidence".to_string(),
                output_contract: role.task_contract.acceptance.clone(),
            })
            .collect(),
        RolePartitionPolicy::ByFocus { partition_key } if planned.is_empty() => (1
            ..=resolved_count)
            .map(|slot| FocusPartitionSlot {
                focus_id: format!("{partition_key}:{slot}"),
                boundary: format!("{partition_key}:{slot}"),
                evidence_responsibility: "independent evidence for this focus".to_string(),
                output_contract: role.task_contract.acceptance.clone(),
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
            shared_baseline: shared_baseline.clone(),
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
