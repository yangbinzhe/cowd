use std::collections::{BTreeMap, BTreeSet};

use harness_contract::acceptance::{AcceptanceVerdict, TerminalFactKind};
use harness_contract::agent::{AgentCapability, AgentTaskIntent};
use harness_contract::context::ChildExecutionBudgetReservation;
use harness_contract::execution_graph::{
    validate_execution_graph, CollaborationEdgeKind, CollaborationProgram,
    CollaborationProgramEdge, CollaborationTeamInstance, DependencyPredicate,
    ExecutionDependencyPolicy, ExecutionEdge, ExecutionEdgeKind, ExecutionGraph,
    ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeSpec, ExecutionNodeStatus,
    ExecutionOrchestrationMetadata, ExecutionParentBinding, ExecutionWorkContract,
    ExecutionWorkRole,
};
use harness_contract::team::{
    FocusPartitionPlan, FocusPartitionSlot, TeamInstantiationRequest, TeamSelectionMode,
    TeamTemplateDefinitionId, TeamTemplateSelector,
};
use thiserror::Error;

use crate::execution_core::graph::executors::AgentTaskExecutor;
use crate::execution_core::{ModelWorkEstimate, ModelWorkEstimateInput, ModelWorkGraphEstimator};
use crate::TeamRuntime;

use super::{
    CapabilityRecipeId, GraphMutationProposal, GraphSemanticNode, RuntimeOrchestrationCommand,
    RuntimeOrchestrationOperation, RuntimeOrchestrationPlan,
};

pub const TEAM_SUBGRAPH_EXECUTOR: &str = "team_subgraph";

#[derive(Debug, Error)]
pub enum OrchestrationCompileError {
    #[error("runtime operation `{0}` does not compile a new execution graph")]
    OperationDoesNotCompile(&'static str),
    #[error("semantic graph proposal is required")]
    MissingProposal,
    #[error("Team instantiation requires an active Runtime service")]
    TeamRuntimeRequired,
    #[error("semantic graph compilation failed: {0}")]
    InvalidProposal(String),
    #[error("Team template resolution failed: {0}")]
    TeamInstantiation(String),
}

#[derive(Debug, Clone)]
pub struct CompiledOrchestration {
    pub graph: ExecutionGraph,
    pub command: ExecutionGraphCommand,
    pub execute_without_protocol: bool,
    pub team_request: Option<TeamInstantiationRequest>,
    pub work_estimate: ModelWorkEstimate,
    /// Deterministic repairs applied while compiling (role alias resolution
    /// and similar). Surfaced in the orchestration receipt for audit.
    pub repairs: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct CompiledGraphMutation {
    pub nodes: Vec<ExecutionNodeSpec>,
    pub edges: Vec<ExecutionEdge>,
    pub semantic_node_instances: BTreeMap<String, Vec<String>>,
}

/// Compile a new semantic Mission root. Model input contains no executor,
/// lease, system path, or mutable runtime identity.
pub fn compile_orchestration(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    plan: &RuntimeOrchestrationPlan,
    parent_execution: Option<ExecutionParentBinding>,
    team_runtime: Option<&TeamRuntime>,
) -> Result<CompiledOrchestration, OrchestrationCompileError> {
    if request.operation != RuntimeOrchestrationOperation::Propose {
        return Err(OrchestrationCompileError::OperationDoesNotCompile(
            request.operation.as_str(),
        ));
    }
    let proposal = request
        .proposal
        .as_ref()
        .ok_or(OrchestrationCompileError::MissingProposal)?;
    let lineage = request.lineage.clone().ok_or_else(|| {
        OrchestrationCompileError::InvalidProposal(
            "orchestration proposal requires canonical execution lineage".to_string(),
        )
    })?;
    lineage
        .validate()
        .map_err(|error| OrchestrationCompileError::InvalidProposal(error.to_string()))?;
    if request.mission_id.as_deref().is_none_or(str::is_empty) {
        return Err(OrchestrationCompileError::InvalidProposal(
            "orchestration proposal requires canonical mission_id".to_string(),
        ));
    }
    let graph_id = format!("mission-graph:{}", proposal.mutation_id);
    let mut repairs = Vec::new();
    let compiled = compile_graph_mutation(
        request_id,
        request,
        plan,
        proposal,
        &graph_id,
        parent_execution.as_ref(),
        team_runtime.ok_or(OrchestrationCompileError::TeamRuntimeRequired)?,
        &BTreeMap::new(),
        &mut repairs,
    )?;
    let mut graph = ExecutionGraph::new(request.intent.clone());
    graph.id = graph_id;
    graph.parent_execution = parent_execution;
    graph.lineage = Some(lineage);
    if graph.parent_execution.is_some() {
        graph.service_class = harness_contract::execution_graph::ExecutionServiceClass::Foreground;
    }
    graph.nodes = compiled.nodes;
    graph.edges = compiled.edges;
    let collaboration_program =
        collaboration_program_from_proposal(proposal, Some(&compiled.semantic_node_instances))?;
    graph.orchestration = Some(ExecutionOrchestrationMetadata {
        mutation_id: proposal.mutation_id.clone(),
        applied_mutation_ids: vec![proposal.mutation_id.clone()],
        semantic_revision: 1,
        source_generation: 1,
        completion: materialize_completion(
            &proposal.completion,
            &compiled.semantic_node_instances,
            &proposal.nodes,
        ),
        collaboration_program,
    });
    apply_strategy_estimates(&mut graph, plan);
    validate_execution_graph(&graph)
        .map_err(|error| OrchestrationCompileError::InvalidProposal(error.to_string()))?;
    let work_estimate = estimate_work_graph(&graph, plan, proposal);
    ensure_positive_work_lift(&graph, &work_estimate)?;
    Ok(CompiledOrchestration {
        graph,
        command: ExecutionGraphCommand::Start {
            expected_revision: 0,
        },
        execute_without_protocol: true,
        team_request: None,
        work_estimate,
        repairs,
    })
}

/// Derive the immutable collaboration obligations from the semantic proposal.
///
/// The runtime accepts arbitrary validated Team topology, but it never lets a
/// Team count live only in prose or in a transient Host prompt.  A graph with
/// Team nodes therefore carries one durable program descriptor.  This is a
/// compiler artifact, not another scheduler or a role-name convention.
pub(crate) fn collaboration_program_from_proposal(
    proposal: &GraphMutationProposal,
    semantic_node_instances: Option<&BTreeMap<String, Vec<String>>>,
) -> Result<Option<CollaborationProgram>, OrchestrationCompileError> {
    let team_nodes = proposal
        .nodes
        .iter()
        .filter(|node| node.recipe == CapabilityRecipeId::Team)
        .collect::<Vec<_>>();
    if team_nodes.is_empty() {
        if proposal.collaboration_program.is_some() {
            return Err(OrchestrationCompileError::InvalidProposal(
                "collaboration program cannot exist without Team nodes".to_string(),
            ));
        }
        return Ok(None);
    }

    let instances = team_nodes
        .iter()
        .flat_map(|node| {
            (0..node.multiplicity).map(move |index| CollaborationTeamInstance {
                instance_id: format!("{}:{}", node.node_id, index.saturating_add(1)),
                semantic_node_id: node.node_id.clone(),
                required: node.required,
            })
        })
        .collect::<Vec<_>>();
    let mut edges = Vec::new();
    for consumer in &team_nodes {
        for producer_id in &consumer.depends_on {
            let Some(producer) = team_nodes.iter().find(|node| node.node_id == *producer_id) else {
                // Dependencies on a non-Team semantic node remain ordinary
                // graph edges; they are not cross-Team handoffs.
                continue;
            };
            for from_index in 0..producer.multiplicity {
                for to_index in 0..consumer.multiplicity {
                    let from = format!("{}:{}", producer.node_id, from_index.saturating_add(1));
                    let to = format!("{}:{}", consumer.node_id, to_index.saturating_add(1));
                    edges.push(CollaborationProgramEdge {
                        edge_id: format!("{from}->{to}"),
                        from,
                        to,
                        kind: CollaborationEdgeKind::Handoff,
                        input_contract: Default::default(),
                        state: Default::default(),
                    });
                }
            }
        }
    }
    let derived = CollaborationProgram {
        program_id: format!("collaboration-program:{}", proposal.mutation_id),
        revision: 1,
        required_team_count: u16::try_from(instances.iter().filter(|team| team.required).count())
            .map_err(|_| {
            OrchestrationCompileError::InvalidProposal(
                "collaboration program Team count exceeds u16".to_string(),
            )
        })?,
        team_instances: instances,
        edges,
        semantic_node_instances: semantic_node_instances.map_or_else(BTreeMap::new, |instances| {
            team_nodes
                .iter()
                .filter_map(|node| {
                    instances
                        .get(&node.node_id)
                        .cloned()
                        .map(|physical| (node.node_id.clone(), physical))
                })
                .collect()
        }),
        control: Default::default(),
    };
    derived
        .validate()
        .map_err(OrchestrationCompileError::InvalidProposal)?;

    if let Some(program) = proposal.collaboration_program.as_ref() {
        program
            .validate()
            .map_err(OrchestrationCompileError::InvalidProposal)?;
        let actual = program
            .team_instances
            .iter()
            .map(|instance| (&instance.instance_id, &instance.semantic_node_id))
            .collect::<BTreeSet<_>>();
        let expected = derived
            .team_instances
            .iter()
            .map(|instance| (&instance.instance_id, &instance.semantic_node_id))
            .collect::<BTreeSet<_>>();
        if actual != expected {
            return Err(OrchestrationCompileError::InvalidProposal(
                "collaboration program Team instances do not match the semantic Team nodes"
                    .to_string(),
            ));
        }
        return Ok(Some(program.clone()));
    }
    Ok(Some(derived))
}

pub(crate) fn estimate_work_graph(
    graph: &ExecutionGraph,
    plan: &RuntimeOrchestrationPlan,
    proposal: &GraphMutationProposal,
) -> ModelWorkEstimate {
    let resources = &plan.execution_decision.strategy.resource_snapshot;
    ModelWorkGraphEstimator.estimate(
        graph,
        &ModelWorkEstimateInput {
            provider_effective_limit: usize::from(resources.provider_effective_limit),
            provider_available: usize::from(resources.provider_concurrency),
            tool_available: usize::from(resources.tool_concurrency),
            agent_available: usize::from(resources.team_slots),
            provider_queue_p95_ms: resources.provider_queue_p95_ms,
            provider_service_p95_ms: resources.provider_service_p95_ms,
            provider_failure_timeout_upper_bound_basis_points: resources
                .provider_failure_timeout_upper_bound_bp,
            provider_samples: resources.sample_count as usize,
            requires_cross_check: proposal
                .nodes
                .iter()
                .any(|node| node.recipe == CapabilityRecipeId::Review),
            ..ModelWorkEstimateInput::default()
        },
    )
}

pub(crate) fn ensure_positive_work_lift(
    graph: &ExecutionGraph,
    estimate: &ModelWorkEstimate,
) -> Result<(), OrchestrationCompileError> {
    if estimate.automatic
        && estimate.topology == crate::execution_core::ModelWorkTopology::Downgraded
        && harness_contract::execution_graph::project_work_graph(graph)
            .is_some_and(|work| work.width > 1)
    {
        return Err(OrchestrationCompileError::InvalidProposal(format!(
            "model_work_negative_lift:{}",
            estimate.reasons.join(",")
        )));
    }
    Ok(())
}

pub(crate) fn apply_strategy_estimates(
    graph: &mut ExecutionGraph,
    plan: &RuntimeOrchestrationPlan,
) {
    let selected = plan
        .execution_decision
        .strategy
        .candidate_estimates
        .iter()
        .find(|estimate| estimate.candidate == plan.execution_decision.strategy.selected_candidate);
    let Some(estimate) = selected else {
        return;
    };
    let count = graph.nodes.len().max(1) as u64;
    let duration_per_node = estimate.estimated_serial_ms.saturating_div(count);
    let input_tokens_per_node = estimate.context_duplication_tokens.saturating_div(count);
    for node in &mut graph.nodes {
        let Some(work) = node.work.as_mut() else {
            continue;
        };
        work.expected_duration_ms = if work.role == ExecutionWorkRole::Synthesize {
            estimate.merge_cost_ms.max(duration_per_node)
        } else {
            duration_per_node
        };
        work.expected_input_tokens = input_tokens_per_node;
    }
}

pub fn compile_graph_mutation(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    plan: &RuntimeOrchestrationPlan,
    proposal: &GraphMutationProposal,
    graph_id: &str,
    root_parent: Option<&ExecutionParentBinding>,
    team_runtime: &TeamRuntime,
    existing_semantic_node_instances: &BTreeMap<String, Vec<String>>,
    repairs: &mut Vec<String>,
) -> Result<CompiledGraphMutation, OrchestrationCompileError> {
    let semantic_ids = proposal
        .nodes
        .iter()
        .map(|node| node.node_id.clone())
        .collect::<BTreeSet<_>>();
    let mut semantic_node_instances = BTreeMap::<String, Vec<String>>::new();
    let mut nodes = Vec::new();
    for semantic in &proposal.nodes {
        let instances = (0..semantic.multiplicity)
            .map(|index| physical_node_id(graph_id, &semantic.node_id, index))
            .collect::<Vec<_>>();
        semantic_node_instances.insert(semantic.node_id.clone(), instances.clone());
        for (index, node_id) in instances.into_iter().enumerate() {
            nodes.push(compile_semantic_node(
                request_id,
                request,
                plan,
                semantic,
                index,
                graph_id,
                &node_id,
                root_parent,
                team_runtime,
                repairs,
            )?);
        }
    }
    let mut edges = Vec::new();
    for semantic in &proposal.nodes {
        let consumers = &semantic_node_instances[&semantic.node_id];
        for dependency in &semantic.depends_on {
            let providers = semantic_node_instances
                .get(dependency)
                .cloned()
                .or_else(|| existing_semantic_node_instances.get(dependency).cloned())
                .ok_or_else(|| {
                    OrchestrationCompileError::InvalidProposal(format!(
                        "semantic dependency `{dependency}` is absent"
                    ))
                })?;
            for provider in &providers {
                for consumer in consumers {
                    edges.push(ExecutionEdge {
                        from: provider.clone(),
                        to: consumer.clone(),
                        kind: ExecutionEdgeKind::DependsOn,
                    });
                    if proposal
                        .nodes
                        .iter()
                        .find(|node| node.node_id == *dependency)
                        .is_some_and(|node| !node.output_artifacts.is_empty())
                    {
                        edges.push(ExecutionEdge {
                            from: provider.clone(),
                            to: consumer.clone(),
                            kind: ExecutionEdgeKind::Produces,
                        });
                    }
                }
            }
        }
    }
    if semantic_ids.is_empty() {
        return Err(OrchestrationCompileError::InvalidProposal(
            "semantic mutation contains no nodes".to_string(),
        ));
    }
    Ok(CompiledGraphMutation {
        nodes,
        edges,
        semantic_node_instances,
    })
}

#[allow(clippy::too_many_arguments)]
fn compile_semantic_node(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    plan: &RuntimeOrchestrationPlan,
    semantic: &GraphSemanticNode,
    instance_index: usize,
    graph_id: &str,
    node_id: &str,
    root_parent: Option<&ExecutionParentBinding>,
    team_runtime: &TeamRuntime,
    repairs: &mut Vec<String>,
) -> Result<ExecutionNodeSpec, OrchestrationCompileError> {
    let mut node = match semantic.recipe {
        CapabilityRecipeId::Team => compile_team_subgraph_node(
            request_id,
            request,
            plan,
            semantic,
            instance_index,
            graph_id,
            node_id,
            root_parent,
            team_runtime,
            repairs,
        ),
        CapabilityRecipeId::SessionDispatch => {
            compile_session_dispatch_node(request_id, request, semantic, node_id)
        }
        CapabilityRecipeId::Agent
        | CapabilityRecipeId::Review
        | CapabilityRecipeId::Synthesis => {
            compile_agent_node(
                request,
                semantic,
                instance_index,
                graph_id,
                node_id,
            )
        }
        CapabilityRecipeId::Direct => Err(OrchestrationCompileError::InvalidProposal(
            "direct work belongs to the current model turn and cannot become a stateful child graph"
                .to_string(),
        )),
    }?;
    let mut work = ExecutionWorkContract::new(match semantic.recipe {
        CapabilityRecipeId::Agent | CapabilityRecipeId::Team => ExecutionWorkRole::EvidenceAnalyze,
        CapabilityRecipeId::Review => ExecutionWorkRole::CrossCheck,
        CapabilityRecipeId::Synthesis => ExecutionWorkRole::Synthesize,
        CapabilityRecipeId::SessionDispatch | CapabilityRecipeId::Direct => ExecutionWorkRole::Plan,
    });
    work.required = semantic.required;
    work.dependency = semantic.dependency.clone();
    work.cancellation_group = semantic.cancellation_group.clone();
    work.required_evidence_refs = semantic.required_evidence_refs.clone();
    work.dependency = default_review_dependency(
        semantic.recipe,
        work.dependency,
        &work.required_evidence_refs,
        !semantic.depends_on.is_empty(),
    );
    node.work = Some(work);
    Ok(node)
}

/// Reviewer/CrossCheck edges consume terminal predecessor facts, not raw
/// execution status. A failed or FrameworkInvalid predecessor with durable
/// evidence stays reviewable; only the typed predicate decides readiness.
fn default_review_dependency(
    recipe: CapabilityRecipeId,
    dependency: ExecutionDependencyPolicy,
    required_evidence_refs: &[String],
    has_predecessors: bool,
) -> ExecutionDependencyPolicy {
    if recipe == CapabilityRecipeId::Review
        && dependency == ExecutionDependencyPolicy::All
        && required_evidence_refs.is_empty()
        && has_predecessors
    {
        return ExecutionDependencyPolicy::EvidenceReady {
            predicate: DependencyPredicate::EvidenceReady {
                minimum: 1,
                required_fact_kinds: vec![
                    TerminalFactKind::ObservedEvidence,
                    TerminalFactKind::AcceptanceVerdict,
                ],
                accepted_execution_statuses: vec![
                    ExecutionNodeStatus::Failed,
                    ExecutionNodeStatus::Completed,
                ],
                accepted_acceptance_verdicts: vec![
                    AcceptanceVerdict::Satisfied,
                    AcceptanceVerdict::Unsatisfied,
                    AcceptanceVerdict::FrameworkInvalid,
                ],
                require_committed_effect: false,
            },
            cancel_remaining: false,
        };
    }
    dependency
}

#[allow(clippy::too_many_arguments)]
fn compile_team_subgraph_node(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    plan: &RuntimeOrchestrationPlan,
    semantic: &GraphSemanticNode,
    instance_index: usize,
    graph_id: &str,
    node_id: &str,
    _root_parent: Option<&ExecutionParentBinding>,
    team_runtime: &TeamRuntime,
    repairs: &mut Vec<String>,
) -> Result<ExecutionNodeSpec, OrchestrationCompileError> {
    let requested_template = semantic
        .template
        .as_deref()
        .unwrap_or_else(|| plan.collaboration_decision.template_id.template_path());
    let mut template_path = requested_template.to_string();
    if template_path.contains("direct-executor") {
        // Framework rule: Team proposals must use a collaboration template.
        // Auto-bind the strategy-recommended template and record the repair
        // instead of letting a non-collaboration template be rejected later.
        template_path = plan
            .collaboration_decision
            .template_id
            .template_path()
            .to_string();
        repairs.push(format!(
            "template_bind:{requested_template}:{template_path}"
        ));
    }
    let trimmed = template_path.trim();
    let (scope, local) = if let Some(local) = trimmed.strip_prefix("workspace/") {
        (harness_contract::agent::DefinitionScope::Workspace, local)
    } else if let Some(local) = trimmed.strip_prefix("user/") {
        (harness_contract::agent::DefinitionScope::User, local)
    } else {
        (
            harness_contract::agent::DefinitionScope::Builtin,
            trimmed.strip_prefix("builtin/").unwrap_or(trimmed),
        )
    };
    let template_id = TeamTemplateDefinitionId::new(scope, local)
        .map_err(|error| OrchestrationCompileError::TeamInstantiation(error.to_string()))?;
    let template_selector = request
        .ephemeral_team_templates
        .get(&semantic.node_id)
        .cloned()
        .map(|snapshot| TeamTemplateSelector::Ephemeral {
            snapshot: Box::new(snapshot),
        })
        .unwrap_or(TeamTemplateSelector::LatestStable {
            template_id: template_id.clone(),
        });
    let team_id = format!(
        "runtime-team:{}:{}:{}",
        request_id, semantic.node_id, instance_index
    );
    // This provisional value only lets `TeamRuntime::plan` resolve the
    // published template.  The durable Team request is rebuilt below from
    // the resolved topology, rather than from a fixed one-Team timeout.
    let deadline_at_ms = crate::tool_invocation::now_ms()
        .saturating_add(harness_contract::agent::DEFAULT_DELEGATED_EXECUTION_TIMEOUT_MS);
    tracing::debug!(
        permission_ceiling = ?request.constraints.permission_ceiling,
        scopes = ?semantic.resource_scopes,
        template = ?match &template_selector {
            TeamTemplateSelector::Ephemeral { snapshot } => snapshot.revision.revision_ref.template_id.as_str(),
            _ => template_id.as_str(),
        },
        "team orchestration compile scopes"
    );
    let team_request = TeamInstantiationRequest {
        request_id: format!("{}:{}:{}", request_id, semantic.node_id, instance_index),
        team_id: team_id.clone(),
        mission_id: request.mission_id.clone().ok_or_else(|| {
            OrchestrationCompileError::InvalidProposal(
                "Team orchestration requires mission_id".to_string(),
            )
        })?,
        lineage: request.lineage.as_ref().cloned().ok_or_else(|| {
            OrchestrationCompileError::InvalidProposal(
                "Team orchestration requires canonical execution lineage".to_string(),
            )
        })?,
        display_name: None,
        role_display_overrides: Vec::new(),
        parent_execution: Some(ExecutionParentBinding {
            execution_id: graph_id.to_string(),
            node_id: node_id.to_string(),
        }),
        selection_mode: TeamSelectionMode::ModelAssisted,
        strategy_binding: request.strategy_binding.clone(),
        template_selector,
        objective: bounded_team_node_objective(semantic),
        acceptance: semantic.evidence_contract.clone(),
        risk: None,
        role_binding_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: focus_partition_plans(semantic, repairs),
        permission_ceiling: request.constraints.permission_ceiling,
        model_lease: request
            .model_lease
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        execution_budget: adaptive_team_execution_budget(
            format!("runtime-team-budget:{node_id}"),
            request,
            semantic,
            deadline_at_ms,
            request.constraints.max_parallel_agents.unwrap_or(32),
            1,
        ),
        deadline_at_ms,
        managed_invocation: None,
        resource_scopes: semantic.resource_scopes.clone(),
        allow_whole_workspace_scope: request
            .constraints
            .permission_ceiling
            .permits(harness_contract::policy::PermissionMode::DangerFullAccess),
        upstream_evidence_refs: Vec::new(),
        upstream_artifact_refs: Vec::new(),
    };
    // The first plan resolves the published template and exposes the real
    // role-branch cardinality. A team with N template roles needs a predicted
    // budget sized for N branches; deriving it from `semantic.multiplicity`
    // alone (1 for a custom template) under-provisions the prediction that is
    // shown to operators. These values meter usage only; they never admit or
    // reject provider IO.
    let role_branch_count = team_runtime
        .plan(team_request.clone())
        .map_err(OrchestrationCompileError::TeamInstantiation)?
        .role_slots
        .len()
        .max(1);
    let mut team_request = team_request;
    // The model may propose a narrower parallelism hint.  When it does not,
    // the only useful default is the resolved Team topology itself.  Global
    // tenant/device capacity is deliberately *not* duplicated here: the
    // ResourceManager remains its single admission owner.
    let max_parallel = request
        .constraints
        .max_parallel_agents
        .unwrap_or(role_branch_count)
        .max(1);
    // A Team deadline is a durable liveness budget, not a template-specific
    // fixed timeout and not an unbounded sentinel.  It expands with the
    // actual number of scheduled waves and the contract's verification work.
    // This preserves room for genuinely complex, serialized work while
    // guaranteeing that a stalled Team reaches a typed terminal state.
    let scaled_deadline = adaptive_team_deadline_at_ms(
        role_branch_count,
        max_parallel,
        !semantic.evidence_contract.is_empty(),
    );
    team_request.deadline_at_ms = scaled_deadline;
    team_request.execution_budget = adaptive_team_execution_budget(
        format!("runtime-team-budget:{node_id}"),
        request,
        semantic,
        scaled_deadline,
        max_parallel,
        role_branch_count,
    );
    team_runtime
        .plan(team_request.clone())
        .map_err(OrchestrationCompileError::TeamInstantiation)?;
    let payload_ref = serde_json::to_string(&team_request)
        .map_err(|error| OrchestrationCompileError::InvalidProposal(error.to_string()))?;
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::Subgraph,
        TEAM_SUBGRAPH_EXECUTOR,
        payload_ref,
    );
    node.id = node_id.to_string();
    node.idempotency_key = format!("{}:{}", request_id, node_id);
    node.resource_scopes = semantic.resource_scopes.clone();
    node.acceptance.criteria = semantic.evidence_contract.clone();
    node.acceptance.required_evidence = semantic.output_artifacts.clone();
    Ok(node)
}

/// Derive a liveness window from the *compiled* Team topology.
///
/// A Team may contain independent branches, serialized waves, and a final
/// verification/synthesis phase.  Each of those phases receives the normal
/// Runtime-issued delegated-work window.  This is a complexity prediction,
/// not a provider billing limit; live capacity remains governed by the
/// ResourceManager and the user can still pause/cancel/finalize the graph.
fn adaptive_team_deadline_at_ms(
    role_branch_count: usize,
    max_parallel: usize,
    requires_verification: bool,
) -> u64 {
    let branches = role_branch_count.max(1);
    let lanes = max_parallel.max(1);
    let execution_waves = branches.div_ceil(lanes);
    // One terminal synthesis phase always follows the Agent waves.  A
    // contract with evidence also reserves a verification phase.
    let phases = execution_waves
        .saturating_add(1)
        .saturating_add(usize::from(requires_verification));
    let duration = harness_contract::agent::DEFAULT_DELEGATED_EXECUTION_TIMEOUT_MS
        .saturating_mul(u64::try_from(phases).unwrap_or(u64::MAX));
    crate::tool_invocation::now_ms().saturating_add(duration)
}

fn focus_partition_plans(
    semantic: &GraphSemanticNode,
    _repairs: &mut Vec<String>,
) -> Vec<FocusPartitionPlan> {
    let mut scope_use_counts = BTreeMap::<String, usize>::new();
    for focus in &semantic.focuses {
        let mut scopes = focus.resource_scopes.clone();
        scopes.sort();
        scopes.dedup();
        for scope in scopes {
            *scope_use_counts.entry(scope).or_default() += 1;
        }
    }
    let mut by_role = BTreeMap::<String, Vec<FocusPartitionSlot>>::new();
    for focus in &semantic.focuses {
        // Role alias repair happens inside TeamInstantiation where the
        // template's exact legal roles are known. Rewriting here would break
        // templates whose legal role id equals a common alias (e.g. a
        // `synthesizer` template vs the `decision_synthesis` alias).
        let role_id = focus.role_id.clone();
        let mut scopes = focus.resource_scopes.clone();
        scopes.sort();
        scopes.dedup();
        let shares_infrastructure_scope = scopes
            .iter()
            .any(|scope| scope_use_counts.get(scope).copied().unwrap_or_default() > 1);
        by_role
            .entry(role_id.clone())
            .or_default()
            .push(FocusPartitionSlot {
                focus_id: focus.focus_id.clone(),
                scope_hash: harness_contract::team::focus_scope_hash(
                    &role_id,
                    &focus.objective,
                    &scopes,
                ),
                boundary: focus.objective.clone(),
                evidence_responsibility: focus.evidence_responsibilities.join("; "),
                capability_cropped_refs: scopes,
                // Identical Runtime-cropped refs mean that workers share an
                // infrastructure lease (for example `network:*`). Their
                // semantic boundaries and evidence responsibilities remain
                // distinct, so the shared transport must not be rejected as
                // duplicated business work.
                overlap_budget_bp: if shares_infrastructure_scope {
                    10_000
                } else {
                    0
                },
                novelty_target_bp: 2_500,
                output_contract: if focus.output_contract.is_empty() {
                    semantic.output_artifacts.clone()
                } else {
                    focus.output_contract.clone()
                },
                output_acceptance: if focus.output_acceptance.is_empty() {
                    semantic.evidence_contract.clone()
                } else {
                    focus.output_acceptance.clone()
                },
            });
    }
    by_role
        .into_iter()
        .map(|(role_id, slots)| FocusPartitionPlan {
            role_id,
            shared_baseline: vec![bounded_team_node_objective(semantic)],
            slots,
        })
        .collect()
}

fn bounded_team_node_objective(semantic: &GraphSemanticNode) -> String {
    let mut focuses = semantic
        .focuses
        .iter()
        .map(|focus| focus.objective.trim())
        .filter(|objective| !objective.is_empty())
        .collect::<Vec<_>>();
    focuses.sort_unstable();
    focuses.dedup();
    let focus_text = if focuses.is_empty() {
        "Use only the Runtime-assigned resource scopes and evidence contract for this Team node."
            .to_string()
    } else {
        focuses.join("; ")
    };
    format!(
        "Bounded Team node `{}`. Complete only these assigned focuses: {} Parent and peer-Team objectives are intentionally outside this Team's authority.",
        semantic.node_id, focus_text
    )
}

fn compile_agent_node(
    request: &RuntimeOrchestrationCommand,
    semantic: &GraphSemanticNode,
    instance_index: usize,
    graph_id: &str,
    node_id: &str,
) -> Result<ExecutionNodeSpec, OrchestrationCompileError> {
    let focus = semantic.focuses.get(instance_index);
    let objective = focus
        .map(|focus| focus.objective.clone())
        .unwrap_or_else(|| semantic.objective.clone());
    let mut resource_scopes = semantic.resource_scopes.clone();
    if let Some(focus) = focus {
        resource_scopes.extend(focus.resource_scopes.iter().cloned());
    }
    resource_scopes.sort();
    resource_scopes.dedup();
    let allowed_tools = request
        .capabilities
        .iter()
        .filter_map(|capability| capability.strip_prefix("tool:").map(str::to_string))
        .collect::<Vec<_>>();
    let granted_capabilities = if !semantic.required {
        vec![AgentCapability::Read, AgentCapability::Search]
    } else if request
        .constraints
        .permission_ceiling
        .permits(harness_contract::policy::PermissionMode::WorkspaceWrite)
    {
        vec![
            AgentCapability::Read,
            AgentCapability::Search,
            AgentCapability::Write,
            AgentCapability::Test,
        ]
    } else {
        vec![AgentCapability::Read, AgentCapability::Search]
    };
    let deadline_at_ms = crate::tool_invocation::now_ms()
        .saturating_add(harness_contract::agent::DEFAULT_DELEGATED_EXECUTION_TIMEOUT_MS);
    let budget_tokens = adaptive_subagent_budget(request);
    let intent = AgentTaskIntent {
        selected_agent_id: semantic.template.clone(),
        definition_ref: None,
        granted_capabilities,
        principal_id: "runtime-orchestrator".to_string(),
        source_turn_id: request
            .session_id
            .clone()
            .unwrap_or_else(|| graph_id.to_string()),
        run_id: format!("{node_id}:run"),
        task_id: format!("{node_id}:task"),
        root_task_id: request
            .lineage
            .as_ref()
            .map(|lineage| lineage.root_task_id.clone())
            .ok_or_else(|| {
                OrchestrationCompileError::InvalidProposal(
                    "Agent orchestration requires root Task lineage".to_string(),
                )
            })?,
        parent_task_id: request
            .lineage
            .as_ref()
            .map(|lineage| lineage.task_id.clone()),
        session_id: request.session_id.clone().unwrap_or_default(),
        mission_id: request.mission_id.clone().ok_or_else(|| {
            OrchestrationCompileError::InvalidProposal(
                "Agent orchestration requires mission_id".to_string(),
            )
        })?,
        team_id: None,
        team_role_identity: None,
        graph_id: graph_id.to_string(),
        node_id: node_id.to_string(),
        attempt: 1,
        expected_graph_revision: 0,
        objective,
        required_acceptance: harness_contract::context::RequiredAcceptance {
            criteria: semantic.evidence_contract.clone(),
            evidence_obligations: Vec::new(),
        },
        output_acceptance: Vec::new(),
        acceptance: semantic.evidence_contract.clone(),
        // The bounded objective, resource scopes and typed acceptance are
        // already persisted fields.  Do not smuggle semantic focus through
        // free-form constraints that a restarted worker may parse differently.
        constraints: Vec::new(),
        context_refs: resolved_context_refs(request, semantic),
        evidence_refs: Vec::new(),
        resource_scopes: resource_scopes.clone(),
        allowed_tools,
        allowed_skills: Vec::new(),
        permission_ceiling: if semantic.required {
            request.constraints.permission_ceiling
        } else {
            harness_contract::policy::PermissionMode::ReadOnly
        },
        model_lease: request
            .model_lease
            .clone()
            .unwrap_or_else(|| "default".to_string()),
        budget_lease: ChildExecutionBudgetReservation::single(
            format!("runtime-agent-budget:{node_id}"),
            node_id,
            "runtime_agent",
            budget_tokens,
            deadline_at_ms,
            1,
        ),
        deadline_at_ms,
        managed_invocation: None,
        idempotency_key: format!("runtime-agent:{node_id}"),
    };
    let payload_ref = serde_json::to_string(&intent)
        .map_err(|error| OrchestrationCompileError::InvalidProposal(error.to_string()))?;
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        AgentTaskExecutor::KIND,
        payload_ref,
    );
    node.id = node_id.to_string();
    node.idempotency_key = format!("runtime-agent:{node_id}");
    node.resource_scopes = resource_scopes;
    node.acceptance.criteria = semantic.evidence_contract.clone();
    node.acceptance.required = intent.required_acceptance.clone();
    node.acceptance.required_evidence = semantic.output_artifacts.clone();
    Ok(node)
}

fn compile_session_dispatch_node(
    request_id: &str,
    request: &RuntimeOrchestrationCommand,
    semantic: &GraphSemanticNode,
    node_id: &str,
) -> Result<ExecutionNodeSpec, OrchestrationCompileError> {
    let source_session_id = request
        .session_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            OrchestrationCompileError::InvalidProposal(
                "session dispatch requires the active source session".to_string(),
            )
        })?;
    let target_session_id = semantic
        .target_session_id
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            OrchestrationCompileError::InvalidProposal(
                "session dispatch requires target_session_id".to_string(),
            )
        })?;
    let handoff = harness_contract::turn::SessionHandoff {
        handoff_id: format!("runtime-handoff:{request_id}:{}", semantic.node_id),
        source_session_id: source_session_id.to_string(),
        target_session_id: target_session_id.to_string(),
        objective: semantic.objective.clone(),
        acceptance: semantic.evidence_contract.clone(),
        scope: semantic.resource_scopes.clone(),
        context_lens: resolved_context_refs(request, semantic),
        evidence_refs: request
            .evidence_refs
            .iter()
            .cloned()
            .map(|reference| {
                harness_contract::turn::opaque_session_evidence_ref(source_session_id, reference)
            })
            .collect(),
        context_budget_lease: None,
        permission_ceiling: request.constraints.permission_ceiling,
        deadline_at_ms: None,
        priority: 128,
        correlation_id: format!("runtime-handoff-correlation:{request_id}"),
        result_contract: semantic.output_artifacts.join(","),
        task_route_hint: Some(harness_contract::task::TaskRouteHint {
            task_id: request
                .lineage
                .as_ref()
                .map(|lineage| lineage.root_task_id.clone()),
            mission_id: request.mission_id.clone(),
            handoff_id: Some(format!("runtime-handoff-correlation:{request_id}")),
            compound_objectives: Vec::new(),
        }),
    };
    let command = harness_contract::turn::SessionDispatchCommand {
        command_id: format!("runtime-dispatch:{request_id}:{}", semantic.node_id),
        action: harness_contract::turn::SessionDispatchAction::Enqueue,
        handoff,
        expected_target_revision: 0,
    };
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::SessionDispatch,
        crate::SESSION_DISPATCH_EXECUTOR,
        serde_json::to_string(&command)
            .map_err(|error| OrchestrationCompileError::InvalidProposal(error.to_string()))?,
    );
    node.id = node_id.to_string();
    node.idempotency_key = command.command_id;
    node.acceptance.criteria = semantic.evidence_contract.clone();
    Ok(node)
}

fn resolved_context_refs(
    request: &RuntimeOrchestrationCommand,
    semantic: &GraphSemanticNode,
) -> Vec<String> {
    let mut refs = request.evidence_refs.clone();
    refs.extend(semantic.required_evidence_refs.iter().cloned());
    refs.sort();
    refs.dedup();
    refs
}

pub(crate) fn materialize_completion(
    completion: &harness_contract::execution_graph::ExecutionCompletionContract,
    instances: &BTreeMap<String, Vec<String>>,
    semantic_nodes: &[GraphSemanticNode],
) -> harness_contract::execution_graph::ExecutionCompletionContract {
    let mut materialized = completion.clone();
    let required_semantic = semantic_nodes
        .iter()
        .filter(|node| node.required)
        .map(|node| node.node_id.as_str())
        .collect::<BTreeSet<_>>();
    materialized.required_node_ids = if completion.required_node_ids.is_empty() {
        instances
            .iter()
            .filter(|(semantic, _)| required_semantic.contains(semantic.as_str()))
            .flat_map(|(_, physical)| physical.iter().cloned())
            .collect()
    } else {
        completion
            .required_node_ids
            .iter()
            .filter(|semantic| required_semantic.contains(semantic.as_str()))
            .flat_map(|semantic| {
                instances
                    .get(semantic)
                    .cloned()
                    .unwrap_or_else(|| vec![semantic.clone()])
            })
            .collect()
    };
    materialized.required_node_ids.sort();
    materialized.required_node_ids.dedup();
    // Required artifact kinds must be reachable by an authorized node. A model
    // proposal may over-declare artifacts (for example a `*.json` terminal
    // file) that no Team node's resource lease can actually write; keeping
    // them in the completion contract makes the graph unsatisfiable even when
    // every role delivered its real output. Path-like artifacts declared by a
    // required Team node are retained only when at least one Team write scope
    // covers them. Generic kinds (`terminal_synthesis`, `workspace_change`)
    // and non-Team node artifacts keep the established semantics.
    let mut retainable_artifacts = BTreeSet::<String>::new();
    for node in semantic_nodes.iter().filter(|node| node.required) {
        if node.recipe == CapabilityRecipeId::Team {
            let write_paths = node
                .resource_scopes
                .iter()
                .filter_map(|scope| scope.strip_prefix("write:"))
                .map(|path| path.trim_end_matches('/'))
                .collect::<BTreeSet<_>>();
            for artifact in &node.output_artifacts {
                // Generic kinds and artifacts covered by the Team's write
                // lease stay; path-like artifacts the Team cannot write are
                // dropped so the completion contract stays satisfiable.
                let path_like = artifact.contains('.');
                let covered = write_paths.iter().any(|path| {
                    artifact == path
                        || artifact.strip_prefix("./") == Some(path)
                        || path.strip_prefix("./") == Some(artifact.as_str())
                });
                if !path_like || covered {
                    retainable_artifacts.insert(artifact.clone());
                }
            }
        } else {
            // Non-Team node artifacts preserve the established semantics.
            retainable_artifacts.extend(node.output_artifacts.iter().cloned());
        }
    }
    if !retainable_artifacts.is_empty() {
        materialized
            .required_artifact_kinds
            .retain(|artifact| retainable_artifacts.contains(artifact));
    }
    materialized
}

fn adaptive_subagent_budget(request: &RuntimeOrchestrationCommand) -> u64 {
    adaptive_runtime_budget_plan(request, 1, 0).subagent_default_budget
}

fn adaptive_team_execution_budget(
    budget_id: impl Into<String>,
    request: &RuntimeOrchestrationCommand,
    semantic: &GraphSemanticNode,
    deadline_at_ms: u64,
    max_parallel: usize,
    expected_parallel_branches: usize,
) -> harness_contract::context::ParentExecutionBudget {
    let expected_verification_passes = usize::from(!semantic.evidence_contract.is_empty());
    let plan = adaptive_runtime_budget_plan(
        request,
        expected_parallel_branches,
        expected_verification_passes,
    );
    // Record-only meter: the ceiling is the complexity prediction, never a
    // window-derived cumulative admission cap.
    crate::team_instantiation::bounded_parent_execution_budget(
        budget_id,
        plan.team_total_budget.max(1),
        deadline_at_ms,
        max_parallel,
    )
    .with_prediction(plan.team_total_budget)
}

fn adaptive_runtime_budget_plan(
    request: &RuntimeOrchestrationCommand,
    expected_parallel_branches: usize,
    expected_verification_passes: usize,
) -> crate::budget_policy::RuntimeBudgetPlan {
    let model = request.model_lease.as_deref().unwrap_or("unknown");
    let context_window = provider::model_context_window(model);
    let max_output = provider::model_max_output_resolution(model, None).tokens;
    crate::budget_policy::RuntimeBudgetPlan::derive(crate::budget_policy::RuntimeBudgetInputs {
        model_context_window: context_window,
        model_max_output_tokens: max_output,
        subsystem_budget_ratio_bp: crate::budget_policy::DEFAULT_SUBSYSTEM_BUDGET_RATIO_BP,
        profile: crate::context_runtime::ContextProfile::SubAgent,
        autonomy_mode: None,
        expected_parallel_branches,
        expected_verification_passes,
    })
}

fn physical_node_id(graph_id: &str, semantic_id: &str, index: u16) -> String {
    format!(
        "{graph_id}:semantic:{semantic_id}:{}",
        usize::from(index) + 1
    )
}

#[must_use]
pub fn guidance_for_compile_result(compiled: bool) -> String {
    if compiled {
        "The semantic proposal was compiled into the canonical Mission graph. Inspect graph revisions and committed artifacts before proposing another mutation."
            .to_string()
    } else {
        "The semantic proposal was rejected before execution. Inspect current runtime state, then revise the proposal without executor, lease, or system-path fields."
            .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestration::request::SemanticFocus;

    #[test]
    fn team_deadline_scales_with_compiled_waves_not_template_name_or_magic_ladder() {
        let base = harness_contract::agent::DEFAULT_DELEGATED_EXECUTION_TIMEOUT_MS;
        let before = crate::tool_invocation::now_ms();
        let parallel = adaptive_team_deadline_at_ms(3, 3, true);
        let serial = adaptive_team_deadline_at_ms(3, 1, true);

        // 3 parallel roles + verify + synthesis = 3 liveness phases.
        assert!(parallel >= before.saturating_add(base.saturating_mul(3)));
        // The same topology in one lane has three Agent waves plus verify and
        // synthesis, so it receives a larger durable window without creating
        // an unbounded deadline.
        assert!(serial >= before.saturating_add(base.saturating_mul(5)));
        assert!(serial > parallel);
        assert_ne!(parallel, u64::MAX);
        assert_ne!(serial, u64::MAX);
    }

    #[test]
    fn team_deadline_normalizes_invalid_zero_parallelism_without_unbounded_sentinel() {
        let before = crate::tool_invocation::now_ms();
        let deadline = adaptive_team_deadline_at_ms(1, 0, false);
        assert!(
            deadline
                >= before.saturating_add(
                    harness_contract::agent::DEFAULT_DELEGATED_EXECUTION_TIMEOUT_MS
                        .saturating_mul(2)
                )
        );
        assert_ne!(deadline, u64::MAX);
    }

    #[test]
    fn review_nodes_default_to_evidence_ready_predicate_not_status_all() {
        let dependency = default_review_dependency(
            CapabilityRecipeId::Review,
            ExecutionDependencyPolicy::All,
            &[],
            true,
        );
        let ExecutionDependencyPolicy::EvidenceReady { predicate, .. } = dependency else {
            panic!("Review edges must consume typed terminal facts");
        };
        let DependencyPredicate::EvidenceReady {
            accepted_execution_statuses,
            accepted_acceptance_verdicts,
            minimum,
            ..
        } = predicate;
        assert_eq!(minimum, 1);
        assert!(accepted_execution_statuses.contains(&ExecutionNodeStatus::Failed));
        assert!(accepted_acceptance_verdicts.contains(&AcceptanceVerdict::FrameworkInvalid));

        let explicit = default_review_dependency(
            CapabilityRecipeId::Review,
            ExecutionDependencyPolicy::Quorum {
                minimum: 2,
                cancel_remaining: false,
            },
            &[],
            true,
        );
        assert_eq!(
            explicit,
            ExecutionDependencyPolicy::Quorum {
                minimum: 2,
                cancel_remaining: false,
            },
            "an explicit dependency contract is never overwritten"
        );
        assert_eq!(
            default_review_dependency(
                CapabilityRecipeId::Synthesis,
                ExecutionDependencyPolicy::All,
                &[],
                true,
            ),
            ExecutionDependencyPolicy::All
        );
        assert_eq!(
            default_review_dependency(
                CapabilityRecipeId::Review,
                ExecutionDependencyPolicy::All,
                &[],
                false,
            ),
            ExecutionDependencyPolicy::All,
            "a Review node without predecessors keeps the plain All dependency"
        );
    }

    #[test]
    fn shared_infrastructure_scope_does_not_erase_distinct_focus_boundaries() {
        let semantic = GraphSemanticNode {
            node_id: "research".to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: "Team A reads secret-a; Team B reads secret-b; merge both".to_string(),
            depends_on: Vec::new(),
            multiplicity: 2,
            focuses: vec![
                SemanticFocus {
                    focus_id: "official".to_string(),
                    role_id: "solution".to_string(),
                    objective: "collect official evidence".to_string(),
                    resource_scopes: vec!["network:*".to_string()],
                    evidence_responsibilities: vec!["official sources".to_string()],
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                },
                SemanticFocus {
                    focus_id: "ecosystem".to_string(),
                    role_id: "solution".to_string(),
                    objective: "collect ecosystem evidence".to_string(),
                    resource_scopes: vec!["network:*".to_string()],
                    evidence_responsibilities: vec!["community sources".to_string()],
                    output_contract: Vec::new(),
                    output_acceptance: Vec::new(),
                },
            ],
            template: None,
            target_session_id: None,
            output_artifacts: vec!["research evidence".to_string()],
            evidence_contract: vec!["source-backed findings".to_string()],
            required_evidence_refs: Vec::new(),
            resource_scopes: vec!["network:*".to_string()],
            required: true,
            dependency: Default::default(),
            cancellation_group: None,
        };

        let mut repairs = Vec::new();
        let plans = focus_partition_plans(&semantic, &mut repairs);
        assert!(repairs.is_empty());
        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].slots.len(), 2);
        assert!(plans[0]
            .slots
            .iter()
            .all(|slot| slot.overlap_budget_bp == 10_000));
        assert_ne!(plans[0].slots[0].boundary, plans[0].slots[1].boundary);
        assert!(plans[0].shared_baseline[0].contains("collect official evidence"));
        assert!(plans[0].shared_baseline[0].contains("collect ecosystem evidence"));
        assert!(!plans[0].shared_baseline[0].contains("secret-a"));
        assert!(!plans[0].shared_baseline[0].contains("secret-b"));
        let bounded = bounded_team_node_objective(&semantic);
        assert!(!bounded.contains("merge both"));
    }

    #[test]
    fn team_semantic_topology_becomes_durable_program_without_forcing_serial_work() {
        let team = |id: &str, depends_on: Vec<String>| GraphSemanticNode {
            node_id: id.to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: format!("{id} workstream"),
            depends_on,
            multiplicity: 1,
            focuses: Vec::new(),
            template: None,
            target_session_id: None,
            output_artifacts: vec!["terminal_synthesis".to_string()],
            evidence_contract: Vec::new(),
            required_evidence_refs: Vec::new(),
            resource_scopes: vec!["read:src".to_string()],
            required: true,
            dependency: ExecutionDependencyPolicy::default(),
            cancellation_group: None,
        };
        let independent = GraphMutationProposal {
            mutation_id: "independent-team-program".to_string(),
            target_execution_id: None,
            expected_revision: None,
            nodes: vec![
                team("research", Vec::new()),
                team("implementation", Vec::new()),
            ],
            completion: Default::default(),
            collaboration_program: None,
            reason: "independent workstreams".to_string(),
        };
        let program = collaboration_program_from_proposal(&independent, None)
            .expect("derive program")
            .expect("Team program");
        assert_eq!(program.required_team_count, 2);
        assert!(program.edges.is_empty());

        let dependent = GraphMutationProposal {
            mutation_id: "review-after-research".to_string(),
            target_execution_id: None,
            expected_revision: None,
            nodes: vec![
                team("research", Vec::new()),
                team("review", vec!["research".to_string()]),
            ],
            completion: Default::default(),
            collaboration_program: None,
            reason: "review consumes research evidence".to_string(),
        };
        let program = collaboration_program_from_proposal(&dependent, None)
            .expect("derive program")
            .expect("Team program");
        assert_eq!(program.edges.len(), 1);
        assert_eq!(program.edges[0].from, "research:1");
        assert_eq!(program.edges[0].to, "review:1");
    }
}
