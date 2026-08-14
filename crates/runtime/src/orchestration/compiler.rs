use std::collections::{BTreeMap, BTreeSet};

use harness_contract::agent::{AgentCapability, AgentTaskIntent};
use harness_contract::context::ContextBudgetLeaseRef;
use harness_contract::execution_graph::{
    validate_execution_graph, ExecutionEdge, ExecutionEdgeKind, ExecutionGraph,
    ExecutionGraphCommand, ExecutionNodeKind, ExecutionNodeSpec, ExecutionOrchestrationMetadata,
    ExecutionParentBinding, ExecutionWorkContract, ExecutionWorkRole,
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
        &BTreeSet::new(),
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
    existing_node_ids: &BTreeSet<String>,
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
                .or_else(|| {
                    existing_node_ids
                        .contains(dependency)
                        .then(|| vec![dependency.clone()])
                })
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
    node.work = Some(work);
    Ok(node)
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
    let template_id = TeamTemplateDefinitionId::new(
        harness_contract::agent::DefinitionScope::Builtin,
        template_path
            .trim()
            .strip_prefix("builtin/")
            .unwrap_or(&template_path),
    )
    .map_err(|error| OrchestrationCompileError::TeamInstantiation(error.to_string()))?;
    let team_id = format!(
        "runtime-team:{}:{}:{}",
        request_id, semantic.node_id, instance_index
    );
    let request = TeamInstantiationRequest {
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
        parent_execution: Some(ExecutionParentBinding {
            execution_id: graph_id.to_string(),
            node_id: node_id.to_string(),
        }),
        selection_mode: TeamSelectionMode::ModelAssisted,
        strategy_binding: request.strategy_binding.clone(),
        template_selector: TeamTemplateSelector::LatestStable { template_id },
        objective: semantic.objective.clone(),
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
        budget_lease: Some(ContextBudgetLeaseRef::new(
            format!("runtime-team-budget:{node_id}"),
            team_id,
            "runtime_team",
            adaptive_subagent_budget(request),
            1,
        )),
        managed_invocation: None,
        resource_scopes: semantic.resource_scopes.clone(),
        upstream_evidence_refs: Vec::new(),
        upstream_artifact_refs: Vec::new(),
    };
    team_runtime
        .plan(request.clone())
        .map_err(OrchestrationCompileError::TeamInstantiation)?;
    let payload_ref = serde_json::to_string(&request)
        .map_err(|error| OrchestrationCompileError::InvalidProposal(error.to_string()))?;
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::Subgraph,
        TEAM_SUBGRAPH_EXECUTOR,
        payload_ref,
    );
    node.id = node_id.to_string();
    node.idempotency_key = format!("{}:{}", request.request_id, node_id);
    node.resource_scopes = semantic.resource_scopes.clone();
    node.acceptance.criteria = semantic.evidence_contract.clone();
    node.acceptance.required_evidence = semantic.output_artifacts.clone();
    Ok(node)
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
            shared_baseline: vec![semantic.objective.clone()],
            slots,
        })
        .collect()
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
        graph_id: graph_id.to_string(),
        node_id: node_id.to_string(),
        attempt: 1,
        expected_graph_revision: 0,
        objective,
        acceptance: semantic.evidence_contract.clone(),
        constraints: focus
            .map(|focus| {
                vec![
                    format!("focus_partition:{}", focus.focus_id),
                    format!(
                        "focus_evidence_responsibility:{}",
                        focus.evidence_responsibilities.join(",")
                    ),
                ]
            })
            .unwrap_or_default(),
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
        budget_lease: ContextBudgetLeaseRef::new(
            format!("runtime-agent-budget:{node_id}"),
            node_id,
            "runtime_agent",
            adaptive_subagent_budget(request),
            1,
        ),
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
    materialized
}

fn adaptive_subagent_budget(request: &RuntimeOrchestrationCommand) -> u64 {
    let model = request.model_lease.as_deref().unwrap_or("unknown");
    let context_window = provider::model_context_window(model);
    let max_output = provider::model_max_output_resolution(model, None).tokens;
    crate::budget_policy::RuntimeBudgetPlan::derive(crate::budget_policy::RuntimeBudgetInputs {
        model_context_window: context_window,
        model_max_output_tokens: max_output,
        subsystem_budget_ratio_bp: crate::budget_policy::DEFAULT_SUBSYSTEM_BUDGET_RATIO_BP,
        profile: crate::context_runtime::ContextProfile::SubAgent,
        autonomy_mode: None,
    })
    .subagent_default_budget
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
    fn shared_infrastructure_scope_does_not_erase_distinct_focus_boundaries() {
        let semantic = GraphSemanticNode {
            node_id: "research".to_string(),
            recipe: CapabilityRecipeId::Team,
            objective: "research independent evidence domains".to_string(),
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
    }
}
