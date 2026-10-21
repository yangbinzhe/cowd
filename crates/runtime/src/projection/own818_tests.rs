use super::*;
use crate::execution_core::graph::{ExecutionNodeBinding, NodeExecutionTicket};
use harness_contract::execution_graph::{
    ExecutionGraph, ExecutionGraphLineage, ExecutionNodeKind, ExecutionNodeSpec,
    ExecutionParentBinding,
};
use harness_contract::projection::{TerminalEffectSettlement, TerminalExecutionPhase};

fn warm(
    services: &RuntimeServices,
    root: &str,
) -> (
    ExecutionProjectionScope,
    BTreeMap<String, history::HistoryState>,
    u64,
    history::RequestBudget,
) {
    let through = services.event_store().durable_commit_cursor().unwrap();
    let graph = services.graph_state_store().projection(root).unwrap();
    let mut budget = history::RequestBudget::default();
    let scope =
        ExecutionProjectionScope::load_budgeted(services, root, &graph, true, through, &mut budget)
            .unwrap();
    let facts =
        history::load_scope_budgeted(services, &scope, through, false, &mut budget).unwrap();
    budget.publish(services).unwrap();
    (scope, facts, through, budget)
}

fn register(
    services: &RuntimeServices,
    parent: Option<ExecutionParentBinding>,
    count: usize,
) -> ExecutionGraph {
    let mut graph = ExecutionGraph::new("818 checkpointed original root/child history")
        .with_lineage(ExecutionGraphLineage {
            session_id: "818-session".into(),
            turn_id: "818-turn".into(),
            root_task_id: "818-task".into(),
            task_id: "818-task".into(),
            generation: 1,
        });
    graph.parent_execution = parent;
    let payload = "818-history-node-effect";
    for _ in 0..count {
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", payload);
        graph
            .node_statuses
            .insert(node.id.clone(), ExecutionNodeStatus::Planned);
        graph.nodes.push(node);
    }
    services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph
}

fn start(
    services: &RuntimeServices,
    mut graph: ExecutionGraph,
    node: &ExecutionNodeSpec,
) -> ExecutionGraph {
    graph = services
        .commit_service()
        .transition_node(
            &graph,
            &node.id,
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .unwrap()
        .graph;
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: node.id.clone(),
        executor_kind: node.executor_kind.clone(),
        service_class: graph.service_class,
        attempt: 1,
        idempotency_key: node.idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    let binding = ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: 1,
        resource_lease_refs: Vec::new(),
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    assert_eq!(node.kind, ExecutionNodeKind::InlineModel);
    let started = services
        .commit_service()
        .bind_and_start_nodes(
            &graph,
            BTreeMap::from([(node.id.clone(), binding)]),
            vec![ticket.clone()],
        )
        .unwrap();
    assert!(services
        .event_store()
        .event_by_idempotency_key(
            &ExecutionNodeBinding::effect_stream_id(&ticket),
            &format!("{}:intent", ticket.idempotency_key),
        )
        .unwrap()
        .is_some());
    started.graph
}

#[tokio::test]
async fn own818_old_root_child_active_unknown_then_required_closure_resumes() {
    let services = RuntimeServices::in_memory().unwrap();
    let mut root = register(&services, None, 31);
    let mut child = register(
        &services,
        Some(ExecutionParentBinding {
            execution_id: root.id.clone(),
            node_id: root.nodes[0].id.clone(),
        }),
        30,
    );
    let (planned_scope, planned_facts, planned_through, mut planned_budget) = warm(&services, &root.id);
    assert!(planned_facts.values().all(|state| state.bindings.is_empty()
        && !state.effect_missing_receipt && !state.incomplete
        && state.effects_checked_through == Some(planned_through)));
    let planned_graph = services.graph_state_store().projection(&root.id).unwrap();
    let planned = execution_terminal_axes(
        &services, &planned_graph, &planned_scope, &planned_facts, planned_through, &mut planned_budget,
    ).unwrap();
    assert_eq!(planned.execution.settlement, TerminalEffectSettlement::Unknown);
    for _ in 0..20 {
        root = services
            .commit_service()
            .apply_command(
                &root,
                &ExecutionGraphCommand::Start {
                    expected_revision: root.revision,
                },
            )
            .unwrap()
            .graph;
        warm(&services, &root.id);
    }
    for node in root.nodes.clone() {
        root = start(&services, root, &node);
        warm(&services, &root.id);
        if node.id != root.nodes.last().unwrap().id {
            root = services
                .commit_service()
                .transition_node(
                    &root,
                    &node.id,
                    ExecutionNodeStatus::Completed,
                    None,
                    Vec::new(),
                )
                .unwrap()
                .graph;
            warm(&services, &root.id);
        }
    }
    for node in child.nodes.clone() {
        child = start(&services, child, &node);
        warm(&services, &root.id);
        child = services
            .commit_service()
            .transition_node(
                &child,
                &node.id,
                ExecutionNodeStatus::Completed,
                None,
                Vec::new(),
            )
            .unwrap()
            .graph;
        warm(&services, &root.id);
    }
    let graph = services.graph_state_store().projection(&root.id).unwrap();
    let (scope, facts, through, mut budget) = warm(&services, &root.id);
    assert_eq!(facts.len(), 2);
    assert_eq!(
        facts
            .values()
            .map(|state| state.bindings.len())
            .sum::<usize>(),
        61
    );
    let before = budget.consumed_work();
    assert!(before.0 <= 256);
    assert!(before.1 <= 1024 * 1024);
    let active =
        execution_terminal_axes(&services, &graph, &scope, &facts, through, &mut budget).unwrap();
    assert!(budget.consumed_work().0 <= 256);
    assert!(budget.consumed_work().1 <= 1024 * 1024);
    assert_eq!(active.execution.phase, TerminalExecutionPhase::Running);
    assert_eq!(
        active.execution.settlement,
        TerminalEffectSettlement::Unsettled
    );
    assert!(!active.execution.coverage_complete);
    assert!(active.outcome.outcome.is_none());
    let query = ProjectionQueryContext {
        principal: "818-principal".into(),
        workspace_id: services.workspace_key().into(),
        session_scopes: Vec::new(),
        mission_scopes: Vec::new(),
        visibility_grants: vec![
            format!("workspace:{}", services.workspace_key()),
            "resource:*".into(),
        ],
        detail_scope: ProjectionDetailScope::Full,
        authorization_revision: 1,
    };
    let full = super::super::snapshot(&services, &root.id, &query)
        .await
        .unwrap();
    assert_eq!(full.terminal_axes, active);
    assert!(serde_json::to_vec(&full).unwrap().len() <= 1024 * 1024);
    assert_eq!(services.execution_projection_cache_stats().2, 0);
    let repeated = super::super::snapshot(&services, &root.id, &query)
        .await
        .unwrap();
    assert_eq!(repeated.terminal_axes, active);
    assert_eq!(services.execution_projection_cache_stats().2, 0);
    let mut foreign = query.clone();
    foreign.workspace_id = "818-foreign".into();
    assert!(super::super::snapshot(&services, &root.id, &foreign)
        .await
        .is_err());
    let mut missing = facts.clone();
    missing.get_mut(&root.id).unwrap().effect_missing_receipt = true;
    let known_missing = execution_terminal_axes(
        &services,
        &graph,
        &scope,
        &missing,
        through,
        &mut history::RequestBudget::default(),
    )
    .unwrap();
    assert_eq!(
        known_missing.execution.settlement,
        TerminalEffectSettlement::Unsettled
    );
    assert!(!known_missing.execution.coverage_complete);
    let mut invalid = facts[&root.id].clone();
    invalid.start_tickets.clear();
    assert!(validate_history_effects(
        &services,
        &invalid,
        through,
        &mut history::RequestBudget::default()
    )
    .is_err());
    invalid = facts[&root.id].clone();
    invalid.through = through + 1;
    let mut wrong_cut = facts.clone();
    wrong_cut.insert(root.id.clone(), invalid);
    assert!(execution_terminal_axes(
        &services,
        &graph,
        &scope,
        &wrong_cut,
        through,
        &mut history::RequestBudget::default()
    )
    .is_err());
    let same_cut =
        execution_terminal_axes(&services, &graph, &scope, &facts, through, &mut budget).unwrap();
    assert_eq!(same_cut, active);
    budget.publish(&services).unwrap();
    assert!(budget.consumed_work().0 <= 256);
    assert!(budget.consumed_work().1 <= 1024 * 1024);
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: "818-source-race".into(),
            scope: RuntimeEventScope::Session,
            kind: "818.diagnostic".into(),
            status: None,
            actor: None,
            refs: Vec::new(),
            payload: serde_json::json!({}),
        })
        .unwrap();
    let raced = execution_terminal_axes(
        &services,
        &graph,
        &scope,
        &facts,
        through,
        &mut history::RequestBudget::default(),
    )
    .unwrap();
    assert!(raced.source_race);
    assert_eq!(raced.execution.phase, TerminalExecutionPhase::Unknown);
    assert_eq!(
        raced.execution.settlement,
        TerminalEffectSettlement::Unknown
    );
    assert!(!raced.execution.coverage_complete);
    let node_id = root.nodes.last().unwrap().id.clone();
    services
        .commit_service()
        .transition_node(
            &root,
            &node_id,
            ExecutionNodeStatus::Completed,
            None,
            Vec::new(),
        )
        .unwrap();
    let graph = services.graph_state_store().projection(&root.id).unwrap();
    let (scope, facts, through, mut shared) = warm(&services, &root.id);
    assert_eq!(facts.values().map(|state| state.bindings.len()).sum::<usize>(), 61);
    let shared_axes =
        execution_terminal_axes(&services, &graph, &scope, &facts, through, &mut shared).unwrap();
    assert!(shared.consumed_work().0 <= 256);
    assert!(shared.consumed_work().1 <= 1024 * 1024);
    let mut independent = history::RequestBudget::default();
    let independent_axes =
        execution_terminal_axes(&services, &graph, &scope, &facts, through, &mut independent)
            .unwrap();
    assert!(independent.consumed_work().0 <= 256);
    assert!(independent.consumed_work().1 <= 1024 * 1024);
    assert_eq!(shared_axes, independent_axes);
    assert_eq!(
        independent_axes.execution.settlement,
        TerminalEffectSettlement::Unsettled,
        "through={through}; current_source_cursor={:?}; shared_work={:?}; independent_work={:?}; facts={:?}; shared_axes={shared_axes:#?}; independent_axes={independent_axes:#?}",
        services.event_store().durable_commit_cursor(),
        shared.consumed_work(),
        independent.consumed_work(),
        facts.iter().map(|(identity, state)| (identity, state.incomplete, state.through,
            state.effects_checked_through, state.effect_missing_receipt,
            state.bindings.len(), state.terminal_proofs.len())).collect::<Vec<_>>()
    );
    assert!(!independent_axes.execution.coverage_complete);
    assert!(independent_axes.outcome.outcome.is_none());
    assert_ne!(independent_axes.execution.phase, TerminalExecutionPhase::Terminal);
    services.shutdown_execution().await;
}

struct NoEffectBackend;

#[async_trait::async_trait]
impl crate::execution_core::graph::executors::ScopedNodeBackend for NoEffectBackend {
    async fn execute(
        &self,
        _: &NodeExecutionTicket,
    ) -> Result<
        crate::execution_core::graph::NodeExecutionOutcome,
        crate::execution_core::graph::NodeExecutorError,
    > {
        Ok(crate::execution_core::graph::NodeExecutionOutcome::new(
            harness_contract::execution_graph::ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: None,
                wait_class: None,
                summary: Some("818 deterministic no-provider completion".into()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            },
        ))
    }
}

struct NoEffectResolver {
    graph_id: String,
    node_id: String,
    idempotency_key: String,
}

impl crate::execution_core::graph::executors::ScopedNodeBackendResolver for NoEffectResolver {
    fn resolve(
        &self,
        ticket: &NodeExecutionTicket,
    ) -> Option<std::sync::Arc<dyn crate::execution_core::graph::executors::ScopedNodeBackend>> {
        (ticket.graph_id == self.graph_id
            && ticket.node_id == self.node_id
            && ticket.idempotency_key == self.idempotency_key
            && ticket.executor_kind == "inline_model"
            && ticket.attempt == 1
            && ticket.payload_ref == "{}")
            .then(|| std::sync::Arc::new(NoEffectBackend) as std::sync::Arc<dyn crate::execution_core::graph::executors::ScopedNodeBackend>)
    }
}

#[tokio::test]
async fn own818_quiescent_real_runner_closure_still_proven_settled_without_goal_or_ack() {
    let services = RuntimeServices::in_memory().unwrap();
    let mut graph =
        ExecutionGraph::new("818 real runner no provider").with_lineage(ExecutionGraphLineage {
            session_id: "818-session".into(),
            turn_id: "818-turn".into(),
            root_task_id: "818-task".into(),
            task_id: "818-task".into(),
            generation: 1,
        });
    let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
    graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    let graph_id = graph.id.clone();
    services
        .model_step_executor()
        .install_resolver(std::sync::Arc::new(NoEffectResolver {
            graph_id: graph_id.clone(),
            node_id: graph.nodes[0].id.clone(),
            idempotency_key: graph.nodes[0].idempotency_key.clone(),
        }));
    services
        .execution_supervisor()
        .submit_and_wait(
            graph,
            ExecutionGraphCommand::Start {
                expected_revision: 0,
            },
        )
        .await
        .unwrap();
    let graph = services.graph_state_store().projection(&graph_id).unwrap();
    let (scope, facts, through, mut budget) = warm(&services, &graph_id);
    let before = budget.consumed_work();
    let axes =
        execution_terminal_axes(&services, &graph, &scope, &facts, through, &mut budget).unwrap();
    assert!(budget.consumed_work().0 > before.0);
    assert_eq!(
        axes.execution.settlement,
        TerminalEffectSettlement::ProvenSettled
    );
    assert!(axes.execution.coverage_complete);
    assert!(axes.outcome.outcome.is_none());
    assert_ne!(axes.execution.phase, TerminalExecutionPhase::Terminal);
    assert_eq!(
        axes.delivery.session_client_evidence,
        harness_contract::projection::SessionClientEvidence::RequiresFreshContext
    );
    let mut missing = facts.clone();
    missing.get_mut(&graph_id).unwrap().start_tickets.clear();
    assert!(execution_terminal_axes(
        &services,
        &graph,
        &scope,
        &missing,
        through,
        &mut history::RequestBudget::default()
    )
    .is_err());
    let mut corrupt = facts.clone();
    for position in corrupt
        .get_mut(&graph_id)
        .unwrap()
        .start_positions
        .values_mut()
    {
        position.0 = through + 1;
    }
    let corrupt_axes = execution_terminal_axes(
        &services,
        &graph,
        &scope,
        &corrupt,
        through,
        &mut history::RequestBudget::default(),
    )
    .unwrap();
    assert!(!corrupt_axes.execution.coverage_complete);
    assert_eq!(
        corrupt_axes.execution.settlement,
        TerminalEffectSettlement::Unknown
    );
    assert!(validate_history_effects(
        &services,
        &corrupt[&graph_id],
        through,
        &mut history::RequestBudget::default()
    )
    .is_err());
    use harness_contract::agent_action::{
        AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, TeamCreateInput,
    };
    services
        .agent_action_service()
        .apply(&AgentActionEnvelope {
            action_id: "818-team".into(),
            expected_revision: None,
            actor: AgentActorBinding {
                objective_id: "818-objective".into(),
                program_id: "818-program".into(),
                session_id: "818-session".into(),
                turn_id: "818-turn".into(),
                root_execution_id: Some(graph_id.clone()),
                required_team_count: 0,
                exact_team_count: None,
                objective_summary: "818 draining proof deferral".into(),
                model_lease: "test".into(),
                permission_ceiling: Some(harness_contract::policy::PermissionMode::ReadOnly),
                resource_scopes: Vec::new(),
                actor_id: "818-root".into(),
                kind: AgentActorKind::Root,
                execution_id: None,
                team_id: None,
                agent_id: None,
            },
            action: AgentAction::TeamCreate(TeamCreateInput {
                name: "818-team".into(),
                mission: "818 honest active Program".into(),
                objective: None,
            }),
        })
        .unwrap();
    let (scope, facts, through, _) = warm(&services, &graph_id);
    let mut no_physical_allowance = history::RequestBudget::default();
    no_physical_allowance.charge(MAX_DELTA_BATCHES, 0).unwrap();
    let draining = execution_terminal_axes(
        &services,
        &graph,
        &scope,
        &facts,
        through,
        &mut no_physical_allowance,
    )
    .unwrap();
    assert_eq!(draining.execution.phase, TerminalExecutionPhase::Draining);
    assert_eq!(
        draining.execution.settlement,
        TerminalEffectSettlement::Unknown
    );
    assert!(!draining.execution.coverage_complete);
    services.shutdown_execution().await;
}

#[tokio::test]
async fn own818_retained_run_start_count_scale_preserves_old_history_and_zero_active_physical_work()
{
    let services = RuntimeServices::in_memory().unwrap();
    let mut root = register(&services, None, 26);
    warm(&services, &root.id);
    for _ in 0..140 {
        root = services
            .commit_service()
            .apply_command(
                &root,
                &ExecutionGraphCommand::Start {
                    expected_revision: root.revision,
                },
            )
            .unwrap()
            .graph;
        warm(&services, &root.id);
    }
    for node in root.nodes.clone() {
        root = start(&services, root, &node);
        warm(&services, &root.id);
    }
    let graph = services.graph_state_store().projection(&root.id).unwrap();
    let (scope, facts, through, mut budget) = warm(&services, &root.id);
    assert_eq!(facts[&root.id].bindings.len(), 26);
    assert!(graph.revision > 140);
    let before = budget.consumed_work();
    assert!(before.0 >= 78 && before.0 < 256);
    let axes =
        execution_terminal_axes(&services, &graph, &scope, &facts, through, &mut budget).unwrap();
    assert_eq!(budget.consumed_work(), before);
    assert_eq!(axes.execution.settlement, TerminalEffectSettlement::Unsettled);
    assert_eq!(axes.execution.phase, TerminalExecutionPhase::Running);
    assert!(!axes.execution.coverage_complete);
    assert!(
        services
            .event_store()
            .projection_checkpoint(&format!("projection-history:{}", root.id))
            .unwrap()
            .unwrap()
            .source_cursor
            <= through
    );
    services.shutdown_execution().await;
}

#[tokio::test]
async fn own818_lookup_and_return_share_counter_and_poison_cannot_be_deferred() {
    let services = RuntimeServices::in_memory().unwrap();
    let graph = register(&services, None, 1);
    let graph = start(&services, graph.clone(), &graph.nodes[0]);
    let through = services.event_store().durable_commit_cursor().unwrap();
    let record = services
        .event_store()
        .latest_for_stream(&graph.id)
        .unwrap()
        .unwrap();
    let key = record.idempotency_key.as_ref().unwrap();
    let mut budget = history::RequestBudget::default();
    budget
        .canonical_record(&services, &graph.id, key, through)
        .unwrap()
        .unwrap();
    assert_eq!(budget.consumed_work().0, 2);
    budget
        .canonical_record(&services, &graph.id, "818-missing", through)
        .unwrap();
    assert_eq!(budget.consumed_work().0, 3);
    assert!(budget.charge(0, 1024 * 1024 + 1).is_err());
    let (scope, facts, through, _) = warm(&services, &graph.id);
    let graph = services.graph_state_store().projection(&graph.id).unwrap();
    assert!(
        execution_terminal_axes(&services, &graph, &scope, &facts, through, &mut budget).is_err()
    );
    services.shutdown_execution().await;
}
