use std::collections::HashMap;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

use futures::{
    stream::{self, Stream},
    StreamExt,
};

use super::*;

fn r1_branch_state(session_id: &str, goal_id: &str) -> TurnGraphState {
    TurnGraphState {
        content: "answer directly".into(),
        task_understanding: Some(harness_contract::strategy::understand(
            &harness_contract::strategy::StrategyInput::from_prompt("answer directly"),
        )),
        collaboration_obligation: None,
        prompter: SharedPrompter::none(),
        first_model_step: true,
        pending_next_model_context: Vec::new(),
        persistent_collaboration_context: Vec::new(),
        assistant_messages: Vec::new(),
        tool_results: Vec::new(),
        iterations: 0,
        input_tokens: 0,
        output_tokens: 0,
        cache_create_tokens: 0,
        cache_read_tokens: 0,
        cache_dimensions_known: false,
        provider_usage_attempt_observed: false,
        output_chars: 0,
        output_chunks: 0,
        wall_duration_ms: 0,
        model: None,
        models_used: Vec::new(),
        first_token_latency_ms: None,
        active_stream_duration_ms: 0,
        summary: None,
        failure: None,
        terminal_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
        pending_transcript: std::collections::BTreeMap::new(),
        pending_member_batches: std::collections::BTreeMap::new(),
        pending_agent_input_consumption: std::collections::BTreeMap::new(),
        deferred_agent_input_consumption: Vec::new(),
        ingress: None,
        turn_transcript_start: 0,
        session_id: session_id.into(),
        turn_id: "r1-branch-turn".into(),
        goal_id: goal_id.into(),
        context_window: 0,
        safety_lease: crate::execution_core::SafetyFusePolicy::derive(
            0,
            harness_contract::core::TaskComplexity::Simple,
            None,
        ),
        terminal_override: None,
        delivery_envelope: None,
        terminal_presentation: None,
        terminal_commit_owner: None,
        authenticated_terminal_owner: None,
        committed_terminal_answer: None,
        committed_terminal_completion: None,
        last_verified_progress: false,
        consecutive_unverified_model_steps: 0,
        closure_ready_steps: 0,
        previous_objective_milestones: Default::default(),
        verified_collaboration_progress: Default::default(),
        verified_answer_ready_steps: 0,
        final_answer_observed: false,
        collaboration_convergence_replans: 0,
        delegated_redrives: 0,
        completion_gap_continuations: 0,
        started_at: std::time::Instant::now(),
        reasoning_only_attempts: 0,
        force_text_only_next_model: false,
        force_tool_allowlist_next_model: None,
        force_tool_call_next_model: false,
        force_reasoning_effort_next_model: None,
        terminal_recovery_attempts: 0,
        provider_protocol_recovery_attempts: 0,
        provider_frame_repair_attempts: std::collections::BTreeMap::new(),
        agentic_protocol_recovery_attempts: 0,
        agentic_program_context_revision: None,
        execution_role: TurnExecutionRole::RootTurn,
        bounded_evidence_role: false,
        focus_novelty_target_bp: 0,
        focus_acceptance_scopes: Vec::new(),
        focus_acceptance_pending_scopes: Vec::new(),
        focus_required_output_fields: Vec::new(),
        structured_output_replans: 0,
        focus_observed_resource_scopes: BTreeSet::new(),
        focus_observed_evidence: Vec::new(),
        focus_action_rejections: 0,
        pending_focus_terminal_candidate: None,
        focus_discovery_replans: 0,
        focus_verification_prefetched: false,
        clean_terminal_synthesis_next: false,
        clean_terminal_synthesis_attempted: false,
        clean_terminal_retry_attempted: false,
        terminal_failure_narration: None,
        consecutive_tool_failure_batches: 0,
        cold_start_failure_replans: 0,
        consecutive_low_novelty_batches: 0,
        successful_tool_calls: 0,
        last_terminal_answer_digest: String::new(),
        repeated_terminal_answers: 0,
        completion_gap_outstanding: false,
        outstanding_closure_rungs: Vec::new(),
        tool_receipts_observed: 0,
        duplicate_tool_calls: 0,
        identical_action_digest: String::new(),
        identical_action_result_digest: String::new(),
        identical_action_streak: 0,
        per_turn_tool_counts: BTreeMap::new(),
        write_attempt_paths: Vec::new(),

        committed_workspace_write_observed: false,
        committed_workspace_write_scopes: BTreeSet::new(),
        committed_workspace_observed_evidence: Vec::new(),
        workspace_mutation_effect_sequences: BTreeSet::new(),

        max_tool_concurrency_observed: 0,
        parallel_tool_batches: 0,
        early_tool_receipts: BTreeMap::new(),
        evaluation_resource_scopes: Vec::new(),
        evaluation_scope_rejections: 0,
        evaluation_judge_only: false,
        collaboration_started: false,
        pending_root_control_plane_requirement: None,

        root_language_replan_attempted: false,
        pending_terminal_artifact: None,
        pending_controlled_recovery_claim_fingerprints: Vec::new(),
        pending_disposition_inputs: Vec::new(),
        terminal_tracker: crate::liveness::TerminalTracker::new(
            crate::liveness::TerminalObligation::default(),
        ),
        terminal_satisfaction_since: None,
    }
}

fn r1_observation_services() -> Arc<crate::RuntimeServices> {
    let root = std::path::PathBuf::from(
        std::env::var_os("COWD_R1_06_FIXTURE_ROOT").expect("explicit private plan fixture root"),
    )
    .join(format!("host-branches-{}", uuid::Uuid::new_v4()));
    assert!(
        root.is_absolute()
            && root.starts_with("/media/yi/Datas/workspace/plan/2026-10/1016-cowd-future-kernel")
    );
    std::fs::create_dir_all(&root).unwrap();
    crate::RuntimeServices::builder(root.join("home"), &root)
        .runtime_event_store(Arc::new(crate::RuntimeEventStore::for_test()))
        .artifact_store(Arc::new(crate::ArtifactStore::for_test_default(
            root.join("artifacts"),
        )))
        .task_aggregate_service(Arc::new(crate::TaskAggregateService::for_test()))
        .build()
        .unwrap()
}

fn r1_branch_goal(services: &crate::RuntimeServices, session: &str) -> String {
    let goal_id = format!("r1-branch-goal-{}", uuid::Uuid::new_v4());
    let goal: GoalContract = serde_json::from_value(serde_json::json!({
        "id":goal_id,"session_id":session,"objective":"answer directly",
        "criteria":[{"id":"checked","statement":"answer checked","status":"open"}],
        "phase":"execution","spec_revision":1,"spec_digest":"branch-spec",
        "scope":"internal","completion":"open","revision":1,"user_sequence":1
    }))
    .unwrap();
    services.goal_store().create(goal).unwrap();
    goal_id
}

#[test]
fn r641_direct_owned_turn_uses_lineage_without_relaxing_goal_binding() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let session = "r641-session";
    let (graph, ticket) =
        r1_branch_graph(&services, session, vec![], ExecutionNodeKind::InlineModel);
    let goal_id = "r641-goal";
    let goal: GoalContract = serde_json::from_value(serde_json::json!({
        "id":goal_id,"session_id":session,"objective":"answer directly",
        "criteria":[{"id":"checked","statement":"answer checked","status":"open"}],
        "phase":"execution","spec_revision":1,"spec_digest":"branch-spec",
        "scope":"internal","completion":"open","revision":1,"user_sequence":1,
        "source_intent_ref":"session_input:r641-original",
        "execution_binding":{"objective_id":"r641-objective","session_id":session,
            "turn_id":"r1-branch-turn","root_execution_id":graph.id,"agentic_program_id":"r641-program"}
    })).unwrap();
    services.goal_store().create(goal).unwrap();
    let mut state = r1_branch_state(session, goal_id);
    assert!(state.ingress.is_none());
    let identity = runtime_observation_identity(&services, &state, &ticket);
    assert_eq!(identity.turn_id.as_deref(), Some("r1-branch-turn"));
    let observation = runtime_observation(
        identity,
        RuntimeObservationKind::ProviderProgress,
        "r641-provider",
        1,
        "answer".into(),
        "r641".into(),
        ObservationResultClass::Succeeded,
    );
    let source = services.goal_store().projection(goal_id).unwrap().unwrap();
    assert!(services
        .goal_store()
        .turn_candidate_event_at(&source, &observation, &ticket, "answer")
        .is_ok());
    state.ingress = Some(TurnIngressRef {
        request_id: "r641-request".into(),
        turn_id: "r1-branch-turn".into(),
        message_id: "r641-message".into(),
        session_id: session.into(),
        primary_task_id: "r641-task".into(),
        root_task_id: "r641-task".into(),
        session_generation: 1,
        input_sequence: 1,
        claim_owner: "r641-owner".into(),
        claim_token: "r641-claim".into(),
        claim_revision: 1,
        handoff_id: None,
    });
    let ingress_identity = runtime_observation_identity(&services, &state, &ticket);
    assert_eq!(ingress_identity.turn_id.as_deref(), Some("r1-branch-turn"));
    let ingress_observation = runtime_observation(
        ingress_identity,
        RuntimeObservationKind::ProviderProgress,
        "r641-provider",
        1,
        "answer".into(),
        "r641-ingress".into(),
        ObservationResultClass::Succeeded,
    );
    assert!(services
        .goal_store()
        .turn_candidate_event_at(&source, &ingress_observation, &ticket, "answer")
        .is_ok());
    for mutate in 0..3 {
        let mut foreign = observation.clone();
        match mutate {
            0 => foreign.identity.session_id = "foreign-session".into(),
            1 => foreign.identity.turn_id = Some("foreign-turn".into()),
            _ => foreign.identity.graph_id = "foreign-graph".into(),
        }
        assert!(services
            .goal_store()
            .turn_candidate_event_at(&source, &foreign, &ticket, "answer")
            .is_err());
    }
}


fn r1_observation_runtime<C: ApiClient + Send + Sync + 'static>(
    session: Session,
    client: C,
    services: &crate::RuntimeServices,
) -> crate::ConversationRuntime<C, RecordingToolExecutor> {
    r1_observation_runtime_with_executor(
        session,
        client,
        services,
        RecordingToolExecutor {
            executed: Arc::new(AtomicUsize::new(0)),
            order: Arc::new(Mutex::new(Vec::new())),
        },
    )
}

fn r1_observation_runtime_with_executor<C: ApiClient + Send + Sync + 'static>(
    session: Session,
    client: C,
    services: &crate::RuntimeServices,
    executor: RecordingToolExecutor,
) -> crate::ConversationRuntime<C, RecordingToolExecutor> {
    let mut runtime = crate::ConversationRuntime::new(
        session,
        client,
        executor,
        PermissionPolicy::new(PermissionMode::DangerFullAccess),
        vec!["answer directly".into()],
    )
    .without_memory()
    .with_runtime_event_store(services.event_store().clone())
    .with_artifact_store(services.artifact_store().clone())
    .with_cowd_event_bus(crate::CowdEventBus::new());
    runtime.set_active_model("r1-observation-fixture");
    runtime.with_model_context_window(262_144)
}

#[test]
fn r1_observation_runtime_installs_bus_without_fabricating_execution_owner() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let runtime = r1_observation_runtime(Session::new(), FinalAnswerClient, &services);
    let bus = runtime
        .cowd_bus()
        .expect("production-shaped caller installs its original bus");
    assert!(bus.current_execution_context().is_none());
    assert!(bus.current_activity_binding().is_none());
    assert!(runtime
        .root_effect_owner()
        .unwrap_err()
        .to_string()
        .contains("root effect requires its original execution context"));
}

#[test]
fn own836_owned_root_scope_uses_lineage_and_rejects_foreign_context() {
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
    let services = crate::RuntimeServices::in_memory().unwrap();
    let bus = crate::CowdEventBus::new();
    let mut graph = ExecutionGraph::new("owned root scope contract");
    assert!(enter_owned_root_scope(&services, &bus, &graph, None).is_err());
    assert!(bus.current_execution_context().is_none());
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: "own836-session".into(),
        turn_id: "own836-turn".into(),
        root_task_id: "own836-task".into(),
        task_id: "own836-task".into(),
        generation: 7,
    });
    assert!(
        matches!(enter_owned_root_scope(&services, &bus, &graph, None), Err(error) if error.to_string().contains("durably registered"))
    );
    let mut graph = own857_register_root_fixture(&services, graph);
    let (scope, lease) = enter_owned_root_scope(&services, &bus, &graph, None).unwrap();
    assert!(scope.is_some() && lease.is_some());
    let original = bus.current_execution_context().unwrap();
    assert_eq!(original.execution_id, graph.id);
    let owner = bus.current_activity_binding().unwrap();
    assert_eq!(owner.generation, 7);
    assert_eq!(owner.fence, 7);
    assert!(owner.node_id.is_none());
    assert!(services.active_execution_bus(&graph.id).is_some());
    let (borrowed_scope, borrowed_lease) =
        enter_owned_root_scope(&services, &bus, &graph, None).unwrap();
    assert!(borrowed_scope.is_none() && borrowed_lease.is_none());
    let mut foreign = graph.clone();
    foreign.id = "foreign-graph".into();
    assert!(enter_owned_root_scope(&services, &bus, &foreign, None).is_err());
    assert_eq!(
        bus.current_execution_context().unwrap().execution_id,
        original.execution_id
    );
    assert_eq!(bus.current_activity_binding().unwrap(), owner);
    drop(scope);
    drop(lease);
    assert!(bus.current_execution_context().is_none());
    assert!(bus.current_activity_binding().is_none());
    assert!(services.active_execution_bus(&graph.id).is_none());
    assert!(!bus.has_live_telemetry_observer());
    graph.lineage.as_mut().unwrap().generation = 0;
    assert!(enter_owned_root_scope(&services, &bus, &graph, None).is_err());
    assert!(bus.current_execution_context().is_none());
}

fn own857_register_root_fixture(
    services: &crate::RuntimeServices,
    mut graph: harness_contract::execution_graph::ExecutionGraph,
) -> harness_contract::execution_graph::ExecutionGraph {
    if graph.nodes.is_empty() {
        graph.nodes.push(ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "{}",
        ));
    }
    let graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    let unadmitted_bus = crate::CowdEventBus::new();
    assert!(matches!(
        enter_owned_root_scope(services, &unadmitted_bus, &graph, None),
        Err(error) if error.to_string().contains("durably registered")
    ));
    assert!(unadmitted_bus.current_execution_context().is_none());
    assert!(!unadmitted_bus.has_live_telemetry_observer());
    let lineage = graph.lineage.as_ref().unwrap();
    services.record_live_execution(
        &lineage.session_id,
        graph.id.clone(),
        lineage.turn_id.clone(),
    );
    graph
}

fn own836_scope_graph() -> harness_contract::execution_graph::ExecutionGraph {
    let mut graph = harness_contract::execution_graph::ExecutionGraph::new("scope identity");
    graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: "own836-session".into(),
        turn_id: "own836-turn".into(),
        root_task_id: "own836-task".into(),
        task_id: "own836-task".into(),
        generation: 7,
    });
    graph
}

#[test]
fn own836_existing_root_rejects_every_identity_and_partial_scope() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let graph = own857_register_root_fixture(&services, own836_scope_graph());
    let bus = crate::CowdEventBus::new();
    let guards = enter_owned_root_scope(&services, &bus, &graph, None).unwrap();
    let context = bus.current_execution_context().unwrap();
    let owner = bus.current_activity_binding().unwrap();
    for (field, value) in [
        ("root_execution_id", serde_json::json!("foreign-root")),
        ("session_id", serde_json::json!("foreign-session")),
        ("turn_id", serde_json::json!("foreign-turn")),
        ("root_task_id", serde_json::json!("foreign-root-task")),
        ("task_id", serde_json::json!("foreign-task")),
        ("activity_id", serde_json::json!("foreign-activity")),
        ("revision", serde_json::json!(2)),
        ("fence", serde_json::json!(6)),
        ("generation", serde_json::json!(6)),
        ("node_id", serde_json::json!("foreign-node")),
        ("parent_activity_id", serde_json::json!("foreign-parent")),
        (
            "initiator_activity_id",
            serde_json::json!("foreign-initiator"),
        ),
        ("agent_run_id", serde_json::json!("foreign-agent")),
        ("agent_instance_id", serde_json::json!("foreign-instance")),
        ("team_run_id", serde_json::json!("foreign-team")),
        ("tool_call_id", serde_json::json!("foreign-call")),
        ("tool_contract_id", serde_json::json!("foreign-tool")),
        ("skill_id", serde_json::json!("foreign-skill")),
        ("approval_id", serde_json::json!("foreign-approval")),
    ] {
        let changed_bus = crate::CowdEventBus::new();
        let mut changed = serde_json::to_value(&owner).unwrap();
        changed[field] = value;
        let changed = serde_json::from_value(changed).unwrap();
        let _scope = changed_bus.enter_execution_with_activity(context.clone(), Some(changed));
        let error =
            validate_existing_owned_root(&services, &changed_bus, &graph, None).unwrap_err();
        assert!(
            error.to_string().contains("context/activity mismatch"),
            "{field}: {error}"
        );
        assert_eq!(bus.current_activity_binding().unwrap(), owner);
    }
    for field in ["execution_id", "session_id", "turn_id"] {
        let changed_bus = crate::CowdEventBus::new();
        let mut changed = serde_json::to_value(&context).unwrap();
        changed[field] = serde_json::json!("foreign-context");
        let _scope = changed_bus.enter_execution_with_activity(
            serde_json::from_value(changed).unwrap(),
            Some(owner.clone()),
        );
        let error =
            validate_existing_owned_root(&services, &changed_bus, &graph, None).unwrap_err();
        assert!(
            error.to_string().contains("context/activity mismatch"),
            "{field}: {error}"
        );
    }
    let partial = crate::CowdEventBus::new();
    let context_only = partial.enter_execution(context.clone());
    assert!(enter_owned_root_scope(&services, &partial, &graph, None).is_err());
    assert!(partial.current_execution_context().is_some());
    drop(context_only);
    let first = partial.enter_execution_with_activity(context.clone(), Some(owner.clone()));
    let mut changed_owner = owner.clone();
    changed_owner.activity_id = "activity-only".into();
    let second =
        partial.enter_execution_with_activity(context.clone(), Some(changed_owner.clone()));
    drop(first);
    assert!(partial.current_execution_context().is_none());
    assert_eq!(partial.current_activity_binding().unwrap(), changed_owner);
    assert!(enter_owned_root_scope(&services, &partial, &graph, None).is_err());
    assert_eq!(partial.current_activity_binding().unwrap(), changed_owner);
    drop(second);
    let impostor = crate::CowdEventBus::new();
    let _scope = impostor.enter_execution_with_activity(context, Some(owner.clone()));
    assert!(
        validate_existing_owned_root(&services, &impostor, &graph, None)
            .unwrap_err()
            .to_string()
            .contains("bus/telemetry lease mismatch")
    );
    let mut parented = graph.clone();
    parented.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: "parent-root".into(),
        node_id: "parent-node".into(),
    });
    assert!(enter_owned_root_scope(&services, &bus, &parented, None).is_err());
    assert_eq!(bus.current_activity_binding().unwrap(), owner);
    drop(guards);
}

#[test]
fn own836_exact_ingress_borrows_original_claim_and_bus_lease() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let mut graph = own836_scope_graph();
    let ingress = TurnIngressRef {
        request_id: "own836-request".into(),
        turn_id: "own836-turn".into(),
        message_id: "own836-message".into(),
        session_id: "own836-session".into(),
        primary_task_id: "own836-task".into(),
        root_task_id: "own836-task".into(),
        session_generation: 7,
        input_sequence: 1,
        claim_owner: "original-owner".into(),
        claim_token: "original-claim".into(),
        claim_revision: 11,
        handoff_id: None,
    };
    graph.id = crate::session_execution::session_ingress_graph_id(
        &ingress.session_id,
        &ingress.request_id,
        &ingress.turn_id,
    );
    let bus = crate::CowdEventBus::new();
    assert!(enter_owned_root_scope(&services, &bus, &graph, Some(&ingress)).is_err());
    let graph = own857_register_root_fixture(&services, graph);
    let lease = services
        .bind_active_execution_bus(&graph.id, bus.clone())
        .unwrap();
    let original = owned_root_activity(&graph, Some(&ingress)).unwrap();
    let scope = bus.enter_execution_with_activity(
        crate::CowdExecutionContext {
            execution_id: graph.id.clone(),
            session_id: ingress.session_id.clone(),
            turn_id: ingress.turn_id.clone(),
        },
        Some(original.clone()),
    );
    let borrowed = enter_owned_root_scope(&services, &bus, &graph, Some(&ingress)).unwrap();
    assert!(borrowed.0.is_none() && borrowed.1.is_none());
    drop(borrowed);
    assert_eq!(bus.current_activity_binding().unwrap().revision, 11);
    assert!(services.active_execution_bus(&graph.id).is_some());
    let mut stale = ingress.clone();
    stale.claim_revision += 1;
    assert!(enter_owned_root_scope(&services, &bus, &graph, Some(&stale)).is_err());
    stale = ingress.clone();
    stale.session_generation -= 1;
    assert!(enter_owned_root_scope(&services, &bus, &graph, Some(&stale)).is_err());
    assert_eq!(bus.current_activity_binding().unwrap(), original);
    drop(lease);
    assert!(enter_owned_root_scope(&services, &bus, &graph, Some(&ingress)).is_err());
    assert_eq!(bus.current_activity_binding().unwrap(), original);
    drop(scope);
}

#[tokio::test]
async fn own836_root_scope_and_bus_lease_clear_on_error_cancel_and_next_turn() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let bus = crate::CowdEventBus::new();
    let graph = own857_register_root_fixture(&services, own836_scope_graph());
    let error: Result<(), RuntimeError> = (|| {
        let _guards = enter_owned_root_scope(&services, &bus, &graph, None)?;
        assert!(services.resolve_active_execution_bus(&graph.id).is_some());
        Err(RuntimeError::new("original execution failed"))
    })();
    assert!(error.is_err());
    assert!(bus.current_execution_context().is_none());
    assert!(services.active_execution_bus(&graph.id).is_none());
    let mut pending = Box::pin(async {
        let guards = enter_owned_root_scope(&services, &bus, &graph, None).unwrap();
        std::future::pending::<()>().await;
        drop(guards);
    });
    assert!(futures::poll!(pending.as_mut()).is_pending());
    assert!(bus.current_execution_context().is_some());
    assert!(bus.has_live_telemetry_observer());
    drop(pending);
    assert!(bus.current_execution_context().is_none());
    assert!(bus.current_activity_binding().is_none());
    assert!(!bus.has_live_telemetry_observer());
    assert!(services.active_execution_bus(&graph.id).is_none());
    for turn in ["own836-turn-next-1", "own836-turn-next-2"] {
        let mut next = own836_scope_graph();
        next.lineage.as_mut().unwrap().turn_id = turn.into();
        let next = own857_register_root_fixture(&services, next);
        let guards = enter_owned_root_scope(&services, &bus, &next, None).unwrap();
        assert_eq!(bus.current_execution_context().unwrap().turn_id, turn);
        let (resolved, resolved_bus) = services.resolve_active_execution_bus(&next.id).unwrap();
        assert_eq!(resolved, next.id);
        assert!(bus.subscribe().same_channel(&resolved_bus.subscribe()));
        drop(guards);
        assert!(bus.current_execution_context().is_none());
        assert!(services.active_execution_bus(&next.id).is_none());
    }
}

#[tokio::test]
async fn own836_public_direct_foreign_owner_and_role_mismatch_fail_before_strategy() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    for mode in ["existing-root", "root-parent", "leaf-no-parent"] {
        let runtime = r1_observation_runtime(Session::new(), FinalAnswerClient, &services);
        let lineage = fresh_test_execution_lineage(runtime.session_id());
        let bus = runtime.cowd_bus().unwrap().clone();
        let mut original = own836_scope_graph();
        original.lineage = Some(lineage.clone());
        let original = own857_register_root_fixture(&services, original);
        let guards = if mode == "existing-root" {
            Some(enter_owned_root_scope(&services, &bus, &original, None).unwrap())
        } else {
            None
        };
        let before = services.event_store().current_commit_cursor();
        let parent = (mode == "root-parent").then(|| {
            harness_contract::execution_graph::ExecutionParentBinding {
                execution_id: "original-parent".into(),
                node_id: "original-node".into(),
            }
        });
        let role = if mode == "leaf-no-parent" {
            TurnExecutionRole::DelegatedLeaf
        } else {
            TurnExecutionRole::RootTurn
        };
        let (runtime, result) = submit_owned_conversation_turn_with_ingress(
            Box::new(runtime),
            services.clone(),
            "never execute a provider",
            &SharedPrompter::none(),
            None,
            parent,
            Some(lineage),
            role,
            0,
            false,
        )
        .await
        .unwrap();
        let error = result.unwrap_err();
        let expected = match mode {
            "existing-root" => "fresh direct root cannot inherit an existing owner",
            "root-parent" => "RootTurn cannot have a parent execution",
            _ => "delegated leaf requires its original parent",
        };
        assert!(error.to_string().contains(expected), "{mode}: {error}");
        assert!(runtime.active_turn_strategy().is_none());
        assert_eq!(services.event_store().current_commit_cursor(), before);
        if mode == "existing-root" {
            assert_eq!(
                bus.current_execution_context().unwrap().execution_id,
                original.id
            );
            assert!(services.active_execution_bus(&original.id).is_some());
        } else {
            assert!(bus.current_execution_context().is_none());
            assert!(bus.current_activity_binding().is_none());
            assert!(!bus.has_live_telemetry_observer());
        }
        drop(guards);
    }
}

#[test]
fn own836_durable_child_resolves_only_its_original_root_bus() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let (root, ticket) = r1_branch_graph(
        &services,
        "own836-child-session",
        vec![],
        ExecutionNodeKind::InlineModel,
    );
    let bus = crate::CowdEventBus::new();
    let guards = enter_owned_root_scope(&services, &bus, &root, None).unwrap();
    let mut child = harness_contract::execution_graph::ExecutionGraph::new("durable child");
    child.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: root.id.clone(),
        node_id: ticket.node_id,
    });
    child.lineage = root.lineage.clone();
    child.nodes.push(ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "{}",
    ));
    let child = services
        .commit_service()
        .register_graph(child)
        .unwrap()
        .graph;
    let (resolved_root, resolved_bus) = services.resolve_active_execution_bus(&child.id).unwrap();
    assert_eq!(resolved_root, root.id);
    assert!(bus.subscribe().same_channel(&resolved_bus.subscribe()));
    assert_eq!(
        resolved_bus
            .current_execution_context()
            .unwrap()
            .execution_id,
        root.id
    );
    assert!(resolved_bus.has_live_telemetry_observer());
    assert!(services.active_execution_bus(&child.id).is_none());
    drop(guards);
    assert!(services.resolve_active_execution_bus(&child.id).is_none());
    assert!(!resolved_bus.has_live_telemetry_observer());
}

#[tokio::test]
async fn own836_model_node_view_preserves_root_lease_counters_and_cancel_restore() {
    let services = crate::RuntimeServices::in_memory().unwrap();
    let session = Session::new();
    let (graph, ticket) = r1_branch_graph(
        &services,
        &session.session_id,
        vec![],
        ExecutionNodeKind::InlineModel,
    );
    let mut runtime = r1_observation_runtime(session, FinalAnswerClient, &services);
    let bus = runtime.cowd_bus().unwrap().clone();
    let guards = enter_owned_root_scope(&services, &bus, &graph, None).unwrap();
    let root = bus.current_activity_binding().unwrap();
    let before = bus.next_model_step_id();
    let model_step;
    {
        let view = runtime.scoped_root_node_activity(&ticket, true).unwrap();
        let node_bus = view.cowd_bus().unwrap();
        let owner = node_bus.current_activity_binding().unwrap();
        assert_eq!(owner.node_id.as_deref(), Some(ticket.node_id.as_str()));
        assert_eq!(owner.root_execution_id, graph.id);
        assert!(node_bus.subscribe().same_channel(&bus.subscribe()));
        assert!(node_bus.has_live_telemetry_observer());
        model_step = node_bus.next_model_step_id();
        assert_ne!(model_step, before);
        assert_eq!(bus.current_activity_binding().unwrap(), root);
        assert_eq!(
            services
                .active_execution_bus(&graph.id)
                .unwrap()
                .current_activity_binding()
                .unwrap(),
            root
        );
    }
    assert_eq!(
        runtime
            .cowd_bus()
            .unwrap()
            .current_activity_binding()
            .unwrap(),
        root
    );
    assert_ne!(bus.next_model_step_id(), model_step);
    let mut foreign = ticket.clone();
    foreign.attempt += 1;
    assert!(runtime.scoped_root_node_activity(&foreign, true).is_err());
    assert_eq!(bus.current_activity_binding().unwrap(), root);
    let mut pending = Box::pin(async {
        let view = runtime.scoped_root_node_activity(&ticket, true).unwrap();
        std::future::pending::<()>().await;
        drop(view);
    });
    assert!(futures::poll!(pending.as_mut()).is_pending());
    assert_eq!(bus.current_activity_binding().unwrap(), root);
    drop(pending);
    assert_eq!(
        runtime
            .cowd_bus()
            .unwrap()
            .current_activity_binding()
            .unwrap(),
        root
    );
    assert!(bus.has_live_telemetry_observer());
    drop(guards);
    assert!(bus.current_execution_context().is_none());
    assert!(!bus.has_live_telemetry_observer());
}

fn r1_observation_branch_runtime<C: ApiClient + Send + Sync + 'static>(
    session: Session,
    client: C,
    services: &crate::RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    goal_id: &str,
) -> (
    Arc<tokio::sync::Mutex<Box<crate::ConversationRuntime<C, RecordingToolExecutor>>>>,
    TurnGraphState,
    (
        Option<crate::CowdExecutionScope>,
        Option<crate::execution_core::services::ActiveExecutionBusLease>,
    ),
) {
    let runtime = r1_observation_runtime(session, client, services);
    let scope = enter_owned_root_scope(services, runtime.cowd_bus().unwrap(), graph, None).unwrap();
    runtime
        .begin_turn_strategy("r1-branch-turn", &graph.objective)
        .unwrap();
    let strategy = runtime
        .bind_turn_strategy_execution("r1-branch-turn", &graph.id)
        .unwrap();
    assert!(strategy.decision.executable);
    assert_eq!(
        strategy.execution_graph_ref.as_deref(),
        Some(graph.id.as_str())
    );
    runtime
        .session_input_stream()
        .set_active_turn(Some(TurnId::from_string("r1-branch-turn")));
    let mut state = r1_branch_state(runtime.session_id(), goal_id);
    state.task_understanding = Some(strategy.decision.strategy.understanding.clone());
    state.collaboration_obligation = strategy.decision.collaboration_obligation.clone();
    state.context_window = runtime.model_context_window();
    state.safety_lease = crate::execution_core::SafetyFusePolicy::derive(
        state.context_window,
        strategy.decision.complexity(),
        None,
    );
    (Arc::new(tokio::sync::Mutex::new(Box::new(runtime))), state, scope)
}

fn r1_same_transaction_model_result(
    events: &crate::RuntimeEventStore,
    history: &[crate::DurableRuntimeEvent],
    owner: &crate::DurableRuntimeEvent,
    observation: &RuntimeObservation,
) -> ExecutionNodeResult {
    use crate::execution_core::graph::ExecutionGraphEvent;
    let node_id = observation.identity.node_id.as_ref().unwrap();
    let graph_id = &observation.identity.graph_id;
    let mut records = history
        .iter()
        .filter(|record| {
            record.scope == crate::RuntimeEventScope::ExecutionGraph
                && record.stream_id == *graph_id
                && record.commit_cursor <= owner.commit_cursor
        })
        .collect::<Vec<_>>();
    records.sort_by_key(|record| (record.commit_cursor, record.transaction_index));
    let mut previous: Option<harness_contract::execution_graph::ExecutionGraph> = None;
    let mut matched = Vec::new();
    for record in records {
        assert_eq!(record.schema_version, 2);
        let value: ExecutionGraphEvent =
            serde_json::from_value(events.resolve_graph_record(record).unwrap()).unwrap();
        assert_eq!(value.kind(), record.kind);
        let projected = value.project(previous.clone()).unwrap();
        assert_eq!(&projected.id, graph_id);
        if record.transaction_id == owner.transaction_id {
            assert_eq!(record.commit_cursor, owner.commit_cursor);
            assert!(
                previous
                    .as_ref()
                    .is_some_and(|graph| !graph.node_results.contains_key(node_id)),
                "observation transaction must introduce its own model result"
            );
            let result = projected
                .node_results
                .get(node_id)
                .expect("same-transaction canonical model result")
                .clone();
            match &value {
                ExecutionGraphEvent::NodeTransitioned {
                    node_id: changed,
                    to,
                    result: Some(explicit),
                    ..
                }
                | ExecutionGraphEvent::NodeTransitionedAndReplanned {
                    node_id: changed,
                    to,
                    result: explicit,
                    ..
                } => {
                    assert_eq!(changed, node_id);
                    assert_eq!(*to, ExecutionNodeStatus::Completed);
                    assert_eq!(explicit, &result);
                }
                ExecutionGraphEvent::Checkpoint { cause, .. } => {
                    assert!(matches!(
                        cause.as_str(),
                        "execution_graph.node_transitioned"
                            | "execution_graph.node_transitioned_and_replanned"
                    ));
                }
                _ => panic!("observation transaction lacks a canonical model result transition"),
            }
            assert_eq!(result.status, ExecutionNodeStatus::Completed);
            assert_eq!(
                projected.node_statuses[node_id],
                ExecutionNodeStatus::Completed
            );
            assert!(projected
                .nodes
                .iter()
                .any(|node| node.id == *node_id && node.kind == ExecutionNodeKind::InlineModel));
            matched.push(result);
        }
        previous = Some(projected);
    }
    assert_eq!(
        matched.len(),
        1,
        "one exact canonical graph result must share the observation transaction"
    );
    matched.pop().unwrap()
}

fn r1_branch_graph(
    services: &crate::RuntimeServices,
    session: &str,
    predecessors: Vec<(ExecutionNodeSpec, ExecutionNodeResult)>,
    kind: ExecutionNodeKind,
) -> (
    harness_contract::execution_graph::ExecutionGraph,
    NodeExecutionTicket,
) {
    r1_branch_graph_with_payload(services, session, predecessors, kind, "{}")
}

fn own862_start_host_fixture_node(
    services: &crate::RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    node: &ExecutionNodeSpec,
) -> (
    harness_contract::execution_graph::ExecutionGraph,
    NodeExecutionTicket,
) {
    let ready = services
        .commit_service()
        .transition_node(graph, &node.id, ExecutionNodeStatus::Ready, None, vec![])
        .unwrap()
        .graph;
    let ticket = NodeExecutionTicket {
        graph_id: ready.id.clone(),
        node_id: node.id.clone(),
        executor_kind: node.executor_kind.clone(),
        service_class: ready.service_class,
        attempt: 1,
        idempotency_key: node.idempotency_key.clone(),
        payload_ref: node.payload_ref.clone(),
    };
    let binding = crate::execution_core::graph::ExecutionNodeBinding {
        executor_kind: ticket.executor_kind.clone(),
        ticket_idempotency_key: ticket.idempotency_key.clone(),
        attempt: ticket.attempt,
        resource_lease_refs: vec![],
        scope_lease_ref: None,
        worktree_lease_ref: None,
    };
    let leaf = node.kind == ExecutionNodeKind::ToolBatch && node.executor_kind == "tool_batch";
    let running = services
        .commit_service()
        .bind_and_start_nodes(
            &ready,
            std::collections::BTreeMap::from([(node.id.clone(), binding)]),
            if leaf { vec![] } else { vec![ticket.clone()] },
        )
        .unwrap()
        .graph;
    let effect_stream =
        crate::execution_core::graph::ExecutionNodeBinding::effect_stream_id(&ticket);
    let intent = services
        .event_store()
        .event_by_idempotency_key(
            &effect_stream,
            &format!("{}:intent", ticket.idempotency_key),
        )
        .unwrap();
    assert_eq!(intent.is_some(), !leaf);
    if let Some(intent) = intent {
        assert_eq!(intent.kind, "execution.effect.intent");
        let start = services
            .event_store()
            .event_by_idempotency_key(
                &format!("{}:node:{}", running.id, node.id),
                &format!("{}:{}:{}", running.id, node.id, running.revision),
            )
            .unwrap();
        assert_eq!(start.unwrap().transaction_id, intent.transaction_id);
    }
    (running, ticket)
}

fn r1_branch_graph_with_payload(
    services: &crate::RuntimeServices,
    session: &str,
    predecessors: Vec<(ExecutionNodeSpec, ExecutionNodeResult)>,
    kind: ExecutionNodeKind,
    payload: &str,
) -> (
    harness_contract::execution_graph::ExecutionGraph,
    NodeExecutionTicket,
) {
    use harness_contract::execution_graph::{ExecutionGraph, ExecutionGraphLineage};
    let mut graph = ExecutionGraph::new("answer directly");
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: session.into(),
        turn_id: "r1-branch-turn".into(),
        root_task_id: "r1-task".into(),
        task_id: "r1-task".into(),
        generation: 1,
    });
    let mut node = ExecutionNodeSpec::new(
        kind,
        match kind {
            ExecutionNodeKind::Synthesize => "synthesize",
            ExecutionNodeKind::ToolBatch => "tool_batch",
            _ => "inline_model",
        },
        payload,
    );
    node.id = format!("{}:target", graph.id);
    node.idempotency_key = format!("{}:attempt", node.id);
    for (predecessor, _) in &predecessors {
        graph.edges.push(ExecutionEdge {
            from: predecessor.id.clone(),
            to: node.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes.push(predecessor.clone());
    }
    graph.nodes.push(node.clone());
    graph = services
        .commit_service()
        .register_graph(graph)
        .unwrap()
        .graph;
    for (predecessor, result) in predecessors {
        for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
            graph = services
                .commit_service()
                .transition_node(&graph, &predecessor.id, status, None, vec![])
                .unwrap()
                .graph;
        }
        graph = services
            .commit_service()
            .transition_node(&graph, &predecessor.id, result.status, Some(result), vec![])
            .unwrap()
            .graph;
    }
    let (graph, ticket) = own862_start_host_fixture_node(services, &graph, &node);
    let lineage = graph.lineage.as_ref().unwrap();
    services.record_live_execution(session, graph.id.clone(), lineage.turn_id.clone());
    (graph, ticket)
}

fn r1_commit_host_outcome(
    services: &crate::RuntimeServices,
    graph: &harness_contract::execution_graph::ExecutionGraph,
    ticket: &NodeExecutionTicket,
    outcome: NodeExecutionOutcome,
) -> Vec<crate::DurableRuntimeEvent> {
    for input in outcome
        .domain_events
        .iter()
        .filter(|input| input.event.kind == "goal.observation")
    {
        let goal_id = input.event.stream_id.strip_prefix("goal:").unwrap();
        let source = services.goal_store().projection(goal_id).unwrap().unwrap();
        assert_eq!(
            input.event.payload["goal_source"],
            serde_json::json!({
                "stream_id":input.event.stream_id,"sequence":source.stream_revision,"goal_revision":source.goal.revision,"spec_digest":source.goal.spec_digest,
            })
        );
    }
    let sources = services
        .commit_service()
        .execution_outcome_sources(graph, ticket, &outcome)
        .unwrap();
    let receipt = services
        .commit_service()
        .transition_execution_outcome(graph, ticket, &outcome, &sources)
        .unwrap();
    let history = services.event_store().all_events(100_000).unwrap();
    let mut committed = history
        .iter()
        .filter(|event| event.transaction_id == receipt.transaction.transaction_id)
        .collect::<Vec<_>>();
    committed.sort_by_key(|event| event.transaction_index);
    assert_eq!(
        committed
            .iter()
            .map(|event| event.event_id.clone())
            .collect::<Vec<_>>(),
        receipt.transaction.event_ids
    );
    let replay = crate::AppendTransactionRequest {
        transaction_id: receipt.transaction.transaction_id.clone(),
        expected_streams: receipt
            .transaction
            .stream_revisions
            .iter()
            .map(|source| crate::ExpectedStreamRevision {
                stream_id: source.stream_id.clone(),
                expected_revision: source.expected_revision,
            })
            .collect(),
        events: committed
            .into_iter()
            .map(|event| crate::RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: event.stream_id.clone(),
                    scope: event.scope,
                    kind: event.kind.clone(),
                    status: event.status.clone(),
                    actor: event.actor.clone(),
                    refs: event.refs.clone(),
                    payload: event.payload.clone(),
                },
                idempotency_key: event.idempotency_key.clone(),
                schema_version: event.schema_version,
            })
            .collect(),
    };
    let duplicate = services.event_store().append_transaction(replay).unwrap();
    assert!(duplicate.duplicate);
    assert_eq!(duplicate.transaction_id, receipt.transaction.transaction_id);
    assert_eq!(duplicate.request_hash, receipt.transaction.request_hash);
    assert_eq!(duplicate.commit_cursor, receipt.transaction.commit_cursor);
    assert_eq!(duplicate.event_ids, receipt.transaction.event_ids);
    assert_eq!(
        duplicate.stream_revisions,
        receipt.transaction.stream_revisions
    );
    assert_eq!(services.event_store().all_events(100_000).unwrap(), history);
    let events = history
        .iter()
        .filter(|event| {
            event.transaction_id == receipt.transaction.transaction_id
                && event.kind == "goal.observation"
        })
        .cloned()
        .collect::<Vec<_>>();
    for event in &events {
        assert_eq!(event.commit_cursor, receipt.transaction.commit_cursor);
        let observation: RuntimeObservation = serde_json::from_value(
            services
                .event_store()
                .resolve_observation_record(event)
                .unwrap(),
        )
        .unwrap();
        if observation.source == "runtime.model_step" {
            r1_same_transaction_model_result(services.event_store(), &history, event, &observation);
        }
    }
    let cold =
        crate::execution_core::graph::ExecutionGraphStateStore::new(services.event_store().clone())
            .load(&graph.id)
            .unwrap();
    assert_eq!(cold, receipt.graph);
    events
}

fn r1_exact_host_observation(
    services: &crate::RuntimeServices,
    records: &[crate::DurableRuntimeEvent],
    ticket: &NodeExecutionTicket,
    suffix: &str,
    source: &str,
) -> RuntimeObservation {
    let key = format!("{}:{suffix}", ticket.idempotency_key);
    let matches = records
        .iter()
        .filter(|event| event.idempotency_key.as_deref() == Some(key.as_str()))
        .collect::<Vec<_>>();
    assert_eq!(matches.len(), 1, "exact source {source} key {key}");
    let observation: RuntimeObservation = serde_json::from_value(
        services
            .event_store()
            .resolve_observation_record(matches[0])
            .unwrap(),
    )
    .unwrap();
    assert_eq!(observation.source, source);
    assert_eq!(observation.identity.graph_id, ticket.graph_id);
    assert_eq!(observation.identity.workspace_id, services.workspace_key());
    assert_eq!(observation.identity.turn_id, None);
    assert_eq!(observation.identity.task_id, None);
    assert_eq!(
        matches[0].stream_id,
        format!("goal:{}", observation.identity.goal_id)
    );
    assert_eq!(observation.freshness.policy_revision, "goal-observation-v2");
    observation
}

fn r1_project_input(stream: &crate::SessionInputStream, session: &str, content: &str) -> String {
    use crate::input_classifier::RuntimeInputState;
    use harness_contract::turn::{InputSourceKind, SessionInputCursor};
    let turn = TurnId::from_string("r1-branch-turn");
    stream.set_active_turn(Some(turn.clone()));
    let envelope = SessionInputEnvelope::text(session, InputSourceKind::Webui, content);
    let input_id = envelope.input_id.to_string();
    let mut receipt = stream.admit(envelope.clone(), RuntimeInputState::active(turn));
    receipt.cursor = Some(SessionInputCursor::new(1, 41));
    stream.project_durable(envelope, receipt);
    input_id
}

fn r1_agent_predecessor(team: bool) -> ExecutionNodeSpec {
    let packet = harness_contract::agent::AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None,
            "r1-agent",
            "r1-run",
            "r1-task",
            "r1-session",
            "r1-mission",
            team.then_some("r1-team"),
            "r1-parent",
            "r1-predecessor",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        objective: "answer directly".into(),
        required_acceptance: Default::default(),
        output_acceptance: vec![],
        cohort_prompt_package: None,
        acceptance: vec!["checked".into()],
        constraints: vec![],
        context_refs: vec![],
        agentic_binding: None,
        evidence_refs: vec![],
        resource_scopes: vec![],
        allowed_tools: vec![],
        allowed_skills: vec![],
        permission_ceiling: PermissionMode::ReadOnly,
        policy_revision: 1,
        model_lease: "test-model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "r1-budget",
            "r1-agent",
            "r1-task",
            100_000,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "r1-agent-attempt".into(),
    };
    assert_eq!(packet.team_id(), team.then_some("r1-team"));
    let encoded = serde_json::to_string(&packet).unwrap();
    let decoded: harness_contract::agent::AgentTaskPacket = serde_json::from_str(&encoded).unwrap();
    assert_eq!(decoded, packet);
    ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent_task", encoded)
}

#[derive(Clone)]
struct R1InputBoundaryClient {
    inject: Arc<Mutex<Option<(crate::SessionInputStream, String)>>>,
    input_id: Arc<Mutex<Option<String>>>,
}

impl ApiClient for R1InputBoundaryClient {
    fn stream(
        &mut self,
        _request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        if let Some((stream, session)) = self.inject.lock().unwrap().take() {
            *self.input_id.lock().unwrap() =
                Some(r1_project_input(&stream, &session, "late supplement"));
        }
        Box::pin(stream::iter(vec![
            Ok(AssistantEvent::TextDelta("terminal answer".into())),
            Ok(AssistantEvent::MessageStop),
        ]))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn r1_event_replay_host_branch_sources_are_exact() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    use crate::execution_core::graph::executors::{ScopedNodeBackend, SynthesizeBackend};
    for mode in ["block", "continue", "clean", "override", "disposition"] {
        let services = r1_observation_services();
        let session = Session::new();
        let goal_id = r1_branch_goal(&services, &session.session_id);
        let (graph, ticket) = r1_branch_graph(
            &services,
            &session.session_id,
            vec![],
            ExecutionNodeKind::InlineModel,
        );
        let (runtime, mut state, _scope) = r1_observation_branch_runtime(
            session.clone(),
            FinalAnswerClient,
            &services,
            &graph,
            &goal_id,
        );
        state.consecutive_unverified_model_steps = state.safety_lease.max_model_steps;
        match mode {
            "continue" => state.last_verified_progress = true,
            "clean" => state.clean_terminal_synthesis_next = true,
            "override" => {
                state.terminal_override = Some((GoalCompletion::Partial, "already blocked".into()))
            }
            "disposition" => {
                let stream = runtime.lock().await.session_input_stream();
                let input_id = r1_project_input(&stream, &session.session_id, "new input");
                let inputs = stream.consume_for_checkpoint(
                    &TurnId::from_string("r1-branch-turn"),
                    TurnInputCheckpoint::BeforeProviderRequest,
                    1,
                );
                assert_eq!(inputs[0].envelope.input_id.to_string(), input_id);
                state.pending_disposition_inputs = inputs;
            }
            _ => {}
        }
        let state = Arc::new(tokio::sync::Mutex::new(state));
        let backend = TurnModelStepBackend {
            ticket: ticket.clone(),
            stop: crate::CancellationToken::new(),
            local_fence: std::sync::Mutex::new(None),
            runtime,
            state: state.clone(),
            services: services.clone(),
        };
        let outcome = backend.execute(&ticket).await.unwrap();
        if mode == "block" {
            let replan = outcome.replan.as_ref().unwrap();
            assert_eq!(replan.nodes.len(), 1);
            assert_eq!(replan.nodes[0].kind, ExecutionNodeKind::Synthesize);
            assert!(replan.nodes[0].id.ends_with("safety-block-synthesize"));
            let intervention = outcome
                .domain_events
                .iter()
                .find(|input| {
                    input.idempotency_key.as_deref()
                        == Some(format!("{}:safety-intervention", ticket.idempotency_key).as_str())
                })
                .unwrap();
            assert_eq!(intervention.event.payload["intervention"]["kind"], "block");
            let records = r1_commit_host_outcome(&services, &graph, &ticket, outcome);
            let observation = r1_exact_host_observation(
                &services,
                &records,
                &ticket,
                "safety-observation",
                "runtime.safety_fuse",
            );
            assert_eq!(observation.kind, RuntimeObservationKind::StrategyHistory);
            assert_eq!(observation.result_class, ObservationResultClass::Failed);
            assert_eq!(
                observation.failure_class,
                Some(ObservationFailureClass::Policy)
            );
            assert_eq!(
                observation.identity.node_id.as_deref(),
                Some(ticket.node_id.as_str())
            );
            assert_eq!(observation.cost_delta, CostDelta::default());
            assert!(observation.evidence_refs.is_empty());
            assert_eq!(
                observation.fingerprint,
                format!(
                    "safety-fuse:{}:{}",
                    ticket.node_id,
                    state.lock().await.safety_lease.max_model_steps
                )
            );
        } else {
            assert!(
                outcome.domain_events.iter().all(|input| !input
                    .idempotency_key
                    .as_ref()
                    .unwrap()
                    .ends_with(":safety-observation")),
                "{mode}"
            );
        }
    }

    for (kind, source, team, failure_class) in [
        (
            ExecutionNodeKind::Approval,
            "runtime.approval_result",
            false,
            ObservationFailureClass::Approval,
        ),
        (
            ExecutionNodeKind::Verify,
            "runtime.verification_result",
            false,
            ObservationFailureClass::Verification,
        ),
        (
            ExecutionNodeKind::AgentTask,
            "runtime.agent_result",
            false,
            ObservationFailureClass::Unknown,
        ),
        (
            ExecutionNodeKind::AgentTask,
            "runtime.team_agent_result",
            true,
            ObservationFailureClass::Unknown,
        ),
    ] {
        for (status, materialized) in [
            (ExecutionNodeStatus::Completed, false),
            (ExecutionNodeStatus::Completed, true),
            (ExecutionNodeStatus::Failed, true),
            (ExecutionNodeStatus::Blocked, true),
            (ExecutionNodeStatus::Cancelled, true),
        ] {
            let services = r1_observation_services();
            let session = Session::new();
            let goal_id = r1_branch_goal(&services, &session.session_id);
            let predecessor = if kind == ExecutionNodeKind::AgentTask {
                r1_agent_predecessor(team)
            } else {
                ExecutionNodeSpec::new(kind, "fixture_predecessor", "{}")
            };
            let predecessor_id = predecessor.id.clone();
            let predecessor_key = predecessor.idempotency_key.clone();
            let mut result = completed_result(
                Some("r1-receipt".into()),
                ExecutionUsage {
                    model: Some("test-model".into()),
                    tool_calls: 2,
                    input_tokens: 11,
                    output_tokens: 7,
                    cached_tokens: 3,
                    duration_ms: 5,
                    ..Default::default()
                },
            );
            result.status = status;
            result.finished_at_ms = 17;
            if materialized {
                let artifact = services
                    .artifact_store()
                    .write_bytes(
                        harness_contract::context::ArtifactWriteDescriptor {
                            media_type: "text/plain".into(),
                            visibility_scope: format!("session:{}", session.session_id),
                            expected_bytes: Some(17),
                            original_name: None,
                        },
                        b"upstream evidence",
                    )
                    .await
                    .unwrap();
                result
                    .evidence_refs
                    .push(harness_contract::context::EvidenceAccessRef::durable(
                        harness_contract::reality::EvidenceRef::observed(
                            "artifact",
                            artifact.selector.clone(),
                        ),
                        artifact.sha256.clone(),
                        artifact.bytes,
                        artifact.media_type.clone(),
                        artifact.selector.clone(),
                        artifact.visibility_scope.clone(),
                    ));
            }
            let evidence_refs = result
                .evidence_refs
                .iter()
                .map(|access| access.evidence_ref.id.clone())
                .collect::<Vec<_>>();
            let (graph, ticket) = r1_branch_graph(
                &services,
                &session.session_id,
                vec![(predecessor, result)],
                ExecutionNodeKind::InlineModel,
            );
            let (runtime, branch_state, _scope) = r1_observation_branch_runtime(
                session.clone(),
                FinalAnswerClient,
                &services,
                &graph,
                &goal_id,
            );
            let backend = TurnModelStepBackend {
                ticket: ticket.clone(),
                stop: crate::CancellationToken::new(),
                local_fence: std::sync::Mutex::new(None),
                runtime,
                state: Arc::new(tokio::sync::Mutex::new(branch_state)),
                services: services.clone(),
            };
            let outcome = backend.execute(&ticket).await.unwrap();
            assert!(
                outcome.domain_events.iter().all(|input| !input
                    .idempotency_key
                    .as_deref()
                    .is_some_and(|key| key.ends_with(":provider-failure-observation"))),
                "upstream source fixture must reach its successful model continuation: {:?}",
                outcome.domain_events
            );
            let records = r1_commit_host_outcome(&services, &graph, &ticket, outcome);
            let observation = r1_exact_host_observation(
                &services,
                &records,
                &ticket,
                "upstream-observation:0",
                source,
            );
            assert_eq!(observation.kind, RuntimeObservationKind::GraphProgress);
            assert_eq!(
                observation.identity.node_id.as_deref(),
                Some(predecessor_id.as_str())
            );
            assert_eq!(observation.identity.goal_id, goal_id);
            assert_eq!(observation.identity.session_id, session.session_id);
            assert_eq!(observation.source_revision, 17);
            assert_eq!(
                observation.result_class,
                if status == ExecutionNodeStatus::Completed {
                    ObservationResultClass::Succeeded
                } else {
                    ObservationResultClass::Failed
                }
            );
            assert_eq!(
                observation.failure_class,
                if status == ExecutionNodeStatus::Completed {
                    None
                } else {
                    Some(failure_class)
                }
            );
            let expected_evidence = evidence_refs
                .iter()
                .cloned()
                .chain(std::iter::once("execution_result:r1-receipt".into()))
                .collect::<Vec<_>>();
            assert_eq!(observation.evidence_refs, expected_evidence);
            assert_eq!(
                observation.evidence_delta.added,
                if status == ExecutionNodeStatus::Completed {
                    expected_evidence
                } else {
                    vec![]
                }
            );
            assert_eq!(
                observation.information_gain.provenance,
                if materialized && status == ExecutionNodeStatus::Completed {
                    MeasureProvenance::Observed
                } else {
                    MeasureProvenance::Unknown
                }
            );
            assert_eq!(
                observation.information_gain.distinguishing_evidence_refs,
                if status == ExecutionNodeStatus::Completed {
                    evidence_refs
                } else {
                    vec![]
                }
            );
            assert_eq!(
                observation.effect_deltas,
                vec![EffectDelta {
                    effect_id: format!("execution-node:{predecessor_id}"),
                    terminal_class: match status {
                        ExecutionNodeStatus::Completed => EffectTerminalClass::Completed,
                        ExecutionNodeStatus::Cancelled => EffectTerminalClass::Cancelled,
                        _ => EffectTerminalClass::Failed,
                    },
                    idempotency_ref: predecessor_key,
                }]
            );
            assert_eq!(
                observation.cost_delta,
                CostDelta {
                    model_steps: 1,
                    tool_calls: 2,
                    duration_ms: 5,
                    input_tokens: 11,
                    output_tokens: 7,
                    cached_tokens: 3
                }
            );
            assert!(services
                .goal_store()
                .projection(&goal_id)
                .unwrap()
                .unwrap()
                .progress
                .applied_observation_keys
                .contains(&observation.idempotency_fingerprint()));
            let original_projection = services.goal_store().projection(&goal_id).unwrap().unwrap();
            let original_head = services
                .event_store()
                .stream_revision(&format!("goal:{goal_id}"))
                .unwrap();
            let original_history = services.event_store().all_events(100_000).unwrap();
            assert_eq!(
                services.event_store().all_events(100_000).unwrap(),
                original_history
            );
            let cold = crate::execution_core::GoalStore::new(services.event_store().clone());
            for original in &records {
                let original_observation: RuntimeObservation = serde_json::from_value(
                    services
                        .event_store()
                        .resolve_observation_record(original)
                        .unwrap(),
                )
                .unwrap();
                let key = original.idempotency_key.as_ref().unwrap();
                for goals in [services.goal_store().as_ref(), &cold] {
                    let replay = goals
                        .observation_event(&original_observation, key.clone())
                        .unwrap();
                    assert_eq!(replay.event.payload, original.payload);
                    assert_eq!(replay.event.kind, original.kind);
                    assert_eq!(replay.event.stream_id, original.stream_id);
                    assert_eq!(replay.event.scope, original.scope);
                    assert_eq!(replay.event.status, original.status);
                    assert_eq!(replay.event.actor, original.actor);
                    assert_eq!(replay.event.refs, original.refs);
                    assert_eq!(replay.schema_version, original.schema_version);
                    assert_eq!(replay.idempotency_key.as_ref(), Some(key));
                    for mutation in ["summary", "duration", "freshness", "source_revision"] {
                        let mut changed = original_observation.clone();
                        match mutation {
                            "summary" => changed.summary.push_str(" different semantic payload"),
                            "duration" => changed.cost_delta.duration_ms += 1,
                            "freshness" => changed.freshness.observed_at_ms += 1,
                            _ => changed.source_revision += 1,
                        }
                        let error = goals
                            .observation_event(&changed, key.clone())
                            .err()
                            .expect("same-key changed observation must fail closed");
                        assert_eq!(
                            error, "observation retry conflicts with original current-schema bytes",
                            "{mutation}"
                        );
                    }
                    assert_eq!(
                        goals.projection(&goal_id).unwrap().unwrap(),
                        original_projection
                    );
                }
            }
            assert_eq!(
                services
                    .event_store()
                    .stream_revision(&format!("goal:{goal_id}"))
                    .unwrap(),
                original_head
            );
            let mut successor = ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                ticket.executor_kind.clone(),
                "{}",
            );
            successor.idempotency_key = format!("{}:next-source-consumer", ticket.idempotency_key);
            let current = services.graph_state_store().load(&graph.id).unwrap();
            let planned = services
                .commit_service()
                .replan(
                    &current,
                    vec![successor.clone()],
                    vec![ExecutionEdge {
                        from: predecessor_id.clone(),
                        to: successor.id.clone(),
                        kind: ExecutionEdgeKind::DependsOn,
                    }],
                    "a new model step consumes the same committed predecessor".into(),
                )
                .unwrap()
                .graph;
            let (running, next_ticket) =
                own862_start_host_fixture_node(&services, &planned, &successor);
            assert_ne!(next_ticket.node_id, ticket.node_id);
            assert_ne!(next_ticket.idempotency_key, ticket.idempotency_key);
            let repeated = backend.execute(&next_ticket).await.unwrap();
            assert!(repeated.domain_events.iter().all(|input| !input
                .idempotency_key
                .as_ref()
                .unwrap()
                .contains(":upstream-observation:")));
            let next_records = r1_commit_host_outcome(&services, &running, &next_ticket, repeated);
            let next_model = r1_exact_host_observation(
                &services,
                &next_records,
                &next_ticket,
                "goal-observation",
                "runtime.model_step",
            );
            let previous_model = r1_exact_host_observation(
                &services,
                &records,
                &ticket,
                "goal-observation",
                "runtime.model_step",
            );
            assert_ne!(
                next_model.idempotency_fingerprint(),
                previous_model.idempotency_fingerprint()
            );
            let expected = services.goal_store().projection(&goal_id).unwrap().unwrap();
            assert_eq!(
                expected
                    .observations
                    .iter()
                    .filter(|candidate| candidate.idempotency_fingerprint()
                        == observation.idempotency_fingerprint())
                    .count(),
                1
            );
            assert!(
                expected.progress.observation_count
                    > original_projection.progress.observation_count
            );
            assert_eq!(
                expected.progress.observation_count,
                expected.progress.applied_observation_keys.len() as u64
            );
            services
                .event_store()
                .delete_projection_checkpoint(&format!("goal-projection:{goal_id}"))
                .unwrap();
            assert_eq!(
                crate::execution_core::GoalStore::new(services.event_store().clone())
                    .projection(&goal_id)
                    .unwrap()
                    .unwrap(),
                expected
            );
        }
    }

    for boundary in ["empty", "before", "after"] {
        let services = r1_observation_services();
        let session = Session::new();
        let goal_id = r1_branch_goal(&services, &session.session_id);
        let (graph, ticket) = r1_branch_graph(
            &services,
            &session.session_id,
            vec![],
            ExecutionNodeKind::InlineModel,
        );
        let inject = Arc::new(Mutex::new(None));
        let input_id = Arc::new(Mutex::new(None));
        let client = R1InputBoundaryClient {
            inject: inject.clone(),
            input_id: input_id.clone(),
        };
        let (runtime, branch_state, _scope) =
            r1_observation_branch_runtime(session.clone(), client, &services, &graph, &goal_id);
        let stream = runtime.lock().await.session_input_stream();
        stream.set_active_turn(Some(TurnId::from_string("r1-branch-turn")));
        if boundary == "before" {
            *input_id.lock().unwrap() = Some(r1_project_input(
                &stream,
                &session.session_id,
                "early supplement",
            ));
        } else if boundary == "after" {
            *inject.lock().unwrap() = Some((stream, session.session_id.clone()));
        }
        let backend = TurnModelStepBackend {
            ticket: ticket.clone(),
            stop: crate::CancellationToken::new(),
            local_fence: std::sync::Mutex::new(None),
            runtime,
            state: Arc::new(tokio::sync::Mutex::new(branch_state)),
            services: services.clone(),
        };
        let outcome = backend.execute(&ticket).await.unwrap();
        let records = r1_commit_host_outcome(&services, &graph, &ticket, outcome);
        if boundary == "empty" {
            assert!(records.iter().all(|event| !event
                .idempotency_key
                .as_ref()
                .unwrap()
                .ends_with(":input-observation")));
        } else {
            let observation = r1_exact_host_observation(
                &services,
                &records,
                &ticket,
                "input-observation",
                "runtime.session_input_checkpoint",
            );
            assert_eq!(observation.kind, RuntimeObservationKind::UserInput);
            assert_eq!(
                observation.result_class,
                ObservationResultClass::Informational
            );
            assert_eq!(observation.failure_class, None);
            assert_eq!(
                observation.evidence_refs,
                vec![format!(
                    "session_input:{}",
                    input_id.lock().unwrap().as_ref().unwrap()
                )]
            );
            assert_eq!(observation.cost_delta, CostDelta::default());
            assert_eq!(
                observation.information_gain.provenance,
                if boundary == "before" {
                    MeasureProvenance::Observed
                } else {
                    MeasureProvenance::Unknown
                }
            );
            assert_eq!(
                observation.unknown_deltas.len(),
                usize::from(boundary == "before")
            );
            if boundary == "before" {
                assert_eq!(
                    observation.unknown_deltas[0].change,
                    ResolutionDeltaKind::Opened
                );
                let model = r1_exact_host_observation(
                    &services,
                    &records,
                    &ticket,
                    "goal-observation",
                    "runtime.model_step",
                );
                assert_eq!(model.unknown_deltas.len(), 1);
                assert_eq!(
                    model.unknown_deltas[0].change,
                    ResolutionDeltaKind::Resolved
                );
                assert_eq!(
                    model.unknown_deltas[0].unknown_id,
                    observation.unknown_deltas[0].unknown_id
                );
            }
        }
    }

    let services = r1_observation_services();
    let session = Session::new();
    let goal_id = r1_branch_goal(&services, &session.session_id);
    let (graph, ticket) = r1_branch_graph(
        &services,
        &session.session_id,
        vec![],
        ExecutionNodeKind::Synthesize,
    );
    let (runtime, branch_state, _scope) = r1_observation_branch_runtime(
        session.clone(),
        FinalAnswerClient,
        &services,
        &graph,
        &goal_id,
    );
    let input_id = r1_project_input(
        &runtime.lock().await.session_input_stream(),
        &session.session_id,
        "replace terminal candidate",
    );
    let backend = TurnSynthesizeBackend {
        runtime,
        state: Arc::new(tokio::sync::Mutex::new(branch_state)),
        services: services.clone(),
    };
    let outcome = backend.synthesize(&ticket).await.unwrap();
    assert_eq!(
        outcome.result.result_ref.as_deref(),
        Some(format!("{}:terminal-superseded", graph.id).as_str())
    );
    assert!(outcome.replan.as_ref().unwrap().nodes[0]
        .id
        .ends_with("input-cursor-replan-model"));
    let records = r1_commit_host_outcome(&services, &graph, &ticket, outcome);
    let observation = r1_exact_host_observation(
        &services,
        &records,
        &ticket,
        "terminal-input-observation",
        "runtime.before_final_answer",
    );
    assert_eq!(observation.kind, RuntimeObservationKind::UserInput);
    assert_eq!(
        observation.result_class,
        ObservationResultClass::Informational
    );
    assert_eq!(observation.failure_class, None);
    assert_eq!(
        observation.evidence_refs,
        vec![format!("session_input:{input_id}")]
    );
    assert_eq!(observation.cost_delta, CostDelta::default());
    assert!(services
        .event_store()
        .session_terminal_for_execution(&session.session_id, &graph.id, "r1-branch-turn")
        .unwrap()
        .is_none());
}





#[derive(Clone)]
struct R1NarratorFenceClient {
    entered: Arc<tokio::sync::Notify>,
    release: Arc<tokio::sync::Notify>,
    attempts: Arc<AtomicUsize>,
}

impl ApiClient for R1NarratorFenceClient {
    fn stream(
        &mut self,
        _request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        let entered = self.entered.clone();
        let release = self.release.clone();
        Box::pin(
            stream::once(async move {
                entered.notify_one();
                release.notified().await;
                Ok(AssistantEvent::TextDelta(
                    "Execution is partial; checked evidence is retained.".into(),
                ))
            })
            .chain(stream::iter(vec![Ok(AssistantEvent::MessageStop)])),
        )
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn r1_event_replay_host_after_narration_fence_does_not_emit_before_fence_observation() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    use crate::execution_core::graph::executors::SynthesizeBackend;
    let services = r1_observation_services();
    let session = Session::new();
    let goal_id = r1_branch_goal(&services, &session.session_id);
    let (graph, ticket) = r1_branch_graph(
        &services,
        &session.session_id,
        vec![],
        ExecutionNodeKind::Synthesize,
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let runtime = Arc::new(tokio::sync::Mutex::new(
        Box::new(crate::ConversationRuntime::new(
            session.clone(),
            R1NarratorFenceClient {
                entered: entered.clone(),
                release: release.clone(),
                attempts: attempts.clone(),
            },
            NoopToolExecutor,
            PermissionPolicy::new(PermissionMode::DangerFullAccess),
            vec!["answer directly".into()],
        )
        .without_memory()
        .with_runtime_event_store(services.event_store().clone())
        .with_artifact_store(services.artifact_store().clone())),
    ));
    let stream = runtime.lock().await.session_input_stream();
    stream.set_active_turn(Some(TurnId::from_string("r1-branch-turn")));
    let mut state = r1_branch_state(&session.session_id, &goal_id);
    state.terminal_override = Some((
        GoalCompletion::Partial,
        "Execution blocked safely: additional verified evidence is required.".into(),
    ));
    let backend = TurnSynthesizeBackend {
        runtime,
        state: Arc::new(tokio::sync::Mutex::new(state)),
        services: services.clone(),
    };
    let ticket_for_task = ticket.clone();
    let task = tokio::spawn(async move { backend.synthesize(&ticket_for_task).await });
    tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified())
        .await
        .expect("real narrator entered before input injection");
    let input_id = r1_project_input(
        &stream,
        &session.session_id,
        "new evidence while narrator is active",
    );
    release.notify_one();
    let outcome = tokio::time::timeout(std::time::Duration::from_secs(5), task)
        .await
        .expect("narrator released")
        .unwrap()
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        outcome.result.result_ref.as_deref(),
        Some(format!("{}:terminal-superseded-after-narration", graph.id).as_str())
    );
    assert!(outcome
        .domain_events
        .iter()
        .all(|input| input.event.kind != "goal.observation"));
    assert!(outcome.replan.as_ref().unwrap().nodes[0]
        .id
        .ends_with("post-narrator-input-replan-model"));
    let records = r1_commit_host_outcome(&services, &graph, &ticket, outcome);
    assert!(records.is_empty());
    assert_eq!(
        stream
            .record_snapshot(&harness_contract::turn::SessionInputId::from_string(
                input_id
            ))
            .unwrap()
            .checkpoint,
        Some(TurnInputCheckpoint::BeforeFinalAnswer)
    );
    assert!(services
        .event_store()
        .session_terminal_for_execution(&session.session_id, &graph.id, "r1-branch-turn")
        .unwrap()
        .is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn r1_event_replay_host_upstream_non_sources_do_not_emit() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    use crate::execution_core::graph::executors::ScopedNodeBackend;
    for mode in ["nonterminal", "missing_result", "unsupported_kind"] {
        let services = r1_observation_services();
        let session = Session::new();
        let goal_id = r1_branch_goal(&services, &session.session_id);
        let mut graph = harness_contract::execution_graph::ExecutionGraph::new("answer directly");
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: session.session_id.clone(),
            turn_id: "r1-branch-turn".into(),
            root_task_id: "r1-task".into(),
            task_id: "r1-task".into(),
            generation: 1,
        });
        let predecessor = ExecutionNodeSpec::new(
            if mode == "unsupported_kind" {
                ExecutionNodeKind::ToolBatch
            } else {
                ExecutionNodeKind::Approval
            },
            "fixture_predecessor",
            "{}",
        );
        let node = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
        graph.edges.push(ExecutionEdge {
            from: predecessor.id.clone(),
            to: node.id.clone(),
            kind: ExecutionEdgeKind::DependsOn,
        });
        graph.nodes = vec![predecessor.clone(), node.clone()];
        graph = services
            .commit_service()
            .register_graph(graph)
            .unwrap()
            .graph;
        if mode != "nonterminal" {
            for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
                graph = services
                    .commit_service()
                    .transition_node(&graph, &predecessor.id, status, None, vec![])
                    .unwrap()
                    .graph;
            }
            graph = services
                .commit_service()
                .transition_node(
                    &graph,
                    &predecessor.id,
                    ExecutionNodeStatus::Completed,
                    (mode == "unsupported_kind").then(|| {
                        completed_result(Some("non-source-receipt".into()), Default::default())
                    }),
                    vec![],
                )
                .unwrap()
                .graph;
        }
        let (graph, ticket) = own862_start_host_fixture_node(&services, &graph, &node);
        let lineage = graph.lineage.as_ref().unwrap();
        services.record_live_execution(
            &lineage.session_id,
            graph.id.clone(),
            lineage.turn_id.clone(),
        );
        let (runtime, state, _scope) = r1_observation_branch_runtime(
            session.clone(),
            FinalAnswerClient,
            &services,
            &graph,
            &goal_id,
        );
        let backend = TurnModelStepBackend {
            ticket: ticket.clone(),
            stop: crate::CancellationToken::new(),
            local_fence: std::sync::Mutex::new(None),
            runtime,
            state: Arc::new(tokio::sync::Mutex::new(state)),
            services: services.clone(),
        };
        let outcome = backend.execute(&ticket).await.unwrap();
        assert!(
            outcome.domain_events.iter().all(|input| !input
                .idempotency_key
                .as_ref()
                .unwrap()
                .contains(":upstream-observation:")),
            "{mode}"
        );
        let records = r1_commit_host_outcome(&services, &graph, &ticket, outcome);
        assert!(records.iter().all(|event| !event
            .idempotency_key
            .as_ref()
            .unwrap()
            .contains(":upstream-observation:")));
        assert!(records.iter().any(|event| event
            .idempotency_key
            .as_ref()
            .unwrap()
            .ends_with(":goal-observation")));
    }
}

struct R1ObservedToolExecutor {
    inner: RecordingToolExecutor,
    services: Arc<crate::RuntimeServices>,
    observed: Mutex<Vec<harness_contract::context::ObservedEvidence>>,
}

#[async_trait::async_trait]
impl ToolExecutor for R1ObservedToolExecutor {
    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        self.inner.tool_discovery_receipt()
    }

    fn available_tool_names(&self) -> Vec<String> {
        self.inner.available_tool_names()
    }

    fn registered_tool_effect(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        self.inner.registered_tool_effect(name, input)
    }

    fn observed_evidence_snapshot(&self) -> Vec<harness_contract::context::ObservedEvidence> {
        self.observed.lock().unwrap().clone()
    }

    async fn execute_output(
        &self,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let output = self.inner.execute_output(name, input).await?;
        if name == "read_file" {
            let bytes = std::fs::read(self.services.workspace_root().join("src/lib.rs"))
                .map_err(|error| ToolError::new(error.to_string()))?;
            let digest = format!("sha256:{:x}", Sha256::digest(&bytes));
            let evidence = self
                .services
                .path_identity_resolver()
                .observe_tool_scope(
                    name,
                    "read:src/lib.rs",
                    Some(&digest),
                    self.inner.executed.load(Ordering::SeqCst) as u64,
                )
                .map_err(|error| ToolError::new(error.to_string()))?;
            self.observed.lock().unwrap().push(evidence);
            return Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
                String::from_utf8(bytes).unwrap(),
            ));
        }
        Ok(output)
    }
}

fn declared_host_fixture_catalog(
    declarations: &[(&str, PermissionMode)],
) -> harness_contract::tool::ToolDiscoveryReceipt {
    harness_contract::tool::ToolDiscoveryReceipt {
        query: "explicit-host-test-fixture".into(),
        catalog_revision: 7,
        descriptors: declarations
            .iter()
            .map(
                |(name, permission)| harness_contract::tool::ToolDescriptorRef {
                    canonical_id: (*name).into(),
                    display_name: (*name).into(),
                    source: "explicit-test-fixture".into(),
                    schema_hash: format!("explicit-fixture:{name}:1"),
                    required_permission: *permission,
                    permission_source: "explicit-test-fixture".into(),
                    health: harness_contract::tool::ToolDescriptorHealth::Healthy,
                },
            )
            .collect(),
        activation_candidates: declarations
            .iter()
            .map(|(name, _)| (*name).into())
            .collect(),
    }
}

#[test]
fn delegated_leaf_never_owns_root_terminal_presentation() {
    assert!(TurnExecutionRole::RootTurn.owns_root_presentation());
    assert!(!TurnExecutionRole::DelegatedLeaf.owns_root_presentation());
    assert!(TurnExecutionRole::DelegatedLeaf.is_delegated_leaf());
}

/// The breaker answers "the same dead end again", so only a failure belongs in
/// its ledger. The plane called it for every call with the failure kind
/// defaulted to `Unknown`, which the breaker accepts, so a completed call also
/// counted. In the live run 40 of 62 refusals were issued against inputs that
/// had never failed, and the refusal told the model it had failed twice about
/// calls it had just succeeded at twice.
#[test]
fn a_completed_call_is_not_recorded_in_the_ledger() {
    let runtime = crate::ConversationRuntime::new(
        Session::new(),
        FinalAnswerClient,
        NoopToolExecutor,
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        vec!["bounded read".to_string()],
    )
    .without_memory();
    // The ledger is keyed by the active turn; with no turn admitted yet this
    // runtime falls back to its session id, which is unique per test.
    let turn = runtime.active_repetition_scope();
    let hash = "hash-completed-call".to_string();
    let mut output = String::new();

    runtime.append_repetition_diagnosis(
        "read_file",
        &hash,
        &[],
        false,
        crate::ToolFailureKind::Unknown,
        &mut output,
    );
    assert!(
            !crate::repetition_guard::is_dispatch_blocked(&turn, "read_file", &hash),
            "a completed call is not a dead end and must not count toward a breaker that reports a failure count"
        );

    // The other half of the rule: the exclusion must not weaken the breaker for
    // the calls it exists for.
    for _ in 0..2 {
        runtime.append_repetition_diagnosis(
            "read_file",
            &hash,
            &[],
            true,
            crate::ToolFailureKind::ExecutionError,
            &mut output,
        );
    }
    assert!(
        crate::repetition_guard::is_dispatch_blocked(&turn, "read_file", &hash),
        "two deterministic failures must still trip the breaker"
    );
}

#[test]
fn collaboration_team_slots_are_independent_of_tool_capacity() {
    assert_eq!(collaboration_team_slots(8, 8), 8);
    assert_eq!(collaboration_team_slots(8, 1), 1);
    // Tool collapsed to one must not reduce Team capacity below Agent.
    assert_eq!(collaboration_team_slots(8, 4), 4);
    assert_eq!(collaboration_team_slots(1, 8), 1);
}

#[test]
fn terminal_commit_state_is_owned_by_the_exact_synthesize_attempt() {
    let owner = ("graph:synthesize".to_string(), 2);

    assert!(terminal_commit_owned_by(
        Some(&owner),
        "graph:synthesize",
        2
    ));
    assert!(!terminal_commit_owned_by(None, "graph:synthesize", 2));
    assert!(!terminal_commit_owned_by(
        Some(&owner),
        "graph:post-input-replan",
        2
    ));
    assert!(!terminal_commit_owned_by(
        Some(&owner),
        "graph:synthesize",
        3
    ));
}

#[test]
fn early_lane_accepts_only_dependency_free_bounded_idempotent_reads() {
    let read_call = ModelToolCall {
        binding: None,
        id: "read".to_string(),
        name: "read_file".to_string(),
        input: r#"{"path":"README.md","limit":20}"#.to_string(),
        depends_on: Vec::new(),
    };
    let read_plan = crate::GovernedToolPlan::from_requests(&[crate::tool_dispatch::ToolRequest {
        tool_use_id: read_call.id.clone(),
        tool_name: read_call.name.clone(),
        input: read_call.input.clone(),
        depends_on: Vec::new(),
    }]);
    assert_eq!(
        early_tool_rejection_reason(&read_call, &read_plan.tasks[0], &read_plan.tasks[0].effect),
        None
    );

    let mut dependent = read_call.clone();
    dependent.depends_on = vec!["prior".to_string()];
    assert_eq!(
        early_tool_rejection_reason(&dependent, &read_plan.tasks[0], &read_plan.tasks[0].effect),
        Some("declared_dependency_waits_for_finalized_dag")
    );

    let write_call = ModelToolCall {
        binding: None,
        id: "write".to_string(),
        name: "edit".to_string(),
        input: r#"{"path":"README.md","content":"changed"}"#.to_string(),
        depends_on: Vec::new(),
    };
    let write_plan = crate::GovernedToolPlan::from_requests(&[crate::tool_dispatch::ToolRequest {
        tool_use_id: write_call.id.clone(),
        tool_name: write_call.name.clone(),
        input: write_call.input.clone(),
        depends_on: Vec::new(),
    }]);
    assert_eq!(
        early_tool_rejection_reason(
            &write_call,
            &write_plan.tasks[0],
            &write_plan.tasks[0].effect
        ),
        Some("descriptor_not_early_safe")
    );
}

#[test]
fn early_read_fingerprint_uses_canonical_tool_arguments_not_json_key_order() {
    let left = crate::GovernedToolPlan::from_requests(&[crate::tool_dispatch::ToolRequest {
        tool_use_id: "left".to_string(),
        tool_name: "read_file".to_string(),
        input: r#"{"limit":20,"path":"README.md"}"#.to_string(),
        depends_on: Vec::new(),
    }]);
    let right = crate::GovernedToolPlan::from_requests(&[crate::tool_dispatch::ToolRequest {
        tool_use_id: "right".to_string(),
        tool_name: "read_file".to_string(),
        input: r#"{"path":"README.md","limit":20}"#.to_string(),
        depends_on: Vec::new(),
    }]);

    assert_eq!(
        early_tool_fingerprint(&left.tasks[0].invocation),
        early_tool_fingerprint(&right.tasks[0].invocation)
    );
}

#[tokio::test]
async fn early_original_predispatch_deferral_retains_owner_and_rejects_drift() {
    use crate::conversation::{EarlyToolCandidate, EarlyToolDispatchResult, EarlyToolDispatcher};
    use crate::execution_core::graph::{NodeExecutor, executors::ScopedNodeBackend};

    let services = crate::RuntimeServices::in_memory().unwrap();
    let session = Session::new();
    let session_id = session.session_id.clone();
    let (graph, ticket) = r1_branch_graph(&services, &session_id, vec![], ExecutionNodeKind::InlineModel);
    let goal_id = r1_branch_goal(&services, &session_id);
    let (runtime, state, _scope) = r1_observation_branch_runtime(
        session, FinalAnswerClient, &services, &graph, &goal_id,
    );
    services.model_step_executor().prepare_original_binding(&ticket).unwrap();
    let dispatcher = {
        let runtime = runtime.lock().await;
        HostEarlyToolDispatcher {
            tool_executor: Arc::clone(runtime.tool_executor()), services: Arc::clone(&services),
            event_bus: runtime.cowd_bus().cloned(), ticket: ticket.clone(), session_id,
            memory_context: runtime.memory_turn_context(), reality_context: runtime.reality_data_lease(),
            model_lease: None, observation_wave_sequence: 1,
            decision: runtime.active_turn_strategy().unwrap().decision,
            permission_policy: runtime.permission_policy().clone(),
            authorization_negotiator: runtime.authorization_negotiator(),
            timeout: std::time::Duration::from_secs(60),
            early_read_locks: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        }
    };
    let candidate = EarlyToolCandidate {
        call: ModelToolCall { binding: None, id: "early-original".into(), name: "read_file".into(),
            input: "{".into(), depends_on: Vec::new() },
        identity: crate::CausalItemIdentity { model_step_id: "original-step".into(), item_id: "early-original".into(),
            segment_id: "original-segment".into(), causal_sequence: 1, delta_sequence: 0,
            tool_call_id: Some("early-original".into()), causal_parent_ids: Vec::new() },
        ready_at_ms: 1,
    };
    let result = dispatcher.dispatch(candidate).await;
    let EarlyToolDispatchResult::Deferred(original) = &result else {
        panic!("malformed original read must return before dispatch: {result:?}");
    };
    let proof = original.original_undispatched.as_ref().expect("original worker retains its pre-dispatch proof");
    assert_eq!(proof.ticket, ticket);
    assert_eq!(proof.call_id, "early-original");
    assert!(dispatcher.confirms_original_undispatched("early-original", original));
    assert!(!dispatcher.confirms_original_undispatched("foreign", original));
    let backend = TurnModelStepBackend {
        ticket: ticket.clone(), stop: crate::CancellationToken::new(),
        local_fence: std::sync::Mutex::new(None), runtime, state: Arc::new(tokio::sync::Mutex::new(state)),
        services: Arc::clone(&services),
    };
    assert!(backend.confirm_original_early_release(&ticket, &[("early-original".into(), result.clone())]).await.unwrap());
    assert!(!backend.confirm_original_early_release(&ticket, &[("foreign".into(), result.clone())]).await.unwrap());
    let mut missing = original.clone();
    missing.original_undispatched = None;
    assert!(!dispatcher.confirms_original_undispatched("early-original", &missing));
    assert!(!backend.confirm_original_early_release(&ticket, &[("early-original".into(), EarlyToolDispatchResult::Deferred(missing))]).await.unwrap());
    let mut drifted = original.clone();
    Arc::make_mut(drifted.original_undispatched.as_mut().unwrap()).identity.tool_call_id = Some("foreign".into());
    assert!(!dispatcher.confirms_original_undispatched("early-original", &drifted));
    assert!(!backend.confirm_original_early_release(&ticket, &[("early-original".into(), EarlyToolDispatchResult::Deferred(drifted))]).await.unwrap());
    let unknown = EarlyToolDispatchResult::Failed(crate::conversation::EarlyToolFailure {
        identity: crate::CausalItemIdentity { model_step_id: "original-step".into(), item_id: "early-original".into(),
            segment_id: "original-segment".into(), causal_sequence: 1, delta_sequence: 0,
            tool_call_id: Some("early-original".into()), causal_parent_ids: Vec::new() },
        tool_call_id: "early-original".into(), owner_ticket: Some(ticket.clone()),
        kind: crate::conversation::EarlyToolFailureKind::AuthorizationCommitUnknown,
        detail: "original authorization result unknown".into(),
    });
    assert!(!backend.confirm_original_early_release(&ticket, &[("early-original".into(), unknown)]).await.unwrap());
}

#[test]
fn referential_followup_never_splices_prior_session_text_into_objective() {
    let mut session = Session::new();
    session
        .push_message(ConversationMessage::user_text(
            "发起团队，完成公开技术标准最新信息的外部调研并给出证据。",
        ))
        .expect("append objective");
    session
        .push_message(ConversationMessage::assistant(vec![ContentBlock::Text {
            text: "上一次执行被阻断。".to_string(),
        }]))
        .expect("append assistant");
    session
        .push_message(ConversationMessage::user_text("/permissions yolo"))
        .expect("append command");
    session
        .push_message(ConversationMessage::user_text("继续"))
        .expect("append prior follow-up");

    let resolved = resolve_session_turn_objective(&session, "继续重新发起完成");
    assert_eq!(resolved, "继续重新发起完成");
    assert!(!resolved.contains("公开技术标准"));
    assert!(!resolved.contains("/permissions"));
}

#[test]
fn explicit_new_objective_never_inherits_session_history() {
    let mut session = Session::new();
    session
        .push_message(ConversationMessage::user_text("调研公开技术标准"))
        .expect("append objective");

    assert_eq!(
        resolve_session_turn_objective(&session, "新任务：检查本地 README"),
        "新任务：检查本地 README"
    );
    assert_eq!(
        resolve_session_turn_objective(&session, "解释这个函数"),
        "解释这个函数"
    );
}

#[test]
fn predecessor_results_become_typed_goal_observations() {
    let mut graph = harness_contract::execution_graph::ExecutionGraph::new("typed predecessor");
    let mut approval = ExecutionNodeSpec::new(ExecutionNodeKind::Approval, "approval", "{}");
    approval.id = "approval-node".to_string();
    approval.idempotency_key = "approval-effect".to_string();
    let mut model = ExecutionNodeSpec::new(ExecutionNodeKind::InlineModel, "inline_model", "{}");
    model.id = "model-node".to_string();
    graph.nodes = vec![approval.clone(), model.clone()];
    graph.edges.push(ExecutionEdge {
        from: approval.id.clone(),
        to: model.id.clone(),
        kind: ExecutionEdgeKind::DependsOn,
    });
    graph
        .node_statuses
        .insert(approval.id.clone(), ExecutionNodeStatus::Completed);
    graph
        .node_statuses
        .insert(model.id.clone(), ExecutionNodeStatus::Running);
    graph.node_results.insert(
        approval.id.clone(),
        completed_result(
            Some("approval:v1:receipt".to_string()),
            ExecutionUsage::default(),
        ),
    );
    let ticket = NodeExecutionTicket {
        graph_id: graph.id.clone(),
        node_id: model.id,
        executor_kind: "inline_model".to_string(),
        service_class: graph.service_class,
        attempt: 1,
        idempotency_key: "model-attempt".to_string(),
        payload_ref: "{}".to_string(),
    };
    let observations = predecessor_goal_observations(
        &graph,
        &ticket,
        &RuntimeObservationIdentity {
            workspace_id: "workspace".to_string(),
            session_id: "session".to_string(),
            turn_id: Some("turn".to_string()),
            task_id: None,
            graph_id: graph.id.clone(),
            goal_id: format!("goal:{}", graph.id),
            node_id: Some(ticket.node_id.clone()),
        },
    );

    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].source, "runtime.approval_result");
    assert_eq!(
        observations[0].effect_deltas[0].terminal_class,
        EffectTerminalClass::Completed
    );
    assert_eq!(
        observations[0].evidence_delta.added,
        vec!["execution_result:approval:v1:receipt".to_string()]
    );
}

#[test]
fn executed_team_failure_never_rebuilds_the_same_graph_in_the_parent() {
    assert!(selected_team_failure_must_block_parent_replay(true));
    assert!(!selected_team_failure_must_block_parent_replay(false));
}
use crate::conversation::{ApiRequest, AssistantEvent, ToolError};

#[test]
fn focus_terminal_candidate_accepts_fact_backed_natural_language() {
    let fenced =
        "```json\n{\"implementation\":\"done\",\"source_verification\":\"receipt\"}\n```\nprose";
    assert_eq!(
        normalized_team_terminal_candidate(
            fenced,
            &[
                "implementation".to_string(),
                "source_verification".to_string()
            ],
        )
        .as_deref(),
        Some("{\"implementation\":\"done\",\"source_verification\":\"receipt\"}")
    );
    let review = normalized_team_terminal_candidate("## Step 4: verify", &["review".into()])
        .expect("receipt-verified review prose should not require JSON syntax");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&review).expect("review carrier")["review"],
        "## Step 4: verify"
    );
    let findings =
        normalized_team_terminal_candidate("Observed the bounded source.", &["findings".into()])
            .expect("research prose should become the findings field");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&findings).expect("normalized findings JSON")
            ["findings"],
        "Observed the bounded source."
    );
    let summary =
        normalized_team_terminal_candidate("已读取并核对两个授权文件。", &["summary".into()])
            .expect("direct executor prose should become the summary field");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&summary).expect("normalized summary JSON")
            ["summary"],
        "已读取并核对两个授权文件。"
    );
    let aliased_findings = normalized_team_terminal_candidate(
        r#"{"summary":"Cargo.toml declares the workspace package metadata."}"#,
        &["findings".into()],
    )
    .expect("verified bounded summary should normalize to findings");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&aliased_findings)
            .expect("normalized findings JSON")["findings"],
        "Cargo.toml declares the workspace package metadata."
    );
    assert!(
        normalized_team_terminal_candidate(
            "{\"review\":\"checked\",\"risks\":[]}",
            &["review".into(), "risks".into()],
        )
        .is_some(),
        "an explicit empty risk list is a valid review result"
    );
    for field in ["unresolved", "unresolved_or_risks"] {
        let candidate = format!("{{\"summary\":\"checked\",\"{field}\":[]}}");
        assert!(
            normalized_team_terminal_candidate(&candidate, &["summary".into(), field.into()],)
                .is_some(),
            "an explicit empty {field} list is a valid reviewed disclosure"
        );
    }
    let technical = normalized_team_terminal_candidate(
        "Updated the implementation and completed a fresh verification read.",
        &["implementation".into(), "source_verification".into()],
    )
    .expect("fact-backed technical prose should not trigger a format-only retry");
    let technical =
        serde_json::from_str::<serde_json::Value>(&technical).expect("technical carrier");
    assert_eq!(
        technical["implementation"],
        "Updated the implementation and completed a fresh verification read."
    );
    assert!(
        normalized_team_terminal_candidate("Updated the implementation.", &["risks".into()],)
            .is_none(),
        "generic prose cannot invent a risk disclosure"
    );
    assert_eq!(
        missing_required_structured_fields(
            "prefix {\"evidence\":\"receipt\",\"review\":\"checked\"}",
            &["review".into(), "risks".into()],
        ),
        vec!["risks".to_string()]
    );
    let custom = normalized_team_terminal_candidate(
            "## applications_survey\nVerified survey.\n\n### Vision\nNested result.\n\n## evidence\ntool://read\n\n## summary\nDone.",
            &[
                "applications_survey".into(),
                "evidence".into(),
                "summary".into(),
            ],
        )
        .expect("an exact Runtime-declared custom heading is a valid carrier");
    let custom = serde_json::from_str::<serde_json::Value>(&custom).expect("custom carrier");
    assert!(custom["applications_survey"]
        .as_str()
        .is_some_and(|value| value.contains("### Vision")));
    assert_eq!(custom["evidence"], "tool://read");
    assert_eq!(custom["summary"], "Done.");

    let recovered_custom = normalized_declared_custom_terminal_after_recovery(
        "## evidence\ntool://read\n\n## summary\nVerified memory findings.",
        &[
            "memory_findings_draft".into(),
            "evidence".into(),
            "summary".into(),
        ],
    )
    .expect("one recovered summary may receive its declared custom wrapper");
    let recovered_custom = serde_json::from_str::<serde_json::Value>(&recovered_custom)
        .expect("recovered custom carrier");
    assert_eq!(
        recovered_custom["memory_findings_draft"],
        "Verified memory findings."
    );
    assert!(
        normalized_declared_custom_terminal_after_recovery(
            "## summary\nReview complete.",
            &["evidence".into(), "summary".into()],
        )
        .is_none(),
        "fixed evidence may never be manufactured by presentation recovery"
    );

    let closed = normalized_terminal_after_bounded_recovery(
        "## findings\nVerified both bounded sources.\n\n## evidence\ntool://read-a\ntool://read-b",
        &[
            "source_paths".into(),
            "evidence".into(),
            "findings".into(),
            "summary".into(),
            "unresolved".into(),
        ],
    )
    .expect("receipt-satisfied terminal should close presentation-only gaps");
    let closed = serde_json::from_str::<serde_json::Value>(&closed).expect("closed terminal JSON");
    assert_eq!(closed["findings"], "Verified both bounded sources.");
    assert_eq!(closed["summary"], "Verified both bounded sources.");
    assert_eq!(closed["source_paths"], "Verified both bounded sources.");
    assert_eq!(closed["evidence"], "tool://read-a\ntool://read-b");
    assert_eq!(
        closed["unresolved"][0]["status"],
        "provider_omitted_after_bounded_recovery"
    );
    assert_eq!(closed["unresolved"][0]["no_empty_state_inferred"], true);
    assert_ne!(closed["unresolved"], serde_json::json!([]));
    assert!(
        normalized_terminal_after_bounded_recovery(
            "## summary\nReview complete.",
            &["evidence".into(), "summary".into(), "unresolved".into()],
        )
        .is_none(),
        "bounded recovery still cannot manufacture fixed evidence"
    );
    let receipts = vec![serde_json::json!({
        "sequence": 7,
        "tool_call_id": "write-html",
        "tool": "edit",
        "evidence_ref": "tool://write-html-receipt",
        "paths": ["group-theory-ai-autonomous-evaluation.html"],
    })];
    let receipt_closed = normalized_receipt_backed_terminal_after_recovery(
        "## summary\nPublished and verified the final HTML artifact.",
        &[
            "digest_receipt".into(),
            "evidence".into(),
            "html_artifact".into(),
            "summary".into(),
            "unresolved".into(),
        ],
        &receipts,
    )
    .expect("actual Runtime receipts close a presentation-only evidence carrier gap");
    let receipt_closed =
        serde_json::from_str::<serde_json::Value>(&receipt_closed).expect("receipt-backed carrier");
    assert_eq!(
        receipt_closed["evidence"][0]["evidence_ref"],
        "tool://write-html-receipt"
    );
    assert_eq!(
        receipt_closed["html_artifact"]["status"],
        "runtime_receipt_backed"
    );
    assert_eq!(
        receipt_closed["digest_receipt"]["receipts"][0]["tool"],
        "edit"
    );
    assert_eq!(
        receipt_closed["unresolved"][0]["no_empty_state_inferred"],
        true
    );
    assert!(normalized_receipt_backed_terminal_after_recovery(
        "## summary\nNo checked receipt.",
        &["evidence".into(), "summary".into()],
        &[],
    )
    .is_none());
    for invalid in ["", "<tool_call>{}</tool_call>", "```tool_use\n{}\n```"] {
        assert!(
            normalized_terminal_after_bounded_recovery(
                invalid,
                &["summary".into(), "unresolved".into()],
            )
            .is_none(),
            "empty or executable protocol output must remain rejected"
        );
    }
}

#[test]
fn runtime_materializes_only_fully_verified_implementation_handoffs() {
    let workspace = tempfile::tempdir().expect("workspace");
    let required = vec!["implementation".into(), "source_verification".into()];
    let observed = BTreeSet::from([
        "read:fixtures/target.txt".to_string(),
        "write:fixtures/target.txt".to_string(),
        "verify_after_write:fixtures/target.txt".to_string(),
    ]);
    let candidate = runtime_verified_implementation_terminal_candidate(
            &required,
            &observed,
            &["fixtures/target.txt".into()],
            &[
                ConversationMessage::tool_result(
                    "read-before",
                    "read_file",
                    format!(
                        "Tool `read_file` completed. Evidence: tool://before-ref. {{\"file\":{{\"filePath\":\"{}\",\"content\":\"old\"}}}}",
                        workspace.path().join("fixtures/target.txt").display()
                    ),
                    false,
                ),
                ConversationMessage::tool_result(
                    "write",
                    "edit",
                    "Tool `write_file` completed. Evidence: tool://write-ref. changed",
                    false,
                ),
                ConversationMessage::tool_result(
                    "read-after",
                    "read_file",
                    format!(
                        "Tool `read_file` completed. Evidence: tool://after-ref. {{\"file\":{{\"filePath\":\"{}\",\"content\":\"new\"}}}}",
                        workspace.path().join("fixtures/target.txt").display()
                    ),
                    false,
                ),
            ],
            workspace.path(),
        )
        .expect("verified handoff");
    let candidate: serde_json::Value = serde_json::from_str(&candidate).unwrap();
    assert_eq!(candidate["implementation"]["status"], "committed");
    assert_eq!(candidate["implementation"]["runtime_receipt_count"], 3);
    assert_eq!(
        candidate["implementation"]["receipts"][1]["evidence_ref"],
        "tool://write-ref"
    );
    assert_eq!(
        candidate["implementation"]["receipts"][0]["paths"][0],
        "fixtures/target.txt"
    );
    assert_eq!(
        candidate["source_verification"]["post_write_evidence_ref"],
        "tool://after-ref"
    );
    assert_eq!(
        candidate["source_verification"]["status"],
        "verified_after_commit"
    );
    assert!(runtime_verified_implementation_terminal_candidate(
        &required,
        &BTreeSet::from(["write:fixtures/target.txt".to_string()]),
        &["fixtures/target.txt".into()],
        &[ConversationMessage::tool_result(
            "write",
            "edit",
            "Tool `write_file` completed. Evidence: tool://write-ref. changed",
            false,
        )],
        workspace.path(),
    )
    .is_none());
    assert!(runtime_verified_implementation_terminal_candidate(
        &["review".into(), "risks".into()],
        &observed,
        &["fixtures/target.txt".into()],
        &[ConversationMessage::tool_result(
            "read",
            "read_file",
            "Tool `read_file` completed. Evidence: tool://read-ref. content=new",
            false,
        )],
        workspace.path(),
    )
    .is_none());
}

#[test]
fn tool_receipt_projection_preserves_runtime_attested_sha256() {
    let digest = "a".repeat(64);
    let output = format!(
            "Tool completed. Evidence: tool://write-ref. {{\"file\":{{\"filePath\":\"report.html\",\"sha256\":\"{digest}\",\"byteLength\":42}}}}"
        );
    assert_eq!(tool_receipt_sha256_digests(&output), vec![digest]);
}

#[derive(Clone)]
struct FinalAnswerClient;

#[derive(Clone)]
struct R612InterleavedGoalClient {
    services: Arc<crate::RuntimeServices>,
    observation: RuntimeObservation,
    changed_goal: bool,
}

impl ApiClient for R612InterleavedGoalClient {
    fn stream(
        &mut self,
        _request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        if self.changed_goal {
            let current = self.services.goal_store().projection(self.observation.goal_id()).unwrap().unwrap().goal;
            self.services.goal_store().revise(&current.id, current.revision, current.user_sequence + 1,
                "r612 concurrent Goal revision", |goal| {
                    goal.constraints.push("new constraint".into());
                    vec!["constraints".into()]
                }).unwrap();
        } else {
            self.services.goal_store().record_observation(self.observation.clone()).unwrap();
        }
        Box::pin(stream::iter(vec![
            Ok(AssistantEvent::TextDelta("terminal answer".to_string())),
            Ok(AssistantEvent::MessageStop),
        ]))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn r612_model_producer_rechecks_goal_and_uses_one_observation_cut() {
    if crate::test_support::skip_if_env_absent("COWD_R1_06_FIXTURE_ROOT") {
        return;
    }
    use crate::execution_core::graph::executors::ScopedNodeBackend;
    for changed_goal in [false, true] {
        let services = r1_observation_services();
        let session = Session::new();
        let (graph, ticket) = r1_branch_graph(&services, &session.session_id, vec![], ExecutionNodeKind::InlineModel);
        let goal_id = format!("r612-goal-{}", uuid::Uuid::new_v4());
        let goal: GoalContract = serde_json::from_value(serde_json::json!({
            "id":goal_id,"session_id":session.session_id,"objective":"answer directly",
            "criteria":[{"id":"checked","statement":"answer checked","status":"open"}],
            "phase":"execution","spec_revision":1,"spec_digest":"branch-spec",
            "scope":"internal","completion":"open","revision":1,"user_sequence":1,
            "source_intent_ref":"session_input:r612-original",
            "execution_binding":{"objective_id":"r612-objective","session_id":session.session_id,
                "turn_id":"r1-branch-turn","root_execution_id":graph.id,"agentic_program_id":"r612-program"}
        })).unwrap();
        services.goal_store().create(goal).unwrap();
        let observation: RuntimeObservation = serde_json::from_value(serde_json::json!({
            "identity":{"workspace_id":"workspace","session_id":session.session_id,"graph_id":graph.id,
                "goal_id":goal_id,"node_id":ticket.node_id},
            "kind":"tool_progress","source":"r612-interleave","source_revision":1,
            "freshness":{"observed_at_ms":1,"policy_revision":"branch-spec"},
            "summary":"interleaved observation","fingerprint":"r612-interleave","result_class":"succeeded"
        })).unwrap();
        let client = R612InterleavedGoalClient { services: services.clone(), observation, changed_goal };
        let (runtime, mut state, _scope) = r1_observation_branch_runtime(session.clone(), client, &services, &graph, &goal_id);
        state.ingress = Some(TurnIngressRef {
            request_id: "r612-request".into(), turn_id: "r1-branch-turn".into(),
            message_id: "r612-message".into(), session_id: session.session_id.clone(),
            primary_task_id: "r1-task".into(), root_task_id: "r1-task".into(),
            session_generation: 1, input_sequence: 1, claim_owner: "r612-owner".into(),
            claim_token: "r612-claim".into(), claim_revision: 1, handoff_id: None,
        });
        let backend = TurnModelStepBackend { ticket: ticket.clone(), stop: crate::CancellationToken::new(), local_fence: std::sync::Mutex::new(None),
            runtime, state: Arc::new(tokio::sync::Mutex::new(state)), services: services.clone() };
        let result = backend.execute(&ticket).await;
        if changed_goal {
            let error = result.err().expect("a changed Goal must be rejected");
            assert!(error.to_string().contains("final model answer saw a changed Goal"), "{error}");
            assert_eq!(services.graph_state_store().load(&graph.id).unwrap(), graph);
        } else {
            let outcome = result.expect("interleaved observation is not a Goal revision");
            let source = services.goal_store().projection(&goal_id).unwrap().unwrap();
            assert_eq!(outcome.source_revisions.get(&format!("goal:{goal_id}")), Some(&source.stream_revision));
            let records = r1_commit_host_outcome(&services, &graph, &ticket, outcome);
            assert!(records.iter().any(|record| record.kind == "goal.observation"));
        }
    }
}

impl ApiClient for FinalAnswerClient {
    fn stream(
        &mut self,
        _request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        Box::pin(stream::iter(vec![
            Ok(AssistantEvent::TextDelta("terminal answer".to_string())),
            Ok(AssistantEvent::MessageStop),
        ]))
    }
}


























struct NoopToolExecutor;

#[async_trait::async_trait]
impl ToolExecutor for NoopToolExecutor {
    async fn execute_output(
        &self,
        name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        Err(ToolError::new(format!("unexpected tool call: {name}")))
    }
}




fn standard_host_with_services(
    services: Arc<crate::RuntimeServices>,
) -> StandardRuntimeHost<NoopToolExecutor> {
    let lineage = test_execution_lineage();
    services.publish_session_execution_policy(
        lineage.session_id.clone(),
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Supervised,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
    let task_spec = services
        .task_runtime_port()
        .bind_task_spec(
            &lineage.session_id,
            Some(harness_contract::policy::PermissionMode::ReadOnly),
            harness_contract::task::TaskSpec::new("test conversation turn"),
        )
        .expect("bind canonical test Task policy");
    if services
        .task_aggregate_service()
        .get(&lineage.task_id)
        .expect("canonical test Task lookup")
        .is_none()
    {
        services
            .task_runtime_port()
            .create(harness_contract::task::TaskCreateCommand {
                task_id: lineage.task_id.clone(),
                mission_id: services.mission_runtime().default_mission_id().to_string(),
                kind: harness_contract::task::TaskKind::Root,
                origin: harness_contract::task::TaskOrigin::User,
                origin_session_id: lineage.session_id.clone(),
                origin_turn_id: lineage.turn_id.clone(),
                root_task_id: lineage.root_task_id.clone(),
                parent_task_id: None,
                predecessor_task_id: None,
                mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
                mission_assigned_by: "runtime.host.test".to_string(),
                spec: task_spec,
                evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                    "test_input",
                    "host-test",
                )],
            })
            .expect("canonical test Task");
    }
    let registry = Arc::new(
        crate::ProviderRegistry::new(crate::config::ProvidersConfig {
            providers: HashMap::from([(
                "test".to_string(),
                crate::config::ProviderConfig {
                    reasoning_effort: None,
                    name: "test".to_string(),
                    // The test never submits a provider request. A closed
                    // loopback address keeps this fixture inert if a
                    // future regression accidentally does.
                    base_url: "http://127.0.0.1:9/v1".to_string(),
                    api_key: "test".to_string(),
                    models: vec!["test-model".to_string()],
                    protocol: Some("completions".to_string()),
                    parallel_tool_calls: Default::default(),
                    early_tool_start: Default::default(),
                },
            )]),
        })
        .expect("valid test provider registry"),
    );
    let mut session = Session::new();
    session.session_id = lineage.session_id.clone();
    StandardRuntimeHost::new(StandardRuntimeHostConfig {
        runtime_services: services,
        session,
        provider_registry: registry,
        model: "test-model".to_string(),
        tool_definitions: Vec::new(),
        tool_executor: Arc::new(NoopToolExecutor),
        permission_policy: PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        system_prompt: vec!["test recovery host".to_string()],
        feature_config: RuntimeFeatureConfig::default(),
        emit_output: false,
        stream_callback: None,
        tool_callback: None,
        model_context_window: None,
        hook_progress_reporter: None,
        external_context_items: Vec::new(),
        immutable_user_prefix: Vec::new(),
        cache_cohort_user_prefix: Vec::new(),
        skill_profiles: Vec::new(),
        agent_skill_profile: AgentSkillProfile::default(),
        skill_instruction_source: None,
        skill_binding_pins: Vec::new(),
        skill_binding_tool_refs: Vec::new(),
        memory_agent_id: "test-agent".to_string(),
        memory_definition_lineage_id: None,
        memory_team_id: None,
        memory_read_scopes: Vec::new(),
        reality_binding: None,
        execution_identity: None,
        execution_lineage: Some(lineage),
        execution_parent: None,
        execution_role: TurnExecutionRole::RootTurn,
        recovered_tool_receipt_count: 0,
        recovered_tool_receipts_require_text_only: false,
    })
    .expect("standard host")
}

fn test_execution_lineage() -> harness_contract::execution_graph::ExecutionGraphLineage {
    harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: "session-test".to_string(),
        turn_id: "turn-test".to_string(),
        root_task_id: "task-root-test".to_string(),
        task_id: "task-root-test".to_string(),
        generation: 1,
    }
}

fn fresh_test_execution_lineage(
    session_id: &str,
) -> harness_contract::execution_graph::ExecutionGraphLineage {
    let identity = uuid::Uuid::new_v4();
    let task_id = format!("task-root-test-{identity}");
    harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: session_id.into(),
        turn_id: format!("turn-test-{identity}"),
        root_task_id: task_id.clone(),
        task_id,
        generation: 1,
    }
}


fn standard_host_for_recovery_test() -> StandardRuntimeHost<NoopToolExecutor> {
    standard_host_with_services(crate::RuntimeServices::in_memory().expect("services"))
}

#[test]
fn standard_host_normalizes_every_entry_to_the_cowd_identity_contract() {
    let prompt = canonical_host_system_prompt(vec![
        "You are a delegated Cowd agent for a bounded task.".to_string(),
        "Provider model: claude-compatible".to_string(),
    ]);
    assert!(prompt
        .first()
        .is_some_and(|head| head.contains("You are Cowd")
            && head.contains(crate::COWD_IDENTITY_CONTRACT_VERSION)));
    assert!(prompt
        .iter()
        .take_while(|section| *section != crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY)
        .any(|guard| guard.contains("non-delegable") && guard.contains("Cowd")));
    let boundary = prompt
        .iter()
        .position(|section| section == crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY)
        .expect("dynamic boundary");
    assert!(prompt[boundary + 1].contains("delegated Cowd agent"));
}

#[test]
fn standard_host_never_infers_a_memory_backend_when_services_selected_none() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    assert!(services.memory_manager().is_none());

    let host = standard_host_with_services(services);

    assert!(host.runtime_ref().memory_manager().is_none());
    assert!(host
        .runtime_ref()
        .memory_status()
        .is_some_and(|status| status.contains("composition root")));
}

#[test]
fn delegated_standard_host_binds_only_its_session_evaluation_lease() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let guard = services
        .evaluation_provider_token_leases()
        .install("session-test", "eval-session-test", 10_000)
        .expect("session evaluation lease");
    let lease = guard.lease();

    let host = standard_host_with_services(Arc::clone(&services));
    assert!(host
        .runtime_ref()
        .uses_evaluation_provider_token_lease(&lease));

    drop(guard);
    assert!(services
        .evaluation_provider_token_leases()
        .get("session-test")
        .expect("registry read")
        .is_none());
    assert!(host
        .runtime_ref()
        .uses_evaluation_provider_token_lease(&lease));
}

#[test]
fn standard_hosts_share_runtime_owned_transport_tool_and_artifact_owners() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let first = standard_host_with_services(Arc::clone(&services));
    let second = standard_host_with_services(Arc::clone(&services));

    assert!(first
        .runtime_ref()
        .uses_tool_execution_plane(services.tool_execution_plane()));
    assert!(second
        .runtime_ref()
        .uses_tool_execution_plane(services.tool_execution_plane()));
    assert!(first
        .runtime_ref()
        .uses_artifact_store(services.artifact_store()));
    assert!(second
        .runtime_ref()
        .uses_artifact_store(services.artifact_store()));

    let transport = services.provider_transport_pool().stats();
    assert_eq!(transport.builds, 1);
    assert_eq!(transport.checkouts, 2);
    assert_eq!(transport.hits, 1);
}


#[tokio::test]
async fn cancelled_awaiter_keeps_runtime_recovery_channel_in_host() {
    let mut host = standard_host_for_recovery_test();
    let runtime = host.runtime.take().expect("fixture runtime");
    let (sender, receiver) = tokio::sync::oneshot::channel();
    host.inflight_turn = Some(receiver);
    let host = Arc::new(tokio::sync::Mutex::new(host));

    let waiting_host = Arc::clone(&host);
    let waiter = tokio::spawn(async move { waiting_host.lock().await.await_started_turn().await });
    tokio::task::yield_now().await;
    waiter.abort();
    let _ = waiter.await;

    assert!(
        sender
            .send((Some(runtime), Err(RuntimeError::new("cancelled test turn"))))
            .is_ok(),
        "cancelling the request waiter must not drop the host-owned receiver"
    );
    let mut host = host.lock().await;
    host.restore_inflight_turn()
        .await
        .expect("the next turn can reclaim the runtime");
    assert!(host.runtime.is_some());
    assert!(host.inflight_turn.is_none());
}

#[tokio::test]
async fn rejected_turn_admission_restores_the_conversation_runtime_to_its_host() {
    let services = crate::RuntimeServices::in_memory().expect("runtime services");
    let mut host = standard_host_with_services(Arc::clone(&services));
    services.shutdown_execution().await;
    let runtime = host.runtime.take().expect("fixture runtime");

    let result = host
        .start_turn(
            runtime,
            "must not be admitted",
            &SharedPrompter::none(),
            None,
        )
        .await;

    assert!(result.is_err());
    assert!(host.runtime.is_some());
    assert!(host.inflight_turn.is_none());
}

#[derive(Clone)]
struct TwoToolClient {
    requests: usize,
    event_store: Arc<crate::RuntimeEventStore>,
    exposure: Option<harness_contract::tool::ToolExposureProjection>,
    executed: Arc<AtomicUsize>,
    executions_seen_before_second_model: Arc<AtomicUsize>,
}

impl ApiClient for TwoToolClient {
    fn configure_tool_exposure(
        &mut self,
        projection: harness_contract::tool::ToolExposureProjection,
    ) {
        self.exposure = Some(projection);
    }

    fn stream(
        &mut self,
        request: ApiRequest,
    ) -> Pin<Box<dyn Stream<Item = Result<AssistantEvent, RuntimeError>> + Send + '_>> {
        self.requests += 1;
        if self.requests == 1 {
            assert_eq!(self.executed.load(Ordering::SeqCst), 0);
            let exposure = self.exposure.as_ref().unwrap();
            assert!(exposure.active_ids.iter().chain(&exposure.bootstrap_ids).any(|active| active == "tool_search"),
                    "another Session must not disarm this real Host request: {exposure:?}; durable decisions={:?}",
                    self.event_store.all_events(256).unwrap().into_iter()
                        .filter(|event| event.kind.contains("text_only") || event.kind.contains("intervention"))
                        .collect::<Vec<_>>());
            Box::pin(stream::iter(vec![
                Ok(AssistantEvent::ToolUse {
                    id: "discover-tools".to_string(),
                    name: "tool_search".to_string(),
                    input: r#"{"query":"read and update source files"}"#.to_string(),
                }),
                Ok(AssistantEvent::MessageStop),
            ]))
        } else if self.requests == 2 {
            assert_eq!(self.executed.load(Ordering::SeqCst), 0);
            let exposure = self.exposure.as_ref().unwrap();
            assert!(
                ["read_file", "edit"].iter().all(|tool| exposure
                    .active_ids
                    .iter()
                    .chain(&exposure.bootstrap_ids)
                    .any(|active| active == tool)),
                "discovered dependency tools were hidden: {exposure:?}; durable decisions={:?}",
                self.event_store
                    .all_events(256)
                    .unwrap()
                    .into_iter()
                    .filter(|event| event.kind.contains("text_only")
                        || event.kind.contains("intervention"))
                    .collect::<Vec<_>>()
            );
            Box::pin(stream::iter(vec![
                Ok(AssistantEvent::ToolUse {
                    id: "read-1".to_string(),
                    name: "read_file".to_string(),
                    input: r#"{"path":"src/lib.rs"}"#.to_string(),
                }),
                Ok(AssistantEvent::ToolUse {
                    id: "write-1".to_string(),
                    name: "edit".to_string(),
                    input: r#"{"path":"src/lib.rs","content":"updated"}"#.to_string(),
                }),
                Ok(AssistantEvent::MessageStop),
            ]))
        } else {
            self.executions_seen_before_second_model
                .store(self.executed.load(Ordering::SeqCst), Ordering::SeqCst);
            let committed_results = request
                .messages
                .iter()
                .flat_map(|message| message.blocks.iter())
                .filter_map(|block| match block {
                    ContentBlock::ToolResult {
                        tool_name, output, ..
                    } => Some((tool_name.as_str(), output.as_str())),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert!(
                    committed_results.iter().any(
                        |(tool, output)| *tool == "read_file"
                            && output.contains("read_file complete")
                    ),
                    "the dependent model request must observe the committed read receipt: {committed_results:?}; actual guidance={:?}; durable observations={:?}",
                    request.prompt.contextual_packets,
                    self.event_store.all_events(256).unwrap().into_iter()
                        .filter(|event| event.kind.contains("observation") || event.kind.contains("failed") || event.kind.contains("text_only"))
                        .collect::<Vec<_>>()
                );
            assert!(
                    committed_results.iter().any(
                        |(tool, output)| *tool == "edit"
                            && output.contains("edit complete")
                    ),
                    "the dependent model request must observe the committed write receipt: {committed_results:?}"
                );
            Box::pin(stream::iter(vec![
                Ok(AssistantEvent::TextDelta("done once".to_string())),
                Ok(AssistantEvent::MessageStop),
            ]))
        }
    }
}

struct RecordingToolExecutor {
    executed: Arc<AtomicUsize>,
    order: Arc<Mutex<Vec<String>>>,
}

#[async_trait::async_trait]
impl ToolExecutor for RecordingToolExecutor {
    fn tool_discovery_receipt(&self) -> harness_contract::tool::ToolDiscoveryReceipt {
        declared_host_fixture_catalog(&[
            ("tool_search", PermissionMode::ReadOnly),
            ("read_file", PermissionMode::ReadOnly),
            ("edit", PermissionMode::WorkspaceWrite),
        ])
    }
    async fn execute_output(
        &self,
        name: &str,
        _input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        let output = if name == "tool_search" {
            let mut receipt = self.tool_discovery_receipt();
            receipt.query = "read and update source files".into();
            receipt
                .descriptors
                .retain(|descriptor| descriptor.canonical_id != "tool_search");
            receipt.activation_candidates = vec!["read_file".into(), "edit".into()];
            serde_json::to_string(&receipt).unwrap()
        } else {
            self.order.lock().unwrap().push(name.to_string());
            self.executed.fetch_add(1, Ordering::SeqCst);
            format!("{name} complete")
        };
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            output,
        ))
    }

    fn available_tool_names(&self) -> Vec<String> {
        vec![
            "tool_search".to_string(),
            "read_file".to_string(),
            "edit".to_string(),
        ]
    }

    fn registered_tool_effect(
        &self,
        name: &str,
        _input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        use harness_contract::policy::{PermissionOperation, PermissionResource, PermissionScope};
        use harness_contract::tool::{
            ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
            ToolPermissionMode,
        };

        match name {
            "tool_search" | "read_file" => Some(ToolEffectDescriptor {
                tool_id: name.to_string(),
                descriptor_hash: format!("test-{name}-v1"),
                effect_kind: ToolEffectKind::Read,
                idempotency: ToolIdempotency::Idempotent,
                scopes: vec![PermissionScope::new(
                    PermissionResource::File,
                    PermissionOperation::Read,
                )],
                required_permission: ToolPermissionMode::ReadOnly,
                approval_class: ToolApprovalClass::None,
                uses_network: false,
                spawns_process: false,
                mutates_packages: false,
                mutates_system: false,
                assessment: harness_contract::policy::EffectAssessment::default(),
            }),
            "edit" => Some(ToolEffectDescriptor {
                tool_id: name.to_string(),
                descriptor_hash: "test-write-file-v1".to_string(),
                effect_kind: ToolEffectKind::Write,
                idempotency: ToolIdempotency::IdempotentWithKey,
                scopes: vec![PermissionScope::new(
                    PermissionResource::File,
                    PermissionOperation::Write,
                )],
                required_permission: ToolPermissionMode::WorkspaceWrite,
                approval_class: ToolApprovalClass::Policy,
                uses_network: false,
                spawns_process: false,
                mutates_packages: false,
                mutates_system: false,
                assessment: harness_contract::policy::EffectAssessment::default(),
            }),
            _ => None,
        }
    }

    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
        if authorization.tool_id != name {
            return Err(ToolError::new("authorization tool does not match request"));
        }
        self.execute_output(name, input).await
    }

    fn classify_tool_safety(
        &self,
        name: &str,
        _input: &str,
    ) -> Option<crate::tool_orchestrator::ToolSafetyCategory> {
        Some(if name == "edit" {
            crate::tool_orchestrator::ToolSafetyCategory::WriteLocal
        } else {
            crate::tool_orchestrator::ToolSafetyCategory::ReadOnly
        })
    }
}




#[tokio::test(flavor = "multi_thread")]
async fn failure_refusal_uses_the_bound_roles_protocol_not_a_root_exit_command() {
    let runtime = crate::ConversationRuntime::new(
        Session::new(),
        FinalAnswerClient,
        NoopToolExecutor,
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        vec!["complete the bound Task".to_string()],
    )
    .without_memory();
    let turn = runtime.active_repetition_scope();
    let input =
        r#"{"task_ref":"task:12345678","evidence_refs":["receipt:program:12345678:claim-7"]}"#;
    let record = crate::ToolInvocationRecord::started(
        "session-refusal-role",
        1,
        "refusal-role",
        "task_submit",
        input,
        crate::ToolSafetyCategory::WriteLocal,
        1,
    );
    for _ in 0..crate::repetition_guard::repetition_bound() {
        crate::repetition_guard::record_tool_failure(
            &turn,
            "task_submit",
            &record.input_hash,
            &record.input_refs,
            crate::ToolFailureKind::ExecutionError,
            "unsupported_evidence_ref: receipt:program:12345678:claim-7",
        );
    }
    let result = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                binding: None,
                id: "refusal-role".into(),
                name: "task_submit".into(),
                input: input.into(),
                depends_on: Vec::new(),
            }],
            &crate::SharedPrompter::none(),
            1,
        )
        .await
        .unwrap();
    let text = format!("{:?}", result.messages);
    assert_eq!(result.failed, 1);
    assert!(
        text.contains("protocol owned by your current role"),
        "{text}"
    );
    assert!(!text.contains("objective_complete_request"), "{text}");
    assert!(!text.contains("wait_for_workers"), "{text}");
}

/// The delegated Agent's tool path must refuse a call the model keeps
/// repeating.
///
/// The hard breaker lived only in `execute_single_tool`, which a delegated
/// Agent never enters: its calls go through `execute_tool_batch_step`. The
/// counter already recorded that path, so one live run committed the
/// identical artifact eleven times against a block bound of six - the count
/// was kept and nothing acted on it.
#[tokio::test(flavor = "multi_thread")]
async fn the_delegated_batch_path_refuses_a_call_that_keeps_repeating() {
    let runtime = crate::ConversationRuntime::new(
        Session::new(),
        FinalAnswerClient,
        NoopToolExecutor,
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        vec!["answer directly".to_string()],
    )
    .without_memory();

    let turn = runtime.active_repetition_scope();
    let input = serde_json::json!({"query": "task:be3f9b4582fb9f712fa8ed66"}).to_string();
    let hash = crate::tool_invocation::stable_hash(&input);
    let bound = crate::repetition_guard::repeated_call_block_bound_for("state_inspect", "{}");
    for _ in 0..bound {
        crate::repetition_guard::record_repeated_call(&turn, "state_inspect", &hash);
    }

    let result = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                binding: None,
                id: "repeat-1".to_string(),
                name: "state_inspect".to_string(),
                input,
                depends_on: Vec::new(),
            }],
            &crate::SharedPrompter::none(),
            1,
        )
        .await
        .expect("the batch step must answer rather than error");

    assert_eq!(result.failed, 1, "{:?}", result.messages);
    assert!(
        result.messages.iter().any(|message| message
            .blocks
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolResult { is_error: true, .. }))),
        "the refusal must reach the model as a tool result: {:?}",
        result.messages
    );
    // The refusal names the tool and the way out, so the model can act on it
    // rather than guessing why its call did not run.
    let text = format!("{:?}", result.messages);
    assert!(text.contains("state_inspect"), "{text}");
    assert!(text.contains("Runtime refused"), "{text}");
    assert!(!text.contains("objective_complete_request"), "{text}");
    assert!(!text.contains("wait_for_workers"), "{text}");
}

/// Alternating between two different inputs is not a repeat: the breaker
/// keys on the exact bytes, so a model doing two different things must not
/// be stopped.
#[tokio::test(flavor = "multi_thread")]
async fn alternating_distinct_inputs_on_the_batch_path_is_not_a_repeat() {
    let runtime = crate::ConversationRuntime::new(
        Session::new(),
        FinalAnswerClient,
        NoopToolExecutor,
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        vec!["answer directly".to_string()],
    )
    .without_memory();

    let turn = runtime.active_repetition_scope();
    let bound = crate::repetition_guard::repeated_call_block_bound_for("state_inspect", "{}");
    for index in 0..bound {
        let input = serde_json::json!({"query": format!("task:{index}")}).to_string();
        let hash = crate::tool_invocation::stable_hash(&input);
        crate::repetition_guard::record_repeated_call(&turn, "state_inspect", &hash);
    }

    // The governed compiler has no such tool registered, so this batch is
    // rejected for a reason that has nothing to do with repetition. What
    // matters is that the repetition breaker is not what stopped it.
    let result = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                binding: None,
                id: "distinct-1".to_string(),
                name: "state_inspect".to_string(),
                input: serde_json::json!({"query": "task:fresh"}).to_string(),
                depends_on: Vec::new(),
            }],
            &crate::SharedPrompter::none(),
            1,
        )
        .await
        .expect("the batch step must answer");

    // The governed compiler has no such tool in this bare runtime, so the
    // call is refused for that reason. What must not happen is a refusal
    // from the repetition breaker, which is what this test is about.
    let text = format!("{:?}", result.messages);
    assert!(
        !text.contains("Runtime refused"),
        "a distinct input must not be refused as a repeat: {text}"
    );
}

#[test]
fn delegated_leaf_does_not_inherit_parent_team_cardinality() {
    let collaboration_obligation = test_collaboration_obligation(2);
    assert_eq!(
        required_team_execution_count_for_execution_context(
            Some(&collaboration_obligation),
            true,
            false,
        ),
        0,
        "a delegated leaf must not recursively inherit the parent Team requirement"
    );
    assert_eq!(
        required_team_execution_count_for_execution_context(
            Some(&collaboration_obligation),
            false,
            true,
        ),
        0,
        "quoted candidate output must not turn an isolated blind Judge into a Team obligation"
    );
    assert_eq!(
        required_team_execution_count_for_execution_context(
            Some(&collaboration_obligation),
            false,
            false,
        ),
        2,
        "production root turns still enforce their typed Team contract"
    );

    assert!(!team_phase_satisfies_parent_goal(2, true, false, 2,));
    assert!(team_phase_satisfies_parent_goal(2, true, true, 2,));
}

#[test]
fn exhausted_team_lease_preserves_parent_artifact_completion() {
    assert_eq!(
            exhausted_team_lease_disposition(true, false),
            ExhaustedTeamLeaseDisposition::CompleteRemainingWrite,
            "completed research Teams must not hide the parent's missing deliverable behind a text-only synthesis"
        );
    assert_eq!(
        exhausted_team_lease_disposition(true, true),
        ExhaustedTeamLeaseDisposition::CleanSynthesis,
    );
    assert_eq!(
        exhausted_team_lease_disposition(false, false),
        ExhaustedTeamLeaseDisposition::CleanSynthesis,
    );
}

#[test]
fn explicit_artifact_path_requires_a_matching_write_receipt() {
    let workspace = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(workspace.path().join("reports")).unwrap();
    std::fs::write(workspace.path().join("reports/report.html"), "wrong").unwrap();
    std::fs::write(workspace.path().join("reports/index.html"), "exact").unwrap();
    let resolver =
        crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
    let required = vec!["write:reports/index.html".to_string()];
    let wrong = vec![resolver
        .observe_tool_scope("edit", "write:reports/report.html", Some("wrong"), 1)
        .unwrap()];
    let exact = vec![resolver
        .observe_tool_scope("edit", "write:reports/index.html", Some("exact"), 1)
        .unwrap()];
    let objective = format!(
        "形成中文 HTML 报告并保存为 {}",
        workspace.path().join("reports/index.html").display()
    );

    assert_eq!(
        crate::workspace_scopes::explicit_workspace_resource_scopes(
            workspace.path(),
            &objective,
            true,
        ),
        required,
    );
    let new_target = workspace.path().join("reports/revised.html");
    let followup = format!("改为保存到 {}", new_target.display());
    assert_eq!(
        required_workspace_write_scopes_for_turn(
            workspace.path(),
            &followup,
            &format!("{objective}\n\nCurrent follow-up: {followup}"),
        ),
        vec!["write:reports/revised.html".to_string()],
    );

    assert!(!write_obligation_satisfied(
        true, &required, &wrong, true, &resolver,
    ));
    assert!(write_obligation_satisfied(
        true, &required, &exact, true, &resolver,
    ));
    assert!(write_obligation_satisfied(
        true,
        &[],
        &wrong,
        false,
        &resolver,
    ));
    // A Program status is no substitute for the current Goal's exact receipt.
    assert!(!write_obligation_satisfied(
        true, &required, &wrong, false, &resolver
    ));
}

#[test]
fn root_response_language_gate_only_rejects_missing_user_language() {
    assert!(response_language_mismatch(
        "请用中文给出最终结论",
        "The task is complete."
    ));
    assert!(!response_language_mismatch(
        "请用中文给出最终结论",
        "任务已经完成。"
    ));
    assert!(!response_language_mismatch(
        "Give the final answer in English",
        "The task is complete."
    ));
    assert!(!response_language_mismatch(
        "只回答 7 乘以 8 的结果。",
        "56"
    ));
    assert!(!response_language_mismatch_for_role(
        "请调研并输出中文报告",
        "Internal evidence collected in English.",
        true,
    ));
    assert!(response_language_mismatch_for_role(
        "请调研并输出中文报告",
        "The final report is complete.",
        false,
    ));
    assert!(should_recover_response_language(true, false));
    assert!(
        !should_recover_response_language(true, true),
        "exhausted presentation rewrite cannot invalidate completed work"
    );
    assert!(!should_recover_response_language(false, false));
}

#[test]
fn collaboration_evidence_survives_every_parent_model_step_without_duplication() {
    let persistent = ContextItem::new(
        "runtime-team-receipt:decision-1",
        ContextSourceKind::Task,
        ContextRole::Evidence,
        "checked Team result",
    );

    let first = model_context_for_step(Vec::new(), std::slice::from_ref(&persistent));
    let second = model_context_for_step(Vec::new(), std::slice::from_ref(&persistent));
    let deduplicated =
        model_context_for_step(vec![persistent.clone()], std::slice::from_ref(&persistent));

    assert_eq!(first, vec![persistent.clone()]);
    assert_eq!(second, vec![persistent.clone()]);
    assert_eq!(deduplicated, vec![persistent]);
}

#[test]
fn applied_agentic_mutation_receipt_is_distinguishing_progress() {
    let applied = harness_contract::agent_action::AgentActionObservation {
        receipt_id: "receipt:team-create".to_string(),
        action_id: "action:team-create".to_string(),
        action: "team_create".to_string(),
        program_id: "program:test".to_string(),
        revision: 2,
        status: harness_contract::agent_action::AgentActionStatus::Applied,
        duplicate: false,
        changed_refs: vec!["team:test".to_string()],
        actionable: vec!["invite an Agent".to_string()],
        projection: None,
        error: None,
    };
    let rejected = harness_contract::agent_action::AgentActionObservation {
        receipt_id: "receipt:rejected".to_string(),
        action_id: "action:rejected".to_string(),
        action: "task_publish".to_string(),
        program_id: "program:test".to_string(),
        revision: 2,
        status: harness_contract::agent_action::AgentActionStatus::Rejected,
        duplicate: false,
        changed_refs: Vec::new(),
        actionable: vec!["revise only this action".to_string()],
        projection: None,
        error: None,
    };
    let messages = vec![
        ConversationMessage::tool_result(
            "call-applied",
            "team_create",
            serde_json::to_string(&applied).expect("applied receipt"),
            false,
        ),
        ConversationMessage::tool_result(
            "call-rejected",
            "task_publish",
            serde_json::to_string(&rejected).expect("rejected receipt"),
            false,
        ),
    ];

    assert_eq!(
        agentic_action_progress_refs(&messages),
        std::collections::BTreeSet::from([
            "agentic_changed:team:test".to_string(),
            "agentic_receipt:receipt:team-create".to_string(),
        ])
    );
}

/// A refusal is the Runtime declining to run a call it has already refused for the same
/// cause. It is not a new failure. The ledger already treats it that way; the failure
/// streak must agree, otherwise the same event is a non-failure for the ledger and a
/// failure for the goal plane - the contradiction that sustained 158 refusals in one live
/// run: refusal -> `tool_failure` observation -> closure action required again -> replan ->
/// same call -> refusal.
#[test]
fn a_refusal_does_not_advance_the_failure_streak() {
    let runtime = crate::ConversationRuntime::new(
        Session::new(),
        FinalAnswerClient,
        NoopToolExecutor,
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        vec!["bounded read".to_string()],
    )
    .without_memory();
    let turn = runtime.active_repetition_scope();
    let call = ModelToolCall {
        binding: None,
        id: "call-refusal-streak".to_string(),
        name: "task_review".to_string(),
        input: "{\"task_ref\":\"task:refusal-streak\"}".to_string(),
        depends_on: vec![],
    };
    let hash = crate::tool_invocation::stable_hash(&call.input);
    let mut output = String::new();

    // Before the breaker trips, an error result is an executed failure and must still
    // advance the streak.
    assert_eq!(
        executed_failures(1, &[call.clone()], &turn),
        1,
        "a failure that could have been executed must still count"
    );

    for _ in 0..2 {
        output = "review physical Agent packet missing:run:abc123".to_string();
        runtime.append_repetition_diagnosis(
            "task_review",
            &hash,
            &[],
            true,
            crate::ToolFailureKind::ExecutionError,
            &mut output,
        );
    }
    assert!(
        crate::repetition_guard::is_dispatch_blocked(&turn, "task_review", &hash),
        "two deterministic failures must still trip the breaker"
    );
    assert_eq!(
            executed_failures(1, &[call], &turn),
            0,
            "the Runtime's own refusal must not be counted as a failure that re-triggers the obligation it refused"
        );
}

/// The protocol-closure rule exists so a model cannot close a mid-protocol Agent Task with
/// prose instead of the durable closure action. That protection must survive. What must
/// change is that it cannot re-dispatch an action the guard would refuse: a refused action
/// cannot close the protocol, and re-dispatching it only re-produces the refusal.
#[test]
fn the_protocol_closure_rule_does_not_re_dispatch_an_action_the_guard_would_refuse() {
    let runtime = crate::ConversationRuntime::new(
        Session::new(),
        FinalAnswerClient,
        NoopToolExecutor,
        PermissionPolicy::new(crate::PermissionMode::DangerFullAccess),
        vec!["bounded read".to_string()],
    )
    .without_memory();
    let turn = runtime.active_repetition_scope();
    let mut protocol = super::host_backend::DelegatedAgenticProtocolState {
        program_id: "program:closure-test".to_string(),
        task_id: "task:closure-test".to_string(),
        mode: "review".to_string(),
        purpose: harness_contract::agent_action::TaskPurpose::Delivery,
        tool_ceiling: super::host_backend::test_protocol_tool_ceiling(),
        status: crate::AgenticTaskStatus::Submitted,
        artifact_refs: vec!["artifact:closure-test".to_string()],
        artifact_evidence_refs: vec![],
        owns_active_attempt: true,
        review_reason: None,
    };
    let synthesize = harness_contract::goal::RuntimeIntervention {
        goal_id: "goal:closure-test".to_string(),
        kind: harness_contract::goal::RuntimeInterventionKind::Synthesize,
        reason: "bounded evidence batches added no coverage".to_string(),
        evidence_refs: vec![],
        expected_graph_revision: None,
    };

    // The protection is intact while the closure action can still be executed.
    assert!(
        protocol_closure_can_be_required(Some(&protocol), Some(&synthesize), &turn),
        "a mid-protocol Task must still be closed by its durable action, not by prose"
    );

    let mut output = String::new();
    for _ in 0..2 {
        output = "review physical Agent packet missing:run:abc123".to_string();
        runtime.append_repetition_diagnosis(
            "task_review",
            "hash-closure-test",
            &[],
            true,
            crate::ToolFailureKind::ExecutionError,
            &mut output,
        );
    }

    // Once the guard would refuse that action, the protocol cannot be closed by it, so the
    // synthesis must stand and the honest partial names the blocked obligation.
    assert!(
        !protocol_closure_can_be_required(Some(&protocol), Some(&synthesize), &turn),
        "the rule must not re-dispatch an action the guard would refuse"
    );

    // Unchanged half of the rule: a terminal protocol never requires the conversion.
    let terminal = harness_contract::goal::RuntimeIntervention {
        kind: harness_contract::goal::RuntimeInterventionKind::Replan,
        ..synthesize.clone()
    };
    assert!(!protocol_closure_can_be_required(
        Some(&protocol),
        Some(&terminal),
        &turn
    ));

    protocol.mode = "execute".into();
    protocol.purpose = harness_contract::agent_action::TaskPurpose::Exploration;
    protocol.status = crate::AgenticTaskStatus::Claimed;
    protocol.tool_ceiling = ["task_submit", "task_withdraw", "read_file"]
        .into_iter()
        .map(str::to_string)
        .collect();
    for name in ["read_file", "task_submit"] {
        for _ in 0..2 {
            runtime.append_repetition_diagnosis(
                name,
                &format!("blocked-{name}"),
                &[],
                true,
                crate::ToolFailureKind::ExecutionError,
                &mut "physical Agent packet missing:run:abc123".into(),
            );
        }
    }
    assert!(
        protocol_closure_can_be_required(Some(&protocol), Some(&synthesize), &turn),
        "a blocked read or submit cannot hide the authorized retirement alternative"
    );
    for _ in 0..2 {
        runtime.append_repetition_diagnosis(
            "task_withdraw",
            "blocked-withdraw",
            &[],
            true,
            crate::ToolFailureKind::ExecutionError,
            &mut "physical Agent packet missing:run:abc123".into(),
        );
    }
    assert!(
        !protocol_closure_can_be_required(Some(&protocol), Some(&synthesize), &turn),
        "all authorized exits blocked must not force another impossible closure round"
    );
    protocol.tool_ceiling.clear();
    assert!(!protocol_closure_can_be_required(
        Some(&protocol),
        Some(&synthesize),
        &turn
    ));
}

/// The gain measure reads the tool result block, and that block carries the
/// model-visible text, not the durable receipt. A receipt whose output exceeds the
/// preview budget arrives truncated, so the parse fails and the action is invisible
/// to the measure. In run #6 the durable record shows `task_claim` changing a Task
/// while the same batch's observation reported no distinguishing evidence: 74 of 75
/// tool-batch observations reported `provenance: unknown`, and the only `observed`
/// one was a `team_create` whose output was small enough to stay inline.
#[test]
fn the_gain_measure_cannot_read_a_receipt_that_arrives_truncated() {
    let receipt = harness_contract::agent_action::AgentActionObservation {
        receipt_id: "receipt:program:test:claim".to_string(),
        action_id: "action:claim".to_string(),
        action: "task_claim".to_string(),
        program_id: "program:test".to_string(),
        revision: 4,
        status: harness_contract::agent_action::AgentActionStatus::Applied,
        duplicate: false,
        changed_refs: vec!["task:t1".to_string()],
        actionable: vec!["submit the deliverable".to_string()],
        projection: Some(serde_json::json!({
            "padding": "x".repeat(1024),
            "counts": { "tasks": 1 }
        })),
        error: None,
    };
    let full = serde_json::to_string(&receipt).expect("receipt");
    let model_visible = full.chars().take(500).collect::<String>();
    assert!(
        model_visible.len() < full.len(),
        "the fixture must actually exceed the preview budget"
    );

    assert_eq!(
        agentic_action_progress_refs(&[ConversationMessage::tool_result(
            "call-1",
            "task_claim",
            &full,
            false,
        )]),
        std::collections::BTreeSet::from([
            "agentic_changed:task:t1".to_string(),
            "agentic_receipt:receipt:program:test:claim".to_string(),
        ]),
        "the durable receipt is readable"
    );
    assert!(
        serde_json::from_str::<harness_contract::agent_action::AgentActionObservation>(
            &model_visible
        )
        .is_err(),
        "the model-visible text is not a parseable receipt"
    );
    assert!(
        agentic_action_progress_refs(&[ConversationMessage::tool_result(
            "call-1",
            "task_claim",
            &model_visible,
            false,
        )])
        .is_empty(),
        "a truncated receipt is invisible to the gain measure"
    );
}

#[test]
fn negated_enumeration_does_not_replan_a_numeric_root_answer() {
    let prompt = "只回答 7 乘以 8 的结果。不要调用工具、创建团队、创建任务或写文件，不要附加解释。";
    let understanding = harness_contract::strategy::understand(
        &harness_contract::strategy::StrategyInput::from_prompt(prompt),
    );
    assert!(understanding.forbids_tool_use);
    assert!(!understanding.requires_write);
    let required = required_write_for_turn(understanding.requires_write, false, &[]);
    assert!(!required);
    assert!(!response_language_mismatch_for_role(prompt, "56", false));
    assert!(!should_recover_response_language(false, false));
    assert!(required_write_for_turn(true, false, &[]));
}

#[test]
fn tool_batch_413_actual_producer_consumer_preserves_calls_and_dependencies() {
    let calls = vec![ModelToolCall {
        binding: None,
        id: "call-413".into(),
        name: "read_file".into(),
        input: serde_json::json!({"path":"source.rs","fixture_extent":"repeated ".repeat(1000)})
            .to_string(),
        depends_on: vec!["original-parent".into()],
    }];
    let payload = encode_tool_calls_with_continuation("session-413", &calls, true).unwrap();
    let (restored, continuation) = decode_tool_batch(&payload).unwrap();
    assert!(continuation);
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].id, calls[0].id);
    assert_eq!(restored[0].name, calls[0].name);
    assert_eq!(restored[0].input, calls[0].input);
    assert_eq!(restored[0].depends_on, calls[0].depends_on);
    assert_eq!(ticket_session_id(&payload).as_deref(), Some("session-413"));
}
