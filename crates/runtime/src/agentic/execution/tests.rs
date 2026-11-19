use super::helpers::deterministic_graph_id;
use super::reviewed_goal_fixture::*;
use super::*;
use harness_contract::agent_action::{
    AgentActorBinding, AgentActorKind, AgentInviteInput, TaskClaimInput, TaskPublishInput,
    TeamCreateInput,
};
use harness_contract::skill::{
    SkillAdapterKind, SkillCapabilityProfile, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
};

/// A model backend that never returns, so the agent runtime worker does not
/// trigger the Program-wide worker-exit reconcile while a test is still
/// sequencing its own dispatch calls.
struct HoldingModel;

#[async_trait::async_trait]
impl crate::AgentRuntimeBackend for HoldingModel {
    fn kind(&self) -> crate::AgentBackendKind {
        crate::AgentBackendKind::InProcess
    }
    fn capabilities(&self) -> crate::AgentBackendCapabilities {
        crate::AgentBackendCapabilities {
            backend: crate::AgentBackendKind::InProcess,
            declares_step_binding_v1: false,
            declared_render_assurance: None,
            supports_input: false,
            supports_interrupt: false,
            supports_pause: false,
            supports_resume: false,
            supports_cancel: false,
            supports_shutdown: false,
        }
    }
    async fn execute(
        &self,
        _packet: harness_contract::agent::AgentTaskPacket,
        _selection: crate::AgentModelSelection,
    ) -> Result<harness_contract::agent::AgentReturnPacket, String> {
        std::future::pending().await
    }
}





#[test]
fn claim_heartbeat_follows_only_nonterminal_physical_graphs() {
    let mut graph = ExecutionGraph::new("heartbeat activity");
    graph
        .node_statuses
        .insert("agent".to_string(), ExecutionNodeStatus::Running);
    assert!(!agentic_graph_is_terminal(&graph));
    graph
        .node_statuses
        .insert("agent".to_string(), ExecutionNodeStatus::Completed);
    assert!(agentic_graph_is_terminal(&graph));
}

#[tokio::test]
async fn heartbeat_waits_for_the_agents_real_claim_and_stops_at_graph_terminal() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "heartbeat-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Heartbeat Team".to_string(),
                mission: "own a real claim".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let member = actions
        .apply(&root(
            "heartbeat-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Worker".to_string(),
                mission: "claim autonomously".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let task = actions
        .apply(&root(
            "heartbeat-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Real claim".to_string(),
                objective: "prove ownership".to_string(),
                acceptance: "claim is fenced to the Agent execution".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let execution_id = "heartbeat-agent-execution";
    let projection = actions.project("program-dispatch").expect("projection");
    let actor = agentic_claim_actor(
        &projection,
        &projection.agents[&member],
        &task,
        execution_id,
    );
    let mut graph = ExecutionGraph::new("heartbeat Agent graph");
    graph.id = execution_id.to_string();
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    // The graph lease is only recognised when the graph's lineage is the very
    // session/turn the actor acts in.
    if let Some(lineage) = graph.lineage.as_mut() {
        lineage.session_id = projection.session_id.clone();
        lineage.turn_id = projection.turn_id.clone();
    }
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        AgentTaskExecutor::KIND,
        "test-packet",
    );
    node.id = "heartbeat-agent-node".to_string();
    node.idempotency_key = "heartbeat-agent-node:1".to_string();
    // The Agent actor proves ownership with the packet on its own AgentTask node.
    node.payload_ref = serde_json::to_string(&crate::test_support::agent_task_packet_for_actor(
        &projection,
        &actor,
        execution_id,
        &node.id,
        &task,
    ))
    .expect("fixture AgentTask packet");
    graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    services
        .commit_service()
        .register_graph(graph)
        .expect("register graph");
    assert_eq!(
        agentic_claim_heartbeat_state(
            Arc::downgrade(services.as_ref()),
            &task,
            execution_id,
            &actor,
        )
        .await,
        AgenticClaimDriverState::AwaitingAgentClaim
    );

    actions
        .apply(&agent(
            "agent-owned-heartbeat-claim",
            &team,
            &member,
            execution_id,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: Some("I accept this work".to_string()),
            }),
        ))
        .expect("real Agent claim");
    assert!(matches!(
        agentic_claim_heartbeat_state(
            Arc::downgrade(services.as_ref()),
            &task,
            execution_id,
            &actor,
        )
        .await,
        AgenticClaimDriverState::Owned { .. }
    ));

    let graph = services
        .graph_state_store()
        .load(execution_id)
        .expect("graph");
    services
        .commit_service()
        .apply_command(
            &graph,
            &ExecutionGraphCommand::Cancel {
                expected_revision: graph.revision,
                reason: "worker ended".to_string(),
            },
        )
        .expect("terminalize graph");
    assert_eq!(
        agentic_claim_heartbeat_state(
            Arc::downgrade(services.as_ref()),
            &task,
            execution_id,
            &actor,
        )
        .await,
        AgenticClaimDriverState::Stop
    );
}

#[tokio::test]
async fn heartbeat_guard_aborts_its_task_when_the_worker_scope_ends() {
    use std::sync::atomic::{AtomicBool, Ordering};

    struct DropSignal(Arc<AtomicBool>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    let dropped = Arc::new(AtomicBool::new(false));
    let task_dropped = Arc::clone(&dropped);
    let task = tokio::spawn(async move {
        let _signal = DropSignal(task_dropped);
        std::future::pending::<()>().await;
    });
    tokio::task::yield_now().await;
    drop(AgenticClaimHeartbeatGuard { task });
    for _ in 0..16 {
        if dropped.load(Ordering::SeqCst) {
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(
        dropped.load(Ordering::SeqCst),
        "ending the physical worker scope must cancel its heartbeat future"
    );
}

#[tokio::test]
async fn root_actor_inherits_the_immutable_graph_delegation_scope() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let session_id = "session-root-scope";
    let turn_id = "turn-root-scope";
    let graph_id = "root-scope-execution";
    let objective = format!(
        "{}完整目标必须保留末尾的独立评审与只读约束",
        "明确要求 ".repeat(60)
    );
    let mut graph = ExecutionGraph::new(&objective);
    graph.id = graph_id.to_string();
    graph.lineage = Some(ExecutionGraphLineage {
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        root_task_id: "task-root-scope".to_string(),
        task_id: "task-root-scope".to_string(),
        generation: 1,
    });
    let mut model = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "payload:root-scope",
    );
    model.id = format!("{graph_id}:model");
    model.idempotency_key = format!("{graph_id}:model:1");
    let model_id = model.id.clone();
    let mut guard = ExecutionNodeSpec::new(
        ExecutionNodeKind::Verify,
        "compile_target_guard",
        "payload:scope-guard",
    );
    guard.id = format!("{graph_id}:resource-constraint");
    guard.idempotency_key = format!("{graph_id}:resource-constraint:1");
    guard.resource_scopes = vec!["workspace:.".to_string(), "network:*".to_string()];
    graph.nodes = vec![guard, model];
    services
        .commit_service()
        .register_graph(graph)
        .expect("register root graph");

    let objective_id = harness_contract::agent_action::root_objective_id(session_id, turn_id);
    let trusted = AgentActorBinding {
        original_execution: None,
        objective_id: objective_id.clone(),
        program_id: harness_contract::agent_action::program_id_for_objective(&objective_id),
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        root_execution_id: Some(graph_id.to_string()),
        required_team_count: 0,
        exact_team_count: None,
        objective_summary: "autonomous scoped work".to_string(),
        model_lease: "model:test".to_string(),
        permission_ceiling: Some(PermissionMode::DangerFullAccess),
        resource_scopes: Vec::new(),
        actor_id: format!("root:{session_id}"),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let resolved = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: graph_id.to_string(),
                node_id: model_id,
            },
            Some(trusted),
        )
        .await
        .expect("resolve root actor");

    assert_eq!(resolved.resource_scopes, ["network:*", "workspace:."]);
    assert_eq!(resolved.objective_summary, objective);
}

























#[test]
fn dispatch_single_flight_is_owned_by_each_runtime_instance() {
    let left = RuntimeServices::in_memory().expect("left runtime");
    let right = RuntimeServices::in_memory().expect("right runtime");
    let key = "same-program|same-task|execute|0".to_string();
    let _left = DispatchFlight::acquire(left.agentic_dispatch_flights(), key.clone())
        .expect("left first flight");
    for _ in 0..128 {
        assert!(
            DispatchFlight::acquire(left.agentic_dispatch_flights(), key.clone()).is_none(),
            "a losing admission must not drop the winner's reservation"
        );
    }
    assert!(
        DispatchFlight::acquire(right.agentic_dispatch_flights(), key.clone()).is_some(),
        "one Runtime instance must never suppress another instance's reconciler"
    );
    drop(_left);
    assert!(DispatchFlight::acquire(left.agentic_dispatch_flights(), key).is_some());
}

#[tokio::test]
async fn market_offer_prefers_available_member_and_exit_reconciles_busy_work() {
    use harness_contract::agent_action::{
        AgentActionStatus, AgentAttemptMode, MessagePublishInput, TaskAttemptDispatchInput,
    };
    let services = RuntimeServices::in_memory().unwrap();
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    services
        .agent_runtime()
        .register_backend(Arc::new(HoldingModel));
    let (execution_id, _owner_lease) = crate::test_support::install_original_dispatch_session(
        &services,
        "session-dispatch",
        "turn-dispatch",
        "dispatch-request",
        "task:program-dispatch",
    );
    let root_task_id = "task:program-dispatch";
    let root_spec = services
        .task_runtime_port()
        .bind_task_spec(
            "session-dispatch",
            None,
            harness_contract::task::TaskSpec::new("program-dispatch root task"),
        )
        .expect("root task spec");
    services
        .task_aggregate_service()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: root_task_id.to_string(),
            mission_id: services.mission_runtime().default_mission_id().to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::User,
            origin_session_id: "session-dispatch".to_string(),
            origin_turn_id: "turn-dispatch".to_string(),
            root_task_id: root_task_id.to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
            mission_assigned_by: "fixture".to_string(),
            spec: root_spec,
            evidence_refs: vec![harness_contract::reality::EvidenceRef::observed(
                "test_fixture",
                "test://program-dispatch/root",
            )],
        })
        .expect("root task aggregate");
    services.shutdown_maintenance().await;
    let team = actions
        .apply(&root_in_dispatch(
            &execution_id,
            "market-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Readers".into(),
                mission: "inspect evidence".into(),
                objective: None,
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for index in 0..2 {
        actions.apply(&root_in_dispatch(&execution_id, &format!("market-member-{index}"), AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
            "team_ref":team,"role":"Reader","mission":"read evidence","required_capabilities":["read"]
        })).unwrap()))).unwrap();
    }
    let mut tasks = Vec::new();
    let mut publications = Vec::new();
    for index in 0..3 {
        let publication = root_in_dispatch(&execution_id, &format!("market-task-{index}"), AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
            "team_ref":team,"title":"Evidence","objective":"inspect evidence","acceptance":"source checked","required_capabilities":["read"]
        })).unwrap()));
        tasks.push(actions.apply(&publication).unwrap().changed_refs[0].clone());
        publications.push(publication);
    }
    let initial = actions.project("program-dispatch").unwrap();
    let mut ranked =
        helpers::eligible_members(&initial, &initial.tasks[&tasks[0]], DispatchMode::Execute);
    ranked.sort_by_key(|member| {
        helpers::member_dispatch_rank(
            &initial,
            member,
            &initial.tasks[&tasks[0]],
            DispatchMode::Execute,
        )
    });
    let volunteer = ranked[1].agent_id.clone();
    let message: MessagePublishInput = serde_json::from_value(serde_json::json!({
        "topic_ref":"topic:program-dispatch","summary":"I can contribute the requested evidence",
        "intent":{"kind":"offer","task_ref":tasks[0]}
    }))
    .unwrap();
    let forged = actions
        .apply(&root_in_dispatch(
            &execution_id,
            "root-fake-offer",
            AgentAction::MessagePublish(message.clone()),
        ))
        .unwrap();
    assert_eq!(
        forged.error.unwrap().code,
        "offer_requires_bound_team_agent"
    );
    let offer = agent_with_lease(&services, 
        "own-offer",
        &team,
        &volunteer,
        "bound-previous-source-run",
        AgentAction::MessagePublish(message),
    );
    let offer_observation = actions.apply(&offer).unwrap();
    if offer_observation.status != AgentActionStatus::Applied {
        eprintln!(
            "OFFER_REJECT status={:?} error={:?}",
            offer_observation.status, offer_observation.error
        );
    }
    assert_eq!(
        offer_observation.status,
        AgentActionStatus::Applied
    );
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: vec!["workspace:.".into()],
    };
    // This admission test authors decline/exit itself. Keep each real Runner
    // backend pending so an unconfigured model cannot fail the opportunity
    // concurrently with those assertions.
    struct MarketPendingWorker {
        task_ref: String,
    }
    #[async_trait::async_trait]
    impl crate::execution_core::graph::executors::AgentTaskBackend for MarketPendingWorker {
        async fn execute(
            &self,
            _: AgentTaskPacket,
        ) -> Result<harness_contract::agent::AgentReturnPacket, String> {
            std::future::pending().await
        }

        async fn cancel(&self, _: &AgentTaskPacket) -> Result<(), String> {
            Ok(())
        }

        fn cancellation_finalized(&self, packet: &AgentTaskPacket) {
            assert_eq!(packet.task_id(), self.task_ref);
        }
    }
    struct MarketWorkerResolver(Vec<Arc<MarketPendingWorker>>);
    impl crate::execution_core::graph::executors::AgentTaskBackendResolver for MarketWorkerResolver {
        fn resolve(
            &self,
            packet: &AgentTaskPacket,
        ) -> Option<Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>> {
            self.0
                .iter()
                .find(|hold| packet.task_id() == hold.task_ref)
                .map(|hold| {
                    hold.clone()
                        as Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>
                })
        }
    }
    services
        .agent_task_executor()
        .install_resolver(Arc::new(MarketWorkerResolver(
            tasks
                .iter()
                .map(|task_ref| {
                    Arc::new(MarketPendingWorker {
                        task_ref: task_ref.clone(),
                    })
                })
                .collect(),
        )));
    eprintln!(
        "PRE_FIRST statuses={:?}",
        actions
            .project("program-dispatch")
            .expect("program")
            .tasks
            .values()
            .map(|task| (&task.task_id, &task.status, &task.claimant))
            .collect::<Vec<_>>()
    );
    {
        let program = actions.project("program-dispatch").expect("program");
        }
    let first = services
        .dispatch_agentic_followups(&offer, context.clone())
        .await
        .unwrap();
    assert!(matches!(first.as_slice(), [receipt] if receipt.agent_ref == volunteer));
    assert_eq!(
        first[0].agent_ref, volunteer,
        "an eligible volunteer precedes deterministic ranking"
    );
    let second = services
        .dispatch_agentic_followups(&publications[1], context.clone())
        .await
        .unwrap();
    let [unique_second_473] = second.as_slice() else { panic!("expected the original second exactly once") };
    assert_ne!(
        unique_second_473.agent_ref, volunteer,
        "an admitted but unclaimed opportunity is busy"
    );
    assert!(services
        .dispatch_agentic_followups(&publications[2], context.clone())
        .await
        .unwrap()
        .is_empty());
    assert!(services
        .dispatch_agentic_followups(&offer, context.clone())
        .await
        .unwrap()
        .is_empty());
    let mut occupied = root_in_dispatch(
        &execution_id,
        "duplicate-member-dispatch",
        AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
            task_ref: tasks[2].clone(),
            execution_id: "forbidden-second-run".into(),
            agent_ref: volunteer.clone(),
            membership_id: format!("membership:{volunteer}:{team}"),
            mode: AgentAttemptMode::Execute,
            generation: 0,
        }),
    );
    occupied.actor.kind = AgentActorKind::Supervisor;
    occupied.actor.actor_id = "runtime.program-supervisor".into();
    assert_eq!(
        actions.apply_runtime_supervisor(&occupied).unwrap().error.unwrap().code,
        "agent_has_active_opportunity"
    );
    let decline = agent_with_lease(&services, 
        "volunteer-declines-original",
        &team,
        &volunteer,
        &first[0].graph_id,
        AgentAction::MessagePublish(
            serde_json::from_value(serde_json::json!({
                "topic_ref":"topic:program-dispatch","summary":"The other source is a better fit",
                "intent":{"kind":"decline","task_ref":tasks[0]}
            }))
            .unwrap(),
        ),
    );
    assert_eq!(
        actions.apply(&decline).unwrap().status,
        AgentActionStatus::Applied
    );
    let resumed = services
        .dispatch_ready_agentic_work("program-dispatch")
        .await
        .unwrap();
    assert!(matches!(resumed.as_slice(), [receipt]
        if receipt.task_ref == tasks[2] && receipt.agent_ref == volunteer));
    assert!(services
        .dispatch_ready_agentic_work("program-dispatch")
        .await
        .unwrap()
        .is_empty());
    let projection = actions.project("program-dispatch").unwrap();
    let live_claims = projection
        .tasks
        .values()
        .filter_map(|task| {
            task.claim_execution_id
                .as_ref()
                .map(|execution| (task.task_id.clone(), execution.clone()))
        })
        .collect::<Vec<_>>();
    for (task_ref, execution) in live_claims {
        let current = actions.project("program-dispatch").unwrap();
        let mut release = root_in_dispatch(
            &execution_id,
            &format!("busy-release-{task_ref}"),
            AgentAction::TaskRelease(harness_contract::agent_action::TaskReleaseInput {
                task_ref: task_ref.clone(),
                reason: "busy worker returned".into(),
                expected_claim_generation: current.tasks.get(&task_ref).map(|task| task.claim_generation),
            }),
        );
        release.actor.kind = AgentActorKind::Supervisor;
        release.actor.actor_id = "runtime.program-supervisor".into();
        release.actor.execution_id = Some(execution);
        release.expected_revision = Some(current.revision);
        let observation = actions.apply_runtime_supervisor(&release).unwrap();
        assert_eq!(
            observation.status,
            AgentActionStatus::Applied,
            "busy release rejected: {:?}",
            observation.error
        );
    }
    let projection = actions.project("program-dispatch").unwrap();
    assert_eq!(
        projection.tasks[&tasks[0]].status,
        AgenticTaskStatus::Published
    );
    assert!(projection.tasks[&tasks[0]].active_attempts.is_empty());
    assert!(projection
        .tasks
        .values()
        .all(|task| task.claimant.is_none() && task.failed_attempts == 0));
}

#[tokio::test]
async fn typed_decline_dispatches_next_member_once_and_exhaustion_stays_quiet() {
    use harness_contract::agent_action::{AgentActionStatus, MessagePublishInput};
    let services = Arc::new(RuntimeServices::in_memory().unwrap());
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    services.agent_task_executor().install_resolver(Arc::new(
        crate::agentic::coordination::tests::ControlledAgenticWorker,
    ));
    let (execution_id, _owner_lease) = crate::test_support::install_original_dispatch_fixture(
        &services,
        "session-dispatch",
        "turn-dispatch",
        "decline-request",
        "task:program-dispatch",
    );
    services.shutdown_maintenance().await;
    let team = actions
        .apply(&root_in_dispatch(&execution_id, 
            "decline-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Candidates".into(),
                mission: "inspect evidence".into(),
                objective: None,
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for index in 0..2 {
        actions.apply(&root_in_dispatch(&execution_id, &format!("decline-member-{index}"), AgentAction::AgentInvite(
            serde_json::from_value(serde_json::json!({"team_ref":team,"role":"Reader","mission":"read evidence","required_capabilities":["read"]})).unwrap()
        ))).unwrap();
    }
    let publish = root_in_dispatch(&execution_id, "decline-task", AgentAction::TaskPublish(
        serde_json::from_value(serde_json::json!({"team_ref":team,"title":"Evidence","objective":"read evidence","acceptance":"evidence inspected","required_capabilities":["read"]})).unwrap()
    ));
    let task = actions.apply(&publish).unwrap().changed_refs[0].clone();
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: vec!["workspace:.".into()],
    };
    let first = services
        .dispatch_agentic_followups(&publish, context.clone())
        .await
        .unwrap();
    assert!(matches!(first.as_slice(), [receipt] if receipt.task_ref == task));
    let mut receipt = first[0].clone();
    let mut visited = BTreeSet::new();
    for index in 0..2 {
        assert!(visited.insert(receipt.agent_ref.clone()));
        let input: MessagePublishInput = serde_json::from_value(serde_json::json!({
            "topic_ref":"topic:program-dispatch", "summary":"Another contributor is better suited",
            "intent":{"task_ref":task,"kind":"decline"}
        }))
        .unwrap();
        let decline = agent_with_lease(
            &services,
            &format!("decline-{index}"),
            &team,
            &receipt.agent_ref,
            &receipt.graph_id,
            AgentAction::MessagePublish(input),
        );
        let observation = actions.apply(&decline).expect("decline applies");
        assert_eq!(
            observation.status,
            AgentActionStatus::Applied,
            "decline {index} rejected for agent {} graph {}: {:?}",
            receipt.agent_ref, receipt.graph_id,
            observation.error
        );
        let declined_revision = actions.project("program-dispatch").unwrap().revision;
        let graph = services
            .graph_state_store()
            .load(&receipt.graph_id)
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
        services
            .agent_runtime()
            .publish_run_exit_for_test(&packet)
            .await
            .unwrap();
        services
            .settle_abandoned_agentic_attempt(&packet, "declined worker returned")
            .await
            .unwrap();
        assert_eq!(
            actions.project("program-dispatch").unwrap().revision,
            declined_revision,
            "semantic decline must not become a backend failure on return"
        );
        let next = services
            .dispatch_agentic_followups(&decline, context.clone())
            .await
            .unwrap();
        assert_eq!(next.len(), usize::from(index == 0));
        assert!(actions.apply(&decline).unwrap().duplicate);
        assert!(services
            .dispatch_agentic_followups(&decline, context.clone())
            .await
            .unwrap()
            .is_empty());
        if let Some(next) = next.first() {
            receipt = next.clone();
        }
    }
    let projection = actions.project("program-dispatch").unwrap();
    assert_eq!(projection.tasks[&task].status, AgenticTaskStatus::Published);
    assert_eq!(projection.tasks[&task].failed_attempts, 0);
    assert!(projection.tasks[&task].active_attempts.is_empty());
    assert!(helpers::eligible_members(
        &projection,
        &projection.tasks[&task],
        DispatchMode::Execute
    )
    .is_empty());
    assert!(services
        .recover_agentic_programs_on_startup()
        .await
        .unwrap()
        .is_empty());
    assert_eq!(
        actions.project("program-dispatch").unwrap().revision,
        projection.revision
    );
}

#[tokio::test]
async fn dispatch_tries_admissible_members_without_duplicate_model_work() {
    let services = Arc::new(RuntimeServices::in_memory().unwrap());
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    services.agent_task_executor().install_resolver(Arc::new(
        crate::agentic::coordination::tests::ControlledAgenticWorker,
    ));
    let (execution_id, _owner_lease) = crate::test_support::install_original_dispatch_fixture(
        &services,
        "session-dispatch",
        "turn-dispatch",
        "admissible-request",
        "task:program-dispatch",
    );
    services.shutdown_maintenance().await;
    let team = actions
        .apply(&root_in_dispatch(&execution_id, 
            "candidate-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Candidates".into(),
                mission: "read evidence".into(),
                objective: None,
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for index in 0..2 {
        actions
            .apply(&root_in_dispatch(&execution_id, 
                &format!("candidate-{index}"),
                AgentAction::AgentInvite(AgentInviteInput {
                    team_ref: team.clone(),
                    role: "Reader".into(),
                    mission: "read evidence".into(),
                    required_capabilities: vec!["read".into()],
                    existing_agent_ref: None,
                    definition_ref: None,
                    model_profile_ref: None,
                    expertise_hints: vec![],
                    execution_requirements: vec![],
                }),
            ))
            .unwrap();
    }
    let task = actions
        .apply(&root_in_dispatch(&execution_id, 
            "candidate-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Read evidence".into(),
                objective: "read evidence".into(),
                acceptance: "evidence inspected".into(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".into()],
                depends_on: vec![],
                obligation_refs: vec![],
                purpose: Default::default(),
                execution_requirements: vec![],
                expertise_hints: vec![],
            }),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    let mut projection = actions.project("program-dispatch").unwrap();
    let mut ordered =
        helpers::eligible_members(&projection, &projection.tasks[&task], DispatchMode::Execute);
    ordered.sort_by_key(|member| {
        helpers::member_dispatch_rank(
            &projection,
            member,
            &projection.tasks[&task],
            DispatchMode::Execute,
        )
    });
    let invalid = ordered[0].agent_id.clone();
    let valid = ordered[1].agent_id.clone();
    drop(ordered);
    projection
        .agents
        .get_mut(&invalid)
        .unwrap()
        .model_profile_ref = Some("missing-profile".into());
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: vec!["workspace:.".into()],
    };
    let trigger = root_in_dispatch(&execution_id, 
        "candidate-wake",
        AgentAction::StateInspect(harness_contract::agent_action::StateInspectInput {
            query: None,
            wait_for_workers: false,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
        }),
    );
    let receipts = services
        .dispatch_agentic_task(
            &projection,
            &task,
            DispatchMode::Execute,
            &context,
            &trigger,
        )
        .await
        .unwrap();
    assert!(matches!(receipts.as_slice(), [receipt] if receipt.agent_ref == valid));
    assert!(services
        .dispatch_agentic_task(
            &projection,
            &task,
            DispatchMode::Execute,
            &context,
            &trigger
        )
        .await
        .unwrap()
        .is_empty());
    // Publishing discussion alone must not launch an idle worker. A typed
    // request for a ready Task enters the existing dispatcher once.
    let followup_task = actions
        .apply(&root_in_dispatch(&execution_id, 
            "typed-wake-task",
            AgentAction::TaskPublish(
                serde_json::from_value(serde_json::json!({
                    "team_ref":projection.tasks[&task].team_id,"title":"new evidence",
                    "objective":"resolve the new cited gap","acceptance":"source checked",
                    "required_capabilities":["read"]
                }))
                .unwrap(),
            ),
        ))
        .unwrap()
        .changed_refs[0]
        .clone();
    for (label, intent, expected) in [
        ("discussion", None, 0),
        (
            "request-help",
            Some(harness_contract::agent_action::TaskIntentKind::RequestHelp),
            1,
        ),
        (
            "repeat-help",
            Some(harness_contract::agent_action::TaskIntentKind::RequestHelp),
            0,
        ),
    ] {
        let message = root_in_dispatch(&execution_id, 
            label,
            AgentAction::MessagePublish(harness_contract::agent_action::MessagePublishInput {
                topic_ref: format!("topic:{}", projection.program_id),
                summary: Some("A new contribution is available".into()),
                content_ref: None,
                refs: vec![followup_task.clone()],
                recipients: vec![],
                issue_dispositions: vec![],
                intent: intent.map(|kind| harness_contract::agent_action::TaskIntent {
                    task_ref: followup_task.clone(),
                    kind,
                    reason_ref: None,
                    requested_capability_refs: vec![],
                }),
            }),
        );
        assert_eq!(
            actions.apply(&message).unwrap().status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        assert_eq!(
            services
                .dispatch_agentic_followups(&message, context.clone())
                .await
                .unwrap()
                .len(),
            expected,
            "{label}"
        );
    }
    projection.agents.get_mut(&valid).unwrap().model_profile_ref =
        Some("also-missing-profile".into());
    // Occupied work is a no-op, even when a stale caller's semantic snapshot
    // contains bad profiles. Test capability rejection at admission itself.
    for member in [&invalid, &valid] {
        let error = super::admission::resolve_agentic_execution_admission(
            &services,
            &projection.agents[member],
            &projection.tasks[&task],
            &context,
            DispatchMode::Execute,
        )
        .unwrap_err();
        assert!(error.contains("profile"), "{error}");
    }
}

#[test]
fn admission_intersects_multiple_definition_skills_and_custom_tools() {
    assert_eq!(
        super::admission::contributed_skill_tools(
            &BTreeSet::from([
                "read_file".into(),
                "custom_data_lookup".into(),
                "not_admitted".into()
            ]),
            &BTreeSet::from(["read_file".into()]),
            &["read_file".into(), "custom_data_lookup".into()]
        ),
        vec!["custom_data_lookup"]
    );
    let services = RuntimeServices::in_memory().expect("runtime");
    let mut entry = services
        .agent_runtime()
        .catalog()
        .all()
        .into_iter()
        .find(|entry| entry.capabilities == vec!["read".to_string()])
        .expect("read definition");
    entry.skill_refs = vec!["skill.one".to_string(), "skill.two@2".to_string()];
    let profile = |skill_id: &str, version: Option<&str>| SkillCapabilityProfile {
        skill_id: skill_id.to_string(),
        name: skill_id.to_string(),
        version: version.map(str::to_string),
        source_root: format!("/skills/{skill_id}"),
        package_fingerprint: format!("digest:{skill_id}"),
        kind: SkillKind::Workflow,
        lifecycle_status: SkillLifecycleStatus::UsablePrompt,
        adapters: vec![SkillAdapterKind::PromptOnly],
        risk_level: SkillRiskLevel::Low,
        entrypoints: Vec::new(),
        inspection_summary: vec!["custom analysis".to_string()],
        structured_dependencies: Vec::new(),
    };
    let catalog = crate::skill::test_support::catalog(
        vec![profile("skill.one", None), profile("skill.two", Some("2"))],
        vec![
            crate::RuntimeSkillPromptAsset {
                skill_id: "skill.one".to_string(),
                version: None,
                content: "one".to_string(),
                pin: crate::skill::test_support::pin(
                    "skill.one",
                    None,
                    "one",
                    &["custom_data_lookup"],
                ),
                source_ref: "skill://one".to_string(),
                tool_refs: vec!["custom_data_lookup".to_string()],
            },
            crate::RuntimeSkillPromptAsset {
                skill_id: "skill.two".to_string(),
                version: Some("2".to_string()),
                content: "two".to_string(),
                pin: crate::skill::test_support::pin(
                    "skill.two",
                    Some("2"),
                    "two",
                    &["custom_report_reader"],
                ),
                source_ref: "skill://two@2".to_string(),
                tool_refs: vec!["custom_report_reader".to_string()],
            },
        ],
    );
    let (skills, skill_tools, skill_pins) = effective_skill_grants(&entry, &catalog).unwrap();
    assert_eq!(skills, vec!["skill.one", "skill.two@2"]);
    assert_eq!(
        skill_pins
            .iter()
            .map(|pin| pin.installation.skill_id.as_str())
            .collect::<Vec<_>>(),
        vec!["skill.one", "skill.two"]
    );
    assert_eq!(
        skill_tools,
        BTreeSet::from([
            "custom_data_lookup".to_string(),
            "custom_report_reader".to_string(),
        ])
    );

    let resolved = resolve_agent_capability(AgentCapabilityRequest {
        role_id: "custom-reader".to_string(),
        allowed_capabilities: vec!["read".to_string()],
        evidence_duties: Vec::new(),
    });
    let mut requested = resolved.allowed_tools.clone();
    requested.extend(AGENT_ACTION_TOOL_IDS.iter().map(|tool| (*tool).to_string()));
    requested.extend(skill_tools.iter().cloned());
    let snapshot = AgenticToolHostSnapshot {
        tools: requested.clone(),
    };
    let (allowed, missing_capabilities, _missing_tool_alternatives) =
        intersect_agentic_tools(&resolved, &requested, &skill_tools, Some(&snapshot))
            .expect("custom tools are admitted from the active skill and host intersection");
    assert!(allowed.contains(&"custom_data_lookup".to_string()));
    assert!(allowed.contains(&"custom_report_reader".to_string()));
    assert!(missing_capabilities.is_empty());
}

#[test]
fn semantic_capability_hints_translate_without_becoming_physical_authority() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "unsupported-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Specialists".to_string(),
                mission: "exercise a workspace extension".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    actions
        .apply(&root(
            "unsupported-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Specialist".to_string(),
                mission: "research evidence and implement a verified Python experiment".to_string(),
                required_capabilities: vec![
                    "custom_domain_operation".to_string(),
                    "web-research".to_string(),
                    "python".to_string(),
                    "verification".to_string(),
                ],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("the semantic action contract must not own the capability catalog");
    let task = root(
        "unsupported-task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team,
            title: "Run domain operation".to_string(),
            objective: "prove semantic labels are translated by Runtime".to_string(),
            acceptance: "a least-privilege physical capability set is derived".to_string(),
            criterion_source: Default::default(),
            acceptance_checks: Vec::new(),
            required_capabilities: vec!["evidence-gathering".to_string()],
            depends_on: Vec::new(),

            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }),
    );
    let task_ref = actions.apply(&task).expect("task").changed_refs[0].clone();
    let projection = actions.project("program-dispatch").expect("projection");
    let member = projection.agents.values().next().expect("member");
    let task = projection.tasks.get(&task_ref).expect("task");
    let translated = super::admission::required_capabilities(member, task, DispatchMode::Execute);

    assert_eq!(translated, ["network", "read", "search", "test", "write"]);
    assert!(!translated.contains(&"custom_domain_operation".to_string()));
    let selected = super::admission::select_catalog_entry(
        &services.agent_runtime().catalog().all(),
        member,
        task,
        &translated,
    )
    .expect("a general autonomous definition composes the standard physical effects");
    assert_eq!(selected.name, "Autonomous");

    let mut reviewer = member.clone();
    reviewer.required_capabilities = vec!["read".into()];
    reviewer.execution_requirements = vec!["test".into()];
    reviewer.definition_ref = Some("builtin/cowd/execute".into());
    let mut producer_task = task.clone();
    producer_task.required_capabilities = vec!["network".into(), "write".into()];
    producer_task.execution_requirements = vec!["web-research".into()];
    let review =
        super::admission::required_capabilities(&reviewer, &producer_task, DispatchMode::Review);
    assert_eq!(review, ["read", "test"]);
    super::admission::select_catalog_entry(
        &services.agent_runtime().catalog().all(),
        &reviewer,
        &producer_task,
        &review,
    )
    .expect("reviewer need not reproduce network research to review its evidence");
    let execute =
        super::admission::required_capabilities(&reviewer, &producer_task, DispatchMode::Execute);
    assert!(execute.contains(&"network".into()) && execute.contains(&"write".into()));
}

#[test]
fn dispatch_rank_spreads_independent_tasks_to_idle_members() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "load-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Parallel workers".to_string(),
                mission: "execute independent work concurrently".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let busy = actions
        .apply(&root(
            "busy-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Manifest inspector".to_string(),
                mission: "inspect package manifests".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("busy agent")
        .changed_refs[0]
        .clone();
    let idle = actions
        .apply(&root(
            "idle-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Exclude verifier".to_string(),
                mission: "verify exclusion rules".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("idle agent")
        .changed_refs[0]
        .clone();
    let first_task = actions
        .apply(&root(
            "first-load-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Inspect manifests".to_string(),
                objective: "inspect package metadata".to_string(),
                acceptance: "report evidence".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("first task")
        .changed_refs[0]
        .clone();
    let second_task = actions
        .apply(&root(
            "second-load-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Verify exclusions".to_string(),
                objective: "verify exclusion rules".to_string(),
                acceptance: "report evidence".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("second task")
        .changed_refs[0]
        .clone();
    actions
        .apply(&agent_with_lease(&services, 
            "busy-claim",
            &team,
            &busy,
            "busy-execution",
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: first_task,
                reason: None,
            }),
        ))
        .expect("claim first task");

    let projection = actions.project("program-dispatch").expect("projection");
    let task = &projection.tasks[&second_task];
    assert!(
        member_dispatch_rank(
            &projection,
            &projection.agents[&idle],
            task,
            DispatchMode::Execute,
        ) < member_dispatch_rank(
            &projection,
            &projection.agents[&busy],
            task,
            DispatchMode::Execute,
        ),
        "an idle eligible member must outrank a member already executing independent work"
    );
}

#[tokio::test]
async fn startup_reconcile_admits_committed_unstarted_work_once() {
    verify_startup_reconciliation(false).await;
}

#[tokio::test]
async fn startup_reconcile_isolates_a_corrupt_program_without_duplicate_work() {
    verify_startup_reconciliation(true).await;
}

async fn verify_startup_reconciliation(with_corrupt_program: bool) {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    // This test reconciles an active graph twice. An unconfigured model may
    // otherwise fail between the two calls and legitimately admit a retry,
    // which is not duplicate admission of the still-running attempt.
    services.agent_task_executor().install_resolver(Arc::new(
        crate::agentic::coordination::tests::ControlledAgenticWorker,
    ));
    if with_corrupt_program {
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: "agentic-program:aaa-corrupt".into(),
                scope: crate::RuntimeEventScope::Program,
                kind: "agentic.action_applied".into(),
                status: None,
                actor: None,
                refs: vec![],
                payload: serde_json::json!({"envelope": "not an action envelope"}),
            })
            .unwrap();
    }
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let (execution_id, _owner_lease) = crate::test_support::install_original_dispatch_fixture(
        &services,
        "session-dispatch",
        "turn-dispatch",
        "startup-request",
        "task:program-dispatch",
    );
    services.shutdown_maintenance().await;
    let team = actions
        .apply(&root_in_dispatch(&execution_id, 
            "recovery-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery".to_string(),
                mission: "resume without model polling".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    actions
        .apply(&root_in_dispatch(&execution_id, 
            "recovery-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Recovery worker".to_string(),
                mission: "resume durable work".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent");
    actions
        .apply(&root_in_dispatch(&execution_id, 
            "recovery-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Resume me".to_string(),
                objective: "prove startup reconciliation".to_string(),
                acceptance: "a real graph is admitted".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task");

    let first = services.recover_agentic_programs_on_startup().await;
    let graph_id = if with_corrupt_program {
        let error = first.expect_err("partial recovery must remain a reported failure");
        assert!(error.contains("aaa-corrupt"), "{error}");
        let projection = actions.project("program-dispatch").unwrap();
        let attempts = projection
            .tasks
            .values()
            .flat_map(|task| task.active_attempts.values())
            .collect::<Vec<_>>();
        assert_eq!(
            attempts.len(),
            1,
            "healthy work must still be physically admitted"
        );
        attempts[0].execution_id.clone()
    } else {
        let first = first.expect("first recovery");
        let [receipt] = first.as_slice() else { panic!("expected one recovered original graph") };
        assert!(!receipt.graph_id.is_empty());
        receipt.graph_id.clone()
    };
    let task_ref = actions
        .project("program-dispatch")
        .unwrap()
        .tasks
        .keys()
        .next()
        .unwrap()
        .clone();
    let graph = services
        .graph_state_store()
        .load(&graph_id)
        .expect("admitted Agent-first graph");
    let packet = serde_json::from_str::<AgentTaskPacket>(&graph.nodes[0].payload_ref)
        .expect("compiled Agent-first packet");
    assert!(
        packet.acceptance.is_empty() && packet.output_acceptance.is_empty(),
        "natural-language Program Task acceptance must stay in the Program review contract, not become an unsatisfiable physical Agent terminal field"
    );
    assert!(graph.nodes[0].acceptance.criteria.is_empty());
    let second = services.recover_agentic_programs_on_startup().await;
    if with_corrupt_program {
        assert!(second.unwrap_err().contains("aaa-corrupt"));
        let projection = actions.project("program-dispatch").unwrap();
        let attempts = projection
            .tasks
            .values()
            .flat_map(|task| task.active_attempts.values())
            .collect::<Vec<_>>();
        assert!(matches!(attempts.as_slice(), [attempt] if attempt.execution_id == graph_id));
        assert_eq!(
            services
                .event_store()
                .stream_revision("agentic-program:aaa-corrupt")
                .unwrap(),
            1
        );
    } else {
        assert!(
            second.expect("idempotent recovery").is_empty(),
            "active graph must suppress duplicate paid work"
        );
    }

    // An unresolved Program must not authorize new work after its root ended.
    let mut projection = actions.project("program-dispatch").expect("projection");
    projection.root_execution_id = Some("cancelled-dispatch-root".into());
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".into(),
        turn_id: "turn-dispatch".into(),
        model_lease: "test".into(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: Vec::new(),
    };
    let trigger = root_in_dispatch(&execution_id, 
        "root-fence-inspect",
        AgentAction::StateInspect(harness_contract::agent_action::StateInspectInput {
            query: None,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
            wait_for_workers: false,
        }),
    );
    let missing = services
        .dispatch_agentic_task(
            &projection,
            &task_ref,
            DispatchMode::Review,
            &context,
            &trigger,
        )
        .await
        .expect_err("a missing bound root must never fall back to unbound dispatch");
    assert!(missing.contains("agentic_dispatch_root_unavailable"));
    let mut root_graph = ExecutionGraph::new("cancelled root");
    root_graph.id = "cancelled-dispatch-root".into();
    crate::test_support::attach_execution_graph_lineage(&mut root_graph);
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        AgentTaskExecutor::KIND,
        "test-packet",
    );
    node.id = "cancelled-root-node".into();
    node.idempotency_key = "cancelled-root-node:1".into();
    root_graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    root_graph.nodes.push(node);
    services
        .commit_service()
        .register_graph(root_graph)
        .expect("register root");
    let root_graph = services
        .graph_state_store()
        .load("cancelled-dispatch-root")
        .expect("root");
    services
        .commit_service()
        .apply_command(
            &root_graph,
            &ExecutionGraphCommand::Cancel {
                expected_revision: root_graph.revision,
                reason: "user cancelled".into(),
            },
        )
        .expect("cancel root");
    for mode in [DispatchMode::Execute, DispatchMode::Review] {
        assert!(services
            .dispatch_agentic_task(&projection, &task_ref, mode, &context, &trigger,)
            .await
            .expect("terminal root is not an admission error")
            .is_empty());
    }
}







#[tokio::test]
async fn startup_reconcile_fails_closed_after_partial_dispatch_without_duplicating_admitted_work() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let (execution_id, _owner_lease) = crate::test_support::install_original_dispatch_fixture(
        &services,
        "session-dispatch",
        "turn-dispatch",
        "partial-recovery-request",
        "task:program-dispatch",
    );
    services.shutdown_maintenance().await;
    let team = actions
        .apply(&root_in_dispatch(&execution_id, 
            "partial-recovery-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Partial recovery".to_string(),
                mission: "prove startup admission remains closed on one failed Task".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let member = actions
        .apply(&root_in_dispatch(&execution_id, 
            "partial-recovery-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Recovery worker".to_string(),
                mission: "run every admissible task".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let valid_task = actions
        .apply(&root_in_dispatch(&execution_id, 
            "partial-recovery-valid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Admissible read".to_string(),
                objective: "admit one idempotent graph".to_string(),
                acceptance: "the graph exists once".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("valid task")
        .changed_refs[0]
        .clone();
    let invalid_task = actions
        .apply(&root_in_dispatch(&execution_id, 
            "partial-recovery-invalid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Unavailable connector action".to_string(),
                objective: "remain durable until a matching Agent definition exists".to_string(),
                acceptance: "startup must stay closed".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["connector_action".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("durable invalid task")
        .changed_refs[0]
        .clone();

    let first_error = services
        .recover_agentic_programs_on_startup()
        .await
        .expect_err("one failed Task must fail the whole startup reconciliation pass");
    assert!(first_error.contains("startup dispatch reconciliation failed"));
    assert!(first_error.contains(&invalid_task));
    assert!(first_error.contains("after 1 idempotent dispatch receipt(s)"));

    let projection = actions.project("program-dispatch").expect("projection");
    let valid_graph_id = deterministic_graph_id(
        &projection.program_id,
        &valid_task,
        &member,
        DispatchMode::Execute,
        projection.tasks[&valid_task]
            .claim_generation
            .saturating_sub(1),
    );
    let before_retry = services
        .graph_state_store()
        .load(&valid_graph_id)
        .expect("the independent valid Task was durably admitted");

    let retry_error = services
        .recover_agentic_programs_on_startup()
        .await
        .expect_err("the unresolved Task must keep startup admission closed");
    assert!(retry_error.contains(&invalid_task));
    let after_retry = services
        .graph_state_store()
        .load(&valid_graph_id)
        .expect("the admitted graph remains available");
    assert_eq!(
        after_retry.id, before_retry.id,
        "retry must retain the deterministic graph identity"
    );
    assert_eq!(
        services
            .event_reader()
            .list_stream(&valid_graph_id)
            .expect("deterministic graph event stream")
            .iter()
            .filter(|event| event.kind == "execution_graph.planned")
            .count(),
        1,
        "retry must not register a second graph or duplicate paid work"
    );
}

#[tokio::test]
async fn action_followups_surface_partial_dispatch_failure_without_duplicating_admitted_work() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    services.agent_task_executor().install_resolver(Arc::new(
        crate::agentic::coordination::tests::ControlledAgenticWorker,
    ));
    let (execution_id, _owner_lease) = crate::test_support::install_original_dispatch_fixture(
        &services,
        "session-dispatch",
        "turn-dispatch",
        "partial-action-request",
        "task:program-dispatch",
    );
    services.shutdown_maintenance().await;
    let team = actions
        .apply(&root_in_dispatch(&execution_id, 
            "partial-action-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Partial action".to_string(),
                mission: "surface every post-commit dispatch failure".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let valid_task = actions
        .apply(&root_in_dispatch(&execution_id, 
            "partial-action-valid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Admissible read".to_string(),
                objective: "admit one deterministic graph".to_string(),
                acceptance: "the graph exists once".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("valid task")
        .changed_refs[0]
        .clone();
    let invalid_task = actions
        .apply(&root_in_dispatch(&execution_id, 
            "partial-action-invalid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Unavailable connector action".to_string(),
                objective: "remain durable until a matching Agent definition exists".to_string(),
                acceptance: "the caller observes the deferred dispatch".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["connector_action".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("durable invalid task")
        .changed_refs[0]
        .clone();
    let invite = root_in_dispatch(&execution_id, 
        "partial-action-agent",
        AgentAction::AgentInvite(AgentInviteInput {
            team_ref: team,
            role: "Read worker".to_string(),
            mission: "run admissible work".to_string(),
            required_capabilities: vec!["read".to_string()],
            existing_agent_ref: None,
            definition_ref: None,
            model_profile_ref: None,
            expertise_hints: Vec::new(),
            execution_requirements: Vec::new(),
        }),
    );
    let member = actions.apply(&invite).expect("agent").changed_refs[0].clone();
    let context = AgenticDispatchContext {
        session_id: "session-dispatch".to_string(),
        turn_id: "turn-dispatch".to_string(),
        model_lease: "test".to_string(),
        permission_ceiling: PermissionMode::ReadOnly,
        resource_scopes: Vec::new(),
    };

    let error = services
        .dispatch_agentic_followups(&invite, context.clone())
        .await
        .expect_err("one failed followup must be visible even when independent work was admitted");
    assert!(error.contains(&invalid_task));
    assert!(error.contains("1 idempotent dispatch receipt(s)"));

    let projection = actions.project("program-dispatch").expect("projection");
    let graph_id = deterministic_graph_id(
        &projection.program_id,
        &valid_task,
        &member,
        DispatchMode::Execute,
        projection.tasks[&valid_task]
            .claim_generation
            .saturating_sub(1),
    );
    let before_retry = services
        .graph_state_store()
        .load(&graph_id)
        .expect("valid Task graph");
    let retry_error = services
        .dispatch_agentic_followups(&invite, context)
        .await
        .expect_err("the unresolved followup remains visible on retry");
    let pre_retry = actions.project("program-dispatch").expect("projection");
    assert!(retry_error.contains(&invalid_task));
    let after_retry = services
        .graph_state_store()
        .load(&graph_id)
        .expect("valid Task graph after retry");
    assert_eq!(after_retry.id, before_retry.id);
    assert_eq!(
        services
            .event_reader()
            .list_stream(&graph_id)
            .unwrap()
            .iter()
            .filter(|event| event.kind == "execution_graph.planned")
            .count(),
        1,
        "worker progress may advance revision; retry must not register another graph"
    );
    assert_eq!(
        services
            .event_reader()
            .list_stream("agentic-program:program-dispatch")
            .unwrap()
            .iter()
            .filter(|event| event.kind == "agentic.action_applied"
                && event.payload["envelope"]["action"]["kind"] == "task_attempt_dispatch"
                && event.payload["envelope"]["action"]["input"]["task_ref"] == valid_task)
            .count(),
        1,
        "retry must not admit another physical attempt for the same task opportunity"
    );
}

#[tokio::test]
async fn startup_reconcile_releases_missing_graph_claim_without_waiting_for_lease() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let (execution_id, _owner_lease) = crate::test_support::install_original_dispatch_fixture(
        &services,
        "session-dispatch",
        "turn-dispatch",
        "orphan-request",
        "task:program-dispatch",
    );
    services.shutdown_maintenance().await;
    let team = actions
        .apply(&root_in_dispatch(&execution_id, 
            "orphan-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Recovery".to_string(),
                mission: "recover orphaned reservations".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let member = actions
        .apply(&root_in_dispatch(&execution_id, 
            "orphan-agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Recovery worker".to_string(),
                mission: "resume immediately".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let task = actions
        .apply(&root_in_dispatch(&execution_id, 
            "orphan-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Orphaned reservation".to_string(),
                objective: "recover it".to_string(),
                acceptance: "new graph admitted".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),

                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let missing_execution = "agentic-graph:missing-after-crash";
    let claim_observation = actions
        .apply(&agent_with_lease(
            &services,
            "orphan-claim",
            &team,
            &member,
            missing_execution,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    assert_eq!(
        claim_observation.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );

    let pre = actions.project("program-dispatch").expect("projection");
    let recovered = services
        .recover_agentic_programs_on_startup()
        .await
        .expect("recovery");
    let [unique_recovered_1801] = recovered.as_slice() else { panic!("expected the original recovered exactly once") };
    assert_ne!(unique_recovered_1801.graph_id, missing_execution);
    let projection = actions.project("program-dispatch").expect("projection");
    assert_eq!(projection.tasks[&task].status, AgenticTaskStatus::Published);
    assert_eq!(projection.tasks[&task].claim_execution_id, None);
    assert_eq!(projection.tasks[&task].claimant, None);
}






// ---- W2: one-shot Task deliverable ---------------------------------------------

async fn one_shot_fixture() -> (
    std::sync::Arc<RuntimeServices>,
    String,
    String,
    String,
    String,
) {
    let services = RuntimeServices::in_memory().expect("runtime");
    services.publish_session_execution_policy(
        "session-dispatch",
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Autonomous,
                1,
                harness_contract::policy::SessionExecutionPolicyOrigin::ConfigDefault,
            ),
        ),
    );
    let actions = services.agent_action_service();
    let team = actions
        .apply(&root(
            "one-shot-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Solo".to_string(),
                mission: "produce".to_string(),
                objective: None,
            }),
        ))
        .expect("team")
        .changed_refs[0]
        .clone();
    let author = actions
        .apply(&root(
            "one-shot-author",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team.clone(),
                role: "Author".to_string(),
                mission: "produce evidence".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("author")
        .changed_refs[0]
        .clone();
    let task = actions
        .apply(&root(
            "one-shot-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "One-shot result".to_string(),
                objective: "produce durable evidence".to_string(),
                acceptance: "independent review accepts the delivered body".to_string(),
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
                required_capabilities: vec!["read".to_string()],
                depends_on: Vec::new(),
                obligation_refs: Vec::new(),
                purpose: Default::default(),
                execution_requirements: Vec::new(),
                expertise_hints: Vec::new(),
            }),
        ))
        .expect("task")
        .changed_refs[0]
        .clone();
    let execution = "one-shot-execution".to_string();
    actions
        .apply(&agent_with_lease(
            &services,
            "one-shot-claim",
            &team,
            &author,
            &execution,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    (services, team, author, task, execution)
}

#[tokio::test]
async fn tool_contract_milestone_persists_once_and_reports_conflicting_writes() {
    let (services, _, author, task_ref, _) = one_shot_fixture().await;
    let projection = services
        .agent_action_service()
        .project("program-dispatch")
        .expect("program projection");
    let task = &projection.tasks[&task_ref];
    let member = &projection.agents[&author];
    let mut admission = super::admission::resolve_agentic_execution_admission(
        &services,
        member,
        task,
        &AgenticDispatchContext {
            session_id: "session-dispatch".to_string(),
            turn_id: "turn-dispatch".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: PermissionMode::ReadOnly,
            resource_scopes: Vec::new(),
        },
        DispatchMode::Execute,
    )
    .expect("bounded worker remains admissible");
    admission.missing_capabilities = vec!["network".to_string()];
    admission.missing_tool_alternatives = vec!["web_search".to_string()];
    for _ in 0..2 {
        super::dispatch::record_tool_contract_milestone(
            &services,
            &projection,
            task,
            member,
            DispatchMode::Execute,
            &admission,
        )
        .expect("persist the same contract idempotently");
    }
    let milestones = services
        .event_store()
        .all_events(256)
        .expect("durable events")
        .into_iter()
        .filter(|event| event.kind == "agentic.tool_contract")
        .collect::<Vec<_>>();
    let [milestone] = milestones.as_slice() else { panic!("expected one original tool contract") };
    assert_eq!(milestone.payload["program_id"], "program-dispatch");
    assert_eq!(milestone.payload["task_ref"], task_ref);
    assert_eq!(milestone.payload["missing_capabilities"][0], "network");

    admission.missing_capabilities.push("write".to_string());
    let error = super::dispatch::record_tool_contract_milestone(
        &services,
        &projection,
        task,
        member,
        DispatchMode::Execute,
        &admission,
    )
    .expect_err("a rejected append must be visible to the dispatch caller");
    assert!(error.contains("conflicts"), "{error}");
    assert!(!admission
        .effective_capabilities
        .contains(&AgentCapability::Network));
    assert!(!admission
        .allowed_tools
        .iter()
        .any(|tool| tool == "web_search"));
}
