use super::helpers::deterministic_graph_id;
use super::*;
use harness_contract::agent_action::{
    AgentActorBinding, AgentActorKind, AgentInviteInput, ArtifactCommitInput, TaskClaimInput,
    TaskPublishInput, TaskReviewDecision, TaskReviewInput, TaskSubmitInput, TeamCreateInput,
};
use harness_contract::skill::{
    SkillAdapterKind, SkillCapabilityProfile, SkillKind, SkillLifecycleStatus, SkillRiskLevel,
};

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
    let mut graph = ExecutionGraph::new("heartbeat Agent graph");
    graph.id = execution_id.to_string();
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        AgentTaskExecutor::KIND,
        "test-packet",
    );
    node.id = "heartbeat-agent-node".to_string();
    node.idempotency_key = "heartbeat-agent-node:1".to_string();
    graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    services
        .commit_service()
        .register_graph(graph)
        .expect("register graph");
    let projection = actions.project("program-dispatch").expect("projection");
    let actor = agentic_claim_actor(
        &projection,
        &projection.agents[&member],
        &task,
        execution_id,
    );
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
    assert_eq!(
        agentic_claim_heartbeat_state(
            Arc::downgrade(services.as_ref()),
            &task,
            execution_id,
            &actor,
        )
        .await,
        AgenticClaimDriverState::Owned
    );

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

fn root(action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            objective_id: "objective-dispatch".to_string(),
            program_id: "program-dispatch".to_string(),
            session_id: "session-dispatch".to_string(),
            turn_id: "turn-dispatch".to_string(),
            root_execution_id: None,
            required_team_count: 1,
            objective_summary: "dispatch objective".to_string(),
            model_lease: "test".to_string(),
            permission_ceiling: Some(PermissionMode::ReadOnly),
            resource_scopes: Vec::new(),
            actor_id: "root:session-dispatch".to_string(),
            kind: AgentActorKind::Root,
            execution_id: None,
            team_id: None,
            agent_id: None,
        },
        expected_revision: None,
        action,
    }
}

fn agent(
    action_id: &str,
    team_id: &str,
    agent_id: &str,
    execution_id: &str,
    action: AgentAction,
) -> AgentActionEnvelope {
    let mut envelope = root(action_id, action);
    envelope.actor.actor_id = agent_id.to_string();
    envelope.actor.kind = AgentActorKind::Agent;
    envelope.actor.execution_id = Some(execution_id.to_string());
    envelope.actor.team_id = Some(team_id.to_string());
    envelope.actor.agent_id = Some(agent_id.to_string());
    envelope
}

#[tokio::test]
async fn root_actor_inherits_the_immutable_graph_delegation_scope() {
    let services = RuntimeServices::in_memory().expect("runtime");
    let session_id = "session-root-scope";
    let turn_id = "turn-root-scope";
    let graph_id = "root-scope-execution";
    let mut graph = ExecutionGraph::new("root scope authority");
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
        objective_id: objective_id.clone(),
        program_id: harness_contract::agent_action::program_id_for_objective(&objective_id),
        session_id: session_id.to_string(),
        turn_id: turn_id.to_string(),
        root_execution_id: Some(graph_id.to_string()),
        required_team_count: 2,
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
}

#[tokio::test]
async fn task_publish_admits_real_agent_graph_with_human_display_identity() {
    exercise_reviewed_goal_delivery(false, false, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn issue_disposition_gates_independent_reviewed_delivery_through_the_same_goal_verifier() {
    exercise_reviewed_goal_delivery(true, false, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn independent_agent_reviews_actual_root_effect_only_after_reading_affected_file() {
    exercise_reviewed_goal_delivery(false, true, DelegatedEffectCase::None).await;
}

#[tokio::test]
async fn delegated_writer_artifact_requires_target_read_and_rejects_late_writes() {
    exercise_reviewed_goal_delivery(false, false, DelegatedEffectCase::Review).await;
}

#[tokio::test]
async fn delegated_writer_withdrawal_fences_new_effects_before_process_cancellation() {
    exercise_reviewed_goal_delivery(false, false, DelegatedEffectCase::Withdraw).await;
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DelegatedEffectCase {
    None,
    Review,
    Withdraw,
}

async fn exercise_reviewed_goal_delivery(
    with_disclosure: bool,
    with_effect: bool,
    delegated_case: DelegatedEffectCase,
) {
    let delegated_effect = delegated_case != DelegatedEffectCase::None;
    let read_host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(read_host.clone()).expect("runtime");
    read_host.services.set(Arc::downgrade(&services)).unwrap();
    let root_mission_id = "mission:auto:dispatch-regression";
    services
        .mission_runtime()
        .create_mission(root_mission_id, "root Agent-first Program", Vec::new())
        .expect("create non-default root mission");
    std::fs::create_dir_all(services.workspace_root().join("crates/runtime"))
        .expect("create bounded test workspace");
    std::fs::write(
        services.workspace_root().join("crates/runtime/Cargo.toml"),
        "[package]\nname = \"runtime-test\"\n",
    )
    .expect("write bounded test manifest");
    let mut root_graph = ExecutionGraph::new("root Agent-first Program");
    root_graph.id = "root-agentic-execution".to_string();
    root_graph.lineage = Some(ExecutionGraphLineage {
        session_id: "session-dispatch".to_string(),
        turn_id: "turn-dispatch".to_string(),
        root_task_id: "task-root-dispatch".to_string(),
        task_id: "task-root-dispatch".to_string(),
        generation: 1,
    });
    let mut root_node = ExecutionNodeSpec::new(
        ExecutionNodeKind::InlineModel,
        "inline_model",
        "payload:root",
    );
    root_node.id = "root-agentic-execution:model".to_string();
    root_node.idempotency_key = "root-agentic-model".to_string();
    root_graph.nodes.push(root_node);
    services
        .commit_service()
        .register_graph(root_graph)
        .expect("register root graph");
    let bind_root = |mut envelope: AgentActionEnvelope| {
        envelope.actor.root_execution_id = Some("root-agentic-execution".to_string());
        envelope
    };
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
    let root_task_spec = services
        .task_runtime_port()
        .bind_task_spec(
            "session-dispatch",
            Some(PermissionMode::DangerFullAccess),
            harness_contract::task::TaskSpec::new("root Agent-first Program"),
        )
        .expect("bind root task policy");
    services
        .task_aggregate_service()
        .create(harness_contract::task::TaskCreateCommand {
            task_id: "task-root-dispatch".to_string(),
            mission_id: root_mission_id.to_string(),
            kind: harness_contract::task::TaskKind::Root,
            origin: harness_contract::task::TaskOrigin::User,
            origin_session_id: "session-dispatch".to_string(),
            origin_turn_id: "turn-dispatch".to_string(),
            root_task_id: "task-root-dispatch".to_string(),
            parent_task_id: None,
            predecessor_task_id: None,
            mission_assignment: harness_contract::task::TaskMissionAssignment::Default,
            mission_assigned_by: "test".to_string(),
            spec: root_task_spec,
            evidence_refs: Vec::new(),
        })
        .expect("create root task");
    let action_service = services.agent_action_service();
    let team_receipt = action_service
        .apply(&bind_root(root(
            "team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research Team".to_string(),
                mission: "investigate".to_string(),
                objective: None,
            }),
        )))
        .expect("team");
    let team_ref = team_receipt.changed_refs[0].clone();
    let agent_ref = action_service
        .apply(&bind_root(root(
            "agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_ref.clone(),
                role: "Researcher".to_string(),
                mission: "collect evidence".to_string(),
                required_capabilities: if delegated_effect {
                    vec!["web-research".into(), "write".into()]
                } else {
                    vec!["web-research".into()]
                },
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        )))
        .expect("agent")
        .changed_refs[0]
        .clone();
    let task = bind_root(root(
        "task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: team_ref.clone(),
            title: "Evidence review".to_string(),
            objective: "review crates/runtime/Cargo.toml".to_string(),
            acceptance: "cite evidence".to_string(),
            required_capabilities: if delegated_effect {
                vec!["python".into(), "verification".into(), "write".into()]
            } else {
                vec!["python".into(), "verification".into()]
            },
            depends_on: Vec::new(),

            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }),
    ));
    let task_ref = action_service.apply(&task).expect("task").changed_refs[0].clone();
    // This fixture drives the tool/action protocol while its actual worker
    // remains active. A backend that already returned is no longer authority
    // for synthetic late submissions after common lifetime settlement.
    let producer_hold = Some({
        Arc::new(ReviewWorkerHold {
            task_ref: task_ref.clone(),
            entered: tokio::sync::Notify::new(),
            cancelled: std::sync::atomic::AtomicBool::new(false),
        })
    });
    if let Some(hold) = &producer_hold {
        services
            .agent_task_executor()
            .install_resolver(Arc::new(ReviewWorkerResolver(hold.clone())));
    }
    let receipts = services
        .dispatch_agentic_followups(
            &task,
            AgenticDispatchContext {
                session_id: "session-dispatch".to_string(),
                turn_id: "turn-dispatch".to_string(),
                model_lease: "default".to_string(),
                permission_ceiling: PermissionMode::DangerFullAccess,
                resource_scopes: vec!["workspace:.".to_string(), "network:*".to_string()],
            },
        )
        .await
        .expect("dispatch");
    assert_eq!(receipts.len(), 1);
    if let Some(hold) = &producer_hold {
        tokio::time::timeout(std::time::Duration::from_secs(10), hold.entered.notified())
            .await
            .unwrap();
    }

    let graph = services
        .graph_state_store()
        .load(&receipts[0].graph_id)
        .expect("graph");
    let packet: AgentTaskPacket =
        serde_json::from_str(&graph.nodes[0].payload_ref).expect("packet");
    assert_eq!(
        packet
            .binding
            .as_ref()
            .map(|binding| binding.definition_ref.definition_id.as_str()),
        Some("builtin/cowd/autonomous")
    );
    packet
        .validate_cohort_prompt_package()
        .expect("Agentic Program cohort scope");
    let cohort = packet
        .cohort_prompt_package
        .as_ref()
        .expect("shared Agentic Program prefix");
    assert!(matches!(
        &cohort.scope,
        harness_contract::agent::CohortPromptScope::AgenticProgram {
            session_id,
            program_id,
            team_id,
        } if session_id == "session-dispatch"
            && program_id == "program-dispatch"
            && team_id == &team_ref
    ));
    let shared_prefix = cohort.render_user_messages().join("\n");
    assert!(shared_prefix.contains("investigate"));
    assert!(!shared_prefix.contains(&task_ref));
    assert!(!shared_prefix.contains(&agent_ref));
    assert_eq!(packet.deadline_at_ms, u64::MAX);
    assert!(packet.objective.contains("First call state_inspect"));
    assert!(packet.objective.contains("actively call task_claim"));
    assert!(packet.objective.contains("Never submit before claiming"));
    assert!(packet.objective.contains("Current work directory"));
    assert!(packet
        .objective
        .contains(&format!("\"scope_ref\":\"{task_ref}\"")));
    assert!(packet.objective.contains("one result owner"));
    assert!(!shared_prefix.contains("Current work directory"));
    assert!(packet.required_acceptance.evidence_obligations.is_empty());
    assert!(packet.objective.contains("read:crates/runtime/Cargo.toml"));
    assert!(packet.objective.contains("orientation only"));
    assert!(!packet
        .objective
        .contains("already committed this exact work"));
    let lineage = graph.lineage.as_ref().expect("child lineage");
    assert_eq!(lineage.root_task_id, "task-root-dispatch");
    assert_eq!(lineage.task_id, receipts[0].task_ref);
    assert_eq!(packet.assignment.root_task_id, "task-root-dispatch");
    assert_eq!(packet.assignment.mission_id, root_mission_id);
    assert_eq!(
        services
            .task_aggregate_service()
            .get(&receipts[0].task_ref)
            .expect("load dispatched task")
            .expect("dispatched task")
            .mission_id,
        root_mission_id
    );
    assert_eq!(
        graph
            .parent_execution
            .as_ref()
            .map(|binding| binding.execution_id.as_str()),
        Some("root-agentic-execution")
    );
    let heartbeat = start_agentic_claim_heartbeat(Arc::downgrade(&services), &packet)
        .expect("start worker-owned claim heartbeat")
        .expect("execute packet owns a heartbeat guard");
    tokio::task::yield_now().await;
    let binding = packet.binding.clone().expect("binding");
    assert_eq!(
        binding.definition_ref.definition_id.as_str(),
        "builtin/cowd/autonomous"
    );
    assert_eq!(
        binding.effective_capabilities,
        vec![
            AgentCapability::Read,
            AgentCapability::Search,
            AgentCapability::Write,
            AgentCapability::Test,
            AgentCapability::Network,
        ]
    );
    assert!(binding.skill_refs.is_empty());
    assert!(binding
        .tool_contract_refs
        .contains(&"state_inspect".to_string()));
    let display = binding.display.expect("display");
    assert_eq!(display.label, "Researcher");
    assert_eq!(display.focus_label.as_deref(), Some("Evidence review"));

    let before_claim = action_service
        .project("program-dispatch")
        .expect("projection before Agent claim");
    assert_eq!(
        before_claim.tasks[&task_ref].status,
        AgenticTaskStatus::Published,
        "dispatch must not impersonate the invited Agent by committing TaskClaim"
    );
    assert!(before_claim.tasks[&task_ref].claimant.is_none());
    assert!(before_claim.tasks[&task_ref].claim_execution_id.is_none());
    drop(heartbeat);

    let actor = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: graph.id.clone(),
                node_id: graph.nodes[0].id.clone(),
            },
            None,
        )
        .await
        .expect("resolve immutable Agent actor");
    assert_eq!(actor.kind, AgentActorKind::Agent);
    assert_eq!(actor.actor_id, agent_ref);
    assert_eq!(actor.agent_id.as_deref(), Some(agent_ref.as_str()));
    assert_eq!(actor.team_id.as_deref(), Some(team_ref.as_str()));
    assert_eq!(actor.execution_id.as_deref(), Some(graph.id.as_str()));

    let mut rebound_graph = graph.clone();
    rebound_graph.id = "agentic-graph:rebound-packet".to_string();
    services
        .commit_service()
        .register_graph(rebound_graph.clone())
        .expect("register corrupted recovery fixture");
    let error = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: rebound_graph.id,
                node_id: rebound_graph.nodes[0].id.clone(),
            },
            None,
        )
        .await
        .expect_err("a packet cannot be rebound to a different parent graph");
    assert_eq!(error, "agent_actor_packet_parent_binding_mismatch");

    action_service
        .apply(&AgentActionEnvelope {
            action_id: "model-agent-claim".to_string(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_ref.clone(),
                reason: Some("I inspected the task and accept it".to_string()),
            }),
        })
        .expect("Agent claims its own work");
    let after_claim = action_service
        .project("program-dispatch")
        .expect("projection after Agent claim");
    assert_eq!(
        after_claim.tasks[&task_ref].status,
        AgenticTaskStatus::Claimed
    );
    assert_eq!(
        after_claim.tasks[&task_ref].claimant.as_deref(),
        Some(agent_ref.as_str())
    );
    assert_eq!(
        after_claim.tasks[&task_ref].claim_execution_id.as_deref(),
        Some(graph.id.as_str())
    );

    let inspect = bind_root(root(
        "inspect",
        AgentAction::StateInspect(harness_contract::agent_action::StateInspectInput {
            query: None,
            wait_for_workers: false,
            scope_ref: None,
            after_revision: None,
            page_cursor: None,
            entry_ref: None,
        }),
    ));
    let before = services
        .graph_state_store()
        .nonterminal_graph_ids_async()
        .await
        .expect("active graphs");
    assert!(services
        .dispatch_agentic_followups(
            &inspect,
            AgenticDispatchContext {
                session_id: "session-dispatch".to_string(),
                turn_id: "turn-dispatch".to_string(),
                model_lease: "default".to_string(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: Vec::new(),
            },
        )
        .await
        .expect("pure inspection")
        .is_empty());
    assert_eq!(
        services
            .graph_state_store()
            .nonterminal_graph_ids_async()
            .await
            .expect("active graphs"),
        before,
        "state_inspect must never admit provider work"
    );

    // Continue real admission through the Objective and experience writers;
    // never manufacture an episode/pattern event in this integration fixture.
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown".into(),
                visibility_scope: "session:session-dispatch".into(),
                expected_bytes: None,
                original_name: Some("result.md".into()),
            },
            if with_disclosure { b"Source-backed findings. Limitation: performance estimate is illustrative, not an enterprise measurement. Disclose this limitation; no measured-performance claim is made.".as_slice() } else { b"Source-backed findings for independent review".as_slice() },
        )
        .await
        .expect("persist content");
    // An ordinary member organizes through its real compiled run grant.
    // Its display role never provides authority and the new membership does
    // not let another run inherit the original organizer's delegation.
    let delegated = |id: &str, action: AgentAction| AgentActionEnvelope {
        action_id: id.into(),
        actor: actor.clone(),
        expected_revision: None,
        action,
    };
    let create = delegated(
        "member-organizes-team",
        AgentAction::TeamCreate(TeamCreateInput {
            name: "Optional verification".into(),
            mission: "organize a bounded follow-up".into(),
            objective: None,
        }),
    );
    let mut missing_run = create.clone();
    missing_run.action_id = "member-no-run-grant".into();
    missing_run.actor.execution_id = Some("nonexistent-organizer-run".into());
    let denied = services.submit_agent_action(&missing_run).await.unwrap();
    assert_eq!(
        denied.status,
        harness_contract::agent_action::AgentActionStatus::Rejected
    );
    let created = services.submit_agent_action(&create).await.unwrap();
    assert_eq!(
        created.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{created:?}"
    );
    let organized_team = created.changed_refs[0].clone();
    let delegated_projection = action_service.project("program-dispatch").unwrap();
    assert_eq!(
        delegated_projection
            .membership_for(&agent_ref, &organized_team)
            .unwrap()
            .delegation_ref,
        actor.execution_id
    );
    let invited = services
        .submit_agent_action(&delegated(
            "member-organizer-invite",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: organized_team.clone(),
                role: "Verification peer".into(),
                mission: "inspect only if needed".into(),
                required_capabilities: vec!["read".into()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: vec![],
                execution_requirements: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        invited.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let publish = delegated(
        "member-organizer-task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: organized_team.clone(),
            title: "Optional check".into(),
            objective: "check need for further work".into(),
            acceptance: "concrete evidence".into(),
            required_capabilities: vec!["read".into()],
            depends_on: vec![],
            obligation_refs: vec![],
            purpose: Default::default(),
            execution_requirements: vec![],
            expertise_hints: vec![],
        }),
    );
    let mut foreign_run = publish.clone();
    foreign_run.action_id = "cannot-inherit-organizer-grant".into();
    foreign_run.actor.execution_id = Some("another-run".into());
    assert_eq!(
        services
            .submit_agent_action(&foreign_run)
            .await
            .unwrap()
            .status,
        harness_contract::agent_action::AgentActionStatus::Rejected
    );
    let published = services.submit_agent_action(&publish).await.unwrap();
    assert_eq!(
        published.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let retired = services
        .submit_agent_action(&bind_root(root(
            "retire-optional-verification",
            AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: published.changed_refs[0].clone(),
                reason_ref: content.selector.clone(),
                evidence_refs: vec![content.selector.clone()],
            }),
        )))
        .await
        .unwrap();
    assert_eq!(
        retired.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let exited = services
        .submit_agent_action(&bind_root(root(
            "exit-optional-team",
            AgentAction::TeamUpdate(harness_contract::agent_action::TeamUpdateInput {
                team_ref: organized_team.clone(),
                mission_ref: None,
                request_retire: true,
                reason_ref: Some(content.selector.clone()),
            }),
        )))
        .await
        .unwrap();
    assert_eq!(
        exited.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    assert_eq!(
        action_service.project("program-dispatch").unwrap().teams[&organized_team].lifecycle,
        crate::agentic::program::AgenticTeamLifecycle::Retired
    );

    let writer = if delegated_effect {
        let writer = crate::agent_in_process_worker::ProcessJsonlToolSession::prepare(
            &services,
            &packet,
            &crate::agent_model_selector::AgentModelSelection {
                model: "test".into(),
                provider: "test".into(),
                registry_revision: 0,
            },
        )
        .unwrap();
        writer
            .execute_tool(
                "delegated-write",
                "write_file",
                r#"{"path":"review-effect.txt","content":"actual delegated write"}"#,
            )
            .await
            .unwrap();
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt")).unwrap(),
            "actual delegated write"
        );
        let (request, calls_before_replay) = {
            let calls = read_host.calls.lock().unwrap();
            (
                calls
                    .iter()
                    .rev()
                    .find(|call| call.tool_name == "write_file")
                    .unwrap()
                    .clone(),
                calls.len(),
            )
        };
        let dispatcher =
            crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
                &services, &packet,
            );
        let (first_replay, second_replay) = tokio::join!(
            dispatcher.execute(request.clone()),
            dispatcher.execute(request)
        );
        assert_eq!(
            first_replay.unwrap().status,
            crate::RuntimeToolExecutionStatus::Executed
        );
        assert_eq!(
            second_replay.unwrap().status,
            crate::RuntimeToolExecutionStatus::Executed
        );
        assert_eq!(
            read_host.calls.lock().unwrap().len(),
            calls_before_replay,
            "durable ToolBatch replay must not repeat the physical write"
        );
        let mut stale_packet = packet.clone();
        stale_packet.attempt += 1;
        let stale = crate::agent_in_process_worker::ProcessJsonlToolSession::prepare(
            &services,
            &stale_packet,
            &crate::agent_model_selector::AgentModelSelection {
                model: "test".into(),
                provider: "test".into(),
                registry_revision: 0,
            },
        )
        .unwrap();
        let error = stale
            .execute_tool(
                "stale-generation-write",
                "write_file",
                r#"{"path":"review-effect.txt","content":"stale generation"}"#,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("tool_effect_authority"),
            "{error}"
        );
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt")).unwrap(),
            "actual delegated write"
        );
        Some(writer)
    } else {
        None
    };
    if delegated_case == DelegatedEffectCase::Withdraw {
        let withdraw = bind_root(root(
            "withdraw-live-producer",
            AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: task_ref.clone(),
                reason_ref: content.selector.clone(),
                evidence_refs: vec![content.selector.clone()],
            }),
        ));
        assert_eq!(
            services
                .submit_agent_action(&withdraw)
                .await
                .unwrap()
                .status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        assert_eq!(
            services
                .graph_state_store()
                .load(packet.graph_id())
                .unwrap()
                .node_statuses[packet.node_id()],
            harness_contract::execution_graph::ExecutionNodeStatus::Running,
            "prove the Program fence before stopping the process"
        );
        let index = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let revision = services.event_store().stream_revision(&index).unwrap();
        let error = writer
            .as_ref()
            .unwrap()
            .execute_tool(
                "withdrawn-but-running",
                "write_file",
                r#"{"path":"review-effect.txt","content":"withdrawn worker must not write"}"#,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("tool_effect_authority"),
            "{error}"
        );
        assert_eq!(
            services.event_store().stream_revision(&index).unwrap(),
            revision
        );
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt")).unwrap(),
            "actual delegated write"
        );
        services
            .dispatch_agentic_followups(
                &withdraw,
                AgenticDispatchContext {
                    session_id: "session-dispatch".into(),
                    turn_id: "turn-dispatch".into(),
                    model_lease: "test".into(),
                    permission_ceiling: PermissionMode::DangerFullAccess,
                    resource_scopes: vec!["workspace:.".into()],
                },
            )
            .await
            .unwrap();
        let current = action_service.project("program-dispatch").unwrap();
        assert_eq!(
            current.tasks[&task_ref].status,
            AgenticTaskStatus::Withdrawn
        );
        assert!(current.tasks[&task_ref].active_attempts.is_empty());
        assert!(producer_hold
            .unwrap()
            .cancelled
            .load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            services.event_store().stream_revision(&index).unwrap(),
            revision,
            "cancellation retains the original effect evidence"
        );
        return;
    }
    let apply_author = |id: &str, action: AgentAction| {
        action_service
            .apply(&AgentActionEnvelope {
                action_id: id.into(),
                actor: actor.clone(),
                expected_revision: None,
                action,
            })
            .expect("author action")
    };
    let artifact = apply_author(
        "experience-artifact",
        AgentAction::ArtifactCommit(ArtifactCommitInput {
            content_ref: content.selector.clone(),
            kind: "report".into(),
            title: "Findings".into(),
            relates_to: vec![],
        }),
    )
    .changed_refs[0]
        .clone();
    let reviewer = action_service
        .apply(&bind_root(root(
            "experience-reviewer",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_ref.clone(),
                role: "Verifier".into(),
                mission: "Inspect evidence".into(),
                required_capabilities: vec!["read".into()],
                existing_agent_ref: None,
                definition_ref: None,
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        )))
        .expect("invite verifier")
        .changed_refs[0]
        .clone();
    let submit = AgentActionEnvelope {
        action_id: "experience-submit".into(),
        actor: actor.clone(),
        expected_revision: None,
        action: AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: task_ref.clone(),
            artifact_refs: vec![artifact.clone()],
            evidence_refs: vec![content.selector.clone()],
            unresolved: if with_disclosure {
                vec!["Performance estimate lacks enterprise measurement".into()]
            } else {
                vec![]
            },
        }),
    };
    assert_eq!(
        action_service.apply(&submit).unwrap().status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    if let Some(writer) = &writer {
        let index = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let revision = services.event_store().stream_revision(&index).unwrap();
        let error = writer
            .execute_tool(
                "late-after-submit",
                "write_file",
                r#"{"path":"review-effect.txt","content":"must never be written"}"#,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("tool_effect_authority"),
            "{error}"
        );
        assert_eq!(
            services.event_store().stream_revision(&index).unwrap(),
            revision
        );
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt")).unwrap(),
            "actual delegated write"
        );
    }
    let review_dispatch = services
        .dispatch_agentic_followups(
            &submit,
            AgenticDispatchContext {
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                model_lease: "test".into(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: if delegated_effect {
                    vec!["workspace:.".into()]
                } else {
                    vec![]
                },
            },
        )
        .await
        .expect("physical reviewer admission");
    assert_eq!(review_dispatch.len(), 1);
    assert_eq!(review_dispatch[0].agent_ref, reviewer);
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        producer_hold.as_ref().unwrap().entered.notified(),
    )
    .await
    .expect("the real independent reviewer remains active during its protocol");
    let review_graph = services
        .graph_state_store()
        .load(&review_dispatch[0].graph_id)
        .unwrap();
    let reviewer_actor = services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: review_graph.id.clone(),
                node_id: review_graph.nodes[0].id.clone(),
            },
            None,
        )
        .await
        .expect("reviewer binding");
    let review_envelope = AgentActionEnvelope {
        action_id: "experience-review".into(),
        actor: reviewer_actor,
        expected_revision: None,
        action: AgentAction::TaskReview(TaskReviewInput {
            task_ref: task_ref.clone(),
            decision: TaskReviewDecision::Accept,
            reason: "Read and checked the persisted artifact".into(),
            evidence_refs: vec![content.selector.clone()],
        }),
    };
    assert!(services
        .submit_agent_action(&review_envelope)
        .await
        .unwrap_err()
        .contains("review_requires_current_physical_reads"));
    let review_packet = serde_json::from_str(&review_graph.nodes[0].payload_ref).unwrap();
    let bridge = crate::agent_in_process_worker::ProcessJsonlToolSession::prepare(
        &services,
        &review_packet,
        &crate::agent_model_selector::AgentModelSelection {
            model: "test".into(),
            provider: "test".into(),
            registry_revision: 0,
        },
    )
    .unwrap();
    bridge
        .execute_tool(
            "read-result",
            "evidence_retrieve",
            &serde_json::json!({"evidence_ref":content.selector}).to_string(),
        )
        .await
        .unwrap();
    let mut newer_generation = action_service.project("program-dispatch").unwrap();
    newer_generation
        .tasks
        .get_mut(&task_ref)
        .unwrap()
        .review_generation += 1;
    assert!(services
        .independent_result_review(
            &review_envelope.actor,
            &newer_generation,
            &[artifact.clone()],
            None,
        )
        .await
        .err()
        .unwrap()
        .contains("attempt is stale"));
    let mut stolen_run = review_envelope.clone();
    stolen_run.action_id = "review-with-producer-run".into();
    stolen_run.actor.execution_id = actor.execution_id.clone();
    assert!(services.submit_agent_action(&stolen_run).await.is_err());
    if delegated_effect {
        assert!(services
            .submit_agent_action(&review_envelope)
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        std::fs::write(
            services.workspace_root().join("unrelated-review.txt"),
            "unrelated",
        )
        .unwrap();
        bridge
            .execute_tool(
                "unrelated-delegated-target",
                "read_file",
                r#"{"path":"unrelated-review.txt"}"#,
            )
            .await
            .unwrap();
        assert!(services
            .submit_agent_action(&review_envelope)
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        bridge
            .execute_tool(
                "actual-delegated-target",
                "read_file",
                r#"{"path":"review-effect.txt"}"#,
            )
            .await
            .unwrap();
    }
    let reviewed = services
        .submit_agent_action(&review_envelope)
        .await
        .unwrap();
    assert_eq!(
        reviewed.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{reviewed:?}"
    );
    services
        .cancel_execution_tree(
            &review_graph.id,
            "mechanical reviewer fixture finished after durable review",
        )
        .await
        .unwrap();
    services
        .cancel_execution_tree(
            packet.graph_id(),
            "mechanical producer fixture finished after durable submission",
        )
        .await
        .unwrap();
    assert!(producer_hold
        .unwrap()
        .cancelled
        .load(std::sync::atomic::Ordering::Acquire));
    if let Some(writer) = writer {
        let error = writer
            .execute_tool(
                "late-after-cancel",
                "write_file",
                r#"{"path":"review-effect.txt","content":"cancelled write must not execute"}"#,
            )
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("tool_effect_authority"),
            "{error}"
        );
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt")).unwrap(),
            "actual delegated write"
        );
    }
    use harness_contract::goal::{GoalCompletion, GoalContract};
    services
        .goal_store()
        .create(GoalContract {
            id: "goal:root-agentic-execution".into(),
            session_id: "session-dispatch".into(),
            objective: "dispatch objective".into(),
            criteria: vec![harness_contract::goal::AcceptanceCriterion {
                id: "reviewed-delivery".into(),
                statement: "Deliver independently reviewed source-backed work".into(),
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: vec!["execution_graph:root-agentic-execution".into()],
                status: harness_contract::goal::AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: vec![],
            phase: "execution".into(),
            evidence_refs: vec![],
            unresolved: vec![],
            blockers: vec![],
            scope: harness_contract::goal::GoalScope::UserObjective,
            user_intent_criterion_id: Some("reviewed-delivery".into()),
            source_intent_ref: Some("session_message:dispatch".into()),
            execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
                objective_id: "objective-dispatch".into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                root_execution_id: "root-agentic-execution".into(),
                agentic_program_id: "program-dispatch".into(),
            }),
            spec_revision: 1,
            spec_digest: "dispatch-test".into(),
            review_refs: vec![],
            waiting: None,
            participation_requirement: None,
            obligations: vec![harness_contract::goal::ObjectiveObligation {
                obligation_id: "source-backed-delivery".into(),
                required: true,
                success_predicate: "Deliver the source-backed result with independent inspection"
                    .into(),
                producer: harness_contract::goal::ObjectiveProducerContract {
                    capability_id: Some("read".into()),
                    ..Default::default()
                },
                evidence_requirement: harness_contract::goal::ObjectiveEvidenceRequirement {
                    required_artifact_kinds: vec!["report".into()],
                    independent_verifier_required: true,
                    reread_required: true,
                },
                state: harness_contract::goal::ObjectiveObligationState::Open,
                artifact_refs: vec![],
                evidence_refs: vec![],
                reread_receipts: vec![],
                verifier_decision: None,
                diagnostic_code: None,
            }],
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        })
        .expect("Objective creation");
    if with_disclosure {
        use harness_contract::agent_action::{
            AgentActionStatus, IssueDisposition, IssueDispositionKind, MessagePublishInput,
            ObjectiveCompleteRequestInput,
        };
        let issue =
            crate::agentic::issues::issues(&action_service.project("program-dispatch").unwrap())
                .remove(0);
        let try_complete = |id: &str| {
            action_service
                .apply(&bind_root(root(
                    id,
                    AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                        result_refs: vec![artifact.clone()],
                        evidence_refs: vec![content.selector.clone()],
                        unresolved: vec![],
                    }),
                )))
                .unwrap()
        };
        assert_eq!(
            try_complete("unclassified-completion").status,
            AgentActionStatus::Rejected
        );
        for (id, disposition) in [
            ("must-resolve", IssueDispositionKind::MustResolve),
            ("explicit-disclosure", IssueDispositionKind::Disclose),
        ] {
            let receipt = action_service
                .apply(&bind_root(root(
                    id,
                    AgentAction::MessagePublish(MessagePublishInput {
                        topic_ref: "topic:program-dispatch".into(),
                        summary: Some(
                            "Explicitly retain the unmeasured estimate limitation".into(),
                        ),
                        content_ref: Some(content.selector.clone()),
                        refs: vec![task_ref.clone()],
                        recipients: vec![],
                        intent: None,
                        issue_dispositions: vec![IssueDisposition {
                            issue_ref: issue.issue_ref.clone(),
                            disposition,
                            reason_ref: content.selector.clone(),
                            evidence_refs: vec![content.selector.clone()],
                        }],
                    }),
                )))
                .unwrap();
            assert_eq!(receipt.status, AgentActionStatus::Applied, "{receipt:?}");
            if disposition == IssueDispositionKind::MustResolve {
                assert_eq!(
                    try_complete("must-resolve-completion").status,
                    AgentActionStatus::Rejected
                );
                assert_eq!(
                    services
                        .goal_store()
                        .get("goal:root-agentic-execution")
                        .unwrap()
                        .unwrap()
                        .completion,
                    GoalCompletion::Open
                );
            }
        }
    }
    // Accepted Task work cannot certify coverage of the original Objective.
    let premature = action_service
        .apply(&bind_root(root(
            "before-original-review",
            AgentAction::ObjectiveCompleteRequest(
                harness_contract::agent_action::ObjectiveCompleteRequestInput {
                    result_refs: vec![artifact.clone()],
                    evidence_refs: vec![content.selector.clone()],
                    unresolved: vec![],
                },
            ),
        )))
        .unwrap();
    assert_eq!(
        premature.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let supervisor =
        crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(services.goal_store()));
    crate::agentic::supervision::reconcile_completion_request(
        &action_service,
        &supervisor,
        "program-dispatch",
    )
    .unwrap();
    let gap = action_service.project("program-dispatch").unwrap();
    assert_eq!(gap.status, crate::AgenticProgramStatus::Open);
    assert_eq!(
        gap.tasks[&task_ref].status,
        crate::AgenticTaskStatus::Accepted
    );
    assert!(services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap()
        .terminal
        .is_none());
    if with_effect {
        let (effect_ref, effect_runtime) =
            root_reads_generic_results(&services, read_host, &content.selector, true, true).await;
        let effect_ref = effect_ref.unwrap();
        let added = services
            .submit_agent_action(&bind_root(root(
                "effect-condition",
                AgentAction::ObjectiveUpdate(
                    harness_contract::agent_action::ObjectiveUpdateInput {
                        criterion_ref: None,
                        operation: harness_contract::agent_action::ObjectiveUpdateOperation::Add,
                        statement_ref: Some(content.selector.clone()),
                        source_refs: vec![content.selector.clone()],
                        reason_ref: Some(content.selector.clone()),
                        evidence_requirements: vec![],
                    },
                ),
            )))
            .await
            .unwrap();
        assert_eq!(
            added.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{added:?}"
        );
        let effect_access = services
            .session_evidence_access(
                "session-dispatch",
                effect_ref.strip_prefix("tool://").unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let wrapped = services
            .submit_agent_action(&bind_root(root(
                "wrapped-effect",
                AgentAction::ArtifactCommit(ArtifactCommitInput {
                    content_ref: effect_access.retrieval_selector.clone(),
                    kind: "receipt".into(),
                    title: "Published effect receipt".into(),
                    relates_to: vec![],
                }),
            )))
            .await
            .unwrap();
        assert_eq!(
            wrapped.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let work = bind_root(root(
            "effect-review-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team_ref.clone(),
                title: "Inspect changed file".into(),
                objective: "Read and review the actual changed file".into(),
                acceptance: "Record the current Goal effect review".into(),
                required_capabilities: vec!["read".into()],
                depends_on: vec![],
                obligation_refs: vec![],
                purpose: harness_contract::agent_action::TaskPurpose::Exploration,
                execution_requirements: vec![],
                expertise_hints: vec![],
            }),
        ));
        let work_receipt = services.submit_agent_action(&work).await.unwrap();
        assert_eq!(
            work_receipt.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let review_context = || AgenticDispatchContext {
            session_id: "session-dispatch".into(),
            turn_id: "turn-dispatch".into(),
            model_lease: "test".into(),
            permission_ceiling: PermissionMode::ReadOnly,
            resource_scopes: vec!["workspace:.".into(), "network:*".into()],
        };
        let hold = Arc::new(ReviewWorkerHold {
            task_ref: work_receipt.changed_refs[0].clone(),
            entered: tokio::sync::Notify::new(),
            cancelled: std::sync::atomic::AtomicBool::new(false),
        });
        services
            .agent_task_executor()
            .install_resolver(Arc::new(ReviewWorkerResolver(hold.clone())));
        let admitted = services
            .dispatch_agentic_followups(&work, review_context())
            .await
            .unwrap();
        assert_eq!(admitted.len(), 1);
        tokio::time::timeout(std::time::Duration::from_secs(10), hold.entered.notified())
            .await
            .expect("review worker actually started");
        let effect_graph = services
            .graph_state_store()
            .load(&admitted[0].graph_id)
            .unwrap();
        let effect_actor = services
            .resolve_agent_action_actor(
                &ExecutionParentBinding {
                    execution_id: effect_graph.id.clone(),
                    node_id: effect_graph.nodes[0].id.clone(),
                },
                None,
            )
            .await
            .unwrap();
        let claimed = services
            .submit_agent_action(&AgentActionEnvelope {
                action_id: "effect-review-claim".into(),
                actor: effect_actor.clone(),
                expected_revision: None,
                action: AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: work_receipt.changed_refs[0].clone(),
                    reason: None,
                }),
            })
            .await
            .unwrap();
        assert_eq!(
            claimed.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{claimed:?}"
        );
        let check_effect = AgentActionEnvelope {
            action_id: "independent-effect-review".into(),
            actor: effect_actor,
            expected_revision: None,
            action: AgentAction::ObjectiveReview(
                harness_contract::agent_action::ObjectiveReviewInput {
                    criterion_ref: added.changed_refs[0].clone(),
                    decision: harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                    result_refs: vec![
                        effect_ref.clone(),
                        wrapped.changed_refs[0].clone(),
                        effect_access.retrieval_selector.clone(),
                    ],
                    evidence_refs: vec![content.selector.clone()],
                    reason_ref: content.selector.clone(),
                },
            ),
        };
        let physical: AgentTaskPacket =
            serde_json::from_str(&effect_graph.nodes[0].payload_ref).unwrap();
        let reader = crate::agent_in_process_worker::ProcessJsonlToolSession::prepare(
            &services,
            &physical,
            &crate::agent_model_selector::AgentModelSelection {
                model: "test".into(),
                provider: "test".into(),
                registry_revision: 0,
            },
        )
        .unwrap();
        reader
            .execute_tool(
                "read-effect-receipt",
                "evidence_retrieve",
                &serde_json::json!({"evidence_ref":effect_ref}).to_string(),
            )
            .await
            .unwrap();
        reader
            .execute_tool(
                "read-effect-content-alias",
                "evidence_retrieve",
                &serde_json::json!({"evidence_ref":effect_access.retrieval_selector}).to_string(),
            )
            .await
            .unwrap();
        assert!(services
            .submit_agent_action(&check_effect)
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        std::fs::write(
            services.workspace_root().join("unrelated-review.txt"),
            "unrelated contents",
        )
        .unwrap();
        reader
            .execute_tool(
                "read-other-file",
                "read_file",
                r#"{"path":"unrelated-review.txt"}"#,
            )
            .await
            .unwrap();
        assert!(services
            .submit_agent_action(&check_effect)
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        reader
            .execute_tool(
                "read-affected-file",
                "read_file",
                r#"{"path":"review-effect.txt"}"#,
            )
            .await
            .unwrap();
        let accepted = services.submit_agent_action(&check_effect).await.unwrap();
        assert_eq!(
            accepted.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{accepted:?}"
        );
        let prior = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let prior_digest = prior
            .reviews
            .last()
            .unwrap()
            .verification
            .as_ref()
            .unwrap()
            .effect_manifest_digest
            .clone();
        write_root_review_effect(
            &effect_runtime,
            "root-write-again",
            "actually revised a second time",
        )
        .await;
        // Negative preparation only: the real request remains uncommitted while
        // this review worker is active. The Goal owner must invalidate its old review.
        let mut proposed = action_service.project("program-dispatch").unwrap();
        proposed.completion_request = Some(
            crate::agentic::program::AgenticCompletionRequestProjection {
                action_id: "test-negative-conclusion".into(),
                requested_by: "root:session-dispatch".into(),
                program_revision: proposed.revision,
                result_refs: vec![artifact.clone()],
                primary_artifact_ref: Some(artifact.clone()),
                evidence_refs: vec![content.selector.clone()],
                unresolved: vec![],
            },
        );
        let stale_conclusion = services
            .goal_store()
            .prepare_program_conclusion(&proposed)
            .unwrap();
        assert!(stale_conclusion.goal.terminal.is_none());
        assert!(stale_conclusion.gaps.contains(&format!(
            "criterion_review_required:{}",
            added.changed_refs[0]
        )));
        let mut refreshed = check_effect.clone();
        refreshed.action_id = "effect-review-after-new-write".into();
        assert!(services
            .submit_agent_action(&refreshed)
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        reader
            .execute_tool(
                "reread-after-second-write",
                "read_file",
                r#"{"path":"review-effect.txt"}"#,
            )
            .await
            .unwrap();
        assert_eq!(
            services
                .submit_agent_action(&refreshed)
                .await
                .unwrap()
                .status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let reviewed = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let proof = reviewed
            .reviews
            .last()
            .unwrap()
            .verification
            .as_ref()
            .unwrap();
        assert!(proof.independence_required);
        assert_eq!(
            proof.reads[0].result_kind,
            harness_contract::goal::GoalResultKind::ToolEffect
        );
        assert_eq!(proof.reads[0].effect_observation_refs.len(), 1);
        assert!(proof.effect_manifest_digest.is_some());
        assert_ne!(proof.effect_manifest_digest, prior_digest);
        assert_eq!(proof.reads.len(), 3);
        assert!(proof
            .reads
            .iter()
            .all(|read| read.result_kind == harness_contract::goal::GoalResultKind::ToolEffect));

        let manifest = proof.work_manifest_digest.clone();
        let withdrawn = bind_root(root(
            "effect-review-finished",
            AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: work_receipt.changed_refs[0].clone(),
                reason_ref: content.selector.clone(),
                evidence_refs: vec![content.selector.clone()],
            }),
        ));
        assert_eq!(
            services
                .submit_agent_action(&withdrawn)
                .await
                .unwrap()
                .status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        services
            .dispatch_agentic_followups(&withdrawn, review_context())
            .await
            .unwrap();
        let drained = services
            .agent_action_service()
            .project("program-dispatch")
            .unwrap();
        assert_eq!(
            drained.tasks[&work_receipt.changed_refs[0]].status,
            crate::AgenticTaskStatus::Withdrawn
        );
        assert!(drained.tasks[&work_receipt.changed_refs[0]]
            .active_attempts
            .is_empty());
        assert!(hold.cancelled.load(std::sync::atomic::Ordering::Acquire));
        assert_eq!(
            crate::agentic::review_evidence::work_manifest_digest(&drained),
            manifest
        );
        let mut stale = check_effect.clone();
        stale.action_id = "withdrawn-review-cannot-mutate-goal".into();
        assert_eq!(
            services.submit_agent_action(&stale).await.unwrap().status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
    } else if delegated_effect {
        let (_, runtime) =
            root_reads_generic_results(&services, read_host, &content.selector, false, false).await;
        root_review_target_tool(
            &runtime,
            "root-verify-delegated-target",
            "read_file",
            "review-effect.txt",
            "",
        )
        .await;
    } else {
        root_reads_review_result(&services, read_host, &content.selector).await;
    }
    let obligation_review = services
        .submit_agent_action(&bind_root(root(
            "source-obligation-review",
            AgentAction::ObjectiveReview(harness_contract::agent_action::ObjectiveReviewInput {
                criterion_ref: "source-backed-delivery".into(),
                decision: harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                result_refs: vec![artifact.clone()],
                evidence_refs: vec![content.selector.clone()],
                reason_ref: content.selector.clone(),
            }),
        )))
        .await
        .unwrap();
    assert_eq!(
        obligation_review.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let reviewed_goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    assert_eq!(
        reviewed_goal.obligations[0].state,
        harness_contract::goal::ObjectiveObligationState::Satisfied
    );
    assert!(!reviewed_goal.obligations[0].reread_receipts.is_empty());
    assert_eq!(
        reviewed_goal.obligations[0].verifier_decision.as_ref(),
        obligation_review.changed_refs.first()
    );
    assert_eq!(
        reviewed_goal.spec_revision,
        if with_effect { 2 } else { 1 },
        "review changes progress, not requirements"
    );
    let semantic_review = services
        .submit_agent_action(&bind_root(root(
            "original-objective-review",
            AgentAction::ObjectiveReview(harness_contract::agent_action::ObjectiveReviewInput {
                criterion_ref: "reviewed-delivery".into(),
                decision: harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                result_refs: vec![artifact.clone()],
                evidence_refs: vec![content.selector.clone()],
                reason_ref: content.selector.clone(),
            }),
        )))
        .await
        .unwrap();
    assert_eq!(
        semantic_review.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let completion = action_service
        .apply(&bind_root(root(
            "experience-complete",
            AgentAction::ObjectiveCompleteRequest(
                harness_contract::agent_action::ObjectiveCompleteRequestInput {
                    result_refs: vec![artifact],
                    evidence_refs: vec![content.selector],
                    unresolved: vec![],
                },
            ),
        )))
        .unwrap();
    assert_eq!(
        completion.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{completion:?}"
    );
    let objective_supervisor =
        crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(services.goal_store()));
    crate::agentic::supervision::reconcile_completion_request(
        &action_service,
        &objective_supervisor,
        "program-dispatch",
    )
    .expect("verified Objective");
    if delegated_effect {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(goal.completion, GoalCompletion::Satisfied);
        assert_eq!(
            action_service.project("program-dispatch").unwrap().status,
            crate::AgenticProgramStatus::Verified
        );
        for review in &goal.reviews {
            let proof = review.verification.as_ref().unwrap();
            assert_eq!(
                proof.effect_source_refs,
                vec![format!(
                    "execution-agent-receipts:{}:{}:{}",
                    packet.graph_id(),
                    packet.node_id(),
                    packet.attempt
                )]
            );
            assert!(proof.effect_manifest_digest.is_some());
            assert!(proof.reads.iter().all(|read| read.result_kind
                == harness_contract::goal::GoalResultKind::ToolEffect
                && !read.effect_observation_refs.is_empty()));
        }
    }
    if with_disclosure {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(goal.completion, GoalCompletion::Satisfied);
        let program = action_service.project("program-dispatch").unwrap();
        assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
        assert_eq!(
            program.tasks[&task_ref].unresolved,
            vec!["Performance estimate lacks enterprise measurement"]
        );
        let issue = crate::agentic::issues::issues(&program).remove(0);
        assert_eq!(
            issue.disposition.unwrap().disposition,
            harness_contract::agent_action::IssueDispositionKind::Disclose
        );
        assert!(issue.adjudication_ref.is_some());
        return;
    }
    let projector =
        crate::evolution::collaboration_experience::CollaborationExperienceProjector::new(
            Arc::clone(services.event_store()),
            services.graph_state_store().clone(),
            "test-workspace".into(),
        );
    loop {
        let pass = projector
            .project_available(64)
            .expect("production episode projection");
        if !pass.backlog {
            break;
        }
    }
    let episodes = services
        .collaboration_experience_episodes(10)
        .expect("public experience read model");
    assert_eq!(episodes.len(), 1);
    assert!(episodes[0].is_pattern_eligible(), "{:#?}", episodes[0]);
    assert!(episodes[0]
        .resource_summary
        .context_reservation_tokens
        .is_none());
    assert!(!serde_json::to_string(&episodes[0])
        .unwrap()
        .contains("Source-backed findings"));
    assert!(
        services
            .collaboration_semantic_patterns(10)
            .unwrap()
            .is_empty(),
        "one Turn is not a reusable pattern"
    );
    projector.project_available(64).expect("idempotent replay");
    assert_eq!(
        services
            .collaboration_experience_episodes(10)
            .unwrap()
            .len(),
        1
    );
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
    let team = actions
        .apply(&root(
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
        actions.apply(&root(&format!("market-member-{index}"), AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
            "team_ref":team,"role":"Reader","mission":"read evidence","required_capabilities":["read"]
        })).unwrap()))).unwrap();
    }
    let mut tasks = Vec::new();
    let mut publications = Vec::new();
    for index in 0..3 {
        let publication = root(&format!("market-task-{index}"), AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
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
        .apply(&root(
            "root-fake-offer",
            AgentAction::MessagePublish(message.clone()),
        ))
        .unwrap();
    assert_eq!(
        forged.error.unwrap().code,
        "offer_requires_bound_team_agent"
    );
    let offer = agent(
        "own-offer",
        &team,
        &volunteer,
        "bound-previous-source-run",
        AgentAction::MessagePublish(message),
    );
    assert_eq!(
        actions.apply(&offer).unwrap().status,
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
    struct MarketWorkerResolver(Vec<Arc<ReviewWorkerHold>>);
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
                    Arc::new(ReviewWorkerHold {
                        task_ref: task_ref.clone(),
                        entered: tokio::sync::Notify::new(),
                        cancelled: std::sync::atomic::AtomicBool::new(false),
                    })
                })
                .collect(),
        )));
    let first = services
        .dispatch_agentic_followups(&offer, context.clone())
        .await
        .unwrap();
    assert_eq!(first.len(), 1);
    assert_eq!(
        first[0].agent_ref, volunteer,
        "an eligible volunteer precedes deterministic ranking"
    );
    let second = services
        .dispatch_agentic_followups(&publications[1], context.clone())
        .await
        .unwrap();
    assert_eq!(second.len(), 1);
    assert_ne!(
        second[0].agent_ref, volunteer,
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
    let mut occupied = root(
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
        actions.apply(&occupied).unwrap().error.unwrap().code,
        "agent_has_active_opportunity"
    );
    let decline = agent(
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
    assert_eq!(resumed.len(), 1);
    assert_eq!(resumed[0].task_ref, tasks[2]);
    assert_eq!(resumed[0].agent_ref, volunteer);
    assert!(services
        .dispatch_ready_agentic_work("program-dispatch")
        .await
        .unwrap()
        .is_empty());
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
    let team = actions
        .apply(&root(
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
        actions.apply(&root(&format!("decline-member-{index}"), AgentAction::AgentInvite(
            serde_json::from_value(serde_json::json!({"team_ref":team,"role":"Reader","mission":"read evidence","required_capabilities":["read"]})).unwrap()
        ))).unwrap();
    }
    let publish = root("decline-task", AgentAction::TaskPublish(
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
    assert_eq!(first.len(), 1);
    let mut receipt = first[0].clone();
    let mut visited = BTreeSet::new();
    for index in 0..2 {
        assert!(visited.insert(receipt.agent_ref.clone()));
        let input: MessagePublishInput = serde_json::from_value(serde_json::json!({
            "topic_ref":"topic:program-dispatch", "summary":"Another contributor is better suited",
            "intent":{"task_ref":task,"kind":"decline"}
        }))
        .unwrap();
        let decline = agent(
            &format!("decline-{index}"),
            &team,
            &receipt.agent_ref,
            &receipt.graph_id,
            AgentAction::MessagePublish(input),
        );
        assert_eq!(
            actions.apply(&decline).unwrap().status,
            AgentActionStatus::Applied
        );
        let declined_revision = actions.project("program-dispatch").unwrap().revision;
        let graph = services
            .graph_state_store()
            .load(&receipt.graph_id)
            .unwrap();
        let packet: AgentTaskPacket = serde_json::from_str(&graph.nodes[0].payload_ref).unwrap();
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
    let team = actions
        .apply(&root(
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
            .apply(&root(
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
        .apply(&root(
            "candidate-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Read evidence".into(),
                objective: "read evidence".into(),
                acceptance: "evidence inspected".into(),
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
    let trigger = root(
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
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].agent_ref, valid);
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
        .apply(&root(
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
        let message = root(
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
    let catalog = RuntimeSkillCatalog::new(
        vec![profile("skill.one", None), profile("skill.two", Some("2"))],
        vec![
            crate::RuntimeSkillPromptAsset {
                skill_id: "skill.one".to_string(),
                version: None,
                content: "one".to_string(),
                source_ref: "skill://one".to_string(),
                tool_refs: vec!["custom_data_lookup".to_string()],
            },
            crate::RuntimeSkillPromptAsset {
                skill_id: "skill.two".to_string(),
                version: Some("2".to_string()),
                content: "two".to_string(),
                source_ref: "skill://two@2".to_string(),
                tool_refs: vec!["custom_report_reader".to_string()],
            },
        ],
    );
    let (skills, skill_tools) = effective_skill_grants(&entry, &catalog);
    assert_eq!(skills, vec!["skill.one", "skill.two@2"]);
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
    let allowed = intersect_agentic_tools(&resolved, &requested, &skill_tools, Some(&snapshot))
        .expect("custom tools are admitted from the active skill and host intersection");
    assert!(allowed.contains(&"custom_data_lookup".to_string()));
    assert!(allowed.contains(&"custom_report_reader".to_string()));
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
        .apply(&agent(
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
    let team = actions
        .apply(&root(
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
        .apply(&root(
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
        .apply(&root(
            "recovery-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Resume me".to_string(),
                objective: "prove startup reconciliation".to_string(),
                acceptance: "a real graph is admitted".to_string(),
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
        assert_eq!(first.len(), 1);
        first[0].graph_id.clone()
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
        assert_eq!(attempts.len(), 1);
        assert_eq!(attempts[0].execution_id, graph_id);
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
    let trigger = root(
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
    let team = actions
        .apply(&root(
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
        .apply(&root(
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
        .apply(&root(
            "partial-recovery-valid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Admissible read".to_string(),
                objective: "admit one idempotent graph".to_string(),
                acceptance: "the graph exists once".to_string(),
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
        .apply(&root(
            "partial-recovery-invalid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team,
                title: "Unavailable connector action".to_string(),
                objective: "remain durable until a matching Agent definition exists".to_string(),
                acceptance: "startup must stay closed".to_string(),
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
        projection.tasks[&valid_task].claim_generation,
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
    let team = actions
        .apply(&root(
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
        .apply(&root(
            "partial-action-valid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Admissible read".to_string(),
                objective: "admit one deterministic graph".to_string(),
                acceptance: "the graph exists once".to_string(),
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
        .apply(&root(
            "partial-action-invalid-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Unavailable connector action".to_string(),
                objective: "remain durable until a matching Agent definition exists".to_string(),
                acceptance: "the caller observes the deferred dispatch".to_string(),
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
    let invite = root(
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
        projection.tasks[&valid_task].claim_generation,
    );
    let before_retry = services
        .graph_state_store()
        .load(&graph_id)
        .expect("valid Task graph");
    let retry_error = services
        .dispatch_agentic_followups(&invite, context)
        .await
        .expect_err("the unresolved followup remains visible on retry");
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
    let team = actions
        .apply(&root(
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
        .apply(&root(
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
        .apply(&root(
            "orphan-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: team.clone(),
                title: "Orphaned reservation".to_string(),
                objective: "recover it".to_string(),
                acceptance: "new graph admitted".to_string(),
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
    assert_eq!(
        actions
            .apply(&agent(
                "orphan-claim",
                &team,
                &member,
                missing_execution,
                AgentAction::TaskClaim(TaskClaimInput {
                    task_ref: task.clone(),
                    reason: None,
                }),
            ))
            .expect("claim")
            .status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );

    let recovered = services
        .recover_agentic_programs_on_startup()
        .await
        .expect("recovery");
    assert_eq!(recovered.len(), 1);
    assert_ne!(recovered[0].graph_id, missing_execution);
    let projection = actions.project("program-dispatch").expect("projection");
    assert_eq!(projection.tasks[&task].status, AgenticTaskStatus::Published);
    assert_eq!(projection.tasks[&task].claim_execution_id, None);
    assert_eq!(projection.tasks[&task].claimant, None);
}

#[tokio::test]
async fn cross_team_reviewer_resolves_own_identity_and_can_accept() {
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
    let author_team = actions
        .apply(&root(
            "author-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Authors".to_string(),
                mission: "produce".to_string(),
                objective: None,
            }),
        ))
        .expect("author team")
        .changed_refs[0]
        .clone();
    let review_team = actions
        .apply(&root(
            "review-team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Reviewers".to_string(),
                mission: "independently verify".to_string(),
                objective: None,
            }),
        ))
        .expect("review team")
        .changed_refs[0]
        .clone();
    let author = actions
        .apply(&root(
            "author",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: author_team.clone(),
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
    let reviewer = actions
        .apply(&root(
            "reviewer",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: review_team.clone(),
                role: "Reviewer".to_string(),
                mission: "verify another team".to_string(),
                required_capabilities: vec!["read".to_string()],
                existing_agent_ref: None,
                definition_ref: Some("builtin/cowd/execute".into()),
                model_profile_ref: None,
                expertise_hints: Vec::new(),
                execution_requirements: Vec::new(),
            }),
        ))
        .expect("reviewer")
        .changed_refs[0]
        .clone();
    let task = actions
        .apply(&root(
            "cross-review-task",
            AgentAction::TaskPublish(TaskPublishInput {
                team_ref: author_team.clone(),
                title: "Cross-Team result".to_string(),
                objective: "produce durable evidence".to_string(),
                acceptance: "independent review".to_string(),
                required_capabilities: vec!["read".to_string(), "network".to_string()],
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
    let author_execution = "author-execution";
    actions
        .apply(&agent(
            "author-claim",
            &author_team,
            &author,
            author_execution,
            AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task.clone(),
                reason: None,
            }),
        ))
        .expect("claim");
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown".to_string(),
                visibility_scope: "session:session-dispatch".to_string(),
                expected_bytes: None,
                original_name: Some("cross-review.md".to_string()),
            },
            b"independently reviewable result",
        )
        .await
        .expect("content");
    let artifact = actions
        .apply(&agent(
            "author-artifact",
            &author_team,
            &author,
            author_execution,
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content.selector.clone(),
                kind: "report".to_string(),
                title: "Cross-Team report".to_string(),
                relates_to: vec![task.clone()],
            }),
        ))
        .expect("artifact")
        .changed_refs[0]
        .clone();
    let submit = agent(
        "author-submit",
        &author_team,
        &author,
        author_execution,
        AgentAction::TaskSubmit(TaskSubmitInput {
            task_ref: task.clone(),
            artifact_refs: vec![artifact],
            evidence_refs: vec![content.selector.clone()],
            unresolved: Vec::new(),
        }),
    );
    actions.apply(&submit).expect("submit");
    let review = services
        .dispatch_agentic_followups(
            &submit,
            AgenticDispatchContext {
                session_id: "session-dispatch".to_string(),
                turn_id: "turn-dispatch".to_string(),
                model_lease: "test".to_string(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: Vec::new(),
            },
        )
        .await
        .expect("review dispatch");
    assert_eq!(review.len(), 1);
    assert_eq!(review[0].agent_ref, reviewer);
    let graph = services
        .graph_state_store()
        .load(&review[0].graph_id)
        .expect("review graph");
    let packet: AgentTaskPacket =
        serde_json::from_str(&graph.nodes[0].payload_ref).expect("packet");
    let agentic = packet
        .agentic_binding
        .as_ref()
        .expect("typed Agentic execution binding");
    assert_eq!(agentic.team_id, review_team);
    assert_eq!(agentic.task_team_id, author_team);
    assert!(matches!(
        &agentic.focus,
        harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref: bound_task_ref }
            if bound_task_ref == &task
    ));
    let binding = ExecutionParentBinding {
        execution_id: graph.id.clone(),
        node_id: graph.nodes[0].id.clone(),
    };
    let reviewer_actor = services
        .resolve_agent_action_actor(&binding, None)
        .await
        .expect("trusted reviewer actor");
    assert_eq!(
        reviewer_actor.team_id.as_deref(),
        Some(review_team.as_str())
    );
    let accepted = actions
        .apply(&AgentActionEnvelope {
            action_id: "cross-team-accept".to_string(),
            actor: reviewer_actor,
            expected_revision: None,
            action: AgentAction::TaskReview(TaskReviewInput {
                task_ref: task.clone(),
                decision: TaskReviewDecision::Accept,
                reason: "artifact evidence inspected".to_string(),
                evidence_refs: vec![content.selector],
            }),
        })
        .expect("review");
    assert_eq!(
        accepted.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    assert_eq!(
        actions
            .project("program-dispatch")
            .expect("projection")
            .tasks[&task]
            .status,
        AgenticTaskStatus::Accepted
    );
}

// Hold only the named Task's producer/reviewer at the existing backend boundary. The
// real GraphRunner starts/cancels it; it never fabricates a model verdict or
// receipt. The test drives its real Process tool/action protocol below.
struct ReviewWorkerHold {
    task_ref: String,
    entered: tokio::sync::Notify,
    cancelled: std::sync::atomic::AtomicBool,
}
struct ReviewWorkerResolver(Arc<ReviewWorkerHold>);
impl crate::execution_core::graph::executors::AgentTaskBackendResolver for ReviewWorkerResolver {
    fn resolve(
        &self,
        packet: &AgentTaskPacket,
    ) -> Option<Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>> {
        packet.agentic_binding.as_ref().filter(|binding| matches!(&binding.focus,
            harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
            | harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref } if task_ref == &self.0.task_ref))
            .map(|_| self.0.clone() as Arc<dyn crate::execution_core::graph::executors::AgentTaskBackend>)
    }
}
#[async_trait::async_trait]
impl crate::execution_core::graph::executors::AgentTaskBackend for ReviewWorkerHold {
    async fn execute(
        &self,
        _: AgentTaskPacket,
    ) -> Result<harness_contract::agent::AgentReturnPacket, String> {
        self.entered.notify_one();
        std::future::pending().await
    }
    async fn cancel(&self, _: &AgentTaskPacket) -> Result<(), String> {
        Ok(())
    }
    fn cancellation_finalized(&self, _: &AgentTaskPacket) {
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

// Test adapter performs real ArtifactStore reads. Runtime owns authorization,
// tool execution receipts and review decisions; this host never writes a proof.
#[derive(Default)]
struct ReviewReadHost {
    services: std::sync::OnceLock<std::sync::Weak<RuntimeServices>>,
    calls: std::sync::Mutex<Vec<crate::RuntimeToolExecutionRequest>>,
}
impl ReviewReadHost {
    async fn tool_output(&self, name: &str, input: &str, scopes: &[String]) -> String {
        if name == "evidence_retrieve" {
            return self.read(input, scopes).await;
        }
        let services = self.services.get().unwrap().upgrade().unwrap();
        let input: serde_json::Value = serde_json::from_str(input).unwrap();
        let path = input["path"].as_str().unwrap();
        assert!(matches!(path, "review-effect.txt" | "unrelated-review.txt"));
        let path = services.workspace_root().join(path);
        match name {
            "write_file" => {
                std::fs::write(&path, input["content"].as_str().unwrap()).unwrap();
                serde_json::json!({"written":true,"path":path}).to_string()
            }
            "read_file" => std::fs::read_to_string(path).unwrap(),
            _ => panic!("unexpected review tool {name}"),
        }
    }

    async fn read(&self, input: &str, scopes: &[String]) -> String {
        let services = self.services.get().unwrap().upgrade().unwrap();
        let input: serde_json::Value = serde_json::from_str(input).unwrap();
        let selector = input["evidence_ref"].as_str().unwrap();
        let artifact = if selector.starts_with("approval:v1:") {
            services
                .approval_result_content("session-dispatch", selector)
                .await
                .unwrap()
                .0
        } else if let Some(id) = selector.strip_prefix("tool://") {
            let access = services
                .session_evidence_access("session-dispatch", id)
                .await
                .unwrap()
                .unwrap();
            services
                .artifact_store()
                .resolve(&access.retrieval_selector)
                .unwrap()
        } else {
            services.artifact_store().resolve(selector).unwrap()
        };
        assert!(scopes.contains(&artifact.visibility_scope));
        let bytes = services
            .artifact_store()
            .read(&artifact, &artifact.visibility_scope, None)
            .await
            .unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let chunks = text.chars().collect::<Vec<_>>().chunks(1500)
            .enumerate().map(|(index, chunk)| serde_json::json!({"index":index,"content":chunk.iter().collect::<String>()})).collect::<Vec<_>>();
        serde_json::json!({"kind":"evidence_retrieve","available":true,"evidence_ref":selector,
            "sha256":artifact.sha256,"bytes":artifact.bytes,"encoding":"utf8",
            "total_chunks":chunks.len(),"chunks":chunks,"coverage":"sequential_content"})
        .to_string()
    }
}
#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for ReviewReadHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        assert!(request.authorization.is_some());
        self.calls.lock().unwrap().push(request.clone());
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(
                self.tool_output(
                    &request.tool_name,
                    &request.input,
                    &request.authorized_scopes,
                )
                .await,
            ),
            error: None,
            evidence_ref: format!("read:{}", request.tool_use_id),
            observed_evidence: vec![],
        }
    }
    fn delegated_tool_effect_descriptor(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        use harness_contract::{policy::*, tool::*};
        let mut scope = PermissionScope::new(
            PermissionResource::File,
            if name == "write_file" {
                PermissionOperation::Write
            } else {
                PermissionOperation::Read
            },
        );
        scope.target = input["path"].as_str().map(str::to_string);
        Some(ToolEffectDescriptor {
            tool_id: name.into(),
            descriptor_hash: format!("review-read:{name}"),
            effect_kind: if name == "write_file" {
                ToolEffectKind::Write
            } else {
                ToolEffectKind::Read
            },
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope],
            required_permission: if name == "write_file" {
                ToolPermissionMode::WorkspaceWrite
            } else {
                ToolPermissionMode::ReadOnly
            },
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: if name == "write_file" {
                EffectAssessment {
                    reversibility: EffectReversibility::Compensatable,
                    externality: EffectExternality::Workspace,
                    data_sensitivity: DataClassification::Internal,
                    novelty: EffectNovelty::Routine,
                    blast_radius: EffectBlastRadius::Workspace,
                }
            } else {
                EffectAssessment::default()
            },
        })
    }
}
struct RootReviewReader(Arc<ReviewReadHost>);
#[async_trait::async_trait]
impl crate::ToolExecutor for RootReviewReader {
    async fn execute_authorized_output(
        &self,
        authorization: &harness_contract::tool::ToolExecutionAuthorization,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        assert_eq!(authorization.tool_id, name);
        self.execute_output(name, input).await
    }
    async fn execute_output(
        &self,
        name: &str,
        input: &str,
    ) -> Result<harness_contract::context::ToolOutputDraft, crate::ToolError> {
        Ok(harness_contract::context::ToolOutputDraft::bounded_inline(
            self.0
                .tool_output(name, input, &["session:session-dispatch".into()])
                .await,
        ))
    }
    fn registered_tool_effect(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        crate::RuntimeExecutionHost::delegated_tool_effect_descriptor(self.0.as_ref(), name, input)
    }
}
struct NoReviewProvider;
impl crate::ApiClient for NoReviewProvider {
    fn stream(
        &mut self,
        _: crate::ApiRequest,
    ) -> std::pin::Pin<
        Box<
            dyn futures::Stream<Item = Result<crate::AssistantEvent, crate::RuntimeError>>
                + Send
                + '_,
        >,
    > {
        panic!("physical review gate must not call a paid Provider")
    }
}
async fn write_root_review_effect(
    runtime: &crate::ConversationRuntime<NoReviewProvider, RootReviewReader>,
    id: &str,
    content: &str,
) {
    root_review_target_tool(runtime, id, "write_file", "review-effect.txt", content).await;
}

async fn root_review_target_tool(
    runtime: &crate::ConversationRuntime<NoReviewProvider, RootReviewReader>,
    id: &str,
    tool_name: &str,
    target: &str,
    content: &str,
) {
    let binding = serde_json::from_value(serde_json::json!({
        "root_execution_id":"root-agentic-execution", "session_id":"session-dispatch", "turn_id":"turn-dispatch",
        "root_task_id":"task-root-dispatch", "task_id":"task-root-dispatch", "activity_id":"root-agentic-execution:model",
        "revision":1,"fence":1,"generation":1
    })).unwrap();
    let _scope = runtime.cowd_bus().unwrap().enter_execution_with_activity(
        crate::CowdExecutionContext {
            execution_id: "root-agentic-execution".into(),
            session_id: "session-dispatch".into(),
            turn_id: "turn-dispatch".into(),
        },
        Some(binding),
    );
    let written = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                id: id.into(),
                name: tool_name.into(),
                input: serde_json::json!({"path":target,"content":content}).to_string(),
                depends_on: vec![],
            }],
            &crate::SharedPrompter::none(),
            4,
        )
        .await
        .unwrap();
    assert_eq!(written.failed, 0, "{:?}", written.messages);
}

async fn root_reads_review_result(
    services: &Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
    selector: &str,
) {
    root_reads_generic_results(services, host, selector, false, false).await;
}

async fn root_reads_generic_results(
    services: &Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
    selector: &str,
    read_tool_receipt: bool,
    write_effect: bool,
) -> (
    Option<String>,
    crate::ConversationRuntime<NoReviewProvider, RootReviewReader>,
) {
    if write_effect {
        let commits = services.commit_service();
        let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(services.event_store()));
        let mut graph = if services
            .event_store()
            .stream_revision("root-agentic-execution")
            .unwrap()
            > 0
        {
            graphs.load("root-agentic-execution").unwrap()
        } else {
            let mut graph = ExecutionGraph::new("Root effect verification");
            graph.id = "root-agentic-execution".into();
            graph.lineage = Some(ExecutionGraphLineage {
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                root_task_id: "task-root-dispatch".into(),
                task_id: "task-root-dispatch".into(),
                generation: 1,
            });
            graph.nodes.push(ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "payload:root",
            ));
            commits.register_graph(graph).unwrap().graph
        };
        let node = graph.nodes[0].id.clone();
        for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
            graph = commits
                .transition_node(&graph, &node, status, None, vec![])
                .unwrap()
                .graph;
        }
    }
    let session_store = Arc::new(crate::test_support::session_store());
    let mut session = crate::session::Session::new();
    session.session_id = "session-dispatch".into();
    session_store
        .create_session(&session::SessionRecord {
            session_id: session.session_id.clone(),
            platform: "test".into(),
            chat_id: "review".into(),
            user_id: None,
            model: None,
            created_at: "2026-09-09T00:00:00Z".into(),
            last_activity: "2026-09-09T00:00:00Z".into(),
            message_count: 0,
            reset_policy: "manual".into(),
            metadata_json: None,
            input_tokens: 0,
            output_tokens: 0,
            status: "active".into(),
        })
        .await
        .unwrap();
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(session_store);
    services
        .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports.clone())
        .unwrap();
    let bus = crate::CowdEventBus::new();
    let binding = serde_json::from_value(serde_json::json!({
        "root_execution_id":"root-agentic-execution", "session_id":"session-dispatch", "turn_id":"turn-dispatch",
        "root_task_id":"task-root-dispatch", "task_id":"task-root-dispatch", "activity_id":"root-agentic-execution:model",
        "revision":1,"fence":1,"generation":1
    })).unwrap();
    let _scope = bus.enter_execution_with_activity(
        crate::CowdExecutionContext {
            execution_id: "root-agentic-execution".into(),
            session_id: "session-dispatch".into(),
            turn_id: "turn-dispatch".into(),
        },
        Some(binding),
    );
    let runtime = crate::ConversationRuntime::new(
        session,
        NoReviewProvider,
        RootReviewReader(host),
        crate::PermissionPolicy::new(if write_effect {
            PermissionMode::DangerFullAccess
        } else {
            PermissionMode::ReadOnly
        }),
        vec!["Read current result".into()],
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(services.event_store()))
    .with_session_journal_port(ports)
    .with_artifact_store(Arc::clone(services.artifact_store()))
    .with_cowd_event_bus(bus);
    runtime
        .begin_turn_strategy("turn-dispatch", "retrieve current result evidence")
        .unwrap();
    runtime
        .bind_turn_strategy_execution("turn-dispatch", "root-agentic-execution")
        .unwrap();
    let result = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                id: "root-read-result".into(),
                name: "evidence_retrieve".into(),
                input: serde_json::json!({"evidence_ref":selector}).to_string(),
                depends_on: vec![],
            }],
            &crate::SharedPrompter::none(),
            1,
        )
        .await
        .unwrap();
    assert_eq!(result.failed, 0, "{:?}", result.messages);
    if !read_tool_receipt {
        return (None, runtime);
    }
    if write_effect {
        let written = runtime.execute_tool_batch_step(&[crate::conversation::ModelToolCall {
            id: "root-write-effect".into(), name: "write_file".into(),
            input: serde_json::json!({"path":"review-effect.txt","content":"actually persisted revised content"}).to_string(),
            depends_on: vec![],
        }], &crate::SharedPrompter::none(), 2).await.unwrap();
        assert_eq!(written.failed, 0, "{:?}", written.messages);
    }
    let events = services
        .event_store()
        .events_for_root_execution_kind(
            "root-agentic-execution",
            "tool.invocation.completed",
            None,
            64,
        )
        .unwrap();
    let reference = events
        .iter()
        .find(|event| {
            event.payload["tool_call_id"]
                == if write_effect {
                    "root-write-effect"
                } else {
                    "root-read-result"
                }
        })
        .unwrap()
        .payload["full_output_ref"]
        .as_str()
        .unwrap()
        .to_string();
    let second = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                id: "root-read-tool-receipt".into(),
                name: "evidence_retrieve".into(),
                input: serde_json::json!({"evidence_ref":reference}).to_string(),
                depends_on: vec![],
            }],
            &crate::SharedPrompter::none(),
            2,
        )
        .await
        .unwrap();
    assert_eq!(second.failed, 0, "{:?}", second.messages);
    (Some(reference), runtime)
}

#[tokio::test]
async fn low_risk_root_reviews_artifact_and_tool_content_without_creating_work_orders() {
    exercise_low_risk_root_review(false, false).await;
}

#[tokio::test]
async fn low_risk_root_verifies_local_writes_without_an_extra_model_or_work_order() {
    exercise_low_risk_root_review(true, false).await;
}

#[tokio::test]
async fn external_decision_is_read_and_reviewed_before_the_original_goal_can_complete() {
    exercise_low_risk_root_review(false, true).await;
}

async fn exercise_low_risk_root_review(local_effect: bool, external_decision: bool) {
    use harness_contract::agent_action::{
        AgentActionStatus, ObjectiveCompleteRequestInput, ObjectiveReviewDecision,
        ObjectiveReviewInput,
    };
    use harness_contract::goal::{GoalCompletion, GoalContract};
    let host = Arc::new(ReviewReadHost::default());
    let services = RuntimeServices::in_memory_with_tool_host(host.clone()).unwrap();
    host.services.set(Arc::downgrade(&services)).unwrap();
    services
        .goal_store()
        .create(GoalContract {
            id: "goal:root-agentic-execution".into(),
            session_id: "session-dispatch".into(),
            objective: "dispatch objective".into(),
            criteria: vec![harness_contract::goal::AcceptanceCriterion {
                id: "reviewed-delivery".into(),
                statement: "Deliver checked source-backed work".into(),
                statement_ref: None,
                source_refs: Vec::new(),
                required_evidence: vec!["execution_graph:root-agentic-execution".into()],
                status: harness_contract::goal::AcceptanceStatus::Open,
                waiver: None,
            }],
            constraints: vec![],
            phase: "execution".into(),
            evidence_refs: vec![],
            unresolved: vec![],
            blockers: vec![],
            scope: harness_contract::goal::GoalScope::UserObjective,
            user_intent_criterion_id: Some("reviewed-delivery".into()),
            source_intent_ref: Some("session_message:dispatch".into()),
            execution_binding: Some(harness_contract::goal::GoalExecutionBinding {
                objective_id: "objective-dispatch".into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                root_execution_id: "root-agentic-execution".into(),
                agentic_program_id: "program-dispatch".into(),
            }),
            spec_revision: 1,
            spec_digest: "dispatch-test".into(),
            review_refs: vec![],
            waiting: None,
            participation_requirement: None,
            obligations: vec![],
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        })
        .unwrap();
    let bind = |id: &str, action: AgentAction| {
        let mut envelope = root(id, action);
        envelope.actor.root_execution_id = Some("root-agentic-execution".into());
        envelope.actor.required_team_count = 0;
        envelope
    };
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-dispatch".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"The current result was read and checked.",
        )
        .await
        .unwrap();
    let artifact = services
        .submit_agent_action(&bind(
            "root-content",
            AgentAction::ArtifactCommit(ArtifactCommitInput {
                content_ref: content.selector.clone(),
                kind: "answer".into(),
                title: "Direct answer".into(),
                relates_to: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(artifact.status, AgentActionStatus::Applied, "{artifact:?}");
    let review = |id: &str, refs: Vec<String>| {
        bind(
            id,
            AgentAction::ObjectiveReview(ObjectiveReviewInput {
                criterion_ref: "reviewed-delivery".into(),
                decision: ObjectiveReviewDecision::Satisfied,
                result_refs: refs,
                evidence_refs: vec![content.selector.clone()],
                reason_ref: content.selector.clone(),
            }),
        )
    };
    assert!(services
        .submit_agent_action(&review(
            "missing-read",
            vec![artifact.changed_refs[0].clone()]
        ))
        .await
        .is_err());
    let (raw, runtime) =
        root_reads_generic_results(&services, host, &content.selector, true, local_effect).await;
    let raw = raw.unwrap();
    let result_artifact = if local_effect {
        let publication = services
            .submit_agent_action(&bind(
                "root-summary-after-write",
                AgentAction::ArtifactCommit(ArtifactCommitInput {
                    content_ref: content.selector.clone(),
                    kind: "answer".into(),
                    title: "Explanation after local write".into(),
                    relates_to: vec![],
                }),
            ))
            .await
            .unwrap();
        assert_eq!(publication.status, AgentActionStatus::Applied);
        publication.changed_refs[0].clone()
    } else {
        artifact.changed_refs[0].clone()
    };
    let mut references = vec![result_artifact, content.selector.clone(), raw];
    if external_decision {
        use harness_contract::policy::*;
        let mut graph = ExecutionGraph::new("external decision source");
        graph.id = "root-agentic-execution".into();
        graph.lineage = Some(ExecutionGraphLineage {
            session_id: "session-dispatch".into(),
            turn_id: "turn-dispatch".into(),
            root_task_id: "task-root-dispatch".into(),
            task_id: "task-root-dispatch".into(),
            generation: 1,
        });
        let node =
            ExecutionNodeSpec::new(ExecutionNodeKind::Approval, "approval", "decision source");
        let reference =
            crate::execution_core::graph::executors::graph_approval_id(&graph.id, &node.id);
        graph.nodes.push(node);
        services.commit_service().register_graph(graph).unwrap();
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: Some("session-dispatch".into()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        services
            .approval_queue()
            .submit_scoped(
                &reference,
                SubmitApprovalRequest {
                    context: ApprovalContext::owned(
                        &source,
                        "operator decision",
                        "root-agentic-execution",
                    ),
                    source,
                    action: "operator decision".into(),
                    summary: "Operator decides whether the proposed action may run".into(),
                    risk: harness_contract::core::TaskRisk::Medium,
                    domain: ApprovalDomain::Execution,
                    blocks_execution: true,
                    evidence_refs: vec![],
                    timeout_policy: ApprovalTimeoutPolicy::AutoDeny,
                },
            )
            .unwrap();
        assert!(services
            .approval_result_content("session-dispatch", &reference)
            .await
            .is_err());
        services
            .approval_queue()
            .decide_surface_human(
                "operator:test",
                ApprovalDecisionCommand {
                    approval_id: reference.clone(),
                    approved: false,
                    skip: false,
                    reason: "Operator declined the requested action".into(),
                    scope: ApprovalGrantScope::Once,
                    actor: ApprovalDecisionActor {
                        kind: ApprovalDecisionActorKind::Human,
                        actor_id: "replaced-by-authenticated-surface".into(),
                    },
                    evidence_refs: vec![],
                },
            )
            .unwrap();
        assert!(services
            .approval_result_content("foreign-session", &reference)
            .await
            .is_err());
        let (_, decided, _) = services
            .approval_result_content("session-dispatch", &reference)
            .await
            .unwrap();
        assert!(
            !decided.decision.unwrap().approved,
            "denial is an authentic result, not approval to execute"
        );
        references.push(reference.clone());
        assert!(services
            .submit_agent_action(&review("external-unread", references.clone()))
            .await
            .is_err());
        let activity = serde_json::from_value(serde_json::json!({
            "root_execution_id":"root-agentic-execution", "session_id":"session-dispatch", "turn_id":"turn-dispatch",
            "root_task_id":"task-root-dispatch", "task_id":"task-root-dispatch", "activity_id":"root-agentic-execution:model",
            "revision":1, "fence":1, "generation":1
        })).unwrap();
        let _scope = runtime.cowd_bus().unwrap().enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "root-agentic-execution".into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
            },
            Some(activity),
        );
        let read = runtime
            .execute_tool_batch_step(
                &[crate::conversation::ModelToolCall {
                    id: "read-current-external-decision".into(),
                    name: "evidence_retrieve".into(),
                    input: serde_json::json!({"evidence_ref":reference}).to_string(),
                    depends_on: vec![],
                }],
                &crate::SharedPrompter::none(),
                5,
            )
            .await
            .unwrap();
        assert_eq!(read.failed, 0, "{:?}", read.messages);
    }
    if local_effect {
        assert!(services
            .submit_agent_action(&review("receipt-only", references.clone()))
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        std::fs::write(
            services.workspace_root().join("unrelated-review.txt"),
            "unrelated",
        )
        .unwrap();
        root_review_target_tool(
            &runtime,
            "root-unrelated-observation",
            "read_file",
            "unrelated-review.txt",
            "",
        )
        .await;
        assert!(services
            .submit_agent_action(&review("wrong-target", references.clone()))
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        root_review_target_tool(
            &runtime,
            "root-current-local-target",
            "read_file",
            "review-effect.txt",
            "",
        )
        .await;
    }
    let reviewed = services
        .submit_agent_action(&review("root-review", references.clone()))
        .await
        .unwrap();
    assert_eq!(reviewed.status, AgentActionStatus::Applied, "{reviewed:?}");
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let proof = goal.reviews.last().unwrap().verification.as_ref().unwrap();
    assert!(!proof.independence_required);
    assert!(proof.review_policy_digest.is_some());
    assert_eq!(proof.reads.len(), if external_decision { 4 } else { 3 });
    if external_decision {
        assert_eq!(proof.result_source_revisions.len(), 1);
        assert!(proof.reads.iter().any(
            |read| read.result_kind == harness_contract::goal::GoalResultKind::ExternalDecision
        ));
        assert!(proof
            .producer_execution_ids
            .iter()
            .any(|id| id.starts_with("approval-decision:")));
    }
    if local_effect {
        assert!(
            proof.reads.iter().all(|read| read.result_kind
                == harness_contract::goal::GoalResultKind::ToolEffect
                && !read.effect_observation_refs.is_empty()),
            "Root explanation and content aliases retain the Root's actual effects"
        );
        let prior_digest = proof.effect_manifest_digest.clone();
        write_root_review_effect(
            &runtime,
            "root-local-write-again",
            "actually changed after review",
        )
        .await;
        let request = services
            .submit_agent_action(&bind(
                "stale-self-review-completion",
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    result_refs: references.clone(),
                    evidence_refs: vec![content.selector.clone()],
                    unresolved: vec![],
                }),
            ))
            .await
            .unwrap();
        assert_eq!(request.status, AgentActionStatus::Applied);
        let supervisor = crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(
            services.goal_store(),
        ));
        crate::agentic::supervision::reconcile_completion_request(
            &services.agent_action_service(),
            &supervisor,
            "program-dispatch",
        )
        .unwrap();
        assert_eq!(
            services
                .goal_store()
                .get("goal:root-agentic-execution")
                .unwrap()
                .unwrap()
                .completion,
            GoalCompletion::Open
        );
        assert_eq!(
            services
                .agent_action_service()
                .project("program-dispatch")
                .unwrap()
                .status,
            crate::AgenticProgramStatus::Open
        );
        assert!(services
            .submit_agent_action(&review("stale-local-observation", references.clone()))
            .await
            .unwrap_err()
            .contains("review_requires_effect_observation"));
        root_review_target_tool(
            &runtime,
            "root-current-local-target-again",
            "read_file",
            "review-effect.txt",
            "",
        )
        .await;
        assert_eq!(
            services
                .submit_agent_action(&review("fresh-local-review", references.clone()))
                .await
                .unwrap()
                .status,
            AgentActionStatus::Applied
        );
        let current = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_ne!(
            current
                .reviews
                .last()
                .unwrap()
                .verification
                .as_ref()
                .unwrap()
                .effect_manifest_digest,
            prior_digest
        );
    } else {
        assert!(proof.reads.iter().any(|read| matches!(
            read.result_kind,
            harness_contract::goal::GoalResultKind::StructuredData
                | harness_contract::goal::GoalResultKind::Content
        )));
    }
    let orphan = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:session-dispatch".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"Unpublished orphan without a Tool or Program producer",
        )
        .await
        .unwrap();
    assert!(services
        .submit_agent_action(&review("unpublished-result", vec![orphan.selector]))
        .await
        .unwrap_err()
        .contains("Runtime-resolved producer"));
    let foreign = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/plain".into(),
                visibility_scope: "session:another-session".into(),
                expected_bytes: None,
                original_name: None,
            },
            b"Another session's private result",
        )
        .await
        .unwrap();
    assert!(services
        .submit_agent_action(&review("foreign-result", vec![foreign.selector]))
        .await
        .unwrap_err()
        .contains("not readable"));
    let requested = services
        .submit_agent_action(&bind(
            "root-complete",
            AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                result_refs: references,
                evidence_refs: vec![content.selector],
                unresolved: vec![],
            }),
        ))
        .await
        .unwrap();
    assert_eq!(
        requested.status,
        AgentActionStatus::Applied,
        "{requested:?}"
    );
    let before = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    let prepared = services
        .goal_store()
        .prepare_program_conclusion(&before)
        .unwrap();
    assert!(prepared.gaps.is_empty(), "{:?}", prepared.gaps);
    assert_eq!(
        prepared.policy_sources.len(),
        if external_decision { 2 } else { 1 }
    );
    // Negative concurrency injection only: no effect or positive read is invented.
    services
        .event_store()
        .append(crate::RuntimeEventInput {
            stream_id: "session:session-dispatch".into(),
            scope: crate::RuntimeEventScope::Session,
            kind: "test.concurrent_policy_source_change".into(),
            status: None,
            actor: Some("test.concurrent_writer".into()),
            refs: vec![],
            payload: serde_json::json!({}),
        })
        .unwrap();
    assert!(services
        .agent_action_service()
        .commit_program_conclusion(&before, prepared)
        .is_err());
    assert_eq!(
        services
            .agent_action_service()
            .project("program-dispatch")
            .unwrap()
            .revision,
        before.revision
    );
    assert!(services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap()
        .terminal
        .is_none());
    let supervisor =
        crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(services.goal_store()));
    crate::agentic::supervision::reconcile_completion_request(
        &services.agent_action_service(),
        &supervisor,
        "program-dispatch",
    )
    .unwrap();
    let final_program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert!(
        final_program.tasks.is_empty()
            && final_program.teams.is_empty()
            && final_program.agents.is_empty()
    );
    assert_eq!(final_program.status, crate::AgenticProgramStatus::Verified);
    if local_effect {
        let target = services.workspace_root().join("review-effect.txt");
        let before = std::fs::read(&target).unwrap();
        let _scope = runtime.cowd_bus().unwrap().enter_execution_with_activity(
            crate::CowdExecutionContext {
                execution_id: "root-agentic-execution".into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
            },
            None,
        );
        let late = runtime.execute_tool_batch_step(&[crate::conversation::ModelToolCall {
            id: "late-root-write-after-verification".into(),
            name: "write_file".into(),
            input: serde_json::json!({"path":"review-effect.txt","content":"forbidden late mutation"}).to_string(),
            depends_on: vec![],
        }], &crate::SharedPrompter::none(), 99).await.unwrap();
        assert_eq!(late.failed, 1, "{:?}", late.messages);
        assert_eq!(std::fs::read(target).unwrap(), before);
        assert_eq!(
            services
                .agent_action_service()
                .project("program-dispatch")
                .unwrap()
                .status,
            crate::AgenticProgramStatus::Verified
        );
    }
    assert_eq!(
        services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap()
            .completion,
        GoalCompletion::Satisfied
    );
    // Negative corruption fixtures only; the successful path above used the
    // actual ConversationRuntime strategy writer without injected evidence.
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let mut independent_goal = goal.clone();
    independent_goal
        .obligations
        .push(harness_contract::goal::ObjectiveObligation {
            obligation_id: "explicit-independent-verification".into(),
            required: true,
            success_predicate: "The original requirement needs another verifier".into(),
            producer: Default::default(),
            evidence_requirement: harness_contract::goal::ObjectiveEvidenceRequirement {
                required_artifact_kinds: vec![],
                independent_verifier_required: true,
                reread_required: true,
            },
            state: harness_contract::goal::ObjectiveObligationState::Open,
            artifact_refs: vec![],
            evidence_refs: vec![],
            reread_receipts: vec![],
            verifier_decision: None,
            diagnostic_code: None,
        });
    assert!(crate::agentic::review_evidence::root_self_review_policy(
        services.event_store(),
        &final_program,
        &independent_goal
    )
    .unwrap()
    .is_none());
    let strategy = services
        .event_store()
        .list_stream("session:session-dispatch")
        .unwrap()
        .into_iter()
        .rev()
        .find(|event| {
            event.kind.starts_with("runtime.strategy.")
                && event.payload["execution_graph_ref"] == "root-agentic-execution"
        })
        .unwrap();
    for (label, field, value) in [
        (
            "higher-risk",
            "risk",
            serde_json::to_value(harness_contract::core::TaskRisk::High).unwrap(),
        ),
        ("legacy-missing-risk", "risk", serde_json::Value::Null),
        (
            "verifier-required",
            "modifiers",
            serde_json::to_value(vec![
                harness_contract::core::ExecutionModifier::WithVerifier,
            ])
            .unwrap(),
        ),
        (
            "reviewer-required",
            "modifiers",
            serde_json::to_value(vec![
                harness_contract::core::ExecutionModifier::WithReviewer,
            ])
            .unwrap(),
        ),
        (
            "approval-required",
            "gates",
            serde_json::to_value(vec![harness_contract::core::ExecutionPolicyGate::Approval])
                .unwrap(),
        ),
        ("legacy-missing-gates", "gates", serde_json::Value::Null),
    ] {
        let mut payload = strategy.payload.clone();
        payload[field] = value;
        payload["test_fault"] = label.into();
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: strategy.stream_id.clone(),
                scope: strategy.scope,
                kind: "runtime.strategy.test_fault_injection".into(),
                status: strategy.status.clone(),
                actor: strategy.actor.clone(),
                refs: strategy.refs.clone(),
                payload,
            })
            .unwrap();
        assert!(
            crate::agentic::review_evidence::root_self_review_policy(
                services.event_store(),
                &final_program,
                &goal
            )
            .unwrap()
            .is_none(),
            "{label}"
        );
    }
}
