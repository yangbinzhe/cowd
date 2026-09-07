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
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
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
                required_capabilities: vec!["web-research".to_string()],
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
            required_capabilities: vec!["python".to_string(), "verification".to_string()],
            depends_on: Vec::new(),

            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }),
    ));
    let task_ref = action_service.apply(&task).expect("task").changed_refs[0].clone();
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
    let heartbeat = start_agentic_claim_heartbeat(Arc::downgrade(services.as_ref()), &packet)
        .expect("start worker-owned claim heartbeat")
        .expect("execute packet owns a heartbeat guard");
    tokio::task::yield_now().await;
    let binding = packet.binding.expect("binding");
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
            b"Source-backed findings for independent review",
        )
        .await
        .expect("persist content");
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
            unresolved: vec![],
        }),
    };
    assert_eq!(
        action_service.apply(&submit).unwrap().status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let review_dispatch = services
        .dispatch_agentic_followups(
            &submit,
            AgenticDispatchContext {
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                model_lease: "test".into(),
                permission_ceiling: PermissionMode::ReadOnly,
                resource_scopes: vec![],
            },
        )
        .await
        .expect("physical reviewer admission");
    assert_eq!(review_dispatch.len(), 1);
    assert_eq!(review_dispatch[0].agent_ref, reviewer);
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
    let reviewed = action_service
        .apply(&AgentActionEnvelope {
            action_id: "experience-review".into(),
            actor: reviewer_actor,
            expected_revision: None,
            action: AgentAction::TaskReview(TaskReviewInput {
                task_ref: task_ref.clone(),
                decision: TaskReviewDecision::Accept,
                reason: "Read and checked the persisted artifact".into(),
                evidence_refs: vec![content.selector.clone()],
            }),
        })
        .unwrap();
    assert_eq!(
        reviewed.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{reviewed:?}"
    );
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
            obligations: vec![],
            recovery: None,
            terminal: None,
            completion: GoalCompletion::Open,
            revision: 1,
            user_sequence: 1,
            reviews: Vec::new(),
        })
        .expect("Objective creation");
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
    assert!(DispatchFlight::acquire(left.agentic_dispatch_flights(), key.clone()).is_none());
    assert!(
        DispatchFlight::acquire(right.agentic_dispatch_flights(), key).is_some(),
        "one Runtime instance must never suppress another instance's reconciler"
    );
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

    let first = services
        .recover_agentic_programs_on_startup()
        .await
        .expect("first recovery");
    assert_eq!(first.len(), 1);
    let graph = services
        .graph_state_store()
        .load(&first[0].graph_id)
        .expect("admitted Agent-first graph");
    let packet = serde_json::from_str::<AgentTaskPacket>(&graph.nodes[0].payload_ref)
        .expect("compiled Agent-first packet");
    assert!(
        packet.acceptance.is_empty() && packet.output_acceptance.is_empty(),
        "natural-language Program Task acceptance must stay in the Program review contract, not become an unsatisfiable physical Agent terminal field"
    );
    assert!(graph.nodes[0].acceptance.criteria.is_empty());
    let second = services
        .recover_agentic_programs_on_startup()
        .await
        .expect("idempotent recovery");
    assert!(
        second.is_empty(),
        "active graph must suppress duplicate paid work"
    );

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
            &first[0].task_ref,
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
            .dispatch_agentic_task(&projection, &first[0].task_ref, mode, &context, &trigger,)
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
    assert_eq!(after_retry.revision, before_retry.revision);
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
