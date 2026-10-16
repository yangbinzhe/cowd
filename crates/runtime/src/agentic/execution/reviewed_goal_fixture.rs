use super::*;
use harness_contract::agent_action::{
    AgentActorBinding, AgentActorKind, AgentInviteInput, ArtifactCommitInput, TaskClaimInput,
    TaskOneShotDeliverable, TaskPublishInput, TaskReviewDecision, TaskReviewInput, TaskSubmitInput,
    TeamCreateInput,
};

pub(super) fn root(action_id: &str, action: AgentAction) -> AgentActionEnvelope {
    AgentActionEnvelope {
        action_id: action_id.to_string(),
        actor: AgentActorBinding {
            objective_id: "objective-dispatch".to_string(),
            program_id: "program-dispatch".to_string(),
            session_id: "session-dispatch".to_string(),
            turn_id: "turn-dispatch".to_string(),
            root_execution_id: None,
            // No user topology constraint in this fixture; the ceiling must
            // therefore not restrict the teams these tests build.
            required_team_count: 0,
            exact_team_count: None,
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

pub(super) fn agent(
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

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum DelegatedEffectCase {
    None,
    Review,
    PendingReview,
    Withdraw,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PhysicalDrainTiming {
    BeforeGoal,
    AfterGoal,
}

struct ReviewedFixtureServices(Arc<RuntimeServices>);

impl std::ops::Deref for ReviewedFixtureServices {
    type Target = Arc<RuntimeServices>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReviewedFixtureServices {
    fn submit_agent_action<'fixture>(
        &'fixture self,
        envelope: &'fixture AgentActionEnvelope,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<
                    Output = Result<harness_contract::agent_action::AgentActionObservation, String>,
                > + Send
                + 'fixture,
        >,
    > {
        Box::pin(self.0.submit_agent_action(envelope))
    }

    fn dispatch_agentic_followups<'fixture>(
        &'fixture self,
        envelope: &'fixture AgentActionEnvelope,
        context: AgenticDispatchContext,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<Vec<AgenticDispatchReceipt>, String>>
                + Send
                + 'fixture,
        >,
    > {
        Box::pin(self.0.dispatch_agentic_followups(envelope, context))
    }
}

pub(super) async fn exercise_reviewed_goal_delivery_on(
    services: Arc<RuntimeServices>,
    read_host: Arc<ReviewReadHost>,
    with_disclosure: bool,
    with_effect: bool,
    delegated_case: DelegatedEffectCase,
    direct_text: bool,
) {
    Box::pin(exercise_reviewed_goal_delivery_with_drain(
        services,
        read_host,
        with_disclosure,
        with_effect,
        delegated_case,
        direct_text,
        PhysicalDrainTiming::BeforeGoal,
    ))
    .await;
}

pub(super) async fn exercise_reviewed_goal_delivery_with_drain(
    services: Arc<RuntimeServices>,
    read_host: Arc<ReviewReadHost>,
    with_disclosure: bool,
    with_effect: bool,
    delegated_case: DelegatedEffectCase,
    direct_text: bool,
    drain_timing: PhysicalDrainTiming,
) {
    let services = ReviewedFixtureServices(services);
    let delegated_effect = delegated_case != DelegatedEffectCase::None;
    let effect_host = Arc::clone(&read_host);
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
    let team_receipt = services
        .submit_agent_action(&bind_root(root(
            "team",
            AgentAction::TeamCreate(TeamCreateInput {
                name: "Research Team".to_string(),
                mission: "investigate".to_string(),
                objective: None,
            }),
        )))
        .await
        .expect("team");
    let team_ref = team_receipt.changed_refs[0].clone();
    let agent_ref = services
        .submit_agent_action(&bind_root(root(
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
        .await
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
            criterion_source: Default::default(),
            acceptance_checks: Vec::new(),
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
    let task_ref = services
        .submit_agent_action(&task)
        .await
        .expect("task")
        .changed_refs[0]
        .clone();
    // This fixture drives the tool/action protocol while its actual worker
    // remains active. A backend that already returned is no longer authority
    // for synthetic late submissions after common lifetime settlement.
    let producer_hold = Some({
        Arc::new(ReviewWorkerHold {
            task_ref: task_ref.clone(),
            entered: tokio::sync::Notify::new(),
            cancelled: std::sync::atomic::AtomicBool::new(false),
            finalized: Default::default(),
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
    assert!(packet.objective.contains("state_inspect"));
    assert!(packet.objective.contains("task_claim"));
    assert!(packet
        .objective
        .contains("执行和提交前须 task_claim 领取该确切 Task"));
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

    services
        .submit_agent_action(&AgentActionEnvelope {
            action_id: "model-agent-claim".to_string(),
            actor: actor.clone(),
            expected_revision: None,
            action: AgentAction::TaskClaim(TaskClaimInput {
                task_ref: task_ref.clone(),
                reason: Some("I inspected the task and accept it".to_string()),
            }),
        })
        .await
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
            criterion_source: Default::default(),
            acceptance_checks: Vec::new(),
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

    let mut pending_effect = None;
    let writer = if delegated_effect {
        let writer = Arc::new(
            crate::agent_in_process_worker::ProcessJsonlToolSession::prepare(
                &services,
                &packet,
                &crate::agent_model_selector::AgentModelSelection {
                    model: "test".into(),
                    provider: "test".into(),
                    registry_revision: 0,
                },
            )
            .unwrap(),
        );
        writer
            .execute_tool(
                "delegated-write",
                "edit",
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
                    .find(|call| call.tool_name == "edit")
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
                "edit",
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
        if delegated_case == DelegatedEffectCase::PendingReview {
            let gate = Arc::new(ReviewWriteHold::default());
            read_host.held_write.set(Arc::clone(&gate)).unwrap();
            let held_writer = Arc::clone(&writer);
            let running = tokio::spawn(async move {
                held_writer
                    .execute_tool(
                        "pending-required-write",
                        "edit",
                        r#"{"path":"review-effect.txt","content":"actual delegated write"}"#,
                    )
                    .await
            });
            tokio::time::timeout(std::time::Duration::from_secs(10), gate.entered.notified())
                .await
                .unwrap();
            pending_effect = Some((gate, running));
        }
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
                "edit",
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
    let artifact = if direct_text {
        let mut wrong_claim = actor.clone();
        wrong_claim.execution_id = Some("unbound-producer-cannot-submit".into());
        let refused = services
            .submit_agent_action(&AgentActionEnvelope {
                action_id: "wrong-claim-text-submit".into(),
                actor: wrong_claim,
                expected_revision: None,
                action: AgentAction::TaskSubmit(TaskSubmitInput {
                    task_ref: task_ref.clone(),
                    artifact_refs: vec![],
                    evidence_refs: vec![],
                    unresolved: vec![],
                    deliverable: Some(TaskOneShotDeliverable {
                        block_index: 0,
                        kind: "report".into(),
                        title: "Not this claimant".into(),
                        resolved_content_ref: Some(content.selector.clone()),
                    }),
                }),
            })
            .await
            .unwrap();
        assert_eq!(
            refused.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert!(action_service
            .project("program-dispatch")
            .unwrap()
            .objective_verdict
            .is_none());
        let submitted = services
            .submit_agent_action(&AgentActionEnvelope {
                action_id: "experience-text-submit".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::TaskSubmit(TaskSubmitInput {
                    task_ref: task_ref.clone(),
                    artifact_refs: vec![],
                    evidence_refs: vec![],
                    unresolved: vec![],
                    deliverable: Some(TaskOneShotDeliverable {
                        block_index: 0,
                        kind: "report".into(),
                        title: "Plain text findings".into(),
                        resolved_content_ref: Some(content.selector.clone()),
                    }),
                }),
            })
            .await
            .unwrap();
        assert_eq!(
            submitted.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{submitted:?}"
        );
        let program = action_service.project("program-dispatch").unwrap();
        assert_eq!(
            program.tasks[&task_ref].status,
            crate::AgenticTaskStatus::Submitted
        );
        assert!(program.objective_verdict.is_none());
        program.tasks[&task_ref].artifact_refs[0].clone()
    } else {
        services
            .submit_agent_action(&AgentActionEnvelope {
                action_id: "experience-artifact".into(),
                actor: actor.clone(),
                expected_revision: None,
                action: AgentAction::ArtifactCommit(ArtifactCommitInput {
                    content_ref: content.selector.clone(),
                    kind: "report".into(),
                    title: "Findings".into(),
                    relates_to: vec![],
                }),
            })
            .await
            .unwrap()
            .changed_refs[0]
            .clone()
    };
    let reviewer = services
        .submit_agent_action(&bind_root(root(
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
        .await
        .expect("invite verifier")
        .changed_refs[0]
        .clone();
    let submit = AgentActionEnvelope {
        action_id: "experience-submit".into(),
        actor: actor.clone(),
        expected_revision: None,
        action: AgentAction::TaskSubmit(TaskSubmitInput {
            deliverable: None,
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
    if !direct_text {
        assert_eq!(
            services.submit_agent_action(&submit).await.unwrap().status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
    }
    if let Some(writer) = writer.as_ref().filter(|_| pending_effect.is_none()) {
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
                "edit",
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
            operator_requirement: None,
            task_ref: task_ref.clone(),
            decision: TaskReviewDecision::Accept,
            reason: "Read and checked the persisted artifact".into(),
            evidence_refs: vec![content.selector.clone()],
        }),
    };
    if let Some((gate, running)) = pending_effect.take() {
        let refused = services.submit_agent_action(&review_envelope).await;
        let unsettled = action_service.project("program-dispatch").unwrap();
        let source = format!(
            "execution-agent-receipts:{}:{}:{}",
            packet.graph_id(),
            packet.node_id(),
            packet.attempt
        );
        let events = services.event_store().list_stream(&source).unwrap();
        let receipts = events
            .iter()
            .filter(|event| event.kind == "execution.agent_tool.receipt")
            .filter_map(|event| event.payload["idempotency_key"].as_str())
            .collect::<std::collections::BTreeSet<_>>();
        let pending = events
            .iter()
            .filter(|event| event.kind == "execution.agent_tool.intent")
            .filter_map(|event| event.payload["idempotency_key"].as_str())
            .filter(|key| !receipts.contains(key))
            .count();
        gate.release.notify_one();
        running.await.unwrap().unwrap();
        let error = refused.unwrap_err();
        assert!(error.contains("effect_review_pending"), "{error}");
        assert_eq!(pending, 1);
        assert_eq!(
            unsettled.tasks[&task_ref].status,
            crate::AgenticTaskStatus::Submitted
        );
        assert!(unsettled.objective_verdict.is_none());
        println!(
            "V3_REQUIRED_PENDING_WITNESS {}",
            serde_json::json!({
                "effect_source": source, "unsettled_intents": pending,
                "task": task_ref, "denial": error, "receipt_after_release": true,
            })
        );
    }
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
        .contains("review attempt generation is stale"));
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
    let drain_workers = || async {
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
            .as_ref()
            .unwrap()
            .cancelled
            .load(std::sync::atomic::Ordering::Acquire));
        let finalized = producer_hold
            .as_ref()
            .unwrap()
            .finalized
            .lock()
            .unwrap()
            .clone();
        for (execution_id, node_id) in [
            (packet.graph_id(), packet.node_id()),
            (review_graph.id.as_str(), review_graph.nodes[0].id.as_str()),
        ] {
            assert!(finalized.iter().any(|value| value == execution_id));
            assert_eq!(
                services
                    .graph_state_store()
                    .load(execution_id)
                    .unwrap()
                    .node_statuses[node_id],
                ExecutionNodeStatus::Cancelled
            );
        }
        if let Some(writer) = writer.as_ref() {
            let error = writer
                .execute_tool(
                    "late-after-cancel",
                    "edit",
                    r#"{"path":"review-effect.txt","content":"cancelled write must not execute"}"#,
                )
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("tool_effect_authority"),
                "{error}"
            );
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                    .unwrap(),
                "actual delegated write"
            );
        }
    };
    if drain_timing == PhysicalDrainTiming::BeforeGoal {
        drain_workers().await;
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
            bind_root(root(
                id,
                AgentAction::ObjectiveCompleteRequest(ObjectiveCompleteRequestInput {
                    blocked: false,
                    blocked_reason: None,
                    human_review_required: false,
                    recommended_next_actions: Vec::new(),
                    result_refs: vec![artifact.clone()],
                    evidence_refs: vec![content.selector.clone()],
                    unresolved: vec![],
                }),
            ))
        };
        assert_eq!(
            services
                .submit_agent_action(&try_complete("unclassified-completion"))
                .await
                .unwrap()
                .status,
            AgentActionStatus::Rejected
        );
        for (id, disposition) in [
            ("must-resolve", IssueDispositionKind::MustResolve),
            ("explicit-disclosure", IssueDispositionKind::Disclose),
        ] {
            let receipt = services
                .submit_agent_action(&bind_root(root(
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
                .await
                .unwrap();
            assert_eq!(receipt.status, AgentActionStatus::Applied, "{receipt:?}");
            if disposition == IssueDispositionKind::MustResolve {
                assert_eq!(
                    services
                        .submit_agent_action(&try_complete("must-resolve-completion"))
                        .await
                        .unwrap()
                        .status,
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
    let premature = services
        .submit_agent_action(&bind_root(root(
            "before-original-review",
            AgentAction::ObjectiveCompleteRequest(
                harness_contract::agent_action::ObjectiveCompleteRequestInput {
                    blocked: false,
                    blocked_reason: None,
                    human_review_required: false,
                    recommended_next_actions: Vec::new(),
                    result_refs: vec![artifact.clone()],
                    evidence_refs: vec![content.selector.clone()],
                    unresolved: vec![],
                },
            ),
        )))
        .await
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
                criterion_source: Default::default(),
                acceptance_checks: Vec::new(),
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
            finalized: Default::default(),
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
                    reason: String::new(),
                },
            ),
        };
        let physical: AgentTaskPacket =
            serde_json::from_str(&effect_graph.nodes[0].payload_ref).unwrap();
        assert_eq!(
            physical.agentic_binding.as_ref().unwrap().task_purpose,
            Some(harness_contract::agent_action::TaskPurpose::Exploration)
        );
        assert!(physical
            .objective
            .contains(crate::agentic::order_contract::EXPLORATION_CLOSEOUT_CONTRACT));
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
        let artifact_count_before_retirement = action_service
            .project("program-dispatch")
            .unwrap()
            .artifacts
            .len();
        let withdrawn = AgentActionEnvelope {
            action_id: "effect-review-finished".into(),
            actor: check_effect.actor.clone(),
            expected_revision: None,
            action: AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
                task_ref: work_receipt.changed_refs[0].clone(),
                reason_ref: content.selector.clone(),
                evidence_refs: vec![content.selector.clone()],
            }),
        };
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
        assert_eq!(drained.artifacts.len(), artifact_count_before_retirement);
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
        if direct_text {
            let unrelated = services
                .artifact_store()
                .write_bytes(
                    harness_contract::context::ArtifactWriteDescriptor {
                        media_type: "text/plain".into(),
                        visibility_scope: "session:session-dispatch".into(),
                        expected_bytes: None,
                        original_name: None,
                    },
                    b"Different content is not the admitted result",
                )
                .await
                .unwrap();
            root_reads_review_result(&services, read_host.clone(), &unrelated.selector).await;
            let rejected = services
                .submit_agent_action(&bind_root(root(
                    "wrong-content-is-not-independent-proof",
                    AgentAction::ObjectiveReview(
                        harness_contract::agent_action::ObjectiveReviewInput {
                            criterion_ref: "reviewed-delivery".into(),
                            decision:
                                harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                            result_refs: vec![artifact.clone()],
                            evidence_refs: vec![content.selector.clone()],
                            reason_ref: content.selector.clone(),
                            reason: String::new(),
                        },
                    ),
                )))
                .await
                .unwrap_err();
            assert!(
                rejected.contains("review_requires_current_physical_reads"),
                "{rejected}"
            );
            assert_eq!(
                services
                    .goal_store()
                    .get("goal:root-agentic-execution")
                    .unwrap()
                    .unwrap()
                    .completion,
                harness_contract::goal::GoalCompletion::Open
            );
        }
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
                reason: String::new(),
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
                reason: String::new(),
            }),
        )))
        .await
        .unwrap();
    assert_eq!(
        semantic_review.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let completion = services
        .submit_agent_action(&bind_root(root(
            "experience-complete",
            AgentAction::ObjectiveCompleteRequest(
                harness_contract::agent_action::ObjectiveCompleteRequestInput {
                    blocked: false,
                    blocked_reason: None,
                    human_review_required: false,
                    recommended_next_actions: Vec::new(),
                    result_refs: vec![artifact],
                    evidence_refs: vec![content.selector],
                    unresolved: vec![],
                },
            ),
        )))
        .await
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
    if drain_timing == PhysicalDrainTiming::AfterGoal {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let program = action_service.project("program-dispatch").unwrap();
        let manifest = crate::agentic::review_evidence::work_manifest_digest(&program);
        assert_eq!(goal.completion, GoalCompletion::Satisfied);
        assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
        assert_eq!(
            program.tasks[&task_ref].status,
            crate::AgenticTaskStatus::Accepted
        );
        assert!(!goal.reviews.is_empty());
        assert!(goal
            .reviews
            .iter()
            .all(|review| review
                .verification
                .as_ref()
                .is_some_and(
                    |proof| !proof.reads.is_empty() && proof.effect_manifest_digest.is_some()
                )));
        assert!(!producer_hold
            .as_ref()
            .unwrap()
            .cancelled
            .load(std::sync::atomic::Ordering::Acquire));
        assert!(producer_hold
            .as_ref()
            .unwrap()
            .finalized
            .lock()
            .unwrap()
            .is_empty());
        for (execution_id, node_id) in [
            (packet.graph_id(), packet.node_id()),
            (review_graph.id.as_str(), review_graph.nodes[0].id.as_str()),
        ] {
            assert_eq!(
                services
                    .graph_state_store()
                    .load(execution_id)
                    .unwrap()
                    .node_statuses[node_id],
                ExecutionNodeStatus::Running
            );
        }
        if let Some(writer) = writer.as_ref() {
            let source = format!(
                "execution-agent-receipts:{}:{}:{}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt
            );
            let revision = services.event_store().stream_revision(&source).unwrap();
            let calls = effect_host.calls.lock().unwrap().len();
            let error = writer.execute_tool("after-goal-before-drain", "edit", r#"{"path":"review-effect.txt","content":"must not overwrite verified target"}"#).await.unwrap_err();
            assert!(
                error.to_string().contains("tool_effect_authority"),
                "{error}"
            );
            assert_eq!(
                services.event_store().stream_revision(&source).unwrap(),
                revision
            );
            assert_eq!(effect_host.calls.lock().unwrap().len(), calls);
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                    .unwrap(),
                "actual delegated write"
            );
        }
        let mut late_semantic = task.clone();
        late_semantic.action_id = "after-goal-before-drain-publish".into();
        let refused = services.submit_agent_action(&late_semantic).await.unwrap();
        assert_eq!(
            refused.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(refused.error.unwrap().code, "program_terminal");
        println!(
            "V3_DRAIN_WITNESS {}",
            serde_json::json!({
                "goal": goal.id, "goal_revision": goal.revision,
                "program_revision": program.revision, "verdict": program.objective_verdict,
                "producer": packet.graph_id(), "reviewer": review_graph.id,
                "physical_before": "running", "effect_host_calls": effect_host.calls.lock().unwrap().len(),
            })
        );
        drain_workers().await;
        assert_eq!(
            services
                .goal_store()
                .get("goal:root-agentic-execution")
                .unwrap()
                .unwrap(),
            goal
        );
        let drained = action_service.project("program-dispatch").unwrap();
        assert_eq!(drained.objective_verdict, program.objective_verdict);
        assert_eq!(
            crate::agentic::review_evidence::work_manifest_digest(&drained),
            manifest
        );
        assert_eq!(
            drained.tasks[&task_ref].status,
            crate::AgenticTaskStatus::Accepted
        );
    }
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
            services.workspace_key().to_owned(),
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

// Hold only the named Task's producer/reviewer at the existing backend boundary. The
// real GraphRunner starts/cancels it; it never fabricates a model verdict or
// receipt. The test drives its real Process tool/action protocol below.
pub(super) struct ReviewWorkerHold {
    pub(super) task_ref: String,
    pub(super) entered: tokio::sync::Notify,
    pub(super) cancelled: std::sync::atomic::AtomicBool,
    pub(super) finalized: std::sync::Mutex<Vec<String>>,
}
pub(super) struct ReviewWorkerResolver(Arc<ReviewWorkerHold>);
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
    fn cancellation_finalized(&self, packet: &AgentTaskPacket) {
        self.finalized
            .lock()
            .unwrap()
            .push(packet.graph_id().to_owned());
        self.cancelled
            .store(true, std::sync::atomic::Ordering::Release);
    }
}

// Test adapter performs real ArtifactStore reads. Runtime owns authorization,
// tool execution receipts and review decisions; this host never writes a proof.
#[derive(Default)]
pub(super) struct ReviewReadHost {
    pub(super) services: std::sync::OnceLock<std::sync::Weak<RuntimeServices>>,
    pub(super) calls: std::sync::Mutex<Vec<crate::RuntimeToolExecutionRequest>>,
    held_write: std::sync::OnceLock<Arc<ReviewWriteHold>>,
}

#[derive(Debug, Default)]
struct ReviewWriteHold {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
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
            "edit" => {
                std::fs::write(&path, input["content"].as_str().unwrap()).unwrap();
                if let Some(gate) = self.held_write.get() {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                }
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
            if name == "edit" {
                PermissionOperation::Write
            } else {
                PermissionOperation::Read
            },
        );
        scope.target = input["path"].as_str().map(str::to_string);
        Some(ToolEffectDescriptor {
            tool_id: name.into(),
            descriptor_hash: format!("review-read:{name}"),
            effect_kind: if name == "edit" {
                ToolEffectKind::Write
            } else {
                ToolEffectKind::Read
            },
            idempotency: ToolIdempotency::Idempotent,
            scopes: vec![scope],
            required_permission: if name == "edit" {
                ToolPermissionMode::WorkspaceWrite
            } else {
                ToolPermissionMode::ReadOnly
            },
            approval_class: ToolApprovalClass::None,
            uses_network: false,
            spawns_process: false,
            mutates_packages: false,
            mutates_system: false,
            assessment: if name == "edit" {
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
pub(super) struct RootReviewReader(pub(super) Arc<ReviewReadHost>);
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
pub(super) struct NoReviewProvider;
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
pub(super) async fn write_root_review_effect(
    runtime: &crate::ConversationRuntime<NoReviewProvider, RootReviewReader>,
    id: &str,
    content: &str,
) {
    root_review_target_tool(runtime, id, "edit", "review-effect.txt", content).await;
}

pub(super) async fn root_review_target_tool(
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
                binding: None,
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

pub(super) async fn root_reads_review_result(
    services: &Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
    selector: &str,
) {
    root_reads_generic_results(services, host, selector, false, false).await;
}

pub(super) fn root_reads_generic_results<'fixture>(
    services: &'fixture Arc<RuntimeServices>,
    host: Arc<ReviewReadHost>,
    selector: &'fixture str,
    read_tool_receipt: bool,
    write_effect: bool,
) -> std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = (
                    Option<String>,
                    crate::ConversationRuntime<NoReviewProvider, RootReviewReader>,
                ),
            > + Send
            + 'fixture,
    >,
> {
    Box::pin(execute_root_review_reads(
        services,
        host,
        selector,
        read_tool_receipt,
        write_effect,
    ))
}

async fn execute_root_review_reads(
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
    let ports = if let Some(port) = services.session_journal_port() {
        port
    } else {
        #[cfg(test)]
        {
            let store = Arc::new(crate::test_support::session_store());
            store
                .create_session(&session::SessionRecord {
                    session_id: "session-dispatch".into(),
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
            let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
            services
                .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports.clone())
                .unwrap();
            ports as Arc<dyn crate::SessionRuntimeJournalPort>
        }
        #[cfg(not(test))]
        {
            panic!("shared integration fixture requires installed Session ports")
        }
    };
    let mut session = crate::session::Session::new().with_workspace_root(services.workspace_root());
    session.session_id = "session-dispatch".into();
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
        .begin_turn_strategy_with_resource_snapshot(
            "turn-dispatch",
            "retrieve current result evidence",
            None,
        )
        .unwrap();
    runtime
        .bind_turn_strategy_execution("turn-dispatch", "root-agentic-execution")
        .unwrap();
    let result = runtime
        .execute_tool_batch_step(
            &[crate::conversation::ModelToolCall {
                binding: None,
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
            binding: None,
            id: "root-write-effect".into(), name: "edit".into(),
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
                binding: None,
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

#[cfg(feature = "test-fixtures")]
pub struct ReviewedGoalFixture {
    pub services: Arc<RuntimeServices>,
    pub goal: harness_contract::goal::GoalContract,
    pub program: crate::AgenticProgramProjection,
}

#[cfg(feature = "test-fixtures")]
pub async fn independently_reviewed_goal_fixture(
    compose: impl FnOnce(Arc<dyn crate::RuntimeExecutionHost>) -> Arc<RuntimeServices>,
    store: Arc<session::UnifiedSessionStore>,
) -> ReviewedGoalFixture {
    let read_host = Arc::new(ReviewReadHost::default());
    let services = compose(read_host.clone());
    store
        .create_session(&session::SessionRecord {
            session_id: "session-dispatch".into(),
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
    let ports = crate::session_runtime_port::TestSessionPortAdapter::new(store);
    services
        .install_session_ports(ports.clone(), ports.clone(), ports.clone(), ports)
        .unwrap();
    exercise_reviewed_goal_delivery_on(
        services.clone(),
        read_host,
        false,
        false,
        DelegatedEffectCase::None,
        true,
    )
    .await;
    let goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    let program = services
        .agent_action_service()
        .project("program-dispatch")
        .unwrap();
    assert_eq!(
        goal.completion,
        harness_contract::goal::GoalCompletion::Satisfied
    );
    assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
    ReviewedGoalFixture {
        services,
        goal,
        program,
    }
}
