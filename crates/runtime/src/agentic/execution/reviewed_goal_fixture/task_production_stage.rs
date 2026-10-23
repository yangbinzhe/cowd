{
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
    if proof_scenario.native_sources() {
        let initial = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(initial.permission_ceiling, PermissionMode::ReadOnly);
        assert_eq!(initial.resource_scopes, vec!["workspace:.".to_string()]);
    }
    let team_ref = team_receipt.changed_refs[0].clone();
    let agent_ref = services
        .submit_agent_action(&bind_root(root(
            "agent",
            AgentAction::AgentInvite(AgentInviteInput {
                team_ref: team_ref.clone(),
                role: "Researcher".to_string(),
                mission: "collect evidence".to_string(),
                required_capabilities: if proof_scenario.bounded_process() {
                    vec!["python".into(), "verification".into(), "write".into()]
                } else if delegated_effect {
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
    let producer_session = wait_real_process(&services, &receipts[0].graph_id).await;

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
        Some(REAL_PROCESS_DEFINITION)
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
            && program_id == fixture_program_id.as_str()
            && team_id == &team_ref
    ));
    let shared_prefix = cohort.render_user_messages().join("\n");
    assert!(shared_prefix.contains("investigate"));
    assert!(!shared_prefix.contains(&task_ref));
    assert!(!shared_prefix.contains(&agent_ref));
    assert_eq!(packet.deadline_at_ms, u64::MAX);
    assert!(matches!(
        packet.agentic_binding.as_ref().map(|binding| &binding.focus),
        Some(harness_contract::agent::AgenticExecutionFocus::TaskExecute {
            task_ref: bound_task_ref,
        }) if bound_task_ref == &task_ref
    ));
    assert!(packet.required_acceptance.evidence_obligations.is_empty());
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
        REAL_PROCESS_DEFINITION
    );
    let expected_capabilities = if proof_scenario.bounded_process() {
        vec![
            AgentCapability::Read,
            AgentCapability::Write,
            AgentCapability::Test,
        ]
    } else {
        vec![
            AgentCapability::Read,
            AgentCapability::Search,
            AgentCapability::Write,
            AgentCapability::Test,
            AgentCapability::Network,
        ]
    };
    assert_eq!(binding.effective_capabilities, expected_capabilities);
    if proof_scenario.bounded_process() {
        assert!(packet
            .allowed_tools
            .iter()
            .any(|tool| tool == "execute_code"));
        assert!(!packet.allowed_tools.iter().any(|tool| matches!(
            tool.as_str(),
            "bash" | "powershell" | "web_fetch" | "web_search"
        )));
    }
    assert!(binding.skill_refs.is_empty());
    assert!(binding
        .tool_contract_refs
        .contains(&"state_inspect".to_string()));
    let display = binding.display.expect("display");
    assert_eq!(display.label, "Researcher");
    assert_eq!(display.focus_label.as_deref(), Some("Evidence review"));

    let before_claim = action_service
        .project(fixture_program_id.as_str())
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

    let shadow_id = "agentic-graph:rebound-packet";
    let original_children = services
        .graph_state_store()
        .child_links("root-agentic-execution")
        .unwrap();
    assert_eq!(
        services.event_store().stream_revision(shadow_id).unwrap(),
        0
    );
    let probe = PhysicalProcessFixture::with_session_owner(
        crate::config::AgentExecutorSandboxProfile::IsolatedReadOnly,
        "packet-parent-negative-255",
        Some(&services.0),
    )
    .await;
    let probe_worker = probe
        .dispatch(
            "packet-parent-negative-255",
            vec!["read:.".into()],
            false,
            false,
            true,
        )
        .await;
    let negative_services = &probe.services;
    let valid_parent = ExecutionParentBinding {
        execution_id: probe_worker.packet.graph_id().into(),
        node_id: probe_worker.packet.node_id().into(),
    };
    let valid_actor = negative_services
        .resolve_agent_action_actor(&valid_parent, None)
        .await
        .expect("same isolated services must resolve its genuinely admitted Process");
    assert_eq!(
        valid_actor.agent_id.as_deref(),
        Some(
            probe_worker
                .packet
                .agentic_binding
                .as_ref()
                .unwrap()
                .agent_id
                .as_str()
        )
    );
    let valid_graph = negative_services
        .graph_state_store()
        .load(&valid_parent.execution_id)
        .unwrap();
    probe_worker.finish_readonly_control().await;
    assert!(confirmed_run_exit_with_retry(
        negative_services.event_store(),
        &probe_worker.packet
    )
    .is_some());
    assert!(!probe_worker.directory().exists());
    let mut rebound_graph = valid_graph.clone();
    rebound_graph.id = shadow_id.into();
    assert_eq!(rebound_graph.nodes, valid_graph.nodes);
    assert_eq!(rebound_graph.lineage, valid_graph.lineage);
    assert_eq!(rebound_graph.parent_execution, valid_graph.parent_execution);
    negative_services
        .commit_service()
        .register_graph(rebound_graph.clone())
        .expect("register corrupted recovery fixture");
    let planned = negative_services
        .graph_state_store()
        .load(shadow_id)
        .unwrap();
    assert!(planned.node_statuses.values().all(
        |status| *status == harness_contract::execution_graph::ExecutionNodeStatus::Planned
    ));
    negative_services
        .commit_service()
        .apply_command(
            &planned,
            &harness_contract::execution_graph::ExecutionGraphCommand::Cancel {
                expected_revision: planned.revision,
                reason: "Retire the never-started isolated packet-parent rejection probe"
                    .into(),
            },
        )
        .unwrap();
    let cancelled = negative_services
        .graph_state_store()
        .load(shadow_id)
        .unwrap();
    assert!(cancelled
        .node_statuses
        .values()
        .all(|status| *status
            == harness_contract::execution_graph::ExecutionNodeStatus::Cancelled));
    assert_eq!(cancelled.nodes, valid_graph.nodes);
    let error = negative_services
        .resolve_agent_action_actor(
            &ExecutionParentBinding {
                execution_id: rebound_graph.id.clone(),
                node_id: rebound_graph.nodes[0].id.clone(),
            },
            None,
        )
        .await
        .expect_err("a packet cannot be rebound to a different parent graph");
    assert_eq!(error, "agent_actor_packet_parent_binding_mismatch");
    assert_eq!(
        negative_services
            .resolve_agent_action_actor(&valid_parent, None)
            .await
            .unwrap(),
        valid_actor
    );
    assert!(negative_services
        .agent_runtime()
        .list()
        .iter()
        .all(|run| run.graph_id != shadow_id));
    assert_eq!(
        negative_services
            .agent_runtime()
            .get(probe_worker.packet.agent_id())
            .unwrap()
            .graph_id,
        probe_worker.packet.graph_id()
    );
    assert!(negative_services
        .graph_state_store()
        .load(shadow_id)
        .unwrap()
        .node_statuses
        .values()
        .all(|status| *status
            == harness_contract::execution_graph::ExecutionNodeStatus::Cancelled));
    negative_services.shutdown_maintenance().await;
    assert_eq!(
        services.event_store().stream_revision(shadow_id).unwrap(),
        0
    );
    assert_eq!(
        services
            .graph_state_store()
            .child_links("root-agentic-execution")
            .unwrap(),
        original_children
    );
    assert_eq!(
        services
            .resolve_agent_action_actor(
                &ExecutionParentBinding {
                    execution_id: graph.id.clone(),
                    node_id: graph.nodes[0].id.clone(),
                },
                None
            )
            .await
            .unwrap(),
        actor
    );

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
        .project(fixture_program_id.as_str())
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
    let delegated_projection = action_service.project(fixture_program_id.as_str()).unwrap();
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
    let optional_before = action_service.project(fixture_program_id.as_str()).unwrap();
    let optional_task = optional_before.tasks[&published.changed_refs[0]].clone();
    let optional_packets = optional_task
        .active_attempts
        .values()
        .map(|attempt| {
            let graph = services
                .graph_state_store()
                .load(&attempt.execution_id)
                .unwrap();
            let nodes = graph
                .nodes
                .iter()
                .filter(|node| node.kind == ExecutionNodeKind::AgentTask)
                .collect::<Vec<_>>();
            assert_eq!(nodes.len(), 1);
            let original: AgentTaskPacket =
                serde_json::from_str(&nodes[0].payload_ref).unwrap();
            assert_eq!(original.graph_id(), attempt.execution_id);
            let binding = original.agentic_binding.as_ref().unwrap();
            assert_eq!(binding.program_id, fixture_program_id.as_str());
            assert_eq!(binding.membership_id, attempt.membership_id);
            assert_eq!(binding.team_id, organized_team);
            assert!(matches!(&binding.focus,
        harness_contract::agent::AgenticExecutionFocus::TaskExecute { task_ref }
            if task_ref == &published.changed_refs[0]));
            original
        })
        .collect::<Vec<_>>();
    let optional_withdrawal = bind_root(root(
        "retire-optional-verification",
        AgentAction::TaskWithdraw(harness_contract::agent_action::TaskWithdrawInput {
            task_ref: published.changed_refs[0].clone(),
            reason_ref: content.selector.clone(),
            evidence_refs: vec![content.selector.clone()],
        }),
    ));
    let retired = services
        .submit_agent_action(&optional_withdrawal)
        .await
        .unwrap();
    assert_eq!(
        retired.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    services
        .dispatch_agentic_followups(
            &optional_withdrawal,
            AgenticDispatchContext {
                session_id: optional_before.session_id.clone(),
                turn_id: optional_before.turn_id.clone(),
                model_lease: optional_before.model_lease.clone(),
                permission_ceiling: optional_before.permission_ceiling,
                resource_scopes: optional_before.resource_scopes.clone(),
            },
        )
        .await
        .expect("original optional withdrawal followup");
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
    for original in &optional_packets {
        wait_retired_process(&services, original, &published.changed_refs[0]).await;
        assert!(confirmed_run_exit_with_retry(services.event_store(), original).is_some());
        assert!(crate::agent_runtime::authenticated_run_lifecycle(
            services.event_store(),
            original
        )
        .unwrap()
        .no_direct_effects());
    }
    tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let current = action_service.project(fixture_program_id.as_str()).unwrap();
            if current.teams[&organized_team].lifecycle
                == crate::agentic::program::AgenticTeamLifecycle::Retired
            {
                let task = &current.tasks[&published.changed_refs[0]];
                assert_eq!(task.status, crate::AgenticTaskStatus::Withdrawn);
                assert!(task.active_attempts.is_empty() && task.pending_retirement.is_none());
                assert!(task.claimant.is_none() && task.claim_execution_id.is_none());
                let had_attempt = optional_task.status == crate::AgenticTaskStatus::Claimed
                    || !optional_task.active_attempts.is_empty();
                assert_eq!(
                    task.claim_generation,
                    optional_task.claim_generation + u64::from(had_attempt)
                );
                for original in &optional_packets {
                    let membership = &original.agentic_binding.as_ref().unwrap().membership_id;
                    assert_eq!(
                        current.memberships[membership].lifecycle,
                        crate::agentic::program::AgenticMembershipLifecycle::Retired
                    );
                }
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|error| {
        own832_diagnostics::capture(
            &services,
            fixture_program_id.as_str(),
            &published.changed_refs[0],
            "original_team_retirement_timeout",
            optional_packets.first(),
        );
        panic!("optional Team retirement missing: {error:?}; see OWN832 snapshot");
    });

    let mut pending_effect = None;
    if proof_scenario.optional_unknown() {
        std::fs::write(
            services.workspace_root().join("review-effect.txt"),
            "physical target exists before the required producer admission",
        )
        .unwrap();
    }
    let delegated_write_content = if proof_scenario.native_sources()
        || matches!(
            proof_scenario,
            R1ProofScenario::PartialDependency
                | R1ProofScenario::TransitiveDependency
                | R1ProofScenario::ReadDelivery(_)
        ) {
        "actual delegated write\nstable middle line\ninitial dependency last line\n"
    } else {
        "actual delegated write"
    };
    let verified_delegated_content = if proof_scenario.native_sources()
        || matches!(
            proof_scenario,
            R1ProofScenario::PartialDependency
                | R1ProofScenario::TransitiveDependency
                | R1ProofScenario::ReadDelivery(_)
        ) {
        "actual delegated write\nstable middle line\nchanged dependency last line\n"
    } else {
        delegated_write_content
    };
    let writer = if delegated_effect {
        let writer = Arc::new(
            RealProcessSession::prepare(
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
            &serde_json::json!({"path":"review-effect.txt","content":delegated_write_content})
                .to_string(),
        )
        .await
        .unwrap();
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                .unwrap(),
            delegated_write_content
        );
        if proof_scenario == R1ProofScenario::ReadDelivery(RealProcessProofCase::Effects) {
            std::fs::write(
                services.workspace_root().join("r1-optional-target.txt"),
                "Initial second target of result A",
            )
            .unwrap();
            writer.execute_tool("f2-real-second-target-A", "edit",
            r#"{"path":"r1-optional-target.txt","content":"Actual second governed target of result A"}"#).await.unwrap();
        }
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
        let stale = RealProcessSession::prepare(
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
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                .unwrap(),
            delegated_write_content
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
                    &serde_json::json!({"path":"review-effect.txt","content":delegated_write_content}).to_string(),
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
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                .unwrap(),
            delegated_write_content
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
        wait_retired_process(&services, &packet, &task_ref).await;
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(
            current.tasks[&task_ref].status,
            AgenticTaskStatus::Withdrawn
        );
        assert!(current.tasks[&task_ref].active_attempts.is_empty());
        wait_retired_process(&services, &packet, &task_ref).await;
        assert_eq!(
            services.event_store().stream_revision(&index).unwrap(),
            revision,
            "cancellation retains the original effect evidence"
        );
        return None;
    }
    let bounded_runs = if proof_scenario.bounded_process() {
        run_bounded_process_versions_283(&services, &read_host, writer.as_ref().unwrap()).await
    } else {
        Vec::new()
    };
    let mut review_evidence_refs = vec![content.selector.clone()];
    if let Some(current) = bounded_runs.last() {
        review_evidence_refs.push(current.body.selector.clone());
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
            .project(fixture_program_id.as_str())
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
        let program = action_service.project(fixture_program_id.as_str()).unwrap();
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
    Some((
        action_service,
        team_ref,
        task,
        task_ref,
        producer_session,
        packet,
        actor,
        content,
        pending_effect,
        delegated_write_content,
        verified_delegated_content,
        writer,
        bounded_runs,
        review_evidence_refs,
        artifact,
        reviewer,
        probe,
    ))
}
