use super::*;
use harness_contract::context::ChildExecutionBudgetReservation;
use harness_contract::tool::{
    ToolApprovalClass, ToolEffectKind, ToolIdempotency, ToolPermissionMode,
};

#[test]
fn controlled_recovery_terminal_is_the_only_tool_scope_graph_event() {
    let record = crate::authorization_negotiator::ControlledRecoveryTerminalRecord {
        recovery_scope: "turn:turn-1".to_string(),
        session_id: "session-1".to_string(),
        turn_id: "turn-1".to_string(),
        execution_id: "execution-1".to_string(),
        fingerprints: Vec::new(),
    };
    let terminal = crate::authorization_negotiator::controlled_recovery_terminal_event(&record)
        .expect("canonical terminal");
    validate_executor_domain_events(std::slice::from_ref(&terminal))
        .expect("canonical controlled recovery terminal is atomic with graph terminal");

    let mut forged = terminal.clone();
    forged.event.kind = "tool.invocation.completed".to_string();
    assert!(matches!(
        validate_executor_domain_events(&[forged]),
        Err(ExecutionCommitError::ProtectedDomainScope(scope)) if scope == "tool"
    ));

    let mut wrong_turn = terminal;
    wrong_turn.event.stream_id = "authorization-recovery:session-1:turn:other-turn".to_string();
    assert!(matches!(
        validate_executor_domain_events(&[wrong_turn]),
        Err(ExecutionCommitError::ProtectedDomainScope(scope)) if scope == "tool"
    ));
}

fn request(id: &str) -> crate::RuntimeToolExecutionRequest {
    crate::RuntimeToolExecutionRequest {
        governed_plan_id: "plan".to_string(),
        governed_plan_revision: 1,
        observation_wave_sequence: 1,
        idempotency_key: format!("idem-{id}"),
        tool_use_id: id.to_string(),
        tool_name: "fixture_tool".to_string(),
        input: format!(r#"{{"id":"{id}"}}"#),
        category: crate::ToolSafetyCategory::ReadOnly,
        authorization: None,
        session_id: Some("session".to_string()),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 0,
        authorized_scopes: Vec::new(),
        memory_context: None,
        model_lease: None,
        parent_execution: None,
        parent_execution_attempt: None,
        execution_decision: None,
        evaluation_isolated: false,
        managed_invocation: None,
        tool_progress: crate::ToolProgressSink::default(),
    }
}

fn outcome(id: &str, output: &str) -> crate::RuntimeToolExecutionOutcome {
    crate::RuntimeToolExecutionOutcome {
        tool_use_id: id.to_string(),
        tool_name: "fixture_tool".to_string(),
        status: crate::RuntimeToolExecutionStatus::Executed,
        category: crate::ToolSafetyCategory::ReadOnly,
        output: Some(output.to_string()),
        error: None,
        evidence_ref: format!("tool://{id}"),
        observed_evidence: Vec::new(),
    }
}

fn mutation_effect(idempotency: ToolIdempotency) -> ToolEffectDescriptor {
    ToolEffectDescriptor {
        tool_id: "fixture_tool".to_string(),
        descriptor_hash: "fixture-effect-v1".to_string(),
        effect_kind: ToolEffectKind::Write,
        idempotency,
        scopes: Vec::new(),
        required_permission: ToolPermissionMode::WorkspaceWrite,
        approval_class: ToolApprovalClass::Policy,
        uses_network: false,
        spawns_process: false,
        mutates_packages: false,
        mutates_system: false,
        assessment: harness_contract::policy::EffectAssessment::default(),
    }
}

fn readonly_effect() -> ToolEffectDescriptor {
    let mut effect = mutation_effect(ToolIdempotency::Idempotent);
    effect.effect_kind = ToolEffectKind::Read;
    effect.required_permission = ToolPermissionMode::ReadOnly;
    effect.approval_class = ToolApprovalClass::None;
    effect
}

fn agent_task_graph() -> ExecutionGraph {
    let packet = AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None,
            "agent-instance",
            "agent-run",
            "task",
            "session",
            "mission",
            Some("team-run"),
            "graph",
            "agent-node",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "verify canonical reverse lineage".to_string(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        requires_managed_collaboration_escalation: false,
        acceptance: Vec::new(),
        team_role_identity: None,
        team_role: None,
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs: Vec::new(),
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "fast".to_string(),
        budget_lease: ChildExecutionBudgetReservation::single(
            "budget",
            "agent-instance",
            "agent",
            1_000,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "agent-task-idempotency".to_string(),
    };
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::AgentTask,
        "agent",
        serde_json::to_string(&packet).expect("serialize Agent task packet"),
    );
    node.id = "agent-node".to_string();
    node.idempotency_key = "agent-node-idempotency".to_string();
    let mut graph = ExecutionGraph::new("lineage");
    graph.id = "graph".to_string();
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    graph
        .node_statuses
        .insert(node.id.clone(), ExecutionNodeStatus::Planned);
    graph.nodes.push(node);
    graph
}

fn waiting_child_join_graph() -> ExecutionGraph {
    let graph_id = "parent-graph";
    let node_id = "child-team";
    let child_id = "team-graph:team-child";
    let request = harness_contract::team::TeamInstantiationRequest {
        request_id: "child-request".to_string(),
        team_id: "team-child".to_string(),
        mission_id: "mission".to_string(),
        lineage: harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "session".to_string(),
            turn_id: "turn".to_string(),
            root_task_id: "root-task".to_string(),
            task_id: "task".to_string(),
            generation: 1,
        },
        parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: graph_id.to_string(),
            node_id: node_id.to_string(),
        }),
        selection_mode: harness_contract::team::TeamSelectionMode::Explicit,
        strategy_binding: None,
        template_selector: harness_contract::team::TeamTemplateSelector::LatestStable {
            template_id: harness_contract::team::TeamTemplateDefinitionId::new(
                harness_contract::agent::DefinitionScope::Builtin,
                "cowd/direct-executor",
            )
            .expect("template id"),
        },
        objective: "resolve child".to_string(),
        acceptance: Vec::new(),
        risk: None,
        role_binding_overrides: Vec::new(),
        display_name: None,
        role_display_overrides: Vec::new(),
        cardinality_overrides: Vec::new(),
        focus_partition_plans: Vec::new(),
        requires_managed_collaboration_escalation: false,
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "fixture-model".to_string(),
        execution_budget: harness_contract::context::ParentExecutionBudget::new(
            "fixture-team-budget",
            65_536,
            u64::MAX,
            32,
            1,
        ),
        deadline_at_ms: u64::MAX,
        managed_invocation: None,
        resource_scopes: Vec::new(),
        allow_whole_workspace_scope: false,
        upstream_evidence_refs: Vec::new(),
        upstream_artifact_refs: Vec::new(),
        upstream_result_context: Vec::new(),
        execution_capacity: None,
    };
    let mut node = ExecutionNodeSpec::new(
        ExecutionNodeKind::Subgraph,
        "team_subgraph",
        serde_json::to_string(&request).expect("serialize child request"),
    );
    node.id = node_id.to_string();
    node.idempotency_key = "child-request".to_string();
    let mut graph = ExecutionGraph::new("parent");
    graph.id = graph_id.to_string();
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    graph.nodes.push(node);
    graph
        .node_statuses
        .insert(node_id.to_string(), ExecutionNodeStatus::WaitingExternal);
    graph.node_results.insert(
        node_id.to_string(),
        ExecutionNodeResult {
            status: ExecutionNodeStatus::WaitingExternal,
            result_ref: Some(format!("execution-graph:{child_id}")),
            summary: None,
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 1,
        },
    );
    graph
        .recovery_cursor
        .node_attempts
        .insert(node_id.to_string(), 1);
    graph
}

#[test]
fn planned_graph_and_continuation_claim_commit_in_one_transaction() {
    let store = Arc::new(crate::RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let candidate = crate::ContinuationCandidate {
        source_session_id: "session".to_string(),
        source_turn_id: "turn-previous".to_string(),
        source_root_id: "root-previous".to_string(),
        team_set_ref: "team_graph:team-previous".to_string(),
        delivery_revision: 9,
        result_refs: vec!["team_graph:team-previous".to_string()],
        handoff_id: None,
    };
    let binding = crate::compile_continuation_binding(
        &candidate,
        "ingress-current",
        9,
        harness_contract::turn::ContinuationAuthorization::Authorized,
        1,
    )
    .expect("binding");
    let mut graph = ExecutionGraph::new("continue verified Team work");
    graph.id = "root-current".to_string();
    crate::test_support::attach_execution_graph_lineage(&mut graph);
    graph.continuation_binding = Some(binding.clone());

    let receipt = service.register_graph(graph).expect("atomic registration");
    assert_eq!(receipt.graph.continuation_binding, Some(binding.clone()));
    let claim = store
        .list_stream("continuation-cas")
        .expect("claim stream")
        .into_iter()
        .next()
        .expect("claim event");
    let planned = store
        .list_stream("root-current")
        .expect("graph stream")
        .into_iter()
        .next()
        .expect("planned graph");
    assert_eq!(claim.transaction_id, planned.transaction_id);
    assert_eq!(claim.commit_cursor, planned.commit_cursor);
    assert_eq!(
        claim
            .payload
            .pointer("/root_graph_id")
            .and_then(serde_json::Value::as_str),
        Some("root-current")
    );

    let mut retry = ExecutionGraph::new("continue verified Team work");
    retry.id = "root-retry-must-not-exist".to_string();
    crate::test_support::attach_execution_graph_lineage(&mut retry);
    retry.continuation_binding = Some(binding);
    let error = match service.register_graph(retry) {
        Ok(_) => panic!("continuation retry must return the existing root"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        ExecutionCommitError::AlreadyAppliedSame { ref graph_id } if graph_id == "root-current"
    ));
    assert!(store
        .list_stream("root-retry-must-not-exist")
        .expect("retry graph stream")
        .is_empty());
}

fn register_waiting_child_join(service: &ExecutionCommitService) -> ExecutionGraph {
    let registered = service
        .register_graph(waiting_child_join_graph())
        .expect("register parent")
        .graph;
    let ready = service
        .transition_node(
            &registered,
            "child-team",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .expect("ready child join")
        .graph;
    let running = service
        .transition_node(
            &ready,
            "child-team",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .expect("start child join")
        .graph;
    service
        .transition_node(
            &running,
            "child-team",
            ExecutionNodeStatus::WaitingExternal,
            Some(ExecutionNodeResult {
                status: ExecutionNodeStatus::WaitingExternal,
                result_ref: Some("execution-graph:team-graph:team-child".to_string()),
                summary: None,
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }),
            Vec::new(),
        )
        .expect("persist child join")
        .graph
}

#[test]
fn graph_events_expose_complete_execution_identity_reverse_refs() {
    let refs = graph_identity_refs(&agent_task_graph());
    let pairs = refs
        .iter()
        .map(|reference| (reference.kind.as_str(), reference.id.as_str()))
        .collect::<BTreeSet<_>>();
    for expected in [
        ("execution_graph", "graph"),
        ("principal", "test.principal"),
        ("workspace", "test-workspace"),
        ("mission", "mission"),
        ("task", "task"),
        ("session", "session"),
        ("turn", "test-turn"),
        ("team_run", "team-run"),
        ("agent_run", "agent-run"),
        ("execution_node", "agent-node"),
    ] {
        assert!(
            pairs.contains(&expected),
            "missing reverse lineage ref {expected:?}: {pairs:?}"
        );
    }
}

#[test]
fn readonly_wave_receipts_commit_atomically_and_replay_idempotently() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let receipts = vec![
        (request("read-1"), outcome("read-1", "one")),
        (request("read-2"), outcome("read-2", "two")),
    ];
    service
        .commit_readonly_tool_receipts(&receipts)
        .expect("commit read wave");
    let first = store
        .event_by_idempotency_key("execution-effect:idem-read-1", "idem-read-1:read-receipt")
        .unwrap()
        .expect("first receipt");
    let second = store
        .event_by_idempotency_key("execution-effect:idem-read-2", "idem-read-2:read-receipt")
        .unwrap()
        .expect("second receipt");
    assert_eq!(first.transaction_id, second.transaction_id);
    service
        .commit_readonly_tool_receipts(&receipts)
        .expect("idempotent replay");
    assert_eq!(
        store
            .list_stream("execution-effect:idem-read-1")
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn readonly_receipt_rehydrates_only_for_the_same_tool_and_input_fingerprint() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(store);
    let original = request("read-recovery");
    service
        .commit_readonly_tool_receipts(&[(original.clone(), outcome("read-recovery", "durable"))])
        .expect("commit bounded read receipt");

    assert!(matches!(
        service
            .begin_tool_effect(&original, &readonly_effect())
            .expect("rehydrate read"),
        ToolEffectState::Completed(crate::RuntimeToolExecutionOutcome {
            output: Some(ref output),
            ..
        }) if output == "durable"
    ));

    let mut collision = original;
    collision.input = r#"{"id":"changed"}"#.to_string();
    assert!(matches!(
        service.begin_tool_effect(&collision, &readonly_effect()),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));
}

#[test]
fn delegated_agent_receipts_are_indexed_atomically_and_reload_without_scanning_effects() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let request = crate::RuntimeToolExecutionRequest {
        parent_execution: Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: "graph-agent-receipts".to_string(),
            node_id: "agent-node".to_string(),
        }),
        parent_execution_attempt: Some(3),
        authorized_scopes: vec!["read:src/lib.rs".to_string()],
        ..request("agent-receipt")
    };
    let outcome = outcome("agent-receipt", "durable observation");
    service
        .commit_readonly_tool_receipts(&[(request.clone(), outcome.clone())])
        .expect("commit exact receipt and index atomically");

    let index_stream = "execution-agent-receipts:graph-agent-receipts:agent-node:3";
    let indexed = store.list_stream(index_stream).expect("indexed stream");
    assert_eq!(indexed.len(), 1);
    let effect = store
        .list_stream(&format!("execution-effect:{}", request.idempotency_key))
        .expect("effect stream");
    assert_eq!(effect.len(), 1);
    assert_eq!(indexed[0].transaction_id, effect[0].transaction_id);

    let recovered = ExecutionCommitService::new(store)
        .load_delegated_agent_tool_receipts("graph-agent-receipts", "agent-node", 3)
        .expect("reload exact attempt receipts");
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].sequence, request.observation_wave_sequence);
    assert_eq!(recovered[0].authorized_scopes, request.authorized_scopes);
    assert_eq!(recovered[0].outcome, outcome);
}

#[test]
fn mutation_intent_blocks_uncertain_replay_and_completed_receipt_rehydrates() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(store);
    let mutation_request = request("mutation");
    let non_idempotent = mutation_effect(ToolIdempotency::NonIdempotent);
    assert_eq!(
        service
            .begin_tool_effect(&mutation_request, &non_idempotent)
            .unwrap(),
        ToolEffectState::Fresh
    );
    assert_eq!(
        service
            .begin_tool_effect(&mutation_request, &non_idempotent)
            .unwrap(),
        ToolEffectState::Uncertain
    );
    let mut outcome = outcome("mutation", &"x".repeat(32 * 1024));
    outcome.observed_evidence = vec![harness_contract::context::ObservedEvidence {
        obligation_id: "write:fixture".to_string(),
        target: harness_contract::context::EvidenceTargetIdentity::Workspace {
            scope: harness_contract::context::WorkspaceScopeIdentity {
                access_mode: harness_contract::context::WorkspaceAccessMode::Write,
                path: harness_contract::context::WorkspacePathIdentity {
                    workspace_id: "workspace".to_string(),
                    repository_id: "repository".to_string(),
                    workspace_relative_path: "fixture.txt".to_string(),
                    repository_relative_path: "fixture.txt".to_string(),
                    object_kind: harness_contract::context::WorkspaceObjectKind::File,
                    observed_revision_or_digest: Some("after".to_string()),
                },
                coverage: harness_contract::context::EvidenceCoverageKind::WriteEffect,
            },
        },
        observed_at_sequence: 1,
        tool_name: "fixture_tool".to_string(),
        provenance: harness_contract::context::ObservedEvidenceProvenance::FreshExecution,
        evidence_ref: None,
        model_observation: None,
        workspace_prior_state: Some(harness_contract::context::WorkspacePriorState::Existing {
            sha256: "before".to_string(),
        }),
    }];
    service
        .commit_tool_effect(&mutation_request, &non_idempotent, &outcome)
        .unwrap();
    let ToolEffectState::Completed(rehydrated) = service
        .begin_tool_effect(&mutation_request, &non_idempotent)
        .unwrap()
    else {
        panic!("completed mutation must rehydrate its receipt");
    };
    assert!(rehydrated.output.unwrap().len() < 20 * 1024);
    assert_eq!(
        rehydrated.observed_evidence[0].workspace_prior_state,
        outcome.observed_evidence[0].workspace_prior_state
    );

    let mut wrong_tool = mutation_request.clone();
    wrong_tool.tool_name = "other_tool".to_string();
    assert!(matches!(
        service.begin_tool_effect(&wrong_tool, &non_idempotent),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));
    let mut wrong_input = mutation_request.clone();
    wrong_input.input = r#"{"id":"other-input"}"#.to_string();
    assert!(matches!(
        service.begin_tool_effect(&wrong_input, &non_idempotent),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));
    let mut wrong_descriptor = non_idempotent.clone();
    wrong_descriptor.descriptor_hash = "fixture-effect-v2".to_string();
    assert!(matches!(
        service.begin_tool_effect(&mutation_request, &wrong_descriptor),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));

    let idempotent_request = request("idempotent-mutation");
    let idempotent = mutation_effect(ToolIdempotency::IdempotentWithKey);
    assert_eq!(
        service
            .begin_tool_effect(&idempotent_request, &idempotent)
            .unwrap(),
        ToolEffectState::Fresh
    );
    let mut changed_idempotent_input = idempotent_request.clone();
    changed_idempotent_input.input = r#"{"id":"changed"}"#.to_string();
    assert!(matches!(
        service.begin_tool_effect(&changed_idempotent_input, &idempotent),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));
    assert_eq!(
        service
            .begin_tool_effect(&idempotent_request, &idempotent)
            .unwrap(),
        ToolEffectState::Fresh
    );
}

#[test]
fn semantic_replan_is_revision_checked_idempotent_and_atomic() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(store);
    let mut graph = agent_task_graph();
    graph.orchestration = Some(
        harness_contract::execution_graph::ExecutionOrchestrationMetadata {
            mutation_id: "initial-mutation".to_string(),
            applied_mutation_ids: vec!["initial-mutation".to_string()],
            collaboration_escalations: Vec::new(),
            semantic_revision: 1,
            source_generation: 1,
            completion: Default::default(),
            collaboration_program: None,
        },
    );
    let registered = service.register_graph(graph).expect("register graph").graph;

    assert!(matches!(
        service.apply_command(
            &registered,
            &ExecutionGraphCommand::ClaimWork {
                expected_revision: registered.revision,
                node_id: "agent-node".to_string(),
                claimant_instance_id: "ineligible".to_string(),
                claimant_role_id: Some("writer".to_string()),
                claimant_capabilities: Vec::new(),
                claimed_at_ms: 90,
                lease_expires_at_ms: 190,
            }
        ),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));
    let mut added =
        ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "bounded-payload");
    added.id = "agent-node-2".to_string();
    added.idempotency_key = "agent-node-2-idempotency".to_string();
    let first = service
        .replan_semantic(
            &registered,
            vec![added.clone()],
            Vec::new(),
            "add bounded reviewer".to_string(),
            "revision-2".to_string(),
            Default::default(),
            None,
            None,
        )
        .expect("semantic revision commits");
    assert_eq!(first.graph.revision, registered.revision + 1);
    assert_eq!(first.graph.nodes.len(), registered.nodes.len() + 1);
    assert_eq!(
        first
            .graph
            .orchestration
            .as_ref()
            .expect("orchestration")
            .applied_mutation_ids,
        vec!["initial-mutation", "revision-2"]
    );

    let duplicate = match service.replan_semantic(
        &first.graph,
        vec![added.clone()],
        Vec::new(),
        "duplicate".to_string(),
        "revision-2".to_string(),
        Default::default(),
        None,
        None,
    ) {
        Ok(_) => panic!("same mutation id cannot commit twice"),
        Err(error) => error,
    };
    assert!(matches!(duplicate, ExecutionCommitError::InvalidReplan(_)));

    let mut stale_added =
        ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "stale-payload");
    stale_added.id = "stale-agent-node".to_string();
    stale_added.idempotency_key = "stale-agent-node-idempotency".to_string();
    let stale = match service.replan_semantic(
        &registered,
        vec![stale_added],
        Vec::new(),
        "stale proposal".to_string(),
        "revision-stale".to_string(),
        Default::default(),
        None,
        None,
    ) {
        Ok(_) => panic!("stale graph revision cannot partially commit"),
        Err(error) => error,
    };
    assert!(matches!(stale, ExecutionCommitError::EventStore(_)));
}

#[test]
fn collaboration_program_revision_keeps_prior_obligations_and_adds_new_teams() {
    use harness_contract::execution_graph::{
        CollaborationEdgeKind, CollaborationProgram, CollaborationProgramControlState,
        CollaborationProgramEdge, CollaborationProgramLifecycle, CollaborationTeamInstance,
        ProgramResourceLedger, TeamAdmissionObligation, TeamAdmissionResourceReservation,
        TeamAdmissionState,
    };

    let mut current = Some(CollaborationProgram {
        program_id: "program-root".to_string(),
        revision: 1,
        approval_policy_digest: "sha256:policy".to_string(),
        required_team_count: 1,
        team_instances: vec![CollaborationTeamInstance {
            instance_id: "research:1".to_string(),
            semantic_node_id: "research".to_string(),
            required: true,
        }],
        edges: Vec::new(),
        semantic_node_instances: BTreeMap::from([(
            "research".to_string(),
            vec!["graph:research:1".to_string()],
        )]),
        control: CollaborationProgramControlState {
            lifecycle: CollaborationProgramLifecycle::Running,
            obligations: vec![TeamAdmissionObligation {
                instance_id: "research:1".to_string(),
                binding_ref: "team-binding:sha256:research".to_string(),
                state: TeamAdmissionState::Admitted,
                child_graph_ref: Some("team-graph:research".to_string()),
                reason_kind: None,
                terminal: None,
                reservation: TeamAdmissionResourceReservation {
                    context_reservation_tokens: 100,
                    output_reservation_tokens: 50,
                    parallel_demand: 1,
                },
                revision: 1,
            }],
            resource_ledger: ProgramResourceLedger {
                context_reservation_tokens: 100,
                output_reservation_tokens: 50,
                parallel_demand: 1,
                deadline_at_ms: 1000,
                admitted_at_ms: 1,
                confidence_basis_points: 10_000,
                revision: 1,
                capacity_profile_id: String::new(),
                capacity_profile_revision: 0,
                capacity_profile_digest: String::new(),
                resolved_parallel_ceiling: 0,
            },
            waiting_relation: None,
            blocker_ref: None,
            next_action: Some("await_graph_transitions".to_string()),
        },
        semantic_intent: None,
    });
    let delta = CollaborationProgram {
        program_id: "ignored-delta-id".to_string(),
        revision: 1,
        approval_policy_digest: "sha256:policy".to_string(),
        required_team_count: 1,
        team_instances: vec![CollaborationTeamInstance {
            instance_id: "review:1".to_string(),
            semantic_node_id: "review".to_string(),
            required: true,
        }],
        edges: vec![CollaborationProgramEdge {
            edge_id: "research:1->review:1".to_string(),
            from: "research:1".to_string(),
            to: "review:1".to_string(),
            kind: CollaborationEdgeKind::ReviewOf,
            input_contract: Default::default(),
            state: Default::default(),
            delivery_receipt: None,
            claim_receipt: None,
        }],
        semantic_node_instances: BTreeMap::from([(
            "review".to_string(),
            vec!["graph:review:1".to_string()],
        )]),
        control: CollaborationProgramControlState {
            lifecycle: CollaborationProgramLifecycle::Admitting,
            obligations: vec![TeamAdmissionObligation {
                instance_id: "review:1".to_string(),
                binding_ref: "team-binding:sha256:review".to_string(),
                state: TeamAdmissionState::Admitting,
                child_graph_ref: None,
                reason_kind: None,
                terminal: None,
                reservation: TeamAdmissionResourceReservation {
                    context_reservation_tokens: 70,
                    output_reservation_tokens: 30,
                    parallel_demand: 1,
                },
                revision: 1,
            }],
            resource_ledger: ProgramResourceLedger {
                context_reservation_tokens: 70,
                output_reservation_tokens: 30,
                parallel_demand: 1,
                deadline_at_ms: 2000,
                admitted_at_ms: 1,
                confidence_basis_points: 10_000,
                revision: 1,
                capacity_profile_id: String::new(),
                capacity_profile_revision: 0,
                capacity_profile_digest: String::new(),
                resolved_parallel_ceiling: 0,
            },
            waiting_relation: Some("team_admission".to_string()),
            blocker_ref: None,
            next_action: Some("admit_exact_team_bindings".to_string()),
        },
        semantic_intent: None,
    };
    let mut policy_drift = delta.clone();
    policy_drift.approval_policy_digest = "sha256:other-policy".to_string();
    assert!(matches!(
        merge_collaboration_program(&mut current, Some(policy_drift)),
        Err(ExecutionCommitError::InvalidReplan(message))
            if message.contains("immutable approval policy digest")
    ));
    assert_eq!(
        current
            .as_ref()
            .expect("current program survives rejected drift")
            .revision,
        1
    );
    merge_collaboration_program(&mut current, Some(delta)).expect("merge additive revision");
    let program = current.expect("program");
    assert_eq!(program.program_id, "program-root");
    assert_eq!(program.revision, 2);
    assert_eq!(program.required_team_count, 2);
    assert_eq!(program.edges.len(), 1);
    assert_eq!(program.semantic_node_instances.len(), 2);
    assert_eq!(
        program.control.lifecycle,
        CollaborationProgramLifecycle::Admitting
    );
    assert_eq!(program.control.obligations.len(), 2);
    assert_eq!(
        program.control.obligations[0].state,
        TeamAdmissionState::Admitted
    );
    assert_eq!(
        program.control.obligations[1].state,
        TeamAdmissionState::Admitting
    );
    assert!(program
        .control
        .obligations
        .iter()
        .all(|obligation| obligation.revision == 2));
    assert_eq!(program.control.resource_ledger.revision, 2);
    assert_eq!(
        program.control.resource_ledger.context_reservation_tokens,
        170
    );
    assert_eq!(
        program.control.resource_ledger.output_reservation_tokens,
        80
    );
    assert_eq!(program.control.resource_ledger.parallel_demand, 2);
    assert_eq!(program.control.resource_ledger.deadline_at_ms, 2000);
    assert_eq!(
        program
            .team_instances
            .iter()
            .map(|team| team.instance_id.as_str())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["research:1", "review:1"])
    );
}

#[test]
fn cross_team_edge_delivery_and_claim_are_fenced_by_node_attempts() {
    use harness_contract::execution_graph::{
        CollaborationEdgeKind, CollaborationProgram, CollaborationProgramEdge,
        CollaborationProgramLifecycle, CollaborationTeamInstance, ExecutionGraphCommand,
        ExecutionOrchestrationMetadata,
    };

    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(store);
    let mut graph = agent_task_graph();
    graph.id = "cross-team-root".to_string();
    let mut consumer = graph.nodes[0].clone();
    consumer.id = "consumer-team".to_string();
    consumer.idempotency_key = "consumer-team-key".to_string();
    graph.nodes[0].id = "producer-team".to_string();
    graph.nodes[0].idempotency_key = "producer-team-key".to_string();
    graph.node_statuses.clear();
    graph
        .node_statuses
        .insert("producer-team".to_string(), ExecutionNodeStatus::Planned);
    graph
        .node_statuses
        .insert("consumer-team".to_string(), ExecutionNodeStatus::Planned);
    graph.nodes.push(consumer);
    graph.orchestration = Some(ExecutionOrchestrationMetadata {
        mutation_id: "cross-team-test".to_string(),
        applied_mutation_ids: Vec::new(),
        collaboration_escalations: Vec::new(),
        semantic_revision: 1,
        source_generation: 1,
        completion: Default::default(),
        collaboration_program: Some(CollaborationProgram {
            program_id: "program-cross-team".to_string(),
            revision: 1,
            approval_policy_digest: "sha256:policy".to_string(),
            required_team_count: 2,
            team_instances: vec![
                CollaborationTeamInstance {
                    instance_id: "producer:1".to_string(),
                    semantic_node_id: "producer".to_string(),
                    required: true,
                },
                CollaborationTeamInstance {
                    instance_id: "consumer:1".to_string(),
                    semantic_node_id: "consumer".to_string(),
                    required: true,
                },
            ],
            edges: vec![CollaborationProgramEdge {
                edge_id: "producer:1->consumer:1".to_string(),
                from: "producer:1".to_string(),
                to: "consumer:1".to_string(),
                kind: CollaborationEdgeKind::Handoff,
                input_contract: Default::default(),
                state: Default::default(),
                delivery_receipt: None,
                claim_receipt: None,
            }],
            semantic_node_instances: BTreeMap::from([
                ("producer".to_string(), vec!["producer-team".to_string()]),
                ("consumer".to_string(), vec!["consumer-team".to_string()]),
            ]),
            control: harness_contract::execution_graph::CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Planning,
                ..Default::default()
            },
            semantic_intent: None,
        }),
    });
    graph.edges = vec![ExecutionEdge {
        from: "producer-team".to_string(),
        to: "consumer-team".to_string(),
        kind: harness_contract::execution_graph::ExecutionEdgeKind::CrossTeamHandoff,
    }];
    let registered = service.register_graph(graph).expect("register graph").graph;
    let registered = service
            .apply_command(
                &registered,
                &ExecutionGraphCommand::ApplyCrossTeamEdgePatch {
                    expected_revision: registered.revision,
                    patch: Box::new(
                        harness_contract::execution_graph::CollaborationIntentPatch {
                            program_id: "program-cross-team".to_string(),
                            base_revision: 1,
                            source_attempt: "producer-team:attempt:0".to_string(),
                            reason: "review the same bounded producer result".to_string(),
                            evidence_refs: Vec::new(),
                            canonical_digest: "e".repeat(64),
                            user_confirmation_ref: None,
                            escalation: None,
                            operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::ChangeEdge {
                                edge_id: "producer:1->consumer:1".to_string(),
                                from_instance_id: "producer:1".to_string(),
                                to_instance_id: "consumer:1".to_string(),
                                edge_kind: CollaborationEdgeKind::ReviewOf,
                                input_contract: Default::default(),
                            },
                        },
                    ),
                },
            )
            .expect("pending edge patch commits atomically")
            .graph;
    let patched_edge = &registered
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("program")
        .edges[0];
    assert_eq!(patched_edge.kind, CollaborationEdgeKind::ReviewOf);
    assert_eq!(
        registered
            .edges
            .iter()
            .filter(|edge| edge.kind.is_dependency())
            .count(),
        0,
        "CrossTeamHandoff is organizational; ArtifactRequires owns physical readiness"
    );
    let ready = service
        .transition_node(
            &registered,
            "producer-team",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .expect("producer ready")
        .graph;
    let running = service
        .transition_node(
            &ready,
            "producer-team",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .expect("producer running")
        .graph;
    let completed = service
        .transition_node(
            &running,
            "producer-team",
            ExecutionNodeStatus::Completed,
            Some(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("artifact://producer-result".to_string()),
                summary: Some("durable producer outcome".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }),
            Vec::new(),
        )
        .expect("producer completed")
        .graph;
    let delivered = completed;
    let edge = &delivered
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("program")
        .edges[0];
    assert_eq!(
        edge.state,
        harness_contract::execution_graph::CrossTeamEdgeState::Delivered
    );
    assert_eq!(
        edge.delivery_receipt
            .as_ref()
            .map(|receipt| receipt.producer_result_ref.as_str()),
        Some("artifact://producer-result")
    );

    let consumer_ready = service
        .transition_node(
            &delivered,
            "consumer-team",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .expect("consumer ready")
        .graph;
    let consumer_running = service
        .transition_node(
            &consumer_ready,
            "consumer-team",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .expect("consumer running")
        .graph;
    let claimed = service
        .apply_command(
            &consumer_running,
            &ExecutionGraphCommand::ClaimCrossTeamEdgeDelivery {
                expected_revision: consumer_running.revision,
                edge_id: "producer:1->consumer:1".to_string(),
                consumer_node_id: "consumer-team".to_string(),
                consumer_attempt: 1,
            },
        )
        .expect("claim commits")
        .graph;
    let edge = &claimed
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("program")
        .edges[0];
    assert_eq!(
        edge.state,
        harness_contract::execution_graph::CrossTeamEdgeState::Claimed
    );
    assert_eq!(
        edge.claim_receipt
            .as_ref()
            .map(|receipt| receipt.consumer_attempt),
        Some(1)
    );
}

#[test]
fn retirement_cancels_only_a_confirmed_unstarted_team_and_revises_program_atomically() {
    use harness_contract::execution_graph::{
        CollaborationEdgeKind, CollaborationProgram, CollaborationProgramEdge,
        CollaborationProgramLifecycle, CollaborationTeamInstance, ExecutionGraphCommand,
        ExecutionOrchestrationMetadata,
    };

    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(store);
    let mut graph = agent_task_graph();
    graph.id = "retire-team-root".to_string();
    let mut consumer = graph.nodes[0].clone();
    consumer.id = "consumer-team".to_string();
    consumer.idempotency_key = "consumer-team-key".to_string();
    graph.nodes[0].id = "producer-team".to_string();
    graph.nodes[0].idempotency_key = "producer-team-key".to_string();
    graph.nodes.push(consumer);
    graph.node_statuses = BTreeMap::from([
        ("producer-team".to_string(), ExecutionNodeStatus::Planned),
        ("consumer-team".to_string(), ExecutionNodeStatus::Planned),
    ]);
    graph.edges = vec![ExecutionEdge {
        from: "producer-team".to_string(),
        to: "consumer-team".to_string(),
        kind: harness_contract::execution_graph::ExecutionEdgeKind::CrossTeamHandoff,
    }];
    graph.orchestration = Some(ExecutionOrchestrationMetadata {
        mutation_id: "retire-team-test".to_string(),
        applied_mutation_ids: Vec::new(),
        collaboration_escalations: Vec::new(),
        semantic_revision: 1,
        source_generation: 1,
        completion: harness_contract::execution_graph::ExecutionCompletionContract {
            required_node_ids: vec!["producer-team".to_string(), "consumer-team".to_string()],
            ..Default::default()
        },
        collaboration_program: Some(CollaborationProgram {
            program_id: "program-retire-team".to_string(),
            revision: 1,
            approval_policy_digest: "sha256:policy".to_string(),
            required_team_count: 2,
            team_instances: vec![
                CollaborationTeamInstance {
                    instance_id: "producer:1".to_string(),
                    semantic_node_id: "producer".to_string(),
                    required: true,
                },
                CollaborationTeamInstance {
                    instance_id: "consumer:1".to_string(),
                    semantic_node_id: "consumer".to_string(),
                    required: true,
                },
            ],
            edges: vec![CollaborationProgramEdge {
                edge_id: "producer:1->consumer:1".to_string(),
                from: "producer:1".to_string(),
                to: "consumer:1".to_string(),
                kind: CollaborationEdgeKind::Handoff,
                input_contract: Default::default(),
                state: Default::default(),
                delivery_receipt: None,
                claim_receipt: None,
            }],
            semantic_node_instances: BTreeMap::from([
                ("producer".to_string(), vec!["producer-team".to_string()]),
                ("consumer".to_string(), vec!["consumer-team".to_string()]),
            ]),
            control: harness_contract::execution_graph::CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Planning,
                ..Default::default()
            },
            semantic_intent: None,
        }),
    });
    let mut started_graph = graph.clone();
    started_graph.id = "retire-team-started-root".to_string();
    let mut active_graph = graph.clone();
    active_graph.id = "retire-team-active-root".to_string();
    let active_program = active_graph
        .orchestration
        .as_mut()
        .and_then(|metadata| metadata.collaboration_program.as_mut())
        .expect("Program");
    active_program.control = harness_contract::execution_graph::CollaborationProgramControlState {
        lifecycle: CollaborationProgramLifecycle::Admitting,
        obligations: vec![
            harness_contract::execution_graph::TeamAdmissionObligation {
                instance_id: "producer:1".to_string(),
                binding_ref: "team-binding:sha256:producer".to_string(),
                state: harness_contract::execution_graph::TeamAdmissionState::Admitting,
                child_graph_ref: None,
                reason_kind: None,
                terminal: None,
                reservation: harness_contract::execution_graph::TeamAdmissionResourceReservation {
                    context_reservation_tokens: 30,
                    output_reservation_tokens: 20,
                    parallel_demand: 1,
                },
                revision: 1,
            },
            harness_contract::execution_graph::TeamAdmissionObligation {
                instance_id: "consumer:1".to_string(),
                binding_ref: "team-binding:sha256:consumer".to_string(),
                state: harness_contract::execution_graph::TeamAdmissionState::Admitting,
                child_graph_ref: None,
                reason_kind: None,
                terminal: None,
                reservation: harness_contract::execution_graph::TeamAdmissionResourceReservation {
                    context_reservation_tokens: 10,
                    output_reservation_tokens: 5,
                    parallel_demand: 1,
                },
                revision: 1,
            },
        ],
        resource_ledger: harness_contract::execution_graph::ProgramResourceLedger {
            context_reservation_tokens: 40,
            output_reservation_tokens: 25,
            parallel_demand: 2,
            deadline_at_ms: 1,
            admitted_at_ms: 1,
            confidence_basis_points: 10_000,
            revision: 1,
            capacity_profile_id: String::new(),
            capacity_profile_revision: 0,
            capacity_profile_digest: String::new(),
            resolved_parallel_ceiling: 0,
        },
        waiting_relation: Some("team_admission".to_string()),
        blocker_ref: None,
        next_action: Some("admit_exact_team_bindings".to_string()),
    };
    let registered = service.register_graph(graph).expect("register graph").graph;
    let started_registered = service
        .register_graph(started_graph)
        .expect("register started graph")
        .graph;
    let active_registered = service
        .register_graph(active_graph)
        .expect("register active graph")
        .graph;
    let patch = |confirmation: Option<&str>| {
        Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                program_id: "program-retire-team".to_string(),
                base_revision: 1,
                source_attempt: "producer-team:attempt:0".to_string(),
                reason: "the bounded consumer branch is no longer required".to_string(),
                evidence_refs: Vec::new(),
                canonical_digest: "r".repeat(64),
                user_confirmation_ref: confirmation.map(str::to_string),
                escalation: None,
                operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::RetireTeam {
                    instance_id: "consumer:1".to_string(),
                },
            })
    };
    let missing_confirmation = service.apply_command(
        &registered,
        &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
            expected_revision: registered.revision,
            patch: patch(None),
        },
    );
    assert!(matches!(
        missing_confirmation,
        Err(ExecutionCommitError::InvalidCommand(message))
            if message.contains("explicit user confirmation")
    ));

    let retired = service
        .apply_command(
            &registered,
            &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
                expected_revision: registered.revision,
                patch: patch(Some("approval:retire-consumer")),
            },
        )
        .expect("confirmed pending Team retires atomically")
        .graph;
    let program = retired
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("program");
    assert_eq!(program.revision, 2);
    assert_eq!(program.required_team_count, 1);
    assert_eq!(program.team_instances.len(), 1);
    assert_eq!(program.team_instances[0].instance_id, "producer:1");
    assert!(program.edges.is_empty());
    assert!(!program.semantic_node_instances.contains_key("consumer"));
    assert_eq!(
        retired.node_statuses["consumer-team"],
        ExecutionNodeStatus::Cancelled
    );
    assert!(retired.edges.is_empty());
    assert_eq!(
        retired
            .orchestration
            .as_ref()
            .expect("metadata")
            .completion
            .required_node_ids,
        vec!["producer-team"]
    );
    assert!(validate_execution_graph(&retired).is_ok());

    let mut topology_graph = registered.clone();
    topology_graph.id = "retire-team-topology-root".to_string();
    let topology_registered = service
        .register_graph(topology_graph)
        .expect("register topology graph")
        .graph;
    let mut replacement = topology_registered.nodes[0].clone();
    replacement.id = "replacement-team".to_string();
    replacement.idempotency_key = "replacement-team-key".to_string();
    let replacement_program = CollaborationProgram {
        program_id: "program-retire-team".to_string(),
        revision: 1,
        approval_policy_digest: "sha256:policy".to_string(),
        required_team_count: 1,
        team_instances: vec![CollaborationTeamInstance {
            instance_id: "replacement:1".to_string(),
            semantic_node_id: "replacement".to_string(),
            required: true,
        }],
        edges: Vec::new(),
        semantic_node_instances: BTreeMap::from([(
            "replacement".to_string(),
            vec!["replacement-team".to_string()],
        )]),
        control: Default::default(),
        semantic_intent: None,
    };
    let replaced = service
        .replan_semantic_with_retirements(
            &topology_registered,
            vec![replacement],
            Vec::new(),
            "split consumer into a replacement workstream".to_string(),
            "replace-consumer-with-replacement".to_string(),
            harness_contract::execution_graph::ExecutionCompletionContract {
                required_node_ids: vec!["replacement-team".to_string()],
                ..Default::default()
            },
            Some(replacement_program),
            None,
            vec!["consumer:1".to_string()],
        )
        .expect("topology replacement commits atomically")
        .graph;
    let replaced_program = replaced
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("Program");
    assert!(replaced_program
        .team_instances
        .iter()
        .all(|instance| instance.instance_id != "consumer:1"));
    assert!(replaced_program
        .semantic_node_instances
        .contains_key("replacement"));
    assert_eq!(
        replaced.node_statuses["consumer-team"],
        ExecutionNodeStatus::Cancelled
    );
    assert_eq!(
        replaced.node_statuses["replacement-team"],
        ExecutionNodeStatus::Planned
    );
    assert!(replaced.edges.is_empty());
    assert_eq!(
        replaced
            .orchestration
            .as_ref()
            .expect("metadata")
            .completion
            .required_node_ids,
        vec!["producer-team".to_string(), "replacement-team".to_string()]
    );
    assert!(validate_execution_graph(&replaced).is_ok());

    let active_retired = service
        .apply_command(
            &active_registered,
            &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
                expected_revision: active_registered.revision,
                patch: patch(Some("approval:retire-consumer")),
            },
        )
        .expect("active Team retirement releases its exact reservation")
        .graph;
    let active_ledger = &active_retired
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("Program")
        .control
        .resource_ledger;
    assert_eq!(active_ledger.context_reservation_tokens, 30);
    assert_eq!(active_ledger.output_reservation_tokens, 20);
    assert_eq!(active_ledger.parallel_demand, 1);

    let started = service
        .transition_node(
            &started_registered,
            "consumer-team",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .expect("consumer becomes ready")
        .graph;
    let started_rejection = service.apply_command(
        &started,
        &ExecutionGraphCommand::ApplyCollaborationTeamRetirement {
            expected_revision: started.revision,
            patch: patch(Some("approval:retire-consumer")),
        },
    );
    assert!(matches!(
        started_rejection,
        Err(ExecutionCommitError::InvalidCommand(message)) if message.contains("requires a planned Team")
    ));
}

#[test]
fn objective_narrowing_rewrites_only_a_planned_team_request_atomically() {
    use harness_contract::execution_graph::{
        CollaborationProgram, CollaborationProgramLifecycle, CollaborationTeamInstance,
        ExecutionGraphCommand, ExecutionOrchestrationMetadata,
    };

    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(store);
    let mut graph = waiting_child_join_graph();
    graph
        .node_statuses
        .insert("child-team".to_string(), ExecutionNodeStatus::Planned);
    graph.node_results.clear();
    graph.orchestration = Some(ExecutionOrchestrationMetadata {
        mutation_id: "narrow-objective-test".to_string(),
        applied_mutation_ids: Vec::new(),
        collaboration_escalations: Vec::new(),
        semantic_revision: 1,
        source_generation: 1,
        completion: Default::default(),
        collaboration_program: Some(CollaborationProgram {
            program_id: "program-narrow-objective".to_string(),
            revision: 1,
            approval_policy_digest: "sha256:policy".to_string(),
            required_team_count: 1,
            team_instances: vec![CollaborationTeamInstance {
                instance_id: "research:1".to_string(),
                semantic_node_id: "research".to_string(),
                required: true,
            }],
            edges: Vec::new(),
            semantic_node_instances: BTreeMap::from([(
                "research".to_string(),
                vec!["child-team".to_string()],
            )]),
            control: harness_contract::execution_graph::CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Planning,
                ..Default::default()
            },
            semantic_intent: None,
        }),
    });
    let registered = service.register_graph(graph).expect("register graph").graph;
    let prioritised = service
            .apply_command(
                &registered,
                &ExecutionGraphCommand::ApplyCollaborationParallelismHint {
                    expected_revision: registered.revision,
                    patch: Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                        program_id: "program-narrow-objective".to_string(),
                        base_revision: 1,
                        source_attempt: "child-team:attempt:0".to_string(),
                        reason: "the independent evidence lane should be scheduled first".to_string(),
                        evidence_refs: Vec::new(),
                        canonical_digest: "p".repeat(64),
                        user_confirmation_ref: None,
                        escalation: None,
                        operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::SetParallelismHint {
                            semantic_node_id: "research".to_string(),
                            parallelism_hint: 200,
                        },
                    }),
                },
            )
            .expect("planned Team soft priority updates atomically")
            .graph;
    assert_eq!(
        prioritised.nodes[0]
            .work
            .as_ref()
            .expect("Team has a work contract")
            .scheduling_priority,
        200
    );
    assert_eq!(
        prioritised.node_statuses["child-team"],
        ExecutionNodeStatus::Planned
    );
    assert_eq!(
        prioritised
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("Program")
            .control
            .resource_ledger
            .parallel_demand,
        0,
        "soft priority must not become a resource reservation"
    );
    let reprioritised = service
            .apply_command(
                &prioritised,
                &ExecutionGraphCommand::ApplyCollaborationParallelismHint {
                    expected_revision: prioritised.revision,
                    patch: Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                        program_id: "program-narrow-objective".to_string(),
                        base_revision: 2,
                        source_attempt: "child-team:attempt:0".to_string(),
                        reason: "the verified evidence lane is now urgent".to_string(),
                        evidence_refs: Vec::new(),
                        canonical_digest: "r".repeat(64),
                        user_confirmation_ref: None,
                        escalation: None,
                        operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::Reprioritize {
                            semantic_node_id: "research".to_string(),
                            priority: 240,
                        },
                    }),
                },
            )
            .expect("planned Team reprioritizes atomically")
            .graph;
    assert_eq!(
        reprioritised.nodes[0]
            .work
            .as_ref()
            .expect("Team has a work contract")
            .scheduling_priority,
        240
    );
    let patch = harness_contract::execution_graph::CollaborationIntentPatch {
        program_id: "program-narrow-objective".to_string(),
        base_revision: 3,
        source_attempt: "child-team:attempt:0".to_string(),
        reason: "the user constrained this branch to a single source".to_string(),
        evidence_refs: Vec::new(),
        canonical_digest: "n".repeat(64),
        user_confirmation_ref: None,
        escalation: None,
        operation:
            harness_contract::execution_graph::CollaborationIntentPatchOperation::NarrowObjective {
                semantic_node_id: "research".to_string(),
                objective: "inspect only the declared source and report its evidence".to_string(),
            },
    };
    let narrowed = service
        .apply_command(
            &reprioritised,
            &ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing {
                expected_revision: reprioritised.revision,
                patch: Box::new(patch),
            },
        )
        .expect("planned Team objective narrows atomically")
        .graph;
    let request = serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
        &narrowed.nodes[0].payload_ref,
    )
    .expect("Team request stays decodable");
    assert_eq!(
        request.objective,
        "inspect only the declared source and report its evidence"
    );
    assert_eq!(
        narrowed
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("program")
            .revision,
        4
    );
    assert_eq!(
        narrowed.node_statuses["child-team"],
        ExecutionNodeStatus::Planned
    );
    let expanded = service
            .apply_command(
                &narrowed,
                &ExecutionGraphCommand::ApplyCollaborationObjectiveNarrowing {
                    expected_revision: narrowed.revision,
                    patch: Box::new(harness_contract::execution_graph::CollaborationIntentPatch {
                        program_id: "program-narrow-objective".to_string(),
                        base_revision: 4,
                        source_attempt: "child-team:attempt:0".to_string(),
                        reason: "the user approved comparison of one additional source".to_string(),
                        evidence_refs: Vec::new(),
                        canonical_digest: "x".repeat(64),
                        user_confirmation_ref: Some("approval:objective-expand".to_string()),
                        escalation: None,
                        operation: harness_contract::execution_graph::CollaborationIntentPatchOperation::ExpandObjective {
                            semantic_node_id: "research".to_string(),
                            objective: "compare the declared source with the approved second source".to_string(),
                        },
                    }),
                },
            )
            .expect("confirmed objective expansion preserves the same Team contract")
            .graph;
    let expanded_request =
        serde_json::from_str::<harness_contract::team::TeamInstantiationRequest>(
            &expanded.nodes[0].payload_ref,
        )
        .expect("expanded Team request stays decodable");
    assert_eq!(
        expanded_request.objective,
        "compare the declared source with the approved second source"
    );
    assert_eq!(
        expanded
            .orchestration
            .as_ref()
            .expect("metadata")
            .collaboration_program
            .as_ref()
            .expect("program")
            .revision,
        5
    );
    assert!(validate_execution_graph(&narrowed).is_ok());
    assert!(validate_execution_graph(&expanded).is_ok());
}

#[test]
fn terminal_producer_without_required_cross_team_facts_blocks_edge_durably() {
    use harness_contract::acceptance::TerminalFactKind;
    use harness_contract::execution_graph::{
        CollaborationEdgeKind, CollaborationProgram, CollaborationProgramEdge,
        CollaborationProgramLifecycle, CollaborationTeamInstance, CrossTeamInputContract,
        ExecutionOrchestrationMetadata,
    };

    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(store);
    let mut graph = agent_task_graph();
    graph.id = "cross-team-blocked-root".to_string();
    let mut consumer = graph.nodes[0].clone();
    consumer.id = "consumer-team".to_string();
    consumer.idempotency_key = "consumer-team-key".to_string();
    graph.nodes[0].id = "producer-team".to_string();
    graph.nodes[0].idempotency_key = "producer-team-key".to_string();
    graph.nodes.push(consumer);
    graph.node_statuses = BTreeMap::from([
        ("producer-team".to_string(), ExecutionNodeStatus::Planned),
        ("consumer-team".to_string(), ExecutionNodeStatus::Planned),
    ]);
    graph.orchestration = Some(ExecutionOrchestrationMetadata {
        mutation_id: "cross-team-blocked-test".to_string(),
        applied_mutation_ids: Vec::new(),
        collaboration_escalations: Vec::new(),
        semantic_revision: 1,
        source_generation: 1,
        completion: Default::default(),
        collaboration_program: Some(CollaborationProgram {
            program_id: "program-cross-team-blocked".to_string(),
            revision: 1,
            approval_policy_digest: "sha256:policy".to_string(),
            required_team_count: 2,
            team_instances: vec![
                CollaborationTeamInstance {
                    instance_id: "producer:1".to_string(),
                    semantic_node_id: "producer".to_string(),
                    required: true,
                },
                CollaborationTeamInstance {
                    instance_id: "consumer:1".to_string(),
                    semantic_node_id: "consumer".to_string(),
                    required: true,
                },
            ],
            edges: vec![CollaborationProgramEdge {
                edge_id: "producer:1->consumer:1".to_string(),
                from: "producer:1".to_string(),
                to: "consumer:1".to_string(),
                kind: CollaborationEdgeKind::Handoff,
                input_contract: CrossTeamInputContract {
                    required_artifact_kinds: Vec::new(),
                    required_fact_kinds: vec![TerminalFactKind::Artifact],
                    require_committed_effect: false,
                    require_satisfied_acceptance: false,
                },
                state: Default::default(),
                delivery_receipt: None,
                claim_receipt: None,
            }],
            semantic_node_instances: BTreeMap::from([
                ("producer".to_string(), vec!["producer-team".to_string()]),
                ("consumer".to_string(), vec!["consumer-team".to_string()]),
            ]),
            control: harness_contract::execution_graph::CollaborationProgramControlState {
                lifecycle: CollaborationProgramLifecycle::Planning,
                ..Default::default()
            },
            semantic_intent: None,
        }),
    });
    let registered = service.register_graph(graph).expect("register graph").graph;
    let ready = service
        .transition_node(
            &registered,
            "producer-team",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .expect("producer ready")
        .graph;
    let running = service
        .transition_node(
            &ready,
            "producer-team",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .expect("producer running")
        .graph;
    let completed = service
        .transition_node(
            &running,
            "producer-team",
            ExecutionNodeStatus::Completed,
            Some(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("artifact://producer-result".to_string()),
                summary: Some("producer omitted the required artifact fact".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 1,
            }),
            Vec::new(),
        )
        .expect("producer completed")
        .graph;
    let edge = &completed
        .orchestration
        .as_ref()
        .expect("metadata")
        .collaboration_program
        .as_ref()
        .expect("program")
        .edges[0];
    assert_eq!(
        edge.state,
        harness_contract::execution_graph::CrossTeamEdgeState::Blocked
    );
    assert!(edge.delivery_receipt.is_none());
    assert!(edge.claim_receipt.is_none());
}

#[test]
fn scoped_cancel_changes_only_the_authorized_node() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(store);
    let mut graph = agent_task_graph();
    let mut peer = ExecutionNodeSpec::new(ExecutionNodeKind::AgentTask, "agent", "peer-payload");
    peer.id = "peer-agent-node".to_string();
    peer.idempotency_key = "peer-agent-node-idempotency".to_string();
    graph.nodes.push(peer);
    let registered = service.register_graph(graph).expect("register graph").graph;
    let cancelled = service
        .apply_command(
            &registered,
            &ExecutionGraphCommand::CancelNode {
                expected_revision: registered.revision,
                node_id: "agent-node".to_string(),
                reason: "cancel one Team lane".to_string(),
            },
        )
        .expect("scoped cancel commits")
        .graph;
    assert_eq!(
        cancelled.node_statuses["agent-node"],
        ExecutionNodeStatus::Cancelled
    );
    assert_eq!(
        cancelled.node_statuses["peer-agent-node"],
        ExecutionNodeStatus::Planned
    );
}

#[test]
fn typed_child_receipt_preserves_failure_evidence_and_usage_atomically() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let registered = register_waiting_child_join(&service);
    let evidence = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("child_result", "child-proof"),
        "a".repeat(64),
        1,
        "application/json",
        "artifact://child-proof",
        "mission:mission",
    );
    let mut result = ExecutionNodeResult {
        status: ExecutionNodeStatus::Failed,
        result_ref: Some("artifact://child-failure".to_string()),
        summary: Some("child failed after producing evidence".to_string()),
        evidence_refs: vec![evidence.clone()],
        failure: Some(harness_contract::execution_graph::ExecutionFailure {
            kind: "child_failure".to_string(),
            message: "bounded fixture failure".to_string(),
            retryable: false,
            evidence_refs: vec![evidence],
        }),
        usage: Default::default(),
        finished_at_ms: 2,
    };
    result.usage.input_tokens = 11;
    result.usage.output_tokens = 7;
    let child_revision = 9;
    let correlation = super::super::runner::child_resolution_correlation(
        &registered.id,
        "child-team",
        "team-graph:team-child",
        1,
        child_revision,
    );
    let committed = service
        .apply_command(
            &registered,
            &ExecutionGraphCommand::ResolveChildExecution {
                expected_revision: registered.revision,
                receipt: Box::new(
                    harness_contract::execution_graph::ChildExecutionTerminalReceipt {
                        parent_execution_id: registered.id.clone(),
                        parent_node_id: "child-team".to_string(),
                        parent_attempt: 1,
                        child_execution_id: "team-graph:team-child".to_string(),
                        child_revision,
                        result: result.clone(),
                        correlation_id: correlation.clone(),
                    },
                ),
            },
        )
        .expect("resolve child")
        .graph;
    assert_eq!(
        committed.node_statuses["child-team"],
        ExecutionNodeStatus::Failed
    );
    assert_eq!(committed.node_results["child-team"], result);
    let lineage = store
        .list_stream("execution-lineage:parent-graph")
        .expect("lineage stream");
    assert!(lineage.iter().any(|event| {
        event.kind == "execution.lineage.child_terminal.v1"
            && event.payload["correlation_id"] == correlation
    }));
}

#[test]
fn typed_child_receipt_fails_closed_for_wrong_attempt_child_or_correlation() {
    let store = Arc::new(RuntimeEventStore::try_open_in_memory().unwrap());
    let service = ExecutionCommitService::new(store);
    let registered = register_waiting_child_join(&service);
    let base = harness_contract::execution_graph::ChildExecutionTerminalReceipt {
        parent_execution_id: registered.id.clone(),
        parent_node_id: "child-team".to_string(),
        parent_attempt: 1,
        child_execution_id: "team-graph:team-child".to_string(),
        child_revision: 2,
        result: ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: Some("assistant_json:done".to_string()),
            summary: Some("done".to_string()),
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 2,
        },
        correlation_id: super::super::runner::child_resolution_correlation(
            &registered.id,
            "child-team",
            "team-graph:team-child",
            1,
            2,
        ),
    };
    let mut wrong_attempt = base;
    wrong_attempt.parent_attempt = 2;
    let error = match service.apply_command(
        &registered,
        &ExecutionGraphCommand::ResolveChildExecution {
            expected_revision: registered.revision,
            receipt: Box::new(wrong_attempt),
        },
    ) {
        Ok(_) => panic!("mismatched attempt must fail closed"),
        Err(error) => error,
    };
    assert!(matches!(error, ExecutionCommitError::InvalidCommand(_)));

    let mut wrong_child = harness_contract::execution_graph::ChildExecutionTerminalReceipt {
        parent_execution_id: registered.id.clone(),
        parent_node_id: "child-team".to_string(),
        parent_attempt: 1,
        child_execution_id: "team-graph:wrong".to_string(),
        child_revision: 2,
        result: ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: None,
            summary: None,
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 2,
        },
        correlation_id: String::new(),
    };
    wrong_child.correlation_id = super::super::runner::child_resolution_correlation(
        &registered.id,
        "child-team",
        &wrong_child.child_execution_id,
        1,
        2,
    );
    assert!(matches!(
        service.apply_command(
            &registered,
            &ExecutionGraphCommand::ResolveChildExecution {
                expected_revision: registered.revision,
                receipt: Box::new(wrong_child),
            },
        ),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));

    let mut wrong_correlation = harness_contract::execution_graph::ChildExecutionTerminalReceipt {
        parent_execution_id: registered.id.clone(),
        parent_node_id: "child-team".to_string(),
        parent_attempt: 1,
        child_execution_id: "team-graph:team-child".to_string(),
        child_revision: 2,
        result: ExecutionNodeResult {
            status: ExecutionNodeStatus::Completed,
            result_ref: None,
            summary: None,
            evidence_refs: Vec::new(),
            failure: None,
            usage: Default::default(),
            finished_at_ms: 2,
        },
        correlation_id: "wrong".to_string(),
    };
    wrong_correlation.correlation_id.push_str("-correlation");
    assert!(matches!(
        service.apply_command(
            &registered,
            &ExecutionGraphCommand::ResolveChildExecution {
                expected_revision: registered.revision,
                receipt: Box::new(wrong_correlation),
            },
        ),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));
}

#[test]
fn work_marketplace_claims_are_cas_fenced_lease_bound_and_replayable() {
    use harness_contract::execution_graph::{
        ExecutionGraphCommand, ExecutionWorkContract, ExecutionWorkEligibility, ExecutionWorkRole,
        ExecutionWorkRuntimeStatus,
    };

    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let mut graph = agent_task_graph();
    graph.nodes[0].work = Some(ExecutionWorkContract {
        collaboration_work_id: Some("research:evidence-lane".to_string()),
        eligibility: ExecutionWorkEligibility {
            allowed_role_ids: vec!["researcher".to_string()],
            required_capabilities: vec!["web_research".to_string()],
            ..Default::default()
        },
        output_artifact_kinds: vec!["research_note".to_string()],
        review_policy: harness_contract::execution_graph::ExecutionWorkReviewPolicy::Peer {
            minimum_reviewers: 2,
            eligible_role_ids: Vec::new(),
        },
        ..ExecutionWorkContract::new(ExecutionWorkRole::EvidenceAnalyze)
    });
    let registered = service.register_graph(graph).expect("register graph").graph;

    let claim_a = service
        .apply_command(
            &registered,
            &ExecutionGraphCommand::ClaimWork {
                expected_revision: registered.revision,
                node_id: "agent-node".to_string(),
                claimant_instance_id: "agent-a".to_string(),
                claimant_role_id: Some("researcher".to_string()),
                claimant_capabilities: vec!["web_research".to_string()],
                claimed_at_ms: 100,
                lease_expires_at_ms: 200,
            },
        )
        .expect("eligible claimant owns offered work")
        .graph;
    let token_a = claim_a.work_states["agent-node"]
        .claim
        .as_ref()
        .expect("claim")
        .claim_token
        .clone();
    assert!(matches!(
        service.apply_command(
            &claim_a,
            &ExecutionGraphCommand::HeartbeatWork {
                expected_revision: claim_a.revision,
                node_id: "agent-node".to_string(),
                claim_token: "forged".to_string(),
                heartbeat_at_ms: 120,
                lease_expires_at_ms: 220,
            }
        ),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));

    let concurrent_claim = service.apply_command(
        &registered,
        &ExecutionGraphCommand::ClaimWork {
            expected_revision: registered.revision,
            node_id: "agent-node".to_string(),
            claimant_instance_id: "agent-b".to_string(),
            claimant_role_id: Some("researcher".to_string()),
            claimant_capabilities: vec!["web_research".to_string()],
            claimed_at_ms: 110,
            lease_expires_at_ms: 210,
        },
    );
    assert!(matches!(
        concurrent_claim,
        Err(ExecutionCommitError::StaleRevision { .. }) | Err(ExecutionCommitError::EventStore(_))
    ));
    let after_race = super::super::state_store::ExecutionGraphStateStore::new(Arc::clone(&store))
        .load(&registered.id)
        .expect("load winning claim");
    assert_eq!(
        after_race.work_states["agent-node"]
            .claim
            .as_ref()
            .expect("winning claim")
            .claimant_instance_id,
        "agent-a"
    );
    assert!(matches!(
        service.apply_command(
            &claim_a,
            &ExecutionGraphCommand::ClaimWork {
                expected_revision: claim_a.revision,
                node_id: "agent-node".to_string(),
                claimant_instance_id: "agent-b".to_string(),
                claimant_role_id: Some("researcher".to_string()),
                claimant_capabilities: vec!["web_research".to_string()],
                claimed_at_ms: 150,
                lease_expires_at_ms: 250,
            }
        ),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));

    let claim_b = service
        .apply_command(
            &claim_a,
            &ExecutionGraphCommand::ClaimWork {
                expected_revision: claim_a.revision,
                node_id: "agent-node".to_string(),
                claimant_instance_id: "agent-b".to_string(),
                claimant_role_id: Some("researcher".to_string()),
                claimant_capabilities: vec!["web_research".to_string()],
                claimed_at_ms: 200,
                lease_expires_at_ms: 300,
            },
        )
        .expect("expired claim can be reclaimed")
        .graph;
    let token_b = claim_b.work_states["agent-node"]
        .claim
        .as_ref()
        .expect("claim")
        .claim_token
        .clone();
    assert_ne!(token_a, token_b);
    assert!(matches!(
        service.apply_command(
            &claim_b,
            &ExecutionGraphCommand::ReleaseWork {
                expected_revision: claim_b.revision,
                node_id: "agent-node".to_string(),
                claim_token: token_a,
                reason: "late worker".to_string(),
            }
        ),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));

    let submitted = service
        .apply_command(
            &claim_b,
            &ExecutionGraphCommand::SubmitWork {
                expected_revision: claim_b.revision,
                node_id: "agent-node".to_string(),
                claim_token: token_b,
                submitted_at_ms: 250,
                submission_ref: "artifact://research-note".to_string(),
            },
        )
        .expect("lease owner submits")
        .graph;
    assert!(matches!(
        service.apply_command(
            &submitted,
            &ExecutionGraphCommand::AcceptWork {
                expected_revision: submitted.revision,
                node_id: "agent-node".to_string(),
                reviewer_instance_id: "agent-b".to_string(),
                reviewer_role_id: Some("researcher".to_string()),
                reviewed_at_ms: 260,
            }
        ),
        Err(ExecutionCommitError::InvalidCommand(_))
    ));
    let challenged = service
        .apply_command(
            &submitted,
            &ExecutionGraphCommand::ChallengeWork {
                expected_revision: submitted.revision,
                node_id: "agent-node".to_string(),
                reviewer_instance_id: "reviewer".to_string(),
                reviewer_role_id: Some("reviewer".to_string()),
                finding: "source coverage is incomplete".to_string(),
                reviewed_at_ms: 270,
            },
        )
        .expect("independent reviewer challenges")
        .graph;
    assert_eq!(
        challenged.work_states["agent-node"].status,
        ExecutionWorkRuntimeStatus::Challenged
    );

    let claim_c = service
        .apply_command(
            &challenged,
            &ExecutionGraphCommand::ClaimWork {
                expected_revision: challenged.revision,
                node_id: "agent-node".to_string(),
                claimant_instance_id: "agent-c".to_string(),
                claimant_role_id: Some("researcher".to_string()),
                claimant_capabilities: vec!["web_research".to_string()],
                claimed_at_ms: 310,
                lease_expires_at_ms: 410,
            },
        )
        .expect("challenged work returns to marketplace")
        .graph;
    let token_c = claim_c.work_states["agent-node"]
        .claim
        .as_ref()
        .expect("claim")
        .claim_token
        .clone();
    let resubmitted = service
        .apply_command(
            &claim_c,
            &ExecutionGraphCommand::SubmitWork {
                expected_revision: claim_c.revision,
                node_id: "agent-node".to_string(),
                claim_token: token_c,
                submitted_at_ms: 350,
                submission_ref: "artifact://research-note-v2".to_string(),
            },
        )
        .expect("reworked submission")
        .graph;
    let first_review = service
        .apply_command(
            &resubmitted,
            &ExecutionGraphCommand::AcceptWork {
                expected_revision: resubmitted.revision,
                node_id: "agent-node".to_string(),
                reviewer_instance_id: "reviewer".to_string(),
                reviewer_role_id: Some("reviewer".to_string()),
                reviewed_at_ms: 360,
            },
        )
        .expect("independent acceptance")
        .graph;
    assert_eq!(
        first_review.work_states["agent-node"].status,
        ExecutionWorkRuntimeStatus::Submitted
    );
    let accepted = service
        .apply_command(
            &first_review,
            &ExecutionGraphCommand::AcceptWork {
                expected_revision: first_review.revision,
                node_id: "agent-node".to_string(),
                reviewer_instance_id: "reviewer-2".to_string(),
                reviewer_role_id: Some("reviewer".to_string()),
                reviewed_at_ms: 370,
            },
        )
        .expect("minimum distinct reviewer threshold accepts")
        .graph;
    assert_eq!(accepted.work_states["agent-node"].reviews.len(), 3);
    assert_eq!(
        accepted.work_states["agent-node"].status,
        ExecutionWorkRuntimeStatus::Accepted
    );

    let replayed = super::super::state_store::ExecutionGraphStateStore::new(store)
        .load(&accepted.id)
        .expect("replay graph");
    assert_eq!(replayed.work_states, accepted.work_states);
}

#[test]
fn completed_physical_work_reconciles_to_runtime_accepted_without_peer_review() {
    use harness_contract::execution_graph::{
        ExecutionGraphCommand, ExecutionWorkContract, ExecutionWorkRole, ExecutionWorkRuntimeStatus,
    };

    let store = Arc::new(RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(store);
    let mut graph = agent_task_graph();
    graph.nodes[0].work = Some(ExecutionWorkContract::new(
        ExecutionWorkRole::EvidenceAnalyze,
    ));
    let graph = service.register_graph(graph).expect("register").graph;
    let graph = service
        .apply_command(
            &graph,
            &ExecutionGraphCommand::ClaimWork {
                expected_revision: graph.revision,
                node_id: "agent-node".to_string(),
                claimant_instance_id: "agent-instance".to_string(),
                claimant_role_id: None,
                claimant_capabilities: Vec::new(),
                claimed_at_ms: 10,
                lease_expires_at_ms: 10_000,
            },
        )
        .expect("claim")
        .graph;
    let graph = service
        .transition_node(
            &graph,
            "agent-node",
            ExecutionNodeStatus::Ready,
            None,
            Vec::new(),
        )
        .expect("ready")
        .graph;
    let graph = service
        .transition_node(
            &graph,
            "agent-node",
            ExecutionNodeStatus::Running,
            None,
            Vec::new(),
        )
        .expect("running")
        .graph;
    let graph = service
        .transition_node(
            &graph,
            "agent-node",
            ExecutionNodeStatus::Completed,
            Some(ExecutionNodeResult {
                status: ExecutionNodeStatus::Completed,
                result_ref: Some("artifact://agent-result".to_string()),
                summary: Some("bounded result".to_string()),
                evidence_refs: Vec::new(),
                failure: None,
                usage: Default::default(),
                finished_at_ms: 100,
            }),
            Vec::new(),
        )
        .expect("completed")
        .graph;
    let state = &graph.work_states["agent-node"];
    assert_eq!(state.status, ExecutionWorkRuntimeStatus::Accepted);
    assert_eq!(
        state.submission_ref.as_deref(),
        Some("artifact://agent-result")
    );
}

#[test]
fn legacy_graph_without_work_runtime_state_decodes_as_implicit_offer() {
    let mut graph = agent_task_graph();
    graph.nodes[0].work = Some(
        harness_contract::execution_graph::ExecutionWorkContract::new(
            harness_contract::execution_graph::ExecutionWorkRole::EvidenceAnalyze,
        ),
    );
    let mut encoded = serde_json::to_value(graph).expect("encode graph");
    encoded
        .as_object_mut()
        .expect("graph object")
        .remove("work_states");
    let decoded: ExecutionGraph = serde_json::from_value(encoded).expect("legacy graph decodes");
    assert!(decoded.work_states.is_empty());
    let projection = harness_contract::execution_graph::project_execution_graph(&decoded);
    assert_eq!(
        projection.nodes[0]
            .work_state
            .as_ref()
            .expect("implicit offer")
            .status,
        harness_contract::execution_graph::ExecutionWorkRuntimeStatus::Offered
    );
}
