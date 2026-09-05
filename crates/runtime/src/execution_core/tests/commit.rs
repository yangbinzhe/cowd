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
        cohort_prompt_package: None,
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

#[test]
fn planned_graph_and_continuation_claim_commit_in_one_transaction() {
    let store = Arc::new(crate::RuntimeEventStore::try_open_in_memory().expect("store"));
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let candidate = crate::session_continuation::ContinuationCandidate {
        source_session_id: "session".to_string(),
        source_turn_id: "turn-previous".to_string(),
        source_root_id: "root-previous".to_string(),
        team_set_ref: "team_graph:team-previous".to_string(),
        delivery_revision: 9,
        result_refs: vec!["team_graph:team-previous".to_string()],
        handoff_id: None,
    };
    let binding = crate::session_continuation::compile_continuation_binding(
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
