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
        reality_context: None,
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
        agentic_binding: None,
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
    let store = Arc::new(crate::RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let candidate = crate::session_continuation::ContinuationCandidate {
        source_session_id: "session".to_string(),
        source_turn_id: "turn-previous".to_string(),
        source_root_id: "root-previous".to_string(),
        team_set_ref: "agentic_program:program-previous".to_string(),
        delivery_revision: 9,
        result_refs: vec!["agentic_program:program-previous".to_string()],
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
    let store = Arc::new(RuntimeEventStore::for_test());
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
    let store = Arc::new(RuntimeEventStore::for_test());
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
    let store = Arc::new(RuntimeEventStore::for_test());
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
    let store = Arc::new(RuntimeEventStore::for_test());
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
    let store = Arc::new(RuntimeEventStore::for_test());
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
fn agentic_child_registration_and_parent_cancel_have_one_atomic_order() {
    for iteration in 0..16 {
        let store = Arc::new(RuntimeEventStore::for_test());
        let commits = ExecutionCommitService::new(Arc::clone(&store));
        let mut root = ExecutionGraph::new("parent cancellation race");
        crate::test_support::attach_execution_graph_lineage(&mut root);
        let node = harness_contract::execution_graph::ExecutionNodeSpec::new(
            ExecutionNodeKind::InlineModel,
            "inline_model",
            "test",
        );
        let parent_node = node.id.clone();
        root.nodes.push(node);
        let root = commits.register_graph(root).unwrap().graph;
        let mut child = ExecutionGraph::new("Agentic child");
        child.lineage = root.lineage.clone();
        child.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: root.id.clone(),
            node_id: parent_node,
        });
        let mut agent = harness_contract::execution_graph::ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            "test",
        );
        agent.resource_scopes.push("program:cancel-race".into());
        child.nodes.push(agent);
        let child_id = child.id.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let worker = {
            let commits = commits.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                barrier.wait();
                commits.register_graph(child)
            })
        };
        barrier.wait();
        commits
            .apply_command(
                &root,
                &ExecutionGraphCommand::Cancel {
                    expected_revision: root.revision,
                    reason: format!("cancel race {iteration}"),
                },
            )
            .unwrap();
        let admitted = worker.join().unwrap();
        let graphs = crate::ExecutionGraphStateStore::new(Arc::clone(&store));
        if admitted.is_ok() {
            assert!(
                graphs
                    .child_links(&root.id)
                    .unwrap()
                    .iter()
                    .any(|link| link.child_execution_id == child_id),
                "child admitted before cancel remains visible to the post-cancel tree scan"
            );
        } else {
            assert!(
                store.list_stream(&child_id).unwrap().is_empty(),
                "losing child cannot leave an orphan graph"
            );
        }
        let mut late = ExecutionGraph::new("late Agentic child");
        late.lineage = root.lineage.clone();
        late.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: root.id.clone(),
            node_id: root.nodes[0].id.clone(),
        });
        let mut agent = harness_contract::execution_graph::ExecutionNodeSpec::new(
            ExecutionNodeKind::AgentTask,
            "agent_task",
            "test",
        );
        agent.resource_scopes.push("program:cancel-race".into());
        late.nodes.push(agent);
        assert!(commits.register_graph(late).is_err());
    }
}

#[test]
fn delegated_effect_intent_is_atomic_with_index_and_current_graph_authority() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(store.clone());
    let mut graph = agent_task_graph();
    // An ordinary graph ToolBatch has the same fresh-effect admission owner;
    // the real Agent claim and Process path is covered by execution tests.
    graph.nodes[0].kind = ExecutionNodeKind::ToolBatch;
    graph.nodes[0].executor_kind = "tool".into();
    let graph = service.register_graph(graph).unwrap().graph;
    let mut request = request("indexed-write");
    request.session_id = Some(graph.lineage.as_ref().unwrap().session_id.clone());
    request.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: graph.id.clone(),
        node_id: graph.nodes[0].id.clone(),
    });
    request.parent_execution_attempt = Some(1);
    let effect = mutation_effect(ToolIdempotency::Idempotent);
    let index = delegated_agent_receipt_stream_id(&request).unwrap();
    assert!(service.begin_tool_effect(&request, &effect).is_err());
    assert_eq!(store.stream_revision(&index).unwrap(), 0);
    let graph = service
        .transition_node(
            &graph,
            &graph.nodes[0].id,
            ExecutionNodeStatus::Ready,
            None,
            vec![],
        )
        .unwrap()
        .graph;
    let graph = service
        .transition_node(
            &graph,
            &graph.nodes[0].id,
            ExecutionNodeStatus::Running,
            None,
            vec![],
        )
        .unwrap()
        .graph;
    assert_eq!(
        service.begin_tool_effect(&request, &effect).unwrap(),
        ToolEffectState::Fresh
    );
    let intent = store.list_stream(&index).unwrap();
    assert_eq!(intent.len(), 1);
    assert_eq!(intent[0].kind, "execution.agent_tool.intent");
    let program = crate::AgenticProgramProjection::empty("program", "objective");
    let snapshot = || {
        crate::agentic::review_evidence::effect_review_snapshot(
            &store,
            &program,
            None,
            &[index.clone()],
        )
    };
    assert!(snapshot().err().unwrap().contains("effect_review_pending"));
    assert!(service
        .load_delegated_agent_tool_receipts(&graph.id, &graph.nodes[0].id, 1)
        .unwrap()
        .is_empty());
    service
        .commit_tool_effect(&request, &effect, &outcome("indexed-write", "written"))
        .unwrap();
    let before = snapshot().unwrap();
    assert_eq!(
        service
            .load_delegated_agent_tool_receipts(&graph.id, &graph.nodes[0].id, 1)
            .unwrap()
            .len(),
        1
    );
    let mut pending = request.clone();
    pending.idempotency_key = "pending-after-review".into();
    pending.tool_use_id = "pending-after-review".into();
    pending.observation_wave_sequence = 2;
    assert_eq!(
        service.begin_tool_effect(&pending, &effect).unwrap(),
        ToolEffectState::Fresh
    );
    assert!(snapshot().err().unwrap().contains("effect_review_pending"));
    // A conclusion prepared before the second write cannot commit even its
    // unrelated terminal stream if the original receipt source changed.
    assert!(store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "negative-stale-effect-conclusion".into(),
            expected_streams: before.additional_sources,
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: "negative-terminal".into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "test.negative_terminal".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: json!({}),
                },
                idempotency_key: Some("negative-terminal".into()),
                schema_version: 1,
            }],
        })
        .is_err());
    assert_eq!(store.stream_revision("negative-terminal").unwrap(), 0);
    service
        .apply_command(
            &graph,
            &ExecutionGraphCommand::CancelNode {
                expected_revision: graph.revision,
                node_id: graph.nodes[0].id.clone(),
                reason: "cancel current producer".into(),
            },
        )
        .unwrap();
    let revision = store.stream_revision(&index).unwrap();
    assert!(
        service.begin_tool_effect(&pending, &effect).is_err(),
        "idempotent retries still require current authority"
    );
    let mut fresh = pending.clone();
    fresh.idempotency_key = "new-after-cancel".into();
    assert!(service.begin_tool_effect(&fresh, &effect).is_err());
    assert_eq!(store.stream_revision(&index).unwrap(), revision);
    assert!(
        matches!(
            service.begin_tool_effect(&request, &effect).unwrap(),
            ToolEffectState::Completed(_)
        ),
        "committed replay needs no fresh effect authority"
    );
    // Cancellation does not erase an already admitted external effect. Its
    // original receipt may still settle and make the retained source readable.
    service
        .commit_tool_effect(
            &pending,
            &effect,
            &outcome("pending-after-review", "settled"),
        )
        .unwrap();
    assert_ne!(snapshot().unwrap().digest, before.digest);
}

#[test]
fn root_effect_fences_lineage_cancellation_and_replay_without_losing_admitted_receipts() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(store.clone());
    let mut graph = agent_task_graph();
    graph.nodes[0].kind = ExecutionNodeKind::ToolBatch;
    graph.nodes[0].executor_kind = "tool".into();
    let mut graph = service.register_graph(graph).unwrap().graph;
    let node = graph.nodes[0].id.clone();
    let lineage = graph.lineage.as_ref().unwrap();
    let context = crate::CowdExecutionContext {
        execution_id: graph.id.clone(),
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
    };
    let mut write = request("root-write");
    write.session_id = Some(context.session_id.clone());
    let effect = mutation_effect(ToolIdempotency::Idempotent);
    assert!(service
        .begin_root_tool_effect(&write, &effect, &context)
        .is_err());
    for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
        graph = service
            .transition_node(&graph, &node, status, None, vec![])
            .unwrap()
            .graph;
    }
    let mut stale = context.clone();
    stale.turn_id = "old-turn".into();
    assert!(service
        .begin_root_tool_effect(&write, &effect, &stale)
        .is_err());
    assert_eq!(
        store
            .stream_revision("execution-effect:idem-root-write")
            .unwrap(),
        0
    );
    let sources = service.root_effect_authority(&context).unwrap();
    assert_eq!(sources.len(), 3);
    assert_eq!(
        sources
            .iter()
            .filter(|source| source.expected_revision == 0)
            .count(),
        2,
        "absence of Goal and Program is included in the source CAS"
    );
    assert_eq!(
        service
            .begin_root_tool_effect(&write, &effect, &context)
            .unwrap(),
        ToolEffectState::Fresh
    );
    service
        .commit_tool_effect(&write, &effect, &outcome("root-write", "written"))
        .unwrap();
    let mut pending = request("root-pending");
    pending.session_id = write.session_id.clone();
    assert_eq!(
        service
            .begin_root_tool_effect(&pending, &effect, &context)
            .unwrap(),
        ToolEffectState::Fresh
    );
    service
        .apply_command(
            &graph,
            &ExecutionGraphCommand::Cancel {
                expected_revision: graph.revision,
                reason: "cancel Root".into(),
            },
        )
        .unwrap();
    assert!(service
        .begin_root_tool_effect(&pending, &effect, &context)
        .is_err());
    let mut late = request("root-late");
    late.session_id = write.session_id.clone();
    assert!(service
        .begin_root_tool_effect(&late, &effect, &context)
        .is_err());
    assert_eq!(
        store
            .stream_revision("execution-effect:idem-root-late")
            .unwrap(),
        0
    );
    assert!(matches!(
        service
            .begin_root_tool_effect(&write, &effect, &context)
            .unwrap(),
        ToolEffectState::Completed(_)
    ));
    service
        .commit_tool_effect(
            &pending,
            &effect,
            &outcome("root-pending", "settled after cancel"),
        )
        .unwrap();
    assert!(matches!(
        service
            .begin_root_tool_effect(&pending, &effect, &context)
            .unwrap(),
        ToolEffectState::Completed(_)
    ));
}

#[test]
fn root_effect_goal_states_and_absent_source_revisions_are_enforced() {
    use harness_contract::goal::GoalCompletion;
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(store.clone());
    let mut graph = agent_task_graph();
    graph.nodes[0].kind = ExecutionNodeKind::ToolBatch;
    graph.nodes[0].executor_kind = "tool".into();
    let mut graph = service.register_graph(graph).unwrap().graph;
    let node = graph.nodes[0].id.clone();
    for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
        graph = service
            .transition_node(&graph, &node, status, None, vec![])
            .unwrap()
            .graph;
    }
    let lineage = graph.lineage.as_ref().unwrap();
    let context = crate::CowdExecutionContext {
        execution_id: graph.id.clone(),
        session_id: lineage.session_id.clone(),
        turn_id: lineage.turn_id.clone(),
    };
    let absent = service.root_effect_authority(&context).unwrap();
    let goals = crate::execution_core::GoalStore::new(store.clone());
    let goal_id = format!("goal:{}", graph.id);
    let mut goal = goals.create(serde_json::from_value(json!({
        "id": goal_id, "session_id": context.session_id, "objective":"check Root admission",
        "criteria": [{"id":"intent", "statement":"check Root admission", "status":"open"}],
        "source_intent_ref":"session_message:root-admission", "user_intent_criterion_id":"intent",
        "spec_revision":1, "spec_digest":"root-admission-fixture",
        "phase":"execution", "completion":"open", "revision":1, "user_sequence":1,
        "execution_binding": {
            "objective_id":"root-admission", "session_id":context.session_id, "turn_id":context.turn_id,
            "root_execution_id":context.execution_id, "agentic_program_id":"root-admission-program"
        }
    })).unwrap()).unwrap();
    assert!(store
        .append_transaction(AppendTransactionRequest {
            transaction_id: "stale-absent-goal".into(),
            expected_streams: absent,
            events: vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id: "stale-root-effect".into(),
                    scope: RuntimeEventScope::ExecutionNode,
                    kind: "test.stale_effect".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: json!({}),
                },
                idempotency_key: Some("stale-root-effect".into()),
                schema_version: 1,
            }],
        })
        .is_err());
    assert_eq!(store.stream_revision("stale-root-effect").unwrap(), 0);
    let mut write = request("goal-state-write");
    write.session_id = Some(context.session_id.clone());
    let effect = mutation_effect(ToolIdempotency::Idempotent);
    assert!(service.root_effect_authority(&context).is_ok());
    for state in [
        GoalCompletion::WaitingExternalDecision,
        GoalCompletion::Blocked,
        GoalCompletion::Partial,
        GoalCompletion::Failed,
        GoalCompletion::Cancelled,
        GoalCompletion::Satisfied,
    ] {
        goal = goals
            .revise(
                &goal_id,
                goal.revision,
                goal.user_sequence + 1,
                "negative admission fixture",
                |goal| {
                    goal.completion = state;
                    vec![]
                },
            )
            .unwrap()
            .0;
        assert!(
            service
                .begin_root_tool_effect(&write, &effect, &context)
                .is_err(),
            "{state:?}"
        );
        assert_eq!(
            store
                .stream_revision("execution-effect:idem-goal-state-write")
                .unwrap(),
            0
        );
    }
}
