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
        compiled_invocation: None,
        step_binding: None,
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

#[tokio::test]
async fn bounded_process_private_terminal_original_owner_recovery_and_public_forgery_283() {
    use harness_contract::context::{
        ArtifactWriteDescriptor, EvidenceTargetIdentity, ToolExecutionTerminal,
    };
    for (exit_code, cancel_before_spawn, wrong_text) in [
        (0, false, None),
        (3, false, None),
        (0, true, None),
        (0, false, Some("stdout_utf8")),
        (0, false, Some("stderr_utf8")),
    ] {
        let workspace = tempfile::tempdir().unwrap();
        std::fs::write(workspace.path().join("source.py"), "VALUE = 7\n").unwrap();
        let store = Arc::new(RuntimeEventStore::for_test());
        let artifacts = Arc::new(crate::ArtifactStore::for_test_default(
            workspace.path().join("artifacts"),
        ));
        let services = super::bounded_process_tests::services(
            workspace.path(),
            store.clone(),
            artifacts.clone(),
        );
        let service =
            ExecutionCommitService::new(store.clone()).with_artifact_store(artifacts.clone());
        let host = tools::ToolHost::builtin("bounded-private-terminal-283", workspace.path())
            .with_authorization_lease_verifier(Arc::new(
                crate::AuthorizationNegotiator::verify_lease_signature,
            ));
        let input = json!({"language":"python","input_files":["source.py"],"timeout_ms":5000,
            "code":format!("import os\nfrom source import VALUE\nassert VALUE == 7\nos.write(1, b'x' * 20000 + bytes([255]))\nos.write(2, b'bounded UTF8 stderr\\n')\nraise SystemExit({exit_code})\n")});
        let mut call = crate::bound_tool_batch::scope_admission_tests::bound_request(
            &host,
            "execute_code",
            input,
        );
        let effect = call.compiled_invocation.as_ref().unwrap().effect.clone();
        crate::bound_tool_batch::scope_admission_tests::authorize_bounded_fixture(
            &host,
            &mut call,
            store.clone(),
            harness_contract::tool::ToolPermissionMode::DangerFullAccess,
        );
        let mut graph = ExecutionGraph::new("actual bounded ToolBatch owner");
        graph.id = "scope2-graph".into();
        graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
            session_id: "scope2-session".into(),
            turn_id: "scope2-turn".into(),
            root_task_id: "bounded-root".into(),
            task_id: "bounded-root".into(),
            generation: 1,
        });
        let mut node = ExecutionNodeSpec::new(
            ExecutionNodeKind::ToolBatch,
            "tool_batch",
            "bounded-process",
        );
        node.id = "bounded-node".into();
        graph.nodes.push(node);
        graph = service.register_graph(graph).unwrap().graph;
        graph = service
            .transition_node(
                &graph,
                "bounded-node",
                ExecutionNodeStatus::Ready,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        let binding = ExecutionNodeBinding {
            executor_kind: graph.nodes[0].executor_kind.clone(),
            ticket_idempotency_key: graph.nodes[0].idempotency_key.clone(),
            attempt: 1,
            resource_lease_refs: vec![],
            scope_lease_ref: None,
            worktree_lease_ref: None,
        };
        graph = service
            .bind_and_start_node(&graph, "bounded-node", binding)
            .unwrap()
            .graph;
        call.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
            execution_id: graph.id.clone(),
            node_id: "bounded-node".into(),
        });
        call.parent_execution_attempt = Some(graph.recovery_cursor.node_attempts["bounded-node"]);
        let resolver =
            crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path())
                .unwrap();
        let scope = crate::bound_tool_batch::admitted_effect_scope(
            &call,
            &effect,
            Some(host.pin_snapshot()),
            &resolver,
        )
        .unwrap();
        assert_eq!(scope.version, 2);
        assert_eq!(
            service
                .begin_bound_tool_effect(&call, &effect, &scope)
                .unwrap(),
            ToolEffectState::Fresh
        );
        let binding = service.bounded_process_binding(&call, &effect).unwrap();
        let normalized = harness_contract::step::parse_strict_json(&call.input).unwrap();
        let reformatted = harness_contract::step::parse_strict_json(
            &serde_json::to_string_pretty(&normalized).unwrap(),
        )
        .unwrap();
        assert_eq!(
            binding.input_sha256,
            ExecutionCommitService::bounded_process_input_sha256(&normalized).unwrap()
        );
        assert_eq!(
            binding.input_sha256,
            ExecutionCommitService::bounded_process_input_sha256(&reformatted).unwrap()
        );
        let mut changed = normalized.clone();
        changed["code"] = "raise SystemExit(290)".into();
        assert_ne!(
            binding.input_sha256,
            ExecutionCommitService::bounded_process_input_sha256(&changed).unwrap()
        );
        assert!(harness_contract::step::parse_strict_json(
            r#"{"code":"pass","code":"raise SystemExit(1)"}"#
        )
        .is_err());
        assert_eq!(effect.descriptor_hash.len(), 16);
        assert_eq!(binding.descriptor_sha256.len(), 64);
        assert_eq!(
            binding.descriptor_sha256,
            ExecutionCommitService::bounded_process_descriptor_sha256(&effect).unwrap()
        );
        let prepared = host
            .pin_snapshot()
            .prepare_bounded_process(
                call.authorization.as_ref().unwrap(),
                &serde_json::from_str(&call.input).unwrap(),
                binding,
            )
            .unwrap();
        let source = artifacts
            .write_bytes(
                ArtifactWriteDescriptor {
                    media_type: "application/octet-stream".into(),
                    visibility_scope: "session:scope2-session".into(),
                    expected_bytes: Some(prepared.source_bundle_bytes().len() as u64),
                    original_name: None,
                },
                prepared.source_bundle_bytes(),
            )
            .await
            .unwrap();
        let mut wrong_archive = source.clone();
        wrong_archive.bytes += 1;
        assert!(service
            .admit_bounded_process(&call, &effect, &prepared, &wrong_archive)
            .is_err());
        let mut wrong_revision = call.clone();
        wrong_revision
            .authorization
            .as_mut()
            .unwrap()
            .policy_revision += 1;
        assert!(service
            .bounded_process_binding(&wrong_revision, &effect)
            .unwrap_err()
            .to_string()
            .contains("grant and request policy revision differ"));
        let mut wrong_parent = call.clone();
        wrong_parent.parent_execution.as_mut().unwrap().node_id = "foreign-owner".into();
        assert!(service
            .bounded_process_binding(&wrong_parent, &effect)
            .is_err());
        let admission = service
            .admit_bounded_process(&call, &effect, &prepared, &source)
            .unwrap();
        let restored =
            ExecutionCommitService::new(store.clone()).with_artifact_store(artifacts.clone());
        assert_eq!(
            restored
                .begin_bound_tool_effect(&call, &effect, &scope)
                .unwrap(),
            ToolEffectState::Uncertain
        );
        assert!(restored
            .admit_bounded_process(&call, &effect, &prepared, &source)
            .is_err());
        let mut forged = outcome(&call.tool_use_id, "a public DTO is not a completed process");
        forged.tool_name = call.tool_name.clone();
        let mut observation = crate::path_identity::untrusted_process_observation_283();
        if let EvidenceTargetIdentity::ToolExecution { execution } = &mut observation.target {
            execution.admission = admission.clone();
            execution.terminal = ToolExecutionTerminal::Settled {
                exit_code: Some(0),
                signal: None,
                stop_reason: harness_contract::context::ToolExecutionStopReason::CommandExit,
                closure: harness_contract::context::ToolProcessClosure {
                    backend: "publicly-fabricated-not-a-private-handle".into(),
                    namespace_inode: 1,
                    init_host_pid: 1,
                    init_start_ticks: 1,
                    all_children_reaped: true,
                    init_reaped: true,
                    kernel_hardening_applied: true,
                    network_disabled: true,
                    project_read_only: true,
                    status_channel_protected: true,
                },
            };
            execution.capture_complete = true;
        }
        forged.observed_evidence.push(observation);
        assert!(restored
            .commit_tool_effect(&call, &effect, &forged)
            .unwrap_err()
            .to_string()
            .contains("public ToolExecution observations are not an execution capability"));
        assert!(restored
            .completed_tool_effect(&call, &effect)
            .unwrap()
            .is_none());
        let cancellation = tools::ToolExecutionCancellation::new();
        if cancel_before_spawn {
            cancellation.cancel();
        }
        let completed = prepared.run(admission, &cancellation);
        if cancel_before_spawn {
            assert!(matches!(
                completed.terminal(),
                ToolExecutionTerminal::NotStarted { .. }
            ));
        } else {
            assert!(
                matches!(completed.terminal(), ToolExecutionTerminal::Settled {
                exit_code:Some(actual), closure, ..
            } if *actual == exit_code && closure.all_children_reaped && closure.init_reaped
                && closure.project_read_only && closure.network_disabled && closure.status_channel_protected)
            );
        }
        if let Some(field) = wrong_text {
            let mut modified: serde_json::Value =
                serde_json::from_slice(completed.body_bytes()).unwrap();
            modified[field] =
                "untrusted replacement text does not equal the complete original stream".into();
            let bytes = serde_json::to_vec(&modified).unwrap();
            let modified_body = artifacts
                .write_bytes(
                    ArtifactWriteDescriptor {
                        media_type: "application/json".into(),
                        visibility_scope: "session:scope2-session".into(),
                        expected_bytes: Some(bytes.len() as u64),
                        original_name: None,
                    },
                    &bytes,
                )
                .await
                .unwrap();
            assert!(completed
                .into_output(modified_body)
                .unwrap_err()
                .to_string()
                .contains("bounded_body_archive_mismatch"));
            assert!(restored
                .completed_tool_effect(&call, &effect)
                .unwrap()
                .is_none());
            assert_eq!(
                restored
                    .begin_bound_tool_effect(&call, &effect, &scope)
                    .unwrap(),
                ToolEffectState::Uncertain
            );
            services.shutdown_execution().await;
            services.shutdown_maintenance().await;
            continue;
        }
        let authority =
            crate::tool_host::BoundedProcessAuthority::bind(services.clone(), call.clone()).unwrap();
        let committed = authority
            .with_commit_context(
                restored.commit_bounded_process(&call, &effect, completed, &artifacts),
            )
            .await
            .unwrap();
        let fact = committed
            .observed_evidence
            .iter()
            .find_map(|observed| match &observed.target {
                EvidenceTargetIdentity::ToolExecution { execution } => Some(execution),
                _ => None,
            })
            .unwrap();
        assert_eq!(
            fact.stdout_bytes,
            if cancel_before_spawn { 0 } else { 20001 }
        );
        assert_eq!(fact.capture_complete, !cancel_before_spawn);
        assert_eq!(fact.admission.source_bundle, source);
        let body = artifacts
            .read(&fact.body, "session:scope2-session", None)
            .await
            .unwrap();
        assert_eq!(body.len() as u64, fact.body.bytes);
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        use base64::Engine;
        let stdout = base64::engine::general_purpose::STANDARD
            .decode(body["stdout_base64"].as_str().unwrap())
            .unwrap();
        assert_eq!(stdout.len() as u64, fact.stdout_bytes);
        assert_eq!(body["capture_complete"], fact.capture_complete);
        if !cancel_before_spawn {
            assert_eq!(stdout.last(), Some(&255));
            assert!(body.get("stdout_utf8").is_none());
            assert_eq!(body["stderr_utf8"], "bounded UTF8 stderr\n");
            assert_eq!(
                base64::engine::general_purpose::STANDARD
                    .decode(body["stderr_base64"].as_str().unwrap())
                    .unwrap(),
                body["stderr_utf8"].as_str().unwrap().as_bytes()
            );
            assert!(committed.output.as_ref().unwrap().len() < 16 * 1024);
        }
        assert!(
            serde_json::to_vec(&committed.observed_evidence)
                .unwrap()
                .len()
                < 128 * 1024
        );
        let before = store
            .stream_event_count(&format!("execution-effect:{}", call.idempotency_key))
            .unwrap();
        graph = restored
            .transition_node(
                &graph,
                "bounded-node",
                ExecutionNodeStatus::Completed,
                None,
                vec![],
            )
            .unwrap()
            .graph;
        assert_eq!(
            graph.node_statuses["bounded-node"],
            ExecutionNodeStatus::Completed
        );
        host.replace_snapshot(host.pin_snapshot().snapshot().clone());
        assert!(call
            .step_binding
            .as_ref()
            .unwrap()
            .resolve(Some(host.pin_snapshot()))
            .is_err());
        let historical = restored
            .completed_tool_effect(&call, &effect)
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&historical).unwrap(),
            serde_json::to_value(&committed).unwrap()
        );
        let parent = call.parent_execution.as_ref().unwrap();
        let queried = restored
            .query_completed_agent_tool_effect(&crate::execution_core::graph::ToolReceiptQuery {
                step_binding: call.step_binding.as_ref().unwrap(),
                parent_execution: parent,
                session_id: call.session_id.as_deref().unwrap(),
                policy_revision: call.policy_revision,
                idempotency_key: &call.idempotency_key,
                tool_use_id: &call.tool_use_id,
                tool_name: &call.tool_name,
                input: &call.input,
            })
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(&queried.outcome).unwrap(),
            serde_json::to_value(&committed).unwrap()
        );
        assert_eq!(
            store
                .stream_event_count(&format!("execution-effect:{}", call.idempotency_key))
                .unwrap(),
            before
        );
        services.shutdown_execution().await;
        services.shutdown_maintenance().await;
    }
}

fn outcome(id: &str, output: &str) -> crate::RuntimeToolExecutionOutcome {
    crate::RuntimeToolExecutionOutcome {
        pending_agent_batch: None,
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

struct ReceiptQueryFixture {
    store: Arc<RuntimeEventStore>,
    service: ExecutionCommitService,
    workspace: tempfile::TempDir,
    host: tools::ToolHost,
    step: Arc<crate::step_snapshot::BoundStep>,
    graph: ExecutionGraph,
    request: crate::RuntimeToolExecutionRequest,
    completed: crate::RuntimeToolExecutionOutcome,
}

fn receipt_query_fixture() -> ReceiptQueryFixture {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let workspace = tempfile::tempdir().unwrap();
    let host = tools::ToolHost::builtin("receipt-recovery-fixture", workspace.path());
    let lease = host.pin_snapshot();
    let advertised = lease
        .snapshot()
        .catalog
        .definitions(None)
        .into_iter()
        .filter(|definition| definition.name == "edit")
        .map(|definition| crate::ProviderToolDefinition {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
        })
        .collect::<Vec<_>>();
    let step = Arc::new(
        crate::step_snapshot::BoundStep::bind(
            harness_contract::step::StepIdentity {
                step_id: "step:receipt-recovery".into(),
                session_id: "session".into(),
                turn_id: "turn".into(),
                execution_id: Some("graph:receipt-recovery".into()),
                actor_ref: Some("fixture".into()),
                run_ref: None,
                attempt: Some(1),
                epoch: Some("1".into()),
                goal_ref: None,
                program_ref: None,
                task_ref: None,
                build_ref: "isolated-fixture".into(),
            },
            lease,
            &advertised,
            &json!({}),
            &json!({}),
            BTreeMap::new(),
            10,
        )
        .unwrap(),
    );
    let binding = crate::step_snapshot::StepCallBinding::provider(step.clone(), "write-once");
    let input = r#"{"action":"write","path":"proof.txt","content":"one physical write"}"#;
    let value = step
        .validate_call(&binding.origin, "session", "write-once", "edit", input, 1)
        .unwrap();
    let mut graph = ExecutionGraph::new("receipt recovery fixture");
    graph.id = "graph:receipt-recovery".into();
    graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: "session".into(),
        turn_id: "turn".into(),
        root_task_id: "task".into(),
        task_id: "task".into(),
        generation: 1,
    });
    let mut node = ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", "fixture");
    node.id = "node:write".into();
    graph.nodes.push(node);
    graph = service.register_graph(graph).unwrap().graph;
    for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
        graph = service
            .transition_node(&graph, "node:write", status, None, vec![])
            .unwrap()
            .graph;
    }
    let mut request = request("write-once");
    request.tool_name = "edit".into();
    request.input = input.into();
    request.policy_revision = 1;
    request.category = crate::ToolSafetyCategory::WriteLocal;
    request.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: "graph:receipt-recovery".into(),
        node_id: "node:write".into(),
    });
    request.parent_execution_attempt = Some(graph.recovery_cursor.node_attempts["node:write"]);
    request.idempotency_key = binding.effect_idempotency_key();
    request.compiled_invocation = Some(
        binding
            .prepare_invocation(Some(host.pin_snapshot()), "edit", &value)
            .unwrap(),
    );
    request.step_binding = Some(binding.clone());
    let effect = request.compiled_invocation.as_ref().unwrap().effect.clone();
    let resolver =
        crate::path_identity::WorkspacePathIdentityResolver::discover(workspace.path()).unwrap();
    let scope = crate::bound_tool_batch::admitted_effect_scope(
        &request,
        &effect,
        Some(host.pin_snapshot()),
        &resolver,
    )
    .unwrap();
    assert_eq!(scope.version, 2);
    scope.validate_purpose_mapping(&effect).unwrap();
    assert_eq!(
        service
            .begin_bound_tool_effect(&request, &effect, &scope)
            .unwrap(),
        ToolEffectState::Fresh
    );
    std::fs::write(workspace.path().join("proof.txt"), "one physical write").unwrap();
    let mut completed = outcome("write-once", "original durable result");
    completed.tool_name = "edit".into();
    completed.category = crate::ToolSafetyCategory::WriteLocal;
    service
        .commit_tool_effect(&request, &effect, &completed)
        .unwrap();
    ReceiptQueryFixture {
        store,
        service,
        workspace,
        host,
        step,
        graph,
        request,
        completed,
    }
}

#[test]
fn expired_serialized_step_queries_the_original_receipt_without_loading_an_adapter() {
    let ReceiptQueryFixture {
        store,
        service,
        workspace,
        host,
        step,
        graph,
        request,
        completed,
    } = receipt_query_fixture();
    let binding = request.step_binding.as_ref().unwrap();
    let input = request.input.as_str();
    let value = harness_contract::step::parse_strict_json(input).unwrap();
    let count = store
        .stream_event_count(&format!("execution-effect:{}", request.idempotency_key))
        .unwrap();
    let mut recovered: crate::RuntimeToolExecutionRequest =
        serde_json::from_value(serde_json::to_value(&request).unwrap()).unwrap();
    recovered.compiled_invocation = None;
    host.replace_snapshot(host.pin_snapshot().snapshot().clone());
    assert!(recovered
        .step_binding
        .as_ref()
        .unwrap()
        .resolve(Some(host.pin_snapshot()))
        .is_err());
    assert!(step
        .validate_call(&binding.origin, "session", "write-once", "edit", input, 10)
        .is_err());
    let mut query = ToolReceiptQuery {
        step_binding: recovered.step_binding.as_ref().unwrap(),
        parent_execution: recovered.parent_execution.as_ref().unwrap(),
        session_id: recovered.session_id.as_deref().unwrap(),
        policy_revision: recovered.policy_revision,
        idempotency_key: &recovered.idempotency_key,
        tool_use_id: &recovered.tool_use_id,
        tool_name: &recovered.tool_name,
        input: &recovered.input,
    };
    let replay = service
        .query_completed_tool_effect(&query)
        .unwrap()
        .unwrap();
    assert_eq!(replay.outcome, completed);
    assert_eq!(replay.invocation.intent.normalized_input, value);
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("proof.txt")).unwrap(),
        "one physical write"
    );
    assert_eq!(
        store
            .stream_event_count(&format!("execution-effect:{}", request.idempotency_key))
            .unwrap(),
        count
    );
    query.input = r#"{"content":"one physical write", "path":"proof.txt", "action":"write"}"#;
    assert!(service
        .query_completed_tool_effect(&query)
        .unwrap()
        .is_some());
    query.input = r#"{"action":"write","path":"proof.txt","content":"changed"}"#;
    assert!(service.query_completed_tool_effect(&query).is_err());
    query.input = input;
    query.session_id = "another-session";
    assert!(service.query_completed_tool_effect(&query).is_err());
    assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 1);
    let system_step = Arc::new((*step).clone().with_parent("runtime-checkpoint"));
    let mut system = request.clone();
    system.step_binding = Some(crate::step_snapshot::StepCallBinding::system(
        system_step,
        "runtime-checkpoint",
        "write-once",
    ));
    service.validate_system_tool_parent(&system).unwrap();
    system.parent_execution = None;
    assert!(service.validate_system_tool_parent(&system).is_err());
    system.parent_execution = request.parent_execution.clone();
    system.session_id = Some("another-session".into());
    assert!(service.validate_system_tool_parent(&system).is_err());
    system.session_id = request.session_id.clone();
    service
        .transition_node(
            &graph,
            "node:write",
            ExecutionNodeStatus::Completed,
            None,
            vec![],
        )
        .unwrap();
    assert!(service.validate_system_tool_parent(&system).is_err());
    let query = ToolReceiptQuery {
        step_binding: binding,
        parent_execution: request.parent_execution.as_ref().unwrap(),
        session_id: request.session_id.as_deref().unwrap(),
        policy_revision: request.policy_revision,
        idempotency_key: &request.idempotency_key,
        tool_use_id: &request.tool_use_id,
        tool_name: &request.tool_name,
        input,
    };
    assert!(service
        .query_completed_tool_effect(&query)
        .unwrap()
        .is_some());
}

#[test]
fn r1_receipt_query_wrong_parent_or_input_has_no_replay() {
    let fixture = receipt_query_fixture();
    let mut other_node =
        ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", "other");
    other_node.id = "node:other".into();
    fixture
        .service
        .replan(
            &fixture.graph,
            vec![other_node],
            vec![],
            "query identity fixture".into(),
        )
        .unwrap();
    let mut other_graph = ExecutionGraph::new("other receipt parent");
    other_graph.id = "graph:other-parent".into();
    other_graph.lineage = fixture.graph.lineage.clone();
    other_graph.nodes = fixture.graph.nodes.clone();
    fixture.service.register_graph(other_graph).unwrap();
    let request = &fixture.request;
    let original_parent = request.parent_execution.as_ref().unwrap();
    let other_parent = harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: original_parent.execution_id.clone(),
        node_id: "node:other".into(),
    };
    let wrong_graph = harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: "graph:other-parent".into(),
        node_id: original_parent.node_id.clone(),
    };
    let missing_node = harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: original_parent.execution_id.clone(),
        node_id: "node:missing".into(),
    };
    let mut wrong_origin = request.step_binding.as_ref().unwrap().clone();
    wrong_origin.origin.call_id = "other-origin".into();
    let mut query = ToolReceiptQuery {
        step_binding: request.step_binding.as_ref().unwrap(),
        parent_execution: original_parent,
        session_id: request.session_id.as_deref().unwrap(),
        policy_revision: request.policy_revision,
        idempotency_key: &request.idempotency_key,
        tool_use_id: &request.tool_use_id,
        tool_name: &request.tool_name,
        input: &request.input,
    };
    let cursor = fixture.store.durable_commit_cursor().unwrap();
    assert!(fixture
        .service
        .query_completed_agent_tool_effect(&query)
        .unwrap()
        .is_some());
    query.parent_execution = &other_parent;
    assert!(fixture
        .service
        .query_completed_tool_effect(&query)
        .unwrap()
        .is_some());
    assert!(fixture
        .service
        .query_completed_agent_tool_effect(&query)
        .is_err());
    for parent in [&wrong_graph, &missing_node] {
        query.parent_execution = parent;
        assert!(fixture.service.query_completed_tool_effect(&query).is_err());
        assert!(fixture
            .service
            .query_completed_agent_tool_effect(&query)
            .is_err());
    }
    query.parent_execution = original_parent;
    for input in [
        r#"{"action":"write","path":"proof.txt","content":"changed"}"#,
        r#"{"action":"write","path":"proof.txt","content":"one physical write","content":"one physical write"}"#,
    ] {
        query.input = input;
        assert!(fixture.service.query_completed_tool_effect(&query).is_err());
        assert!(fixture
            .service
            .query_completed_agent_tool_effect(&query)
            .is_err());
    }
    query.input = &request.input;
    query.session_id = "another-session";
    assert!(fixture.service.query_completed_tool_effect(&query).is_err());
    query.session_id = request.session_id.as_deref().unwrap();
    query.policy_revision = 0;
    assert!(fixture.service.query_completed_tool_effect(&query).is_err());
    query.policy_revision = request.policy_revision;
    query.tool_use_id = "another-call";
    assert!(fixture.service.query_completed_tool_effect(&query).is_err());
    query.tool_use_id = &request.tool_use_id;
    query.tool_name = "another-tool";
    assert!(fixture.service.query_completed_tool_effect(&query).is_err());
    query.tool_name = &request.tool_name;
    query.step_binding = &wrong_origin;
    assert!(fixture.service.query_completed_tool_effect(&query).is_err());
    assert_eq!(fixture.store.durable_commit_cursor().unwrap(), cursor);
    assert_eq!(
        std::fs::read_to_string(fixture.workspace.path().join("proof.txt")).unwrap(),
        "one physical write"
    );
}

#[test]
fn r1_receipt_query_is_readonly_and_does_not_dispatch() {
    let ReceiptQueryFixture {
        store,
        service,
        workspace,
        host,
        step,
        graph,
        mut request,
        completed,
    } = receipt_query_fixture();
    request.step_binding = Some(
        serde_json::from_value(
            serde_json::to_value(request.step_binding.as_ref().unwrap()).unwrap(),
        )
        .unwrap(),
    );
    request.compiled_invocation = None;
    drop(host);
    drop(step);
    let binding = request.step_binding.as_ref().unwrap();
    assert!(binding.resolve(None).is_err());
    let path = workspace.path().join("proof.txt");
    std::fs::write(&path, "changed after the original write").unwrap();
    let cursor = store.durable_commit_cursor().unwrap();
    let events = store.all_events(usize::MAX).unwrap();
    let mut query = ToolReceiptQuery {
        step_binding: binding,
        parent_execution: request.parent_execution.as_ref().unwrap(),
        session_id: request.session_id.as_deref().unwrap(),
        policy_revision: request.policy_revision,
        idempotency_key: &request.idempotency_key,
        tool_use_id: &request.tool_use_id,
        tool_name: &request.tool_name,
        input: &request.input,
    };
    for replay in [
        service
            .query_completed_tool_effect(&query)
            .unwrap()
            .unwrap(),
        service
            .query_completed_agent_tool_effect(&query)
            .unwrap()
            .unwrap(),
    ] {
        assert_eq!(replay.outcome, completed);
        assert_eq!(
            replay.invocation.intent.normalized_input,
            harness_contract::step::parse_strict_json(&request.input).unwrap()
        );
    }
    query.idempotency_key = "missing-effect";
    assert!(service
        .query_completed_tool_effect(&query)
        .unwrap()
        .is_none());
    assert!(service
        .query_completed_agent_tool_effect(&query)
        .unwrap()
        .is_none());
    assert_eq!(store.durable_commit_cursor().unwrap(), cursor);
    assert_eq!(store.all_events(usize::MAX).unwrap(), events);
    assert_eq!(
        service.effect_authority_graphs.load(&graph.id).unwrap(),
        graph
    );
    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "changed after the original write"
    );
    assert_eq!(std::fs::read_dir(workspace.path()).unwrap().count(), 1);
}

fn readonly_effect() -> ToolEffectDescriptor {
    let mut effect = mutation_effect(ToolIdempotency::Idempotent);
    effect.effect_kind = ToolEffectKind::Read;
    effect.required_permission = ToolPermissionMode::ReadOnly;
    effect.approval_class = ToolApprovalClass::None;
    effect
}

/// Agent receipt request bound to the `order-graph`/`order-node`/attempt-7
/// index stream used by the out-of-order recovery tests.
fn request_with_parent(seq: u64, id: &str) -> crate::RuntimeToolExecutionRequest {
    let mut request = request(id);
    request.observation_wave_sequence = seq;
    request.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: "order-graph".to_string(),
        node_id: "order-node".to_string(),
    });
    request.parent_execution_attempt = Some(7);
    request
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
    let mut conflicting = receipts.clone();
    conflicting[0].1.output = Some("foreign outcome".into());
    assert!(service.commit_readonly_tool_receipts(&conflicting).is_err());
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
        compiled_invocation: None,
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
fn a_nodes_reads_are_visible_across_its_own_attempts_but_never_across_nodes() {
    // A read is a fact about the Agent node, not about one dispatch of it. The
    // per-attempt stream made a re-dispatched reviewer start from zero reads, so
    // it was refused for "not having read" bodies it had already read, and each
    // refusal burned the retry budget until the attempt settled.
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    for (attempt, id) in [(1u32, "first-read"), (2, "second-read")] {
        let mut request = request_with_parent(attempt as u64, id);
        request.parent_execution_attempt = Some(attempt);
        service
            .commit_readonly_tool_receipts(&[(request, outcome(id, &format!("{id}-durable")))])
            .expect("commit attempt receipt");
    }
    // A different node reading the same body must stay invisible: independence
    // is about who read it, not merely that somebody did.
    let mut other_node = request_with_parent(1, "other-node-read");
    other_node.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: "order-graph".to_string(),
        node_id: "other-node".to_string(),
    });
    other_node.parent_execution_attempt = Some(1);
    service
        .commit_readonly_tool_receipts(&[(other_node, outcome("other-node-read", "other-durable"))])
        .expect("commit the other node receipt");

    let unioned = service
        .load_delegated_agent_tool_receipts_all_attempts("order-graph", "order-node", 2)
        .expect("union this node's attempts");
    assert_eq!(
        unioned.len(),
        2,
        "both attempts of this node count: {:?}",
        unioned
            .iter()
            .map(|receipt| receipt.outcome.tool_use_id.clone())
            .collect::<Vec<_>>()
    );
    assert!(unioned
        .iter()
        .all(|receipt| receipt.outcome.tool_use_id != "other-node-read"));
    // The per-attempt loader is unchanged: attempt-exact callers still see one.
    let exact = service
        .load_delegated_agent_tool_receipts("order-graph", "order-node", 1)
        .expect("exact attempt");
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].outcome.tool_use_id, "first-read");
}

#[test]
fn delegated_agent_receipts_reload_restores_causal_order_when_indexed_writes_are_out_of_sequence() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let later = {
        let mut request = request_with_parent(2, "later");
        request.authorized_scopes = vec!["read:src/late.rs".to_string()];
        request
    };
    let earlier = {
        let mut request = request_with_parent(1, "earlier");
        request.authorized_scopes = vec!["read:src/early.rs".to_string()];
        request
    };
    service
        .commit_readonly_tool_receipts(&[(later.clone(), outcome("later", "later-durable"))])
        .expect("commit higher-sequence receipt first");
    service
        .commit_readonly_tool_receipts(&[(earlier.clone(), outcome("earlier", "earlier-durable"))])
        .expect("commit lower-sequence receipt second");

    let recovered = service
        .load_delegated_agent_tool_receipts("order-graph", "order-node", 7)
        .expect("reload exact attempt receipts");
    assert_eq!(recovered.len(), 2);
    assert_eq!(recovered[0].sequence, 1);
    assert_eq!(recovered[1].sequence, 2);
    assert_eq!(recovered[0].outcome, outcome("later", "later-durable"));
    assert_eq!(recovered[1].outcome, outcome("earlier", "earlier-durable"));
}

#[test]
fn delegated_agent_receipts_reload_rejects_duplicate_causal_sequence() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(Arc::clone(&store));
    let stream = "execution-agent-receipts:order-graph:order-node:7";
    for identifier in ["dup-a", "dup-b"] {
        store.append_batch_if_revision(
            stream.to_string(), store.stream_revision(stream).unwrap(), identifier.to_string(),
            vec![RuntimeTransactionEventInput {
                event: RuntimeEventInput {
                    stream_id:stream.into(), scope:RuntimeEventScope::ExecutionNode,
                    kind:"execution.agent_tool.receipt".into(), status:Some("completed".into()),
                    actor:Some("governed_tool".into()), refs:vec![],
                    payload:serde_json::json!({"sequence":1}),
                }, idempotency_key:Some(identifier.into()), schema_version:1,
            }],
        ).unwrap();
    }

    assert!(matches!(
        service.load_delegated_agent_tool_receipts("order-graph", "order-node", 7),
        Err(ExecutionCommitError::InvalidCommand(message))
            if message.contains("duplicate causal sequence")
    ));
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
    let mut wrong_attempt = pending.clone();
    wrong_attempt.parent_execution_attempt = Some(2);
    assert!(service
        .commit_tool_effect(
            &wrong_attempt,
            &effect,
            &outcome("pending-after-review", "settled")
        )
        .is_err());
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

fn root_ledger_scope(effect: &ToolEffectDescriptor) -> harness_contract::goal::ProofEffectScope {
    assert!(effect.scopes.is_empty());
    harness_contract::goal::ProofEffectScope {
        version: 2,
        descriptor_hash: effect.descriptor_hash.clone(),
        complete: false,
        targets: Vec::new(),
        scope_purposes: Vec::new(),
    }
}

fn root_ledger_owner(
    store: &RuntimeEventStore,
    graph: &ExecutionGraph,
    node_id: &str,
) -> (harness_contract::projection::RuntimeActivityBinding, u32) {
    let original = store
        .latest_for_stream(&format!("{}:node:{node_id}", graph.id))
        .unwrap()
        .or_else(|| store.latest_for_stream(&graph.id).unwrap())
        .unwrap();
    let owner = original
        .activity_binding()
        .expect("original persisted activity");
    let attempt = graph
        .recovery_cursor
        .node_attempts
        .get(node_id)
        .copied()
        .unwrap_or_default();
    (owner, attempt)
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
    let effect = mutation_effect(ToolIdempotency::Idempotent);
    // Root receipts authenticate the original Step and compiled payload. A
    // legacy unbound request cannot exercise retained-replay/settlement.
    let bound_request = |id: &str| {
        let mut request = request(id);
        request.session_id = Some(context.session_id.clone());
        let snapshot: harness_contract::step::StepSnapshot = serde_json::from_value(json!({
            "contract_version":harness_contract::step::STEP_CONTRACT_VERSION,
            "identity":{"step_id":"root-ledger-step","session_id":context.session_id,
                "turn_id":context.turn_id,"execution_id":context.execution_id,"build_ref":"isolated-root-ledger"},
            "provider_attempt":{"attempt_id":"root-ledger-attempt","origin_step_id":"root-ledger-step",
                "model_profile_digest":"fixture-profile","request_digest":"fixture-request"},
            "catalog_digest":"fixture-catalog","tools":[],"related_revisions":{},"context_refs":[],
            "expires_at_ms":i64::MAX,
        })).unwrap();
        let origin = harness_contract::step::CallOrigin {
            origin_step_id: "root-ledger-step".into(),
            call_id: id.into(),
            catalog_digest: "fixture-catalog".into(),
            source: harness_contract::step::CallSource::OfflineTest,
        };
        let binding =
            crate::step_snapshot::StepCallBinding::persisted(origin.clone(), Arc::new(snapshot));
        request.idempotency_key = binding.effect_idempotency_key();
        request.compiled_invocation = Some(harness_contract::tool::GovernedToolInvocation {
            origin: Some(origin),
            contract_version: 1,
            invocation_id: id.into(),
            intent: harness_contract::tool::ToolIntent {
                invocation_id: id.into(),
                tool_name: request.tool_name.clone(),
                normalized_input: serde_json::from_str(&request.input).unwrap(),
            },
            effect: effect.clone(),
            resource_demand: crate::governed_tool_plan::resource_demand_from_effect(&effect),
            explicit_dependencies: vec![],
            compiled_dependencies: vec![],
            catalog_revision: 1,
            descriptor_set_hash: "fixture-descriptors".into(),
            idempotency_key: request.idempotency_key.clone(),
        });
        request.step_binding = Some(binding);
        request
    };
    let write = bound_request("root-write");
    let scope = root_ledger_scope(&effect);
    let (owner, attempt) = root_ledger_owner(&store, &graph, &node);
    assert!(service
        .begin_root_tool_effect(&write, &effect, &context, &scope, &owner, attempt)
        .is_err());
    for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
        graph = service
            .transition_node(&graph, &node, status, None, vec![])
            .unwrap()
            .graph;
    }
    let mut stale = context.clone();
    let (owner, attempt) = root_ledger_owner(&store, &graph, &node);
    stale.turn_id = "old-turn".into();
    assert!(service
        .begin_root_tool_effect(&write, &effect, &stale, &scope, &owner, attempt)
        .is_err());
    assert_eq!(
        store
            .stream_revision(&format!("execution-effect:{}", write.idempotency_key))
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
            .begin_root_tool_effect(&write, &effect, &context, &scope, &owner, attempt)
            .unwrap(),
        ToolEffectState::Fresh
    );
    service
        .commit_tool_effect(&write, &effect, &outcome("root-write", "written"))
        .unwrap();
    let pending = bound_request("root-pending");
    assert_eq!(
        service
            .begin_root_tool_effect(&pending, &effect, &context, &scope, &owner, attempt)
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
        .begin_root_tool_effect(&pending, &effect, &context, &scope, &owner, attempt)
        .is_err());
    let late = bound_request("root-late");
    assert!(service
        .begin_root_tool_effect(&late, &effect, &context, &scope, &owner, attempt)
        .is_err());
    assert_eq!(
        store
            .stream_revision(&format!("execution-effect:{}", late.idempotency_key))
            .unwrap(),
        0
    );
    assert!(matches!(
        service
            .begin_root_tool_effect(&write, &effect, &context, &scope, &owner, attempt)
            .unwrap(),
        ToolEffectState::Completed(_)
    ));
    let actual = outcome("root-pending", "settled after cancel");
    assert!(service
        .commit_tool_effect(
            &pending,
            &effect,
            &outcome("foreign-call", "settled after cancel")
        )
        .is_err());
    // Persisted admission cannot be borrowed by a new call, different Step,
    // input, attempt or descriptor after cancellation.
    assert!(service.commit_tool_effect(&late, &effect, &actual).is_err());
    let mut wrong = pending.clone();
    wrong.input = "{\"path\":\"foreign\"}".into();
    assert!(service
        .commit_tool_effect(&wrong, &effect, &actual)
        .is_err());
    wrong = pending.clone();
    let original = wrong.step_binding.take().unwrap();
    let mut snapshot = (*original.snapshot).clone();
    snapshot.identity.turn_id.push_str(":foreign");
    wrong.step_binding = Some(crate::step_snapshot::StepCallBinding::persisted(
        original.origin,
        Arc::new(snapshot),
    ));
    assert!(service
        .commit_tool_effect(&wrong, &effect, &actual)
        .is_err());
    wrong = pending.clone();
    wrong.parent_execution_attempt = Some(2);
    assert!(service
        .commit_tool_effect(&wrong, &effect, &actual)
        .is_err());
    let mut wrong_effect = effect.clone();
    wrong_effect.descriptor_hash.push_str(":foreign");
    assert!(service
        .commit_tool_effect(&pending, &wrong_effect, &actual)
        .is_err());
    service
        .commit_tool_effect(&pending, &effect, &actual)
        .unwrap();
    let cursor = store.durable_commit_cursor().unwrap();
    service
        .commit_tool_effect(&pending, &effect, &actual)
        .unwrap();
    assert_eq!(store.durable_commit_cursor().unwrap(), cursor);
    assert!(service
        .commit_tool_effect(
            &pending,
            &effect,
            &outcome("root-pending", "forged outcome")
        )
        .is_err());
    assert!(matches!(
        service
            .begin_root_tool_effect(&pending, &effect, &context, &scope, &owner, attempt)
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
    let scope = root_ledger_scope(&effect);
    let (owner, attempt) = root_ledger_owner(&store, &graph, &node);
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
                .begin_root_tool_effect(&write, &effect, &context, &scope, &owner, attempt)
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


#[test]
fn own864_original_index_allocates_across_native_wave_and_scoped_read_replay_and_recovery() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(store.clone());
    let mut graph = agent_task_graph();
    graph.nodes[0].kind = ExecutionNodeKind::ToolBatch;
    graph.nodes[0].executor_kind = "tool".into();
    let mut graph = service.register_graph(graph).unwrap().graph;
    for status in [ExecutionNodeStatus::Ready, ExecutionNodeStatus::Running] {
        graph = service.transition_node(&graph, &graph.nodes[0].id, status, None, vec![])
            .unwrap().graph;
    }
    let mut write = request("own864-native-claim");
    write.session_id = Some(graph.lineage.as_ref().unwrap().session_id.clone());
    write.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id:graph.id.clone(), node_id:graph.nodes[0].id.clone(),
    });
    write.parent_execution_attempt = Some(1);
    write.observation_wave_sequence = 1;
    let effect = mutation_effect(ToolIdempotency::Idempotent);
    assert_eq!(service.begin_tool_effect(&write, &effect).unwrap(), ToolEffectState::Fresh);
    service.commit_tool_effect(&write, &effect, &outcome("own864-native-claim", "claimed")).unwrap();
    let mut read = write.clone();
    read.idempotency_key = "own864-preview".into();
    read.tool_use_id = "own864-preview".into();
    read.observation_wave_sequence = 1;
    service.commit_readonly_tool_receipts(&[(read.clone(), outcome("own864-preview", "read"))]).unwrap();
    let before = store.list_stream(&delegated_agent_receipt_stream_id(&read).unwrap()).unwrap();
    let recovered = ExecutionCommitService::new(store.clone());
    recovered.commit_tool_effect(&write, &effect, &outcome("own864-native-claim", "claimed")).unwrap();
    recovered.commit_readonly_tool_receipts(&[(read.clone(), outcome("own864-preview", "read"))]).unwrap();
    assert_eq!(before, store.list_stream(&delegated_agent_receipt_stream_id(&read).unwrap()).unwrap());
    let receipts = recovered.load_delegated_agent_tool_receipts(&graph.id, &graph.nodes[0].id, 1).unwrap();
    assert_eq!(receipts.len(), 2);
    assert!(receipts[0].sequence < receipts[1].sequence);
    let source_sequences = before.iter().filter_map(ExecutionCommitService::receipt_observation_sequence)
        .collect::<Vec<_>>();
    assert_eq!(source_sequences, vec![1, 1]);
    let mut false_source = before.iter().find(|event| event.kind == "execution.agent_tool.receipt")
        .unwrap().clone();
    false_source.payload["sequence_base"] = json!(u64::MAX);
    assert!(ExecutionCommitService::receipt_observation_sequence(&false_source).is_none());
    false_source = before.iter().find(|event| event.kind == "execution.agent_tool.receipt")
        .unwrap().clone();
    false_source.payload["index_revision_before"] = json!(u64::MAX);
    assert!(ExecutionCommitService::receipt_observation_sequence(&false_source).is_none());
    assert_eq!(recovered.delegated_observation_high_water(&graph.id, &graph.nodes[0].id, 1).unwrap(), 1);
    let mut later = read.clone();
    later.idempotency_key = "own864-later".into();
    later.tool_use_id = "own864-later".into();
    recovered.commit_readonly_tool_receipts(&[(later, outcome("own864-later", "later"))]).unwrap();
    let final_receipts = recovered.load_delegated_agent_tool_receipts(&graph.id, &graph.nodes[0].id, 1).unwrap();
    assert_eq!(final_receipts.len(), 3);
    assert!(final_receipts[2].sequence > receipts[1].sequence);
    let repeated = request_with_parent(1, "own864-repeat-wave");
    let pair = (repeated.clone(), outcome("own864-repeat-wave", "read"));
    recovered.commit_readonly_tool_receipts(&[pair.clone(), pair]).unwrap();
    let repeat = recovered.load_delegated_agent_tool_receipts("order-graph", "order-node", 7).unwrap();
    assert_eq!(repeat.len(), 1);
    let mut collision = repeated;
    collision.input = "different original input".into();
    assert!(recovered.commit_readonly_tool_receipts(&[(collision,
        outcome("own864-repeat-wave", "read"))]).is_err());
}

#[test]
fn own864_original_index_serializes_independent_concurrent_services_and_same_wave_batch() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let workers = (0..4).map(|worker| {
        let store = store.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let service = ExecutionCommitService::new(store);
            let identifier = format!("own864-concurrent-{worker}");
            let first = request_with_parent(1, &identifier);
            let second_identifier = format!("{identifier}-second");
            let second = request_with_parent(1, &second_identifier);
            barrier.wait();
            service.commit_readonly_tool_receipts(&[
                (first, outcome(&identifier, "first")),
                (second, outcome(&second_identifier, "second")),
            ]).unwrap();
        })
    }).collect::<Vec<_>>();
    for worker in workers { worker.join().unwrap(); }
    let service = ExecutionCommitService::new(store);
    let receipts = service.load_delegated_agent_tool_receipts("order-graph", "order-node", 7).unwrap();
    assert_eq!(receipts.len(), 8);
    assert_eq!(receipts.iter().map(|receipt| receipt.sequence).collect::<BTreeSet<_>>().len(), 8);
    assert!(receipts.windows(2).all(|pair| pair[0].sequence < pair[1].sequence));
}

#[test]
fn own864_original_index_recovers_legacy_high_water_and_rejects_unknown_schema() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let stream = "execution-agent-receipts:order-graph:order-node:7";
    let legacy = RuntimeTransactionEventInput {
        event: RuntimeEventInput {
            stream_id:stream.into(), scope:RuntimeEventScope::ExecutionNode,
            kind:"execution.agent_tool.receipt".into(), status:Some("completed".into()),
            actor:Some("governed_tool".into()), refs:vec![],
            payload:json!({"sequence":1000,"effect_kind":"read","effect_scope":null,
                "authorized_scopes":[],"outcome":outcome("own864-legacy", "legacy")}),
        }, idempotency_key:Some("own864-legacy".into()), schema_version:1,
    };
    store.append_batch_if_revision(stream, 0, "own864-legacy", vec![legacy.clone()]).unwrap();
    let service = ExecutionCommitService::new(store.clone());
    service.commit_readonly_tool_receipts(&[(request_with_parent(1, "own864-after-legacy"),
        outcome("own864-after-legacy", "read"))]).unwrap();
    let receipts = service.load_delegated_agent_tool_receipts("order-graph", "order-node", 7).unwrap();
    assert_eq!(receipts.iter().map(|receipt| receipt.sequence).collect::<Vec<_>>(), vec![1000, 1001]);
    let mut unsupported = legacy;
    unsupported.schema_version = 99;
    unsupported.idempotency_key = Some("own864-unsupported".into());
    store.append_batch_if_revision(stream, 2, "own864-unsupported", vec![unsupported]).unwrap();
    assert!(service.load_delegated_agent_tool_receipts("order-graph", "order-node", 7).is_err());
    assert!(service.commit_readonly_tool_receipts(&[(request_with_parent(1, "own864-refuse"),
        outcome("own864-refuse", "read"))]).is_err());
}

#[test]
fn own864_original_index_concurrent_same_effect_replays_only_once() {
    let store = Arc::new(RuntimeEventStore::for_test());
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let workers = (0..4).map(|_| {
        let store = store.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            let service = ExecutionCommitService::new(store);
            barrier.wait();
            service.commit_readonly_tool_receipts(&[(request_with_parent(1, "own864-same-effect"),
                outcome("own864-same-effect", "read"))]).unwrap();
        })
    }).collect::<Vec<_>>();
    for worker in workers { worker.join().unwrap(); }
    let receipts = ExecutionCommitService::new(store)
        .load_delegated_agent_tool_receipts("order-graph", "order-node", 7).unwrap();
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].sequence, 1);
}

#[test]
fn own864_exact_receipt_sequence_binds_step_and_effect_not_reused_call_id() {
    let workspace = tempfile::tempdir().unwrap();
    std::fs::write(workspace.path().join("same.txt"), "same").unwrap();
    let host = tools::ToolHost::builtin("own864-step-owner", workspace.path());
    let input = json!({"path":"same.txt","complete":true});
    let mut first = crate::bound_tool_batch::scope_admission_tests::bound_request(
        &host, "read_file", input.clone());
    first.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id:"scope2-graph".into(), node_id:"scope2-node".into(),
    });
    first.parent_execution_attempt = Some(1);
    let mut identity = first.step_binding.as_ref().unwrap().snapshot.identity.clone();
    identity.step_id = "own864-second-original-step".into();
    let lease = host.pin_snapshot();
    let definitions = lease.snapshot().catalog.definitions(None).into_iter()
        .filter(|definition| definition.name == "read_file")
        .map(|definition| crate::ProviderToolDefinition {
            name:definition.name, description:definition.description, input_schema:definition.input_schema,
        }).collect::<Vec<_>>();
    let step = Arc::new(crate::step_snapshot::BoundStep::bind(identity, lease, &definitions,
        &json!({}), &json!({}), BTreeMap::new(), i64::MAX).unwrap());
    let binding = crate::step_snapshot::StepCallBinding::provider(step, &first.tool_use_id);
    let mut second = first.clone();
    second.idempotency_key = binding.effect_idempotency_key();
    second.compiled_invocation = Some(binding.prepare_invocation(Some(host.pin_snapshot()),
        "read_file", &input).unwrap());
    second.step_binding = Some(binding);
    assert_eq!(first.tool_use_id, second.tool_use_id);
    assert_ne!(first.idempotency_key, second.idempotency_key);
    let store = Arc::new(RuntimeEventStore::for_test());
    let service = ExecutionCommitService::new(store);
    let mut result = outcome(&first.tool_use_id, "same");
    result.tool_name = "read_file".into();
    service.commit_readonly_tool_receipts(&[(first.clone(), result.clone())]).unwrap();
    service.commit_readonly_tool_receipts(&[(second.clone(), result.clone())]).unwrap();
    let first_sequence = service.exact_delegated_receipt_sequence(&first).unwrap();
    let second_sequence = service.exact_delegated_receipt_sequence(&second).unwrap();
    assert!(first_sequence < second_sequence);
    service.commit_readonly_tool_receipts(&[(second.clone(), result)]).unwrap();
    assert_eq!(service.exact_delegated_receipt_sequence(&second).unwrap(), second_sequence);
    let mut foreign = second.clone();
    foreign.idempotency_key = first.idempotency_key.clone();
    assert!(service.exact_delegated_receipt_sequence(&foreign).is_err());
    foreign = second;
    foreign.observation_wave_sequence += 1;
    assert!(service.exact_delegated_receipt_sequence(&foreign).is_err());
}
