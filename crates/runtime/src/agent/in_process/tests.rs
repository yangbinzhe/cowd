use std::collections::BTreeSet;

use super::*;

use harness_contract::agent::{AgentCommand, AgenticExecutionBinding, AgenticExecutionFocus};
use harness_contract::agent_action::{
    AgentAction, AgentActionEnvelope, AgentActorBinding, AgentActorKind, AgentInviteInput,
    ArtifactCommitInput, TaskClaimInput, TaskPublishInput, TaskSubmitInput, TeamCreateInput,
};
use harness_contract::turn::TurnId;
use sha2::{Digest, Sha256};

fn own834_refusal_error<'outcome>(
    outcome: &'outcome crate::RuntimeToolExecutionOutcome,
    call_id: &str,
    tool_name: &str,
) -> &'outcome str {
    assert_eq!(outcome.tool_use_id, call_id);
    assert_eq!(outcome.tool_name, tool_name);
    assert_eq!(outcome.status, crate::RuntimeToolExecutionStatus::Failed);
    assert!(outcome.output.is_none());
    assert!(outcome.evidence_ref.is_empty());
    assert!(outcome.observed_evidence.is_empty());
    outcome.error.as_deref().expect("refusal has original reason")
}

#[tokio::test]
async fn process_read_confirmation_174_fences_original_cut_and_identity() {
    for mutation in ["none", "source_race", "frame", "fingerprint", "policy"] {
        let root = tempfile::tempdir().unwrap();
        let (services, host, packet) = discovery_test_services(root.path(), true);
        // Use the original scheduler to acquire and retain the parent attempt.
        let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
            services.executor_registry().clone(),
            services.graph_state_store().clone(),
            services.commit_service().clone(),
            services.resource_manager().clone(),
            services.scope_locks().clone(),
            services.worktree_leases().clone(),
            services.workspace_key(),
            services.workspace_root(),
        );
        let snapshot = runner.pump_snapshot(packet.graph_id(), &BTreeSet::new()).await.unwrap();
        assert_eq!(snapshot.ready.len(), 1);
        let wave = runner.prepare_ready_wave(packet.graph_id(), snapshot.ready).await.unwrap();
        assert_eq!(wave.len(), 1);
        let binding = wave[0].binding.as_ref().expect("original scheduler binding");
        assert_eq!(binding.attempt, packet.attempt);
        let running = services.graph_state_store().load(packet.graph_id()).unwrap();
        let (ticket, _) = services.graph_state_store()
            .binding_start_ticket(&running, packet.node_id(), binding).unwrap();
        crate::execution_core::graph::validate_node_ticket_start(services.event_store(), &ticket, None).unwrap();
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-discovery".into(),
            registry_revision: 0,
        };
        let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        let step = bridge.begin_step().unwrap();
        let step_id = step["step"]["identity"]["step_id"].as_str().unwrap();
        let request_id = "confirmation-174";
        let fingerprint = "confirmation-174-original-fingerprint";
        assert!(bridge
            .replay_or_begin_transport_request(request_id, fingerprint)
            .unwrap()
            .is_none());
        let output = bridge
            .execute_step_tool(
                step_id,
                1,
                request_id,
                "read_file",
                r#"{"path":"src/a.rs","complete":true}"#,
            )
            .await
            .unwrap();
        assert!(crate::context_evidence::read_body_seal("read_file", &output).is_some());
        let original_receipts = services.commit_service()
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .unwrap();
        assert!(original_receipts.iter().any(|receipt|
            receipt.outcome.tool_use_id == request_id && receipt.outcome.tool_name == "read_file"));
        assert!(services.commit_service()
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt + 1)
            .unwrap().is_empty());
        let selected = recoverable_effect_receipts(&original_receipts);
        assert!(!selected.is_empty());
        let prompt = recovered_agent_tool_receipt_prompt(&selected, false).unwrap();
        assert!(prompt.contains("Do not call tools") && prompt.contains("ToolHost receipts"));
        assert!(prompt.chars().count() <= 49_000);
        let mut sent = serde_json::json!({
            "kind":"tool_result", "run_id":packet.run_id(), "agent_id":packet.agent_id(),
            "request_id":request_id, "origin_step_id":step_id, "output":output,
        });
        bridge
            .complete_transport_request(request_id, fingerprint, &sent)
            .unwrap();
        let cut = bridge
            .process_read_confirmation_revision("read_file", &sent)
            .unwrap()
            .unwrap();
        assert_eq!(
            bridge
                .process_read_confirmation_revision("edit", &sent)
                .unwrap(),
            None
        );
        assert_eq!(
            bridge
                .process_read_confirmation_revision(
                    "read_file",
                    &serde_json::json!({"error":"failed"})
                )
                .unwrap(),
            None
        );
        sent["protocol_version"] = 3.into();
        sent["sequence"] = 1.into();
        let frame = serde_json::to_vec(&sent).unwrap();
        let digest = crate::context_evidence::read_sha256(&frame);
        let stream = format!("session:{}", packet.session_id());
        if mutation == "source_race" {
            services
                .event_store()
                .append(crate::RuntimeEventInput {
                    stream_id: stream.clone(),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "test.original_source_advanced_after_send_cut".into(),
                    status: None,
                    actor: None,
                    refs: Vec::new(),
                    payload: serde_json::json!({}),
                })
                .unwrap();
        }
        if mutation == "policy" {
            let control = services
                .session_execution_policy_control(packet.session_id())
                .unwrap();
            let mut policy = control.snapshot();
            policy.revision += 1;
            control.replace(policy).unwrap();
        }
        if mutation == "frame" {
            sent["output"] = "not-the-sent-frame".into();
        }
        let supplied_fingerprint = if mutation == "fingerprint" {
            "foreign-fingerprint"
        } else {
            fingerprint
        };
        let result = bridge.record_process_read_delivery(
            "read_file",
            supplied_fingerprint,
            &sent,
            &digest,
            frame.len(),
            cut,
        );
        let deliveries = || {
            services
                .event_store()
                .list_stream(&stream)
                .unwrap()
                .into_iter()
                .filter(|event| event.kind == crate::context_evidence::PROCESS_DELIVERY_KIND)
                .collect::<Vec<_>>()
        };
        if mutation == "none" {
            result.unwrap();
            let original = deliveries();
            assert_eq!(original.len(), 1);
            assert_eq!(original[0].sequence, cut + 1);
            bridge
                .record_process_read_delivery(
                    "read_file",
                    fingerprint,
                    &sent,
                    &digest,
                    frame.len(),
                    cut,
                )
                .unwrap();
            assert_eq!(deliveries(), original);
            let mut conflicting = sent.clone();
            conflicting["output"] = "conflicting presentation".into();
            let bytes = serde_json::to_vec(&conflicting).unwrap();
            assert!(bridge
                .record_process_read_delivery(
                    "read_file",
                    fingerprint,
                    &conflicting,
                    &crate::context_evidence::read_sha256(&bytes),
                    bytes.len(),
                    cut
                )
                .is_err());
            assert_eq!(deliveries(), original);
        } else {
            let error = result.unwrap_err();
            if mutation == "source_race" {
                assert!(
                    error.contains("confirmation unknown") && error.contains("revision mismatch"),
                    "{error}"
                );
            }
            assert!(deliveries().is_empty(), "{mutation}");
        }
        assert_eq!(
            host.calls.load(Ordering::SeqCst),
            1,
            "{mutation}: confirmation must not redo ToolHost"
        );
        assert!(services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }
}

#[test]
fn evidence_retrieve_174_digest_only_receipt_is_not_acquisition_body() {
    let text = "actual immutable small artifact";
    let output = serde_json::json!({
        "kind":"evidence_retrieve", "available":true, "evidence_ref":"artifact://original",
        "sha256":crate::context_evidence::read_sha256(text.as_bytes()), "bytes":text.len(),
        "total_chunks":1, "chunks":[{"index":0,"content":text}],
    })
    .to_string();
    assert!(crate::context_evidence::read_body_seal("evidence_retrieve", &output).is_some());
    let compact = crate::agentic::review_evidence::compact_read_receipt(&output);
    assert!(crate::context_evidence::read_body_seal("evidence_retrieve", &compact).is_none());
    let mut missing: serde_json::Value = serde_json::from_str(&output).unwrap();
    missing["chunks"][0]
        .as_object_mut()
        .unwrap()
        .remove("content");
    assert!(
        crate::context_evidence::read_body_seal("evidence_retrieve", &missing.to_string())
            .is_none()
    );
}


#[tokio::test]
async fn r1_composite_discovery_durable_controls_and_single_axis_rejections() {
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_configured_services(
        root.path(),
        false,
        &[
            "read_file",
            "tool_orchestrate",
            "tool_search",
            "working_context",
        ],
        PermissionMode::ReadOnly,
        vec!["read:src/a.rs".into()],
        true,
    );
    let (live, _, allowed) =
        model_loop::activate_native_tools(host.clone(), &packet, true).unwrap();
    let executor = discovery_native_executor(&services, live, &packet, allowed);
    assert!(executor.tool_batch.is_some());
    let step = discovery_bound_step(
        &executor,
        &packet,
        "native-controls-search",
        PermissionMode::ReadOnly,
    );
    let control_input = r#"{"operation":"list"}"#;
    let control_binding =
        crate::step_snapshot::StepCallBinding::provider(step.clone(), "native-context");
    let authorization = discovery_native_authorization(
        &executor,
        "working_context",
        control_input,
        "native-context",
    );
    let output = executor
        .execute_step_authorized_invocation_output(
            Some(&control_binding),
            "native-context",
            &authorization,
            "working_context",
            control_input,
        )
        .await
        .unwrap();
    let control: serde_json::Value = serde_json::from_str(output.model_text()).unwrap();
    assert_eq!(control["kind"], "runtime.working_context");
    assert_eq!(control["coverage"]["active_references"], 0);
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    let query = "select:edit,web_fetch,execute_code,bash,checkpoint,read_file";
    assert!(!step
        .lease()
        .search(query, 5)
        .activation_candidates
        .contains(&"read_file".into()));
    let mut replay_call = None;
    for (index, max_results) in [None, Some(1), Some(7)].into_iter().enumerate() {
        let mut value = serde_json::json!({"query":query});
        if let Some(limit) = max_results {
            value["max_results"] = serde_json::json!(limit);
        }
        let input = value.to_string();
        let call_id = format!("native-search-{index}");
        let binding = crate::step_snapshot::StepCallBinding::provider(step.clone(), &call_id);
        let authorization =
            discovery_native_authorization(&executor, "tool_search", &input, &call_id);
        let before = host.calls.load(Ordering::SeqCst);
        let forged_id = format!("{call_id}-forged");
        let forged_binding =
            crate::step_snapshot::StepCallBinding::provider(step.clone(), &forged_id);
        let mut forged =
            discovery_native_authorization(&executor, "tool_search", &input, &forged_id);
        forged.authorization_lease.signature.push_str("-forged");
        let error = executor
            .execute_step_authorized_invocation_output(
                Some(&forged_binding),
                &forged_id,
                &forged,
                "tool_search",
                &input,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("signature"), "{error}");
        assert_eq!(host.calls.load(Ordering::SeqCst), before);
        assert!(executor
            .execute_authorized_output(&authorization, "tool_search", &input)
            .await
            .unwrap_err()
            .to_string()
            .contains("production_tool_origin_required"));
        assert!(executor
            .execute_authorized_invocation_output(&call_id, &authorization, "tool_search", &input)
            .await
            .unwrap_err()
            .to_string()
            .contains("production_tool_origin_required"));
        let output = executor
            .execute_step_authorized_invocation_output(
                Some(&binding),
                &call_id,
                &authorization,
                "tool_search",
                &input,
            )
            .await
            .unwrap();
        let receipt: harness_contract::tool::ToolDiscoveryReceipt =
            serde_json::from_str(output.model_text()).unwrap();
        assert_eq!(receipt.query, query);
        assert_eq!(receipt.catalog_revision, step.lease().revision());
        assert_eq!(receipt.activation_candidates, vec!["read_file"]);
        assert_eq!(receipt.descriptors.len(), 1);
        assert_eq!(receipt.descriptors[0].canonical_id, "read_file");
        assert_eq!(host.calls.load(Ordering::SeqCst), before + 1);
        if index == 0 {
            replay_call = Some(crate::conversation::ModelToolCall {
                id: call_id,
                name: "tool_search".into(),
                input,
                depends_on: Vec::new(),
                binding: Some(
                    serde_json::from_value(serde_json::to_value(binding).unwrap()).unwrap(),
                ),
            });
        }
    }
    let missing_query = r#"{"query":"not_a_real_tool_51f72"}"#;
    let missing_binding =
        crate::step_snapshot::StepCallBinding::provider(step.clone(), "native-empty-search");
    let authorization = discovery_native_authorization(
        &executor,
        "tool_search",
        missing_query,
        "native-empty-search",
    );
    let output = executor
        .execute_step_authorized_invocation_output(
            Some(&missing_binding),
            "native-empty-search",
            &authorization,
            "tool_search",
            missing_query,
        )
        .await
        .unwrap();
    let empty: harness_contract::tool::ToolDiscoveryReceipt =
        serde_json::from_str(output.model_text()).unwrap();
    assert!(empty.descriptors.is_empty() && empty.activation_candidates.is_empty());
    for (index, (invalid, field)) in [
        (
            serde_json::json!({"query":"read","max_results":0}),
            "max_results",
        ),
        (
            serde_json::json!({"query":"read","max_results":-1}),
            "max_results",
        ),
        (
            serde_json::json!({"query":"read","max_results":0.5}),
            "max_results",
        ),
        (serde_json::json!({"max_results":1}), "query"),
    ]
    .into_iter()
    .enumerate()
    {
        let call_id = format!("native-invalid-search-{index}");
        let binding = crate::step_snapshot::StepCallBinding::provider(step.clone(), &call_id);
        let input = invalid.to_string();
        let authorization =
            discovery_native_authorization(&executor, "tool_search", &input, &call_id);
        let before = executor.receipts.lock().unwrap().len();
        let calls_before = host.calls.load(Ordering::SeqCst);
        let error = executor
            .execute_step_authorized_invocation_output(
                Some(&binding),
                &call_id,
                &authorization,
                "tool_search",
                &input,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("input violates its pinned schema") && error.contains(field),
            "{error}"
        );
        assert_eq!(executor.receipts.lock().unwrap().len(), before);
        assert_eq!(host.calls.load(Ordering::SeqCst), calls_before);
    }
    let call = replay_call.unwrap();
    let binding = call.binding.as_ref().unwrap();
    let parent = harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: executor.execution_id.clone(),
        node_id: executor.node_id.clone(),
    };
    let key = binding.effect_idempotency_key();
    let query_context = crate::execution_core::graph::ToolReceiptQuery {
        step_binding: binding,
        parent_execution: &parent,
        session_id: &executor.session_id,
        policy_revision: executor.policy_revision,
        idempotency_key: &key,
        tool_use_id: &call.id,
        tool_name: &call.name,
        input: &call.input,
    };
    let historical = services
        .commit_service()
        .query_completed_agent_tool_effect(&query_context)
        .unwrap()
        .unwrap();
    let history: harness_contract::tool::ToolDiscoveryReceipt =
        serde_json::from_str(historical.outcome.output.as_deref().unwrap()).unwrap();
    assert!(!history.activation_candidates.is_empty());
    let process_root = tempfile::tempdir().unwrap();
    let (process_services, process_host, process_packet) = discovery_configured_services(
        process_root.path(),
        true,
        &[
            "read_file",
            "tool_orchestrate",
            "tool_search",
            "working_context",
        ],
        PermissionMode::ReadOnly,
        vec!["read:src/a.rs".into()],
        true,
    );
    let selection = AgentModelSelection {
        model: process_packet.model_lease.clone(),
        provider: "local-discovery".into(),
        registry_revision: 0,
    };
    let bridge =
        ProcessJsonlToolSession::prepare(&process_services, &process_packet, &selection).unwrap();
    let response = bridge.begin_step().unwrap();
    let step_id = response["step"]["identity"]["step_id"].as_str().unwrap();
    let output = bridge
        .execute_step_tool(
            step_id,
            1,
            "process-context",
            "working_context",
            control_input,
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&output).unwrap()["kind"],
        "runtime.working_context"
    );
    let output = bridge
        .execute_step_tool(
            step_id,
            2,
            "process-search",
            "tool_search",
            &serde_json::json!({"query":query,"max_results":1}).to_string(),
        )
        .await
        .unwrap();
    let receipt: harness_contract::tool::ToolDiscoveryReceipt =
        serde_json::from_str(&output).unwrap();
    assert_eq!(receipt.activation_candidates, vec!["read_file"]);
    assert_eq!(
        receipt.catalog_revision,
        process_host.tools.pin_snapshot().revision()
    );
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 2);
    assert!(bridge
        .execute_tool(
            "process-unbound-search",
            "tool_search",
            r#"{"query":"read"}"#
        )
        .await
        .unwrap_err()
        .contains("production_tool_origin_required"));
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 2);
    let unknown_process = r#"{"steps":[{"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},{"id":"unknown","op":"not_an_operation"}]}"#;
    let error = bridge
        .execute_step_tool(
            step_id,
            3,
            "process-unknown",
            "tool_orchestrate",
            unknown_process,
        )
        .await
        .unwrap_err();
    assert!(
        error.contains("input violates its pinned schema") && error.contains("$.steps[1].op"),
        "{error}"
    );
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 2);
    let denied_process = r#"{"steps":[{"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},{"id":"write","op":"call","tool":"edit","input":{"action":"write","path":"src/a.rs","content":"DENIED"}}]}"#;
    let pinned = bridge.steps.lock().unwrap().back().unwrap().clone();
    let denied_binding = crate::step_snapshot::StepCallBinding::process(
        pinned.clone(),
        process_packet.run_id(),
        4,
        "process-unauthorized-child",
    );
    pinned
        .validate_call(
            &denied_binding.origin,
            process_packet.session_id(),
            "process-unauthorized-child",
            "tool_orchestrate",
            denied_process,
            chrono::Utc::now().timestamp_millis(),
        )
        .unwrap();
    let receipts_before = bridge.executor.receipts.lock().unwrap().len();
    let error = bridge
        .execute_step_tool(
            step_id,
            4,
            "process-unauthorized-child",
            "tool_orchestrate",
            denied_process,
        )
        .await
        .unwrap_err();
    assert!(
        error.contains("authorization denied") || error.contains("authorization unavailable"),
        "{error}"
    );
    assert!(!error.contains("pinned schema"), "{error}");
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        bridge.executor.receipts.lock().unwrap().len(),
        receipts_before
    );
    assert_eq!(
        std::fs::read_to_string(process_services.workspace_root().join("src/a.rs")).unwrap(),
        "narrow-read-proof"
    );

    let axis_root = tempfile::tempdir().unwrap();
    let (axis_services, axis_host, axis_packet) = discovery_configured_services(
        axis_root.path(),
        false,
        &[
            "read_file",
            "edit",
            "web_fetch",
            "execute_code",
            "tool_orchestrate",
        ],
        PermissionMode::DangerFullAccess,
        vec!["write:.".into(), "network:*".into()],
        false,
    );
    let mut axis_policy = axis_services
        .session_execution_policy(axis_packet.session_id())
        .unwrap();
    axis_policy.permission_mode = PermissionMode::ReadOnly;
    axis_services.publish_session_execution_policy(
        axis_packet.session_id(),
        crate::permissions::SessionExecutionPolicyControl::from_policy(axis_policy),
    );
    let (live, _, allowed) =
        model_loop::activate_native_tools(axis_host.clone(), &axis_packet, true).unwrap();
    assert_eq!(allowed, axis_packet.allowed_tools.iter().cloned().collect());
    let mut axis_executor = discovery_native_executor(&axis_services, live, &axis_packet, allowed);
    let axis_step = discovery_bound_step(
        &axis_executor,
        &axis_packet,
        "single-axis",
        PermissionMode::DangerFullAccess,
    );
    let mixed = serde_json::json!({"steps":[
        {"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},
        {"id":"first","op":"call","tool":"edit","input":{"action":"write","path":"src/a.rs","content":"DENIED"}},
        {"id":"second","op":"call","tool":"edit","input":{"action":"write","path":"src/sibling.rs","content":"DENIED"}},
        {"id":"network","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"never dispatch"}}
    ]});
    let complete = vec![
        "read:src/a.rs",
        "write:src/a.rs",
        "write:src/sibling.rs",
        "write:.cowd/checkpoints",
        "network:*",
    ];
    axis_executor.resource_scopes = Some(complete.iter().map(|scope| scope.to_string()).collect());
    tool_turn::reject_orchestrated_tools_outside_allow_list(
        &axis_executor.allowed_tools,
        "tool_orchestrate",
        &mixed.to_string(),
    )
    .unwrap();
    axis_executor
        .enforce_resource_ceiling("tool_orchestrate", &mixed.to_string())
        .unwrap();
    let normalized = normalize_delegated_resource_paths(
        "tool_orchestrate",
        &mixed.to_string(),
        axis_services.workspace_root(),
        axis_services.path_identity_resolver(),
        axis_executor.resource_scopes.as_deref(),
    )
    .unwrap();
    for missing in [
        "network:*",
        "write:src/a.rs",
        "write:src/sibling.rs",
        "write:.cowd/checkpoints",
    ] {
        axis_executor.resource_scopes = Some(
            complete
                .iter()
                .filter(|scope| **scope != missing)
                .map(|scope| scope.to_string())
                .collect(),
        );
        let call_id = format!("single-axis-{missing}");
        let authorization = discovery_authorization_with_mode(
            &axis_executor,
            "tool_orchestrate",
            &normalized,
            &call_id,
            PermissionMode::DangerFullAccess,
        );
        let binding = crate::step_snapshot::StepCallBinding::provider(axis_step.clone(), &call_id);
        let error = axis_executor
            .execute_step_authorized_invocation_output(
                Some(&binding),
                &call_id,
                &authorization,
                "tool_orchestrate",
                &normalized,
            )
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("network/File operation resource lease"),
            "{missing}: {error}"
        );
        assert_eq!(axis_host.calls.load(Ordering::SeqCst), 0);
        assert!(axis_executor.receipts.lock().unwrap().is_empty());
    }
    let compute = serde_json::json!({"steps":[
        {"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},
        {"id":"compute","op":"compute","language":"python","code":"print(1)"}
    ]})
    .to_string();
    axis_executor.resource_scopes = Some(vec!["read:.".into()]);
    tool_turn::reject_orchestrated_tools_outside_allow_list(
        &axis_executor.allowed_tools,
        "tool_orchestrate",
        &compute,
    )
    .unwrap();
    axis_executor
        .enforce_resource_ceiling("tool_orchestrate", &compute)
        .unwrap();
    axis_executor.resource_scopes = Some(vec!["read:src/a.rs".into()]);
    let authorization = discovery_authorization_with_mode(
        &axis_executor,
        "tool_orchestrate",
        &compute,
        "single-axis-compute",
        PermissionMode::DangerFullAccess,
    );
    let binding =
        crate::step_snapshot::StepCallBinding::provider(axis_step.clone(), "single-axis-compute");
    assert!(axis_executor
        .execute_step_authorized_invocation_output(
            Some(&binding),
            "single-axis-compute",
            &authorization,
            "tool_orchestrate",
            &compute
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("network/File operation resource lease"));
    axis_executor.resource_scopes = Some(complete.iter().map(|scope| scope.to_string()).collect());
    axis_executor.allowed_tools.remove("web_fetch");
    let authorization = discovery_authorization_with_mode(
        &axis_executor,
        "tool_orchestrate",
        &normalized,
        "single-axis-child",
        PermissionMode::DangerFullAccess,
    );
    let binding =
        crate::step_snapshot::StepCallBinding::provider(axis_step.clone(), "single-axis-child");
    let child_error = axis_executor
        .execute_step_authorized_invocation_output(
            Some(&binding),
            "single-axis-child",
            &authorization,
            "tool_orchestrate",
            &normalized,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(child_error.contains("allow-list"), "{child_error}");
    axis_executor.allowed_tools.insert("web_fetch".into());
    let unknown = r#"{"steps":[{"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},{"id":"unknown","op":"not_an_operation"}]}"#;
    let unknown_effect = axis_step
        .lease()
        .describe_effect("tool_orchestrate", &serde_json::from_str(unknown).unwrap());
    assert_eq!(
        unknown_effect.effect_kind,
        harness_contract::tool::ToolEffectKind::Unknown
    );
    let error = tool_turn::reject_orchestrated_tools_outside_allow_list(
        &axis_executor.allowed_tools,
        "tool_orchestrate",
        unknown,
    )
    .unwrap_err()
    .to_string();
    assert!(
        error.contains("tool_orchestrate input is invalid"),
        "{error}"
    );
    let mode_input = r#"{"action":"write","path":"src/a.rs","content":"DENIED"}"#;
    let effect = axis_executor
        .registered_tool_effect("edit", &serde_json::from_str(mode_input).unwrap())
        .unwrap();
    let policy = permission_policy(None, PermissionMode::ReadOnly, &axis_executor.allowed_tools);
    let denied = crate::AuthorizationNegotiator::new().assess_effective(
        &policy,
        &crate::AuthorizationRequest {
            principal_id: "single-axis-mode".into(),
            capability: "edit".into(),
            input: mode_input.into(),
            idempotency_key: "single-axis-mode".into(),
            effect,
            parent_ceiling: PermissionMode::DangerFullAccess,
            parent_lease_id: None,
            policy_revision: axis_executor.policy_revision,
            recovery_scope: "single-axis".into(),
            context: crate::PermissionContext::default(),
            safe_alternatives: Vec::new(),
        },
    );
    assert!(denied.assessment.lease.is_none());
    axis_executor.resource_scopes = Some(axis_packet.resource_scopes.clone());
    assert!(axis_executor
        .prepare_step_catalog(PermissionMode::DangerFullAccess)
        .is_ok());
    assert!(axis_executor
        .prepare_step_catalog(PermissionMode::ReadOnly)
        .is_err());
    assert!(axis_executor.receipts.lock().unwrap().is_empty());
    let definitions = axis_step
        .lease()
        .snapshot()
        .catalog
        .definitions(Some(&axis_executor.allowed_tools))
        .into_iter()
        .map(|definition| crate::ProviderToolDefinition {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
        })
        .collect();
    let counter = Arc::new(AtomicU64::new(0));
    let bus = crate::CowdEventBus::new();
    let _scope = bus.enter_execution(crate::CowdExecutionContext {
        execution_id: axis_packet.run_id().into(),
        session_id: axis_packet.session_id().into(),
        turn_id: axis_packet
            .assignment
            .execution_identity
            .turn_id()
            .unwrap()
            .into(),
    });
    let mut runtime = discovery_runtime(
        &axis_services,
        &axis_packet,
        Arc::new(axis_executor),
        definitions,
        counter.clone(),
        bus.clone(),
    );
    let error = runtime
        .r1_dispatch_provider_step_for_test(discovery_request())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("agent_tool_inventory_drift"), "{error}");
    assert_eq!(counter.load(Ordering::SeqCst), 0);
    assert_eq!(axis_host.calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        std::fs::read_to_string(axis_services.workspace_root().join("src/a.rs")).unwrap(),
        "narrow-read-proof"
    );
    assert_eq!(
        std::fs::read_to_string(axis_services.workspace_root().join("src/sibling.rs")).unwrap(),
        "untouched-sibling"
    );
    assert!(axis_services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
    assert!(process_services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
    assert!(services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
}

fn discovery_bootstrap_request(
    executor: &ScopedRuntimeToolExecutor,
    binding: crate::step_snapshot::StepCallBinding,
    tool: &str,
    input: &str,
) -> RuntimeToolExecutionRequest {
    let mut request =
        RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
            tool_use_id: binding.origin.call_id.clone(),
            tool_name: tool.into(),
            input: input.into(),
            depends_on: Vec::new(),
        });
    request.governed_plan_id = executor.execution_id.clone();
    request.session_id = Some(executor.session_id.clone());
    request.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: executor.execution_id.clone(),
        node_id: executor.node_id.clone(),
    });
    request.parent_execution_attempt = Some(executor.attempt);
    request.policy_revision = executor.policy_revision;
    request.sandbox_posture = executor.sandbox_posture;
    request.governed_plan_revision = 1;
    request.observation_wave_sequence = 1;
    request.idempotency_key = binding.effect_idempotency_key();
    request.authorization = Some(discovery_native_authorization(
        executor,
        tool,
        input,
        &binding.origin.call_id,
    ));
    request.compiled_invocation = Some(
        binding
            .prepare_invocation(
                executor.pin_step_catalog(),
                tool,
                &serde_json::from_str(input).unwrap(),
            )
            .unwrap(),
    );
    request.step_binding = Some(binding);
    request
}


#[tokio::test]
async fn r1_composite_discovery_metadata_requires_actual_host_exposure() {
    assert!(EchoRuntimeExecutionHost.pin_step_catalog().is_none());
    assert!(EchoRuntimeExecutionHost
        .delegated_tool_definitions(&["read_file".into(), "unregistered_tool".into()])
        .is_empty());
    for process in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (services, host, packet) = discovery_test_services(root.path(), process);
        let packet_before = serde_json::to_vec(&packet).unwrap();
        let (live, definitions, allowed) =
            model_loop::activate_native_tools(host.clone(), &packet, true).unwrap();
        assert!(definitions
            .iter()
            .any(|definition| definition.name == "tool_search"));
        let native = Arc::new(discovery_native_executor(&services, live, &packet, allowed));
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-exposure".into(),
            registry_revision: 0,
        };
        let bridge = process
            .then(|| ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap());
        let executor = bridge
            .as_ref()
            .map_or(native.as_ref(), |bridge| bridge.executor.as_ref());
        let calls = Arc::new(AtomicU64::new(0));
        let bus = crate::CowdEventBus::new();
        let _scope = bus.enter_execution(crate::CowdExecutionContext {
            execution_id: packet.run_id().into(),
            session_id: packet.session_id().into(),
            turn_id: packet
                .assignment
                .execution_identity
                .turn_id()
                .unwrap()
                .into(),
        });
        let mut runtime = discovery_runtime(
            &services,
            &packet,
            native.clone(),
            definitions,
            calls.clone(),
            bus.clone(),
        )
        .with_checkpoint_identity(
            "exposure-workspace",
            Some(packet.assignment.execution_identity.clone()),
        );
        let step = if let Some(bridge) = &bridge {
            bridge.begin_step().unwrap();
            bridge.steps.lock().unwrap().back().unwrap().clone()
        } else {
            runtime
                .r1_dispatch_provider_step_for_test(discovery_request())
                .await
                .unwrap()
                .unwrap()
        };
        let revision = step.lease().revision();
        for mutation in ["missing", "schema", "description", "duplicate", "identical"] {
            *host.exposure.lock().unwrap() = Some(("read_file", mutation));
            let before_metadata = host.exposure_calls.load(Ordering::SeqCst);
            let receipt =
                executor.tool_discovery_receipt_for_step(step.lease(), PermissionMode::ReadOnly);
            assert_eq!(
                host.exposure_calls.load(Ordering::SeqCst),
                before_metadata + 1
            );
            assert!(!receipt
                .descriptors
                .iter()
                .any(|descriptor| descriptor.canonical_id == "read_file"));
            assert!(!receipt.activation_candidates.contains(&"read_file".into()));
            assert!(executor
                .prepare_step_catalog(PermissionMode::ReadOnly)
                .is_err());
            let input = r#"{"query":"select:read_file,tool_orchestrate"}"#;
            let call_id = format!("exposure-search-{mutation}");
            let output = if let Some(bridge) = &bridge {
                bridge
                    .execute_step_tool(
                        &step.snapshot().identity.step_id,
                        1,
                        &call_id,
                        "tool_search",
                        input,
                    )
                    .await
                    .unwrap()
            } else {
                let binding =
                    crate::step_snapshot::StepCallBinding::provider(step.clone(), &call_id);
                let authorization =
                    discovery_native_authorization(executor, "tool_search", input, &call_id);
                executor
                    .execute_step_authorized_invocation_output(
                        Some(&binding),
                        &call_id,
                        &authorization,
                        "tool_search",
                        input,
                    )
                    .await
                    .unwrap()
                    .model_text()
                    .to_string()
            };
            let projected: harness_contract::tool::ToolDiscoveryReceipt =
                serde_json::from_str(&output).unwrap();
            assert!(!projected
                .descriptors
                .iter()
                .any(|descriptor| descriptor.canonical_id == "read_file"));
            assert!(!projected
                .activation_candidates
                .contains(&"read_file".into()));
            assert_eq!(host.tools.pin_snapshot().revision(), revision);
        }
        for mutation in ["missing", "schema", "description", "duplicate", "identical"] {
            *host.exposure.lock().unwrap() = Some(("tool_search", mutation));
            let receipt =
                executor.tool_discovery_receipt_for_step(step.lease(), PermissionMode::ReadOnly);
            assert!(!receipt
                .descriptors
                .iter()
                .any(|descriptor| descriptor.canonical_id == "tool_search"));
            let input = r#"{"query":"select:read_file"}"#;
            let call_id = format!("bootstrap-exposure-{process}-{mutation}");
            let binding = if process {
                crate::step_snapshot::StepCallBinding::process(
                    step.clone(),
                    packet.run_id(),
                    5,
                    &call_id,
                )
            } else {
                crate::step_snapshot::StepCallBinding::provider(step.clone(), &call_id)
            };
            let request = discovery_bootstrap_request(executor, binding, "tool_search", input);
            let before = host.calls.load(Ordering::SeqCst);
            let outcome = executor
                .tool_batch
                .as_ref()
                .unwrap()
                .execute(request)
                .await
                .unwrap();
            let error = own834_refusal_error(&outcome, &call_id, "tool_search");
            assert!(
                error.contains("tool_effect_authority"),
                "{process}/{mutation}: {error}"
            );
            assert_eq!(host.calls.load(Ordering::SeqCst), before);
            assert_eq!(host.tools.pin_snapshot().revision(), revision);
        }
        *host.exposure.lock().unwrap() = Some(("tool_search", "missing"));
        assert!(executor
            .prepare_step_catalog(PermissionMode::ReadOnly)
            .is_ok());
        let receipt =
            executor.tool_discovery_receipt_for_step(step.lease(), PermissionMode::ReadOnly);
        assert!(!receipt
            .descriptors
            .iter()
            .any(|descriptor| descriptor.canonical_id == "tool_search"));
        let before_provider = calls.load(Ordering::SeqCst);
        let before_leaf = host.calls.load(Ordering::SeqCst);
        let error = runtime
            .r1_dispatch_provider_step_for_test(discovery_request())
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("agent_tool_inventory_drift"),
            "real binder: {error}"
        );
        assert_eq!(calls.load(Ordering::SeqCst), before_provider);
        assert_eq!(host.calls.load(Ordering::SeqCst), before_leaf);
        let zero_counter = Arc::new(AtomicU64::new(0));
        let (_, advertised, _) = {
            *host.exposure.lock().unwrap() = None;
            let activated = model_loop::activate_native_tools(host.clone(), &packet, true).unwrap();
            *host.exposure.lock().unwrap() = Some(("tool_search", "missing"));
            activated
        };
        let mut zero_runtime = discovery_runtime(
            &services,
            &packet,
            native.clone(),
            advertised,
            zero_counter.clone(),
            bus.clone(),
        )
        .with_checkpoint_identity(
            "exposure-workspace",
            Some(packet.assignment.execution_identity.clone()),
        );
        assert!(zero_runtime
            .r1_dispatch_provider_step_for_test(discovery_request())
            .await
            .is_err());
        assert_eq!(zero_counter.load(Ordering::SeqCst), 0);
        if let Some(bridge) = &bridge {
            let response = bridge.begin_step().unwrap();
            assert!(!response["tools"]
                .as_array()
                .unwrap()
                .iter()
                .any(|definition| definition["name"] == "tool_search"));
            let step_id = response["step"]["identity"]["step_id"].as_str().unwrap();
            assert!(bridge
                .execute_step_tool(
                    step_id,
                    1,
                    "exposure-ordinary-read",
                    "read_file",
                    r#"{"path":"src/a.rs"}"#
                )
                .await
                .unwrap()
                .contains("narrow-read-proof"));
            let bound = bridge.steps.lock().unwrap().back().unwrap().clone();
            let binding = crate::step_snapshot::StepCallBinding::process(
                bound,
                packet.run_id(),
                1,
                "exposure-ordinary-read",
            );
            discovery_assert_receipt_revision(
                executor,
                &binding,
                "read_file",
                r#"{"path":"src/a.rs"}"#,
                revision,
            );
            assert_eq!(host.calls.load(Ordering::SeqCst), before_leaf + 1);
            assert!(bridge
                .execute_step_tool(
                    step_id,
                    2,
                    "unadvertised-bootstrap",
                    "tool_search",
                    r#"{"query":"read"}"#
                )
                .await
                .is_err());
        }
        *host.exposure.lock().unwrap() = None;
        let input = r#"{"query":"select:read_file"}"#;
        let before_leaf = host.calls.load(Ordering::SeqCst);
        if let Some(bridge) = &bridge {
            let response = bridge.begin_step().unwrap();
            let step_id = response["step"]["identity"]["step_id"].as_str().unwrap();
            let output = bridge
                .execute_step_tool(step_id, 1, "restored-bootstrap", "tool_search", input)
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_str::<harness_contract::tool::ToolDiscoveryReceipt>(&output)
                    .unwrap()
                    .activation_candidates,
                vec!["read_file"]
            );
        } else {
            let fresh = runtime
                .r1_dispatch_provider_step_for_test(discovery_request())
                .await
                .unwrap()
                .unwrap();
            let binding =
                crate::step_snapshot::StepCallBinding::provider(fresh, "restored-bootstrap");
            let authorization = discovery_native_authorization(
                executor,
                "tool_search",
                input,
                "restored-bootstrap",
            );
            let output = executor
                .execute_step_authorized_invocation_output(
                    Some(&binding),
                    "restored-bootstrap",
                    &authorization,
                    "tool_search",
                    input,
                )
                .await
                .unwrap();
            assert_eq!(
                serde_json::from_str::<harness_contract::tool::ToolDiscoveryReceipt>(
                    output.model_text()
                )
                .unwrap()
                .activation_candidates,
                vec!["read_file"]
            );
        }
        assert_eq!(host.calls.load(Ordering::SeqCst), before_leaf + 1);
        assert_eq!(host.tools.pin_snapshot().revision(), revision);
        assert_eq!(serde_json::to_vec(&packet).unwrap(), packet_before);
        assert!(services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }
    struct NoCatalogExecutor;
    #[async_trait::async_trait]
    impl ToolExecutor for NoCatalogExecutor {
        async fn execute_output(
            &self,
            _: &str,
            _: &str,
        ) -> Result<harness_contract::context::ToolOutputDraft, ToolError> {
            panic!("None compatibility must not execute a tool")
        }
    }
    struct NoCatalogProvider(Arc<AtomicU64>);
    impl crate::conversation::ApiClient for NoCatalogProvider {
        fn step_tool_definitions(&self) -> Option<Vec<crate::ProviderToolDefinition>> {
            panic!("None compatibility must return before requesting advertised definitions")
        }
        fn stream(
            &mut self,
            request: crate::conversation::ApiRequest,
        ) -> std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<crate::conversation::AssistantEvent, crate::RuntimeError>,
                    > + Send
                    + '_,
            >,
        > {
            assert!(request.bound_step.is_none());
            self.0.fetch_add(1, Ordering::SeqCst);
            Box::pin(futures::stream::empty())
        }
    }
    let counter = Arc::new(AtomicU64::new(0));
    let mut runtime = crate::ConversationRuntime::new_with_features(
        crate::Session::new(),
        NoCatalogProvider(counter.clone()),
        Arc::new(NoCatalogExecutor),
        PermissionPolicy::new(PermissionMode::ReadOnly),
        vec!["None compatibility".into()],
        &crate::RuntimeFeatureConfig::default(),
    )
    .without_memory();
    assert!(runtime
        .r1_dispatch_provider_step_for_test(discovery_request())
        .await
        .unwrap()
        .is_none());
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[derive(Clone)]
struct DiscoveryProvider {
    definitions: Vec<crate::ProviderToolDefinition>,
    calls: Arc<AtomicU64>,
}

impl crate::conversation::ApiClient for DiscoveryProvider {
    fn step_tool_definitions(&self) -> Option<Vec<crate::ProviderToolDefinition>> {
        Some(self.definitions.clone())
    }

    fn stream(
        &mut self,
        request: crate::conversation::ApiRequest,
    ) -> std::pin::Pin<
        Box<
            dyn futures::Stream<
                    Item = Result<crate::conversation::AssistantEvent, crate::RuntimeError>,
                > + Send
                + '_,
        >,
    > {
        assert!(request.bound_step.is_some());
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(futures::stream::empty())
    }
}

fn discovery_assert_receipt_revision(
    executor: &ScopedRuntimeToolExecutor,
    binding: &crate::step_snapshot::StepCallBinding,
    tool: &str,
    input: &str,
    revision: u64,
) {
    let parent = harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: executor.execution_id.clone(),
        node_id: executor.node_id.clone(),
    };
    let input = normalize_delegated_resource_paths(
        tool,
        input,
        &executor.workspace_root,
        &executor.path_identity_resolver,
        executor.resource_scopes.as_deref(),
    )
    .unwrap();
    let key = binding.effect_idempotency_key();
    let query = crate::execution_core::graph::ToolReceiptQuery {
        step_binding: binding,
        parent_execution: &parent,
        session_id: &executor.session_id,
        policy_revision: executor.policy_revision,
        idempotency_key: &key,
        tool_use_id: &binding.origin.call_id,
        tool_name: tool,
        input: &input,
    };
    let replay = executor
        .commit_service
        .as_ref()
        .unwrap()
        .query_completed_agent_tool_effect(&query)
        .unwrap()
        .unwrap();
    assert_eq!(replay.invocation.catalog_revision, revision);
    assert_eq!(replay.outcome.status, RuntimeToolExecutionStatus::Executed);
}

fn discovery_request() -> crate::conversation::ApiRequest {
    crate::conversation::ApiRequest {
        bound_step: None,
        prompt: crate::PromptAssembly::new(vec!["discovery".into()]),
        messages: Vec::<crate::ConversationMessage>::new().into(),
        model: "local-discovery".into(),
        reasoning_effort_override: None,
        request_compiler_cache_hit: false,
        budget: crate::context_ledger::RequestBudgetReport::for_attempt(
            "local-discovery",
            8192,
            1024,
            0,
            0,
            0,
        ),
        preparation: Default::default(),
        provider_evidence_context: None,
    }
}

fn discovery_runtime(
    services: &Arc<RuntimeServices>,
    packet: &AgentTaskPacket,
    executor: Arc<ScopedRuntimeToolExecutor>,
    definitions: Vec<crate::ProviderToolDefinition>,
    calls: Arc<AtomicU64>,
    bus: crate::CowdEventBus,
) -> crate::ConversationRuntime<DiscoveryProvider, ScopedRuntimeToolExecutor> {
    let mut session = crate::Session::new();
    session.session_id = packet.session_id().into();
    let policy = permission_policy(
        services.session_execution_policy_control(packet.session_id()),
        packet.permission_ceiling,
        &executor.allowed_tools,
    );
    crate::ConversationRuntime::new_with_features(
        session,
        DiscoveryProvider { definitions, calls },
        executor,
        policy,
        vec!["discovery".into()],
        &crate::RuntimeFeatureConfig::default(),
    )
    .without_memory()
    .with_runtime_event_store(Arc::clone(services.event_store()))
    .with_cowd_event_bus(bus)
    .with_checkpoint_identity(
        services.workspace_key(),
        Some(packet.assignment.execution_identity.clone()),
    )
}

fn discovery_control_definition() -> tools::RuntimeToolDefinition {
    tools::RuntimeToolDefinition {
        name: "working_context".into(),
        description: Some("Runtime working context".into()),
        input_schema: serde_json::to_value(schemars::schema_for!(
            crate::working_context::WorkingContextInput
        ))
        .unwrap(),
        required_permission: PermissionMode::ReadOnly,
        effect_resolver: harness_contract::tool::ToolEffectResolverSpec {
            resolver_id: "runtime.readonly".into(),
            resolver_version: 1,
        },
    }
}

fn discovery_replace_runtime(
    host: &DiscoveryTestHost,
    definitions: Vec<tools::RuntimeToolDefinition>,
) {
    let catalog = tools::ToolCatalog::builtin()
        .with_runtime_tools(definitions)
        .unwrap();
    host.tools.replace_snapshot(tools::ToolHostSnapshot::new(
        Arc::new(catalog),
        Arc::new(tools::lsp_client::LspRegistry::new()),
        None,
    ));
}

fn discovery_bound_step(
    executor: &ScopedRuntimeToolExecutor,
    packet: &AgentTaskPacket,
    step_id: &str,
    mode: PermissionMode,
) -> Arc<crate::step_snapshot::BoundStep> {
    assert_ne!(packet.run_id(), packet.graph_id());
    let lease = executor.prepare_step_catalog(mode).unwrap().unwrap();
    let definitions = lease
        .snapshot()
        .catalog
        .definitions(Some(&executor.allowed_tools))
        .into_iter()
        .map(|definition| crate::ProviderToolDefinition {
            name: definition.name,
            description: definition.description,
            input_schema: definition.input_schema,
        })
        .collect::<Vec<_>>();
    let step = Arc::new(
        crate::step_snapshot::BoundStep::bind(
            serde_json::from_value(serde_json::json!({
                "step_id":step_id, "session_id":packet.session_id(),
                "turn_id":packet.assignment.execution_identity.turn_id().unwrap(),
                "execution_id":packet.run_id(), "run_ref":packet.run_id(),
                "actor_ref":packet.assignment.execution_identity.principal_id(), "attempt":null,
                "build_ref":"discovery-livepin"
            }))
            .unwrap(),
            lease,
            &definitions,
            &serde_json::json!({}),
            &serde_json::json!({}),
            BTreeMap::from([("policy".into(), executor.policy_revision.to_string())]),
            chrono::Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap(),
    );
    assert_eq!(
        step.snapshot().identity.execution_id.as_deref(),
        Some(packet.run_id())
    );
    assert_eq!(
        step.snapshot().identity.run_ref.as_deref(),
        Some(packet.run_id())
    );
    assert_eq!(
        step.snapshot().identity.actor_ref.as_deref(),
        Some(packet.assignment.execution_identity.principal_id())
    );
    assert_eq!(step.snapshot().identity.attempt, None);
    step
}

fn discovery_authorization_with_mode(
    executor: &ScopedRuntimeToolExecutor,
    tool: &str,
    input: &str,
    call_id: &str,
    mode: PermissionMode,
) -> harness_contract::tool::ToolExecutionAuthorization {
    discovery_authorization_with_mode_and_issuer(executor, tool, input, call_id, mode, None)
}

fn discovery_authorization_with_mode_and_issuer(
    executor: &ScopedRuntimeToolExecutor,
    tool: &str,
    input: &str,
    call_id: &str,
    mode: PermissionMode,
    issuer_store: Option<&crate::RuntimeEventStore>,
) -> harness_contract::tool::ToolExecutionAuthorization {
    use harness_contract::policy::{
        ApprovalContext, ApprovalDecisionActor, ApprovalDecisionActorKind, ApprovalDecisionCommand,
        ApprovalDomain, ApprovalGrantScope, ApprovalGrantStatus, ApprovalSource,
        ApprovalSourceKind, ApprovalTimeoutPolicy, AuthorizationPath, CapabilityGapKind, RiskLevel,
        SubmitApprovalRequest,
    };
    let input_value = serde_json::from_str(input).unwrap();
    let pin = executor
        .host
        .pin_step_catalog()
        .expect("fixture requires the real pinned ToolHost");
    let effect = pin.describe_effect(tool, &input_value);
    assert_eq!(
        executor.registered_tool_effect(tool, &input_value).unwrap(),
        effect
    );
    let required = crate::AuthorizationNegotiator::compile_effective_descriptor(&effect, input)
        .descriptor
        .required_permission;
    assert!(
        mode.permits(required),
        "fixture ceiling must cover {tool}/{call_id}: {required:?}"
    );
    let policy = permission_policy(None, mode, &executor.allowed_tools);
    let request = crate::AuthorizationRequest {
        principal_id: "discovery-native".into(),
        capability: tool.into(),
        input: input.into(),
        idempotency_key: call_id.into(),
        effect,
        parent_ceiling: mode,
        parent_lease_id: None,
        policy_revision: executor.policy_revision,
        recovery_scope: "discovery-native".into(),
        context: crate::PermissionContext::default(),
        safe_alternatives: Vec::new(),
    };
    let negotiator = crate::AuthorizationNegotiator::new();
    let evaluated = negotiator.assess_effective(&policy, &request);
    let mut assessment = evaluated.assessment.clone();
    if assessment.lease.is_none() {
        assert_eq!(
            assessment.path,
            AuthorizationPath::HumanApproval,
            "{tool}/{call_id}: {assessment:?}"
        );
        assert_eq!(
            assessment.gap.as_ref().unwrap().kind,
            CapabilityGapKind::ApprovalRequired
        );
        let store = Arc::new(crate::RuntimeEventStore::for_test());
        let approvals = crate::ApprovalQueue::new(store.clone());
        let source = ApprovalSource {
            kind: ApprovalSourceKind::Session,
            session_id: Some(executor.session_id.clone()),
            agent_id: None,
            team_id: None,
            mission_id: None,
            resource_ref: None,
            review_ref: None,
            application: None,
        };
        let mut context = ApprovalContext::owned(
            &source,
            tool,
            executor.path_identity_resolver.workspace_id(),
        );
        context.principal_id = request.principal_id.clone();
        context.invocation_id = Some(call_id.into());
        context.execution_id = Some(executor.execution_id.clone());
        context.effect = Some(evaluated.effective.descriptor.clone());
        context.policy_revision = executor.policy_revision;
        context.resource_targets = evaluated
            .effective
            .descriptor
            .scopes
            .iter()
            .map(|scope| scope.target.clone())
            .collect::<Option<Vec<_>>>()
            .unwrap_or_default();
        let risk = match assessment.risk {
            RiskLevel::Low => harness_contract::core::TaskRisk::Low,
            RiskLevel::Medium => harness_contract::core::TaskRisk::Medium,
            RiskLevel::High => harness_contract::core::TaskRisk::High,
            RiskLevel::Critical => harness_contract::core::TaskRisk::Critical,
        };
        let pending = approvals
            .submit_scoped_with_policy(
                format!("fixture-approval:{call_id}"),
                SubmitApprovalRequest {
                    source,
                    context,
                    action: format!("execute:{tool}"),
                    summary: format!(
                        "Human fixture approves this exact pinned invocation: {call_id}"
                    ),
                    risk,
                    domain: ApprovalDomain::Execution,
                    blocks_execution: true,
                    evidence_refs: vec![evaluated.effective.fingerprint.clone()],
                    timeout_policy: ApprovalTimeoutPolicy::AutoDeny,
                },
                None,
                false,
                vec![ApprovalGrantScope::Once],
            )
            .unwrap();
        let human = crate::security::test_human_interactive_principal();
        approvals
            .decide(
                &human,
                ApprovalDecisionCommand {
                    approval_id: pending.approval_id.clone(),
                    approved: true,
                    skip: false,
                    reason: "explicit fixture human decision for exact descriptor and invocation"
                        .into(),
                    scope: ApprovalGrantScope::Once,
                    actor: ApprovalDecisionActor {
                        kind: ApprovalDecisionActorKind::Human,
                        actor_id: human.claims().principal_id.clone(),
                    },
                    evidence_refs: vec![evaluated.effective.fingerprint.clone()],
                },
            )
            .unwrap();
        let grant = approvals.grant_for_approval(&pending.approval_id).unwrap();
        assert_eq!(grant.issued_by.kind, ApprovalDecisionActorKind::Human);
        for mutation in ["revoked", "invocation", "policy", "descriptor"] {
            let mut invalid = grant.clone();
            match mutation {
                "revoked" => invalid.status = ApprovalGrantStatus::Revoked,
                "invocation" => invalid.invocation_id = Some(format!("{call_id}-foreign")),
                "policy" => invalid.policy_revision += 1,
                "descriptor" => invalid.effect_descriptor_hash = Some("foreign".into()),
                _ => unreachable!(),
            }
            assert!(
                negotiator
                    .approve_effective(&policy, &request, &evaluated.effective, &invalid)
                    .lease
                    .is_none(),
                "{mutation}"
            );
        }
        assessment = negotiator.approve_effective(&policy, &request, &evaluated.effective, &grant);
        assert_eq!(assessment.path, AuthorizationPath::HumanApproval);
        approvals.consume_once_grant(&grant.grant_id).unwrap();
        assert!(approvals.active_grants().is_empty());
        assert!(
            store
                .stream_revision(&format!("approval:{}", pending.approval_id))
                .unwrap()
                > 0
        );
    }
    let signed = assessment
        .lease
        .clone()
        .unwrap_or_else(|| panic!("real authorization refused {tool}/{call_id}: {assessment:?}"));
    assert!(crate::AuthorizationNegotiator::verify_lease_signature(
        &signed
    ));
    let authorized = crate::ToolPolicy
        .authorize(&evaluated.effective, &assessment, call_id, signed, 30)
        .unwrap()
        .authorization;
    pin.validate_authorization(&authorized, tool, &input_value)
        .unwrap();
    if let Some(store) = issuer_store {
        let _ = negotiator.take_transitions_for_persistence();
        for transition in negotiator.transitions_awaiting_persistence() {
            crate::authorization_negotiator::persist_authorization_transition(
                store,
                &format!("authorization-lease:{}", transition.lease.lease_id),
                "own834.discovery_fixture",
                &transition,
            ).unwrap();
            negotiator.acknowledge_persisted_transitions(
                std::slice::from_ref(&transition.transition_id),
            );
        }
        assert!(store.stream_revision(&format!(
            "authorization-lease:{}", authorized.authorization_lease.lease_id,
        )).unwrap() > 0);
    }
    authorized
}

#[tokio::test]
async fn r1_composite_discovery_reload_uses_current_step_catalog_or_refuses_before_model() {
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_test_services(root.path(), false);
    // Use the original scheduler to acquire and retain the parent attempt.
    let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
        services.executor_registry().clone(),
        services.graph_state_store().clone(),
        services.commit_service().clone(),
        services.resource_manager().clone(),
        services.scope_locks().clone(),
        services.worktree_leases().clone(),
        services.workspace_key(),
        services.workspace_root(),
    );
    let snapshot = runner.pump_snapshot(packet.graph_id(), &BTreeSet::new()).await.unwrap();
    assert_eq!(snapshot.ready.len(), 1);
    let wave = runner.prepare_ready_wave(packet.graph_id(), snapshot.ready).await.unwrap();
    assert_eq!(wave.len(), 1);
    let binding = wave[0].binding.as_ref().expect("original scheduler binding");
    assert_eq!(binding.attempt, packet.attempt);
    let running = services.graph_state_store().load(packet.graph_id()).unwrap();
    let (ticket, _) = services.graph_state_store()
        .binding_start_ticket(&running, packet.node_id(), binding).unwrap();
    crate::execution_core::graph::validate_node_ticket_start(services.event_store(), &ticket, None).unwrap();
    let (live, definitions, allowed) =
        model_loop::activate_native_tools(host.clone(), &packet, true).unwrap();
    let executor = Arc::new(discovery_native_executor(&services, live, &packet, allowed));
    let calls = Arc::new(AtomicU64::new(0));
    let bus = crate::CowdEventBus::new();
    let _execution = bus.enter_execution(crate::CowdExecutionContext {
        execution_id: packet.run_id().into(),
        session_id: packet.session_id().into(),
        turn_id: packet
            .assignment
            .execution_identity
            .turn_id()
            .unwrap()
            .into(),
    });
    let mut runtime = discovery_runtime(
        &services,
        &packet,
        executor.clone(),
        definitions,
        calls.clone(),
        bus.clone(),
    );
    let mut stale_compile = discovery_request();
    stale_compile.budget.compiled_policy_revision = Some(executor.policy_revision + 1);
    assert!(runtime
        .r1_dispatch_provider_step_for_test(stale_compile)
        .await
        .unwrap_err()
        .to_string()
        .contains("context_policy_changed_after_compilation"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let old_step = runtime
        .r1_dispatch_provider_step_for_test(discovery_request())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    host.tools
        .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
    let current = host.tools.pin_snapshot();
    assert_ne!(current.revision(), old_step.lease().revision());
    let receipts_before = executor.receipts.lock().unwrap().len();
    let old_binding = crate::step_snapshot::StepCallBinding::provider(old_step, "native-late");
    let restored: crate::step_snapshot::StepCallBinding =
        serde_json::from_value(serde_json::to_value(old_binding).unwrap()).unwrap();
    let input = r#"{"path":"src/a.rs"}"#;
    let authorization =
        discovery_native_authorization(&executor, "read_file", input, "native-late");
    let error = executor
        .execute_step_authorized_invocation_output(
            Some(&restored),
            "native-late",
            &authorization,
            "read_file",
            input,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("step_binding_stale"), "{error}");
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
    assert_eq!(executor.receipts.lock().unwrap().len(), receipts_before);
    let step = runtime
        .r1_dispatch_provider_step_for_test(discovery_request())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(step.lease().revision(), current.revision());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    for (call_id, tool, value) in [
        (
            "native-live-read",
            "read_file",
            serde_json::json!({"path":"src/a.rs"}),
        ),
        (
            "native-live-composite",
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"read","op":"call","tool":"read_file","input":{"path":"src/a.rs"}}
            ]}),
        ),
    ] {
        let input = value.to_string();
        let authorization = discovery_native_authorization(&executor, tool, &input, call_id);
        let binding = crate::step_snapshot::StepCallBinding::provider(step.clone(), call_id);
        let output = executor
            .execute_step_authorized_invocation_output(
                Some(&binding),
                call_id,
                &authorization,
                tool,
                &input,
            )
            .await
            .unwrap();
        assert!(output.model_text().contains("narrow-read-proof"));
        discovery_assert_receipt_revision(&executor, &binding, tool, &input, current.revision());
    }
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);
    assert_eq!(executor.receipts.lock().unwrap().len(), receipts_before + 2);
    let durable = services
        .commit_service()
        .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
        .unwrap();
    assert_eq!(durable.len(), 2);
    assert!(durable.iter().any(|receipt|
        receipt.outcome.tool_use_id == "native-live-read" && receipt.outcome.tool_name == "read_file"));
    assert!(services.commit_service()
        .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt + 1)
        .unwrap().is_empty());
    let selected = recoverable_effect_receipts(&durable);
    assert!(!selected.is_empty());
    let prompt = recovered_agent_tool_receipt_prompt(&selected, false).unwrap();
    assert!(prompt.contains("Do not call tools") && prompt.contains("ToolHost receipts"));
    assert!(prompt.chars().count() <= 49_000);
    assert!(durable
        .iter()
        .all(|receipt| receipt.outcome.status == RuntimeToolExecutionStatus::Executed));
    assert_eq!(
        executor
            .tool_discovery_receipt_for_step(step.lease(), PermissionMode::ReadOnly)
            .catalog_revision,
        current.revision()
    );
    let mut missing_pin = discovery_native_executor(
        &services,
        host.clone(),
        &packet,
        packet.allowed_tools.iter().cloned().collect(),
    );
    missing_pin.host = Arc::new(NoopRuntimeExecutionHost);
    assert!(missing_pin
        .prepare_step_catalog(PermissionMode::ReadOnly)
        .unwrap_err()
        .to_string()
        .contains("catalog unavailable"));
    assert!(!missing_pin.has_tool("read_file"));
    assert!(missing_pin
        .tool_discovery_receipt()
        .activation_candidates
        .is_empty());

    for mutation in ["schema", "presentation", "resolver", "floor", "missing"] {
        let case_root = tempfile::tempdir().unwrap();
        let (case_services, case_host, case_packet) = discovery_configured_services(
            case_root.path(),
            false,
            &["read_file", "tool_orchestrate", "working_context"],
            PermissionMode::ReadOnly,
            vec!["read:src/a.rs".into()],
            true,
        );
        let original = discovery_control_definition();
        let (live, advertised, allowed) =
            model_loop::activate_native_tools(case_host.clone(), &case_packet, true).unwrap();
        let executor = Arc::new(discovery_native_executor(
            &case_services,
            live,
            &case_packet,
            allowed,
        ));
        let old_step = discovery_bound_step(
            &executor,
            &case_packet,
            "before-incompatible-reload",
            PermissionMode::ReadOnly,
        );
        let old_metadata = executor.tool_discovery_receipt();
        let mut changed = original;
        match mutation {
            "schema" => {
                changed.input_schema =
                    serde_json::json!({"type":"object","properties":{"query":{"type":"string"}}})
            }
            "presentation" => changed.description = Some("changed presentation".into()),
            "resolver" => changed.effect_resolver.resolver_version += 1,
            "floor" => changed.required_permission = PermissionMode::WorkspaceWrite,
            "missing" => {}
            _ => unreachable!(),
        }
        discovery_replace_runtime(
            &case_host,
            if mutation == "missing" {
                vec![]
            } else {
                vec![changed]
            },
        );
        let current_metadata = executor.tool_discovery_receipt();
        assert_ne!(
            current_metadata.catalog_revision,
            old_metadata.catalog_revision
        );
        if mutation == "missing" {
            assert!(!executor.has_tool("working_context"));
            assert!(!current_metadata
                .activation_candidates
                .contains(&"working_context".into()));
        } else {
            assert!(executor.has_tool("working_context"));
            let fresh = case_host.tools.pin_snapshot().catalog_receipt();
            let descriptor = current_metadata
                .descriptors
                .iter()
                .find(|descriptor| descriptor.canonical_id == "working_context")
                .unwrap();
            assert!(fresh.descriptors.contains(descriptor));
            if mutation == "floor" {
                assert_eq!(
                    descriptor.required_permission,
                    PermissionMode::WorkspaceWrite
                );
                let mut policy = case_services
                    .session_execution_policy(case_packet.session_id())
                    .unwrap();
                policy.permission_mode = PermissionMode::DangerFullAccess;
                case_services.publish_session_execution_policy(
                    case_packet.session_id(),
                    crate::permissions::SessionExecutionPolicyControl::from_policy(policy),
                );
            }
            if mutation == "schema" {
                assert_ne!(
                    descriptor.schema_hash,
                    old_metadata
                        .descriptors
                        .iter()
                        .find(|descriptor| descriptor.canonical_id == "working_context")
                        .unwrap()
                        .schema_hash
                );
            }
        }
        let counter = Arc::new(AtomicU64::new(0));
        let case_bus = crate::CowdEventBus::new();
        let _case_execution = case_bus.enter_execution(crate::CowdExecutionContext {
            execution_id: case_packet.run_id().into(),
            session_id: case_packet.session_id().into(),
            turn_id: case_packet
                .assignment
                .execution_identity
                .turn_id()
                .unwrap()
                .into(),
        });
        let mut runtime = discovery_runtime(
            &case_services,
            &case_packet,
            executor.clone(),
            advertised,
            counter.clone(),
            case_bus.clone(),
        );
        if mutation == "resolver" {
            assert!(executor
                .prepare_step_catalog(PermissionMode::ReadOnly)
                .is_ok());
            let binding =
                crate::step_snapshot::StepCallBinding::provider(old_step, "resolver-late");
            let persisted: crate::step_snapshot::StepCallBinding =
                serde_json::from_value(serde_json::to_value(binding).unwrap()).unwrap();
            assert!(persisted
                .resolve(Some(case_host.tools.pin_snapshot()))
                .unwrap_err()
                .to_string()
                .contains("step_binding_stale"));
        } else {
            let error = runtime
                .r1_dispatch_provider_step_for_test(discovery_request())
                .await
                .unwrap_err()
                .to_string();
            assert!(
                error.contains("agent_tool_inventory_drift")
                    || error.contains("disagrees with pinned catalog"),
                "{mutation}: {error}"
            );
        }
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(case_host.calls.load(Ordering::SeqCst), 0);
        assert!(executor.receipts.lock().unwrap().is_empty());
        assert!(case_services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }

    let process_root = tempfile::tempdir().unwrap();
    let (process_services, process_host, process_packet) =
        discovery_test_services(process_root.path(), true);
    // This Process fixture has its own store and must retain its own scheduler owner.
    let process_runner = crate::execution_core::graph::ExecutionGraphRunner::new(
        process_services.executor_registry().clone(),
        process_services.graph_state_store().clone(),
        process_services.commit_service().clone(),
        process_services.resource_manager().clone(),
        process_services.scope_locks().clone(),
        process_services.worktree_leases().clone(),
        process_services.workspace_key(),
        process_services.workspace_root(),
    );
    let process_snapshot = process_runner
        .pump_snapshot(process_packet.graph_id(), &BTreeSet::new())
        .await
        .unwrap();
    assert_eq!(process_snapshot.ready.len(), 1);
    let process_wave = process_runner
        .prepare_ready_wave(process_packet.graph_id(), process_snapshot.ready)
        .await
        .unwrap();
    assert_eq!(process_wave.len(), 1);
    let process_binding = process_wave[0]
        .binding
        .as_ref()
        .expect("original Process scheduler binding");
    assert_eq!(process_binding.attempt, process_packet.attempt);
    let process_running = process_services.graph_state_store()
        .load(process_packet.graph_id()).unwrap();
    let (process_ticket, _) = process_services.graph_state_store()
        .binding_start_ticket(&process_running, process_packet.node_id(), process_binding)
        .unwrap();
    crate::execution_core::graph::validate_node_ticket_start(
        process_services.event_store(), &process_ticket, None,
    ).unwrap();
    let selection = AgentModelSelection {
        model: process_packet.model_lease.clone(),
        provider: "local-discovery".into(),
        registry_revision: 0,
    };
    let bridge =
        ProcessJsonlToolSession::prepare(&process_services, &process_packet, &selection).unwrap();
    let old = bridge.begin_step().unwrap();
    let old_id = old["step"]["identity"]["step_id"].as_str().unwrap();
    process_host
        .tools
        .replace_snapshot(process_host.tools.pin_snapshot().snapshot().clone());
    let before_stale = process_services.commit_service()
        .load_delegated_agent_tool_receipts(
            process_packet.graph_id(), process_packet.node_id(), process_packet.attempt,
        ).unwrap();
    let error = bridge
        .execute_step_tool(old_id, 1, "process-late", "read_file", input)
        .await
        .unwrap_err();
    assert_eq!(error, "step_binding_stale");
    assert!(!bridge.requests.lock().unwrap().calls.contains_key("process-late"));
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 0);
    assert!(bridge.executor.receipts.lock().unwrap().is_empty());
    assert_eq!(process_services.commit_service()
        .load_delegated_agent_tool_receipts(
            process_packet.graph_id(), process_packet.node_id(), process_packet.attempt,
        ).unwrap(), before_stale);
    let retry = bridge
        .execute_step_tool(old_id, 1, "process-late", "read_file", input)
        .await
        .unwrap_err();
    assert_eq!(retry, error);
    assert!(!bridge.requests.lock().unwrap().calls.contains_key("process-late"));
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 0);
    assert_eq!(process_services.commit_service()
        .load_delegated_agent_tool_receipts(
            process_packet.graph_id(), process_packet.node_id(), process_packet.attempt,
        ).unwrap(), before_stale);
    assert!(bridge
        .execute_step_tool("foreign-step", 2, "process-foreign", "read_file", input)
        .await
        .is_err());
    let next = bridge.begin_step().unwrap();
    let next_id = next["step"]["identity"]["step_id"].as_str().unwrap();
    for (sequence, tool, value) in [
        (1, "read_file", serde_json::json!({"path":"src/a.rs"})),
        (
            2,
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"read","op":"call","tool":"read_file","input":{"path":"src/a.rs"}}
            ]}),
        ),
    ] {
        let call_id = format!("process-live-{sequence}");
        let output = bridge
            .execute_step_tool(next_id, sequence, &call_id, tool, &value.to_string())
            .await
            .unwrap();
        assert!(output.contains("narrow-read-proof"));
        let bound = bridge.steps.lock().unwrap().back().unwrap().clone();
        let binding = crate::step_snapshot::StepCallBinding::process(
            bound,
            process_packet.run_id(),
            sequence,
            &call_id,
        );
        discovery_assert_receipt_revision(
            &bridge.executor,
            &binding,
            tool,
            &value.to_string(),
            process_host.tools.pin_snapshot().revision(),
        );
    }
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 2);
    let bound = bridge.steps.lock().unwrap().back().unwrap().clone();
    assert_eq!(
        bound.lease().revision(),
        process_host.tools.pin_snapshot().revision()
    );
    let durable = process_services
        .commit_service()
        .load_delegated_agent_tool_receipts(
            process_packet.graph_id(),
            process_packet.node_id(),
            process_packet.attempt,
        )
        .unwrap();
    assert_eq!(durable.len(), 2);
    assert_eq!(
        durable[..]
            .iter()
            .map(|receipt| (
                receipt.outcome.tool_use_id.as_str(),
                receipt.outcome.tool_name.as_str(),
                receipt.outcome.status.clone()
            ))
            .collect::<Vec<_>>(),
        vec![
            (
                "process-live-1",
                "read_file",
                RuntimeToolExecutionStatus::Executed
            ),
            (
                "process-live-2",
                "tool_orchestrate",
                RuntimeToolExecutionStatus::Executed
            ),
        ]
    );

    // A completed original call keeps its owner even after the catalog moves.
    process_host.tools.replace_snapshot(process_host.tools.pin_snapshot().snapshot().clone());
    let replay = bridge
        .execute_step_tool(next_id, 1, "process-live-1", "read_file", input)
        .await
        .unwrap();
    assert!(replay.contains("narrow-read-proof"));
    assert_eq!(process_host.calls.load(Ordering::SeqCst), 2);
    assert_eq!(process_services.commit_service()
        .load_delegated_agent_tool_receipts(
            process_packet.graph_id(), process_packet.node_id(), process_packet.attempt,
        ).unwrap(), durable);

    for mutation in [
        "schema",
        "presentation",
        "resolver",
        "floor",
        "missing",
        "scope",
        "policy",
        "pin",
    ] {
        let case_root = tempfile::tempdir().unwrap();
        let (case_services, case_host, case_packet) = discovery_configured_services(
            case_root.path(),
            true,
            &["read_file", "tool_orchestrate", "working_context"],
            PermissionMode::ReadOnly,
            vec!["read:src/a.rs".into()],
            true,
        );
        // Current-read cases and the scope denial use this case's original parent owner.
        let _case_scheduler_owner = if matches!(mutation, "schema" | "presentation" | "resolver" | "scope") {
            let runner = crate::execution_core::graph::ExecutionGraphRunner::new(
                case_services.executor_registry().clone(),
                case_services.graph_state_store().clone(),
                case_services.commit_service().clone(),
                case_services.resource_manager().clone(),
                case_services.scope_locks().clone(),
                case_services.worktree_leases().clone(),
                case_services.workspace_key(),
                case_services.workspace_root(),
            );
            let snapshot = runner
                .pump_snapshot(case_packet.graph_id(), &BTreeSet::new())
                .await
                .unwrap();
            assert_eq!(snapshot.ready.len(), 1);
            let wave = runner
                .prepare_ready_wave(case_packet.graph_id(), snapshot.ready)
                .await
                .unwrap();
            assert_eq!(wave.len(), 1);
            let binding = wave[0].binding.as_ref().expect("original Process scheduler binding");
            assert_eq!(binding.attempt, case_packet.attempt);
            let running = case_services.graph_state_store()
                .load(case_packet.graph_id()).unwrap();
            let (ticket, _) = case_services.graph_state_store()
                .binding_start_ticket(&running, case_packet.node_id(), binding)
                .unwrap();
            crate::execution_core::graph::validate_node_ticket_start(
                case_services.event_store(), &ticket, None,
            ).unwrap();
            Some((runner, wave))
        } else {
            None
        };
        let original = discovery_control_definition();
        let selection = AgentModelSelection {
            model: case_packet.model_lease.clone(),
            provider: "local-discovery".into(),
            registry_revision: 0,
        };
        let mut bridge =
            ProcessJsonlToolSession::prepare(&case_services, &case_packet, &selection).unwrap();
        let old = bridge.begin_step().unwrap();
        let original_step = bridge.steps.lock().unwrap().back().unwrap().clone();
        let mut changed = original;
        match mutation {
            "schema" => {
                changed.input_schema =
                    serde_json::json!({"type":"object","properties":{"query":{"type":"string"}}})
            }
            "presentation" => changed.description = Some("Process current presentation".into()),
            "resolver" => changed.effect_resolver.resolver_version += 1,
            "floor" => changed.required_permission = PermissionMode::WorkspaceWrite,
            "missing" => {}
            "scope" => {
                Arc::get_mut(&mut bridge.executor).unwrap().resource_scopes = Some(Vec::new())
            }
            "policy" => {
                let control = case_services
                    .session_execution_policy_control(case_packet.session_id())
                    .unwrap();
                let mut snapshot = control.snapshot();
                snapshot.revision += 1;
                control.replace(snapshot).unwrap();
            }
            "pin" => {
                Arc::get_mut(&mut bridge.executor).unwrap().host =
                    Arc::new(NoopRuntimeExecutionHost)
            }
            _ => unreachable!(),
        }
        discovery_replace_runtime(
            &case_host,
            if mutation == "missing" {
                vec![]
            } else {
                vec![changed]
            },
        );
        if mutation == "scope" {
            // Discovery authenticates the interface; the actual call must honor
            // the empty resource ceiling before executing the host effect.
            let current = bridge.begin_step().unwrap();
            let step_id = current["step"]["identity"]["step_id"].as_str().unwrap();
            let error = bridge
                .execute_step_tool(step_id, 1, "process-scope-denied", "read_file", input)
                .await
                .unwrap_err();
            assert!(
                error.contains("outside the Agent network/File operation resource lease"),
                "scope: {error}"
            );
            assert_eq!(case_host.calls.load(Ordering::SeqCst), 0);
            assert!(bridge.executor.receipts.lock().unwrap().is_empty());
            let durable = case_services.commit_service()
                .load_delegated_agent_tool_receipts(
                    case_packet.graph_id(), case_packet.node_id(), case_packet.attempt,
                ).unwrap();
            assert!(durable.iter().all(|receipt|
                receipt.outcome.tool_use_id != "process-scope-denied"
                    || receipt.outcome.status != RuntimeToolExecutionStatus::Executed));
        } else if matches!(mutation, "floor" | "missing" | "policy" | "pin") {
            let error = bridge.begin_step().unwrap_err();
            assert!(
                error.contains("agent_tool_inventory_drift")
                    || error.contains("policy revision is stale"),
                "{mutation}: {error}"
            );
        } else {
            let current = bridge.begin_step().unwrap();
            let step_id = current["step"]["identity"]["step_id"].as_str().unwrap();
            let actual = case_host.tools.pin_snapshot();
            let definitions: Vec<crate::ProviderToolDefinition> =
                serde_json::from_value(current["tools"].clone()).unwrap();
            for definition in definitions {
                assert!(actual
                    .snapshot()
                    .catalog
                    .definitions(None)
                    .iter()
                    .any(|registered| registered.name == definition.name
                        && registered.input_schema == definition.input_schema
                        && registered.description == definition.description));
            }
            assert!(bridge
                .execute_step_tool(step_id, 1, "process-current-read", "read_file", input)
                .await
                .unwrap()
                .contains("narrow-read-proof"));
            assert_eq!(case_host.calls.load(Ordering::SeqCst), 1);
            let current_step = bridge.steps.lock().unwrap().back().unwrap().clone();
            let binding = crate::step_snapshot::StepCallBinding::process(
                current_step.clone(),
                case_packet.run_id(),
                1,
                "process-current-read",
            );
            discovery_assert_receipt_revision(
                &bridge.executor,
                &binding,
                "read_file",
                input,
                actual.revision(),
            );
            assert_eq!(current_step.lease().revision(), actual.revision());
        }
        let old_binding = crate::step_snapshot::StepCallBinding::process(
            original_step,
            case_packet.run_id(),
            2,
            "old-after-reload",
        );
        let persisted: crate::step_snapshot::StepCallBinding =
            serde_json::from_value(serde_json::to_value(old_binding).unwrap()).unwrap();
        let original_stale = persisted
            .resolve(Some(case_host.tools.pin_snapshot()))
            .unwrap_err();
        assert_eq!(original_stale.to_string(), "step_binding_stale");
        let before = case_host.calls.load(Ordering::SeqCst);
        let before_receipts = bridge.executor.receipts.lock().unwrap().len();
        let old_error = bridge
            .execute_step_tool(
                old["step"]["identity"]["step_id"].as_str().unwrap(),
                2,
                "old-after-reload",
                "read_file",
                input,
            )
            .await
            .unwrap_err();
        if mutation == "pin" {
            assert_eq!(old_error, "step_tool_host_unavailable");
        } else {
            assert_eq!(old_error, original_stale.to_string(), "{mutation}");
        }
        assert!(!bridge.requests.lock().unwrap().calls.contains_key("old-after-reload"));
        assert_eq!(case_host.calls.load(Ordering::SeqCst), before);
        assert_eq!(
            bridge.executor.receipts.lock().unwrap().len(),
            before_receipts
        );
        assert_eq!(
            std::fs::read_to_string(case_services.workspace_root().join("src/a.rs")).unwrap(),
            "narrow-read-proof"
        );
        assert!(case_services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }
    assert!(process_services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
    assert!(services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
}

pub(super) struct DiscoveryTestHost {
    pub(super) tools: tools::ToolHost,
    pub(super) calls: AtomicU64,
    services: Mutex<Weak<RuntimeServices>>,
    exposure: Mutex<Option<(&'static str, &'static str)>>,
    exposure_calls: AtomicU64,
    effect_override: Mutex<Option<harness_contract::tool::ToolEffectDescriptor>>,
}

impl DiscoveryTestHost {
    pub(super) fn new(root: &std::path::Path) -> Self {
        Self {
            tools: tools::ToolHost::builtin("discovery-fixture", root)
                .with_authorization_lease_verifier(Arc::new(
                    crate::AuthorizationNegotiator::verify_lease_signature,
                )),
            calls: AtomicU64::new(0),
            services: Mutex::new(Weak::new()),
            exposure: Mutex::new(None),
            exposure_calls: AtomicU64::new(0),
            effect_override: Mutex::new(None),
        }
    }
}

#[async_trait::async_trait]
impl RuntimeExecutionHost for DiscoveryTestHost {
    fn bounded_process_authority(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<crate::tool_host::BoundedProcessAuthority, crate::ToolError> {
        let services =
            self.services.lock().unwrap().upgrade().ok_or_else(|| {
                crate::ToolError::new("bounded fixture original Runtime unavailable")
            })?;
        crate::tool_host::BoundedProcessAuthority::bind(services, request.clone())
    }

    fn bounded_process_lease(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> Result<tools::ToolHostLease, crate::ToolError> {
        let binding = request
            .step_binding
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("bounded fixture requires original Step"))?;
        let step = binding
            .resolve(Some(self.tools.pin_snapshot()))
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        let input = step
            .validate_call(
                &binding.origin,
                request
                    .session_id
                    .as_deref()
                    .ok_or_else(|| crate::ToolError::new("missing Session"))?,
                &request.tool_use_id,
                &request.tool_name,
                &request.input,
                chrono::Utc::now().timestamp_millis(),
            )
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        let authorization = request
            .authorization
            .as_ref()
            .ok_or_else(|| crate::ToolError::new("missing original authorization"))?;
        step.lease()
            .validate_authorization(authorization, &request.tool_name, &input)
            .map_err(|error| crate::ToolError::new(error.to_string()))?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(step.lease().clone())
    }

    fn bounded_process_plane(&self) -> Option<Arc<crate::ToolExecutionPlane>> {
        self.services
            .lock()
            .unwrap()
            .upgrade()
            .map(|services| Arc::clone(services.tool_execution_plane()))
    }

    fn pin_step_catalog(&self) -> Option<tools::ToolHostLease> {
        Some(self.tools.pin_snapshot())
    }

    fn delegated_tool_definitions(&self, names: &[String]) -> Vec<crate::ProviderToolDefinition> {
        self.exposure_calls.fetch_add(1, Ordering::SeqCst);
        let requested = names.iter().cloned().collect::<BTreeSet<_>>();
        let mut definitions = self
            .tools
            .pin_snapshot()
            .snapshot()
            .catalog
            .definitions(Some(&requested))
            .into_iter()
            .map(|definition| crate::ProviderToolDefinition {
                name: definition.name,
                description: definition.description,
                input_schema: definition.input_schema,
            })
            .collect::<Vec<_>>();
        if let Some((name, mutation)) = *self.exposure.lock().unwrap() {
            if mutation == "missing" {
                definitions.retain(|definition| definition.name != name);
            } else if let Some(definition) = definitions
                .iter_mut()
                .find(|definition| definition.name == name)
            {
                match mutation {
                    "schema" => definition.input_schema = serde_json::json!({"type":"object"}),
                    "description" => {
                        definition.description = Some("changed actual exposure".into())
                    }
                    "duplicate" | "identical" => {
                        let mut conflict = definition.clone();
                        if mutation == "duplicate" {
                            conflict.description = Some("conflicting duplicate".into());
                        }
                        definitions.push(conflict);
                    }
                    _ => panic!("unknown exposure mutation"),
                }
            }
        }
        definitions
    }

    fn delegated_tool_effect_descriptor(
        &self,
        name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        let lease = self.tools.pin_snapshot();
        lease.snapshot().catalog.canonical_name(name)?;
        if name == "tool_search" {
            if let Some(effect) = self.effect_override.lock().unwrap().clone() {
                return Some(effect);
            }
        }
        Some(lease.describe_effect(name, input))
    }

    async fn execute_runtime_tool(
        &self,
        request: &RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        let prepared = (|| {
            let binding = request.step_binding.as_ref().ok_or("missing Step")?;
            let step = binding
                .resolve(Some(self.tools.pin_snapshot()))
                .map_err(|error| error.to_string())?;
            let input = step
                .validate_call(
                    &binding.origin,
                    request.session_id.as_deref().ok_or("missing Session")?,
                    &request.tool_use_id,
                    &request.tool_name,
                    &request.input,
                    chrono::Utc::now().timestamp_millis(),
                )
                .map_err(|error| error.to_string())?;
            let authorization = request
                .authorization
                .as_ref()
                .ok_or("missing authorization")?;
            step.lease()
                .validate_authorization(authorization, &request.tool_name, &input)
                .map_err(|error| error.to_string())?;
            Ok::<_, String>((step, input))
        })();
        let mut typed_observations = Vec::new();
        let result = match prepared {
            Err(error) => Err(error),
            Ok((step, input)) => {
                self.calls.fetch_add(1, Ordering::SeqCst);
                if request.tool_name == "evidence_retrieve" {
                    // Synthetic bytes exercise the real Process bridge and receipt
                    // owner; this fixture does not claim an ArtifactStore read.
                    let content = "synthetic-process-evidence";
                    Ok(serde_json::json!({"kind":"evidence_retrieve","available":true,
                        "evidence_ref":input["evidence_ref"],
                        "sha256":crate::context_evidence::read_sha256(content.as_bytes()),
                        "bytes":content.len(),"total_chunks":1,
                        "chunks":[{"index":0,"content":content}]}).to_string())
                } else if request.tool_name == "working_context" {
                    let services = self.services.lock().unwrap().upgrade();
                    match services.zip(request.memory_context.as_ref()) {
                        Some((services, context))
                            if request.session_id.as_deref()
                                == Some(context.session_id.as_str()) =>
                        {
                            match serde_json::from_value::<
                                crate::working_context::WorkingContextInput,
                            >(input)
                            {
                                Ok(command) => services
                                    .working_context_command(
                                        context,
                                        &request.tool_use_id,
                                        command,
                                        vec![format!("session:{}", context.session_id)],
                                    )
                                    .await
                                    .and_then(|output| {
                                        serde_json::to_string(&output)
                                            .map_err(|error| error.to_string())
                                    }),
                                Err(error) => Err(error.to_string()),
                            }
                        }
                        _ => Err("working context Runtime/Session binding missing".into()),
                    }
                } else {
                    let lease = step.lease().clone();
                    let authorization = request.authorization.as_ref().unwrap().clone();
                    let tool_name = request.tool_name.clone();
                    if request.compiled_invocation.as_ref().is_some_and(|invocation| {
                        invocation.effect.uses_network && !invocation.effect.spawns_process
                    }) {
                        match (crate::ExecutionCommitService::local_network_binding(request), request.session_id.as_deref(), request.step_binding.as_ref()) {
                            (Err(error), _, _) => Err(error.to_string()),
                            (Ok(binding), Some(session), Some(original)) => lease.execute_joined_local_network(
                                &authorization, &tool_name, &request.input, binding, session, &original.origin,
                            ).await.map(|(output, fact)| {
                                typed_observations.push(fact.into_observation(
                                    request.tool_name.clone(), request.observation_wave_sequence,
                                ));
                                output
                            }).map_err(|error| {
                                // A failed joined call carries the facts it
                                // produced; they travel through the same
                                // observed evidence slot.
                                if let Some(fact) = error.local_network_fact() {
                                    typed_observations.push(fact.into_observation(
                                        request.tool_name.clone(), request.observation_wave_sequence,
                                    ));
                                }
                                error.to_string()
                            }),
                            _ => Err("local network original Session/Step missing".into()),
                        }
                    } else {
                        crate::ToolExecutionPlane::adapt_blocking(move || {
                            lease
                                .execute(&authorization, &tool_name, &input)
                                .map_err(|error| error.to_string())
                        })
                        .await
                        .map_err(|error| error.to_string())
                        .and_then(|result| result)
                    }
                }
            }
        };
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: if result.is_ok() {
                RuntimeToolExecutionStatus::Executed
            } else {
                RuntimeToolExecutionStatus::Failed
            },
            category: request.category,
            output: result.as_ref().ok().cloned(),
            error: result.err(),
            evidence_ref: format!("real-toolhost:{}", request.tool_use_id),
            observed_evidence: typed_observations,
            pending_agent_batch: None,
        }
    }
}

pub(super) fn discovery_test_services(
    root: &std::path::Path,
    process: bool,
) -> (
    Arc<RuntimeServices>,
    Arc<DiscoveryTestHost>,
    AgentTaskPacket,
) {
    discovery_configured_services(
        root,
        process,
        &["read_file", "tool_orchestrate"],
        PermissionMode::ReadOnly,
        vec!["read:src/a.rs".into()],
        false,
    )
}

fn discovery_configured_services(
    root: &std::path::Path,
    process: bool,
    names: &[&str],
    ceiling: PermissionMode,
    scopes: Vec<String>,
    controls: bool,
) -> (
    Arc<RuntimeServices>,
    Arc<DiscoveryTestHost>,
    AgentTaskPacket,
) {
    use harness_contract::agent::{
        AgentDefinitionId, AgentExecutorPolicy, DefinitionScope, ReleaseAssignment,
        ReleaseAssignmentStatus, ReleaseAuthorization, ReleaseChannel, RevisionSelector,
    };
    let workspace = root.join("workspace");
    std::fs::create_dir_all(workspace.join("src")).unwrap();
    std::fs::write(workspace.join("src/a.rs"), "narrow-read-proof").unwrap();
    std::fs::write(workspace.join("src/sibling.rs"), "untouched-sibling").unwrap();
    let host = Arc::new(DiscoveryTestHost::new(&workspace));
    if controls || names.contains(&"evidence_retrieve") {
        let mut definitions = Vec::new();
        if controls { definitions.push(discovery_control_definition()); }
        if names.contains(&"evidence_retrieve") {
            definitions.push(tools::RuntimeToolDefinition {
                name:"evidence_retrieve".into(), description:Some("Read immutable evidence".into()),
                input_schema:serde_json::json!({"type":"object","properties":{"evidence_ref":{"type":"string"}},
                    "required":["evidence_ref"],"additionalProperties":false}),
                required_permission:PermissionMode::ReadOnly,
                effect_resolver:harness_contract::tool::ToolEffectResolverSpec {
                    resolver_id:"runtime.readonly".into(), resolver_version:1,
                },
            });
        }
        discovery_replace_runtime(&host, definitions);
    }
    let services = RuntimeServices::test_builder(root.join("home"), &workspace)
        .tool_execution_host(host.clone())
        .build()
        .unwrap();
    *host.services.lock().unwrap() = Arc::downgrade(&services);
    let mut packet = test_agent_packet(Vec::new());
    packet.allowed_tools = names.iter().map(|name| name.to_string()).collect();
    packet.resource_scopes = scopes;
    packet.permission_ceiling = ceiling;
    let definition = if names.contains(&"web_fetch") {
        "cowd/autonomous"
    } else {
        "cowd/execute"
    };
    let mut definition_id = AgentDefinitionId::new(DefinitionScope::Builtin, definition).unwrap();
    if process {
        let registry = services.definition_registry();
        let base = registry
            .resolve_agent(&definition_id, RevisionSelector::LatestApprovedStable)
            .unwrap();
        let spec = crate::ProcessJsonlSpec::new(
            "command:discovery-worker",
            "discovery-worker",
            Vec::new(),
        );
        let mut manifest = base.revision.manifest.clone();
        definition_id =
            AgentDefinitionId::new(DefinitionScope::Workspace, "discovery/process-reader").unwrap();
        manifest.definition_id = definition_id.clone();
        manifest.executor = AgentExecutorPolicy::ProcessJsonl {
            command_ref: spec.command_ref,
            command_digest: spec.command_digest,
        };
        let stored = registry
            .agents()
            .store_revision(manifest, &base.agent_markdown)
            .unwrap();
        registry
            .agents()
            .record_release_assignment(&ReleaseAssignment {
                scope: DefinitionScope::Workspace,
                revision_ref: stored.revision.revision_ref.clone(),
                channel: ReleaseChannel::Stable,
                status: ReleaseAssignmentStatus::Active,
                authorization: ReleaseAuthorization::HumanApproval {
                    approval_ref: "test:discovery-process".into(),
                },
                content_digest: stored.revision.content_digest,
            })
            .unwrap();
    }
    let mut request = crate::AgentBindingRequest::new(
        definition_id,
        RevisionSelector::LatestApprovedStable,
        packet.agent_id(),
        packet.session_id(),
        packet.task_id(),
    );
    request.team_id = packet.team_id().map(str::to_string);
    request.granted_capabilities = vec![harness_contract::agent::AgentCapability::Read];
    if names.contains(&"tool_search") {
        request
            .granted_capabilities
            .push(harness_contract::agent::AgentCapability::Search);
    }
    if names.contains(&"edit") {
        request
            .granted_capabilities
            .push(harness_contract::agent::AgentCapability::Write);
    }
    if names.contains(&"execute_code") {
        request
            .granted_capabilities
            .push(harness_contract::agent::AgentCapability::Test);
    }
    if names.contains(&"web_fetch") {
        request
            .granted_capabilities
            .push(harness_contract::agent::AgentCapability::Network);
    }
    request.allowed_tool_contract_refs = packet.allowed_tools.clone();
    let binding = crate::AgentBindingCompiler::new(Arc::clone(services.definition_registry()))
        .compile(request)
        .unwrap()
        .snapshot;
    assert_eq!(
        binding
            .tool_contract_refs
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        packet.allowed_tools.iter().cloned().collect()
    );
    packet.assignment = crate::test_support::agent_assignment(
        Some(binding.definition_ref.clone()),
        packet.agent_id(),
        packet.run_id(),
        packet.task_id(),
        packet.session_id(),
        packet.mission_id(),
        packet.team_id(),
        packet.graph_id(),
        packet.node_id(),
    );
    packet.binding = Some(binding);
    let mut graph = harness_contract::execution_graph::ExecutionGraph::new("discovery fixture");
    graph.id = packet.graph_id().into();
    graph.lineage = Some(harness_contract::execution_graph::ExecutionGraphLineage {
        session_id: packet.session_id().into(),
        turn_id: packet
            .assignment
            .execution_identity
            .turn_id()
            .unwrap()
            .into(),
        root_task_id: packet.assignment.root_task_id.clone(),
        task_id: packet.task_id().into(),
        generation: 1,
    });
    let mut node = harness_contract::execution_graph::ExecutionNodeSpec::new(
        harness_contract::execution_graph::ExecutionNodeKind::AgentTask,
        "agent_task",
        serde_json::to_string(&packet).unwrap(),
    );
    node.id = packet.node_id().into();
    graph.nodes.push(node);
    services.commit_service().register_graph(graph).unwrap();
    services.publish_session_execution_policy(
        packet.session_id(),
        crate::permissions::SessionExecutionPolicyControl::from_policy(
            harness_contract::policy::SessionExecutionPolicy::from_profile(
                harness_contract::policy::AutonomyProfileId::Supervised,
                packet.policy_revision,
                harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
            ),
        ),
    );
    (services, host, packet)
}

fn discovery_native_executor(
    services: &Arc<RuntimeServices>,
    host: Arc<dyn RuntimeExecutionHost>,
    packet: &AgentTaskPacket,
    allowed_tools: BTreeSet<String>,
) -> ScopedRuntimeToolExecutor {
    let policy = services
        .session_execution_policy(packet.session_id())
        .unwrap();
    ScopedRuntimeToolExecutor {
        tool_batch: Some(
            crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
                services, packet,
            ),
        ),
        host,
        allowed_tools,
        session_id: packet.session_id().into(),
        sandbox_posture: policy.sandbox_posture,
        policy_revision: policy.revision,
        memory_context: memory::MemoryTurnContext::new(packet.session_id(), packet.agent_id()),
        reality_context: packet
            .binding
            .as_ref()
            .map(|binding| binding.data_lease.clone()),
        model_lease: packet.model_lease.clone(),
        execution_id: packet.graph_id().into(),
        node_id: packet.node_id().into(),
        attempt: packet.attempt,
        workspace_root: services.workspace_root().into(),
        path_identity_resolver: Arc::clone(services.path_identity_resolver()),
        scope_locks: Arc::clone(services.scope_locks()),
        commit_service: Some(services.commit_service().clone()),
        resource_scopes: Some(packet.resource_scopes.clone()),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    }
}

#[tokio::test]
async fn bounded_process_fixture_live_authority_rejects_revoked_session_before_source_copy_287() {
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_configured_services(
        root.path(),
        false,
        &["execute_code"],
        PermissionMode::DangerFullAccess,
        vec!["read:.".into()],
        false,
    );
    let (live, _, allowed) =
        model_loop::activate_native_tools(host.clone(), &packet, true).unwrap();
    let executor = discovery_native_executor(&services, live, &packet, allowed);
    let input =
        serde_json::json!({"language":"python","code":"print(open('/project/src/a.rs').read())",
        "input_files":["src/a.rs"],"timeout_ms":5000})
        .to_string();
    let step = discovery_bound_step(
        &executor,
        &packet,
        "live-authority-287",
        PermissionMode::DangerFullAccess,
    );
    let binding = crate::step_snapshot::StepCallBinding::provider(step, "live-authority-call-287");
    let authorization = discovery_authorization_with_mode(
        &executor,
        "execute_code",
        &input,
        "live-authority-call-287",
        PermissionMode::DangerFullAccess,
    );
    let mut request =
        RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
            tool_use_id: "live-authority-call-287".into(),
            tool_name: "execute_code".into(),
            input: input.clone(),
            depends_on: vec![],
        });
    request.compiled_invocation = Some(
        binding
            .prepare_invocation(
                Some(host.tools.pin_snapshot()),
                "execute_code",
                &serde_json::from_str(&input).unwrap(),
            )
            .unwrap(),
    );
    request.idempotency_key = binding.effect_idempotency_key();
    request.step_binding = Some(binding);
    request.authorization = Some(authorization);
    request.session_id = Some(packet.session_id().into());
    request.policy_revision = executor.policy_revision;
    request.sandbox_posture = executor.sandbox_posture;
    request.authorized_scopes = packet.resource_scopes.clone();
    request.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: packet.graph_id().into(),
        node_id: packet.node_id().into(),
    });
    request.parent_execution_attempt = Some(packet.attempt);
    let mut graph = services
        .graph_state_store()
        .load(packet.graph_id())
        .unwrap();
    for status in [
        harness_contract::execution_graph::ExecutionNodeStatus::Ready,
        harness_contract::execution_graph::ExecutionNodeStatus::Running,
    ] {
        graph = services
            .commit_service()
            .transition_node(&graph, packet.node_id(), status, None, vec![])
            .unwrap()
            .graph;
    }
    let effect = &request.compiled_invocation.as_ref().unwrap().effect;
    let scope = crate::bound_tool_batch::admitted_effect_scope(
        &request,
        effect,
        Some(host.tools.pin_snapshot()),
        services.path_identity_resolver(),
    )
    .unwrap();
    assert_eq!(
        services
            .commit_service()
            .begin_bound_tool_effect(&request, effect, &scope)
            .unwrap(),
        crate::execution_core::graph::ToolEffectState::Fresh
    );
    let authority = host.bounded_process_authority(&request).unwrap();
    authority.check(&request).unwrap();
    let mut substituted = request.clone();
    substituted.input.push(' ');
    assert!(authority
        .check(&substituted)
        .unwrap_err()
        .to_string()
        .contains("immutable authority request changed"));
    let control = services
        .session_execution_policy_control(packet.session_id())
        .unwrap();
    let mut policy = control.snapshot();
    policy.revision += 1;
    control.replace(policy).unwrap();
    assert!(authority
        .check(&request)
        .unwrap_err()
        .to_string()
        .contains("live Session authority revoked or changed"));
    assert!(host.bounded_process_authority(&request).is_err());
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
    assert!(services
        .commit_service()
        .completed_tool_effect(&request, effect)
        .unwrap()
        .is_none());
    assert_eq!(
        std::fs::read_to_string(services.workspace_root().join("src/a.rs")).unwrap(),
        "narrow-read-proof"
    );
    assert!(services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
}

fn discovery_native_authorization(
    executor: &ScopedRuntimeToolExecutor,
    tool: &str,
    input: &str,
    call_id: &str,
) -> harness_contract::tool::ToolExecutionAuthorization {
    let descriptor = executor
        .registered_tool_effect(tool, &serde_json::from_str(input).unwrap())
        .unwrap();
    let policy = permission_policy(None, PermissionMode::ReadOnly, &executor.allowed_tools);
    let negotiator = crate::AuthorizationNegotiator::new();
    let evaluated = negotiator.assess_effective(
        &policy,
        &crate::AuthorizationRequest {
            principal_id: "discovery-native".into(),
            capability: tool.into(),
            input: input.into(),
            idempotency_key: call_id.into(),
            effect: descriptor,
            parent_ceiling: PermissionMode::ReadOnly,
            parent_lease_id: None,
            policy_revision: executor.policy_revision,
            recovery_scope: "discovery-native".into(),
            context: crate::PermissionContext::default(),
            safe_alternatives: Vec::new(),
        },
    );
    crate::ToolPolicy
        .authorize(
            &evaluated.effective,
            &evaluated.assessment,
            call_id,
            evaluated.assessment.lease.clone().unwrap(),
            30,
        )
        .unwrap()
        .authorization
}

#[test]
fn r1_composite_discovery_pinned_catalog_does_not_authorize_unknown_calls() {
    let root = tempfile::tempdir().unwrap();
    let host = DiscoveryTestHost::new(root.path());
    let requested = BTreeSet::from(["read_file".into(), "tool_orchestrate".into()]);
    let lease = host.tools.pin_snapshot();
    let metadata = lease.catalog_receipt();
    // This catalog-only check is independent of the retired, ungranted
    // tool_search execution fixture.
    let definition = lease.snapshot().catalog
        .definitions(Some(&BTreeSet::from(["tool_search".into()])))
        .pop().expect("builtin tool_search definition");
    for mutation in ["registration", "resolver", "version"] {
        let mut resolver = tools::builtin_effect_resolver_spec("tool_search");
        if mutation == "resolver" { resolver.resolver_id = "runtime.readonly".into(); }
        if mutation == "version" { resolver.resolver_version += 1; }
        let shadow = tools::RuntimeToolDefinition {
            name: "tool_search".into(), description: definition.description.clone(),
            input_schema: definition.input_schema.clone(), required_permission: PermissionMode::ReadOnly,
            effect_resolver: resolver,
        };
        assert!(tools::ToolCatalog::builtin().with_runtime_tools(vec![shadow]).is_err(),
            "registration layer must reject builtin shadow: {mutation}");
    }
    for definition in
        host.delegated_tool_definitions(&requested.iter().cloned().collect::<Vec<_>>())
    {
        let narrow = vec!["read:src/a.rs".into()];
        let discovered = crate::tool_host::discover_delegated_tool(
            &lease,
            &metadata,
            &definition,
            &requested,
            PermissionMode::ReadOnly,
            Some(&narrow),
        )
        .unwrap();
        assert_eq!(
            discovered,
            *metadata
                .descriptors
                .iter()
                .find(|descriptor| descriptor.canonical_id == definition.name)
                .unwrap()
        );
        assert!(!crate::tool_host::effect_has_resource_lease(
            &lease.describe_effect(&definition.name, &serde_json::json!({})),
            &narrow,
            crate::tool_host::lexical_resource_path_is_authorized,
        ));
        assert!(!delegated_tool_is_eligible(
            None,
            None,
            &definition,
            &requested,
            PermissionMode::ReadOnly,
            Some(&narrow)
        ));
        assert!(crate::tool_host::discover_delegated_tool(
            &lease,
            &metadata,
            &definition,
            &BTreeSet::new(),
            PermissionMode::ReadOnly,
            Some(&narrow),
        )
        .is_none());
        let mut fake_schema = definition.clone();
        fake_schema.input_schema = serde_json::json!({"type":"object"});
        assert!(crate::tool_host::discover_delegated_tool(
            &lease,
            &metadata,
            &fake_schema,
            &requested,
            PermissionMode::ReadOnly,
            Some(&narrow),
        )
        .is_none());

    }
    let edit = host
        .delegated_tool_definitions(&["edit".into()])
        .pop()
        .unwrap();
    assert!(crate::tool_host::discover_delegated_tool(
        &lease,
        &metadata,
        &edit,
        &BTreeSet::from(["edit".into()]),
        PermissionMode::ReadOnly,
        Some(&["write:src/a.rs".into()]),
    )
    .is_none());
    let mut unknown = edit;
    unknown.name = "unregistered_read_file".into();
    assert!(crate::tool_host::discover_delegated_tool(
        &lease,
        &metadata,
        &unknown,
        &BTreeSet::from([unknown.name.clone()]),
        PermissionMode::DangerFullAccess,
        Some(&["read:src/a.rs".into()]),
    )
    .is_none());
    let custom = tools::RuntimeToolDefinition {
        name: "custom_read_file".into(),
        description: None,
        input_schema: serde_json::json!({"type":"object"}),
        required_permission: tools::permissions::PermissionMode::ReadOnly,
        effect_resolver: tools::builtin_effect_resolver_spec("read_file"),
    };
    let catalog = tools::ToolCatalog::builtin()
        .with_runtime_tools(vec![custom.clone()])
        .unwrap();
    let custom_host = tools::ToolHost::new(
        "custom-discovery",
        root.path(),
        tools::ToolHostSnapshot::new(
            Arc::new(catalog),
            Arc::new(tools::lsp_client::LspRegistry::new()),
            None,
        ),
    );
    let custom_lease = custom_host.pin_snapshot();
    let custom_definition = crate::ProviderToolDefinition {
        name: custom.name.clone(),
        description: None,
        input_schema: custom.input_schema.clone(),
    };
    assert!(crate::tool_host::discover_delegated_tool(
        &custom_lease,
        &custom_lease.catalog_receipt(),
        &custom_definition,
        &BTreeSet::from([custom.name.clone()]),
        PermissionMode::ReadOnly,
        Some(&["read:src/a.rs".into()]),
    )
    .is_none());
    let mut shadow = custom;
    shadow.name = "read_file".into();
    shadow.effect_resolver.resolver_id = "runtime.readonly".into();
    assert!(tools::ToolCatalog::builtin()
        .with_runtime_tools(vec![shadow])
        .is_err());
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn r1_composite_discovery_native_narrow_read_keeps_exact_execution_ceiling() {
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_test_services(root.path(), false);
    let (pinned, definitions, allowed) =
        model_loop::activate_native_tools(host.clone(), &packet, true).unwrap();
    assert_eq!(allowed, packet.allowed_tools.iter().cloned().collect());
    let executor = discovery_native_executor(&services, pinned, &packet, allowed);
    let catalog = executor.pin_step_catalog().unwrap();
    let metadata = catalog.catalog_receipt();
    let step = Arc::new(
        crate::step_snapshot::BoundStep::bind(
            serde_json::from_value(serde_json::json!({
                "step_id":"native-discovery", "session_id":packet.session_id(),
                "turn_id":packet.assignment.execution_identity.turn_id().unwrap(),
                "execution_id":packet.run_id(), "run_ref":packet.run_id(),
                "actor_ref":packet.assignment.execution_identity.principal_id(), "attempt":null,
                "build_ref":"discovery-fixture"
            }))
            .unwrap(),
            catalog.clone(),
            &definitions,
            &serde_json::json!({}),
            &serde_json::json!({}),
            BTreeMap::new(),
            chrono::Utc::now().timestamp_millis() + 60_000,
        )
        .unwrap(),
    );
    for (call_id, tool, value) in [
        (
            "native-read",
            "read_file",
            serde_json::json!({"path":"src/a.rs"}),
        ),
        (
            "native-composite",
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"read","op":"call","tool":"read_file","input":{"path":"src/a.rs"}}
            ]}),
        ),
    ] {
        let input = normalize_delegated_resource_paths(
            tool,
            &value.to_string(),
            services.workspace_root(),
            services.path_identity_resolver(),
            Some(&packet.resource_scopes),
        )
        .unwrap();
        let authorization = discovery_native_authorization(&executor, tool, &input, call_id);
        let binding = crate::step_snapshot::StepCallBinding::provider(step.clone(), call_id);
        let output = executor
            .execute_step_authorized_invocation_output(
                Some(&binding),
                call_id,
                &authorization,
                tool,
                &input,
            )
            .await
            .unwrap();
        assert!(output.model_text().contains("narrow-read-proof"));
    }
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);
    assert_eq!(executor.receipts.lock().unwrap().len(), 2);
    let receipt = executor.tool_discovery_receipt();
    assert_eq!(receipt.catalog_revision, metadata.catalog_revision);
    assert_eq!(
        receipt
            .activation_candidates
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        executor.allowed_tools
    );
    for descriptor in &receipt.descriptors {
        assert!(metadata.descriptors.contains(descriptor));
        assert_ne!(descriptor.canonical_id, "checkpoint");
    }
    assert!(executor.has_tool("checkpoint"));
    let receipts_before = executor.receipts.lock().unwrap().len();
    for (tool, value) in discovery_denied_calls() {
        let input = value.to_string();
        let binding =
            crate::step_snapshot::StepCallBinding::provider(step.clone(), "native-denied");
        let valid_input = if tool == "tool_orchestrate" {
            serde_json::json!({"steps":[{"id":"read","op":"call","tool":"read_file","input":{"path":"src/a.rs"}}]}).to_string()
        } else {
            r#"{"path":"src/a.rs"}"#.to_string()
        };
        let valid = discovery_native_authorization(
            &executor,
            tool,
            &normalize_delegated_resource_paths(
                tool,
                &valid_input,
                services.workspace_root(),
                services.path_identity_resolver(),
                Some(&packet.resource_scopes),
            )
            .unwrap(),
            "native-denied",
        );
        assert!(
            executor
                .execute_step_authorized_invocation_output(
                    Some(&binding),
                    "native-denied",
                    &valid,
                    tool,
                    &input,
                )
                .await
                .is_err(),
            "{tool} {input}"
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("src/sibling.rs")).unwrap(),
            "untouched-sibling"
        );
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("src/a.rs")).unwrap(),
            "narrow-read-proof"
        );
    }
    assert_eq!(executor.receipts.lock().unwrap().len(), receipts_before);
    let edit = serde_json::json!({"steps":[
        {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"src/a.rs","content":"DENIED"}}
    ]});
    assert!(executor
        .enforce_resource_ceiling("tool_orchestrate", &edit.to_string())
        .is_err());
    host.tools
        .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
    let reloaded = executor.tool_discovery_receipt();
    assert_ne!(reloaded.catalog_revision, receipt.catalog_revision);
    assert_eq!(reloaded.descriptors, receipt.descriptors);
    assert_eq!(
        reloaded.activation_candidates,
        receipt.activation_candidates
    );
    assert_ne!(host.tools.pin_snapshot().revision(), catalog.revision());
    assert_eq!(
        executor
            .prepare_step_catalog(PermissionMode::ReadOnly)
            .unwrap()
            .unwrap()
            .revision(),
        reloaded.catalog_revision
    );
    for scopes in [
        vec![],
        vec!["read:src/../a.rs".into()],
        vec!["network:*".into()],
    ] {
        let mut denied = packet.clone();
        denied.resource_scopes = scopes;
        assert!(model_loop::activate_native_tools(host.clone(), &denied, true).is_err());
    }
    let no_pin: Arc<dyn RuntimeExecutionHost> = Arc::new(NoopRuntimeExecutionHost);
    assert!(model_loop::activate_native_tools(no_pin, &packet, true).is_err());
    assert_eq!(host.calls.load(Ordering::SeqCst), 2);
    assert!(services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
}

pub(super) fn discovery_denied_calls() -> Vec<(&'static str, serde_json::Value)> {
    vec![
        ("read_file", serde_json::json!({})),
        ("read_file", serde_json::json!({"path":"src/sibling.rs"})),
        ("read_file", serde_json::json!({"path":"../secret"})),
        ("read_file", serde_json::json!({"path":"/etc/passwd"})),
        ("tool_orchestrate", serde_json::json!({})),
        (
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},
                {"id":"edit","op":"call","tool":"edit","input":{"action":"write","path":"src/a.rs","content":"DENIED"}}
            ]}),
        ),
        (
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},
                {"id":"network","op":"call","tool":"web_fetch","input":{"url":"https://example.invalid","prompt":"denied"}}
            ]}),
        ),
        (
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},
                {"id":"unknown","op":"call","tool":"not_registered","input":{}}
            ]}),
        ),
        (
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"prefix","op":"call","tool":"read_file","input":{"path":"src/a.rs"}},
                {"id":"compute","op":"compute","language":"python","code":"print(1)"}
            ]}),
        ),
        (
            "tool_orchestrate",
            serde_json::json!({"steps":[
                {"id":"read","op":"call","tool":"read_file","input":{"path":"$missing.path"}}
            ]}),
        ),
    ]
}


#[tokio::test]
async fn native_coordination_model_loop_consumes_wake_once() {
    native_coordination_wire(false, false).await;
}

#[tokio::test]
async fn native_durable_supplement_crosses_real_model_checkpoint_and_graph_commit() {
    native_coordination_wire(true, false).await;
}

#[tokio::test]
async fn native_durable_input_survives_failed_provider_step_before_consumption_commit() {
    native_coordination_wire(true, true).await;
}

async fn native_coordination_wire(supplement: bool, fail_input_request: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let provider = Arc::new(
        crate::ProviderRegistry::new_with_context_windows(
            model_protocol::provider_config::ProvidersConfig {
                providers: std::collections::HashMap::from([(
                    "test".into(),
                    model_protocol::provider_config::ProviderConfig {
                        reasoning_effort: None,
                        name: "test".into(),
                        base_url: format!("http://{address}/v1"),
                        api_key: "local-test-only".into(),
                        models: vec!["test".into()],
                        protocol: Some("responses".into()),
                        parallel_tool_calls: Default::default(),
                        early_tool_start: Default::default(),
                    },
                )]),
            },
            std::collections::BTreeMap::from([("test".into(), 128_000)]),
        )
        .unwrap(),
    );
    let fixture = crate::agentic::coordination::tests::fixture_with_provider(provider).await;
    let requests = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let captured = requests.clone();
    let input_services = fixture.services.clone();
    let input_agent_id = fixture.packet.agent_id().to_string();
    let wake = fixture.wake_ref.clone();
    let topic = format!(
        "topic:{}",
        fixture.packet.agentic_binding.as_ref().unwrap().team_id
    );
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let header_end = loop {
                let mut chunk = [0; 4096];
                let read = socket.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                bytes.extend_from_slice(&chunk[..read]);
                if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let headers = String::from_utf8_lossy(&bytes[..header_end]);
            let length: usize = headers
                .lines()
                .find_map(|line| {
                    let (key, value) = line.split_once(':')?;
                    key.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse().unwrap())
                })
                .unwrap();
            while bytes.len() < header_end + length {
                let mut chunk = [0; 4096];
                let read = socket.read(&mut chunk).await.unwrap();
                assert!(read > 0);
                bytes.extend_from_slice(&chunk[..read]);
            }
            let request =
                String::from_utf8(bytes[header_end..header_end + length].to_vec()).unwrap();
            let first = {
                let mut all = captured.lock().unwrap();
                all.push(request.clone());
                all.len() == 1
            };
            assert!(
                request.contains(&wake),
                "actual model wire must consume its source wake"
            );
            assert!(request.contains("source A"));
            if supplement && captured.lock().unwrap().len() == 2 {
                assert!(
                    request.contains("DURABLE_NATIVE_SUPPLEMENT"),
                    "actual next request must consume the accepted supplement"
                );
            }
            let ordinal = captured.lock().unwrap().len();
            if fail_input_request && ordinal == 2 {
                assert_eq!(
                    input_services
                        .agent_runtime()
                        .pending_agent_inputs(
                            &input_agent_id,
                            &input_services
                                .agent_runtime()
                                .get(&input_agent_id)
                                .unwrap()
                                .run_id
                        )
                        .unwrap()
                        .len(),
                    1
                );
                let body = r#"{"error":{"message":"intentional pre-consumption model failure","type":"invalid_request_error"}}"#;
                socket.write_all(format!("HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                continue;
            }
            if fail_input_request && ordinal == 3 {
                assert!(
                    request.contains("DURABLE_NATIVE_SUPPLEMENT"),
                    "failed Provider step must not lose input: {request}"
                );
            }
            let logical_round = ordinal - usize::from(fail_input_request && ordinal > 2);
            // Original Turn input handling has two fences: abandon the
            // pre-input response, then commit its input-disposition replan.
            // Only the following model step may publish a fresh tool plan.
            let events = if first || (supplement && logical_round <= 3) {
                if supplement && first {
                    let snapshot = input_services.agent_runtime().get(&input_agent_id).unwrap();
                    let receipt = input_services
                        .agent_runtime()
                        .command(AgentCommandRequest {
                            command_id: "native-durable-supplement".into(),
                            agent_id: input_agent_id.clone(),
                            expected_revision: snapshot.revision,
                            command: AgentCommand::SendInput,
                            input: Some(AgentInput::UserSupplement(
                                "DURABLE_NATIVE_SUPPLEMENT: retain the empty-input boundary".into(),
                            )),
                        })
                        .await;
                    assert!(receipt.accepted, "{receipt:?}");
                }
                let arguments = serde_json::json!({"topic_ref":topic,"summary":"Source A needs an explicit empty-input boundary check", "refs":[wake]}).to_string();
                let call_id = format!("coord-answer-{}", captured.lock().unwrap().len());
                vec![
                    serde_json::json!({"type":"response.created","response":{"id":"coord-tool","model":"test"}}),
                    serde_json::json!({"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","call_id":call_id,"name":"message_publish"}}),
                    serde_json::json!({"type":"response.function_call_arguments.delta","output_index":0,"delta":arguments}),
                    serde_json::json!({"type":"response.completed","response":{"id":"coord-tool","model":"test","output":[{"type":"function_call","call_id":call_id,"name":"message_publish","arguments":arguments}],"usage":{"input_tokens":16,"output_tokens":12}}}),
                ]
            } else {
                assert_eq!(
                    captured.lock().unwrap().len(),
                    2 + 2 * usize::from(supplement) + usize::from(fail_input_request),
                    "no unprompted model loop after the durable reply"
                );
                vec![
                    serde_json::json!({"type":"response.created","response":{"id":"coord-final","model":"test"}}),
                    serde_json::json!({"type":"response.output_text.delta","delta":"The requested boundary check has been published to the original Topic."}),
                    serde_json::json!({"type":"response.completed","response":{"id":"coord-final","model":"test","output":[],"usage":{"input_tokens":16,"output_tokens":12}}}),
                ]
            };
            let mut body = events
                .into_iter()
                .map(|event| format!("data: {event}\n\n"))
                .collect::<String>();
            body.push_str("data: [DONE]\n\n");
            socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        }
    });
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        fixture
            .services
            .agent_runtime()
            .execute_task(fixture.packet.clone()),
    )
    .await;
    let outcome = result.expect("local native model loop timeout").unwrap();
    server.abort();
    assert!(outcome.failure.is_none(), "{outcome:?}");
    assert!(
        fixture
            .services
            .agent_runtime()
            .pending_agent_inputs(fixture.packet.agent_id(), fixture.packet.run_id())
            .unwrap()
            .is_empty(),
        "real ModelStep graph commit must acknowledge consumption"
    );
    assert_eq!(
        requests.lock().unwrap().len(),
        2 + 2 * usize::from(supplement) + usize::from(fail_input_request)
    );
    let projection = fixture
        .services
        .agent_action_service()
        .project("coord-program")
        .unwrap();
    let consumed = projection.coordination_requests()[0]
        .1
        .coordination
        .as_ref()
        .unwrap();
    assert!(consumed.settled && consumed.reason.is_none());
    assert_eq!(
        projection.tasks[&fixture.task_ref]
            .claim_execution_id
            .as_deref(),
        Some("primary-execution")
    );
    assert_eq!(projection.tasks[&fixture.task_ref].failed_attempts, 0);
    assert!(fixture
        .services
        .dispatch_ready_agentic_work("coord-program")
        .await
        .unwrap()
        .is_empty());
    assert!(fixture
        .services
        .agent_runtime()
        .execute_task(fixture.packet.clone())
        .await
        .unwrap()
        .failure
        .is_none());
    assert_eq!(
        requests.lock().unwrap().len(),
        2 + 2 * usize::from(supplement) + usize::from(fail_input_request)
    );
}

fn test_authorization_lease(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    ceiling: PermissionMode,
    idempotency_key: &str,
) -> harness_contract::policy::AuthorizationLease {
    harness_contract::policy::AuthorizationLease {
        lease_id: format!("test-lease:{idempotency_key}"),
        principal_id: "test-agent".to_string(),
        parent_lease_id: None,
        capability: descriptor.tool_id.clone(),
        scopes: descriptor.scopes.clone(),
        ceiling,
        issued_at_ms: 0,
        expires_at_ms: u64::MAX,
        max_uses: 1,
        remaining_uses: 1,
        idempotency_key: idempotency_key.to_string(),
        policy_revision: 1,
        effect_descriptor_hash: descriptor.descriptor_hash.clone(),
        signature: "test-signature".to_string(),
        status: harness_contract::policy::AuthorizationLeaseStatus::Active,
    }
}

fn test_capability_assessment(
    descriptor: &harness_contract::tool::ToolEffectDescriptor,
    required_mode: PermissionMode,
) -> harness_contract::policy::CapabilityAssessment {
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(descriptor, "{}");
    harness_contract::policy::CapabilityAssessment {
        assessment_id: "test-assessment".to_string(),
        capability: descriptor.tool_id.clone(),
        effect: effective.descriptor.assessment,
        requested_scopes: effective.descriptor.scopes,
        required_mode,
        active_ceiling: PermissionMode::DangerFullAccess,
        parent_ceiling: PermissionMode::DangerFullAccess,
        risk: harness_contract::policy::RiskLevel::Low,
        path: harness_contract::policy::AuthorizationPath::PolicyAutoGrant,
        lease: None,
        gap: None,
        evidence_refs: Vec::new(),
        assessed_at_ms: 0,
    }
}

fn scoped_receipt(
    sequence: u64,
    effect_kind: harness_contract::tool::ToolEffectKind,
    path: &str,
    before: Option<&str>,
    after: Option<&str>,
) -> ScopedToolExecutionReceipt {
    ScopedToolExecutionReceipt {
        sequence,
        provider_invocation_id: None,
        tool_name: "test_tool".to_string(),
        effect_kind,
        resource_scopes: vec![format!(
            "{}:{path}",
            if effect_kind == harness_contract::tool::ToolEffectKind::Write {
                "write"
            } else {
                "read"
            }
        )],
        paths: vec![path.to_string()],
        prior_states: before
            .map(|sha256| {
                BTreeMap::from([(
                    path.to_string(),
                    harness_contract::context::WorkspacePriorState::Existing {
                        sha256: sha256.to_string(),
                    },
                )])
            })
            .unwrap_or_else(|| {
                BTreeMap::from([(
                    path.to_string(),
                    harness_contract::context::WorkspacePriorState::Absent,
                )])
            }),
        after_digests: BTreeMap::from([(path.to_string(), after.map(str::to_string))]),
        observed_bytes: BTreeMap::new(),
        observed_evidence: Vec::new(),
    }
}

fn test_agent_packet(
    evidence_refs: Vec<harness_contract::context::EvidenceAccessRef>,
) -> AgentTaskPacket {
    AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None,
            "agent",
            "run",
            "task",
            "session",
            "mission",
            Some("team"),
            "graph",
            "node",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "review".into(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        acceptance: Vec::new(),
        cohort_prompt_package: None,
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs,
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget",
            "agent",
            "agent",
            1,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "key".into(),
        agentic_binding: None,
    }
}

#[test]
fn task_specific_role_brief_is_after_the_shared_system_boundary() {
    let first = test_agent_packet(Vec::new());
    let mut second = first.clone();
    second.objective = "independently audit the alternate design".to_string();
    second.resource_scopes = vec!["read:crates/runtime".to_string()];

    let first_segments = system_prompt(&first, std::path::Path::new("/workspace"), &[]);
    let second_segments = system_prompt(&second, std::path::Path::new("/workspace"), &[]);
    let boundary = first_segments
        .iter()
        .position(|segment| segment == crate::SYSTEM_PROMPT_CACHE_COHORT_BOUNDARY)
        .expect("delegated prompt carries the extraction boundary");
    assert_eq!(
        first_segments[..boundary],
        second_segments[..boundary],
        "role-private objective and resource scope must not fragment the shared system prefix"
    );
    assert_ne!(
        first_segments[boundary + 1..],
        second_segments[boundary + 1..],
        "the extracted suffix remains a private Runtime-attested role brief"
    );
}

#[test]
fn agentic_prompt_names_only_the_actions_owned_by_its_bound_focus() {
    let mut packet = test_agent_packet(Vec::new());
    packet.agentic_binding = Some(AgenticExecutionBinding {
        program_id: "program:test".into(),
        agent_id: "agent:test".into(),
        membership_id: "membership:test".into(),
        team_id: "team:test".into(),
        task_team_id: "team:test".into(),
        source_spec_revision: 1,
        task_purpose: None,
        focus: AgenticExecutionFocus::TaskExecute {
            task_ref: "task:execute-only".into(),
        },
    });
    packet.allowed_tools = vec!["task_submit".into(), "task_review".into()];
    let prompt = system_prompt(
        &packet,
        std::path::Path::new("/workspace"),
        &packet.allowed_tools,
    )
    .join("\n");
    assert!(prompt.contains("task:execute-only"));
    assert!(prompt.contains("only the Task bound to this execution"));
    assert!(!prompt.contains("review peers independently"));
    assert!(prompt.contains("root coordinator owns Program-level closure"));
}

#[test]
fn review_and_coordination_prompts_preserve_their_bound_action_owner() {
    let cases = [
        (
            AgenticExecutionFocus::TaskReview {
                task_ref: "task:review-only".into(),
            },
            "TaskReview owns only the submitted Task",
        ),
        (
            AgenticExecutionFocus::Coordination {
                wake_ref: "wake:coordination-only".into(),
            },
            "Coordination owns only the authorized wake",
        ),
        (
            AgenticExecutionFocus::ObjectiveReview {
                criterion_refs: vec!["criterion:review-only".into()],
            },
            "ObjectiveReview owns only these bound criteria",
        ),
    ];
    for (focus, expected_owner) in cases {
        let mut packet = test_agent_packet(Vec::new());
        packet.agentic_binding = Some(AgenticExecutionBinding {
            program_id: "program:test".into(),
            agent_id: "agent:test".into(),
            membership_id: "membership:test".into(),
            team_id: "team:test".into(),
            task_team_id: "team:test".into(),
            source_spec_revision: 1,
            task_purpose: None,
            focus,
        });
        packet.allowed_tools = vec!["task_submit".into(), "task_review".into()];
        let prompt = system_prompt(
            &packet,
            std::path::Path::new("/workspace"),
            &packet.allowed_tools,
        )
        .join("\n");
        assert!(prompt.contains(expected_owner), "{prompt}");
        assert!(!prompt.contains("claim eligible bounded work"));
        assert!(!prompt.contains("review peers independently"));
        assert!(!prompt.contains("TaskExecute owns only"));
    }
}

#[test]
fn change_and_source_verification_require_digest_delta_and_post_write_read() {
    let unchanged = vec![scoped_receipt(
        1,
        harness_contract::tool::ToolEffectKind::Write,
        "src/lib.rs",
        Some("same"),
        Some("same"),
    )];
    assert!(materialized_change_receipts(&unchanged).is_empty());

    let read_before_write = vec![
        scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Read,
            "src/lib.rs",
            Some("before"),
            Some("before"),
        ),
        scoped_receipt(
            2,
            harness_contract::tool::ToolEffectKind::Write,
            "src/lib.rs",
            Some("before"),
            Some("after"),
        ),
    ];
    let change = materialized_change_receipts(&read_before_write)
        .pop()
        .expect("real digest change");
    assert!(has_matching_pre_write_evidence(&change, &read_before_write));
    assert!(!has_matching_read_receipt(
        &change,
        &read_before_write,
        true
    ));

    let write_then_read = vec![
        scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Write,
            "src/lib.rs",
            Some("before"),
            Some("after"),
        ),
        scoped_receipt(
            2,
            harness_contract::tool::ToolEffectKind::Read,
            "src/lib.rs",
            Some("after"),
            Some("after"),
        ),
    ];
    let ungrounded = materialized_change_receipts(&write_then_read)
        .pop()
        .expect("digest changed");
    assert_eq!(ungrounded.reread_sequence, Some(2));
    assert!(!has_matching_pre_write_evidence(
        &ungrounded,
        &write_then_read
    ));
    assert!(has_matching_read_receipt(
        &ungrounded,
        &write_then_read,
        true
    ));
    assert_eq!(
        tool_output_byte_length(
            r#"Tool completed. Evidence: tool://read. {"file":{"filePath":"src/lib.rs","byteLength":56262}}"#
        ),
        Some(56_262)
    );

    let mut verified = read_before_write;
    verified.push(scoped_receipt(
        3,
        harness_contract::tool::ToolEffectKind::Read,
        "src/lib.rs",
        Some("after"),
        Some("after"),
    ));
    assert!(has_matching_pre_write_evidence(&change, &verified));
    assert!(has_matching_read_receipt(&change, &verified, true));
}

#[test]
fn new_file_source_verification_uses_runtime_absence_proof_and_post_write_read() {
    let write_then_read = vec![
        scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Write,
            "evidence/report.html",
            None,
            Some("created"),
        ),
        scoped_receipt(
            2,
            harness_contract::tool::ToolEffectKind::Read,
            "evidence/report.html",
            Some("created"),
            Some("created"),
        ),
    ];
    let change = materialized_change_receipts(&write_then_read)
        .pop()
        .expect("new file is a materialized change");
    assert!(has_matching_pre_write_evidence(&change, &write_then_read));
    assert!(has_matching_read_receipt(&change, &write_then_read, true));

    let mut missing_absence_proof = write_then_read.clone();
    missing_absence_proof[0]
        .prior_states
        .remove("evidence/report.html");
    assert!(!has_matching_pre_write_evidence(
        &change,
        &missing_absence_proof
    ));

    assert!(!has_matching_read_receipt(
        &change,
        &write_then_read[..1],
        true
    ));
}

#[test]
fn upstream_review_matches_normalized_receipt_path_and_its_digest_key() {
    let change = harness_contract::agent::AgentChangeReceipt {
        path: "fixtures/auto-strategy-write/target.txt".to_string(),
        before_sha256: Some("before".to_string()),
        after_sha256: "after".to_string(),
        write_sequence: 3,
        bytes: None,
        reread_sequence: None,
        reread_evidence_ref: None,
    };
    let receipt = ScopedToolExecutionReceipt {
        sequence: 1,
        provider_invocation_id: None,
        tool_name: "read_file".to_string(),
        effect_kind: harness_contract::tool::ToolEffectKind::Read,
        resource_scopes: vec!["read:./fixtures/auto-strategy-write/target.txt".to_string()],
        paths: vec!["./fixtures/auto-strategy-write/target.txt".to_string()],
        prior_states: BTreeMap::from([(
            "./fixtures/auto-strategy-write/target.txt".to_string(),
            harness_contract::context::WorkspacePriorState::Existing {
                sha256: "after".to_string(),
            },
        )]),
        after_digests: BTreeMap::from([(
            "./fixtures/auto-strategy-write/target.txt".to_string(),
            Some("after".to_string()),
        )]),
        observed_bytes: BTreeMap::new(),
        observed_evidence: Vec::new(),
    };

    assert!(has_matching_read_receipt(&change, &[receipt], false));
}

#[test]
fn fresh_tool_receipt_is_evidence_even_when_content_ref_matches_upstream() {
    let upstream = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("tool", "same-content"),
        "sha256:same",
        1,
        "text/plain",
        "artifact://art_worker_upstream",
        "session:session",
    );
    let packet = test_agent_packet(vec![upstream.clone()]);
    assert!(!produced_runtime_evidence(
        &packet,
        &[upstream.clone()],
        &[]
    ));
    assert!(produced_runtime_evidence(
        &packet,
        &[upstream],
        &[scoped_receipt(
            1,
            harness_contract::tool::ToolEffectKind::Read,
            "fixtures/target.txt",
            Some("same"),
            Some("same"),
        )],
    ));
}

#[test]
fn network_receipts_satisfy_only_the_network_evidence_lease() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("resolver");
    let required = resolver.compile_obligation_or_unresolved("network:*");
    let observed = resolver
        .observe_tool_scope("web_search", "network:*", None, 1)
        .expect("network receipt");
    assert!(crate::path_identity::observed_evidence_satisfies(
        &required, &observed
    ));
}

#[test]
fn unqualified_team_scope_matches_typed_runtime_receipts() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("scope");
    std::fs::write(root.path().join("crates/runtime/src/lib.rs"), "checked").expect("file");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("resolver");
    let required = resolver.compile_obligation_or_unresolved("read:crates/runtime");
    let observed = resolver
        .observe_tool_scope(
            "read_file",
            "read:crates/runtime/src/lib.rs",
            Some("sha256:checked"),
            1,
        )
        .expect("receipt");
    assert!(crate::path_identity::observed_evidence_satisfies(
        &required, &observed
    ));
}

#[test]
fn upstream_change_receipt_is_recovered_from_durable_evidence_binding() {
    let change = harness_contract::agent::AgentChangeReceipt {
        path: "fixtures/target.txt".to_string(),
        before_sha256: Some("before".to_string()),
        after_sha256: "after".to_string(),
        write_sequence: 3,
        bytes: None,
        reread_sequence: None,
        reread_evidence_ref: None,
    };
    let encoded = serde_json::to_string(&change).expect("change receipt JSON");
    let evidence = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("runtime_change", encoded),
        "sha256:change",
        1,
        "application/json",
        "artifact://art_worker_change",
        "session:session",
    );
    let packet = test_agent_packet(vec![evidence]);

    assert_eq!(packet_upstream_change_receipts(&packet), vec![change]);
}

#[test]
fn upstream_change_receipt_uses_the_final_digest_after_repeated_writes() {
    let changes = [
        harness_contract::agent::AgentChangeReceipt {
            path: "fixtures/target.txt".to_string(),
            before_sha256: Some("before".to_string()),
            after_sha256: "intermediate".to_string(),
            write_sequence: 3,
            bytes: None,
            reread_sequence: None,
            reread_evidence_ref: None,
        },
        harness_contract::agent::AgentChangeReceipt {
            path: "fixtures/target.txt".to_string(),
            before_sha256: Some("intermediate".to_string()),
            after_sha256: "terminal".to_string(),
            write_sequence: 8,
            bytes: None,
            reread_sequence: None,
            reread_evidence_ref: None,
        },
    ];
    let evidence = changes
        .iter()
        .map(|change| {
            let encoded = serde_json::to_string(change).expect("change receipt JSON");
            harness_contract::context::EvidenceAccessRef::durable(
                harness_contract::context::EvidenceRef::observed("runtime_change", encoded),
                "sha256:change",
                1,
                "application/json",
                "artifact://art_worker_change",
                "session:session",
            )
        })
        .collect();
    let packet = test_agent_packet(evidence);

    assert_eq!(
        packet_upstream_change_receipts(&packet),
        vec![changes[1].clone()]
    );
}

#[test]
fn explicit_empty_risk_list_is_a_materialized_review_result() {
    use harness_contract::agent::StructuredOutputField;

    assert!(structured_field_materialized(
        StructuredOutputField::Risks,
        Some(&serde_json::json!([])),
    ));
    assert!(!structured_field_materialized(
        StructuredOutputField::Risks,
        None,
    ));
    assert!(!structured_field_materialized(
        StructuredOutputField::Review,
        Some(&serde_json::json!([])),
    ));
}

struct NoopRuntimeExecutionHost;

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for NoopRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        _request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        panic!("the capability advertisement test must not execute a tool")
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        test_tool_descriptor_for_input(tool_name, input)
    }
}

struct EchoRuntimeExecutionHost;

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for EchoRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        if request.tool_name == "artifact_materialize" {
            assert_eq!(
                request.category,
                crate::ToolSafetyCategory::WriteLocal,
                "materializing an Artifact must not masquerade as a read at the isolation boundary"
            );
        }
        if request.authorization.is_none() {
            return crate::RuntimeToolExecutionOutcome {
                tool_use_id: request.tool_use_id.clone(),
                tool_name: request.tool_name.clone(),
                status: crate::RuntimeToolExecutionStatus::BlockedPermission,
                category: request.category,
                output: None,
                error: Some("missing propagated authorization".to_string()),
                evidence_ref: format!("agent-tool:{}", request.tool_use_id),
                observed_evidence: Vec::new(),
                pending_agent_batch: None,
            };
        }
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some(format!("authorized:{}", request.tool_name)),
            error: None,
            evidence_ref: format!("agent-tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
            pending_agent_batch: None,
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        test_tool_descriptor_for_input(tool_name, input)
    }
}

struct ConcurrencyTrackingRuntimeExecutionHost {
    active: std::sync::atomic::AtomicUsize,
    max_active: std::sync::atomic::AtomicUsize,
}

impl ConcurrencyTrackingRuntimeExecutionHost {
    fn new() -> Self {
        Self {
            active: std::sync::atomic::AtomicUsize::new(0),
            max_active: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn reset(&self) {
        self.active.store(0, Ordering::SeqCst);
        self.max_active.store(0, Ordering::SeqCst);
    }
}

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for ConcurrencyTrackingRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(std::time::Duration::from_millis(40)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: request.category,
            output: Some("{}".to_string()),
            error: None,
            evidence_ref: format!("agent-tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
            pending_agent_batch: None,
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        test_tool_descriptor_for_input(tool_name, input)
    }
}

fn concurrency_test_executor(
    root: &std::path::Path,
    host: Arc<ConcurrencyTrackingRuntimeExecutionHost>,
    scope_locks: Arc<ScopeLockManager>,
) -> ScopedRuntimeToolExecutor {
    ScopedRuntimeToolExecutor {
        tool_batch: None,
        host,
        allowed_tools: BTreeSet::from(["edit".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root)
                .expect("path identities"),
        ),
        scope_locks,
        commit_service: None,
        resource_scopes: None,
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    }
}

#[test]
fn delegated_control_effect_parent_is_the_scoped_packet_not_the_root() {
    let workspace = tempfile::tempdir().unwrap();
    let mut executor = concurrency_test_executor(
        workspace.path(),
        Arc::new(ConcurrencyTrackingRuntimeExecutionHost::new()),
        Arc::new(ScopeLockManager::new()),
    );
    executor.execution_id = "physical-agent-graph".into();
    executor.node_id = "physical-agent-node".into();
    executor.attempt = 9;
    executor.allowed_tools.extend([
        "task_claim".into(),
        "task_submit".into(),
        "task_review".into(),
    ]);
    let (parent, attempt) = executor.durable_tool_effect_parent().unwrap();
    assert_eq!(parent.execution_id, "physical-agent-graph");
    assert_eq!(parent.node_id, "physical-agent-node");
    assert_eq!(attempt, 9);
    for tool in ["task_claim", "task_submit", "task_review"] {
        assert!(!executor.owns_durable_tool_effect(tool), "{tool}");
    }
}

#[tokio::test]
async fn delegated_leaf_effects_serialize_conflicts_and_parallelize_unrelated_paths() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("fixtures/sub")).expect("fixture directories");
    let host = Arc::new(ConcurrencyTrackingRuntimeExecutionHost::new());
    let locks = Arc::new(ScopeLockManager::new());
    let first = Arc::new(concurrency_test_executor(
        root.path(),
        Arc::clone(&host),
        Arc::clone(&locks),
    ));
    let second = Arc::new(concurrency_test_executor(
        root.path(),
        Arc::clone(&host),
        Arc::clone(&locks),
    ));

    let same = tokio::join!(
        first.execute_scoped(
            "edit",
            r#"{"path":"fixtures/sub/target.txt","content":"one"}"#,
            None,
            None,
        ),
        second.execute_scoped(
            "edit",
            r#"{"path":"fixtures/sub/target.txt","content":"two"}"#,
            None,
            None,
        )
    );
    same.0.expect("first same-path effect");
    same.1.expect("second same-path effect");
    assert_eq!(host.max_active.load(Ordering::SeqCst), 1);

    host.reset();
    let parent_child = tokio::join!(
        first.execute_scoped(
            "edit",
            r#"{"path":"fixtures/sub","content":"parent"}"#,
            None,
            None,
        ),
        second.execute_scoped(
            "edit",
            r#"{"path":"fixtures/sub/target.txt","content":"child"}"#,
            None,
            None,
        )
    );
    parent_child.0.expect("parent-path effect");
    parent_child.1.expect("child-path effect");
    assert_eq!(host.max_active.load(Ordering::SeqCst), 1);

    host.reset();
    let unrelated = tokio::join!(
        first.execute_scoped(
            "edit",
            r#"{"path":"fixtures/left.txt","content":"left"}"#,
            None,
            None,
        ),
        second.execute_scoped(
            "edit",
            r#"{"path":"fixtures/right.txt","content":"right"}"#,
            None,
            None,
        )
    );
    unrelated.0.expect("left effect");
    unrelated.1.expect("right effect");
    assert_eq!(host.max_active.load(Ordering::SeqCst), 2);
}

struct InputSensitiveRuntimeExecutionHost;

impl InputSensitiveRuntimeExecutionHost {
    fn descriptor(
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        let mut descriptor = test_tool_descriptor_for_input(tool_name, input)?;
        let encoded = serde_json::to_vec(input).ok()?;
        descriptor.descriptor_hash = format!("input:{:x}", Sha256::digest(encoded));
        Some(descriptor)
    }
}

#[async_trait::async_trait]
impl crate::RuntimeExecutionHost for InputSensitiveRuntimeExecutionHost {
    async fn execute_runtime_tool(
        &self,
        request: &crate::RuntimeToolExecutionRequest,
    ) -> crate::RuntimeToolExecutionOutcome {
        let parsed = serde_json::from_str::<serde_json::Value>(&request.input).ok();
        let current_hash = parsed.as_ref().and_then(|input| {
            Self::descriptor(&request.tool_name, input).map(|descriptor| descriptor.descriptor_hash)
        });
        let authorized_hash = request
            .authorization
            .as_ref()
            .map(|authorization| authorization.descriptor_hash.as_str());
        let authorized = current_hash.as_deref() == authorized_hash;
        crate::RuntimeToolExecutionOutcome {
            tool_use_id: request.tool_use_id.clone(),
            tool_name: request.tool_name.clone(),
            status: if authorized {
                crate::RuntimeToolExecutionStatus::Executed
            } else {
                crate::RuntimeToolExecutionStatus::BlockedPermission
            },
            category: request.category,
            output: authorized.then(|| format!("authorized:{}", request.tool_name)),
            error: (!authorized).then(|| "tool authorization is stale".to_string()),
            evidence_ref: format!("agent-tool:{}", request.tool_use_id),
            observed_evidence: Vec::new(),
            pending_agent_batch: None,
        }
    }

    fn delegated_tool_effect_descriptor(
        &self,
        tool_name: &str,
        input: &serde_json::Value,
    ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
        Self::descriptor(tool_name, input)
    }
}

fn test_tool_descriptor_for_input(
    tool_name: &str,
    input: &serde_json::Value,
) -> Option<harness_contract::tool::ToolEffectDescriptor> {
    use harness_contract::policy::{
        DataClassification, EffectAssessment, EffectBlastRadius, EffectExternality, EffectNovelty,
        EffectReversibility, PermissionOperation, PermissionResource, PermissionScope,
    };
    use harness_contract::tool::{
        ToolApprovalClass, ToolEffectDescriptor, ToolEffectKind, ToolIdempotency,
        ToolPermissionMode,
    };

    if tool_name == "execute_code" {
        // Mirrors the registered contract: the kernel-hardened, network-off,
        // read-only sandbox declares no unbounded scope. Its file scopes are
        // exactly the declared `input_files`; an explicit empty set declares the
        // workspace root the snapshot is materialized from.
        let mut scopes = Vec::new();
        match input.get("input_files").and_then(serde_json::Value::as_array) {
            Some(paths) if !paths.is_empty() => {
                for path in paths {
                    let Some(path) = path.as_str() else { continue };
                    scopes.push(PermissionScope {
                        resource: PermissionResource::File,
                        operation: PermissionOperation::Read,
                        target: Some(path.to_string()),
                    });
                }
            }
            _ => scopes.push(PermissionScope {
                resource: PermissionResource::File,
                operation: PermissionOperation::Read,
                target: Some(".".to_string()),
            }),
        }
        return Some(ToolEffectDescriptor {
            tool_id: tool_name.to_string(),
            descriptor_hash: "test-host:execute_code".to_string(),
            effect_kind: ToolEffectKind::Process,
            idempotency: ToolIdempotency::Unknown,
            scopes,
            required_permission: ToolPermissionMode::ReadOnly,
            approval_class: ToolApprovalClass::Policy,
            uses_network: false,
            spawns_process: true,
            mutates_packages: false,
            mutates_system: false,
            assessment: EffectAssessment {
                reversibility: EffectReversibility::Reversible,
                externality: EffectExternality::Workspace,
                data_sensitivity: DataClassification::Internal,
                novelty: EffectNovelty::NewTarget,
                blast_radius: EffectBlastRadius::Workspace,
            },
        });
    }
    let (effect_kind, operation, required_permission, resource) = match tool_name {
        "read_file" | "grep_search" | "glob_search" => (
            ToolEffectKind::Read,
            PermissionOperation::Read,
            ToolPermissionMode::ReadOnly,
            PermissionResource::File,
        ),
        "team_create" => (
            ToolEffectKind::Read,
            PermissionOperation::Read,
            ToolPermissionMode::ReadOnly,
            PermissionResource::Tool,
        ),
        "checkpoint" | "edit" | "artifact_materialize" => (
            ToolEffectKind::Write,
            PermissionOperation::Write,
            ToolPermissionMode::WorkspaceWrite,
            PermissionResource::File,
        ),
        _ => return None,
    };
    Some(ToolEffectDescriptor {
        tool_id: tool_name.to_string(),
        descriptor_hash: format!("test-host:{tool_name}"),
        effect_kind,
        idempotency: ToolIdempotency::Idempotent,
        scopes: vec![PermissionScope {
            resource,
            operation,
            target: input
                .get("path")
                .or_else(|| input.get("file_path"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string),
        }],
        required_permission,
        approval_class: ToolApprovalClass::None,
        uses_network: false,
        spawns_process: false,
        mutates_packages: false,
        mutates_system: false,
        assessment: harness_contract::policy::EffectAssessment::default(),
    })
}

#[test]
fn read_only_ceiling_never_escalates_for_a_write_tool() {
    let tools = BTreeSet::from(["edit".to_string()]);
    let policy = permission_policy(None, PermissionMode::ReadOnly, &tools);
    assert_eq!(policy.active_mode(), PermissionMode::ReadOnly);
    assert_eq!(
        policy.required_mode_for("edit"),
        PermissionMode::WorkspaceWrite
    );
}

#[tokio::test]
async fn runtime_artifact_materialization_uses_registered_write_classification() {
    let root = tempfile::tempdir().unwrap();
    let mut executor = concurrency_test_executor(
        root.path(),
        Arc::new(ConcurrencyTrackingRuntimeExecutionHost::new()),
        Arc::new(ScopeLockManager::new()),
    );
    executor.host = Arc::new(EchoRuntimeExecutionHost);
    executor.allowed_tools.insert("artifact_materialize".into());
    let input = r#"{"path":"output.html","operation":"read"}"#;
    let descriptor = test_tool_descriptor_for_input(
        "artifact_materialize",
        &serde_json::from_str(input).unwrap(),
    )
    .unwrap();
    let effective =
        crate::AuthorizationNegotiator::compile_effective_descriptor(&descriptor, input);
    let authorization = crate::ToolPolicy
        .authorize(
            &effective,
            &test_capability_assessment(&descriptor, PermissionMode::WorkspaceWrite),
            "materialize",
            test_authorization_lease(&descriptor, PermissionMode::WorkspaceWrite, "materialize"),
            30,
        )
        .unwrap()
        .authorization;
    assert_eq!(
        executor
            .execute_delegated_runtime_tool(
                "artifact_materialize",
                input,
                authorization.clone(),
                None
            )
            .await
            .unwrap(),
        "authorized:artifact_materialize"
    );
    assert!(executor
        .execute_delegated_runtime_tool("unregistered", input, authorization.clone(), None)
        .await
        .unwrap_err()
        .to_string()
        .contains("no registered effect descriptor"));
    let services = RuntimeServices::in_memory().unwrap();
    executor.tool_batch = Some(
        crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
            &services,
            &test_agent_packet(vec![]),
        ),
    );
    assert!(executor.owns_durable_tool_effect("artifact_materialize"));
    assert!(executor.owns_tool_resource_admission("artifact_materialize"));
    assert!(!executor.owns_tool_resource_admission("checkpoint"));
    // With a production dispatcher, an absent parent fails admission. The
    // compatibility Echo host must no longer return a successful direct call.
    assert!(executor
        .execute_delegated_runtime_tool("artifact_materialize", input, authorization, None)
        .await
        .is_err());
}

#[tokio::test]
async fn process_step_evidence_read_commits_once_and_rejects_changed_replay() {
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_configured_services(root.path(), true,
        &["evidence_retrieve"], PermissionMode::ReadOnly, vec![], false);
    let selection = AgentModelSelection { model:packet.model_lease.clone(),
        provider:"local-discovery".into(), registry_revision:0 };
    let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
    let step = bridge.begin_step().unwrap();
    let step_id = step["step"]["identity"]["step_id"].as_str().unwrap();
    let input = r#"{"evidence_ref":"artifact://synthetic"}"#;
    let first = bridge.execute_step_tool(step_id, 1, "process-evidence", "evidence_retrieve", input).await.unwrap();
    assert!(first.contains("synthetic-process-evidence"));
    assert_eq!(bridge.execute_step_tool(step_id, 1, "process-evidence", "evidence_retrieve", input).await.unwrap(), first);
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    assert!(bridge.execute_step_tool(step_id, 1, "process-evidence", "evidence_retrieve",
        r#"{"evidence_ref":"artifact://foreign"}"#).await.is_err());
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    let receipts = services.event_store().all_events(4096).unwrap().into_iter().filter(|event|
        event.kind == "execution.read.receipt" && event.payload["tool_use_id"] == "process-evidence"
    ).collect::<Vec<_>>();
    assert_eq!(receipts.len(), 1, "Process direct owner must persist exactly one original read");
    assert_eq!(receipts[0].payload["outcome"]["status"], "executed");
    assert!(!receipts[0].payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null());
}

#[test]
fn workspace_change_contract_retains_the_required_write_scope() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("fixtures")).expect("fixture directory");
    std::fs::write(root.path().join("fixtures/target.txt"), "target").expect("fixture file");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let mut packet = test_agent_packet(Vec::new());
    packet.required_acceptance = resolver.compile_required_acceptance(
        &[
            "implementation".to_string(),
            "source_verification".to_string(),
        ],
        &[
            "write:fixtures/target.txt".to_string(),
            "verify_after_write:fixtures/target.txt".to_string(),
        ],
    );

    assert_eq!(
        packet_focus_acceptance_scopes(&packet),
        [
            "verify_after_write:fixtures/target.txt",
            "write:fixtures/target.txt"
        ]
    );
    packet.required_acceptance = resolver.compile_required_acceptance(
        &["review".to_string()],
        &["verify_upstream_change:fixtures/target.txt".to_string()],
    );
    assert_eq!(
        packet_focus_acceptance_scopes(&packet),
        ["verify_upstream_change:fixtures/target.txt"]
    );
}

#[test]
fn exact_model_delivery_policy_is_scoped_to_the_matching_invocation() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("fixtures")).expect("fixtures");
    std::fs::write(root.path().join("fixtures/target.txt"), "target").expect("target");
    std::fs::write(root.path().join("fixtures/other.txt"), "other").expect("other");
    let resolver = Arc::new(
        crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
            .expect("path identities"),
    );
    let mut required =
        resolver.compile_required_acceptance(&[], &["read:fixtures/target.txt".to_string()]);
    crate::path_identity::require_provider_model_observation(&mut required);
    let obligation_id = required.evidence_obligations[0].obligation_id.clone();
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(InputSensitiveRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: resolver,
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["read:fixtures".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: required.evidence_obligations,
    };

    assert_eq!(
        executor.model_delivery_requirement(
            "read_file",
            r#"{"path":"fixtures/target.txt","complete":true}"#,
        ),
        crate::ToolModelDeliveryRequirement::exact(vec![obligation_id])
    );
    assert_eq!(
        executor.model_delivery_requirement(
            "read_file",
            r#"{"path":"fixtures/other.txt","complete":true}"#,
        ),
        crate::ToolModelDeliveryRequirement::Bounded
    );
    assert_eq!(
        executor.model_delivery_requirement("grep_search", r#"{"path":"fixtures"}"#),
        crate::ToolModelDeliveryRequirement::Bounded
    );
}

#[test]
fn evidence_scope_contract_projects_a_typed_read_resource_scope() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime")).expect("fixture directory");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let mut packet = test_agent_packet(Vec::new());
    packet.required_acceptance = resolver.compile_required_acceptance(
        &["evidence_scope:crates/runtime".to_string()],
        &["read:crates/runtime".to_string()],
    );

    assert_eq!(
        packet_focus_acceptance_scopes(&packet),
        ["read:crates/runtime"]
    );
}

#[test]
fn network_evidence_scope_preserves_its_resource_kind() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let mut packet = test_agent_packet(Vec::new());
    packet.required_acceptance = resolver.compile_required_acceptance(
        &["evidence_scope:network:*".to_string()],
        &["network:*".to_string()],
    );

    assert_eq!(packet_focus_acceptance_scopes(&packet), ["network:*"]);
}

#[test]
fn acceptance_contract_projects_materialized_output_fields_to_the_host() {
    let mut packet = test_agent_packet(Vec::new());
    packet.output_acceptance = vec![
        harness_contract::agent::OutputAcceptanceRequirement {
            criterion: "evidence".to_string(),
            check: harness_contract::agent::OutputAcceptanceCheck::ScopedEvidence {
                scopes: vec!["read:fixtures/target.txt".to_string()],
            },
        },
        harness_contract::agent::OutputAcceptanceRequirement {
            criterion: "review".to_string(),
            check: harness_contract::agent::OutputAcceptanceCheck::UpstreamReview,
        },
        harness_contract::agent::OutputAcceptanceRequirement {
            criterion: "risks".to_string(),
            check: harness_contract::agent::OutputAcceptanceCheck::StructuredField {
                field: harness_contract::agent::StructuredOutputField::Risks,
            },
        },
    ];

    assert_eq!(
        packet_required_output_fields(&packet),
        ["review".to_string(), "risks".to_string()]
    );
}

#[test]
fn workspace_internal_absolute_resource_path_is_normalized_once() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let target = root.path().join("fixtures/target.txt");
    let input = serde_json::json!({
        "path": target,
        "content": format!("do not rewrite {}", root.path().display()),
    })
    .to_string();

    let normalized =
        normalize_delegated_resource_paths("edit", &input, root.path(), &resolver, None)
            .expect("normalize internal absolute path");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], "fixtures/target.txt");
    assert!(normalized["content"]
        .as_str()
        .is_some_and(|content| content.contains(&root.path().display().to_string())));
}

#[test]
fn sole_directory_scope_normalizes_a_bare_delegated_read_path() {
    let root = tempfile::tempdir().expect("workspace");
    std::fs::create_dir_all(root.path().join("external-app")).expect("project directory");
    std::fs::write(
        root.path().join("external-app/Cargo.toml"),
        "[package]\nname='external-app'\n",
    )
    .expect("fixture");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let input = serde_json::json!({"path": "Cargo.toml"}).to_string();
    assert_eq!(
        resolver
            .resolve_existing("external-app")
            .expect("scope directory")
            .object_kind,
        harness_contract::context::WorkspaceObjectKind::Directory
    );
    let candidate = resolver
        .resolve_existing("external-app/Cargo.toml")
        .expect("candidate");
    let scope = resolver.resolve_existing("external-app").expect("scope");
    assert!(
        resource_path_is_authorized(
            &resolver,
            "external-app/Cargo.toml",
            &["read:external-app".to_string()],
            false,
        ),
        "candidate={candidate:?}; scope={scope:?}"
    );
    let direct = normalize_single_scope_relative_read_value(
        "read_file",
        serde_json::json!({"path": "Cargo.toml"}),
        &resolver,
        Some(&["read:external-app".to_string()]),
    );
    assert_eq!(direct["path"], "external-app/Cargo.toml");

    let normalized = normalize_delegated_resource_paths(
        "read_file",
        &input,
        root.path(),
        &resolver,
        Some(&["read:external-app".to_string()]),
    )
    .expect("normalize sole scoped read");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], "external-app/Cargo.toml");
}

#[test]
fn ambiguous_or_existing_bare_read_path_is_never_retargeted() {
    let root = tempfile::tempdir().expect("workspace");
    for project in ["one", "two"] {
        std::fs::create_dir_all(root.path().join(project)).expect("project directory");
        std::fs::write(root.path().join(project).join("Cargo.toml"), project).expect("fixture");
    }
    std::fs::write(root.path().join("Cargo.toml"), "root").expect("root fixture");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let input = serde_json::json!({"path": "Cargo.toml"}).to_string();

    let normalized = normalize_delegated_resource_paths(
        "read_file",
        &input,
        root.path(),
        &resolver,
        Some(&["read:one".to_string(), "read:two".to_string()]),
    )
    .expect("preserve ambiguous path");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], "Cargo.toml");
}

#[test]
fn whole_workspace_lease_bounds_to_workspace_but_never_escapes() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    std::fs::create_dir_all(root.path().join("evidence")).expect("evidence directory");
    let root_input = serde_json::json!({
        "pattern": "**/*.rs",
        "path": root.path(),
    })
    .to_string();
    let normalized = normalize_delegated_resource_paths(
        "glob_search",
        &root_input,
        root.path(),
        &resolver,
        Some(&["write:.".to_string()]),
    )
    .expect("normalize workspace root");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], ".");
    // `write:.` is a whole-workspace lease issued only to full-trust
    // Teams; it authorizes any path inside the workspace.
    assert!(resource_path_is_authorized(
        &resolver,
        "evidence/new-report.html",
        &["write:.".to_string()],
        true,
    ));
    // Root execution graphs express the same full-trust lease as
    // `workspace:.`; delegated Agents must not lose that authority while the
    // permission/capability ceilings continue to control concrete effects.
    assert!(resource_path_is_authorized(
        &resolver,
        "evidence/new-report.html",
        &["workspace:.".to_string()],
        true,
    ));
    enforce_glob_scope(
        &serde_json::json!({"path": ".", "pattern": "**/*.rs"}),
        &["workspace:.".to_string()],
    )
    .expect("workspace root lease admits an in-workspace glob");
    // Traversal outside the workspace is never authorized, even under a
    // whole-workspace lease.
    assert!(!resource_path_is_authorized(
        &resolver,
        "../outside.html",
        &["write:.".to_string()],
        true,
    ));
}

#[test]
fn glob_batch_checks_each_actual_search_against_the_original_ceiling() {
    let runtime = vec!["read:crates/runtime".to_string()];
    let all = vec!["read:.".to_string()];
    let allowed = serde_json::json!({"patterns":[
        {"path":"crates/runtime/src", "pattern":"*.rs"},
        {"path":"crates/runtime", "pattern":"src/*.rs"}
    ]});
    let original = allowed.clone();
    enforce_glob_scope(&allowed, &runtime).expect("each actual child stays within the lease");
    assert_eq!(allowed, original, "scope validation does not rewrite input");
    let outside = serde_json::json!({"patterns":[
        {"path":"crates/runtime/src", "pattern":"*.rs"},
        {"path":"crates/gateway/src", "pattern":"*.rs"}
    ]});
    assert!(enforce_glob_scope(&outside, &runtime).is_err());
    let shadow = serde_json::json!({"path":"crates/runtime", "pattern":"*.rs", "patterns":[
        {"path":"crates/gateway", "pattern":"*.rs"}
    ]});
    assert!(enforce_glob_scope(&shadow, &runtime).is_err(), "batch takes dispatch precedence");
    for path in [serde_json::Value::Null, serde_json::json!(""), serde_json::json!(".")] {
        let item = serde_json::json!({"patterns":[{"path":path,"pattern":"*.rs"}]});
        assert!(enforce_glob_scope(&item, &all).is_ok());
        assert!(enforce_glob_scope(&item, &runtime).is_err(), "root is not a narrow lease");
    }
    let omitted = serde_json::json!({"path":"crates/runtime", "patterns":[{"pattern":"*.rs"}]});
    assert!(enforce_glob_scope(&omitted, &all).is_ok());
    assert!(enforce_glob_scope(&omitted, &runtime).is_err(), "batch items do not inherit top-level path");
    assert!(enforce_glob_scope(&serde_json::json!({"patterns":[]}), &runtime).is_ok(), "executor permits an empty batch");
    for invalid in [serde_json::json!({"patterns":null}), serde_json::json!({"patterns":[{}]}),
        serde_json::json!({"patterns":[{"path":123,"pattern":"*.rs"}]}),
        serde_json::json!({"patterns":[{"path":"../outside","pattern":"*.rs"}]}),
        serde_json::json!({"patterns":[{"path":"crates/runtime","pattern":"../*.rs"}]})] {
        assert!(enforce_glob_scope(&invalid, &all).is_err(), "invalid item must not be authorized");
    }
    assert!(enforce_glob_scope(&serde_json::json!({"path":"crates/runtime", "pattern":"*.rs"}), &runtime).is_ok());
}

#[test]
fn exact_new_artifact_scope_remains_narrow_and_writable() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    std::fs::create_dir_all(root.path().join("evidence")).expect("evidence directory");
    assert!(resource_path_is_authorized(
        &resolver,
        "evidence/report.html",
        &["write:evidence/report.html".to_string()],
        true,
    ));
    assert!(!resource_path_is_authorized(
        &resolver,
        "evidence/other.html",
        &["write:evidence/report.html".to_string()],
        true,
    ));
}

#[test]
fn virtual_team_delivery_directory_scope_authorizes_new_descendants() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    assert!(resource_path_is_authorized(
        &resolver,
        "team.dependencies/research/README.md",
        &["write:team.dependencies".to_string()],
        true,
    ));
    assert!(!resource_path_is_authorized(
        &resolver,
        "team.other/research/README.md",
        &["write:team.dependencies".to_string()],
        true,
    ));
    let normalized = normalize_delegated_resource_paths(
        "glob_search",
        &serde_json::json!({"path":"", "pattern":"team.dependencies/**"}).to_string(),
        root.path(),
        &resolver,
        Some(&["write:team.dependencies".to_string()]),
    )
    .expect("normalize virtual delivery glob");
    let normalized: serde_json::Value = serde_json::from_str(&normalized).expect("json");
    assert_eq!(normalized["path"], "team.dependencies");
    assert_eq!(normalized["pattern"], "**");
}

#[test]
fn evaluation_virtual_directory_allows_first_write_but_not_sibling_or_traversal() {
    let root = tempfile::tempdir().unwrap();
    let resolver =
        crate::path_identity::WorkspacePathIdentityResolver::discover(root.path()).unwrap();
    let scope = format!(".cowd/evaluation/{}", "a".repeat(64));
    let allowed = vec![format!("write:{scope}")];
    assert!(resource_path_is_authorized(
        &resolver,
        &format!("{scope}/proof.txt"),
        &allowed,
        true
    ));
    for path in [
        format!(".cowd/evaluation/{}/proof.txt", "b".repeat(64)),
        format!("{scope}/../foreign.txt"),
        "/tmp/foreign.txt".into(),
    ] {
        assert!(
            !resource_path_is_authorized(&resolver, &path, &allowed, true),
            "{path}"
        );
    }
    assert!(!is_virtual_directory_scope(".cowd/evaluation"));
    assert!(!is_virtual_directory_scope(".cowd/evaluation/invalid"));
}

#[test]
fn absolute_escape_and_parent_traversal_remain_unauthorized() {
    let root = tempfile::tempdir().expect("workspace");
    let resolver = crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
        .expect("path identities");
    let allowed = root.path().join("fixtures/target.txt");
    std::fs::create_dir_all(allowed.parent().expect("parent")).expect("scope directory");
    std::fs::write(&allowed, "before").expect("scope file");
    let outside = root.path().parent().expect("parent").join("outside.txt");
    let outside_input = serde_json::json!({"path": outside}).to_string();
    assert_eq!(
        normalize_delegated_resource_paths(
            "read_file",
            &outside_input,
            root.path(),
            &resolver,
            None,
        )
        .expect("unchanged outside input"),
        outside_input
    );
    assert!(!resource_path_is_authorized(
        &resolver,
        outside.to_string_lossy().as_ref(),
        &["read:fixtures/target.txt".into()],
        false,
    ));
    assert!(!resource_path_is_authorized(
        &resolver,
        "fixtures/../outside.txt",
        &["read:fixtures/target.txt".into()],
        false,
    ));
}

#[test]
fn permission_policy_uses_the_explicit_packet_ceiling() {
    let tools = BTreeSet::from(["edit".to_string()]);
    let policy = permission_policy(None, PermissionMode::WorkspaceWrite, &tools);
    assert_eq!(policy.active_mode(), PermissionMode::WorkspaceWrite);
    assert_eq!(
        policy.required_mode_for("edit"),
        PermissionMode::WorkspaceWrite
    );
}

#[test]
fn sandboxed_process_effect_is_bounded_by_its_declared_inputs() {
    let root = tempfile::tempdir().expect("scoped workspace");
    // The declared input and both lease roots must exist for the path-identity
    // resolver, which is what actually certifies a bounded scope.
    for relative in [
        "crates/runtime/src/lib.rs",
        "crates/gateway/src/lib.rs",
    ] {
        let path = root.path().join(relative);
        std::fs::create_dir_all(path.parent().expect("parent")).expect("create scope dir");
        std::fs::write(&path, "// fixture\n").expect("write fixture file");
    }
    let build = |scopes: Vec<String>| ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["execute_code".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: Some(crate::execution_core::graph::ExecutionCommitService::new(
            Arc::new(crate::RuntimeEventStore::for_test()),
        )),
        resource_scopes: Some(scopes),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    // An explicit empty input_files set declares the workspace root, because the
    // sandbox still materializes its read-only snapshot from there. The
    // whole-workspace lease is therefore still required, and a narrower one is
    // still refused: this is the case the boundary existed for.
    let no_inputs = r#"{"language":"python","code":"print(1)","input_files":[]}"#;
    let whole_workspace = build(vec!["read:.".to_string()]);
    assert!(whole_workspace.owns_durable_tool_effect("execute_code"));
    whole_workspace
        .enforce_resource_ceiling("execute_code", no_inputs)
        .expect("whole-workspace read lease admits the read-only sandbox");
    assert!(build(vec!["read:src".to_string()])
        .enforce_resource_ceiling("execute_code", no_inputs)
        .is_err());

    // A declared finite input set is bounded to exactly those paths, so a lease
    // that covers them certifies the run. This is what makes a bounded
    // workspace-wide reduction reachable from a read-only session.
    let declared = r#"{"language":"python","code":"print(1)","input_files":["crates/runtime/src/lib.rs"]}"#;
    build(vec!["read:crates/runtime".to_string()])
        .enforce_resource_ceiling("execute_code", declared)
        .expect("a lease covering the declared inputs certifies the bounded sandbox");
    // ...and a lease that does not cover them is still refused. The boundary is
    // narrowed, never widened to "any lease admits".
    assert!(build(vec!["read:crates/gateway".to_string()])
        .enforce_resource_ceiling("execute_code", declared)
        .is_err());
    // The whole-workspace lease still covers a declared input, as it must.
    build(vec!["read:.".to_string()])
        .enforce_resource_ceiling("execute_code", declared)
        .expect("a whole-workspace lease covers the declared inputs");
}

#[tokio::test]
async fn team_tool_boundary_enforces_the_exact_focus_scope() {
    let root = tempfile::tempdir().expect("scoped workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime/src")).expect("runtime scope");
    std::fs::write(root.path().join("crates/runtime/src/lib.rs"), "checked").expect("runtime file");
    std::fs::create_dir_all(root.path().join("crates/gateway")).expect("gateway scope");
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from([
            "read_file".to_string(),
            "grep_search".to_string(),
            "glob_search".to_string(),
            "context_retrieve".to_string(),
        ]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["read:crates/runtime".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    executor
        .enforce_resource_ceiling("read_file", r#"{"path":"crates/runtime/src/lib.rs"}"#)
        .expect("in-scope read");
    assert!(executor
        .enforce_resource_ceiling("read_file", r#"{"path":"crates/gateway/src/lib.rs"}"#,)
        .is_err());
    assert!(executor
        .enforce_resource_ceiling("read_file", r#"{"path":"../secret"}"#)
        .is_err());
    assert!(executor
        .enforce_resource_ceiling("grep_search", r#"{"pattern":"unsafe"}"#)
        .is_err());
    executor
        .enforce_resource_ceiling(
            "context_retrieve",
            r#"{"source":"session_history","scope":"current"}"#,
        )
        .expect("Runtime-bound context retrieval is not a filesystem escape");

    let normalized = normalize_delegated_resource_paths(
        "glob_search",
        r#"{"pattern":"crates/runtime/**/*.rs","path":"."}"#,
        root.path(),
        &executor.path_identity_resolver,
        executor.resource_scopes.as_deref(),
    )
    .expect("bounded glob normalization");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&normalized).expect("normalized input"),
        serde_json::json!({"pattern":"**/*.rs","path":"crates/runtime"})
    );
    executor
        .enforce_resource_ceiling("glob_search", &normalized)
        .expect("the narrowed glob remains inside the exact focus scope");

    let outside_glob = normalize_delegated_resource_paths(
        "glob_search",
        r#"{"pattern":"crates/gateway/**/*.rs","path":"."}"#,
        root.path(),
        &executor.path_identity_resolver,
        executor.resource_scopes.as_deref(),
    )
    .expect("outside glob stays representable");
    assert!(executor
        .enforce_resource_ceiling("glob_search", &outside_glob)
        .is_err());

    let descriptor = test_tool_descriptor_for_input(
        "read_file",
        &serde_json::json!({"path": "crates/runtime/src/lib.rs"}),
    )
    .expect("read descriptor");
    let authorization = harness_contract::tool::ToolExecutionAuthorization {
        request_id: "absolute-read".into(),
        tool_id: "read_file".into(),
        descriptor_hash: descriptor.descriptor_hash.clone(),
        policy_revision: 1,
        scope: descriptor.scopes[0].clone(),
        authorization_lease: harness_contract::policy::AuthorizationLease {
            lease_id: "permission:read_only".into(),
            principal_id: "test-agent".into(),
            parent_lease_id: None,
            capability: "read_file".into(),
            scopes: descriptor.scopes.clone(),
            ceiling: harness_contract::policy::PermissionMode::ReadOnly,
            issued_at_ms: 0,
            expires_at_ms: u64::MAX,
            max_uses: 1,
            remaining_uses: 1,
            idempotency_key: "absolute-read".into(),
            policy_revision: 1,
            effect_descriptor_hash: descriptor.descriptor_hash.clone(),
            signature: "test-signature".into(),
            status: harness_contract::policy::AuthorizationLeaseStatus::Active,
        },
        timeout_lease: "timeout:30".into(),
        idempotency_key: None,
    };
    let absolute_input = serde_json::json!({
        "path": root.path().join("crates/runtime/src/lib.rs"),
    })
    .to_string();
    executor
        .execute_authorized_output(&authorization, "read_file", &absolute_input)
        .await
        .expect("workspace-internal absolute read is normalized and executed");
    let receipts = executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(receipts.len(), 1);
    assert_eq!(receipts[0].paths, ["crates/runtime/src/lib.rs"]);
    assert!(
        receipts[0].observed_evidence.is_empty(),
        "a successful delegated read with unstructured adapter output must not be promoted into exact-content evidence"
    );
}

#[tokio::test]
async fn absolute_path_authorization_and_execution_share_the_normalized_descriptor() {
    let root = tempfile::tempdir().expect("scoped workspace");
    let target = root.path().join("fixtures/target.txt");
    std::fs::create_dir_all(target.parent().expect("target parent")).expect("scope directory");
    std::fs::write(&target, "checked").expect("scope file");
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(InputSensitiveRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["read:fixtures/target.txt".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    let absolute_value = serde_json::json!({"path": target});
    let descriptor = executor
        .registered_tool_effect("read_file", &absolute_value)
        .expect("normalized effect descriptor");
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
        &descriptor,
        &absolute_value.to_string(),
    );
    let authorization = crate::ToolPolicy
        .authorize(
            &effective,
            &test_capability_assessment(&descriptor, PermissionMode::ReadOnly),
            "absolute-agent-read",
            test_authorization_lease(&descriptor, PermissionMode::ReadOnly, "absolute-agent-read"),
            30,
        )
        .expect("normalized read authorization")
        .authorization;

    assert_eq!(
        executor
            .execute_authorized_output(&authorization, "read_file", &absolute_value.to_string(),)
            .await
            .expect("same normalized descriptor must remain current"),
        harness_contract::context::ToolOutputDraft::bounded_inline("authorized:read_file")
    );
    let receipts = executor
        .receipts
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(receipts[0].paths, ["fixtures/target.txt"]);
}

#[cfg(unix)]
#[test]
fn team_tool_boundary_rejects_symlink_escape_for_existing_and_new_targets() {
    use std::os::unix::fs::symlink;

    let root = tempfile::tempdir().expect("scoped workspace");
    let outside = tempfile::tempdir().expect("outside workspace");
    std::fs::create_dir_all(root.path().join("crates/runtime")).expect("runtime scope");
    std::fs::write(outside.path().join("secret.txt"), "secret").expect("outside fixture");
    symlink(outside.path(), root.path().join("crates/runtime/escape")).expect("workspace symlink");
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string(), "edit".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: root.path().to_path_buf(),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(root.path())
                .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec![
            "read:crates/runtime".to_string(),
            "write:crates/runtime".to_string(),
        ]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    assert!(executor
        .enforce_resource_ceiling(
            "read_file",
            r#"{"path":"crates/runtime/escape/secret.txt"}"#,
        )
        .is_err());
    assert!(executor
        .enforce_resource_ceiling(
            "edit",
            r#"{"path":"crates/runtime/escape/new.txt","content":"denied"}"#,
        )
        .is_err());
}

#[tokio::test]
async fn scoped_executor_advertises_only_packet_authorized_tools() {
    let root = tempfile::tempdir().unwrap();
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(DiscoveryTestHost::new(root.path())),
        allowed_tools: BTreeSet::from(["read_file".to_string(), "grep_search".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: None,
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    assert!(executor.has_registered_tools());
    assert_eq!(
        executor.available_tool_names(),
        vec![
            "tool_search".to_string(),
            "grep_search".to_string(),
            "read_file".to_string(),
        ]
    );
    assert!(executor.classify_tool_safety("read_file", "{}").is_some());
    assert!(executor.classify_tool_safety("edit", "{}").is_none());
    let discovery: harness_contract::tool::ToolDiscoveryReceipt = serde_json::from_str(
        &executor
            .execute_output("tool_search", r#"{"query":"read"}"#)
            .await
            .expect("bootstrap search should return the canonical receipt")
            .model_text(),
    )
    .expect("canonical discovery receipt");
    assert_eq!(discovery.query, "read");
    assert_eq!(
        discovery.activation_candidates,
        vec!["grep_search", "read_file"]
    );
    assert!(executor.has_tool("checkpoint"));
    assert!(!executor
        .available_tool_names()
        .contains(&"checkpoint".to_string()));
    assert!(executor
        .execute_output("checkpoint", r#"{"label":"model"}"#)
        .await
        .is_err());
}

#[tokio::test]
async fn scoped_executor_routes_hidden_checkpoint_for_runtime_guard_only() {
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(EchoRuntimeExecutionHost),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec![
            "read:README.md".to_string(),
            "write:fixtures/target.txt".to_string(),
        ]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    let descriptor = executor
        .registered_tool_effect("checkpoint", &serde_json::json!({"label": "guard"}))
        .expect("Runtime guard must see the hidden checkpoint descriptor");
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
        &descriptor,
        r#"{"action":"create","label":"guard"}"#,
    );
    let authorization = crate::ToolPolicy
        .authorize(
            &effective,
            &test_capability_assessment(&descriptor, PermissionMode::WorkspaceWrite),
            "agent-checkpoint-test",
            test_authorization_lease(
                &descriptor,
                PermissionMode::WorkspaceWrite,
                "agent-checkpoint-test",
            ),
            30,
        )
        .expect("Runtime should authorize its internal checkpoint")
        .authorization;

    assert_eq!(
        executor
            .execute_authorized_output(
                &authorization,
                "checkpoint",
                r#"{"action":"create","label":"guard"}"#,
            )
            .await
            .expect("hidden checkpoint should reach the pinned Runtime host"),
        harness_contract::context::ToolOutputDraft::bounded_inline("authorized:checkpoint")
    );
    assert!(
        executor
            .receipts
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
    assert!(
        executor
            .internal_checkpoint_input(serde_json::json!({
                "action":"create", "label":"guard",
                "paths":["model-must-not-control-this"]
            }))
            .is_err(),
        "an explicit foreign range must not be replaced with the Team grant"
    );
}

#[test]
fn root_write_scope_compiles_to_the_checkpoint_whole_workspace_form() {
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(NoopRuntimeExecutionHost),
        allowed_tools: BTreeSet::new(),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::WorkspaceWriteSandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: None,
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: Some(vec!["workspace:.".to_string()]),
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };

    let input = executor
        .internal_checkpoint_input(serde_json::json!({
            "label": "guard"
        }))
        .expect("whole-workspace checkpoint input");
    assert_eq!(input.get("label"), Some(&serde_json::json!("guard")));
    assert!(input.get("paths").is_none());
    assert!(
        executor
            .internal_checkpoint_input(serde_json::json!({
                "action":"create", "label":"guard", "paths":[]
            }))
            .is_err(),
        "explicit empty range must never fall back to the legacy whole-workspace form"
    );
}

#[test]
fn structured_agent_output_accepts_fenced_json_without_trusting_prose() {
    let exact = r#"{"implementation":"done","source_verification":"receipt"}"#;
    let fenced = format!("```json\n{exact}\n```\n\nHuman-readable evidence summary.");

    assert_eq!(
        structured_agent_output(exact),
        structured_agent_output(&fenced)
    );
    assert_eq!(
        structured_agent_output(&fenced).and_then(|object| object.get("implementation").cloned()),
        Some(serde_json::Value::String("done".to_string()))
    );
    assert!(structured_agent_output("implementation completed in prose").is_none());
    assert!(structured_agent_output(r#"prefix {"unrelated":"claim"} suffix"#).is_none());
    assert!(structured_agent_output(r#"{"unrelated":"claim"}"#).is_none());
}

#[test]
fn structured_agent_output_tolerates_safe_provider_shape_drift() {
    let wrapped = r#"{"output":{"Conclusion":"done","证据":["tool://read"]}}"#;
    let output = structured_agent_output(wrapped).expect("known wrapped contract");
    assert_eq!(output["summary"], "done");
    assert_eq!(output["evidence"][0], "tool://read");

    let encoded = r#"{"data":"{\"finding\":\"one finding\",\"gaps\":[]}"}"#;
    let output = structured_agent_output(encoded).expect("encoded contract object");
    assert_eq!(output["findings"], "one finding");
    assert_eq!(output["unresolved"], serde_json::json!([]));

    let localized = "### 总结\n完成读取。\n\n**风险**\n无。";
    let output = structured_agent_output(localized).expect("localized headings");
    assert_eq!(output["summary"], "完成读取。");
    assert_eq!(output["risks"], "无。");

    let trailing = "{\"findings\":\"verified\",\"risks\":[],}";
    let output = structured_agent_output(trailing).expect("trailing comma repair");
    assert_eq!(output["findings"], "verified");
    assert_eq!(output["risks"], serde_json::json!([]));

    let labeled = "Summary: verified result\nRisks: none identified";
    let output = structured_agent_output(labeled).expect("exact labeled contract");
    assert_eq!(output["summary"], "verified result");
    assert_eq!(output["risks"], "none identified");

    let legacy = "**Field: retired_acceptance_field**\nACCEPTED";
    assert!(
        structured_agent_output(legacy).is_none(),
        "retired presentation labels cannot become acceptance evidence"
    );

    let current =
        "## Key Decisions\nKeep the fenced Program binding.\n\n## Unresolved Or Risks\nNone.";
    let output = structured_agent_output(current).expect("current Team fields");
    assert_eq!(output["key_decisions"], "Keep the fenced Program binding.");
    assert_eq!(output["unresolved_or_risks"], "None.");

    assert!(structured_agent_output(r#"{"output":{"unrelated":"claim"}}"#).is_none());
}

#[test]
fn structured_agent_output_normalizes_only_exact_contract_headings() {
    let markdown =
        "intro\n\n## Review\nverified from fresh receipts\n\n## Risks\nNone identified.\n";
    let output = structured_agent_output(markdown).expect("heading contract");
    assert_eq!(output["review"], "verified from fresh receipts");
    assert_eq!(output["risks"], "None identified.");
    assert!(structured_agent_output("Review complete; no risks.").is_none());

    let quoted_upstream_then_terminal = concat!(
        "upstream: {\"implementation\":\"done\",\"source_verification\":\"old\"}\n",
        "terminal: {\"review\":\"verified\",\"risks\":\"none\"}"
    );
    let output = structured_agent_output(quoted_upstream_then_terminal)
        .expect("last embedded contract object");
    assert!(output.get("implementation").is_none());
    assert_eq!(output["review"], "verified");
}

#[test]
fn explicit_terminal_sections_outrank_embedded_contract_examples() {
    let report = concat!(
        "# Reviewer terminal report\n\n",
        "## findings\n",
        "The reviewed parser accepts examples such as `",
        "{\"evidence\":\"embedded source example\",\"review\":\"not the terminal\"}",
        "`, but the source example is not this report's contract.\n\n",
        "## evidence\n",
        "tool://runtime-read-receipt\n\n",
        "## summary\n",
        "Independent review completed with no contradiction.\n\n",
        "## unresolved\n",
        "[]\n",
    );

    let output = structured_agent_output(report).expect("explicit terminal contract");
    assert_eq!(
        output["summary"],
        "Independent review completed with no contradiction."
    );
    assert_eq!(output["unresolved"], "[]");
    assert!(output["findings"]
        .as_str()
        .is_some_and(|value| value.contains("embedded source example")));
    assert_ne!(output["evidence"], "embedded source example");
}

#[test]
fn disclosure_fields_share_explicit_empty_list_semantics() {
    for field in ["risks", "unresolved", "unresolved_or_risks"] {
        assert!(structured_contract_field_materialized(
            field,
            Some(&serde_json::json!([])),
        ));
        assert!(!structured_contract_field_materialized(field, None));
        assert!(!structured_contract_field_materialized(
            field,
            Some(&serde_json::Value::Null),
        ));
        assert!(!structured_contract_field_materialized(
            field,
            Some(&serde_json::json!("")),
        ));
    }
}

#[test]
fn declared_custom_artifact_accepts_exact_json_or_markdown_only() {
    let required = vec![
        "applications_survey".to_string(),
        "evidence".to_string(),
        "summary".to_string(),
    ];
    let markdown = concat!(
        "## applications_survey\n",
        "Survey body.\n\n",
        "### Vision / 3D\n",
        "Nested details remain part of the custom artifact.\n\n",
        "## evidence\n",
        "tool://read-1\n\n",
        "## summary\n",
        "Survey complete.\n",
    );
    let output = structured_agent_output_for_fields(markdown, &required)
        .expect("declared custom Markdown artifact");
    assert!(output["applications_survey"]
        .as_str()
        .is_some_and(|value| value.contains("### Vision / 3D")));
    assert_eq!(output["evidence"], "tool://read-1");
    assert_eq!(output["summary"], "Survey complete.");

    let json = r#"{"applications_survey":{"vision":"verified"},"summary":"done"}"#;
    let output =
        structured_agent_output_for_fields(json, &required).expect("declared custom JSON artifact");
    assert_eq!(output["applications_survey"]["vision"], "verified");
    assert_eq!(output["summary"], "done");

    assert!(
        structured_agent_output_for_fields("## undeclared_artifact\nself reported", &required,)
            .is_none()
    );
    assert!(structured_agent_output(r#"{"applications_survey":"self reported"}"#).is_none());
}

#[test]
fn verified_narrative_terminal_accepts_prose_without_accepting_tool_markup() {
    use harness_contract::agent::StructuredOutputField;

    let prose = normalized_narrative_terminal_body(
        "Cargo.toml declares the workspace package metadata.",
        &[StructuredOutputField::Findings],
    )
    .expect("verified bounded prose should be a findings carrier");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&prose).expect("normalized JSON")["findings"],
        "Cargo.toml declares the workspace package metadata."
    );
    assert!(
        normalized_narrative_terminal_body(
            "Both researchers confirmed the workspace metadata.",
            &[
                StructuredOutputField::Summary,
                StructuredOutputField::Unresolved,
            ],
        )
        .is_none(),
        "Runtime must not invent an unresolved conclusion that the Agent omitted"
    );
    let mixed_contract = normalized_narrative_terminal_body(
        "Terminal review completed from three fresh source receipts.\n\n{\"unresolved\":[]}",
        &[
            StructuredOutputField::Review,
            StructuredOutputField::Unresolved,
        ],
    )
    .expect("an explicit unresolved declaration may coexist with a Runtime-normalized review");
    let mixed_output =
        serde_json::from_str::<serde_json::Value>(&mixed_contract).expect("normalized JSON");
    assert_eq!(mixed_output["unresolved"], serde_json::json!([]));
    assert!(structured_field_materialized(
        StructuredOutputField::Unresolved,
        mixed_output.get("unresolved"),
    ));
    assert!(mixed_output["review"]
        .as_str()
        .is_some_and(|review| review.contains("Terminal review completed")));
    assert!(normalized_narrative_terminal_body(
        "<synthesized_terminal evidence_committed=1 />",
        &[StructuredOutputField::Findings],
    )
    .is_none());
    assert!(normalized_narrative_terminal_body(
        "<tool_call>read_file</tool_call>",
        &[StructuredOutputField::Findings],
    )
    .is_none());
    assert!(
        normalized_narrative_terminal_body("reviewed", &[StructuredOutputField::Review],).is_some(),
        "a verified review may use ordinary terminal prose"
    );
}

#[test]
fn verified_narrative_terminal_accepts_technical_prose_but_not_risk_declarations() {
    use harness_contract::agent::StructuredOutputField;

    let prose = "Updated the parser and verified the changed file with a fresh read.";
    let normalized = normalized_narrative_terminal_body(
        prose,
        &[
            StructuredOutputField::Implementation,
            StructuredOutputField::SourceVerification,
            StructuredOutputField::Review,
        ],
    )
    .expect("receipt-verified technical prose should not require JSON syntax");
    let output = serde_json::from_str::<serde_json::Value>(&normalized).expect("JSON carrier");
    assert_eq!(output["implementation"], prose);
    assert_eq!(output["source_verification"], prose);
    assert_eq!(output["review"], prose);

    assert!(
        normalized_narrative_terminal_body(prose, &[StructuredOutputField::Risks],).is_none(),
        "Runtime must not infer that risks were considered"
    );
    assert!(
        normalized_narrative_terminal_body(prose, &[StructuredOutputField::UnresolvedOrRisks],)
            .is_none(),
        "Runtime must not infer unresolved work from generic prose"
    );
}

#[test]
fn terminal_structured_acceptance_is_single_pass_and_never_invents_missing_fields() {
    let terminal = r#"{"implementation":"done"}"#;
    let first = structured_agent_output(terminal).expect("deterministic terminal JSON");
    let second = structured_agent_output(terminal).expect("repeat deterministic parse");

    assert_eq!(first, second);
    assert_eq!(first["implementation"], "done");
    assert!(first.get("source_verification").is_none());
    assert!(first.get("review").is_none());
    assert!(structured_agent_output("implementation completed in prose").is_none());
}

#[tokio::test]
async fn scoped_executor_propagates_runtime_authorization_for_normal_agent_tools() {
    struct LeaseCheckingHost(harness_contract::agent::AgentDataLease);
    #[async_trait::async_trait]
    impl crate::RuntimeExecutionHost for LeaseCheckingHost {
        async fn execute_runtime_tool(
            &self,
            request: &crate::RuntimeToolExecutionRequest,
        ) -> crate::RuntimeToolExecutionOutcome {
            assert_eq!(request.reality_context.as_ref(), Some(&self.0));
            EchoRuntimeExecutionHost.execute_runtime_tool(request).await
        }
        fn delegated_tool_effect_descriptor(
            &self,
            name: &str,
            input: &serde_json::Value,
        ) -> Option<harness_contract::tool::ToolEffectDescriptor> {
            test_tool_descriptor_for_input(name, input)
        }
    }
    let reality_lease = harness_contract::agent::AgentDataLease {
        session_id: "session".into(),
        task_id: "task".into(),
        team_id: None,
        read_scopes: vec![harness_contract::agent::CognitiveReadScope::Session],
        write_mode: harness_contract::agent::CognitiveWriteMode::CandidateOnly,
        fact_boundaries: vec!["observed".into()],
        fact_refs: vec!["fact:exact".into()],
        matrix_snapshot_refs: vec!["matrix:source_snapshot:exact".into()],
    };
    let executor = ScopedRuntimeToolExecutor {
        tool_batch: None,
        host: Arc::new(LeaseCheckingHost(reality_lease.clone())),
        allowed_tools: BTreeSet::from(["read_file".to_string()]),
        session_id: "session".to_string(),
        sandbox_posture: harness_contract::policy::SandboxPosture::ReadOnlySandbox,
        policy_revision: 1,
        memory_context: memory::MemoryTurnContext::new("session", "agent"),
        reality_context: Some(reality_lease),
        model_lease: "model".to_string(),
        execution_id: "graph".to_string(),
        node_id: "node".to_string(),
        attempt: 1,
        workspace_root: std::path::PathBuf::from("/workspace"),
        path_identity_resolver: Arc::new(
            crate::path_identity::WorkspacePathIdentityResolver::discover(
                &std::env::current_dir().expect("current directory"),
            )
            .expect("path identities"),
        ),
        scope_locks: Arc::new(ScopeLockManager::new()),
        commit_service: None,
        resource_scopes: None,
        managed_invocation: None,
        next_receipt_sequence: AtomicU64::new(0),
        receipts: Mutex::new(Vec::new()),
        provider_model_obligations: Vec::new(),
    };
    let descriptor = executor
        .registered_tool_effect("read_file", &serde_json::json!({"path": "README.md"}))
        .expect("allow-listed delegated tool must describe its effect");
    let effective = crate::AuthorizationNegotiator::compile_effective_descriptor(
        &descriptor,
        r#"{"path":"README.md"}"#,
    );
    let authorization = crate::ToolPolicy
        .authorize(
            &effective,
            &test_capability_assessment(&descriptor, PermissionMode::ReadOnly),
            "agent-test",
            test_authorization_lease(&descriptor, PermissionMode::ReadOnly, "agent-test"),
            30,
        )
        .expect("read tool should be authorized")
        .authorization;
    assert_eq!(
        executor
            .execute_authorized_output(&authorization, "read_file", r#"{"path":"README.md"}"#)
            .await
            .expect("authorized tool should execute"),
        harness_contract::context::ToolOutputDraft::bounded_inline("authorized:read_file")
    );
    assert!(executor
        .execute_output("read_file", r#"{"path":"README.md"}"#)
        .await
        .is_err());
    assert!(executor
        .execute_authorized_output(&authorization, "edit", r#"{"path":"README.md"}"#)
        .await
        .is_err());
}

#[test]
fn durable_audits_are_promoted_to_agent_evidence_refs() {
    let packet = AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None, "agent", "run", "task", "session", "mission", None, "graph", "node",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "inspect".into(),
        required_acceptance: Default::default(),
        output_acceptance: Vec::new(),
        acceptance: Vec::new(),
        cohort_prompt_package: None,
        constraints: Vec::new(),
        context_refs: Vec::new(),
        evidence_refs: vec![harness_contract::context::EvidenceAccessRef::durable(
            harness_contract::context::EvidenceRef::observed("upstream", "frame"),
            "sha256:frame",
            1,
            "text/plain",
            "artifact://art_worker_fixture_1",
            "session:session",
        )],
        resource_scopes: Vec::new(),
        allowed_tools: Vec::new(),
        allowed_skills: Vec::new(),
        permission_ceiling: harness_contract::policy::PermissionMode::ReadOnly,
        model_lease: "model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget",
            "agent",
            "agent",
            1,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "key".into(),
        agentic_binding: None,
    };
    let tool_access = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("tool", "tool-1"),
        "sha256:tool",
        1,
        "text/plain",
        "artifact://art_worker_fixture_2",
        "session:session",
    );
    let audits = vec![harness_contract::context::EvidenceAuditProjection {
        evidence_ref: tool_access.evidence_ref.clone(),
        content_kind: harness_contract::context::EvidenceContentKind::Text,
        raw_tokens: 1,
        receipt_tokens: 1,
        omitted_tokens: 0,
        raw_available: true,
        access: Some(tool_access),
    }];

    assert_eq!(
        agent_evidence_refs(&packet, &audits, &[])
            .into_iter()
            .map(|reference| reference.evidence_ref.id)
            .collect::<Vec<_>>(),
        vec!["tool-1".to_string(), "frame".to_string()]
    );
}

#[test]
fn exact_acceptance_evidence_requires_a_complete_model_receipt() {
    use harness_contract::context::{
        EvidenceCoverageKind, EvidenceObligation, EvidenceObligationKind,
        EvidenceObservationRequirement, EvidenceTargetIdentity, ObservedEvidence,
        ObservedEvidenceProvenance, ProviderModelObservationAttestation, RequiredAcceptance,
        WorkspaceAccessMode, WorkspaceObjectKind, WorkspacePathIdentity, WorkspaceScopeIdentity,
    };

    let access = harness_contract::context::EvidenceAccessRef::durable(
        harness_contract::context::EvidenceRef::observed("tool", "exact-read"),
        "sha256:exact-read",
        42,
        "application/json",
        "artifact://art_worker_exact_read",
        "session:session",
    );
    let observed = ObservedEvidence {
        obligation_id: "read:src/lib.rs".to_string(),
        target: EvidenceTargetIdentity::Workspace {
            scope: WorkspaceScopeIdentity {
                access_mode: WorkspaceAccessMode::Read,
                path: WorkspacePathIdentity {
                    workspace_id: "workspace".to_string(),
                    repository_id: "repository".to_string(),
                    workspace_relative_path: "src/lib.rs".to_string(),
                    repository_relative_path: "src/lib.rs".to_string(),
                    object_kind: WorkspaceObjectKind::File,
                    observed_revision_or_digest: Some("d".repeat(64)),
                },
                coverage: EvidenceCoverageKind::ExactContent,
            },
        },
        observed_at_sequence: 1,
        tool_name: "read_file".to_string(),
        provenance: ObservedEvidenceProvenance::FreshExecution,
        evidence_ref: Some(access.clone()),
        model_observation: None,
        workspace_prior_state: None,
    };
    let receipt = ScopedToolExecutionReceipt {
        sequence: 1,
        provider_invocation_id: Some("tool-call-1".to_string()),
        tool_name: "read_file".to_string(),
        effect_kind: harness_contract::tool::ToolEffectKind::Read,
        resource_scopes: vec!["read:src/lib.rs".to_string()],
        paths: vec!["src/lib.rs".to_string()],
        prior_states: BTreeMap::new(),
        after_digests: BTreeMap::new(),
        observed_bytes: BTreeMap::new(),
        observed_evidence: vec![observed.clone()],
    };
    let required = RequiredAcceptance {
        criteria: Vec::new(),
        evidence_obligations: vec![EvidenceObligation {
            obligation_id: "required-read".to_string(),
            kind: EvidenceObligationKind::ContentRead,
            target: observed.target.clone(),
            observation_requirement: EvidenceObservationRequirement::ProviderModel,
        }],
    };
    let complete = ProviderModelObservationAttestation {
        provider_invocation_id: "tool-call-1".to_string(),
        obligation_ids: vec!["required-read".to_string()],
        raw_ref: access.evidence_ref.clone(),
        model_receipt_sha256: format!("sha256:{}", "a".repeat(64)),
        raw_tokens: 42,
        receipt_tokens: 42,
        omitted_tokens: 0,
        complete: true,
        provider_request_sequence: 2,
        provider_attempt: 1,
        model: "qwen3.8-max".to_string(),
    };

    let promoted = model_observed_evidence(
        &required,
        std::slice::from_ref(&complete),
        std::slice::from_ref(&receipt),
    );
    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0].model_observation.as_ref(), Some(&complete));
    assert!(crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(
        &required.evidence_obligations[0],
        &promoted,
    ));

    let mut omitted = complete.clone();
    omitted.omitted_tokens = 1;
    omitted.complete = false;
    let incomplete = model_observed_evidence(&required, &[omitted], &[receipt.clone()]);
    assert!(!crate::acceptance_evaluator::AcceptanceEvaluator::evaluate(
        &required.evidence_obligations[0],
        &incomplete,
    ));

    let mut unrelated = complete;
    unrelated.provider_invocation_id = "tool-call-2".to_string();
    let uncorrelated = model_observed_evidence(&required, &[unrelated], &[receipt]);
    assert_eq!(uncorrelated, vec![observed]);
}

#[test]
fn delegated_child_session_inherits_the_runtime_services_workspace() {
    let workspace = std::path::Path::new("/workspace/project");
    let session = delegated_child_session("parent-session", "model", workspace);

    assert_eq!(session.session_id, "parent-session");
    assert_eq!(session.model.as_deref(), Some("model"));
    assert_eq!(session.workspace_root(), Some(workspace));
}

#[cfg(unix)]
#[tokio::test]
async fn native_scope_registers_before_validation_and_fences_full_packet_on_release() {
    let worker = InProcessAgentWorker::new(Weak::new());
    let packet = test_agent_packet(vec![]);
    let digest = |packet: &AgentTaskPacket| {
        format!("{:x}", Sha256::digest(serde_json::to_vec(packet).unwrap()))
    };
    let original = digest(&packet);
    assert_eq!(
        worker.local_hook_scope_closed(packet.run_id(), &original),
        Some(false)
    );
    let error = worker
        .execute(
            packet.clone(),
            AgentModelSelection {
                model: "inert".into(),
                provider: "not-called".into(),
                registry_revision: 0,
            },
        )
        .await
        .unwrap_err();
    assert!(error.contains("Binding"));
    assert_eq!(
        worker.local_hook_scope_closed(packet.run_id(), &original),
        Some(true)
    );
    let mut foreign_attempt = packet.clone();
    foreign_attempt.attempt += 1;
    let mut foreign_payload = packet.clone();
    foreign_payload.objective = "foreign payload".into();
    for foreign in [foreign_attempt, foreign_payload] {
        assert_eq!(
            worker.local_hook_scope_closed(packet.run_id(), &digest(&foreign)),
            Some(false)
        );
        worker.release_local_hook_scope(packet.run_id(), &digest(&foreign));
        assert!(worker
            .active_runs
            .lock()
            .unwrap()
            .contains_key(packet.run_id()));
    }
    worker.release_local_hook_scope(packet.run_id(), &original);
    assert!(!worker
        .active_runs
        .lock()
        .unwrap()
        .contains_key(packet.run_id()));
    assert_eq!(
        worker.local_hook_scope_closed(packet.run_id(), &original),
        Some(false)
    );
}

#[tokio::test]
async fn send_input_enters_the_live_child_turn_inbox() {
    let worker = InProcessAgentWorker::new(Weak::new());
    let stream = crate::SessionInputStream::new("child-session");
    stream.set_active_turn(Some(TurnId::from_string("child-turn")));
    worker.active_runs.lock().unwrap().insert(
        "run-1".into(),
        ActiveInProcessRun {
            cancellation: crate::CancellationToken::new(),
            session_id: "child-session".into(),
            input_stream: Some(stream.clone()),
            packet_sha256: "input-packet".into(),
            hook_scope: crate::HookExecutionScope::default(),
            owner_ended: false,
        },
    );
    worker
        .command(
            &AgentRunHandle {
                run_id: "run-1".into(),
                agent_id: "agent-1".into(),
                backend: AgentBackendKind::InProcess,
                revision: 1,
                status: harness_contract::agent::AgentStatus::Running,
            },
            &AgentCommandRequest {
                command_id: "input-1".into(),
                agent_id: "agent-1".into(),
                expected_revision: 1,
                command: AgentCommand::SendInput,
                input: Some(AgentInput::UserSupplement("use the new requirement".into())),
            },
        )
        .await
        .expect("input accepted");
    let inbox = stream.inbox_snapshot(Some(TurnId::from_string("child-turn")));
    assert_eq!(inbox.items.len(), 1);
    assert_eq!(inbox.items[0].content_preview, "use the new requirement");
    assert!(worker.capabilities().supports_input);
    assert!(!worker.capabilities().supports_pause);
}

#[tokio::test]
async fn cancel_acknowledges_delivery_before_cleanup_and_tombstones_are_race_safe() {
    let worker = Arc::new(InProcessAgentWorker::new(Weak::new()));
    let cancellation = crate::CancellationToken::new();
    worker.active_runs.lock().unwrap().insert(
        "run-cancel".into(),
        ActiveInProcessRun {
            cancellation: cancellation.clone(),
            session_id: "child-session".into(),
            input_stream: Some(crate::SessionInputStream::new("child-session")),
            packet_sha256: "original-cancel-packet".into(),
            hook_scope: crate::HookExecutionScope::default(),
            owner_ended: false,
        },
    );
    let cleanup = ActiveRunCleanup {
        worker: worker.as_ref(),
        run_id: "run-cancel".into(),
    };
    let handle = AgentRunHandle {
        run_id: "run-cancel".into(),
        agent_id: "agent-cancel".into(),
        backend: AgentBackendKind::InProcess,
        revision: 1,
        status: harness_contract::agent::AgentStatus::Running,
    };
    let request = AgentCommandRequest {
        command_id: "cancel-1".into(),
        agent_id: "agent-cancel".into(),
        expected_revision: 1,
        command: AgentCommand::Cancel,
        input: None,
    };
    let cancel = {
        let worker = Arc::clone(&worker);
        let handle = handle.clone();
        let request = request.clone();
        tokio::spawn(async move { worker.command(&handle, &request).await })
    };
    cancellation.cancelled().await;
    tokio::time::timeout(std::time::Duration::from_secs(1), cancel)
        .await
        .expect("cancel acknowledges without waiting for cleanup")
        .expect("cancel task joins")
        .expect("cancel is accepted");
    assert!(worker
        .active_runs
        .lock()
        .unwrap()
        .contains_key("run-cancel"));
    assert!(worker
        .pending_cancellations
        .lock()
        .unwrap()
        .contains("run-cancel"));
    assert_eq!(
        worker.local_hook_scope_closed("run-cancel", "original-cancel-packet"),
        Some(false)
    );
    drop(cleanup);
    assert!(worker
        .active_runs
        .lock()
        .unwrap()
        .contains_key("run-cancel"));
    assert!(!worker.run_completed("run-cancel"));
    assert_eq!(
        worker.local_hook_scope_closed("run-cancel", "foreign-packet"),
        Some(false)
    );
    worker.release_local_hook_scope("run-cancel", "foreign-packet");
    assert!(worker
        .active_runs
        .lock()
        .unwrap()
        .contains_key("run-cancel"));
    assert_eq!(
        worker.local_hook_scope_closed("run-cancel", "original-cancel-packet"),
        Some(true)
    );
    // The Kernel calls this acknowledgement only after its exact local_exit
    // commit. Future Drop and a foreign packet cannot free the original owner.
    worker.release_local_hook_scope("run-cancel", "original-cancel-packet");
    assert_eq!(
        worker.local_hook_scope_closed("run-cancel", "original-cancel-packet"),
        Some(false)
    );
    assert!(worker.active_runs.lock().unwrap().is_empty());
    assert!(worker.pending_cancellations.lock().unwrap().is_empty());
    assert!(worker.run_completed("run-cancel"));

    // A command arriving in the just-completed/no-active window must use
    // the bounded tombstone instead of waiting ten seconds as if the run
    // had not registered yet.
    tokio::time::timeout(
        std::time::Duration::from_secs(1),
        worker.command(&handle, &request),
    )
    .await
    .expect("completed tombstone resolves cancellation")
    .expect("completed cancellation is idempotent");
    assert!(worker.pending_cancellations.lock().unwrap().is_empty());

    // A snapshot without a live backend handle must not be acknowledged as
    // cancelled. It can be a restored orphan and no execution owner exists to
    // observe an admission tombstone.
    let pending_handle = AgentRunHandle {
        run_id: "run-admission-race".into(),
        agent_id: "agent-admission-race".into(),
        ..handle
    };
    let missing = tokio::time::timeout(
        std::time::Duration::from_secs(1),
        worker.command(&pending_handle, &request),
    )
    .await
    .expect("missing-handle cancel is non-blocking");
    assert_eq!(
        missing,
        Err(harness_contract::agent::AgentCommandRejectReason::UnsupportedByBackend)
    );
    assert!(worker.pending_cancellations.lock().unwrap().is_empty());
}

#[test]
fn blocked_child_turn_is_not_relabelled_as_completed_agent_work() {
    let (status, failure) = agent_terminal_outcome(
        harness_contract::goal::GoalCompletion::Partial,
        "provider path exhausted",
    );
    assert_eq!(status, AgentTerminalStatus::Blocked);
    assert_eq!(failure.as_deref(), Some("provider path exhausted"));
}

#[test]
fn delegated_prompt_rejects_simulated_tool_markup() {
    let mut packet = AgentTaskPacket {
        assignment: crate::test_support::agent_assignment(
            None,
            "agent",
            "run",
            "task",
            "session",
            "mission",
            Some("team"),
            "graph",
            "node",
        ),
        attempt: 1,
        expected_graph_revision: 0,
        policy_revision: 1,
        objective: "inspect source".into(),
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
        model_lease: "model".into(),
        budget_lease: harness_contract::context::ChildExecutionBudgetReservation::single(
            "budget",
            "agent",
            "agent",
            1,
            u64::MAX,
            1,
        ),
        deadline_at_ms: u64::MAX,
        binding: None,
        managed_invocation: None,
        idempotency_key: "key".into(),
        agentic_binding: None,
    };
    let prompt_segments = system_prompt(&packet, std::path::Path::new("/workspace"), &[]);
    let cohort_boundary = prompt_segments
        .iter()
        .position(|segment| segment == crate::SYSTEM_PROMPT_CACHE_COHORT_BOUNDARY)
        .expect("cache cohort boundary");
    let dynamic_boundary = prompt_segments
        .iter()
        .position(|segment| segment == crate::SYSTEM_PROMPT_DYNAMIC_BOUNDARY)
        .expect("dynamic boundary");
    let objective = prompt_segments
        .iter()
        .position(|segment| segment.starts_with("Objective:"))
        .expect("immutable objective");
    assert!(cohort_boundary < objective);
    assert!(objective < dynamic_boundary);
    assert_eq!(dynamic_boundary, prompt_segments.len() - 1);
    let prompt = prompt_segments.join("\n");
    assert!(prompt.contains("Never write simulated tool syntax"));
    assert!(prompt.contains("If no native tool is authorized, answer directly"));
    assert!(!prompt.contains("## Runtime clock"));

    packet.resource_scopes = vec!["read:external-app".to_string()];
    let scoped_prompt = system_prompt(&packet, std::path::Path::new("/workspace"), &[]).join("\n");
    assert!(scoped_prompt.contains("scope read:project means project/Cargo.toml"));
    assert!(scoped_prompt.contains("never bare Cargo.toml"));

    packet.objective = "update fixtures/target.txt".into();
    packet.output_acceptance = vec![harness_contract::agent::OutputAcceptanceRequirement {
        criterion: "implementation".to_string(),
        check: harness_contract::agent::OutputAcceptanceCheck::WorkspaceChange {
            field: harness_contract::agent::StructuredOutputField::Implementation,
            scopes: vec!["write:fixtures/target.txt".to_string()],
        },
    }];
    let mutation_prompt = system_prompt(
        &packet,
        std::path::Path::new("/workspace"),
        &["read_file".into(), "edit".into()],
    )
    .join("\n");
    assert!(mutation_prompt.contains("Read each target at most once before mutation"));
    assert!(mutation_prompt.contains("write:fixtures/target.txt"));
    assert!(mutation_prompt.contains("Repeated reads"));
    assert!(mutation_prompt.contains("Native structured output"));
    assert!(!mutation_prompt.contains("Return exactly one JSON object"));
}

#[test]
fn scoped_tool_effect_key_is_stable_across_worker_recovery() {
    let first = deterministic_scoped_tool_idempotency_key(
        "graph-1",
        "node-1",
        2,
        3,
        "edit",
        r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
    );
    let recovered = deterministic_scoped_tool_idempotency_key(
        "graph-1",
        "node-1",
        2,
        3,
        "edit",
        r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
    );
    let next_effect = deterministic_scoped_tool_idempotency_key(
        "graph-1",
        "node-1",
        2,
        4,
        "edit",
        r#"{\"path\":\"src/lib.rs\",\"content\":\"updated\"}"#,
    );

    assert_eq!(first, recovered);
    assert_ne!(first, next_effect);
    assert!(first.contains("agent-tool:graph-1:node-1:2:3:edit:"));
}


#[test]
fn recovered_receipt_context_is_bounded_and_explicitly_fences_tools() {
    let receipt = crate::execution_core::graph::DurableAgentToolReceipt {
        committed_cursor: 1,
        effect_scope: None,
        effect_scopes: Vec::new(),
        sequence: 7,
        effect_kind: harness_contract::tool::ToolEffectKind::Write,
        authorized_scopes: vec!["write:src/lib.rs".to_string()],
        outcome: crate::RuntimeToolExecutionOutcome {
            tool_use_id: "tool-7".to_string(),
            tool_name: "edit".to_string(),
            status: crate::RuntimeToolExecutionStatus::Executed,
            category: crate::ToolSafetyCategory::WriteLocal,
            output: Some("committed output".to_string()),
            error: None,
            evidence_ref: "tool://receipt-7".to_string(),
            observed_evidence: Vec::new(),
            pending_agent_batch: None,
        },
    };

    let mut read = receipt.clone();
    read.outcome.tool_name = "evidence_retrieve".into();
    read.effect_kind = harness_contract::tool::ToolEffectKind::Read;
    let retained = recoverable_effect_receipts(&[read.clone(), receipt.clone()]);
    assert_eq!(retained, vec![receipt.clone()]);
    assert!(recoverable_effect_receipts(&[read]).is_empty());
    let prompt = recovered_agent_tool_receipt_prompt(&[receipt], false).expect("recovery prompt");
    assert!(prompt.contains("committed output"));
    assert!(prompt.contains("Do not call tools"));
    assert!(prompt.contains("ToolHost receipts"));
}


#[test]
fn autonomous_checkpoint_tool_overlay_is_minimal_and_action_specific() {
    let mut packet = test_agent_packet(Vec::new());
    packet.allowed_tools = vec![
        "tool_search".to_string(),
        "task_claim".to_string(),
        "message_publish".to_string(),
        "read_file".to_string(),
        "edit".to_string(),
    ];

    assert!(autonomy_checkpoint_tool_plan(&packet, false, false).is_empty());
    assert_eq!(
        autonomy_checkpoint_tool_plan(&packet, true, false),
        vec!["task_claim".to_string(), "message_publish".to_string()]
    );
    let execution = autonomy_checkpoint_tool_plan(&packet, true, true);
    assert_eq!(
        execution,
        vec![
            "task_claim".to_string(),
            "message_publish".to_string(),
            "read_file".to_string(),
            "edit".to_string(),
        ]
    );
    assert!(!execution.iter().any(|tool| tool == "tool_search"));
}

#[test]
fn generic_team_packet_has_no_retired_graph_work_market_checkpoint() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    let packet = test_agent_packet(Vec::new());
    assert!(
        agent_autonomy_checkpoint(&services, &packet)
            .expect("generic packet is not an Agent-first protocol error")
            .is_none(),
        "generic ExecutionGraph/Team packets must not revive a second task ownership plane"
    );
}

#[tokio::test]
async fn native_checkpoint_closes_only_after_its_typed_decline() {
    use harness_contract::agent_action::{
        AgentActionStatus, AgentAttemptMode, TaskAttemptDispatchInput,
    };
    let services = Arc::new(RuntimeServices::in_memory().unwrap());
    let actions = services.agent_action_service();
    let mut actor = AgentActorBinding {
        original_execution: None,
        objective_id: "decline-objective".into(),
        program_id: "decline-program".into(),
        session_id: "decline-session".into(),
        turn_id: "decline-turn".into(),
        root_execution_id: None,
        required_team_count: 1,
        exact_team_count: None,
        objective_summary: "choose useful work".into(),
        model_lease: "test".into(),
        permission_ceiling: Some(PermissionMode::ReadOnly),
        resource_scopes: vec![],
        actor_id: "root".into(),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let apply = |id: &str, actor: &AgentActorBinding, action| {
        actions
            .apply(&AgentActionEnvelope {
                action_id: id.into(),
                actor: actor.clone(),
                expected_revision: None,
                action,
            })
            .unwrap()
    };
    let team = apply(
        "team",
        &actor,
        AgentAction::TeamCreate(TeamCreateInput {
            name: "Readers".into(),
            mission: "read evidence".into(),
            objective: None,
        }),
    )
    .changed_refs[0]
        .clone();
    let member = apply("member", &actor, AgentAction::AgentInvite(serde_json::from_value(serde_json::json!({
        "team_ref":team,"role":"Reader","mission":"inspect evidence","required_capabilities":["read"]
    })).unwrap())).changed_refs[0].clone();
    let task = apply("task", &actor, AgentAction::TaskPublish(serde_json::from_value(serde_json::json!({
        "team_ref":team,"title":"Evidence","objective":"read evidence","acceptance":"cited source"
    })).unwrap())).changed_refs[0].clone();
    let mut packet = test_agent_packet(vec![]);
    packet.assignment = crate::test_support::agent_assignment(
        None,
        &member,
        "decline-run",
        &task,
        "decline-session",
        "decline-run",
        Some(&team),
        "decline-graph",
        "decline-node",
    );
    packet.allowed_tools = vec!["message_publish".into(), "task_claim".into()];
    packet.agentic_binding = Some(AgenticExecutionBinding {
        program_id: "decline-program".into(),
        agent_id: member.clone(),
        membership_id: format!("membership:{member}:{team}"),
        team_id: team.clone(),
        task_team_id: team.clone(),
        source_spec_revision: 1,
        task_purpose: None,
        focus: AgenticExecutionFocus::TaskExecute {
            task_ref: task.clone(),
        },
    });
    actor.kind = AgentActorKind::Supervisor;
    actor.actor_id = "runtime.program-supervisor".into();
    actor.execution_id = Some(packet.graph_id().into());
    assert_eq!(
        apply(
            "dispatch",
            &actor,
            AgentAction::TaskAttemptDispatch(TaskAttemptDispatchInput {
                task_ref: task.clone(),
                execution_id: packet.graph_id().into(),
                agent_ref: member.clone(),
                membership_id: format!("membership:{member}:{team}"),
                mode: AgentAttemptMode::Execute,
                generation: 0,
            })
        )
        .status,
        AgentActionStatus::Applied
    );
    let before = agent_autonomy_checkpoint(&services, &packet)
        .unwrap()
        .unwrap();
    let checkpoint: serde_json::Value =
        serde_json::from_str(before.prompt.rsplit_once("\n\n").unwrap().1).unwrap();
    assert_eq!(
        checkpoint["required_actions"][0]["options"][1]["input"]["intent"]["kind"],
        "decline"
    );
    actor.kind = AgentActorKind::Agent;
    actor.actor_id = member.clone();
    actor.agent_id = Some(member);
    actor.team_id = Some(team);
    assert_eq!(apply("prose", &actor, AgentAction::MessagePublish(serde_json::from_value(serde_json::json!({
        "topic_ref":"topic:decline-program", "summary":"DECLINE: narrative only", "refs":[task]
    })).unwrap())).status, AgentActionStatus::Applied);
    assert!(agent_autonomy_checkpoint(&services, &packet)
        .unwrap()
        .is_some());
    assert_eq!(apply("typed", &actor, AgentAction::MessagePublish(serde_json::from_value(serde_json::json!({
        "topic_ref":"topic:decline-program", "summary":"I lack the required expertise", "intent":{"kind":"decline","task_ref":task}
    })).unwrap())).status, AgentActionStatus::Applied);
    assert!(agent_autonomy_checkpoint(&services, &packet)
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn native_coordination_checkpoint_rejects_unrelated_reply() {
    use crate::agentic::coordination::tests::fixture;
    let fixture = fixture(false).await;
    let checkpoint = agent_autonomy_checkpoint(&fixture.services, &fixture.packet)
        .unwrap()
        .unwrap();
    assert!(checkpoint.prompt.contains(&fixture.wake_ref));
    assert!(agentic_task_protocol_pending(&fixture.services, &fixture.packet).unwrap());
    let actions = fixture.services.agent_action_service();
    let mut unrelated = fixture.request.clone();
    unrelated.action_id = "requestor-not-coordinator-reply".into();
    if let AgentAction::MessagePublish(input) = &mut unrelated.action {
        input.intent = None;
        input.refs = vec![fixture.wake_ref.clone()];
        input.summary = Some("requestor self reply".into());
    }
    actions.apply(&unrelated).unwrap();
    assert!(
        agent_autonomy_checkpoint(&fixture.services, &fixture.packet)
            .unwrap()
            .is_some()
    );

}

#[tokio::test]
async fn generalized_coalesced_coordination_wake() {
    let fixture = crate::agentic::coordination::tests::fixture(false).await;
    let actions = fixture.services.agent_action_service();
    for index in 0..128 {
        let mut duplicate = fixture.request.clone();
        duplicate.action_id = format!("model-duplicate-{index}");
        assert_eq!(
            actions.apply(&duplicate).unwrap().status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
    }
    assert_eq!(
        actions
            .project("coord-program")
            .unwrap()
            .coordination_requests()
            .len(),
        1
    );
    let checkpoint = agent_autonomy_checkpoint(&fixture.services, &fixture.packet)
        .unwrap()
        .unwrap();
    let packet = fixture.packet.clone();
    let services = fixture.services.clone();
    // Deterministic decision stub consumes the actual Native checkpoint and
    // immutable packet, then calls the shared authorized runtime tool path.
    // No fixture writes its response into the Program on its behalf.
    let worker = tokio::spawn(async move {
        assert!(checkpoint
            .prompt
            .contains("Check the boundary condition in source A"));
        let binding = packet.agentic_binding.as_ref().unwrap();
        let AgenticExecutionFocus::Coordination { wake_ref } = &binding.focus else {
            panic!("expected coordination")
        };
        let bridge = ProcessJsonlToolSession::prepare(
            &services,
            &packet,
            &AgentModelSelection {
                model: "test".into(),
                provider: "deterministic-stub".into(),
                registry_revision: 0,
            },
        )
        .unwrap();
        let reply=bridge.execute_tool("native-stub-response","message_publish",&serde_json::json!({
            "topic_ref":format!("topic:{}",binding.team_id),"summary":"Source A needs an empty-input boundary check before acceptance","refs":[wake_ref]
        }).to_string()).await.unwrap();
        let observation: serde_json::Value = serde_json::from_str(&reply).unwrap();
        assert_eq!(observation["status"], "applied");
        assert!(agent_autonomy_checkpoint(&services, &packet)
            .unwrap()
            .is_none());
        services
            .settle_abandoned_agentic_attempt(&packet, "deterministic worker exit")
            .await
            .unwrap();
    });
    worker.await.unwrap();
    let settled = actions.project("coord-program").unwrap();
    assert_eq!(
        settled.tasks[&fixture.task_ref]
            .claim_execution_id
            .as_deref(),
        Some("primary-execution")
    );
    assert_eq!(
        settled
            .topics
            .values()
            .flatten()
            .filter(|entry| entry.source_execution_id.as_deref() == Some(fixture.packet.graph_id()))
            .count(),
        1
    );
    for _ in 0..3 {
        assert!(fixture
            .services
            .dispatch_ready_agentic_work("coord-program")
            .await
            .unwrap()
            .is_empty());
    }
    let mut novel = fixture.request.clone();
    novel.action_id = "model-new-source".into();
    if let AgentAction::MessagePublish(input) = &mut novel.action {
        input.summary = Some("New source B contradicts the boundary result".into());
    }
    assert_eq!(
        actions.apply(&novel).unwrap().status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    let next = fixture
        .services
        .dispatch_ready_agentic_work("coord-program")
        .await
        .unwrap();
    assert_eq!(next.len(), 1);
    assert_ne!(next[0].graph_id, fixture.packet.graph_id());
}

#[tokio::test]
async fn cross_team_review_checkpoint_is_valid_but_cross_team_execution_is_fenced() {
    let services = Arc::new(RuntimeServices::in_memory().expect("runtime"));
    let actions = services.agent_action_service();
    let root_actor = || AgentActorBinding {
        original_execution: None,
        objective_id: "objective-checkpoint".to_string(),
        program_id: "program-checkpoint".to_string(),
        session_id: "session-checkpoint".to_string(),
        turn_id: "turn-checkpoint".to_string(),
        root_execution_id: Some("root-checkpoint".to_string()),
        required_team_count: 2,
        exact_team_count: None,
        objective_summary: "exercise independent cross-Team review".to_string(),
        model_lease: "model".to_string(),
        permission_ceiling: Some(PermissionMode::ReadOnly),
        resource_scopes: vec!["workspace:.".to_string()],
        actor_id: "root:session-checkpoint".to_string(),
        kind: AgentActorKind::Root,
        execution_id: None,
        team_id: None,
        agent_id: None,
    };
    let apply_root = |action_id: &str, action: AgentAction| {
        actions.apply(&AgentActionEnvelope {
            action_id: action_id.to_string(),
            actor: root_actor(),
            expected_revision: None,
            action,
        })
    };
    let author_team = apply_root(
        "author-team",
        AgentAction::TeamCreate(TeamCreateInput {
            name: "Authors".to_string(),
            mission: "produce evidence".to_string(),
            objective: None,
        }),
    )
    .expect("author team")
    .changed_refs[0]
        .clone();
    let review_team = apply_root(
        "review-team",
        AgentAction::TeamCreate(TeamCreateInput {
            name: "Reviewers".to_string(),
            mission: "independently inspect another Team".to_string(),
            objective: None,
        }),
    )
    .expect("review team")
    .changed_refs[0]
        .clone();
    let author = apply_root(
        "author",
        AgentAction::AgentInvite(AgentInviteInput {
            team_ref: author_team.clone(),
            role: "Author".to_string(),
            mission: "produce".to_string(),
            required_capabilities: vec!["read".to_string()],
            existing_agent_ref: None,
            definition_ref: None,
            model_profile_ref: None,
            expertise_hints: Vec::new(),
            execution_requirements: Vec::new(),
        }),
    )
    .expect("author")
    .changed_refs[0]
        .clone();
    let reviewer = apply_root(
        "reviewer",
        AgentAction::AgentInvite(AgentInviteInput {
            team_ref: review_team.clone(),
            role: "Reviewer".to_string(),
            mission: "verify independently".to_string(),
            required_capabilities: vec!["read".to_string()],
            existing_agent_ref: None,
            definition_ref: None,
            model_profile_ref: None,
            expertise_hints: Vec::new(),
            execution_requirements: Vec::new(),
        }),
    )
    .expect("reviewer")
    .changed_refs[0]
        .clone();
    let task = apply_root(
        "task",
        AgentAction::TaskPublish(TaskPublishInput {
            team_ref: author_team.clone(),
            title: "Cross-Team review target".to_string(),
            objective: "produce durable evidence".to_string(),
            acceptance: "independent review".to_string(),
            criterion_source: Default::default(),
            acceptance_checks: Vec::new(),
            required_capabilities: vec!["read".to_string()],
            depends_on: Vec::new(),

            obligation_refs: Vec::new(),
            purpose: Default::default(),
            execution_requirements: Vec::new(),
            expertise_hints: Vec::new(),
        }),
    )
    .expect("task")
    .changed_refs[0]
        .clone();
    let author_execution = "author-execution";
    let mut author_actor = root_actor();
    author_actor.actor_id.clone_from(&author);
    author_actor.kind = AgentActorKind::Agent;
    author_actor.execution_id = Some(author_execution.to_string());
    author_actor.team_id = Some(author_team.clone());
    author_actor.agent_id = Some(author.clone());
    let apply_author = |action_id: &str, action: AgentAction| {
        actions.apply(&AgentActionEnvelope {
            action_id: action_id.to_string(),
            actor: author_actor.clone(),
            expected_revision: None,
            action,
        })
    };
    apply_author(
        "claim",
        AgentAction::TaskClaim(TaskClaimInput {
            task_ref: task.clone(),
            reason: None,
        }),
    )
    .expect("claim");
    let mut execution_packet = test_agent_packet(Vec::new());
    execution_packet.assignment = crate::test_support::agent_assignment(
        None,
        &author,
        author_execution,
        &task,
        "session-checkpoint",
        author_execution,
        Some(&author_team),
        author_execution,
        "author-node",
    );
    execution_packet.allowed_tools = vec![
        "task_submit".to_string(),
        "artifact_commit".to_string(),
        "read_file".to_string(),
    ];
    execution_packet.agentic_binding = Some(AgenticExecutionBinding {
        program_id: "program-checkpoint".to_string(),
        agent_id: author.clone(),
        membership_id: format!("membership:{author}:{author_team}"),
        team_id: author_team.clone(),
        task_team_id: author_team.clone(),
        source_spec_revision: 1,
        task_purpose: None,
        focus: AgenticExecutionFocus::TaskExecute {
            task_ref: task.clone(),
        },
    });
    let execution_checkpoint = agent_autonomy_checkpoint(&services, &execution_packet)
        .expect("claimed execution checkpoint")
        .expect("claimed Task requires a submission");
    let execution_state: serde_json::Value =
        serde_json::from_str(execution_checkpoint.prompt.rsplit_once("\n\n").unwrap().1)
            .expect("checkpoint JSON");
    assert_eq!(
        execution_state["required_actions"][0]["required_protocol"],
        serde_json::json!(["task_submit"])
    );
    assert!(execution_checkpoint.prompt.contains("normal response text"));
    assert!(execution_checkpoint
        .prompt
        .contains("same Task, Agent, and attempt"));
    assert!(!execution_checkpoint
        .prompt
        .contains("independent task_review verdict"));
    assert!(execution_checkpoint
        .tool_ids
        .iter()
        .any(|tool| tool == "artifact_commit"));
    let content = services
        .artifact_store()
        .write_bytes(
            harness_contract::context::ArtifactWriteDescriptor {
                media_type: "text/markdown".to_string(),
                visibility_scope: "session:session-checkpoint".to_string(),
                expected_bytes: None,
                original_name: Some("cross-team.md".to_string()),
            },
            b"independently reviewable evidence",
        )
        .await
        .expect("content");
    let artifact = apply_author(
        "artifact",
        AgentAction::ArtifactCommit(ArtifactCommitInput {
            content_ref: content.selector.clone(),
            kind: "report".to_string(),
            title: "Cross-Team report".to_string(),
            relates_to: vec![task.clone()],
        }),
    )
    .expect("artifact")
    .changed_refs[0]
        .clone();
    apply_author(
        "submit",
        AgentAction::TaskSubmit(TaskSubmitInput {
            deliverable: None,
            task_ref: task.clone(),
            artifact_refs: vec![artifact],
            evidence_refs: vec![content.selector],
            unresolved: Vec::new(),
        }),
    )
    .expect("submit");

    let mut packet = test_agent_packet(Vec::new());
    packet.assignment = crate::test_support::agent_assignment(
        None,
        &reviewer,
        "review-checkpoint",
        &task,
        "session-checkpoint",
        "review-checkpoint",
        Some(&review_team),
        "review-checkpoint-graph",
        "review-checkpoint-node",
    );
    packet.allowed_tools = vec!["task_review".to_string(), "read_file".to_string()];
    packet.context_refs = vec![task.clone()];
    packet.agentic_binding = Some(AgenticExecutionBinding {
        program_id: "program-checkpoint".to_string(),
        agent_id: reviewer.clone(),
        membership_id: format!("membership:{reviewer}:{review_team}"),
        team_id: review_team,
        task_team_id: author_team,
        source_spec_revision: 1,
        task_purpose: None,
        focus: AgenticExecutionFocus::TaskReview {
            task_ref: task.clone(),
        },
    });
    let checkpoint = agent_autonomy_checkpoint(&services, &packet)
        .expect("cross-Team review checkpoint must remain valid")
        .expect("submitted Task requires a review action");
    assert!(checkpoint.prompt.contains("inspect_and_review"));
    assert!(checkpoint.tool_ids.iter().any(|tool| tool == "task_review"));
    assert!(!checkpoint.prompt.contains("actively task_claim"));
    assert!(!checkpoint.prompt.contains("prefer one task_submit"));

    packet
        .agentic_binding
        .as_mut()
        .expect("test packet has a typed Agent-first binding")
        .focus = AgenticExecutionFocus::TaskExecute {
        task_ref: task.clone(),
    };
    let error = match agent_autonomy_checkpoint(&services, &packet) {
        Err(error) => error,
        Ok(_) => panic!("cross-Team execution must remain fenced"),
    };
    assert_eq!(
        error,
        "Agent-first packet member is outside the bound Task Team"
    );
}

#[test]
fn autonomous_checkpoint_progress_digest_ignores_projection_revision_only() {
    let first = "Runtime safe checkpoint committed.\n\n{\"kind\":\"runtime_agent_autonomy_checkpoint\",\"program_revision\":7,\"required_actions\":[{\"action\":\"decide_claim_or_decline\",\"task_ref\":\"task:1\"}],\"unread_agentic_topic_entries\":[]}";
    let second = first.replace("\"program_revision\":7", "\"program_revision\":42");
    assert_eq!(
        autonomy_checkpoint_progress_digest(first),
        autonomy_checkpoint_progress_digest(&second),
        "projection-only graph revisions must not defeat the liveness fuse"
    );
}

#[test]
fn autonomy_progress_digest_advances_on_semantic_workspace_progress_not_receipt_replay() {
    let prompt = "Runtime safe checkpoint committed.\n\n{\"kind\":\"runtime_agent_autonomy_checkpoint\",\"program_revision\":7,\"required_actions\":[{\"action\":\"execute_commit_submit\",\"task_ref\":\"task:1\"}]}";
    let receipt = |sequence, digest: &str| ScopedToolExecutionReceipt {
        sequence,
        provider_invocation_id: Some(format!("provider:{sequence}")),
        tool_name: "edit".to_string(),
        effect_kind: harness_contract::tool::ToolEffectKind::Write,
        resource_scopes: vec!["workspace:.".to_string()],
        paths: vec!["result.md".to_string()],
        prior_states: BTreeMap::new(),
        after_digests: BTreeMap::from([("result.md".to_string(), Some(digest.to_string()))]),
        observed_bytes: BTreeMap::new(),
        observed_evidence: Vec::new(),
    };
    let first = receipt(1, "sha256:first");
    let replay = receipt(2, "sha256:first");
    let changed = receipt(3, "sha256:changed");

    assert_eq!(
        agent_autonomy_progress_digest(prompt, std::slice::from_ref(&first)),
        agent_autonomy_progress_digest(prompt, &[first, replay]),
        "volatile receipt identity must not defeat the liveness fuse"
    );
    assert_ne!(
        agent_autonomy_progress_digest(prompt, &[]),
        agent_autonomy_progress_digest(prompt, std::slice::from_ref(&changed)),
        "new durable content must keep a complex task alive"
    );
}

#[test]
fn autonomous_checkpoint_never_repeats_after_non_satisfied_terminal() {
    use harness_contract::goal::GoalCompletion;

    assert!(autonomy_continuation_may_advance(GoalCompletion::Satisfied));
    for completion in [
        GoalCompletion::Open,
        GoalCompletion::Partial,
        GoalCompletion::WaitingExternalDecision,
        GoalCompletion::Cancelled,
    ] {
        assert!(!autonomy_continuation_may_advance(completion));
    }
}

fn process_request_366(
    services: &Arc<RuntimeServices>,
    bridge: &ProcessJsonlToolSession,
    packet: &AgentTaskPacket,
    call_id: &str,
) -> RuntimeToolExecutionRequest {
    let step = bridge.steps.lock().unwrap().back().unwrap().clone();
    let binding = crate::step_snapshot::StepCallBinding::process(step, packet.run_id(), 1, call_id);
    let input = r#"{"path":"src/a.rs","complete":true}"#;
    let value = serde_json::from_str(input).unwrap();
    let invocation = binding
        .prepare_invocation(bridge.executor.host.pin_step_catalog(), "read_file", &value)
        .unwrap();
    let policy = permission_policy(
        services.session_execution_policy_control(packet.session_id()),
        packet.permission_ceiling,
        &bridge.executor.allowed_tools,
    );
    let authorization_id = format!(
        "process-tool:{:x}",
        Sha256::digest(serde_json::to_vec(&(packet.graph_id(), packet.attempt, call_id)).unwrap())
    );
    let negotiator = crate::AuthorizationNegotiator::new();
    let evaluated = negotiator.assess_effective(
        &policy,
        &crate::AuthorizationRequest {
            principal_id: format!("agent:{}", packet.agent_id()),
            capability: "read_file".into(),
            input: input.into(),
            idempotency_key: authorization_id.clone(),
            effect: invocation.effect.clone(),
            parent_ceiling: packet.permission_ceiling,
            parent_lease_id: None,
            policy_revision: bridge.executor.policy_revision,
            recovery_scope: format!("execution:{}", packet.graph_id()),
            context: crate::PermissionContext::default(),
            safe_alternatives: vec![],
        },
    );
    let authorization = crate::ToolPolicy
        .authorize(
            &evaluated.effective,
            &evaluated.assessment,
            authorization_id,
            evaluated.assessment.lease.clone().unwrap(),
            60,
        )
        .unwrap()
        .authorization;
    let mut request =
        RuntimeToolExecutionRequest::from_tool_request(&crate::tool_dispatch::ToolRequest {
            tool_use_id: call_id.into(),
            tool_name: "read_file".into(),
            input: input.into(),
            depends_on: vec![],
        });
    request.idempotency_key = binding.effect_idempotency_key();
    request.compiled_invocation = Some(invocation);
    request.step_binding = Some(binding);
    request.governed_plan_id = packet.graph_id().into();
    request.governed_plan_revision = 1;
    request.observation_wave_sequence = 1;
    request.category = crate::ToolSafetyCategory::ReadOnly;
    request.authorization = Some(authorization);
    request.session_id = Some(packet.session_id().into());
    request.policy_revision = bridge.executor.policy_revision;
    request.sandbox_posture = bridge.executor.sandbox_posture;
    request.authorized_scopes = bridge.executor.authorized_scopes_for_tool();
    request.parent_execution = Some(harness_contract::execution_graph::ExecutionParentBinding {
        execution_id: packet.graph_id().into(),
        node_id: packet.node_id().into(),
    });
    request.parent_execution_attempt = Some(packet.attempt);
    serde_json::from_value(serde_json::to_value(request).unwrap()).unwrap()
}

fn process_pair_366(
    services: &RuntimeServices,
    request: &RuntimeToolExecutionRequest,
) -> Vec<crate::DurableRuntimeEvent> {
    let parent = request.parent_execution.as_ref().unwrap();
    let store = services.event_store();
    [
        (
            format!("execution-effect:{}", request.idempotency_key),
            format!("{}:read-receipt", request.idempotency_key),
        ),
        (
            format!(
                "execution-agent-receipts:{}:{}:{}",
                parent.execution_id,
                parent.node_id,
                request.parent_execution_attempt.unwrap()
            ),
            format!("agent-tool-receipt:{}", request.idempotency_key),
        ),
    ]
    .into_iter()
    .filter_map(|(stream, key)| store.event_by_idempotency_key(&stream, &key).unwrap())
    .collect()
}

#[tokio::test]
async fn agent_tool_366_executed_terminal_before_delivery_survives_host_reload() {
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_test_services(root.path(), true);
    let selection = AgentModelSelection {
        model: packet.model_lease.clone(),
        provider: "local-discovery".into(),
        registry_revision: 0,
    };
    let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
    bridge.begin_step().unwrap();
    let request = process_request_366(&services, &bridge, &packet, "terminal-before-delivery-366");
    let descriptor = request.compiled_invocation.as_ref().unwrap().effect.clone();
    let executed = crate::bound_tool_batch::execute_bound_agent_tool(
        host.as_ref(),
        Some(services.commit_service()),
        services.path_identity_resolver(),
        services.scope_locks(),
        &request,
        &descriptor,
    )
    .await
    .unwrap();
    assert_eq!(executed.status, RuntimeToolExecutionStatus::Executed);
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    let original = process_pair_366(&services, &request);
    assert_eq!(original.len(), 2);
    assert_eq!(original[0].transaction_id, original[1].transaction_id);
    assert!(!services
        .event_store()
        .all_events(100_000)
        .unwrap()
        .iter()
        .any(|event| event.stream_id.starts_with("agent-tool-batch:")));
    host.tools
        .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
    assert_eq!(
        request
            .step_binding
            .as_ref()
            .unwrap()
            .resolve(Some(host.tools.pin_snapshot()))
            .unwrap_err()
            .to_string(),
        "step_binding_stale"
    );
    let dispatcher =
        crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
            &services, &packet,
        );
    let delivered = dispatcher.execute(request.clone()).await.unwrap();
    assert_eq!(delivered.status, RuntimeToolExecutionStatus::Executed);
    assert_eq!(delivered.output, executed.output);
    assert_eq!(delivered.error, executed.error);
    assert_eq!(delivered.tool_use_id, executed.tool_use_id);
    assert!(delivered
        .observed_evidence
        .iter()
        .all(|evidence| evidence.provenance
            == harness_contract::context::ObservedEvidenceProvenance::RetainedReplay));
    assert_eq!(process_pair_366(&services, &request), original);
    let mut retry = request.clone();
    retry.governed_plan_revision += 1;
    retry.observation_wave_sequence += 1;
    assert_eq!(dispatcher.execute(retry).await.unwrap(), delivered);
    assert_eq!(process_pair_366(&services, &request), original);
    assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    assert!(bridge.executor.receipts.lock().unwrap().is_empty());
    assert_eq!(services.tool_execution_plane().stats().active, 0);
    assert!(services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
}

#[tokio::test]
async fn agent_tool_366_actual_stale_and_concurrent_replay_keep_original_failed_pair() {
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_test_services(root.path(), true);
    let selection = AgentModelSelection {
        model: packet.model_lease.clone(),
        provider: "local-discovery".into(),
        registry_revision: 0,
    };
    let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
    bridge.begin_step().unwrap();
    let request = process_request_366(&services, &bridge, &packet, "failed-pair-366");
    host.tools
        .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
    let dispatcher =
        crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
            &services, &packet,
        );
    let mut second = request.clone();
    second.governed_plan_revision += 1;
    second.observation_wave_sequence += 1;
    let (first, second) = tokio::join!(
        dispatcher.execute(request.clone()),
        dispatcher.execute(second)
    );
    let failed = first.unwrap();
    assert_eq!(second.unwrap(), failed);
    assert_eq!(failed.status, RuntimeToolExecutionStatus::Failed);
    assert_eq!(failed.error.as_deref(), Some("step_binding_stale"));
    assert!(failed.output.is_none() && failed.observed_evidence.is_empty());
    let original = process_pair_366(&services, &request);
    assert_eq!(original.len(), 2);
    assert_eq!(original[0].transaction_id, original[1].transaction_id);
    assert_eq!(original[0].commit_cursor, original[1].commit_cursor);
    assert!(original[1].transaction_index > original[0].transaction_index);
    assert_eq!(
        original[0].payload["outcome"],
        original[1].payload["outcome"]
    );
    for event in &original {
        assert!(event.payload["resolved_effect_scope"].is_null());
        assert!(event.payload[crate::context_evidence::READ_ACQUISITION_FIELD].is_null());
        assert!(event.payload["process_admission_ref"].is_null());
    }
    let mut third = request.clone();
    third.governed_plan_revision += 2;
    third.observation_wave_sequence += 2;
    assert_eq!(dispatcher.execute(third).await.unwrap(), failed);
    assert_eq!(process_pair_366(&services, &request), original);
    assert_eq!(
        services
            .commit_service()
            .load_delegated_agent_tool_receipts(packet.graph_id(), packet.node_id(), packet.attempt)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
    assert!(bridge.executor.receipts.lock().unwrap().is_empty());
    assert_eq!(services.tool_execution_plane().stats().active, 0);
    assert!(services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
}

#[tokio::test]
async fn agent_tool_366_stale_rejection_does_not_bypass_original_authority() {
    for mutation in [
        "call",
        "key",
        "actor",
        "turn",
        "run",
        "attempt",
        "catalog",
        "expired",
        "signature",
        "policy",
        "scope",
        "descriptor",
        "process",
        "network",
        "write",
        "provider",
        "generic_error",
    ] {
        let root = tempfile::tempdir().unwrap();
        let (services, host, packet) = discovery_test_services(root.path(), true);
        let selection = AgentModelSelection {
            model: packet.model_lease.clone(),
            provider: "local-discovery".into(),
            registry_revision: 0,
        };
        let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
        bridge.begin_step().unwrap();
        let mut request =
            process_request_366(&services, &bridge, &packet, "rejected-authority-366");
        if mutation != "generic_error" {
            host.tools
                .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
        }
        match mutation {
            "call" => request.step_binding.as_mut().unwrap().origin.call_id = "foreign-call".into(),
            "key" => request.idempotency_key = "foreign-key".into(),
            "actor" => {
                Arc::make_mut(&mut request.step_binding.as_mut().unwrap().snapshot)
                    .identity
                    .actor_ref = Some("foreign".into())
            }
            "turn" => {
                Arc::make_mut(&mut request.step_binding.as_mut().unwrap().snapshot)
                    .identity
                    .turn_id = "foreign".into()
            }
            "run" => {
                Arc::make_mut(&mut request.step_binding.as_mut().unwrap().snapshot)
                    .identity
                    .run_ref = Some("foreign".into())
            }
            "attempt" => {
                Arc::make_mut(&mut request.step_binding.as_mut().unwrap().snapshot)
                    .identity
                    .attempt = Some(packet.attempt + 1)
            }
            "catalog" | "generic_error" => {
                let binding = request.step_binding.as_mut().unwrap();
                binding.origin.catalog_digest = "foreign".into();
                Arc::make_mut(&mut binding.snapshot).catalog_digest = "foreign".into();
                request.compiled_invocation.as_mut().unwrap().origin = Some(binding.origin.clone());
            }
            "expired" => {
                Arc::make_mut(&mut request.step_binding.as_mut().unwrap().snapshot).expires_at_ms =
                    1
            }
            "signature" => {
                request
                    .authorization
                    .as_mut()
                    .unwrap()
                    .authorization_lease
                    .signature = "forged".into()
            }
            "policy" => {
                let control = services
                    .session_execution_policy_control(packet.session_id())
                    .unwrap();
                let mut policy = control.snapshot();
                policy.revision += 1;
                control.replace(policy).unwrap();
            }
            "scope" => request.authorized_scopes.push("read:src/sibling.rs".into()),
            "descriptor" => {
                request
                    .compiled_invocation
                    .as_mut()
                    .unwrap()
                    .effect
                    .descriptor_hash = "foreign".into()
            }
            "process" => {
                request
                    .compiled_invocation
                    .as_mut()
                    .unwrap()
                    .effect
                    .spawns_process = true
            }
            "network" => {
                request
                    .compiled_invocation
                    .as_mut()
                    .unwrap()
                    .effect
                    .uses_network = true
            }
            "write" => {
                request
                    .compiled_invocation
                    .as_mut()
                    .unwrap()
                    .effect
                    .effect_kind = harness_contract::tool::ToolEffectKind::Write
            }
            "provider" => {
                let binding = request.step_binding.as_mut().unwrap();
                binding.origin.source = harness_contract::step::CallSource::Provider {
                    attempt_id: binding.snapshot.provider_attempt.attempt_id.clone(),
                };
                request.compiled_invocation.as_mut().unwrap().origin = Some(binding.origin.clone());
            }
            _ => unreachable!(),
        }
        let dispatcher =
            crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
                &services, &packet,
            );
        let returned = dispatcher.execute(request.clone()).await;
        if mutation == "generic_error" {
            let outcome = returned.unwrap();
            let error = own834_refusal_error(
                &outcome,
                &request.tool_use_id,
                &request.tool_name,
            );
            assert!(
                error.contains("step_catalog_digest_mismatch"),
                "{error}"
            );
        } else {
            assert!(returned.is_err(), "{mutation}: {returned:?}");
        }
        assert!(
            process_pair_366(&services, &request).is_empty(),
            "{mutation}"
        );
        assert_eq!(host.calls.load(Ordering::SeqCst), 0, "{mutation}");
        assert!(
            bridge.executor.receipts.lock().unwrap().is_empty(),
            "{mutation}"
        );
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("src/a.rs")).unwrap(),
            "narrow-read-proof"
        );
        assert_eq!(
            std::fs::read_to_string(services.workspace_root().join("src/sibling.rs")).unwrap(),
            "untouched-sibling"
        );
        assert_eq!(services.tool_execution_plane().stats().active, 0);
        assert!(services
            .execution_supervisor()
            .shutdown()
            .await
            .errors
            .is_empty());
    }
}

#[tokio::test]
async fn agent_tool_366_receipt_transaction_failure_is_not_a_local_failed_success() {
    use crate::runtime_event_store::{
        AppendTransactionRequest, ExpectedStreamRevision, RuntimeTransactionEventInput,
    };
    let root = tempfile::tempdir().unwrap();
    let (services, host, packet) = discovery_test_services(root.path(), true);
    let selection = AgentModelSelection {
        model: packet.model_lease.clone(),
        provider: "local-discovery".into(),
        registry_revision: 0,
    };
    let bridge = ProcessJsonlToolSession::prepare(&services, &packet, &selection).unwrap();
    bridge.begin_step().unwrap();
    let request = process_request_366(&services, &bridge, &packet, "commit-conflict-366");
    let key = format!("{}:read-receipt", request.idempotency_key);
    let transaction_id = format!("readonly-tool-wave:{:x}", Sha256::digest(key.as_bytes()));
    services
        .event_store()
        .append_transaction(AppendTransactionRequest {
            transaction_id,
            expected_streams: vec![ExpectedStreamRevision {
                stream_id: "test:transaction-conflict-366".into(),
                expected_revision: 0,
            }],
            events: vec![RuntimeTransactionEventInput {
                event: crate::RuntimeEventInput {
                    stream_id: "test:transaction-conflict-366".into(),
                    scope: crate::RuntimeEventScope::Session,
                    kind: "test.injected_transaction_conflict".into(),
                    status: None,
                    actor: None,
                    refs: vec![],
                    payload: serde_json::json!({"fault":"reserved transaction identity"}),
                },
                idempotency_key: Some("fault-366".into()),
                schema_version: 1,
            }],
        })
        .unwrap();
    host.tools
        .replace_snapshot(host.tools.pin_snapshot().snapshot().clone());
    let dispatcher =
        crate::execution_core::graph::executors::agent_tool::AgentToolBatchDispatcher::new(
            &services, &packet,
        );
    let error = dispatcher.execute(request.clone()).await.unwrap_err();
    assert!(
        error
            .to_string()
            .contains("stale Step rejection receipt commit failed"),
        "{error}"
    );
    assert!(process_pair_366(&services, &request).is_empty());
    assert_eq!(host.calls.load(Ordering::SeqCst), 0);
    assert!(bridge.executor.receipts.lock().unwrap().is_empty());
    assert_eq!(services.tool_execution_plane().stats().active, 0);
    assert!(services
        .execution_supervisor()
        .shutdown()
        .await
        .errors
        .is_empty());
}


#[test]
fn process_human_grant_cannot_exceed_packet_or_service_mode() {
    let tools = BTreeSet::from(["execute_code".to_string()]);
    for (service_mode, packet_mode, allowed) in [
        (PermissionMode::WorkspaceWrite, PermissionMode::DangerFullAccess, false),
        (PermissionMode::DangerFullAccess, PermissionMode::WorkspaceWrite, false),
        (PermissionMode::ReadOnly, PermissionMode::DangerFullAccess, false),
        (PermissionMode::DangerFullAccess, PermissionMode::DangerFullAccess, true),
    ] {
        let control = crate::permissions::SessionExecutionPolicyControl::new(service_mode);
        let policy = permission_policy(Some(control), packet_mode, &tools);
        assert_eq!(process_tool_approval_ceiling_allows(&policy, packet_mode,
            PermissionMode::DangerFullAccess), allowed);
    }
}

/// A7 Patch A, C-a: a turn call whose input cannot be normalized
/// deterministically is an error, never a silent fallback to raw bytes. The
/// tool_plane turn entry routes that error into `refused_before_dispatch`, so
/// the call never enters `requests`/`pending`, is never dispatched and commits
/// no receipt - the fail-closed shape the repetition guard's refusal path
/// already uses. A well-formed input normalizes and stays representable.
#[tokio::test]
async fn a7_patch_a_normalization_unavailable_refuses_before_dispatch() {
    let root = tempfile::tempdir().unwrap();
    let executor = concurrency_test_executor(
        root.path(),
        Arc::new(ConcurrencyTrackingRuntimeExecutionHost::new()),
        Arc::new(ScopeLockManager::new()),
    );
    let error = executor
        .normalize_turn_tool_input("read_file", "{not json")
        .expect_err("an unnormalizable input must be an error, not a raw fallback");
    assert!(
        error.to_string().contains("invalid scoped tool input"),
        "the refusal must name the unreadable input: {error}"
    );
    let input = r#"{"path":"crates/runtime/src/lib.rs"}"#;
    let normalized = executor
        .normalize_turn_tool_input("read_file", input)
        .expect("a well-formed input normalizes");
    assert_eq!(normalized, input, "an in-workspace relative input is unchanged");
}
