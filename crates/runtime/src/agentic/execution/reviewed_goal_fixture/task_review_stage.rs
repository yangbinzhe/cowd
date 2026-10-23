{
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
            std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                .unwrap(),
            delegated_write_content
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
                resource_scopes: vec!["read:.".into()],
            },
        )
        .await
        .unwrap_or_else(|error| {
            own832_diagnostics::capture(
                &services,
                fixture_program_id.as_str(),
                &task_ref,
                "original_review_dispatch_failed",
                Some(&packet),
            );
            panic!("physical reviewer admission: {error}; see OWN832 snapshot");
        });
    let review_projection = action_service.project(fixture_program_id.as_str()).unwrap();
    let submitted_task = &review_projection.tasks[&task_ref];
    if submitted_task.status != crate::AgenticTaskStatus::Submitted
        || submitted_task
            .active_attempts
            .values()
            .filter(|attempt| {
                attempt.mode == harness_contract::agent_action::AgentAttemptMode::Review
            })
            .count()
            != 1
        || submitted_task.active_attempts.values().any(|attempt| {
            attempt.mode == harness_contract::agent_action::AgentAttemptMode::Review
                && (attempt.agent_id != reviewer
                    || attempt.generation != submitted_task.review_generation)
        })
        || review_dispatch.len() > 1
        || review_dispatch.iter().any(|receipt| {
            receipt.task_ref != task_ref
                || receipt.agent_ref != reviewer
                || receipt.mode != "review"
                || submitted_task.active_attempts.values().all(|attempt| {
                    attempt.mode != harness_contract::agent_action::AgentAttemptMode::Review
                        || attempt.execution_id != receipt.graph_id
                })
        })
    {
        own832_diagnostics::capture(
            &services,
            fixture_program_id.as_str(),
            &task_ref,
            "original_review_after_submit_dispatch",
            Some(&packet),
        );
    }
    assert_eq!(submitted_task.status, crate::AgenticTaskStatus::Submitted);
    let review_attempts = submitted_task
        .active_attempts
        .values()
        .filter(|attempt| {
            attempt.mode == harness_contract::agent_action::AgentAttemptMode::Review
        })
        .collect::<Vec<_>>();
    assert_eq!(
        review_attempts.len(),
        1,
        "{submitted_task:?}; dispatch={review_dispatch:?}"
    );
    let original_review = review_attempts[0];
    assert_eq!(original_review.agent_id, reviewer);
    assert_eq!(original_review.generation, submitted_task.review_generation);
    assert!(review_dispatch.len() <= 1, "{review_dispatch:?}");
    for receipt in &review_dispatch {
        assert_eq!(receipt.graph_id, original_review.execution_id);
        assert_eq!(receipt.task_ref, task_ref);
        assert_eq!(receipt.agent_ref, reviewer);
        assert_eq!(receipt.mode, "review");
    }
    let original_review_graph_id = original_review.execution_id.clone();
    let reviewer_session = wait_real_process(&services, &original_review_graph_id).await;
    let review_binding = reviewer_session.packet.agentic_binding.as_ref().unwrap();
    assert_eq!(review_binding.program_id, fixture_program_id.as_str());
    assert_eq!(review_binding.agent_id, reviewer);
    assert_eq!(review_binding.membership_id, original_review.membership_id);
    assert!(matches!(&review_binding.focus,
    harness_contract::agent::AgenticExecutionFocus::TaskReview { task_ref: reviewed }
        if reviewed == &task_ref));
    let review_graph = services
        .graph_state_store()
        .load(&original_review_graph_id)
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
    let mut review_envelope = AgentActionEnvelope {
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
        let unsettled = action_service.project(fixture_program_id.as_str()).unwrap();
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
    let unread_review_error = services
        .submit_agent_action(&review_envelope)
        .await
        .unwrap_err();
    println!("V3_REQUIRED_UNREAD_REVIEW_DENIAL {unread_review_error}");
    let expected_unread_review_error = if delegated_effect {
        "review_requires_effect_observation"
    } else {
        "review_requires_current_physical_reads"
    };
    assert!(
        unread_review_error.contains(expected_unread_review_error),
        "{unread_review_error}"
    );
    let review_packet = serde_json::from_str(&review_graph.nodes[0].payload_ref).unwrap();
    let bridge = RealProcessSession::prepare(
        &services,
        &review_packet,
        &crate::agent_model_selector::AgentModelSelection {
            model: "test".into(),
            provider: "test".into(),
            registry_revision: 0,
        },
    )
    .unwrap();
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
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        if proof_scenario == R1ProofScenario::ReadDelivery(RealProcessProofCase::Effects) {
            let missing_second_target_error = services
                .submit_agent_action(&review_envelope)
                .await
                .unwrap_err();
            assert!(
                missing_second_target_error.contains("review_requires_effect_observation"),
                "{missing_second_target_error}"
            );
            bridge
                .execute_tool(
                    "actual-delegated-second-target",
                    "read_file",
                    r#"{"path":"r1-optional-target.txt","complete":true}"#,
                )
                .await
                .unwrap();
        }
        if !bounded_runs.is_empty() {
            for path in ["bounded_source.py", "bounded_test.py"] {
                bridge
                    .execute_tool(
                        &format!("bounded-file-read:{path}"),
                        "read_file",
                        &serde_json::json!({"path":path,"complete":true}).to_string(),
                    )
                    .await
                    .unwrap();
            }
        }
        let unread_body_error = services
            .submit_agent_action(&review_envelope)
            .await
            .unwrap_err();
        assert!(
            unread_body_error.contains("review_requires_current_physical_reads"),
            "{unread_body_error}"
        );
    }
    let assert_fresh_action_239 = |before: usize,
                                   envelope: &AgentActionEnvelope,
                                   outcome: &Result<
        harness_contract::agent_action::AgentActionObservation,
        String,
    >| {
        use sha2::{Digest, Sha256};
        let calls = read_host.calls.lock().unwrap();
        let delta = &calls[before..];
        assert_eq!(
            delta.len(),
            1,
            "fresh Process action must enter the actual Host exactly once"
        );
        let request = &delta[0];
        assert_eq!(request.tool_name, envelope.action.kind());
        assert_eq!(
            request.session_id.as_deref(),
            Some(review_packet.session_id())
        );
        let parent = request.parent_execution.as_ref().unwrap();
        assert_eq!(parent.execution_id, review_packet.graph_id());
        assert_eq!(parent.node_id, review_packet.node_id());
        assert_eq!(
            request.parent_execution_attempt,
            Some(review_packet.attempt)
        );
        assert_eq!(
            request.authorization.as_ref().unwrap().tool_id,
            envelope.action.kind()
        );
        let binding = request.step_binding.as_ref().unwrap();
        assert_eq!(binding.origin.call_id, request.tool_use_id);
        assert_eq!(
            binding.origin.origin_step_id,
            binding.snapshot.identity.step_id
        );
        assert_eq!(
            binding.snapshot.identity.session_id,
            review_packet.session_id()
        );
        assert_eq!(
            Some(binding.snapshot.identity.turn_id.as_str()),
            review_packet.assignment.execution_identity.turn_id()
        );
        assert_eq!(
            binding.snapshot.identity.execution_id.as_deref(),
            Some(review_packet.graph_id())
        );
        assert_eq!(
            binding.snapshot.identity.run_ref.as_deref(),
            Some(review_packet.run_id())
        );
        assert_eq!(
            binding.snapshot.identity.actor_ref.as_deref(),
            Some(review_packet.agent_id())
        );
        assert_eq!(
            binding.snapshot.identity.attempt,
            Some(review_packet.attempt)
        );
        assert!(
            matches!(&binding.origin.source, harness_contract::step::CallSource::Process { worker_id, .. }
        if worker_id == review_packet.run_id())
        );
        let metadata = bridge.action_metadata_path_239(&request.tool_use_id);
        let value: serde_json::Value =
            serde_json::from_slice(&std::fs::read(metadata).unwrap()).unwrap();
        assert_eq!(value["envelope"], serde_json::to_value(envelope).unwrap());
        let scope = serde_json::json!([
            review_packet.session_id(),
            review_packet.graph_id(),
            review_packet.node_id(),
            review_packet.run_id(),
            review_packet.agent_id(),
            review_packet.attempt,
            review_packet
                .binding
                .as_ref()
                .map(|binding| binding.binding_digest.as_str())
        ]);
        let stream = format!(
            "agent-process-transport:{:x}",
            Sha256::digest(scope.to_string().as_bytes())
        );
        let key = format!("{:x}", Sha256::digest(request.tool_use_id.as_bytes()));
        let completed = services
            .event_store()
            .event_by_idempotency_key(&stream, &format!("completed:{key}"))
            .unwrap()
            .unwrap();
        let started = services
            .event_store()
            .event_by_idempotency_key(&stream, &format!("started:{key}"))
            .unwrap()
            .unwrap();
        let input: serde_json::Value = serde_json::from_str(&request.input).unwrap();
        assert_eq!(
            input,
            serde_json::to_value(&envelope.action).unwrap()["input"]
        );
        let fingerprint = harness_contract::step::canonical_digest(
            "process-tool-call",
            &serde_json::json!(["tool_result", request.tool_name, input]),
        )
        .unwrap();
        for (event, stage) in [(&started, "started"), (&completed, "completed")] {
            assert_eq!(event.kind, format!("agent.process_request_{stage}"));
            assert_eq!(event.scope, crate::RuntimeEventScope::Agent);
            assert_eq!(event.schema_version, 1);
            assert_eq!(event.status.as_deref(), Some(stage));
            assert_eq!(event.actor.as_deref(), Some(review_packet.agent_id()));
            assert_eq!(event.payload["fingerprint"], fingerprint);
            assert_eq!(event.payload["request_id"], request.tool_use_id);
            assert_eq!(event.payload["authority"], "transport_replay_only");
        }
        assert!(started.commit_cursor < completed.commit_cursor);
        let response = &completed.payload["response"];
        assert_eq!(response["kind"], "tool_result");
        assert_eq!(response["origin_step_id"], binding.origin.origin_step_id);
        assert_eq!(response["request_id"], request.tool_use_id);
        assert_eq!(response["run_id"], review_packet.run_id());
        assert_eq!(response["agent_id"], review_packet.agent_id());
        match outcome {
            Ok(observation) => {
                assert_eq!(
                    serde_json::from_str::<serde_json::Value>(
                        response["output"].as_str().unwrap()
                    )
                    .unwrap(),
                    serde_json::to_value(observation).unwrap()
                );
                assert!(response.get("error").is_none());
            }
            Err(error) => assert_eq!(response["error"].as_str(), Some(error.as_str())),
        }
        request.tool_use_id.clone()
    };
    let action_side_effects_239 = || {
        let calls = read_host
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.tool_name != "task_review")
            .map(|request| serde_json::to_value(request).unwrap())
            .collect::<Vec<_>>();
        let receipts = [&packet, &review_packet]
            .into_iter()
            .map(|packet| {
                services
                    .event_store()
                    .list_stream(&format!(
                        "execution-agent-receipts:{}:{}:{}",
                        packet.graph_id(),
                        packet.node_id(),
                        packet.attempt
                    ))
                    .unwrap()
                    .into_iter()
                    .map(|event| serde_json::to_value(event).unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        let targets = ["review-effect.txt", "out/a"]
            .map(|path| std::fs::read(services.workspace_root().join(path)).ok());
        (calls, receipts, targets)
    };
    let replay_probe_230 = if !delegated_effect {
        let request_id = format!("unread-review-230:{}", uuid::Uuid::new_v4());
        let before_239 = read_host.calls.lock().unwrap().len();
        let error = bridge
            .action_with_request_id(&review_envelope, &request_id)
            .await
            .unwrap_err();
        assert_eq!(
            assert_fresh_action_239(before_239, &review_envelope, &Err(error.clone())),
            request_id
        );
        assert!(
            error.contains("review_requires_current_physical_reads"),
            "{error}"
        );
        assert_eq!(
            read_host
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.tool_use_id == request_id)
                .count(),
            1
        );
        let metadata = bridge
            .prepare_action_metadata_239(&review_envelope, &request_id)
            .unwrap();
        let original = std::fs::read(&metadata).unwrap();
        let before = read_host.calls.lock().unwrap().len();
        let revision = action_service
            .project(fixture_program_id.as_str())
            .unwrap()
            .revision;
        for mutation in 0..4 {
            let mut conflict = review_envelope.clone();
            match mutation {
                0 => conflict.action_id.push_str(":different-business"),
                1 => conflict.expected_revision = Some(revision),
                2 => conflict.expected_revision = Some(revision + 1),
                _ => match &mut conflict.action {
                    AgentAction::TaskReview(input) => {
                        input.reason.push_str(":different-payload")
                    }
                    _ => unreachable!(),
                },
            }
            assert!(bridge
                .action_with_request_id(&conflict, &request_id)
                .await
                .unwrap_err()
                .contains("invocation metadata conflict"));
            assert_eq!(std::fs::read(&metadata).unwrap(), original);
            assert_eq!(read_host.calls.lock().unwrap().len(), before);
            assert_eq!(
                action_service
                    .project(fixture_program_id.as_str())
                    .unwrap()
                    .revision,
                revision
            );
        }
        let reserved_id = format!("reserved-before-send-239:{}", uuid::Uuid::new_v4());
        let mut reserved = review_envelope.clone();
        reserved.expected_revision = Some(revision);
        let path = bridge
            .prepare_action_metadata_239(&reserved, &reserved_id)
            .unwrap();
        let bytes = std::fs::read(&path).unwrap();
        for expected_revision in [None, Some(revision + 1)] {
            let mut contender = reserved.clone();
            contender.expected_revision = expected_revision;
            assert!(bridge
                .action_with_request_id(&contender, &reserved_id)
                .await
                .unwrap_err()
                .contains("invocation metadata conflict"));
            assert_eq!(std::fs::read(&path).unwrap(), bytes);
            assert_eq!(read_host.calls.lock().unwrap().len(), before);
            assert_eq!(
                action_service
                    .project(fixture_program_id.as_str())
                    .unwrap()
                    .revision,
                revision
            );
        }
        let reserved_before = read_host.calls.lock().unwrap().len();
        let reserved_error = bridge
            .action_with_request_id(&reserved, &reserved_id)
            .await
            .unwrap_err();
        assert!(reserved_error.contains("review_requires_current_physical_reads"));
        assert_eq!(
            assert_fresh_action_239(reserved_before, &reserved, &Err(reserved_error)),
            reserved_id
        );
        Some((request_id, error))
    } else {
        None
    };
    let original_evidence_output = bridge
        .execute_tool(
            "read-result",
            "evidence_retrieve",
            &serde_json::json!({"evidence_ref":content.selector}).to_string(),
        )
        .await
        .unwrap();
    let evidence_input = serde_json::json!({"evidence_ref":content.selector});
    let descriptor = read_host
        .catalog
        .get()
        .unwrap()
        .pin_snapshot()
        .describe_effect("evidence_retrieve", &evidence_input);
    assert_eq!(
        descriptor.effect_kind,
        harness_contract::tool::ToolEffectKind::Read
    );
    let original_request = read_host
        .calls
        .lock()
        .unwrap()
        .iter()
        .find(|request| {
            request.tool_name == "evidence_retrieve"
                && request.tool_use_id == "read-result"
                && request.parent_execution.as_ref().is_some_and(|parent| {
                    parent.execution_id == review_packet.graph_id()
                        && parent.node_id == review_packet.node_id()
                })
        })
        .cloned()
        .expect("actual reviewer ToolHost invocation must be recorded");
    let original_acquisition = crate::context_evidence::read_acquisition_seal(
        &original_request,
        &original_evidence_output,
    )
    .expect("original complete content must seal before storage bounding");
    let original_body: serde_json::Value =
        serde_json::from_str(&original_evidence_output).unwrap();
    let original_chunks = original_body["chunks"].as_array().unwrap();
    assert_eq!(
        original_body["total_chunks"].as_u64().unwrap(),
        original_chunks.len() as u64
    );
    let original_content = original_chunks
        .iter()
        .map(|chunk| chunk["content"].as_str().unwrap())
        .collect::<String>();
    assert_eq!(
        original_acquisition.body.source_bytes,
        original_content.len() as u64
    );
    assert_eq!(
        original_acquisition.body.source_sha256,
        crate::context_evidence::read_sha256(original_content.as_bytes())
    );
    let digest_only =
        crate::agentic::review_evidence::compact_read_receipt(&original_evidence_output);
    assert!(
        crate::context_evidence::read_body_seal("evidence_retrieve", &digest_only).is_none()
    );
    assert!(
        crate::context_evidence::read_acquisition_seal(&original_request, &digest_only)
            .is_none(),
        "digest-only storage metadata cannot become an independent physical read"
    );
    let material = assert_process_delivery(&services, &review_packet, "read-result");
    assert_eq!(
        serde_json::to_value(&material.acquisition).unwrap(),
        serde_json::to_value(&original_acquisition).unwrap()
    );
    assert_eq!(
        material.presentation_sha256,
        crate::context_evidence::read_sha256(original_evidence_output.as_bytes())
    );
    assert_eq!(
        material.presentation_bytes,
        original_evidence_output.len() as u64
    );
    assert_eq!(material.acquisition.tool_name, "evidence_retrieve");
    assert_eq!(material.acquisition.body.source_ref, content.selector);
    assert!(!material.body_omitted);
    if !bounded_runs.is_empty() {
        let current = bounded_runs.last().unwrap();
        let mut selected = review_envelope.clone();
        selected.action_id = "bounded-unread-body-283".into();
        if let AgentAction::TaskReview(input) = &mut selected.action {
            input.evidence_refs = review_evidence_refs.clone();
        }
        let missing = services.submit_agent_action(&selected).await.unwrap_err();
        assert!(
            missing.contains("review_requires_complete_process_body"),
            "{missing}"
        );
        for (index, run) in bounded_runs.iter().enumerate() {
            read_bounded_body_pages_283(
                &services,
                &bridge,
                &format!("bounded-independent-body:{index}"),
                &run.body,
                (index + 1 == bounded_runs.len()).then_some(&selected),
            )
            .await;
        }
        let mut old = selected.clone();
        old.action_id = "bounded-old-version-selection-283".into();
        if let AgentAction::TaskReview(input) = &mut old.action {
            input.evidence_refs = vec![
                content.selector.clone(),
                bounded_runs[0].body.selector.clone(),
            ];
        }
        let denied = services.submit_agent_action(&old).await.unwrap_err();
        assert!(
            denied.contains("process")
                && (denied.contains("input") || denied.contains("current")),
            "{denied}"
        );
        if let AgentAction::TaskReview(input) = &mut review_envelope.action {
            input.evidence_refs = review_evidence_refs.clone();
        }
        assert_eq!(current.admission.inputs.len(), 2);
    }
    let mut newer_generation = action_service.project(fixture_program_id.as_str()).unwrap();
    if let Some((request_id, original_error)) = &replay_probe_230 {
        let original_host = read_host
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.tool_use_id == *request_id)
            .map(|request| serde_json::to_value(request).unwrap())
            .collect::<Vec<_>>();
        let before = read_host.calls.lock().unwrap().len();
        let replayed_error = bridge
            .action_with_request_id(&review_envelope, request_id)
            .await
            .unwrap_err();
        assert_eq!(&replayed_error, original_error);
        assert_eq!(read_host.calls.lock().unwrap().len(), before);
        assert_eq!(
            read_host
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|request| request.tool_use_id == *request_id)
                .map(|request| serde_json::to_value(request).unwrap())
                .collect::<Vec<_>>(),
            original_host
        );
        assert_eq!(
        read_host
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|request| request.tool_use_id == *request_id)
            .count(),
        1,
        "same transport identity must replay its refusal without re-entering the action host"
    );
    }
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
    let side_effects_before_239 = replay_probe_230.as_ref().map(|_| action_side_effects_239());
    if replay_probe_230.is_some() {
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        let mut stale = review_envelope.clone();
        stale.expected_revision = Some(current.revision + 1);
        let request_id = format!("fresh-cas-conflict-239:{}", uuid::Uuid::new_v4());
        let before = read_host.calls.lock().unwrap().len();
        let denied = bridge
            .action_with_request_id(&stale, &request_id)
            .await
            .unwrap();
        assert_eq!(
            denied.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(denied.error.as_ref().unwrap().code, "stale_revision");
        assert_eq!(
            assert_fresh_action_239(before, &stale, &Ok(denied)),
            request_id
        );
        assert_eq!(
            action_service
                .project(fixture_program_id.as_str())
                .unwrap()
                .revision,
            current.revision
        );
        assert!(services
            .event_store()
            .event_by_idempotency_key(
                &format!("agentic-program:{}", fixture_program_id),
                &format!("agent-action:{}", review_envelope.action_id)
            )
            .unwrap()
            .is_none());
        assert!(bridge
            .action_with_request_id(&review_envelope, &request_id)
            .await
            .unwrap_err()
            .contains("invocation metadata conflict"));
        assert_eq!(read_host.calls.lock().unwrap().len(), before + 1);
        assert_eq!(Some(action_side_effects_239()), side_effects_before_239);
    }
    let fresh_before_239 = read_host.calls.lock().unwrap().len();
    let reviewed = services
        .submit_agent_action(&review_envelope)
        .await
        .unwrap();
    if replay_probe_230.is_some() {
        let fresh_id =
            assert_fresh_action_239(fresh_before_239, &review_envelope, &Ok(reviewed.clone()));
        assert_ne!(fresh_id, replay_probe_230.as_ref().unwrap().0);
        assert_eq!(Some(action_side_effects_239()), side_effects_before_239);
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        let revision = current.revision;
        let task_before = serde_json::to_value(&current.tasks[&task_ref]).unwrap();
        let duplicate_before = read_host.calls.lock().unwrap().len();
        let retried = services
            .submit_agent_action(&review_envelope)
            .await
            .unwrap();
        let duplicate_id =
            assert_fresh_action_239(duplicate_before, &review_envelope, &Ok(retried.clone()));
        assert_ne!(duplicate_id, fresh_id);
        assert_ne!(duplicate_id, replay_probe_230.as_ref().unwrap().0);
        assert_eq!(retried.action_id, review_envelope.action_id);
        assert!(retried.duplicate);
        let program = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(
            serde_json::to_value(&program.tasks[&task_ref]).unwrap(),
            task_before
        );
        let applied = services
            .event_store()
            .list_stream(&format!("agentic-program:{}", fixture_program_id))
            .unwrap()
            .into_iter()
            .filter(|event| {
                event.kind == "agentic.action_applied"
                    && event.payload["envelope"]["action_id"] == review_envelope.action_id
            })
            .collect::<Vec<_>>();
        assert_eq!(applied.len(), 1);
        assert_eq!(
            applied[0].payload["envelope"],
            serde_json::to_value(&review_envelope).unwrap()
        );
        assert_eq!(Some(action_side_effects_239()), side_effects_before_239);
        assert_eq!(
            action_service
                .project(fixture_program_id.as_str())
                .unwrap()
                .revision,
            revision,
            "a fresh transport request must retain business action idempotency after apply"
        );
    }
    assert_eq!(
        reviewed.status,
        harness_contract::agent_action::AgentActionStatus::Applied,
        "{reviewed:?}"
    );
    Some((reviewer_session, review_graph, bridge))
}
