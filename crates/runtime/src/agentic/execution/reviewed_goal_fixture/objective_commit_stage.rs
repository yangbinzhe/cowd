{
    let obligation_result = if proof_scenario == R1ProofScenario::MaterialEnumerationRace {
        let reviewer = scoped_goal_reviewer.as_ref().unwrap();
        let note = services
            .artifact_store()
            .write_bytes(
                harness_contract::context::ArtifactWriteDescriptor {
                    media_type: "text/plain".into(),
                    visibility_scope: "session:session-dispatch".into(),
                    expected_bytes: None,
                    original_name: Some("r1-concurrent-material.txt".into()),
                },
                b"Material M, read after the actual enumeration cut.",
            )
            .await
            .unwrap();
        let committed = services
            .submit_agent_action(&bind_root(root(
                "r1-raced-material-publication",
                AgentAction::ArtifactCommit(ArtifactCommitInput {
                    content_ref: note.selector.clone(),
                    kind: "note".into(),
                    title: "Concurrent actual review material".into(),
                    relates_to: Vec::new(),
                }),
            )))
            .await
            .unwrap();
        assert_eq!(
            committed.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let source = format!(
            "execution-agent-receipts:{}:{}:{}",
            reviewer.packet.graph_id(),
            reviewer.packet.node_id(),
            reviewer.packet.attempt
        );
        let before_revision = services.event_store().stream_revision(&source).unwrap();
        let before_reviews = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap()
            .reviews
            .len();
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let mut actual_events = Vec::new();
        let inject = async {
            tokio::time::timeout(std::time::Duration::from_secs(10), entered.notified())
                .await
                .expect("actual producer material-cut checkpoint");
            let output = reviewer
                .reader
                .execute_tool(
                    "r1-concurrent-material-read",
                    "evidence_retrieve",
                    &serde_json::json!({"evidence_ref":note.selector}).to_string(),
                )
                .await
                .unwrap();
            assert!(output.contains("Material M"), "{output}");
            actual_events = services
                .event_store()
                .list_stream(&source)
                .unwrap()
                .into_iter()
                .filter(|event| {
                    event.sequence > before_revision
                        && event.kind == "execution.agent_tool.receipt"
                        && event.payload["effect_kind"] == "read"
                        && event.payload["outcome"]["status"] == "executed"
                })
                .map(|event| event.event_id)
                .collect();
            assert!(
                !actual_events.is_empty(),
                "M must have an actual read receipt after the cut"
            );
            release.notify_one();
        };
        #[cfg(test)]
        let guarded = crate::agentic::review_evidence::with_proof_material_cut_barrier(
            source.clone(),
            entered.clone(),
            release.clone(),
            services.0.submit_agent_action(&obligation_envelope),
        );
        #[cfg(not(test))]
        let guarded = async {
            Err::<harness_contract::agent_action::AgentActionObservation, String>(
                "material-cut fault control is available only in unit tests".into(),
            )
        };
        let (rejected, ()) = tokio::join!(guarded, inject);
        let rejected = rejected.unwrap_err();
        assert!(
            rejected.contains("actual read material enumeration cut changed"),
            "{rejected}"
        );
        let after = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(after.reviews.len(), before_reviews);
        assert!(after.terminal.is_none());
        raced_material = Some((
            source,
            actual_events,
            note.selector,
            committed.changed_refs[0].clone(),
        ));
        services.submit_agent_action(&obligation_envelope).await
    } else {
        services.submit_agent_action(&obligation_envelope).await
    };
    if proof_scenario.optional_unknown()
        && !(proof_scenario.invalid_child()
            && obligation_result.as_ref().is_ok_and(|result| {
                result.status == harness_contract::agent_action::AgentActionStatus::Applied
            }))
        && !matches!(
            proof_scenario,
            R1ProofScenario::OptionalSharedCheckpoint
                | R1ProofScenario::OptionalNotRetiring
                | R1ProofScenario::OptionalSourceRace
                | R1ProofScenario::OptionalForgedB
                | R1ProofScenario::OptionalForeignB
                | R1ProofScenario::PhysicalAfterA
        )
    {
        let reason = match obligation_result {
            Err(error) => error,
            Ok(observation) => {
                assert_eq!(
                    observation.status,
                    harness_contract::agent_action::AgentActionStatus::Rejected
                );
                observation
                    .error
                    .expect("explicit unknown rejection")
                    .message
            }
        };
        assert!(
            reason.contains("effect_review_pending")
                || reason.contains("unknown")
                || reason.contains("unsettled")
                || (proof_scenario.invalid_child()
                    && (reason.contains("child")
                        || reason.contains("ToolBatch")
                        || reason.contains("attempt")
                        || reason.contains("parent"))),
            "{reason}"
        );
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(
            goal.completion,
            harness_contract::goal::GoalCompletion::Open
        );
        assert!(goal.terminal.is_none());
        assert!(action_service
            .project(fixture_program_id.as_str())
            .unwrap()
            .objective_verdict
            .is_none());
        let mut optional = optional_effect.unwrap();
        optional.assert_pending(&services);
        if proof_scenario == R1ProofScenario::PhysicalOpaque {
            assert!(
                reason.contains("opaque Process history"),
                "must reject the target physical dimension: {reason}"
            );
            optional.hold.release.notify_one();
            (&mut optional.worker).await.unwrap().unwrap();
            optional.process.finish().await;
            wait_admitted_graph_terminal(&services, &optional.packet).await;
            let lifecycle = crate::agent_runtime::authenticated_run_lifecycle(
                services.event_store(),
                &optional.packet,
            )
            .unwrap();
            assert!(!lifecycle.no_direct_effects());
            assert!(
                confirmed_run_exit_with_retry(services.event_store(), &optional.packet)
                    .is_some()
            );
            assert!(!process_control_directory(&services, &optional.packet).exists());
            let mut retried = obligation_envelope.clone();
            retried.action_id.push_str(":after-real-reap");
            let refused = services.submit_agent_action(&retried).await;
            let message = match refused {
                Err(error) => error,
                Ok(result) => {
                    assert_eq!(
                        result.status,
                        harness_contract::agent_action::AgentActionStatus::Rejected
                    );
                    result.error.unwrap().message
                }
            };
            assert!(message.contains("opaque Process history"), "{message}");
            let program = services
                .agent_action_service()
                .project(fixture_program_id.as_str())
                .unwrap();
            assert!(!program.tasks[&optional.task_ref].active_attempts.is_empty());
        } else if proof_scenario.invalid_child() {
            optional
                .finish_rejected_child(&services, proof_scenario)
                .await;
        } else {
            optional.finish(&services).await;
        }
        scoped_goal_reviewer.take().unwrap().finish(&services).await;
        services
            .cancel_execution_tree(&review_graph.id, "R1 negative fixture complete")
            .await
            .unwrap();
        services
            .cancel_execution_tree(packet.graph_id(), "R1 negative fixture complete")
            .await
            .unwrap();
        assert_eq!(
            services
                .goal_store()
                .get(&goal.id)
                .unwrap()
                .unwrap()
                .completion,
            harness_contract::goal::GoalCompletion::Open
        );
        println!(
            "R1_OPTIONAL_UNKNOWN_REJECTED {}",
            serde_json::json!({"reason":reason,"goal":goal.id})
        );
        return None;
    }
    let obligation_review = obligation_result.unwrap();
    #[cfg(test)]
    if proof_scenario == R1ProofScenario::PhysicalLineage {
        assert_physical_lineage_history_cut(&services, &_physical_history);
    }
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
    if !bounded_runs.is_empty() {
        let proof = reviewed_goal
            .reviews
            .iter()
            .find(|review| review.criterion_ref == "source-backed-delivery")
            .unwrap()
            .verification
            .as_ref()
            .unwrap();
        let inputs = proof.input_set.as_ref().unwrap();
        for run in &bounded_runs {
            let stream = format!("execution-effect:{}", run.admission.binding.call_key);
            assert!(
                inputs.source_revisions.contains_key(&stream),
                "every historical run remains a source cut"
            );
            for suffix in ["process-admission", "receipt"] {
                let original = services
                    .event_store()
                    .event_by_idempotency_key(
                        &stream,
                        &format!("{}:{suffix}", run.admission.binding.call_key),
                    )
                    .unwrap()
                    .unwrap();
                assert!(
                    inputs
                        .material_events
                        .iter()
                        .any(|event| event.stream_id == stream
                            && event.event_id == original.event_id),
                    "original process admission and terminal must remain material: {suffix}"
                );
            }
        }
        let program = action_service.project(fixture_program_id.as_str()).unwrap();
        let verifier = crate::agentic::review_evidence::ProcessInputVerifier::new(Arc::clone(
            services.scope_locks(),
        ));
        let process_frame = verifier.frame();
        assert!(crate::agentic::review_evidence::validate_proof_inputs(services.event_store(), &program,
        Some(&reviewed_goal), proof, crate::agentic::review_evidence::ResultContentReaders {
            process_inputs:None, artifacts:Some(services.artifact_store().as_ref()),
            history:services.session_history_reader().as_deref(),
        }).is_err(), "an otherwise authentic selected Process proof cannot silently fall back to file-only context");
        crate::agentic::review_evidence::validate_proof_inputs(
            services.event_store(),
            &program,
            Some(&reviewed_goal),
            proof,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: Some(&process_frame),
                artifacts: Some(services.artifact_store().as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        )
        .expect("original full process proof must validate before any single-field omission");
        for omitted in ["run", "source-cut", "body-read"] {
            let mut broken = proof.clone();
            let old_stream = format!(
                "execution-effect:{}",
                bounded_runs[1].admission.binding.call_key
            );
            match omitted {
                "run" => broken
                    .input_set
                    .as_mut()
                    .unwrap()
                    .material_events
                    .retain(|event| event.stream_id != old_stream),
                "source-cut" => {
                    broken
                        .input_set
                        .as_mut()
                        .unwrap()
                        .source_revisions
                        .remove(&old_stream);
                }
                "body-read" => {
                    assert!(bounded_selected_body_receipts.len() > 1);
                    let selected = bounded_selected_body_receipts.first().unwrap();
                    assert!(broken
                        .reads
                        .iter()
                        .any(|read| read.effect_observation_refs.contains(selected)));
                    for read in &mut broken.reads {
                        read.effect_observation_refs
                            .retain(|reference| reference != selected);
                    }
                }
                _ => unreachable!(),
            }
            assert!(
                crate::agentic::review_evidence::validate_proof_inputs(
                    services.event_store(),
                    &program,
                    Some(&reviewed_goal),
                    &broken,
                    crate::agentic::review_evidence::ResultContentReaders {
                        process_inputs: Some(&process_frame),
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: services.session_history_reader().as_deref(),
                    }
                )
                .is_err(),
                "omitting {omitted} must not preserve the original review proof"
            );
        }
    }
    if let Some((source, actual_events, selector, artifact_ref)) = raced_material {
        let inputs = reviewed_goal
            .reviews
            .iter()
            .find(|review| review.criterion_ref == "source-backed-delivery")
            .unwrap()
            .verification
            .as_ref()
            .unwrap()
            .input_set
            .as_ref()
            .unwrap();
        for event_id in &actual_events {
            assert!(inputs
                .material_events
                .iter()
                .any(|event| event.stream_id == source && &event.event_id == event_id));
        }
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(
            crate::agentic::review_evidence::review_result_identity(&current, &selector)
                .unwrap(),
            artifact_ref
        );
        assert!(inputs.artifact_refs.contains(&artifact_ref));
        assert!(
            inputs.auxiliary_refs.contains(&selector),
            "{:?}",
            inputs.auxiliary_refs
        );
        println!(
            "R1_MATERIAL_CUT_WITNESS {}",
            serde_json::json!({
                "source":source,"actual_read_events":actual_events,"material":artifact_ref,
                "rejected_without_review_append":true,"fresh_retry_includes_material":true,
            })
        );
    }
    assert_eq!(
        reviewed_goal.obligations[0].verifier_decision.as_ref(),
        obligation_review.changed_refs.first()
    );
    assert_eq!(
        reviewed_goal.spec_revision,
        if proof_scenario.mechanical_extra() {
            if with_effect {
                3
            } else {
                2
            }
        } else if with_effect {
            2
        } else {
            1
        },
        "review changes progress, not requirements"
    );
    let semantic_review = services
        .submit_agent_action(&bind_review(
            root(
                "original-objective-review",
                AgentAction::ObjectiveReview(
                    harness_contract::agent_action::ObjectiveReviewInput {
                        criterion_ref: "reviewed-delivery".into(),
                        decision:
                            harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                        result_refs: reviewed_delivery_refs.clone(),
                        evidence_refs: review_evidence_refs.clone(),
                        reason_ref: content.selector.clone(),
                        reason: String::new(),
                    },
                ),
            ),
            scoped_goal_reviewer.as_ref(),
        ))
        .await
        .unwrap();
    assert_eq!(
        semantic_review.status,
        harness_contract::agent_action::AgentActionStatus::Applied
    );
    #[cfg(test)]
    if proof_scenario == R1ProofScenario::MechanicalExtra {
        let extra = bind_review(
            root(
                "real-additional-accepted-review",
                AgentAction::ObjectiveReview(
                    harness_contract::agent_action::ObjectiveReviewInput {
                        criterion_ref: "additional-accepted-condition".into(),
                        decision:
                            harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                        result_refs: reviewed_delivery_refs.clone(),
                        evidence_refs: vec![content.selector.clone()],
                        reason_ref: content.selector.clone(),
                        reason: "actual independent additional criterion".into(),
                    },
                ),
            ),
            scoped_goal_reviewer.as_ref(),
        );
        let applied = services.submit_agent_action(&extra).await.unwrap();
        assert_eq!(
            applied.status,
            harness_contract::agent_action::AgentActionStatus::Applied,
            "{applied:?}"
        );
        let current = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        services
            .goal_store()
            .revise(
                &current.id,
                current.revision,
                current.user_sequence + 1,
                "unrelated optional historical review",
                |goal| {
                    let mut historical = goal.reviews.last().unwrap().clone();
                    historical.criterion_ref = "unrelated-optional".into();
                    historical.review_id = "unrelated-optional-historical".into();
                    historical.verification = None;
                    goal.reviews.push(historical);
                    vec!["reviews".into()]
                },
            )
            .unwrap();
    }
    if let Some(reviewer) = scoped_goal_reviewer.take() {
        reviewer.finish(&services).await;
    }
    if matches!(
        proof_scenario,
        R1ProofScenario::UnrelatedNote
            | R1ProofScenario::ExecutionEpoch
            | R1ProofScenario::AcceptancePolicy
    ) {
        let reviewed = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let before = action_service.project(fixture_program_id.as_str()).unwrap();
        let original_manifest = crate::agentic::review_evidence::work_manifest_digest(&before);
        assert!(!reviewed.reviews.is_empty());
        for review in &reviewed.reviews {
            let proof = review
                .verification
                .as_ref()
                .expect("real independent proof");
            assert_eq!(proof.input_set.as_ref().unwrap().version, 2);
            crate::agentic::review_evidence::validate_proof_inputs(
                services.event_store(),
                &before,
                Some(&reviewed),
                proof,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref(),
                },
            )
            .expect("proof is current before the controlled change");
        }
        match proof_scenario {
            R1ProofScenario::UnrelatedNote => {
                let original = reviewed
                    .reviews
                    .iter()
                    .find(|review| review.criterion_ref == "reviewed-delivery")
                    .unwrap()
                    .verification
                    .as_ref()
                    .unwrap();
                let alias = services
                    .independent_result_review(
                        &bind_root(task.clone()).actor,
                        &before,
                        &[content.selector.clone()],
                        Some("reviewed-delivery"),
                    )
                    .await
                    .expect("the real content alias selects its authoritative producer Task");
                assert_eq!(
                    alias.verification.input_set.as_ref().unwrap().task_refs,
                    original.input_set.as_ref().unwrap().task_refs
                );
                assert_eq!(
                    alias.verification.input_set.as_ref().unwrap().artifact_refs,
                    original.input_set.as_ref().unwrap().artifact_refs
                );
                assert_eq!(
                    alias.verification.work_manifest_digest,
                    original.work_manifest_digest
                );
                assert!(alias.verification.reads.iter().any(|read| {
                    read.sha256 == content.sha256 && read.bytes == content.bytes
                }));
                let note = services
                    .artifact_store()
                    .write_bytes(
                        harness_contract::context::ArtifactWriteDescriptor {
                            media_type: "text/plain".into(),
                            visibility_scope: "session:session-dispatch".into(),
                            expected_bytes: None,
                            original_name: Some("r1-unrelated-note.txt".into()),
                        },
                        b"An unrelated observation; not a required result or review material.",
                    )
                    .await
                    .unwrap();
                let observation = services
                    .submit_agent_action(&bind_root(root(
                        "r1-unrelated-note",
                        AgentAction::ArtifactCommit(ArtifactCommitInput {
                            content_ref: note.selector,
                            kind: "note".into(),
                            title: "Auxiliary observation".into(),
                            relates_to: Vec::new(),
                        }),
                    )))
                    .await
                    .unwrap();
                assert_eq!(
                    observation.status,
                    harness_contract::agent_action::AgentActionStatus::Applied,
                    "{observation:?}"
                );
                let current = action_service.project(fixture_program_id.as_str()).unwrap();
                assert_eq!(current.artifacts.len(), before.artifacts.len() + 1);
                assert_ne!(
                    crate::agentic::review_evidence::work_manifest_digest(&current),
                    original_manifest,
                    "the historical all-artifact invalidation really changes"
                );
            }
            R1ProofScenario::ExecutionEpoch => {
                let previous = services
                    .session_execution_policy("session-dispatch")
                    .unwrap();
                let replacement =
                    harness_contract::policy::SessionExecutionPolicy::from_profile(
                        harness_contract::policy::AutonomyProfileId::Cautious,
                        previous.revision + 1,
                        harness_contract::policy::SessionExecutionPolicyOrigin::SessionExplicit,
                    );
                services
                    .session_execution_policy_control("session-dispatch")
                    .unwrap()
                    .replace(replacement.clone())
                    .unwrap();
                assert_eq!(
                    services.session_execution_policy("session-dispatch"),
                    Some(replacement.clone())
                );
                assert_ne!(previous.permission_mode, replacement.permission_mode);
                assert!(replacement.revision > previous.revision);
                assert_eq!(
                    action_service.project(fixture_program_id.as_str()).unwrap(),
                    before,
                    "a future execution epoch must not rewrite business facts"
                );
            }
            R1ProofScenario::AcceptancePolicy => {
                let calls_before = effect_host.calls.lock().unwrap().len();
                let mut session = crate::Session::new();
                session.session_id = "session-dispatch".into();
                let runtime = crate::ConversationRuntime::new(
                    session,
                    NoReviewProvider,
                    RootReviewReader(Arc::clone(&effect_host)),
                    crate::PermissionPolicy::new(PermissionMode::DangerFullAccess),
                    vec!["Inspect the same committed result".into()],
                )
                .without_memory()
                .with_runtime_event_store(Arc::clone(services.event_store()));
                runtime
                    .begin_turn_strategy_with_resource_snapshot(
                        "turn-dispatch",
                        "调研外部资料并形成报告",
                        None,
                    )
                    .unwrap();
                runtime
                    .bind_turn_strategy_execution("turn-dispatch", "root-agentic-execution")
                    .unwrap();
                let call = crate::conversation::ModelToolCall {
                binding: None,
                id: "r1-policy-mutation-plan".into(),
                name: "edit".into(),
                input: r#"{"action":"write","path":"review-effect.txt","content":"requires actual approval before mutation"}"#.into(),
                depends_on: Vec::new(),
            };
                let request = crate::tool_dispatch::ToolRequest {
                    tool_use_id: call.id.clone(),
                    tool_name: call.name.clone(),
                    input: call.input.clone(),
                    depends_on: Vec::new(),
                };
                let prepared = crate::ToolExecutor::prepare_governed_invocations(
                    runtime.tool_executor().as_ref(),
                    std::slice::from_ref(&request),
                );
                let plan = crate::GovernedToolCompiler
                    .compile(
                        services.workspace_root(),
                        std::slice::from_ref(&request),
                        |name, input| {
                            prepared.iter().find_map(|invocation| {
                                (invocation.intent.tool_name == name
                                    && invocation.intent.normalized_input == *input)
                                    .then(|| {
                                        (
                                            invocation.effect.clone(),
                                            invocation.catalog_revision,
                                            invocation.descriptor_set_hash.clone(),
                                        )
                                    })
                            })
                        },
                    )
                    .unwrap();
                let decision = runtime
                    .retarget_active_turn_strategy_for_governed_plan(
                        &plan,
                        std::slice::from_ref(&call),
                    )
                    .unwrap();
                assert_eq!(
                    decision.pattern(),
                    harness_contract::core::ExecutionPattern::Execute
                );
                assert!(decision
                    .gates()
                    .contains(&harness_contract::core::ExecutionPolicyGate::Permission));
                println!(
                    "R1_ACCEPTANCE_POLICY_REVISION {}",
                    serde_json::json!({
                        "risk":decision.risk(),"modifiers":decision.modifiers(),"gates":decision.gates(),
                    })
                );
                assert_eq!(effect_host.calls.lock().unwrap().len(), calls_before);
            }
            _ => unreachable!(),
        }
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        let current_goal = services.goal_store().get(&reviewed.id).unwrap().unwrap();
        assert_eq!(current_goal, reviewed);
        for review in &reviewed.reviews {
            let proof = review.verification.as_ref().unwrap();
            let validation = crate::agentic::review_evidence::validate_proof_inputs(
                services.event_store(),
                &current,
                Some(&current_goal),
                proof,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref(),
                },
            );
            if proof_scenario == R1ProofScenario::AcceptancePolicy {
                let error = validation
                    .err()
                    .expect("changed acceptance policy stales the proof");
                assert!(error.contains("stale v2 proof"), "{error}");
                let fresh = crate::agentic::review_evidence::proof_input_snapshot(
                    services.event_store(),
                    &current,
                    Some(&current_goal),
                    proof,
                    crate::agentic::review_evidence::ResultContentReaders {
                        process_inputs: None,
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: services.session_history_reader().as_deref(),
                    },
                )
                .unwrap();
                assert_ne!(
                    fresh.policy_digest,
                    proof.input_set.as_ref().unwrap().acceptance_policy_digest
                );
            } else {
                let snapshot = validation
                    .expect("unrelated changes preserve the original independent verdict");
                assert_eq!(
                    snapshot.semantic_digest,
                    proof.input_set.as_ref().unwrap().semantic_digest
                );
            }
        }
        if proof_scenario == R1ProofScenario::AcceptancePolicy {
            for (index, review) in reviewed.reviews.iter().enumerate() {
                let refreshed = services
                .submit_agent_action(&bind_root(root(
                    &format!("r1-policy-refreshed-review-{index}"),
                    AgentAction::ObjectiveReview(
                        harness_contract::agent_action::ObjectiveReviewInput {
                            criterion_ref: review.criterion_ref.clone(),
                            decision: harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                            result_refs: review.result_refs.clone(),
                            evidence_refs: review.evidence_refs.clone(),
                            reason_ref: content.selector.clone(),
                            reason: "Review the same read evidence under the current acceptance policy".into(),
                        },
                    ),
                )))
                .await
                .unwrap();
                assert_eq!(
                    refreshed.status,
                    harness_contract::agent_action::AgentActionStatus::Applied,
                    "{refreshed:?}"
                );
            }
            let refreshed = services.goal_store().get(&reviewed.id).unwrap().unwrap();
            let program = action_service.project(fixture_program_id.as_str()).unwrap();
            for review in refreshed.reviews.iter().skip(reviewed.reviews.len()) {
                crate::agentic::review_evidence::validate_proof_inputs(
                    services.event_store(),
                    &program,
                    Some(&refreshed),
                    review.verification.as_ref().unwrap(),
                    crate::agentic::review_evidence::ResultContentReaders {
                        process_inputs: None,
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: services.session_history_reader().as_deref(),
                    },
                )
                .expect("fresh independent review closes the policy-change recovery");
            }
        }
        println!(
            "R1_PROOF_PRESERVATION {}",
            serde_json::json!({
                "scenario": match proof_scenario {
                    R1ProofScenario::UnrelatedNote => "unrelated_note",
                    R1ProofScenario::ExecutionEpoch => "execution_epoch",
                    R1ProofScenario::AcceptancePolicy => "acceptance_policy",
                    _ => unreachable!(),
                },
                "before_revision": before.revision,
                "after_revision": current.revision,
                "actual_review_count": reviewed.reviews.len(),
                "goal_spec_revision": reviewed.spec_revision,
            })
        );
    }
    #[cfg(test)]
    if proof_scenario == R1ProofScenario::Mechanical {
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let proof = goal.reviews.last().unwrap().verification.as_ref().unwrap();
        let history = services.session_history_reader();
        let readers = crate::agentic::review_evidence::ResultContentReaders {
            process_inputs: None,
            artifacts: Some(services.artifact_store().as_ref()),
            history: history.as_deref(),
        };
        let before = crate::agentic::review_evidence::validate_proof_inputs(
            services.event_store(),
            &current,
            Some(&goal),
            proof,
            readers,
        )
        .unwrap();
        let published = services
            .submit_agent_action(&bind_root(root(
                "mechanical-unrelated-note",
                AgentAction::MessagePublish(
                    harness_contract::agent_action::MessagePublishInput {
                        topic_ref: format!("topic:{}", current.program_id),
                        summary: Some("unrelated note".into()),
                        content_ref: None,
                        refs: vec![],
                        recipients: vec![],
                        intent: None,
                        issue_dispositions: vec![],
                    },
                ),
            )))
            .await
            .unwrap();
        assert_eq!(
            published.status,
            harness_contract::agent_action::AgentActionStatus::Applied
        );
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        let after = crate::agentic::review_evidence::validate_proof_inputs(
            services.event_store(),
            &current,
            Some(&goal),
            proof,
            readers,
        )
        .unwrap();
        assert_eq!(before.semantic_digest, after.semantic_digest);
    }
    Some((optional_effect,))
}
