{
    services
        .goal_store()
        .create(GoalContract {
            id: "goal:root-agentic-execution".into(),
            session_id: "session-dispatch".into(),
            objective: {
                #[cfg(test)]
                if let Ok(Some(objective)) =
                    NATIVE_TASK_CASE.try_with(|case| case.objective.clone())
                {
                    objective
                } else {
                    "dispatch objective".into()
                }
                #[cfg(not(test))]
                {
                    "dispatch objective".into()
                }
            },
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
                objective_id: fixture_objective_id.as_str().into(),
                session_id: "session-dispatch".into(),
                turn_id: "turn-dispatch".into(),
                root_execution_id: "root-agentic-execution".into(),
                agentic_program_id: fixture_program_id.as_str().into(),
            }),
            spec_revision: 1,
            spec_digest: "dispatch-test".into(),
            review_refs: vec![],
            waiting: None,
            participation_requirement: None,
            obligations: vec![harness_contract::goal::ObjectiveObligation {
                obligation_id: "source-backed-delivery".into(),
                required: true,
                success_predicate:
                    "Deliver the source-backed result with independent inspection".into(),
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
        let disclosure_stage = boxed_fixture_stage(|| async {
            use harness_contract::agent_action::{
                AgentActionStatus, IssueDisposition, IssueDispositionKind, MessagePublishInput,
                ObjectiveCompleteRequestInput,
            };
            let issue = crate::agentic::issues::issues(
                &action_service.project(fixture_program_id.as_str()).unwrap(),
            )
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
                            topic_ref: format!("topic:{fixture_program_id}"),
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
        });
        disclosure_stage.await;
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
    let supervisor = crate::execution_core::goal::ObjectiveSupervisor::new(Arc::clone(
        services.goal_store(),
    ));
    let mut reconciled = crate::agentic::supervision::reconcile_completion_request(
        &action_service,
        &supervisor,
        fixture_program_id.as_str(),
    );
    if reconciled
        .as_ref()
        .err()
        .is_some_and(|error| error.contains("revision mismatch"))
    {
        let drain_before_retry = drain_timing == PhysicalDrainTiming::BeforeGoal;
        eprintln!("R1_RECONCILE_CAS_RETRY drain_before_retry={drain_before_retry} retry=1");
        if drain_before_retry {
            drain_workers().await;
        }
        reconciled = crate::agentic::supervision::reconcile_completion_request(
            &action_service,
            &supervisor,
            fixture_program_id.as_str(),
        );
    }
    reconciled.unwrap();
    let gap = action_service.project(fixture_program_id.as_str()).unwrap();
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
    let mut scoped_goal_reviewer = if proof_scenario.needs_scoped_review() {
        Some(
            start_r1_scoped_goal_reviewer(
                &services,
                &bind_root(root("r1-scoped-reviewer-root", task.action.clone())).actor,
                &content.selector,
                "r1-scoped-reviewer",
                !proof_scenario.native_sources()
                    && !matches!(
                        proof_scenario,
                        R1ProofScenario::TransitiveDependency
                            | R1ProofScenario::ReviewerInfluence
                            | R1ProofScenario::ReadDelivery(_)
                    ),
            )
            .await,
        )
    } else {
        None
    };
    if with_effect {
        let effect_observation_stage = boxed_fixture_stage(|| async {
            let (effect_ref, effect_runtime) =
                root_reads_generic_results(&services, read_host, &content.selector, true, true)
                    .await;
            let effect_ref = effect_ref.unwrap();
            let added = services
                .submit_agent_action(&bind_root(root(
                    "effect-condition",
                    AgentAction::ObjectiveUpdate(
                        harness_contract::agent_action::ObjectiveUpdateInput {
                            criterion_ref: None,
                            operation:
                                harness_contract::agent_action::ObjectiveUpdateOperation::Add,
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
            let admitted = services
                .dispatch_agentic_followups(&work, review_context())
                .await
                .unwrap();
            assert_eq!(admitted.len(), 1);
            let effect_session = wait_real_process(&services, &admitted[0].graph_id).await;
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
                        decision:
                            harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
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
            let reader = RealProcessSession::prepare(
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
                    &serde_json::json!({"evidence_ref":effect_access.retrieval_selector})
                        .to_string(),
                )
                .await
                .unwrap();
            let missing_effect_observation = services
                .submit_agent_action(&check_effect)
                .await
                .unwrap_err();
            assert!(missing_effect_observation.contains("review_requires_effect_observation"),
    "effect-negative prerequisite failed before its intended gate: {missing_effect_observation}");
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
            let accepted = services.submit_agent_action(&check_effect).await
    .unwrap_or_else(|error| panic!(
        "effect review failed without weakening alias coverage: {error}; original_results={:?}; diagnostics={}",
        match &check_effect.action {
            AgentAction::ObjectiveReview(input) => &input.result_refs,
            _ => unreachable!(),
        },
        r1_goal_proof_diagnostic(&services, fixture_program_id.as_str()),
    ));
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
    "actually persisted revised content\nstable middle line\nactually revised a second time\n",
)
.await;
            // Negative preparation only: the real request remains uncommitted while
            // this review worker is active. The Goal owner must invalidate its old review.
            let mut proposed = action_service.project(fixture_program_id.as_str()).unwrap();
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
                    r#"{"path":"review-effect.txt","limit":1}"#,
                )
                .await
                .unwrap();
            let partial_refresh = services.submit_agent_action(&refreshed).await.unwrap_err();
            assert!(
                partial_refresh.contains("review_requires_effect_observation"),
                "{partial_refresh}"
            );
            reader
                .execute_tool(
                    "reread-other-dependency-after-unscoped-root-write",
                    "read_file",
                    r#"{"path":"unrelated-review.txt"}"#,
                )
                .await
                .unwrap();
            for (call_id, input) in [
                (
                    "reread-only-middle-line",
                    r#"{"path":"review-effect.txt","offset":1,"limit":1}"#,
                ),
                (
                    "reread-only-first-two-lines",
                    r#"{"path":"review-effect.txt","limit":2}"#,
                ),
            ] {
                let output = reader
                    .execute_tool(call_id, "read_file", input)
                    .await
                    .unwrap();
                let actual: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(actual["truncated"], true);
                assert_eq!(actual["file"]["totalLines"], 3);
                assert!(!actual["file"]["content"]
                    .as_str()
                    .unwrap()
                    .contains("actually revised a second time"));
                let error = services.submit_agent_action(&refreshed).await.unwrap_err();
                assert!(
                    error.contains("review_requires_effect_observation"),
                    "{error}"
                );
                println!(
                    "R1_PARTIAL_DEPENDENCY_REJECTED {}",
                    serde_json::json!({
                        "call_id":call_id,"start_line":actual["file"]["startLine"],
                        "num_lines":actual["file"]["numLines"],"total_lines":actual["file"]["totalLines"],
                        "reason":error,
                    })
                );
            }
            reader
                .execute_tool(
                    "reread-complete-after-partial-windows",
                    "read_file",
                    r#"{"path":"review-effect.txt","complete":true}"#,
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
            let current = action_service.project(fixture_program_id.as_str()).unwrap();
            let publication = current
                .artifacts
                .get(&wrapped.changed_refs[0])
                .expect("the original wrapped publication must remain authoritative");
            assert_eq!(publication.artifact_ref, wrapped.changed_refs[0]);
            assert_eq!(publication.content_ref, effect_access.retrieval_selector);
            let body_artifact = services
                .artifact_store()
                .resolve(&publication.content_ref)
                .unwrap();
            assert_eq!(body_artifact.selector, publication.content_ref);
            assert_eq!(
                body_artifact.visibility_scope,
                format!("session:{}", physical.session_id())
            );
            let original_body = services
                .artifact_store()
                .read_blocking(&body_artifact, &body_artifact.visibility_scope, None)
                .unwrap();
            let original_sha = crate::context_evidence::read_sha256(&original_body);
            assert_eq!(original_sha, body_artifact.sha256);
            assert_eq!(original_body.len() as u64, body_artifact.bytes);
            let raw_access = services
                .session_evidence_access(
                    physical.session_id(),
                    effect_ref.strip_prefix("tool://").unwrap(),
                )
                .await
                .unwrap()
                .expect("the original ToolRaw mapping must still exist");
            assert_eq!(raw_access.retrieval_selector, publication.content_ref);
            assert_eq!(raw_access.sha256, body_artifact.sha256);
            assert_eq!(raw_access.bytes, body_artifact.bytes);
            assert_eq!(raw_access.visibility_scope, body_artifact.visibility_scope);
            assert_ne!(
        effect_ref, publication.content_ref,
        "logical ToolRaw identity must not be confused with its opaque content selector"
    );
            let receipt_union = proof
                .reads
                .iter()
                .flat_map(|read| read.receipt_refs.iter())
                .cloned()
                .collect::<std::collections::BTreeSet<_>>();
            let (file_receipt, file_material, _) = r1_original_process_read_659(
                &services,
                &physical,
                "reread-complete-after-partial-windows",
            );
            let crate::context_evidence::ReadExtent::FileLines {
                start_line,
                line_count,
                total_lines,
                source_omitted,
                ..
            } = &file_material.acquisition.body.extent
            else {
                panic!("the effect witness must be the actual complete File acquisition");
            };
            assert_eq!(*start_line, 1);
            assert_eq!(line_count, total_lines);
            assert!(!source_omitted);
            let actual_file =
                std::fs::read(services.workspace_root().join("review-effect.txt")).unwrap();
            assert_eq!(
                file_material.acquisition.body.source_sha256,
                crate::context_evidence::read_sha256(&actual_file)
            );
            assert_eq!(
                file_material.acquisition.body.source_bytes,
                actual_file.len() as u64
            );
            let mut independent_receipts = std::collections::BTreeSet::new();
            let mut independent_primaries = std::collections::BTreeSet::new();
            for (call_id, input_ref, result_ref) in [
                (
                    "read-effect-receipt",
                    effect_ref.as_str(),
                    effect_ref.as_str(),
                ),
                (
                    "read-effect-content-alias",
                    effect_access.retrieval_selector.as_str(),
                    wrapped.changed_refs[0].as_str(),
                ),
            ] {
                let (receipt_ref, material, output) =
                    r1_original_process_read_659(&services, &physical, call_id);
                assert!(independent_receipts.insert(receipt_ref.clone()));
                assert!(independent_primaries.insert((
                    material.acquisition.primary_stream.clone(),
                    material.acquisition.primary_key.clone(),
                )));
                assert!(
            receipt_union.contains(&receipt_ref),
            "original call {call_id} receipt {receipt_ref} is missing from proof coverage"
        );
                let covered = proof
                    .reads
                    .iter()
                    .find(|read| read.result_ref == result_ref)
                    .expect("the exact ToolRaw/publication proof identity must remain");
                assert!(covered.receipt_refs.contains(&receipt_ref),
        "original call {call_id} receipt {receipt_ref} must cover its own result {result_ref}");
                assert_eq!(
                    material.acquisition.input_sha256,
                    crate::context_evidence::read_sha256(
                        serde_json::json!({"evidence_ref":input_ref})
                            .to_string()
                            .as_bytes()
                    )
                );
                assert_eq!(material.acquisition.tool_name, "evidence_retrieve");
                let body = &material.acquisition.body;
                assert_eq!(body.source_ref, input_ref);
                assert_eq!(body.source_sha256, original_sha);
                assert_eq!(body.source_bytes, body_artifact.bytes);
                assert_eq!(covered.content_ref, body_artifact.selector);
                assert_eq!(
                    covered.sha256.trim_start_matches("sha256:"),
                    body.source_sha256.trim_start_matches("sha256:")
                );
                assert_eq!(covered.bytes, body.source_bytes);
                let output_body =
                    crate::context_evidence::read_body_seal("evidence_retrieve", &output)
                        .expect("the original response must retain a verifiable body extent");
                assert_eq!(&output_body, body);
                let crate::context_evidence::ReadExtent::ArtifactChunks {
                    total_chunks,
                    chunks,
                } = &body.extent
                else {
                    panic!("evidence retrieval must retain original chunk extent");
                };
                assert_eq!(
                    chunks
                        .iter()
                        .map(|chunk| chunk.index)
                        .collect::<std::collections::BTreeSet<_>>(),
                    (0..*total_chunks).collect::<std::collections::BTreeSet<_>>()
                );
                let actual_output: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(actual_output["evidence_ref"], input_ref);
                assert_eq!(actual_output["available"], true);
                assert_eq!(actual_output["bytes"], body_artifact.bytes);
                assert_eq!(
                    actual_output["sha256"]
                        .as_str()
                        .unwrap()
                        .trim_start_matches("sha256:"),
                    body.source_sha256.trim_start_matches("sha256:")
                );
                let delivered = actual_output["chunks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|chunk| chunk["content"].as_str().unwrap())
                    .collect::<String>();
                assert_eq!(delivered.as_bytes(), original_body.as_slice());
                assert_eq!(
                    chunks.iter().map(|chunk| chunk.body_bytes).sum::<u64>(),
                    body.source_bytes
                );
                for chunk in chunks {
                    let actual = actual_output["chunks"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .find(|actual| actual["index"].as_u64() == Some(chunk.index))
                        .unwrap();
                    let text = actual["content"].as_str().unwrap();
                    assert_eq!(chunk.body_bytes, text.len() as u64);
                    assert_eq!(
                        chunk.body_sha256,
                        crate::context_evidence::read_sha256(text.as_bytes())
                    );
                }
                assert!(
            covered.effect_observation_refs.contains(&file_receipt),
            "{result_ref} must cite the real current complete File witness {file_receipt}"
        );
            }
            assert_eq!(independent_receipts.len(), 2);
            assert_eq!(independent_primaries.len(), 2);
            let requested = match &check_effect.action {
                AgentAction::ObjectiveReview(input) => &input.result_refs,
                _ => unreachable!(),
            };
            assert_eq!(
                requested.len(),
                3,
                "all original result citations must remain"
            );
            let identity = |reference: &str| {
                crate::agentic::review_evidence::review_result_identity(&current, reference)
                    .unwrap()
            };
            let expected_identities = requested
                .iter()
                .map(|reference| identity(reference))
                .collect::<std::collections::BTreeSet<_>>();
            let actual_identities = proof
                .reads
                .iter()
                .map(|read| identity(&read.result_ref))
                .collect::<std::collections::BTreeSet<_>>();
            assert_eq!(actual_identities, expected_identities);
            assert_eq!(
        actual_identities.len(),
        proof.reads.len(),
        "proof coverage must contain exactly one entry per authenticated material identity"
    );
            assert_eq!(
                identity(&requested[1]),
                identity(&requested[2]),
                "logical artifact and exact content selector denote one publication"
            );
            assert_ne!(
                identity(&requested[0]),
                identity(&requested[1]),
                "the original ToolRaw receipt remains a separate authenticated identity"
            );
            assert!(proof
                .reads
                .iter()
                .all(|read| read.result_kind
                    == harness_contract::goal::GoalResultKind::ToolEffect));

            let manifest = proof.work_manifest_digest.clone();
            let artifact_count_before_retirement = action_service
                .project(fixture_program_id.as_str())
                .unwrap()
                .artifacts
                .len();
            let withdrawn = AgentActionEnvelope {
                action_id: "effect-review-finished".into(),
                actor: check_effect.actor.clone(),
                expected_revision: None,
                action: AgentAction::TaskWithdraw(
                    harness_contract::agent_action::TaskWithdrawInput {
                        task_ref: work_receipt.changed_refs[0].clone(),
                        reason_ref: content.selector.clone(),
                        evidence_refs: vec![content.selector.clone()],
                    },
                ),
            };
            effect_session
                .retire_control(&withdrawn, &work_receipt.changed_refs[0])
                .await;
            services
                .dispatch_agentic_followups(&withdrawn, review_context())
                .await
                .unwrap();
            wait_retired_process(
                &services,
                &effect_session.packet,
                &work_receipt.changed_refs[0],
            )
            .await;
            let drained = services
                .agent_action_service()
                .project(fixture_program_id.as_str())
                .unwrap();
            assert_eq!(
                drained.tasks[&work_receipt.changed_refs[0]].status,
                crate::AgenticTaskStatus::Withdrawn
            );
            assert!(drained.tasks[&work_receipt.changed_refs[0]]
                .active_attempts
                .is_empty());
            wait_retired_process(
                &services,
                &effect_session.packet,
                &work_receipt.changed_refs[0],
            )
            .await;
            assert_eq!(drained.artifacts.len(), artifact_count_before_retirement);
            let current_goal = services
                .goal_store()
                .get("goal:root-agentic-execution")
                .unwrap()
                .unwrap();
            let current = crate::agentic::review_evidence::validate_proof_inputs(
                services.event_store(),
                &drained,
                Some(&current_goal),
                proof,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref(),
                },
            )
            .expect("retiring the reviewer does not change its actual reviewed inputs");
            assert_eq!(current.work_digest, manifest);
            let mut stale = check_effect.clone();
            stale.action_id = "withdrawn-review-cannot-mutate-goal".into();
            assert_eq!(
                services.submit_agent_action(&stale).await.unwrap().status,
                harness_contract::agent_action::AgentActionStatus::Rejected
            );
        });
        effect_observation_stage.await;
    } else if delegated_effect {
        if scoped_goal_reviewer.is_none() {
            let delegated_target_stage = boxed_fixture_stage(|| async {
                let (_, runtime) = root_reads_generic_results(
                    &services,
                    read_host,
                    &content.selector,
                    false,
                    false,
                )
                .await;
                root_review_target_tool(
                    &runtime,
                    "root-verify-delegated-target",
                    "read_file",
                    "review-effect.txt",
                    "",
                )
                .await;
            });
            delegated_target_stage.await;
        }
    } else {
        let root_material_stage = boxed_fixture_stage(|| async {
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
                root_reads_review_result(&services, read_host.clone(), &unrelated.selector)
                    .await;
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
        });
        root_material_stage.await;
    }
    if let R1ProofScenario::LegacyReplay(unknown_version) = proof_scenario {
        assert!(drain_timing == PhysicalDrainTiming::BeforeGoal);
        exercise_legacy_review_replay(
            &services,
            &effect_host,
            unknown_version,
            bind_root(root(
                "r1-historical-objective-review",
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
            )),
        )
        .await;
        return None;
    }
    let mut optional_effect = if proof_scenario.optional_unknown() {
        let optional_production_stage = boxed_fixture_stage(|| async {
            Some(
                start_r1_optional_effect(
                    &services,
                    &effect_host,
                    &bind_root(root("r1-optional-actor", task.action.clone())).actor,
                    &content.selector,
                    proof_scenario,
                )
                .await,
            )
        });
        optional_production_stage.await
    } else {
        None
    };
    if proof_scenario.invalid_child() {
        use sha2::Digest;
        let optional = optional_effect.as_ref().unwrap();
        let links = services
            .graph_state_store()
            .child_links(&optional.graph_id)
            .unwrap();
        assert_eq!(links.len(), 1);
        let original = services
            .graph_state_store()
            .load(&links[0].child_execution_id)
            .unwrap();
        assert_eq!(original.nodes.len(), 1);
        assert_eq!(original.nodes[0].kind, ExecutionNodeKind::ToolBatch);
        use crate::execution_core::graph::executors::agent_tool::{
            decode_bound_agent_tool, decode_bound_agent_tool_graph,
            decode_bound_agent_tool_node, encode_bound_agent_tool,
        };
        let mut bound = decode_bound_agent_tool_graph(&original).unwrap();
        match proof_scenario {
            R1ProofScenario::OptionalForeignChild => {
                bound.request.parent_execution.as_mut().unwrap().node_id =
                    "foreign-agent-node".into();
            }
            R1ProofScenario::OptionalWrongChildAttempt => {
                bound.request.parent_execution_attempt = Some(optional.packet.attempt + 1);
            }
            R1ProofScenario::OptionalMixedChild => {}
            _ => unreachable!(),
        }
        let packed =
            encode_bound_agent_tool(&bound.agent_id, &bound.run_id, bound.request).unwrap();
        let envelope = harness_contract::step::parse_strict_json(&packed).unwrap();
        let payload = serde_json::to_string_pretty(&envelope).unwrap();
        let decoded = decode_bound_agent_tool(&payload).unwrap();
        if proof_scenario == R1ProofScenario::OptionalWrongChildAttempt {
            assert_eq!(
                decoded.request.parent_execution_attempt,
                Some(optional.packet.attempt + 1)
            );
            assert_eq!(decoded.request.parent_execution, original.parent_execution);
        }
        assert_ne!(payload, original.nodes[0].payload_ref);
        let digest = format!("{:x}", sha2::Sha256::digest(payload.as_bytes()));
        let mut child = ExecutionGraph::new("Adversarial typed child input");
        child.id = format!("agent-tool-batch:{digest}");
        child.lineage = original.lineage.clone();
        child.service_class = original.service_class;
        child.parent_execution = original.parent_execution.clone();
        let mut node =
            ExecutionNodeSpec::new(ExecutionNodeKind::ToolBatch, "tool_batch", payload);
        node.id = format!("agent-tool:{digest}");
        node.idempotency_key = node.id.clone();
        child.nodes.push(node);
        if proof_scenario == R1ProofScenario::OptionalMixedChild {
            child.nodes.push(ExecutionNodeSpec::new(
                ExecutionNodeKind::InlineModel,
                "inline_model",
                "unowned-child-payload",
            ));
        }
        decode_bound_agent_tool_node(&child.id, &child.nodes[0]).unwrap();
        match proof_scenario {
            R1ProofScenario::OptionalForeignChild => {
                assert!(decode_bound_agent_tool_graph(&child)
                    .unwrap_err()
                    .to_string()
                    .contains("original parent mismatch"));
            }
            R1ProofScenario::OptionalWrongChildAttempt => {
                assert_eq!(
                    decode_bound_agent_tool_graph(&child)
                        .unwrap()
                        .request
                        .parent_execution_attempt,
                    Some(optional.packet.attempt + 1)
                );
            }
            R1ProofScenario::OptionalMixedChild => {
                assert!(decode_bound_agent_tool_graph(&child)
                    .unwrap_err()
                    .to_string()
                    .contains("requires one original node"));
            }
            _ => unreachable!(),
        }
        services.commit_service().register_graph(child).unwrap();
        optional.assert_pending(&services);
    }
    Some((scoped_goal_reviewer, optional_effect))
}
