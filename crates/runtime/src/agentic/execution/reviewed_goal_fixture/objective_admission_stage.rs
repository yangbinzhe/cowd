{
    let native_sources = if proof_scenario.native_sources() {
        let native_root_actor = bind_root(root("native-root", task.action.clone())).actor;
        Some(
            exercise_native_task_sources(
                &services,
                &effect_host,
                &native_root_actor,
                proof_scenario,
            )
            .await,
        )
    } else {
        None
    };
    #[cfg(test)]
    if proof_scenario == R1ProofScenario::TaskReviewAdmission
        && !NATIVE_TASK_CASE
            .try_with(|case| case.complete_goal)
            .unwrap_or(false)
    {
        assert!(drain_timing == PhysicalDrainTiming::BeforeGoal);
        scoped_goal_reviewer.take().unwrap().finish(&services).await;
        services.shutdown_maintenance().await;
        return None;
    }
    let mut reviewed_delivery_refs = vec![artifact.clone()];
    if let Some(native) = native_sources.as_ref() {
        let reviewer = scoped_goal_reviewer.as_ref().unwrap();
        for (index, selector) in native.result_contents.iter().enumerate() {
            reviewer
                .reader
                .execute_tool(
                    &format!("native-result-independent-read:{index}"),
                    "evidence_retrieve",
                    &serde_json::json!({"evidence_ref":selector}).to_string(),
                )
                .await
                .unwrap();
        }
        reviewer
            .reader
            .execute_tool(
                "native-goal-independent-current-target-read",
                "read_file",
                r#"{"path":"review-effect.txt","complete":true}"#,
            )
            .await
            .unwrap();
        reviewed_delivery_refs.extend(native.result_refs.clone());
    }
    #[cfg(test)]
    if let R1ProofScenario::ProgramPublication(case) = proof_scenario {
        let native = native_sources.as_ref().unwrap();
        let root_actor = bind_root(root("program-publication-root", task.action.clone())).actor;
        if exercise_program_publication_review(
            &services,
            &effect_host,
            &root_actor,
            scoped_goal_reviewer.as_ref().unwrap(),
            native,
            case,
            &content.selector,
            &mut reviewed_delivery_refs,
        )
        .await
        {
            scoped_goal_reviewer.take().unwrap().finish(&services).await;
            drain_workers().await;
            return None;
        }
    }
    if proof_scenario == R1ProofScenario::ReviewerInfluence {
        let reviewer_influence_stage = boxed_fixture_stage(|| async {
            let reviewer = scoped_goal_reviewer.as_ref().unwrap();
            let (derived_selector, derived_artifact) = publish_r1_material(
                &services,
                &reviewer.actor,
                "r1-reviewer-owned-material",
                b"Actual reviewer-authored input D for a subsequently produced result.",
                Vec::new(),
            )
            .await;
            let (_, _root_reader) = root_reads_generic_results(
                &services,
                effect_host.clone(),
                &derived_selector,
                false,
                false,
            )
            .await;
            let (result_selector, result_artifact) = publish_r1_material(
                &services,
                &bind_root(root("r1-influenced-root", task.action.clone())).actor,
                "r1-primary-using-reviewer-material",
                b"Root produced R-prime after actually reading D.",
                vec![derived_artifact.clone()],
            )
            .await;
            reviewer
                .reader
                .execute_tool(
                    "r1-reviewer-influenced-result-read",
                    "evidence_retrieve",
                    &serde_json::json!({"evidence_ref":result_selector}).to_string(),
                )
                .await
                .unwrap();
            let before = services
                .goal_store()
                .get("goal:root-agentic-execution")
                .unwrap()
                .unwrap();
            let refusal = services
    .submit_agent_action(&bind_review(root(
        "r1-transitive-self-review-refusal",
        AgentAction::ObjectiveReview(
            harness_contract::agent_action::ObjectiveReviewInput {
                criterion_ref: "source-backed-delivery".into(),
                decision:
                    harness_contract::agent_action::ObjectiveReviewDecision::Satisfied,
                result_refs: vec![artifact.clone(), result_artifact.clone()],
                evidence_refs: vec![content.selector.clone()],
                reason_ref: content.selector.clone(),
                reason: String::new(),
            },
        ),
    ), scoped_goal_reviewer.as_ref()))
    .await
    .unwrap_err();
            assert!(
                refusal.contains("producer")
                    && (refusal.contains("reviewer") || refusal.contains("independen")),
                "{refusal}; reviewer_task={:?}; derived_artifact={:?}; root_result={:?}",
                services
                    .agent_action_service()
                    .project(fixture_program_id.as_str())
                    .map(|program| program.tasks.get(&reviewer.task_ref).cloned()),
                services
                    .agent_action_service()
                    .project(fixture_program_id.as_str())
                    .map(|program| program.artifacts.get(&derived_artifact).cloned()),
                services
                    .agent_action_service()
                    .project(fixture_program_id.as_str())
                    .map(|program| program.artifacts.get(&result_artifact).cloned()),
            );
            assert_eq!(
                services.goal_store().get(&before.id).unwrap().unwrap(),
                before
            );
            assert!(before.terminal.is_none());
            assert!(action_service
                .project(fixture_program_id.as_str())
                .unwrap()
                .objective_verdict
                .is_none());
            println!(
                "R1_PRIMARY_REVIEWER_INFLUENCE_REFUSED {}",
                serde_json::json!({
                    "reviewer":reviewer.actor.actor_id,"derived_input":derived_artifact,"primary":result_artifact,"reason":refusal,"half_a":false,
                })
            );
            _root_reader
        });
        let _root_reader = reviewer_influence_stage.await;
        scoped_goal_reviewer.take().unwrap().finish(&services).await;
        services
            .cancel_execution_tree(&review_graph.id, "primary influence negative complete")
            .await
            .unwrap();
        services
            .cancel_execution_tree(packet.graph_id(), "primary influence negative complete")
            .await
            .unwrap();
        return None;
    }
    let transitive_inputs = if proof_scenario == R1ProofScenario::TransitiveDependency
        || proof_scenario.mechanical_approval()
    {
        let transitive_production_stage = boxed_fixture_stage(|| async {
            let (_, runtime) = root_reads_generic_results(
                &services,
                effect_host.clone(),
                &content.selector,
                false,
                true,
            )
            .await;
            write_root_review_effect(
                &runtime,
                "r1-transitive-input-setup",
                delegated_write_content,
            )
            .await;
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                    .unwrap(),
                delegated_write_content
            );
            let producer = start_r1_scoped_goal_reviewer(
                &services,
                &bind_root(root("r1-transitive-root", task.action.clone())).actor,
                &content.selector,
                "r1-derived-producer",
                true,
            )
            .await;
            let approval_ref = create_r1_transitive_decision(&services);
            producer
                .reader
                .execute_tool(
                    "r1-derived-approval-read",
                    "evidence_retrieve",
                    &serde_json::json!({"evidence_ref":approval_ref}).to_string(),
                )
                .await
                .unwrap();
            let (_, _, approval_source) = services
                .approval_result_content("session-dispatch", &approval_ref)
                .await
                .unwrap();
            let (derived_selector, derived_artifact) = publish_r1_material(
                &services,
                &producer.actor,
                "r1-derived-publication",
                b"Derived observation D uses the actual approval and physical input X.",
                vec![artifact.clone()],
            )
            .await;
            root_review_input_tool(
                &runtime,
                "r1-root-derived-material-read",
                "evidence_retrieve",
                &serde_json::json!({"evidence_ref":derived_selector}).to_string(),
            )
            .await;
            let (result_selector, result_artifact) = publish_r1_material(
                &services,
                &bind_root(root("r1-transitive-result-actor", task.action.clone())).actor,
                "r1-transitive-result-publication",
                b"Root result R uses the actually retrieved derived observation D.",
                vec![derived_artifact],
            )
            .await;
            let reviewer = scoped_goal_reviewer.as_ref().unwrap();
            reviewer
                .reader
                .execute_tool(
                    "r1-transitive-result-read",
                    "evidence_retrieve",
                    &serde_json::json!({"evidence_ref":result_selector}).to_string(),
                )
                .await
                .unwrap();
            reviewed_delivery_refs.push(result_artifact);
            let producer_graph = producer.packet.graph_id().to_owned();
            producer.finish(&services).await;
            (runtime, producer_graph, approval_source)
        });
        Some(transitive_production_stage.await)
    } else {
        None
    };
    #[cfg(test)]
    if proof_scenario == R1ProofScenario::MechanicalExtra {
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
                "actual additional accepted and waived conditions",
                |goal| {
                    let mut extra = goal.criteria[0].clone();
                    extra.id = "additional-accepted-condition".into();
                    extra.status = harness_contract::goal::AcceptanceStatus::Open;
                    extra.required_evidence = vec![content.selector.clone()];
                    goal.criteria.push(extra.clone());
                    extra.id = "waived-additional".into();
                    extra.status = harness_contract::goal::AcceptanceStatus::Waived;
                    extra.waiver = Some(harness_contract::goal::CriterionWaiver {
                        actor: "operator:fixture".into(),
                        reason: "explicit bounded waiver".into(),
                        permission_receipt: content.selector.clone(),
                    });
                    goal.criteria.push(extra);
                    let mut optional = goal.obligations[0].clone();
                    optional.obligation_id = "unrelated-optional".into();
                    optional.required = false;
                    optional.state = harness_contract::goal::ObjectiveObligationState::Open;
                    optional.verifier_decision = None;
                    goal.obligations.push(optional.clone());
                    optional.obligation_id = "additional-accepted-condition".into();
                    goal.obligations.push(optional);
                    let mut intersection = goal.obligations[0].clone();
                    intersection.obligation_id = goal.user_intent_criterion_id.clone().unwrap();
                    intersection.artifact_refs = reviewed_delivery_refs.clone();
                    intersection.state = harness_contract::goal::ObjectiveObligationState::Open;
                    intersection.verifier_decision = None;
                    goal.obligations.push(intersection);
                    goal.spec_revision += 1;
                    goal.spec_digest = crate::execution_core::goal::goal_spec_digest(goal);
                    vec![
                        "criteria".into(),
                        "obligations".into(),
                        "spec_revision".into(),
                        "spec_digest".into(),
                    ]
                },
            )
            .unwrap();
    }
    let bounded_selected_body_receipts = if !bounded_runs.is_empty() {
        let bounded_receipt_stage = boxed_fixture_stage(|| async {
            let mut bounded_selected_body_receipts = Vec::new();
            let reviewer = scoped_goal_reviewer.as_ref().unwrap();
            for path in ["bounded_source.py", "bounded_test.py"] {
                reviewer
                    .reader
                    .execute_tool(
                        &format!("bounded-goal-file:{path}"),
                        "read_file",
                        &serde_json::json!({"path":path,"complete":true}).to_string(),
                    )
                    .await
                    .unwrap();
            }
            for (index, run) in bounded_runs.iter().enumerate() {
                let receipts = read_bounded_body_pages_283(
                    &services,
                    &reviewer.reader,
                    &format!("bounded-goal-body:{index}"),
                    &run.body,
                    None,
                )
                .await;
                if index + 1 == bounded_runs.len() {
                    bounded_selected_body_receipts = receipts;
                }
            }
            let (_, runtime) = execute_root_review_reads_with_approval(
                &services,
                effect_host.clone(),
                &content.selector,
                true,
                false,
                PermissionMode::DangerFullAccess,
                Some(Arc::clone(services.approval_coordinator())),
            )
            .await;
            assert!(!crate::ToolExecutor::owns_durable_tool_effect(
                &RootReviewReader(effect_host.clone()),
                "execute_code"
            ));
            let bounded_input = serde_json::json!({"language":"python","code":"exec(open('/project/bounded_test.py').read())",
    "input_files":["bounded_source.py","bounded_test.py"],"timeout_ms":5000}).to_string();
            runtime
                .execute_approved_bounded_code(&bounded_input)
                .await
                .unwrap();
            let request = effect_host
                .calls
                .lock()
                .unwrap()
                .iter()
                .find(|request| {
                    request.tool_name == "execute_code"
                        && request.tool_use_id == "bounded-root-nonowns-283"
                })
                .cloned()
                .unwrap();
            assert!(request.parent_execution.is_none());
            let effect = &request.compiled_invocation.as_ref().unwrap().effect;
            let receipt = services
                .commit_service()
                .completed_tool_effect(&request, effect)
                .unwrap()
                .unwrap();
            let fact = receipt
                .observed_evidence
                .iter()
                .find_map(|observed| match &observed.target {
                    harness_contract::context::EvidenceTargetIdentity::ToolExecution {
                        execution,
                    } => Some(execution),
                    _ => None,
                })
                .unwrap();
            assert!(fact.stdout_bytes > 16 * 1024);
            read_bounded_body_pages_283(
                &services,
                &reviewer.reader,
                "bounded-goal-root-body",
                &fact.body,
                None,
            )
            .await;
            bounded_selected_body_receipts
        });
        bounded_receipt_stage.await
    } else {
        Vec::new()
    };
    let obligation_envelope = bind_review(
        root(
            "source-obligation-review",
            AgentAction::ObjectiveReview(
                harness_contract::agent_action::ObjectiveReviewInput {
                    criterion_ref: "source-backed-delivery".into(),
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
    );
    if let Some((runtime, producer_graph, approval_source)) = transitive_inputs.as_ref() {
        let transitive_revalidation_stage = boxed_fixture_stage(|| async {
            let reviewer = scoped_goal_reviewer.as_ref().unwrap();
            reviewer
                .reader
                .execute_tool(
                    "r1-independent-transitive-baseline-input-read",
                    "read_file",
                    r#"{"path":"review-effect.txt","complete":true}"#,
                )
                .await
                .unwrap();
            let (_, baseline_read, _) = r1_original_process_read_659(
                &services,
                &reviewer.packet,
                "r1-independent-transitive-baseline-input-read",
            );
            let actual_input =
                std::fs::read(services.workspace_root().join("review-effect.txt")).unwrap();
            let setup_request = effect_host
                .calls
                .lock()
                .unwrap()
                .iter()
                .find(|request| {
                    request.tool_name == "edit"
                        && request.tool_use_id == "r1-transitive-input-setup"
                })
                .cloned()
                .expect("the actual root setup write must remain recorded");
            let setup_receipt = services
                .event_store()
                .event_by_idempotency_key(
                    &format!("execution-effect:{}", setup_request.idempotency_key),
                    &format!("{}:receipt", setup_request.idempotency_key),
                )
                .unwrap()
                .expect("the root setup write needs its actual original receipt");
            assert_eq!(setup_receipt.kind, "execution.effect.receipt");
            assert_eq!(setup_receipt.payload["outcome"]["status"], "executed");
            assert!(baseline_read.acquisition_cursor > setup_receipt.commit_cursor);
            assert_eq!(
                baseline_read.acquisition.body.source_sha256,
                crate::context_evidence::read_sha256(&actual_input)
            );
            assert_eq!(
                baseline_read.acquisition.body.source_bytes,
                actual_input.len() as u64
            );
            let crate::context_evidence::ReadExtent::FileLines {
                start_line,
                line_count,
                total_lines,
                source_omitted,
                ..
            } = &baseline_read.acquisition.body.extent
            else {
                panic!("transitive baseline needs its independent actual File read, not an artifact read");
            };
            assert_eq!(*start_line, 1);
            assert_eq!(line_count, total_lines);
            assert!(!source_omitted);
            let mut baseline = obligation_envelope.clone();
            baseline.action_id = "r1-transitive-baseline-review".into();
            let applied = services
                .submit_agent_action(&baseline)
                .await
                .unwrap_or_else(|error| {
                    panic!(
                        "transitive baseline review failed: {error}; diagnostics={}",
                        r1_goal_proof_diagnostic(&services, fixture_program_id.as_str()),
                    )
                });
            assert_eq!(
                applied.status,
                harness_contract::agent_action::AgentActionStatus::Applied,
                "{applied:?}"
            );
            let goal = services
                .goal_store()
                .get("goal:root-agentic-execution")
                .unwrap()
                .unwrap();
            let proof = goal.reviews.last().unwrap().verification.as_ref().unwrap();
            let inputs = proof.input_set.as_ref().unwrap();
            assert!(inputs.packet_digests.contains_key(producer_graph));
            assert_eq!(
                proof
                    .result_source_revisions
                    .get(&approval_source.stream_id),
                Some(&approval_source.expected_revision)
            );
            assert!(inputs.material_events.iter().any(|event| event
                .stream_id
                .starts_with(&format!("execution-agent-receipts:{producer_graph}:"))));
            assert!(inputs
                .required_scopes
                .iter()
                .any(|target| target.path.workspace_relative_path == "review-effect.txt"));
            let current = action_service.project(fixture_program_id.as_str()).unwrap();
            let original_snapshot = crate::agentic::review_evidence::validate_proof_inputs(
                services.event_store(),
                &current,
                Some(&goal),
                proof,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref(),
                },
            )
            .expect("the unmodified real transitive proof must remain consumable");
            assert_eq!(original_snapshot.semantic_digest, inputs.semantic_digest);
            for mutation in 0..4 {
                let mut omitted = proof.clone();
                let selected = omitted.input_set.as_mut().unwrap();
                match mutation {
                    0 => selected.required_scopes.clear(),
                    1 => selected.material_events.clear(),
                    2 => {
                        selected.packet_digests.remove(producer_graph);
                    }
                    _ => selected.source_revisions.clear(),
                }
                let rejected = crate::agentic::review_evidence::proof_input_snapshot(
                    services.event_store(),
                    &current,
                    Some(&goal),
                    &omitted,
                    crate::agentic::review_evidence::ResultContentReaders {
                        process_inputs: None,
                        artifacts: Some(services.artifact_store().as_ref()),
                        history: services.session_history_reader().as_deref(),
                    },
                );
                assert!(rejected.is_err(), "manifest omission {mutation} must fail original-source reconstruction, not only the final digest comparison");
                assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
            }
            write_root_review_effect(
                runtime,
                "r1-transitive-change-input",
                verified_delegated_content,
            )
            .await;
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                    .unwrap(),
                verified_delegated_content
            );
            let current = action_service.project(fixture_program_id.as_str()).unwrap();
            assert!(crate::agentic::review_evidence::validate_proof_inputs(
                services.event_store(),
                &current,
                Some(&goal),
                proof,
                crate::agentic::review_evidence::ResultContentReaders {
                    process_inputs: None,
                    artifacts: Some(services.artifact_store().as_ref()),
                    history: services.session_history_reader().as_deref()
                }
            )
            .is_err());
            let rejected = services
                .submit_agent_action(&obligation_envelope)
                .await
                .unwrap_err();
            assert!(
                rejected.contains("review_requires_effect_observation"),
                "{rejected}"
            );
            scoped_goal_reviewer
                .as_ref()
                .unwrap()
                .reader
                .execute_tool(
                    "r1-independent-transitive-input-read",
                    "read_file",
                    r#"{"path":"review-effect.txt","complete":true}"#,
                )
                .await
                .unwrap();
            println!(
                "R1_TRANSITIVE_MATERIAL_WITNESS {}",
                serde_json::json!({
                    "producer_graph":producer_graph,"approval_source":approval_source,
                    "fresh_review_without_changed_input_refused":true,"independent_input_retrieved":true,
                })
            );
        });
        transitive_revalidation_stage.await;
    }
    if proof_scenario == R1ProofScenario::PartialDependency {
        let partial_dependency_stage = boxed_fixture_stage(|| async {
            let reviewer = scoped_goal_reviewer.as_ref().unwrap();
            let (_, root_runtime) = root_reads_generic_results(
                &services,
                effect_host.clone(),
                &content.selector,
                false,
                true,
            )
            .await;
            write_root_review_effect(
                &root_runtime,
                "r1-change-dependent-last-line",
                verified_delegated_content,
            )
            .await;
            assert_eq!(
                std::fs::read_to_string(services.workspace_root().join("review-effect.txt"))
                    .unwrap(),
                verified_delegated_content
            );
            let actual_write = services
                .event_store()
                .list_stream("session:session-dispatch")
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "tool.invocation.completed"
                        && event.payload["tool_call_id"] == "r1-change-dependent-last-line"
                })
                .expect("the dependency mutation must have a real completed invocation");
            assert!(actual_write.payload["governed_plan_id"].as_str().is_some());
            let actual_root_journal = services.event_store().list_stream("session:session-dispatch").unwrap()
    .into_iter().filter(|event| event.kind == "tool.execution_plan.created"
        || event.kind.starts_with("tool.invocation."))
    .map(|event| serde_json::json!({"kind":event.kind,"payload":event.payload,"cursor":event.commit_cursor}))
    .collect::<Vec<_>>();
            println!(
                "R1_ROOT_DEPENDENCY_JOURNAL {}",
                serde_json::json!(actual_root_journal)
            );
            reviewer
                .reader
                .execute_tool(
                    "r1-refresh-other-dependency",
                    "read_file",
                    r#"{"path":"unrelated-review.txt","complete":true}"#,
                )
                .await
                .unwrap();
            for (index, input) in [
                serde_json::json!({"path":"review-effect.txt","limit":1}),
                serde_json::json!({"path":"review-effect.txt","offset":1,"limit":1}),
                serde_json::json!({"path":"review-effect.txt","limit":2}),
            ]
            .into_iter()
            .enumerate()
            {
                let output = reviewer
                    .reader
                    .execute_tool(
                        &format!("r1-partial-dependency-{index}"),
                        "read_file",
                        &input.to_string(),
                    )
                    .await
                    .unwrap();
                let actual: serde_json::Value = serde_json::from_str(&output).unwrap();
                assert_eq!(actual["truncated"], true, "{actual}");
                assert_eq!(actual["file"]["totalLines"], 3);
                assert!(!output.contains("changed dependency last line"), "{output}");
                let rejected = services
                    .submit_agent_action(&obligation_envelope)
                    .await
                    .unwrap_err();
                assert!(
                    rejected.contains("review_requires_effect_observation"),
                    "{rejected}"
                );
            }
            root_review_target_tool(
                &root_runtime,
                "r1-producer-only-complete-read",
                "read_file",
                "review-effect.txt",
                "",
            )
            .await;
            let root_read = services
                .event_store()
                .list_stream("session:session-dispatch")
                .unwrap()
                .into_iter()
                .find(|event| {
                    event.kind == "tool.invocation.completed"
                        && event.payload["tool_call_id"] == "r1-producer-only-complete-read"
                })
                .expect(
                    "the non-reviewer must really complete the read, not merely attempt it",
                );
            let root_output_ref = root_read.payload["full_output_ref"].as_str().unwrap();
            let access = services
                .session_evidence_access(
                    "session-dispatch",
                    root_output_ref.strip_prefix("tool://").unwrap(),
                )
                .await
                .unwrap()
                .unwrap();
            let root_output = services
                .artifact_store()
                .resolve(&access.retrieval_selector)
                .unwrap();
            let root_bytes = services
                .artifact_store()
                .read(&root_output, "session:session-dispatch", None)
                .await
                .unwrap();
            let actual_root_output: serde_json::Value =
                serde_json::from_slice(&root_bytes).unwrap();
            assert_eq!(actual_root_output["truncated"], false);
            assert_eq!(actual_root_output["file"]["startLine"], 1);
            assert_eq!(actual_root_output["file"]["numLines"], 3);
            assert!(String::from_utf8(root_bytes)
                .unwrap()
                .contains("changed dependency last line"));
            let rejected = services
                .submit_agent_action(&obligation_envelope)
                .await
                .unwrap_err();
            assert!(
                rejected.contains("review_requires_effect_observation"),
                "{rejected}"
            );
            let output = reviewer
                .reader
                .execute_tool(
                    "r1-independent-complete-dependency",
                    "read_file",
                    r#"{"path":"review-effect.txt","complete":true}"#,
                )
                .await
                .unwrap();
            let actual: serde_json::Value = serde_json::from_str(&output).unwrap();
            assert_eq!(actual["truncated"], false);
            assert_eq!(actual["file"]["numLines"], 3);
            assert!(output.contains("changed dependency last line"));
            println!(
                "R1_PARTIAL_DEPENDENCY_WITNESS {}",
                serde_json::json!({
                    "reviewer":reviewer.packet.graph_id(), "partial_windows_rejected":3,
                    "producer_only_read_rejected":true, "complete_independent_read":actual,
                })
            );
        });
        partial_dependency_stage.await;
    }
    if let R1ProofScenario::ReadDelivery(case) = proof_scenario {
        let reviewer = scoped_goal_reviewer.as_ref().unwrap();
        if exercise_process_proof_case(
            &services,
            &effect_host,
            reviewer,
            &obligation_envelope,
            &content.selector,
            &bind_root(root("real-process-case-root", task.action.clone())).actor,
            case,
        )
        .await
        {
            let current = services
                .agent_action_service()
                .project(&reviewer.actor.program_id)
                .unwrap();
            if current.tasks[&reviewer.task_ref]
                .pending_retirement
                .is_none()
                && current.tasks[&reviewer.task_ref].status
                    != crate::AgenticTaskStatus::Withdrawn
            {
                let retirement = bind_root(root(
                    "negative-probe-original-retirement",
                    AgentAction::TaskWithdraw(
                        harness_contract::agent_action::TaskWithdrawInput {
                            task_ref: reviewer.task_ref.clone(),
                            reason_ref: content.selector.clone(),
                            evidence_refs: vec![content.selector.clone()],
                        },
                    ),
                ));
                assert_eq!(
                    services
                        .submit_agent_action(&retirement)
                        .await
                        .unwrap()
                        .status,
                    harness_contract::agent_action::AgentActionStatus::Applied
                );
            }
            services
                .cancel_execution_tree(reviewer.packet.graph_id(), "delivery negative complete")
                .await
                .unwrap();
            wait_retired_process(&services, &reviewer.packet, &reviewer.task_ref).await;
            drain_workers().await;
            return None;
        }
    }
    let mut raced_material = None;
    #[cfg(test)]
    if matches!(
        proof_scenario,
        R1ProofScenario::ProgramPublication(_)
            | R1ProofScenario::Mechanical
            | R1ProofScenario::MechanicalApproval
            | R1ProofScenario::MechanicalExtra
            | R1ProofScenario::MechanicalCrossTeam
            | R1ProofScenario::ReviewAdmission
    ) {
        let reviewer = scoped_goal_reviewer.as_ref().unwrap();
        assert!(
            !exercise_process_proof_case(
                &services,
                &effect_host,
                reviewer,
                &obligation_envelope,
                &content.selector,
                &bind_root(root("program-process-positive", task.action.clone())).actor,
                RealProcessProofCase::NativeSources
            )
            .await
        );
    }
    #[cfg(test)]
    let physical_history = if proof_scenario == R1ProofScenario::PhysicalLineage {
        prepare_physical_lineage_history(
            &services,
            &effect_host,
            &bind_root(root("physical-history-root", task.action.clone())).actor,
            &content.selector,
            &obligation_envelope,
        )
        .await
    } else {
        Vec::new()
    };
    if proof_scenario == R1ProofScenario::PhysicalOpaque {
        assert_other_processes_host_only(&services, &optional_effect.as_ref().unwrap().packet);
    }
    #[cfg(test)]
    if proof_scenario == R1ProofScenario::ReviewAdmission {
        let current = action_service.project(fixture_program_id.as_str()).unwrap();
        let AgentAction::ObjectiveReview(input) = &obligation_envelope.action else {
            unreachable!()
        };
        let donor = services
            .independent_result_review(
                &obligation_envelope.actor,
                &current,
                &input.result_refs,
                Some(&input.criterion_ref),
            )
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "original review admission failed: {error}; diagnostics={}",
                    r1_goal_proof_diagnostic(&services, &current.program_id),
                )
            });
        assert_eq!(donor.verification.input_set.as_ref().unwrap().version, 2);
        assert!(!donor
            .verification
            .input_set
            .as_ref()
            .unwrap()
            .material_events
            .is_empty());
        let probe = NATIVE_REVIEW_ASYNC_PROBE.with(|slot| slot.borrow_mut().take().unwrap());
        probe(services.0.clone(), obligation_envelope.clone(), donor).await;
        assert!(drain_timing == PhysicalDrainTiming::BeforeGoal);
        scoped_goal_reviewer.take().unwrap().finish(&services).await;
        services.shutdown_maintenance().await;
        return None;
    }
    Some((
        native_sources,
        reviewed_delivery_refs,
        transitive_inputs,
        bounded_selected_body_receipts,
        obligation_envelope,
        raced_material,
        {
            #[cfg(test)]
            {
                physical_history
            }
            #[cfg(not(test))]
            {
                ()
            }
        },
    ))
}
