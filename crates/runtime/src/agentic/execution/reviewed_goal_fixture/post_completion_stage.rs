{
    if drain_timing == PhysicalDrainTiming::AfterGoal {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        let program = action_service.project(fixture_program_id.as_str()).unwrap();
        let manifest = crate::agentic::review_evidence::work_manifest_digest(&program);
        assert_eq!(
            goal.completion,
            GoalCompletion::Satisfied,
            "{program:?} {goal:?}"
        );
        assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
        assert_eq!(
            program.tasks[&task_ref].status,
            crate::AgenticTaskStatus::Accepted
        );
        assert!(!goal.reviews.is_empty());
        assert!(goal
            .reviews
            .iter()
            .all(|review| review
                .verification
                .as_ref()
                .is_some_and(
                    |proof| !proof.reads.is_empty() && proof.effect_manifest_digest.is_some()
                )));
        assert!(confirmed_run_exit_with_retry(services.event_store(), &packet).is_none());
        assert!(confirmed_run_exit_with_retry(
            services.event_store(),
            &reviewer_session.packet
        )
        .is_none());
        for (execution_id, node_id) in [
            (packet.graph_id(), packet.node_id()),
            (review_graph.id.as_str(), review_graph.nodes[0].id.as_str()),
        ] {
            assert_eq!(
                services
                    .graph_state_store()
                    .load(execution_id)
                    .unwrap()
                    .node_statuses[node_id],
                ExecutionNodeStatus::Running
            );
        }
        if let Some(writer) = writer.as_ref() {
            let source = format!(
                "execution-agent-receipts:{}:{}:{}",
                packet.graph_id(),
                packet.node_id(),
                packet.attempt
            );
            let revision = services.event_store().stream_revision(&source).unwrap();
            let calls = effect_host.calls.lock().unwrap().len();
            let target = services.workspace_root().join("review-effect.txt");
            let bytes_before = std::fs::read(&target).unwrap();
            let error = writer.execute_tool("after-goal-before-drain", "edit", r#"{"path":"review-effect.txt","content":"must not overwrite verified target"}"#).await.unwrap_err();
            let expected_fence = if proof_scenario == R1ProofScenario::ExecutionEpoch {
                "ProcessJsonl packet policy revision is stale"
            } else {
                "tool_effect_authority"
            };
            assert!(error.to_string().contains(expected_fence), "{error}");
            assert_eq!(
                services.event_store().stream_revision(&source).unwrap(),
                revision
            );
            assert_eq!(effect_host.calls.lock().unwrap().len(), calls);
            assert_eq!(std::fs::read(&target).unwrap(), bytes_before);
        }
        let mut late_semantic = task.clone();
        late_semantic.action_id = "after-goal-before-drain-publish".into();
        let refused = services.submit_agent_action(&late_semantic).await.unwrap();
        assert_eq!(
            refused.status,
            harness_contract::agent_action::AgentActionStatus::Rejected
        );
        assert_eq!(refused.error.unwrap().code, "program_terminal");
        println!(
            "V3_DRAIN_WITNESS {}",
            serde_json::json!({
                "goal": goal.id, "goal_revision": goal.revision,
                "program_revision": program.revision, "verdict": program.objective_verdict,
                "producer": packet.graph_id(), "reviewer": review_graph.id,
                "physical_before": "running", "effect_host_calls": effect_host.calls.lock().unwrap().len(),
            })
        );
        drain_workers().await;
        assert_eq!(
            services
                .goal_store()
                .get("goal:root-agentic-execution")
                .unwrap()
                .unwrap(),
            goal
        );
        let drained = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(drained.objective_verdict, program.objective_verdict);
        assert_eq!(
            crate::agentic::review_evidence::work_manifest_digest(&drained),
            manifest
        );
        assert_eq!(
            drained.tasks[&task_ref].status,
            crate::AgenticTaskStatus::Accepted
        );
    }
    if let Some((_, _, approval_source)) = transitive_inputs.as_ref() {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(goal.completion, GoalCompletion::Satisfied);
        crate::agentic::review_evidence::committed_proof_sources(
            services.event_store(),
            &goal,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: None,
                artifacts: Some(services.artifact_store().as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        )
        .unwrap();
        let program_before = action_service.project(fixture_program_id.as_str()).unwrap();
        let protected_streams = [
            format!("goal:{}", goal.id),
            format!("agentic-program:{}", fixture_program_id),
        ]
        .into_iter()
        .map(|stream| {
            let events = services.event_store().list_stream(&stream).unwrap();
            (stream, events)
        })
        .collect::<Vec<_>>();
        let original = services
            .event_store()
            .list_stream(&approval_source.stream_id)
            .unwrap()
            .pop()
            .unwrap();
        services
            .event_store()
            .append(crate::RuntimeEventInput {
                stream_id: approval_source.stream_id.clone(),
                scope: original.scope,
                kind: original.kind,
                status: original.status,
                actor: original.actor,
                refs: original.refs,
                payload: original.payload,
            })
            .unwrap();
        assert!(
            services
                .event_store()
                .stream_revision(&approval_source.stream_id)
                .unwrap()
                > approval_source.expected_revision
        );
        let cursor_after_fault = services.event_store().durable_commit_cursor().unwrap();
        let refusal = crate::agentic::review_evidence::committed_proof_sources(
            services.event_store(),
            &goal,
            crate::agentic::review_evidence::ResultContentReaders {
                process_inputs: None,
                artifacts: Some(services.artifact_store().as_ref()),
                history: services.session_history_reader().as_deref(),
            },
        )
        .err()
        .expect("changed authenticated decision source must invalidate B's old content");
        assert_eq!(
            refusal,
            "external decision content predates its current authenticated decision source"
        );
        assert_eq!(services.goal_store().get(&goal.id).unwrap().unwrap(), goal);
        assert_eq!(
            action_service.project(fixture_program_id.as_str()).unwrap(),
            program_before
        );
        for (stream, events) in protected_streams {
            assert_eq!(services.event_store().list_stream(&stream).unwrap(), events);
        }
        assert_eq!(
            services.event_store().durable_commit_cursor().unwrap(),
            cursor_after_fault
        );
        println!(
            "R1_TRANSITIVE_APPROVAL_SOURCE_FAULT {}",
            serde_json::json!({
                "source":approval_source.stream_id,"fault":"duplicate original decision event through actual event owner",
                "committed_b_inputs_refused":refusal,"historical_a_unchanged":true,
                "rejection_class":"authenticated_decision_content_freshness",
                "half_write":false,"not_claimed":"CAS race, new legitimate approval decision or full B/Session fault chain",
            })
        );
    }
    if delegated_effect {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(goal.completion, GoalCompletion::Satisfied);
        assert_eq!(
            action_service
                .project(fixture_program_id.as_str())
                .unwrap()
                .status,
            crate::AgenticProgramStatus::Verified
        );
        assert!(
            crate::execution_core::goal::assessment::current_reviews_satisfy_the_goal(&goal)
        );
        let conditions = crate::agentic::review_evidence::required_review_conditions(&goal);
        assert!(!conditions.is_empty());
        for condition in conditions {
            let review = goal
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == condition)
                .expect("satisfied goal requires its current required review");
            assert_eq!(review.decision, "satisfied");
            assert_eq!(review.spec_revision, goal.spec_revision);
            assert_eq!(review.input_manifest_digest, goal.spec_digest);
            let proof = review
                .verification
                .as_ref()
                .expect("current satisfied required review must carry verification");
            if let Some(native) = native_sources.as_ref() {
                for source_packet in [&packet, &native.packet] {
                    assert!(
                        proof.effect_source_refs.contains(&format!(
                            "execution-agent-receipts:{}:{}:{}",
                            source_packet.graph_id(),
                            source_packet.node_id(),
                            source_packet.attempt,
                        )),
                        "the combined branch must fence both actual producer sources"
                    );
                }
            } else {
                assert_eq!(
                    proof.effect_source_refs,
                    vec![format!(
                        "execution-agent-receipts:{}:{}:{}",
                        packet.graph_id(),
                        packet.node_id(),
                        packet.attempt
                    )]
                );
            }
            assert!(proof.effect_manifest_digest.is_some());
            if native_sources.is_none() {
                assert!(proof.reads.iter().all(|read| read.result_kind
                    == harness_contract::goal::GoalResultKind::ToolEffect
                    && !read.effect_observation_refs.is_empty()));
            } else {
                let original = proof
                    .reads
                    .iter()
                    .find(|read| read.result_ref == artifact)
                    .unwrap();
                assert_eq!(
                    original.result_kind,
                    harness_contract::goal::GoalResultKind::ToolEffect
                );
                assert!(!original.effect_observation_refs.is_empty());
            }
        }
    }
    if with_disclosure {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        assert_eq!(goal.completion, GoalCompletion::Satisfied);
        let program = action_service.project(fixture_program_id.as_str()).unwrap();
        assert_eq!(program.status, crate::AgenticProgramStatus::Verified);
        assert_eq!(
            program.tasks[&task_ref].unresolved,
            vec!["Performance estimate lacks enterprise measurement"]
        );
        let issue = crate::agentic::issues::issues(&program).remove(0);
        assert_eq!(
            issue.disposition.unwrap().disposition,
            harness_contract::agent_action::IssueDispositionKind::Disclose
        );
        assert!(issue.adjudication_ref.is_some());
        return;
    }
    if let Some(native) = native_sources.as_ref() {
        let goal = services
            .goal_store()
            .get("goal:root-agentic-execution")
            .unwrap()
            .unwrap();
        for criterion in ["reviewed-delivery", "source-backed-delivery"] {
            let proof = goal
                .reviews
                .iter()
                .rev()
                .find(|review| review.criterion_ref == criterion)
                .and_then(|review| review.verification.as_ref())
                .unwrap();
            assert!(proof.input_set.as_ref().unwrap().packet_digests.contains_key(native.packet.graph_id()),
            "Goal must consume the actual Native Task source, not merely observe a neighboring material");
            assert!(proof
            .producer_execution_ids
            .iter()
            .any(|execution| execution
                == &format!("agent-run:{}", native.packet.run_id())),
            "Goal must consume the actual Native Task producer execution, not merely observe a neighboring material");
            for reference in &native.result_refs {
                assert!(
                    proof.reads.iter().any(|read| &read.result_ref == reference),
                    "the original independent Goal review must cover every Native result"
                );
            }
        }
    }
    let episode_program = action_service.project(fixture_program_id.as_str()).unwrap();
    let episode_goal = services
        .goal_store()
        .get("goal:root-agentic-execution")
        .unwrap()
        .unwrap();
    assert_eq!(
        episode_program.status,
        crate::AgenticProgramStatus::Verified,
        "episode requires the actual original A, not a synthetic B: {}",
        r1_goal_proof_diagnostic(&services, fixture_program_id.as_str())
    );
    assert_eq!(
        episode_goal.completion,
        GoalCompletion::Satisfied,
        "episode requires the original satisfied Goal"
    );
    let episode_verdict = episode_program
        .objective_verdict
        .as_ref()
        .expect("verified Program must retain its real Objective verdict");
    assert_eq!(episode_verdict.goal_id, episode_goal.id);
    let episode_sources = services
        .event_store()
        .list_stream(&format!("agentic-program:{fixture_program_id}"))
        .unwrap();
    assert!(
        episode_sources
            .iter()
            .any(|event| event.scope == crate::RuntimeEventScope::Program
                && event.kind == "agentic.objective_verdict_bound"
                && event.payload["verdict"] == serde_json::to_value(episode_verdict).unwrap()),
        "the original A transaction must emit the actual evolution projection source"
    );
    let projector =
        crate::evolution::collaboration_experience::CollaborationExperienceProjector::new(
            Arc::clone(services.event_store()),
            services.graph_state_store().clone(),
            services.workspace_key().to_owned(),
        );
    let episode_checkpoint_before = services
        .event_store()
        .projection_checkpoint("projector:agentic-collaboration-experience:v1");
    let mut episode_projection_passes = Vec::new();
    loop {
        let pass = projector
            .project_available(64)
            .expect("production episode projection");
        episode_projection_passes.push(pass);
        if !pass.backlog {
            break;
        }
    }
    let episodes = services
        .collaboration_experience_episodes(10)
        .expect("public experience read model");
    if episodes.len() != 1 {
        let checkpoint_after = services
            .event_store()
            .projection_checkpoint("projector:agentic-collaboration-experience:v1");
        let verdicts = services
            .event_store()
            .list_stream(&format!("agentic-program:{fixture_program_id}"))
            .map(|events| {
                events
                    .into_iter()
                    .filter(|event| event.kind == "agentic.objective_verdict_bound")
                    .collect::<Vec<_>>()
            });
        let evolution_streams = services
            .event_store()
            .stream_ids_for_scope(crate::RuntimeEventScope::Evolution);
        eprintln!(
        "R1_EPISODE_PROJECTION_FAILURE checkpoints_before={episode_checkpoint_before:?}; checkpoints_after={checkpoint_after:?}; passes={episode_projection_passes:?}; verdicts={verdicts:?}; evolution_streams={evolution_streams:?}; diagnostics={}",
        r1_goal_proof_diagnostic(&services, fixture_program_id.as_str()),
    );
    }
    assert_eq!(
        episodes.len(),
        1,
        "original verified Objective must produce exactly one durable episode"
    );
    if proof_scenario == R1ProofScenario::OptionalSourceRace {
        assert_eq!(
            episodes[0].outcome,
            harness_contract::evolution::CollaborationExperienceOutcome::Completed
        );
        assert_eq!(episodes[0].coverage.required_obligation_count, 2);
        assert_eq!(episodes[0].coverage.satisfied_obligation_count, 1);
        assert_eq!(episodes[0].coverage.coverage_basis_points, 5000);
        assert!(!episodes[0].coverage.reusable);
        assert!(!episodes[0].is_pattern_eligible());
    } else {
        assert!(episodes[0].is_pattern_eligible(), "{:#?}", episodes[0]);
    }
    assert!(episodes[0]
        .resource_summary
        .context_reservation_tokens
        .is_none());
    assert!(!serde_json::to_string(&episodes[0])
        .unwrap()
        .contains("Source-backed findings"));
    assert!(
        services
            .collaboration_semantic_patterns(10)
            .unwrap()
            .is_empty(),
        "one Turn is not a reusable pattern"
    );
    projector.project_available(64).expect("idempotent replay");
    assert_eq!(
        services
            .collaboration_experience_episodes(10)
            .unwrap()
            .len(),
        1
    );
}
